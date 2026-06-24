# Colmena — improvements to make two real footguns catchable first-time

Context: these two issues bit a downstream project (`mediavida-api`, a 3-node
Raft cluster on Fly.io) and were hard to detect — they only surface under leader
elections / quorum churn / version skew, and the library *already half-detects
both but neither prevents them nor surfaces them loudly*. Each section below is
self-contained and actionable. Tackle them in priority order; they're independent.

Repo: `github.com/mentasystems/colmena` (this working copy: `/Users/jairo/colmena`).
Current `CommandFormatVersion = 2` (`version.go:22`).

---

## Issue 1 — `ConsistencyWeak` (the default) makes reads UNAVAILABLE without a leader, and the naming hides it

### What happened
The downstream app stored per-device sessions in colmena and read them with the
default consistency (`ConsistencyWeak`). `Weak` forwards reads to the leader, so
during a leader election / quorum blip / partition there is no reachable leader
and **the read returns an error**. The app's auth gate mapped that error to HTTP
401 and logged users out — even though their session was perfectly valid and sat
replicated in every node's local SQLite. The failure only appears during
leadership changes, so it's intermittent and very hard to reproduce.

### Root cause / why it's a footgun
- `consistency.go`: `ConsistencyNone` (local read, always available) vs
  `ConsistencyWeak` (leader-forwarded, fails with no leader) vs `Strong` vs
  `Lease`. The naming is backwards-intuitive: **"Weak" sounds like "most relaxed
  / most available", but it is LESS available than `None`.** A newcomer leaves
  the default or picks "Weak" expecting a normal always-available DB read.
- The godocs describe **freshness** and **latency** for each level but never
  state the **availability** axis — i.e. that `Weak`/`Strong` reads *return an
  error when there is no reachable leader*. That single missing sentence is what
  made this invisible until production.
- `config.go:174` silently defaults `Consistency` to `Weak`, so the
  surprising-on-failure behavior is the default.

### Tasks (priority: do the docs + typed error first — small, zero-risk)

1. **Godoc availability line.** In `consistency.go`, add to `ConsistencyWeak`
   and `ConsistencyStrong` an explicit availability note, e.g.:
   `// AVAILABILITY: this read returns an error when there is no reachable
   leader (elections, quorum loss, partition). Use ConsistencyNone or
   ConsistencyLease if reads must stay available during leadership changes.`
   Mirror the inverse on `None`/`Lease` ("stays available without a leader").

2. **README availability table.** The docs already cover latency + freshness;
   add the missing column **"Available without a leader?"**:
   - `None`: yes (local replica) · `Lease`: yes, within the lease window ·
     `Weak`: **no** · `Strong`: **no**.
   Plus a one-line decision guide: "pick by availability first, then freshness."

3. **Typed sentinel error for the no-leader case.** When a `Weak`/`Strong` read
   fails because there is no reachable leader, surface a distinguishable error
   (e.g. `var ErrNoLeader = errors.New("colmena: no reachable leader")` or
   `ErrUnavailable`) instead of a generic/forwarded error. Downstream had to
   invent its own sentinel and string-match to tell "transient unavailability"
   (→ retry / 503) apart from a real DB error or a genuine "row not found".
   A typed error from the driver makes the correct handling obvious.
   - Check the forward path in `driver.go` (`leaderQuery` / `QueryContext`,
     ~lines 86–114) and wrap the no-leader/forward-failure with the sentinel.

4. **(Optional) Make `OpenDB` consistency explicit.** Consider requiring an
   explicit `ConsistencyLevel` (or at least a doc banner on the default) so the
   availability trade-off is a conscious choice, not a silent default.

### Acceptance
- Reading the `ConsistencyWeak` godoc tells you, without running anything, that
  the read can fail without a leader and what to use instead.
- A caller can do `errors.Is(err, colmena.ErrNoLeader)` to map transient
  unavailability to a retry without string-matching.

---

## Issue 2 — `CommandFormatVersion` bumps wedge old nodes mid-rolling-deploy; detected but not prevented

### What happened
The cluster ran an old colmena (format v1). Deploying a newer colmena (v0.12.0,
which **writes** `CommandFormatVersion = 2`) one node at a time via a normal
rolling deploy created a mixed-version window: the upgraded node became (or
contacted) a leader and replicated v2-encoded commands, and the still-v1 nodes
could not decode them — logging `fsm apply unmarshal error: unmarshal command
version 2: unsupported format version` in a tight loop while silently failing to
apply committed entries (state divergence). Recovery required rolling the odd
node back and eventually deploying **all** nodes together (`--strategy
immediate`). A plain "bump the dep + rolling deploy" is unsafe across a format
bump, and nothing in the library stops you.

### Root cause / why it's a footgun
- `command.go:88` encodes with the **constant** `CommandFormatVersion`
  unconditionally — a node writes the newest format the moment it boots,
  regardless of whether its peers can read it.
- The handshake **already exchanges** `CommandFormatVersion` both ways and
  **already detects** the skew: `node.go:674` logs
  `"peer X (vY) writes command format vN, local max vM — will reject its log
  entries"`. But it's a buried `log.Printf` with no guardrail and no escalation.
- `fsm apply unmarshal error … unsupported format version` (the apply-side
  symptom) is logged at INFO and the node keeps limping. "I cannot apply
  committed entries" = my state is diverging — that should be loud.

### Tasks (priority: the negotiated write-version is the highest-value change in this file)

1. **Negotiated effective write-version (the real fix).** The leader must not
   *write* a newer `CommandFormatVersion` until **every current voter** advertises
   it can *read* it. Compute `effectiveWriteVersion = min(maxSupportedFormat
   across all voters)` from the handshake data already collected, and have the
   command encoder (`command.go:88`) use `effectiveWriteVersion` instead of the
   raw constant. Bump to v2 only once all voters report ≥ v2. This makes rolling
   upgrades **safe by construction** — removing the entire class of "wedged
   mid-deploy" bugs. Add tests: 3-node cluster, one node upgraded → leader keeps
   writing v1 until all upgraded, then flips to v2; old nodes never see an
   undecodable entry. Apply the same idea to `SnapshotFormatVersion`.

2. **Surface the skew in `Stats()` / health.** Expose `FormatSkew bool` and/or
   `PeersBehind []NodeID` (and the inverse — peers ahead of me) so a health check
   or dashboard shows "cluster mid-migration" instead of it only being visible as
   `fsm apply` log spam. Ideally a health check can be wired to fail on skew.

3. **Escalate the apply-side error.** Raise `fsm apply unmarshal error:
   unsupported format version` from INFO to WARN/ERROR, emit a metric/counter,
   and consider failing readiness — silent non-apply of committed entries is a
   divergence condition, not an info event.

4. **Write `UPGRADING.md`.** Document that crossing a `CommandFormatVersion`
   (or `SnapshotFormatVersion`) bump is a **format migration, not a normal
   deploy**. Until task 1 lands, the safe procedure is: deploy all nodes together
   (Fly `--strategy immediate`, or stop-all/start-all) so there is no mixed-format
   window; do NOT do a one-at-a-time rolling deploy across a format bump. Note
   that the new version can *read* both old and new formats (decode switch in
   `command.go`, `case 1` / `case 2`) — the danger is purely old nodes reading
   new writes during the transition.

### Acceptance
- A one-at-a-time rolling deploy across a `CommandFormatVersion` bump no longer
  produces `unsupported format version` on any node (leader writes the old
  format until all voters support the new one).
- `Stats()` / health makes a version-skewed cluster obvious without log diving.

---

## Notes / cross-references
- `BUGS.md` item #1 ("Query RPC is not leadership-gated → forwarded reads
  silently return stale data") shows awareness of consistency subtleties; the
  Issue 1 docs work should tie into that.
- `PLAN_FLY.md` §3.5 and prior cold-start work already cover rolling-deploy
  quorum safety and the full-cluster-restart deadlock (fixed in v0.12.0); Issue 2
  is the *format-version* dimension of safe rolling deploys, which those docs
  don't yet address.
- Keep changes backward-compatible: the decode side must continue to read all
  previously-written format versions (it does today: `command.go` `case 1`/`2`).
