# PLAN_FLY — Fly.io autodiscovery for Colmena

Status: planning. Authored 2026-06-07.
Goal: let a Colmena node **auto-form and auto-join a cluster on Fly.io** with
zero hard-coded peer addresses, surviving Fly's ephemeral machines (every deploy
recreates machines with new private IPs).

First consumer: `mediavida-api` (see `~/mediavida/PLAN_SCALE.md`, Phase 2), which
embeds Colmena as the HA session/job store. 3 voter nodes in a single region.

---

## 1. Why the existing `lan/` package doesn't work on Fly

`lan/` discovers peers via **mDNS/zeroconf** (`lan/discovery.go`, `hashicorp/mdns`
+ `zeroconf`). mDNS relies on multicast on a shared L2 segment. **Fly's private
network (6PN) is a WireGuard mesh with no multicast** — mDNS browsing finds
nobody. Everything else in `lan/` is reusable; only the *peer discovery
transport* must change.

What we keep from `lan/` (logic, not transport):

- `decideRole` / `decideRoleFromPeers` — bootstrap-vs-join election from the peer
  set (`lan/lan.go`).
- `sweepDeadVoters` — leader reaps voters not seen for a TTL via `RemoveNode`.
- `promoteIfNeeded` — auto-promote a non-voter when a voter dies.
- `waitForFormedCluster`, role naming, the `Cluster` lifecycle in `lan/lan.go`.
- mTLS identity (`lan/identity.go`) — still useful, but on Fly the 6PN is already
  private; mTLS becomes optional (decide in §6).

The core `Node` membership API is already sufficient:
`New(Config{NodeID, Bind, Bootstrap, Join})`, `AddVoter`, `AddNonvoter`,
`RemoveNode`, `Nodes()`, `IsLeader`, `LeaderAddr`, `WaitForLeader`, and the
`Join` RPC handler (`node.go`).

---

## 2. Fly primitives to use instead of mDNS

Fly exposes cluster topology through **internal DNS** over 6PN (no multicast
needed):

- `<app>.internal` — AAAA records: the 6PN IPv6 of **every** instance of the app.
- `<region>.<app>.internal` — AAAA: instances in a specific region (we pin one
  region, so this is the working set).
- `vms.<app>.internal` — TXT: `"<machine_id> <region>,<machine_id> <region>,…"`
  (machine id + region for each instance; handy for stable node IDs).
- `_instances.internal` / `regions.<app>.internal` — auxiliary.

Per-machine env (already set by Fly):

- `FLY_PRIVATE_IP` — this machine's 6PN address (use as Raft advertise addr).
- `FLY_MACHINE_ID` / `FLY_ALLOC_ID` — stable-per-machine id → **Raft NodeID**.
- `FLY_REGION`, `FLY_APP_NAME`.

Discovery on Fly = **periodically resolve `<region>.<app>.internal` (AAAA) or
`vms.<app>.internal` (TXT)** to get the current peer set. That replaces the mDNS
browse loop; the role/sweep/promote logic stays.

---

## 3. Proposed design

### 3.1 Pluggable discovery interface

Refactor so the membership lifecycle in `lan/lan.go` depends on a small
`Discovery` interface, not on zeroconf directly:

```go
// peer set provider; both mDNS and Fly implement this.
type Discovery interface {
    // Start announcing self and begin observing peers.
    Start(ctx context.Context, self Peer) error
    // Current known peers (excluding self), best-effort snapshot.
    Peers() []Peer
    Close() error
}

type Peer struct {
    NodeID    string // FLY_MACHINE_ID on Fly
    Addr      string // host:port for Raft/RPC (6PN IPv6 on Fly)
    Voter     bool
    Bootstrap bool   // advertises it is/was the bootstrapper
    LastSeen  time.Time
}
```

Extract the transport-agnostic parts of `lan/discovery.go` (the `peer`/
`peerRecord` model, `snapshot`, dead-peer TTL) into shared code; keep mDNS as one
`Discovery` impl, add a Fly impl.

### 3.2 New subpackage `fly/`

`fly/discovery.go` implements `Discovery` by DNS polling:

- Resolve `<FLY_REGION>.<FLY_APP_NAME>.internal` AAAA every N seconds (e.g. 2–5s)
  via the Fly internal resolver (`[fdaa::3]:53` / system resolver inside the VM).
- Cross-reference `vms.<app>.internal` TXT to map IP → `machine_id` so peers get
  **stable NodeIDs** (an IP alone is not stable across recreation).
- Maintain `Peers()` with a `LastSeen` TTL; a machine that disappears from DNS
  for > TTL is considered gone.
- Self: `NodeID = FLY_MACHINE_ID`, `Addr = [FLY_PRIVATE_IP]:<raftPort>`.

No announce step is needed (Fly DNS *is* the registry) — `Start` can be a no-op
beyond kicking off the poll loop. mTLS identity from `lan/identity.go` can be
reused if we keep auth on the Raft transport.

### 3.3 Bootstrap vs join (cold start)

Reuse `decideRoleFromPeers`, fed by Fly DNS instead of mDNS:

- On boot, poll DNS for the peer set.
- If **no formed cluster** is visible and this node wins the deterministic
  election (e.g. lowest `machine_id`), it `Bootstrap`s. Others `Join` via the
  leader's RPC `Join`.
- Avoid the **first-deploy race** (N machines boot at once, all see an empty
  set): make bootstrap deterministic (lowest id bootstraps; everyone else waits
  `waitForFormedCluster` then joins). Document a fallback env
  (`COLMENA_BOOTSTRAP_EXPECT=3`) so a node won't bootstrap until it has seen the
  expected peer count or a timeout, to reduce split-brain risk on cold start.

### 3.4 Leave / reaping on deploy

Fly rolling deploys **destroy and recreate machines** (new id, new IP). Two
mechanisms, used together:

- **Graceful leave on SIGTERM**: on shutdown, if leader, transfer leadership;
  then best-effort ask the leader to `RemoveNode(selfID)`. Fly sends SIGTERM with
  a grace period before SIGKILL — wire this into the consumer's shutdown path.
- **Leader-side sweep** (`sweepDeadVoters` reused): the leader removes voters
  absent from Fly DNS for > TTL. This is the safety net when a machine dies
  without a clean leave.

### 3.5 Quorum safety during rolling deploys

3 voters tolerate 1 failure. A naive rolling deploy that replaces a voter before
its replacement has joined can momentarily drop to 2 healthy of 3 (fine) — but
replacing two overlapping in time breaks quorum. Mitigations:

- Configure Fly deploy as **rolling, `max_unavailable = 1`**, one machine at a
  time, with a health check that only reports healthy **after the node has
  rejoined and caught up** (Raft applied index near leader).
- Prefer **add-then-remove**: new machine joins as non-voter, catches up, gets
  promoted, only then is the old one removed (`AddNonvoter` → `AddVoter` →
  `RemoveNode`). `promoteIfNeeded` already covers promotion.

---

## 4. Config surface

New `Config` (or `fly.Config`) fields / env mapping:

| Field | Source on Fly | Notes |
|---|---|---|
| `NodeID` | `FLY_MACHINE_ID` | stable per machine |
| `AdvertiseAddr` | `[FLY_PRIVATE_IP]:<port>` | 6PN IPv6 |
| `Region` | `FLY_REGION` | pin discovery to one region |
| `AppName` | `FLY_APP_NAME` | builds the `.internal` names |
| `DiscoveryInterval` | default 3s | DNS poll cadence |
| `PeerTTL` | default 15s | dead-peer threshold |
| `ExpectedVoters` | `COLMENA_BOOTSTRAP_EXPECT` | cold-start anti-split-brain |

A convenience constructor: `fly.Start(fly.Config{...}) (*Cluster, error)`
mirroring `lan.Start`, so the consumer does one call and gets a clustered `*Node`.

---

## 5. Implementation checklist

- [ ] Extract a `Discovery` interface + shared peer model out of `lan/`
      (transport-agnostic lifecycle in `lan/lan.go`).
- [ ] Add `fly/` subpackage: DNS-poll discovery (`<region>.<app>.internal` AAAA +
      `vms.<app>.internal` TXT → IP↔machine_id).
- [ ] Map Fly env → Config (NodeID, advertise, region, app).
- [ ] Deterministic bootstrap election + `ExpectedVoters` gate.
- [ ] SIGTERM graceful leave (leadership transfer + `RemoveNode`).
- [ ] Reuse `sweepDeadVoters` against Fly DNS as the reaping safety net.
- [ ] Add-then-remove rolling-deploy flow + a "rejoined & caught up" health check
      helper the consumer can expose to Fly.
- [ ] `fly.Start` convenience constructor.
- [ ] Tests: simulate DNS responses (inject a resolver) — bootstrap race, peer
      churn, voter death + promotion, rolling replace keeps quorum.
- [ ] Example under `examples/` and a `fly/README.md`.
- [ ] Doc note: pin all nodes to **one region** (Raft latency); multi-region is
      out of scope.

---

## 6. Open questions

- **mTLS on 6PN**: 6PN is already a private WireGuard mesh per org. Keep
  Colmena's mTLS for defense-in-depth, or drop it on Fly for simplicity? Lean:
  keep it (reuse `lan/identity.go`), low cost.
- **Internal resolver access**: confirm the VM can query `[fdaa::3]:53` directly
  vs. relying on the libc resolver; pick whichever is reliable from Go's
  `net.Resolver`.
- **Stable NodeID across recreation**: `FLY_MACHINE_ID` changes when a machine is
  recreated on deploy. That's fine with add-then-remove (old id reaped, new id
  joins), but confirm Raft is happy with a constantly-rotating server set under
  frequent deploys; if churn is high, consider Fly **standby/clone** machines
  with stable ids, or attach a small per-node volume to persist NodeID.
- **First real-traffic deployment**: Colmena is extensively tested/benchmarked
  but has only run on anonat.org (negligible traffic). mediavida would be its
  first serious load — treat this Fly integration as the hardening milestone.
