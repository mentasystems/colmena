# SINGLE.md — Cross-node single-flight / distributed once

> **Status:** design spec, not yet implemented. Hand this to an implementing
> agent. Author: design session for the mediavida re-login storm (see
> "Motivation"). Implement in this package (`github.com/mentasystems/colmena`).

## Motivation

Colmena already gives callers a Raft-replicated SQL store with tunable read
consistency (`ConsistencyNone | Weak | Strong | Lease`, see `consistency.go`).
What it does **not** give them is a way to ensure that an *expensive,
side-effecting operation runs at most once across the whole cluster at a time*.

Concrete driver: the `mediavida` app runs 3 nodes, each holding one shared
upstream account (one Mediavida login). When the upstream session looks dead,
every node independently tries to re-login. Three simultaneous logins of the
same account from three different egress IPs is exactly what trips the
upstream's anti-bot / guard (email-PIN) defense, which then *fails* the
re-login on all three — a self-inflicted outage. The app wants: "if a re-login
is needed, exactly one node performs it; the others wait for and reuse its
result."

This generalizes well beyond mediavida:

- Leader-only cron / scheduled jobs (send the daily digest once, not 3×).
- Cache stampede protection across nodes (one node recomputes, rest wait).
- One-time migrations / backfills triggered lazily on first request.
- Any "elect one node to do X, fan the result back" pattern.

Today callers can hand-roll this with `IsLeader()` + the SQL store, but it's
subtle (lease expiry, executor crash, result propagation, idempotency) and
everyone gets it slightly wrong. This is infrastructure, so it belongs in
Colmena — *not* the domain logic of validating a Mediavida cookie.

## Goal / non-goals

**Goal:** a small, generic primitive — "run this function once per logical key
across the cluster, within a TTL window; concurrent callers on any node
coalesce onto the single in-flight execution and observe its outcome."

**Non-goals:**
- Not a general mutex held for arbitrary durations (that invites deadlocks on
  node crash). It is *single-flight with a TTL*, not a lock you `Unlock()` by
  hand — though see "Optional: explicit lock" below.
- Not exactly-once *delivery* of side effects. Raft + TTL gives *at-most-once
  per window* with at-least-once only under executor crash mid-flight. Callers
  whose side effect is not idempotent must reconcile (documented below).
- No change to the existing consistency levels or SQL API.

## Proposed public API

Keep it idiomatic with the rest of `node.go` (methods on `*Node`). Two layers:

### Layer 1 — `Do` (single-flight with TTL), the primary API

```go
// Do runs fn exactly once per key across the cluster within a ttl window and
// returns its result to every caller that coalesces onto that execution.
//
// Semantics:
//   - The first caller (cluster-wide) to claim `key` becomes the executor and
//     runs fn locally. The claim is a Raft-committed lease: linearizable, so
//     two nodes cannot both become executor for the same key.
//   - Concurrent callers on any node — while the executor is in flight — block
//     until it finishes, then receive the same (result, err).
//   - After fn returns, the result is committed under the key with the given
//     ttl. Callers arriving within ttl get the cached result without running
//     fn again ("already done this window"). After ttl, the next caller
//     re-executes.
//   - If the executor's node crashes mid-flight, the lease expires after
//     `leaseTTL` (= min(ttl, LeaseTimeout); see config) and the next caller
//     re-claims and re-runs. This is the only path to a second execution.
//
// fn runs on whichever node first claims the key — NOT necessarily the leader,
// and not necessarily the calling node. Do must therefore ship fn's *result*
// (bytes) through Raft, not fn itself. fn stays local to the executor; only
// callers on the executor node run the real closure, remote callers receive
// the committed bytes. See "Execution model" for how this is reconciled.
func (n *Node) Do(ctx context.Context, key string, ttl time.Duration, fn func(ctx context.Context) ([]byte, error)) ([]byte, error)
```

> **Design note on "fn runs where?"** The cleanest model that avoids shipping
> code is: **the executor is always the node that wins the claim, and only that
> node runs fn.** Remote coalescing callers do not run fn; they poll/wait for
> the committed result row. This keeps `fn` an ordinary local closure (can
> close over node-local state like an HTTP client / cookie jar — exactly what
> mediavida needs). The claim + result live in Raft so every node sees them.

### Layer 2 — explicit primitives (optional, build `Do` on top)

Expose these if callers need finer control; `Do` should be implementable purely
in terms of them:

```go
// TryClaim attempts to become the executor for key, committing a lease valid
// for leaseTTL. Returns claimed=true if this node won, false if someone else
// holds a live claim or a fresh result already exists.
func (n *Node) TryClaim(key string, leaseTTL time.Duration) (claimed bool, existing []byte, err error)

// Commit stores fn's result for key with ttl and clears the in-flight lease,
// unblocking waiters. Must be called by the executor after fn returns.
func (n *Node) Commit(key string, result []byte, ttl time.Duration) error

// Release clears the lease for key without a result (executor failed cleanly);
// the next caller re-claims immediately instead of waiting for lease expiry.
func (n *Node) Release(key string) error
```

## Execution model (recommended implementation)

Back it with a **Raft-replicated table**, because Raft writes through the leader
are linearizable — that is the atomic compare-and-set needed for the claim.

1. **Storage.** A system table in a reserved DB (e.g. `__colmena_singleflight`),
   created at startup via the existing FSM/migrate path:

   ```sql
   CREATE TABLE IF NOT EXISTS singleflight (
     key         TEXT PRIMARY KEY,
     state       TEXT NOT NULL,        -- 'inflight' | 'done'
     executor    TEXT,                 -- NodeID that holds the claim
     lease_until INTEGER,              -- unix-nanos; claim/lease expiry
     result      BLOB,                 -- fn output once state='done'
     result_err  TEXT,                 -- non-empty if fn returned an error
     done_until  INTEGER               -- unix-nanos; result valid window (ttl)
   );
   ```

2. **Claim (CAS).** `TryClaim` issues a single conditional write **through the
   Raft log** (reuse `execute`/`Command`/`ExecMulti`). The condition, evaluated
   on the leader at apply time so it is atomic:

   - If a row exists with `state='done'` and `done_until > now` → return that
     result, claimed=false.
   - Else if a row exists with `state='inflight'` and `lease_until > now` →
     someone else is executing, claimed=false (caller becomes a waiter).
   - Else (no row / expired lease / expired result) → UPSERT
     `state='inflight', executor=<thisNodeID>, lease_until = now+leaseTTL`,
     return claimed=true.

   Because the comparison and the write are one Raft command applied on the
   leader, two nodes racing the same key cannot both win. **Do not** implement
   the read-then-write as two round-trips — it must be one applied command (an
   FSM-side conditional, or a SQL `INSERT ... ON CONFLICT ... WHERE`).

3. **Execute.** The winner runs `fn` locally. While running it must **renew the
   lease** (heartbeat: re-commit `lease_until = now + leaseTTL` every
   `leaseTTL/3`) so a long-but-healthy fn isn't preempted. Stop renewing when fn
   returns.

4. **Commit.** Winner writes `state='done', result=…, result_err=…,
   done_until = now+ttl` through Raft. This unblocks waiters.

5. **Wait.** Non-winners poll the row with `ConsistencyWeak` (leader-fresh) on a
   short backoff (e.g. 25ms → 200ms cap) until `state='done'` (return result) or
   the lease expires with no result (loop back to step 2 and try to claim).
   Respect the caller's `ctx` for cancellation/timeout. *(A push/notify channel
   could replace polling later; polling is fine for v1 and matches the existing
   `leaseLoop` style.)*

### Why TTL, not a hand-released lock

A held lock requires the holder to survive and explicitly release. A node crash
then wedges the key until an operator intervenes. A TTL'd claim self-heals:
worst case, the key is unavailable for `leaseTTL` after a crash, then the next
caller re-runs. Pick `leaseTTL` ≈ a few × `HeartbeatTimeout` (default 1s) and
clamp it to fn's realistic max runtime. Default suggestion: `leaseTTL =
max(5*HeartbeatTimeout, 10s)`.

## Correctness & edge cases the implementer MUST handle

- **Linearizable claim.** Claim CAS goes through Raft and is decided on the
  leader. Never decide a claim from a follower's local read.
- **Executor crash mid-flight.** Lease expires → next caller re-claims. This is
  the *only* path to >1 execution. Document that `fn` side effects must be
  idempotent OR that the caller tolerates rare double-execution. (mediavida: a
  second login is harmless — it just rotates cookies — so it's fine.)
- **Leadership change during a claim.** Raft handles it; the in-flight command
  either commits or doesn't. A claim that didn't commit means the node simply
  didn't win — it becomes a waiter. No special-casing in caller code.
- **fn returns an error.** Commit the error (`result_err`) with a *short*
  `done_until` (or `ttl=0` ⇒ don't cache the failure) so a transient failure
  doesn't get pinned for the full ttl. Recommended: cache successes for `ttl`,
  cache failures for `min(ttl, errTTL)` with a small `errTTL` (e.g. 1s) or not
  at all. Make this a parameter or a documented default.
- **ctx cancellation.** A waiter whose ctx is cancelled returns ctx.Err()
  without affecting the executor. An executor whose ctx is cancelled should
  pass it into fn; if fn respects it and returns, Commit the error.
- **Single-node cluster.** Must still work (claim always wins locally, no
  forwarding). Mirror the `ConsistencyStrong` single-node test.
- **Clock skew.** `lease_until`/`done_until` are evaluated on the leader at
  apply time using the leader's clock, so all comparisons use one clock. Do not
  compare a follower's local `now` against a timestamp written by the leader for
  the authoritative decision — push the comparison into the applied command.
- **Key namespace.** Document that keys are global strings; suggest callers
  namespace them (`"mv:relogin:<account>"`).

## How mediavida will use it

Replace the uncoordinated re-login (each node calling `scraper.Relogin()`
independently) with a coalesced one. In `withRelogin` / `restoreFromRecord`:

```go
// only one node logs in for this account per window; the rest reuse cookies
res, err := node.Do(ctx, "mv:relogin:"+account, 30*time.Second,
    func(ctx context.Context) ([]byte, error) {
        if err := scraper.Relogin(); err != nil {
            return nil, err
        }
        return scraper.MarshalCookies()   // serialized cookie jar
    })
if err != nil {
    return errReauthRequired
}
scraper.LoadCookies(res)   // every node ends up with the same fresh cookies
```

This removes the 3-IP login storm regardless of how requests are routed, and is
complementary to (not a replacement for) the app-side fix that stopped probing
upstream on every cross-node session rehydration.

## Tests to add (mirror existing `*_test.go` style)

- `TestDo_SingleNode_RunsOnce` — N concurrent `Do` on one node ⇒ fn runs once,
  all get the same result.
- `TestDo_MultiNode_Coalesce` — 3-node cluster, concurrent `Do` of one key from
  all 3 ⇒ fn runs on exactly one node (assert via a counter inside fn / a
  side-channel), all callers receive identical bytes.
- `TestDo_ResultCachedWithinTTL` — second `Do` within ttl does not re-run fn.
- `TestDo_ReExecutesAfterTTL` — after ttl, fn runs again.
- `TestDo_ExecutorCrash_LeaseExpiry` — kill the executor node mid-fn; a caller
  on a surviving node re-claims after lease expiry and completes.
- `TestDo_ErrorNotPinnedForFullTTL` — fn error is not cached for the full ttl.
- `TestDo_ContextCancel` — waiter respects ctx cancellation.
- Benchmark `Do` hit (cached) and miss (claim+run) paths, like
  `colmena_bench_test.go`.

## Open questions for the implementer to resolve

1. Should waiters poll (simple, v1) or get a notify channel from the FSM apply
   hook (lower latency)? Start with polling; leave a TODO.
2. Should `Do`'s result be `[]byte` (generic, requires caller marshal) or a
   generic `Do[T any]` wrapper? Go generics are available; a typed helper
   `DoJSON[T]` on top of the `[]byte` core would be ergonomic. Keep the core
   `[]byte`.
3. Reserved-DB naming/creation: confirm the migrate path can own a system DB
   without colliding with user `OpenDB` names. Prefix reserved names with
   `__colmena_`.
4. Garbage collection of stale `done` rows past `done_until`: piggyback a
   cleanup on claim, or a periodic sweep in `leaseLoop`? Cheapest is to delete
   on next claim of the same key; add a periodic sweep only if keys churn.
