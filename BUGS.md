# Colmena — bug & improvement log

Findings from a code review on 2026-06-09. **Fix pass executed 2026-06-10
(v0.11.0):** items 1, 2, 3, 6, 7, 8, 9 (idle-deadline part), 10 and 11
(promote-gating part) are FIXED in the working tree; 5 is addressed by
correcting the documentation to match the real mechanism. Regression tests
live in `bugfixes_test.go`. Additional bugs found and fixed during the pass
are listed at the bottom ("Fix-pass addenda").

Each entry is tagged with a **status**:

- **VERIFIED** — confirmed by reading the code (file:line cited).
- **FIXED** — already corrected in the working tree.
- **FLAGGED** — surfaced by the review pass but NOT independently confirmed; verify before acting.

Severity is the practical blast radius, not theoretical.

> Context: this review was triggered while debugging a Mediavida login storm. The
> storm itself was an application bug (duplicate MV sessions), NOT a Colmena bug —
> but the dig surfaced the items below. The single-bug `BUG.md` (the `BeginTx`
> `RowsAffected` issue) is folded in here as item 4.

---

## 1. `Query` RPC is not leadership-gated → forwarded reads silently return stale data

**Status:** FIXED (2026-06-10) · **Severity:** HIGH

> **Fix:** `RPCQueryRequest` now carries `Consistency`; the `Query` handler is
> leadership-gated like `Execute` and re-runs `verifyLeader()` for Strong reads.
> Pre-v0.11 peers (no consistency field) are treated as Weak. Tests:
> `TestRPCQuery_NotLeaderGated`, `TestStrongRead_FromFollower`.

`node.go:519-525` (`Execute`) and `node.go:557-563` (`Forward`) both gate on
leadership:

```go
func (s *RPCService) Execute(...) error {
    if s.node.raft.State() != raft.Leader { resp.Error = "not the leader"; return nil }
    ...
}
```

But `Query` (`node.go:527-555`) does **not**:

```go
func (s *RPCService) Query(req *RPCQueryRequest, resp *RPCQueryResponse) error {
    dbName := req.DB
    if dbName == "" { dbName = "default" }
    st, err := s.node.stores.get(dbName)   // no leadership check
    ...
    rows, err := st.query(req.SQL, req.Args...)  // reads local SQLite
```

Also, `RPCQueryRequest` carries **no consistency level**, so the leader cannot
distinguish a forwarded Weak read from a forwarded Strong read.

**Mechanism / impact:**
- Every follower serves `ConsistencyWeak`/`ConsistencyStrong` by forwarding to the
  node it *thinks* is leader (`driver.go` `leaderQuery`/`forwardQuery`, target from
  `raft.LeaderWithID()`). If that view is stale (leader changed mid-request) the
  RPC lands on a deposed leader, which answers from its local SQLite anyway →
  **stale read with no error**. Writes mis-routed the same way fail loudly
  ("not the leader"); reads fail *silently*.
- **`ConsistencyStrong` is not linearizable from a follower.** The `verifyLeader()`
  quorum round-trip only runs in the *local* branch on the originating node
  (`driver.go`). When the read is forwarded, the leader never re-verifies quorum
  and doesn't even know Strong was requested (no consistency field on the wire).
  This breaks the documented guarantee in `consistency.go` ("impossible to get
  stale data, even during leadership transitions") for the common multi-node case.

**Fix:** add a consistency field to `RPCQueryRequest`; gate the `Query` handler on
`raft.State() == Leader` and, for Strong, call `verifyLeader()` before reading —
mirroring `Execute`.

---

## 2. WriteBatcher merges statements from different DBs into one command → writes hit the wrong DB

**Status:** FIXED (2026-06-10) · **Severity:** HIGH (live with default config + multi-DB usage)

> **Fix:** `flush()` groups pending entries by `cmd.DB` ("" normalized to
> "default") and emits one merged `CommandExecuteMulti` per database.
> Test: `TestBatcher_CrossDBIsolation`.

`batcher.go:92-104` (`flush`):

```go
var allStmts []Statement
offsets := make([]int, len(entries))
for i, e := range entries {
    offsets[i] = len(allStmts)
    allStmts = append(allStmts, e.cmd.Statements...)   // statements from ALL entries
}
merged := &Command{
    Type:       CommandExecuteMulti,
    DB:         entries[0].cmd.DB,                      // ...but tagged with entry[0]'s DB
    Statements: allStmts,
}
```

**Mechanism / impact:**
- The batcher is a **single per-Node instance** (`node.go:38`, `node.go:243-244`
  routes every DB's writes through `batcher.submit`).
- Batching is **on by default**: `config.go:184-185` sets `BatchWindow = 2ms` when
  unset.
- If two goroutines write to *different* databases (e.g. `OpenDB("a")` and
  `OpenDB("b")`, or an app DB + the `jobs` DB) within the same 2ms window, all
  their statements are merged into one `CommandExecuteMulti` routed to
  `entries[0].cmd.DB`. The other DB's statements execute against the wrong store.
- Symptom depends on schema overlap: usually a spurious `no such table` →
  the whole batch's single transaction rolls back → *both* callers get an error;
  with colliding table names, a **silent write into the wrong DB**.
- This is reachable in any multi-DB deployment (e.g. Mediavida runs an app DB plus
  the `jobs` DB concurrently on the leader).

**Fix:** never merge entries whose `cmd.DB` differs — segment `pending` (or the
flush) by DB and emit one merged command per DB.

---

## 3. `rpcPool.markFailed` never closes the connection and there is no reaper → fd/goroutine leak

**Status:** FIXED (2026-06-10) · **Severity:** MEDIUM

> **Fix:** `markFailed` now `Close()`s and evicts the entry immediately, and
> `get()` opportunistically sweeps any entry idle ≥ `maxIdle` (covers
> recycled-IP addresses that are never dialed again). The unused `failures`
> field was removed. Test: `TestRPCPool_MarkFailedEvicts`.

`rpc_pool.go` `markFailed`:

```go
func (p *rpcPool) markFailed(raftAddr string) {
    ...
    if entry, ok := p.clients[rpcAddr]; ok {
        entry.failures++          // increments only — never Close()s
    }
}
```

A failed entry's `*rpc.Client` is only closed on the **next `get()` for that exact
`rpcAddr`** (`rpc_pool.go` get: "Stale or failed — close and reconnect").

**Mechanism / impact:**
- On a forward error the leader address often changes (new election), so the failed
  entry for the *old* address is never `get()`-ed again → its `*rpc.Client`, reader
  goroutine and fd leak until `close()`.
- No background sweep: `get()` only evicts the specific address requested, and the
  `maxIdle` (5 min) check only fires on a subsequent `get()` of that same address —
  which never comes for a recycled IP. On Fly, every deploy recycles machine IPs, so
  dead entries accumulate on long-lived leaders.

**Fix:** in `markFailed`, `Close()` + `delete` the entry immediately; and/or add a
periodic sweep over all entries older than `maxIdle`.

---

## 4. `BeginTx` writes returned `RowsAffected() == 0` (UPDATE/DELETE) — FIXED, with a residual

**Status:** FIXED (commit `1a8ab39`) · residual: **FLAGGED for docs/API**

History: in v0.8.0 (`driver.go:62-69`, commit `2fc9558`), every statement issued
through `tx.ExecContext` inside `db.BeginTx` was buffered and returned a literal
`driver.RowsAffected(0)` synchronously (real SQL ran only at `Commit` via one
`CommandExecuteMulti`). So `RowsAffected()` was a constant `0` for *all* writes in a
Tx — INSERT included. INSERT "worked" only because callers check inserted-row
existence (a later SELECT), never the count; UPDATE/DELETE is where the count is
load-bearing (the `WHERE … claimed_at IS NULL` + check-rowcount race-claim idiom),
so that's where it surfaced. The data was always written correctly; only the count
was wrong. (This corrects both hypotheses in the original `BUG.md`: it was neither a
snapshot-visibility issue nor out-of-band apply — it was a hard-wired `0`.)

Fixed in the working tree: `driver.go` now returns a lazy `*txExecResult`
(`RowsAffected()`/`LastInsertId()` return `ErrTxResultPending` until Commit, then
are filled from each `applyResult.Results[i]`; Rollback fails them with
`ErrTxRolledBack`). Regression test: `colmena_test.go` `TestTransaction_RowsAffected`.

**Residual (worth addressing):** the row count is only readable **after `Commit`**.
The canonical guard-then-decide idiom —

```go
res, _ := tx.ExecContext(... UPDATE ... WHERE claimed_at IS NULL)
if n, _ := res.RowsAffected(); n == 0 { tx.Rollback(); return ErrAlreadyClaimed }
tx.Commit()
```

— still cannot work through `database/sql`, because statements don't execute until
Commit (`RowsAffected()` returns `ErrTxResultPending` mid-tx). The error is now
explicit instead of a silent `0` (the important win), but to restore the idiom you'd
need a bespoke `node.WriteTransaction(...)` returning per-statement counts as part of
the call, before deciding to commit (the "Better:" option in the old `BUG.md`).
Document the post-Commit-only semantics prominently regardless.

---

## 5. Lease reads can exceed the documented staleness bound and serve under silently-lost leadership

**Status:** ADDRESSED VIA DOCS (2026-06-10) · **Severity:** MEDIUM

> **Resolution:** the `ConsistencyLease` documentation in `consistency.go` now
> describes the real mechanism (locally-granted lease from last-contact
> polling), the actual worst-case staleness (~0.75 × HeartbeatTimeout) and the
> silent-leadership-loss window, instead of claiming a leader-piggybacked
> lease. A coordinated leader lease remains future work if tighter bounds are
> ever needed.

`node.go` `leaseLoop` (~`:670-697`) derives the local read lease from
`raft.Stats()["last_contact"]`, polling every `HeartbeatTimeout/4` and extending the
lease by `HeartbeatTimeout/2` when last contact `< HeartbeatTimeout/2` — rather than
the leader piggybacking a lease timestamp on heartbeats as the docs
(`consistency.go:46-52`) describe.

Concerns to verify:
- The follower grants its own lease unilaterally with no coordination with the
  leader's `LeaderLeaseTimeout`, so the "leader hasn't stepped down while a follower
  still serves under lease" safety property isn't enforced.
- Real staleness ≈ `HeartbeatTimeout/4` (poll age) + `HeartbeatTimeout/2` (grant) ≈
  `0.75 × HeartbeatTimeout`, larger than the advertised "≤ LeaseTimeout".
- `lease.valid()` is pure wall-clock `time.Now().Before(validUntil)` with no
  re-check against current Raft state at read time → a Lease read can be served
  locally and stale for the remaining window after leadership was already lost.

(Leader-local Lease reads are fine — the lease is extended unconditionally each tick.)

---

## 6. `GracefulLeave` can strand a small cluster by self-removing the last voter

**Status:** FIXED (2026-06-10) · **Severity:** MEDIUM

> **Fix:** `GracefulLeave` skips self-removal when this node is the only voter
> (or the configuration can't be read), so a single-machine Fly app survives
> rolling deploys with its Raft config intact. The multi-voter
> remove-then-Close-without-confirming residual is unchanged (peers fall back
> to the dead-voter sweep, which is the documented behavior).

`fly/fly.go` (~`:429-452`): if `TransferLeadership` fails (e.g. single-voter
cluster), the node still calls `RemoveNode(self)` then `Close()`, removing the only
voter from its own configuration → empty config → the cluster cannot elect a leader
again, and the data dir is effectively un-restartable as a cluster member without
re-bootstrap. Also, with multiple voters, `RemoveNode` is followed immediately by
`Close()` without confirming the removal committed to a quorum, so the "graceful"
optimization can silently no-op and peers still wait out `DeadVoterTimeout`.

Relevant to single-machine Fly apps doing rolling deploys.

---

## 7. Continuous backup `takeSnapshot` copies the main DB file in WAL mode without a consistent view

**Status:** FIXED (2026-06-10) · **Severity:** MEDIUM

> **Fix:** `takeSnapshot` now snapshots through the SQLite Online Backup API
> (`store.backupTo`) instead of a raw file copy, and the PASSIVE checkpoint
> moved BEFORE the snapshot. The ordering also closes a second (previously
> unlisted) hole: with checkpoint-after-snapshot, frames written between the
> WAL upload and the WAL reset vanished from the backend until the next
> generation — a restore in that window silently lost those writes.

`backup.go` (~`:146-194`): holds a read tx claiming to "prevent any checkpoint from
modifying the DB file while we copy it", then `os.Open`s and streams the raw main DB
file (`dbPath`). In WAL mode the main file is not the current state (recent committed
pages live in `-wal`), and a read tx on the reader connection does not stop the
writer connection from checkpointing the WAL into the main file mid-copy → the
streamed snapshot can be a torn mix of pages. The separate WAL backup helps only if
restore replays it, and a checkpoint *during* the copy still tears the main file.

**Fix:** route this through the SQLite Online Backup API already implemented in
`store.backupTo` instead of a raw file copy. (This is the continuous-backup path that
the animux user relies on.)

---

## 8. Write args are JSON-coerced to `float64` (lossy for large integers)

**Status:** FIXED (2026-06-10) · **Severity:** MEDIUM (upgraded: also corrupted `[]byte` args)

> **Fix:** command format **v2** — statement args are encoded as the same
> `TaggedValue`s used by query responses, so `int64` keeps full precision,
> `[]byte` stays a blob (v1 silently stored it as base64 TEXT — worse than the
> float64 issue), and `time.Time` survives. v1/legacy entries decode exactly
> as before (float64) for replay determinism. Pre-v0.11 nodes reject v2
> entries, so upgrade all nodes before resuming writes (noted in README).
> Tests: `TestWriteArgs_TypePreservation`, `TestCommandV2_RoundTrip`,
> `TestCommandV1_DecodeCompat`.

`command.go` marshals/unmarshals `Statement.Args []interface{}` with plain
`json.Marshal`/`Unmarshal`; JSON has no integer type, so every numeric arg becomes
`float64` before binding to SQLite (`store.go`). This happens identically on leader
and followers, so it does **not** diverge replicas — but it is lossy: `int64` args
above 2^53 lose precision, and integers bind as REAL not INTEGER, which can change
type-affinity/comparison behavior (e.g. `WHERE id = ?` against an INTEGER PK). The
type-preserving `TaggedValue` machinery is used only for query *responses*, never for
write *args*.

**Fix:** route write args through the same tagged encoding used for responses.

---

## 9. RPC server has no read deadline or payload size cap

**Status:** PARTIALLY FIXED (2026-06-10) · **Severity:** MEDIUM (mTLS limits exposure)

> **Fix (deadline + goroutine tracking):** accepted RPC connections get a
> rolling 15-minute read deadline (`rpcIdleConn`), so open-and-never-send or
> stall-mid-message peers can't pin a `ServeConn` goroutine/fd forever; the
> timeout deliberately exceeds the client pool's 5-minute `maxIdle` so pooled
> clients never reuse a server-killed conn. Conns are tracked and force-closed
> on shutdown (see #10).
> **Not fixed:** per-message payload size cap — net/rpc's gob codec gives no
> message-boundary hook short of a custom codec; with mandatory mTLS between
> cluster members the residual exposure is a misbehaving *member*, accepted
> for now.

`node.go` `startRPC` (~`:633-657`) does `go rpcServer.ServeConn(conn)` with no
deadline and no bound on gob-decoded payloads (`RPCExecuteRequest.Command`,
`RPCForwardRequest.Payload`, `RPCQueryRequest.Args`). A peer can send a huge value
→ unbounded allocation (OOM/DoS), or open a conn and never send → a `ServeConn`
goroutine blocks forever (leak under repetition). Per-conn goroutines and the accept
loop are untracked; `Close()` relies on the listener close alone.

**Fix:** set read deadlines, cap payload sizes, and track/drain RPC goroutines on
shutdown.

---

## 10. Shutdown ordering: in-flight RPC handlers race store/raft teardown

**Status:** FIXED (2026-06-10) · **Severity:** MEDIUM

> **Fix:** every state-touching RPC handler (`Execute`, `Query`, `Forward`,
> `Join`, `Status`) enters through `RPCService.begin()`, which refuses new
> work once `closed` is set and registers in-flight handlers in `rpcWG`.
> `Close()` now: closes the listener → force-closes tracked conns → waits
> `rpcWG` → only then tears down the pool, raft and stores.

`Close()` (`node.go:207-234`) closes the RPC listener (stops new accepts) but does
not wait for in-flight `ServeConn` goroutines before `stores.close()` and
`raft.Shutdown()`. A handler already past the listener can call `stores.get(...)` →
`st.query(...)` on a `*sql.DB` being closed concurrently, or `Execute` →
`applyRaft` on a raft instance shutting down. No `closed` check in any `RPCService`
method and no WaitGroup draining RPC goroutines. Mostly returns errors rather than
panicking, but it's a latent closed-resource hazard.

**Fix:** add a `closed` guard to RPC handlers and a WaitGroup to drain in-flight RPCs
before tearing down stores/raft.

---

## 11. Fly rolling-deploy: promote/sweep races and live-peer removal on recycled IPs

**Status:** PARTIALLY FIXED (2026-06-10) · **Severity:** MEDIUM (availability/latency, not data loss)

> **Fix (promote gating):** `RPCStatusResponse` gained a `CaughtUp` flag
> (fresh leader contact + applied ≥ commit, always true on leaders);
> `promoteIfNeeded` probes candidates in NodeID order and prefers a caught-up
> one, falling back to the first merely-reachable candidate so a mixed-version
> or degraded cluster still restores quorum.
> **Not changed (assessed as acceptable):** the Join same-address removal — a
> Fly machine keeps its 6PN IP for its lifetime, so an address collision means
> the old machine is gone and removing its stale entry is correct. The
> sweep/promote non-atomic `Nodes()` reads remain (worst case: one wasted
> tick).

- `fly/fly.go` `promoteIfNeeded` promotes the smallest-NodeID non-voter when
  `voters < VoterQuorum` without a caught-up/health gate → can promote a brand-new or
  not-yet-synced machine into the voter set, stalling commits until it catches up.
- `node.go:582-592` `Join`: when an existing server has the same Address but a
  different ID, the handler `RemoveServer`s it. On Fly, a recycled private IP on a new
  machine can cause a join to remove the *old* voter even if it's still alive →
  transient quorum reduction.
- `sweepDeadVoters` and `promoteIfNeeded` read `Nodes()` non-atomically on the same
  tick, so the config can be in flux between them.

These are availability/latency hazards during the exact prod rolling-deploy path
(Raft still protects against split-brain).

---

## Areas reviewed and found solid

`handler.go` (handlerRegistry RWMutex usage), `metrics.go` (atomic counters),
`consistency.go` `readLease` mutex discipline, `WriteBatcher` result demux + buffered
result channels + all-or-nothing partial-batch handling, `storeManager.get`
double-checked locking, `command.go` envelope versioning / malformed-input safety
(no panic; unknown version rejected), FSM determinism guards (`validate.go` rejects
`random()`/`current_timestamp`/etc. before Raft), apply-results returned only to the
proposing leader, and the deterministic lexicographic-smallest-advertise election in
`cluster`.

One inherent design surface (not a coding bug): a per-node `Apply` error (disk-full,
a constraint one replica hits) silently diverges that replica — Raft considers the
index applied but the SQLite state differs, with no reconciliation. Inherent to
SQL-over-Raft; flagged for awareness.

---

## Fix-pass addenda (2026-06-10) — additional bugs found & fixed

**A. `Node.LeaderAddr()` returned the leader's node ID, not its address.**
`node.go` destructured `raft.LeaderWithID()` into the wrong half
(`_, id := ...; return string(id)`). The existing test only asserted
non-emptiness, so it passed. Fixed; `LeaderID()` added for callers that want
the ID. Test: `TestNode_LeaderAddrReturnsAddress`.

**B. Forwarded queries with `time.Time` args failed gob encoding.**
`RPCQueryRequest.Args` travels as `[]interface{}` over net/rpc; gob
auto-registers basic types but not `time.Time`, so any follower read with a
time argument errored. Fixed with `gob.Register(time.Time{})` in package
init. Test: `TestForwardQuery_TimeArg`.

**C. `lan/discovery.go` `collect()` read `d.nodeID` without the announce
lock.** Not a Go-memory-model race today (the field is written before the
browse goroutine starts and never re-written), but one refactor away from
one; now snapshotted under `announceMu` at collect start.

**D. Subpackage review (jobs, lan, cluster, fly/discovery): clean.** Notably,
`jobs` stores payloads as strings, so it was never exposed to the v1 []byte
arg corruption (#8), and its claim UPDATE runs as a direct single-statement
Exec, so it is not affected by the buffered-tx RowsAffected residual (#4).

## Suggested fix order — RESOLVED

All three priority items (#2, #1, #3) plus #6, #7, #8, #10 and the
consequential halves of #9/#11 were fixed on 2026-06-10 (v0.11.0,
command-format v2). Remaining open threads, all assessed low-priority:

- #4 residual: a bespoke `WriteTransaction` API returning per-statement
  counts before commit (the claim idiom through `database/sql` stays
  impossible by design; now documented in README Limitations).
- #5: a leader-coordinated lease if tighter staleness bounds are ever needed
  (docs now describe the real guarantee).
- #9 residual: RPC payload size cap (needs a custom net/rpc codec).
- #11 residual: atomic sweep/promote snapshot; Join address-collision
  removal confirmed correct on Fly (machine IPs are stable for a machine's
  lifetime).
