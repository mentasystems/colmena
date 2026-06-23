# BUG: cold-start quorum deadlock — a full-cluster restart never re-elects a leader

Filed against the `fly` clustering package (`github.com/mentasystems/colmena/fly`).
Reproduced in production on the `mediavida-api` app (3-node Fly cluster,
`VoterQuorum = 3`) on 2026-06-22 → 2026-06-23: a routine Fly host restart that
bounced all 3 machines at once took the cluster permanently down (~16.5h) until a
manual, data-preserving wipe of the Raft state. The app's HTTP service never
became healthy again on its own.

## Symptom

When **all** voters of a formed cluster go down at roughly the same time (Fly host
maintenance, a platform-wide restart, a simultaneous OOM, etc.) and then come back,
the cluster **cannot re-elect a leader** and never recovers:

- One node ends up stuck as a perpetual Raft candidate (term climbs without bound,
  e.g. `term=868`), logging `Election timeout reached, restarting election` and
  `failed to make requestVote RPC: ... connect: connection refused` against the
  other voters forever.
- The other voters crash-loop: each boots, fails to find/elect a leader within the
  bootstrap window, `log.Fatal`s out of the consumer's `Start` error path, reboots,
  hits Fly's **"machine has reached its max restart count of 10"**, and is parked
  `stopped`.
- `Cluster.Healthy()` stays false on every node → the consumer's `/health` returns
  503 → Fly's proxy never routes → the app is down. Manually `fly machine start`-ing
  the stopped nodes does not help: they re-enter the same crash-loop.

## Root cause

The Raft transport listener (`:RaftPort`) is only bound **inside `colmena.New`**,
which `fly.Start` calls **after** `decideStart` has resolved the bootstrap-vs-join
decision (`fly/fly.go`). So during the entire `decideStart` window a recovering node
is **not listening on `:9000`** and cannot answer `RequestVote`. The deadlock is
the interaction of three facts:

1. **The bootstrap winner can't actually bootstrap.** `decideStart` picks a
   deterministic winner (smallest advertise address, `winsElection`) and returns
   `startPlan{bootstrap: true}`. But `colmena.New` with `cfg.Bootstrap = true` only
   force-installs a **single-server** configuration via `raft.BootstrapCluster`
   (`node.go:144-156`). On a node that already has persisted 3-server Raft state,
   `BootstrapCluster` returns `raft.ErrCantBootstrap`, which is **intentionally
   ignored** — so the node keeps its existing **3-voter** configuration and now needs
   2 of 3 votes to elect a leader. It cannot get them (see #2), so it spins as a
   candidate forever.

2. **The election losers wait passively and never bind their listener.** Losers
   return to the `decideStart` loop and `probeForLeader` until `joinDeadline`
   (`BootstrapTimeout + 60s`). They only leave the loop — and only then reach
   `colmena.New`, binding `:9000` — if they (a) find an existing leader to join, or
   (b) themselves win the election. In a full cold restart neither happens: there is
   no leader (the winner from #1 can't elect alone), and they already lost the
   election. So they never bind `:9000`, never vote, and at `joinDeadline` return the
   error `"no leader appeared and did not win bootstrap election within ..."`.

3. **The consumer treats that error as fatal**, so the process exits, Fly restarts
   it, and after 10 restarts the machine is parked `stopped` — removing it from the
   set permanently and guaranteeing the survivor can never reach quorum.

Net: **a recovered multi-server configuration can only elect a leader if a quorum of
its members have their Raft listeners up simultaneously, but `decideStart` prevents a
loser from binding its listener until a leader already exists.** Circular →
permanent deadlock. This is specific to *losing the whole quorum at once*; a rolling
deploy (one node at a time, the designed path) is unaffected because a leader always
remains up to be joined.

## Reproduction

1. Form a 3-node cluster on Fly with `VoterQuorum = 3`, let it commit some writes so
   every volume has a 3-server Raft configuration persisted in `raft.db`.
2. Stop/restart **all three** machines within a short window (simulate a host
   restart): `fly machine restart <id>` for all three, or `fly machine stop` all
   then `start` all.
3. Observe: the machines never converge. Logs show one node as an endless candidate
   (`requestVote ... connection refused`, monotonically rising term) and the others
   `log.Fatal`-ing with `"no leader appeared and did not win bootstrap election"`,
   then `"max restart count of 10"` → `stopped`.

A focused unit/integration test should drive `decideStart` (and the join path) with
a simulated discovery that returns all peers but no reachable leader, while each
peer also has pre-existing multi-server Raft state, and assert the cluster reaches a
single leader without external intervention.

## Expected behavior

A full-cluster cold restart must **self-heal**: once a quorum of the persisted
voters are running, the cluster must elect a leader and report `Healthy()` without
operator intervention and without data loss.

## Suggested direction (validate before implementing)

The core gap is that nodes recovering existing state don't bring up Raft and
participate in an election; they gate on `decideStart` finding a leader first. Some
viable shapes (pick/combine after analysis):

- **Recover-then-elect instead of wait.** When a node has pre-existing Raft state
  for a cluster it's a member of, skip the passive "wait for a leader" path and go
  straight to `colmena.New` with its existing configuration (binding `:9000` and
  participating in the normal Raft election) rather than depending on `probeForLeader`
  to discover a leader that can't exist yet. Real Raft members re-electing among
  themselves is exactly the supported path — the `fly` bootstrap gate is only needed
  for the *first ever* cold start, not for recovery.
- **Single-server force-recovery as a last resort.** If, after a bounded timeout, the
  deterministic election winner still can't reach quorum, have *only that one node*
  (winner is agreed by all via `winsElection`, so this is split-brain-safe) call
  `raft.RecoverCluster` to rewrite its configuration to single-server, become leader,
  then re-add the other voters as they reappear (`promoteIfNeeded`/`AddVoter` already
  exist). Preserves the FSM/SQLite data (only `raft.db` config is rewritten).
- **Don't make bootstrap failure fatal in the consumer.** Independently, the
  `mediavida` consumer's `startColmenaCluster` should retry `fly.Start` with backoff
  instead of `log.Fatal` (which trips Fly's max-restart-count and parks the machine).
  But that alone is insufficient — the library must also stop deadlocking.

## Constraints / watch-outs

- **No split-brain.** Any force-recovery or self-bootstrap must be performed by at
  most one node for a given member set. `winsElection` (Advertise-keyed, agreed by
  all nodes in both healthy and degraded discovery modes) is the existing
  single-winner primitive — reuse it; don't invent a second election that could
  disagree.
- **No data loss.** The FSM SQLite files (`<name>.db`, e.g. `mv.db`, `default.db`)
  live on the volume and are only overwritten when an FSM snapshot is installed
  (`fsm.go` `Restore`). A fix must not drop them. `RecoverCluster` rewrites only the
  Raft log/config, which is the desired scope.
- **Don't regress the rolling-deploy path.** The current `decideStart` gate exists to
  prevent split-brain on the *initial* cold start and to keep rolling deploys
  quorum-safe (`min_machines_running`, `max_unavailable = 1`). New recovery behavior
  must only trigger on true full-quorum-loss, not during a normal one-at-a-time
  deploy where a leader is still reachable.

## Acceptance criteria

- Restarting all N voters of a formed cluster simultaneously results in a single
  elected leader and `Healthy() == true` on all nodes, with no manual steps.
- No split-brain under simultaneous restart, partial restarts, or flapping nodes.
- Pre-existing FSM data survives the recovery.
- The rolling-deploy / single-cold-start behaviors are unchanged (existing tests in
  `fly/` still pass; add a regression test for the full-restart case).

## Resolution (implemented)

Fixed via **direction #1 (recover-then-elect)** — the clean, split-brain-safe
shape. Changes:

- **`colmena.HasExistingState(dataDir)`** (`node.go`): opens (and immediately
  closes) the persisted Raft stores and reports whether the data dir already
  holds cluster state — a non-empty log, a recorded term, or a snapshot. It
  distinguishes a true first-ever cold start from a returning member.
- **`colmena.Config.Recover`** (`config.go`): a new start mode meaning "bring
  Raft up on the persisted on-disk configuration — no `Bootstrap`, no `Join` —
  and participate in the normal election." `validate()` accepts it and rejects
  combining it with `Bootstrap`/`Join`. In `New` it simply skips both the
  single-server `BootstrapCluster` and the join RPC, so Raft loads its persisted
  multi-server configuration, binds `:RaftPort`, and elects normally.
- **`fly.Start`** (`fly/fly.go`): before the cold-start gate, it calls
  `HasExistingState`. If state exists, it **bypasses `decideStart` entirely** and
  starts the node with `Recover = true`. The gate (and its `decideStart` →
  `log.Fatal` error path) now only runs on a true first cold start (no state),
  where it is still needed to avoid split-brain while force-installing a
  single-server configuration. This breaks the circular deadlock: a recovering
  loser no longer waits for a leader before binding its listener — it binds
  immediately and votes, so a quorum of returning members re-elects on its own.
  Safety comes from Raft's own quorum rule (a majority of the persisted voters
  must agree), so no second election primitive is introduced.
- **Resource-leak fix** (`node.go`): `Node.Close` never closed the BoltDB log
  store (`raft.Shutdown` does not close externally-provided stores), leaking the
  file lock until process exit. `Close` now closes it after Raft shuts down — a
  latent bug on its own, and required for the documented "call `HasExistingState`
  after `Close`" contract to actually work (otherwise the reopen blocks on the
  stale lock).

Regression tests: `fly.TestFullRestartReElects` forms a 3-voter cluster, closes
all nodes (state preserved), restarts every node with no bootstrap/join, and
asserts exactly one leader re-emerges with all three back as voters — the
full-quorum-loss scenario this bug describes. `fly.TestHasExistingState` and the
extended `TestConfig_Validate` cases cover the new primitives.

**Out of scope (separate repo):** direction #3 — making `startColmenaCluster`'s
`fly.Start` failure retry with backoff instead of `log.Fatal` — lives in the
`mediavida` consumer. With this library fix the consumer no longer reaches the
fatal path on a full restart, but the retry hardening is still worth doing there
independently.

## Workaround used in the 2026-06-23 incident (operational, not a fix)

Data-preserving manual recovery: on every node `rm -f /data/raft.db /data/raft.db-*`
and `rm -rf /data/snapshots` (keeping `mv.db`/`default.db`), then `fly machine
restart` all nodes together. With the Raft log gone, the election winner's
`BootstrapCluster` runs on an empty state (no `ErrCantBootstrap`), forms a fresh
single-server cluster, becomes leader, and the others join as voters. The SQLite
data survives because no snapshot exists to `Restore` over it. This is a manual
break-glass, not the fix this bug asks for.
