# colmena/fly — zero-config clustering on Fly.io

`fly` is the Fly.io counterpart of the [`lan`](../lan) package. It brings up a
Colmena node that **auto-forms and auto-joins a Raft cluster on Fly.io with no
hard-coded peer addresses**, surviving the ephemeral machines that every deploy
recreates (each gets a new private IP and machine id).

Deploy the same image to N machines in **one** region and they cluster
themselves.

## Why not the `lan` package?

`lan` discovers peers over **mDNS**, which needs multicast on a shared L2
segment. Fly's private network (6PN) is a **WireGuard mesh with no multicast**,
so mDNS finds nobody. `fly` keeps all of `lan`'s clustering logic (bootstrap
election, dead-voter sweep, non-voter promotion — shared via the
[`cluster`](../cluster) package) and only swaps the peer-discovery transport:

| | `lan` | `fly` |
|---|---|---|
| Peer discovery | mDNS browse | poll Fly internal DNS |
| Node id | random UUID (persisted) | `FLY_MACHINE_ID` |
| Advertise addr | first non-loopback IPv4 | `[FLY_PRIVATE_IP]:port` (IPv6) |
| Formed-cluster detect | mDNS TXT flags | `colmena.ProbeStatus` RPC |
| Dead-peer signal | absent from mDNS | absent from Fly DNS |

## Quickstart

```go
cfg, err := fly.FromEnv() // fills NodeID/PrivateIP/Region/AppName from the Fly env
if err != nil { log.Fatal(err) }
cfg.DataDir = "/data"     // a persistent Fly volume
cfg.VoterQuorum = 3

c, err := fly.Start(cfg)
if err != nil { log.Fatal(err) }

db := c.Node.OpenDB("default", colmena.ConsistencyNone)
// ... use db ...

// Health check for fly.toml, and graceful leave on SIGTERM:
http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
    if c.Healthy() { w.WriteHeader(200) } else { w.WriteHeader(503) }
})
```

A complete runnable program is in [`examples/fly-cluster`](../examples/fly-cluster).

## How it works

On boot the node resolves the current peer set from Fly internal DNS:

- `vms.<app>.internal` (TXT) → the machine ids in our region (stable Raft node ids),
- `<id>.vm.<app>.internal` (AAAA) → each machine's 6PN IP (the dial address).

It then decides:

1. **Probe** each visible peer with `colmena.ProbeStatus`. If any reports a
   leader, **join** it (as voter while the cluster has fewer than `VoterQuorum`
   voters, else as a non-voting learner).
2. Otherwise it's a cold start. A node **bootstraps** only once the
   `ExpectedVoters` gate is satisfied — it has seen at least `ExpectedVoters-1`
   peers, or `BootstrapTimeout` elapsed — **and** it wins the deterministic
   lowest-machine-id election. This guarantees exactly one bootstrapper and
   prevents a fresh low-id machine from starting a second cluster (split-brain).

A background loop on the leader **reaps** voters absent from Fly DNS for longer
than `DeadVoterTimeout` and **promotes** a non-voter to restore the quorum. On
`SIGTERM`, `GracefulLeave` transfers leadership and removes the node so a rolling
deploy doesn't wait for the sweep.

### Orphan self-heal

A node that was **reaped while it was offline** comes back holding stale Raft
state that still names it a member. The live cluster has moved on without it, so
its vote requests are rejected forever (`not in configuration`) and it never
forms or joins a quorum — Fly kills it, it restarts, and the loop repeats
indefinitely. The same background loop detects this: once the node's own
committed configuration no longer lists it **and** a healthy peer (one that sees
a leader) confirms the exclusion for `OrphanConfirmations` consecutive ticks
(default 3), the node **wipes its local state and restarts** to rejoin as a
brand-new member — the leader re-adds it through the normal join/promote path.

The detection is deliberately conservative so it never fires during a legitimate
full-cluster restart: in that case every node recovers a configuration that
*includes itself*, so no node is ever seen as removed. Self-heal triggers only
when the cluster is demonstrably healthy without this node.

- `OrphanConfirmations` — consecutive confirmations before healing (default 3).
- `OnSelfHeal` — override the default action (`os.Exit(1)` to let Fly restart the
  machine); the node has already been closed and its state wiped when it runs.
- `ForceCleanStart` (or env `COLMENA_FORCE_CLEAN_START=1`) — operator escape
  hatch: discard persisted state at startup and rejoin fresh, for a node wedged
  by stale state that the automatic path can't reach (e.g. it can't see a peer).

## Required `fly.toml`

Pin **one** region, deploy **rolling one machine at a time**, mount a persistent
volume at `DataDir`, and gate the deploy on the health check:

```toml
app = "my-colmena-app"
primary_region = "mad"

[build]

[env]
  COLMENA_DATA_DIR = "/data"
  # Optional: don't bootstrap until this many machines are visible on cold start.
  COLMENA_BOOTSTRAP_EXPECT = "3"

[[mounts]]
  source = "colmena_data"
  destination = "/data"

[deploy]
  strategy = "rolling"
  max_unavailable = 1   # never replace more than one voter at a time

[http_service]
  internal_port = 8080
  auto_stop_machines = false
  auto_start_machines = false
  min_machines_running = 3

  [[http_service.checks]]
    interval = "5s"
    timeout = "2s"
    grace_period = "30s"   # >= BootstrapTimeout, so a joining node has time to catch up
    method = "get"
    path = "/health"
```

Create the 3 machines in the single region, e.g.:

```sh
fly volumes create colmena_data --count 3 --region mad
fly scale count 3 --region mad
```

## SIGTERM / graceful leave

Fly sends `SIGTERM` before `SIGKILL` with a grace period. Wire `GracefulLeave`
into your shutdown path with a timeout **shorter** than that grace period:

```go
stop := make(chan os.Signal, 1)
signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)
<-stop
ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
defer cancel()
_ = c.GracefulLeave(ctx)
```

## Tuning

| Field | Default | Notes |
|---|---|---|
| `VoterQuorum` | 3 | Fault-tolerant core (tolerates 1 failure). |
| `ExpectedVoters` | `VoterQuorum` (or `COLMENA_BOOTSTRAP_EXPECT`) | Cold-start anti-split-brain gate. |
| `BootstrapTimeout` | 15s | Fallback before bootstrapping with a partial set. |
| `DiscoveryInterval` | 3s | Fly DNS poll cadence. |
| `PeerTTL` | 15s | ≈5 missed polls before a peer is dropped (absorbs deploy lag). |
| `DeadVoterTimeout` | 30s | Much shorter than `lan`'s 5m — Fly recreates machines routinely, so a vanished voter must free its slot fast. |
| `RaftPort` | 9000 | RPC sidecar listens on `RaftPort+1`. |

## mTLS

Off by default: the 6PN is already a private WireGuard mesh per org. To enable,
build a `*tls.Config` (e.g. with the helpers behind `lan`'s identity package, or
your own CA) and set `Config.TLSConfig`; the same config is reused for the
bootstrap `ProbeStatus` dials.

## Constraints

- **Single region only.** Raft is latency-sensitive; cross-region quorum is out
  of scope. All voters must share one region.
- **Persistent volume.** Mount `DataDir` on a Fly volume so a restarted machine
  recovers its Raft state instead of cold-starting.
