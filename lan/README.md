# colmena/lan

Zero-config Colmena clustering on a local network. Flash one binary onto every
machine and they form a cluster automatically — mDNS for discovery, embedded
CA for identity, automatic voter / non-voter policy.

```go
import (
    _ "embed"

    "github.com/mentasystems/colmena/lan"
)

//go:embed ca.crt
var caCert []byte

//go:embed ca.key
var caKey []byte

func main() {
    cluster, err := lan.Start(lan.Config{
        DataDir: "/var/lib/colmena",
        Bind:    "0.0.0.0:9000",
        CACert:  caCert,
        CAKey:   caKey,
    })
    if err != nil { log.Fatal(err) }
    defer cluster.Close()

    db := cluster.Node.DB() // standard *sql.DB
    db.Exec("CREATE TABLE ...")
}
```

## What it does

`lan.Start` wraps `colmena.New` with everything needed to bring up a node
without per-machine configuration:

- **Persistent NodeID.** A random UUID is generated on first boot and stored
  in `<DataDir>/node_id`. Reflashing a Pi yields a fresh ID, so Raft sees it
  as a new member instead of trying to recover an old one with an empty log.
- **Per-node TLS cert.** On first boot, the node generates an ECDSA P-256
  key pair and a leaf certificate signed by the embedded CA. SANs include
  every non-loopback IP on the host plus `127.0.0.1` and `::1`.
- **mDNS discovery.** The node announces itself under
  `_colmena-<clusterID>._tcp.local.`, where `<clusterID>` is the first
  8 bytes of `SHA-256(CACert)` hex-encoded. Two clusters with different CAs
  use different service names and never see each other on the same LAN.
- **Bootstrap election.** When several fresh nodes start at the same time,
  they all advertise `bootstrapping=true`. After `DiscoveryWindow`, the node
  with the lexicographically smallest NodeID bootstraps the cluster and the
  rest wait to join.
- **Voter / non-voter policy.** The first `VoterQuorum` nodes (default 3)
  become Raft voters. Subsequent nodes join as non-voting learners — they
  replicate the log and serve local reads at full SQLite speed (~6µs) but
  do not count toward quorum, so they don't add latency to writes.
- **Dead-peer sweeper + auto-promotion.** The leader removes peers that
  have been unreachable longer than `DeadVoterTimeout` (default 5 min)
  and immediately promotes the smallest-NodeID non-voter to fill any
  vacated voter slot. So if a voter dies, the cluster restores its
  target `VoterQuorum` without manual intervention — non-voters are a
  hot pool of failover candidates, not just read replicas.
- **Stale-address replacement.** If a peer is reflashed (new random
  NodeID) but keeps its DHCP IP, the leader's Join RPC removes the
  stale entry at that address before adding the new one, so the same
  hardware can rejoin without waiting for the sweeper.

## Cluster identity = embedded CA

Cluster membership is "having the right CA cert + key embedded in the
binary". One image == one cluster. The hash of the CA cert determines the
mDNS service name, so:

- Same CA on N images → they form one cluster on the LAN.
- Different CA on different images → multiple isolated clusters, even on
  the same network.

This is the right model for homelab / appliance scenarios where you flash
identical images and want the device to "just work" when plugged in. It is
**not** the right model for shared infrastructure: anyone who can read the
binary can mint certs for the cluster. Treat the image as a credential.

For a real deployment, generate a single CA per cluster offline and embed it:

```go
//go:embed ca.crt
var caCert []byte

//go:embed ca.key
var caKey []byte
```

For development or testing, `lan.Config{}` with empty `CACert`/`CAKey`
runs the cluster in plaintext. Don't ship that.

## Configuration

```go
lan.Config{
    DataDir          string         // Required. Colmena state + persisted node_id and TLS material.
    Bind             string         // Required. "host:port" for Raft + RPC sidecar (RPC uses port+1).
    Advertise        string         // Optional. Auto-detects first non-loopback IPv4 if empty.

    CACert           []byte         // PEM-encoded CA cert (typically go:embed).
    CAKey            []byte         // PEM-encoded CA private key (typically go:embed).

    VoterQuorum      int            // Target voter count. Default: 3.
    DiscoveryWindow  time.Duration  // mDNS listen window before deciding bootstrap/join. Default: 8s.
    DeadVoterTimeout time.Duration  // Leader removes unreachable peers after this and auto-promotes a non-voter to fill the slot. Default: 5m. 0 disables both.

    Consistency      colmena.ConsistencyLevel // Default: ConsistencyNone (local reads — the typical reason to use this package).
    BatchWindow      time.Duration             // Passed through to colmena.Config.
    BatchMaxSize     int                       // Passed through to colmena.Config.
    OnApply          func(...)                 // Passed through to colmena.Config.
    Backup           *colmena.BackupConfig     // Passed through to colmena.Config.
    LogOutput        io.Writer                 // Default: os.Stderr.

    ServiceName      string         // Test-only override of mDNS service type.
}
```

## Lifecycle

```go
cluster, err := lan.Start(cfg)        // blocks for ~DiscoveryWindow + a few seconds
defer cluster.Close()                  // stops mDNS, sweeper, and the underlying node

cluster.Node.WaitForLeader(15 * time.Second)
db := cluster.Node.DB()
```

`cluster.Node` is the standard `*colmena.Node`. Everything that works with
`colmena.New` (custom handlers, jobs, OnApply hooks, multiple databases)
works the same way here.

## Read scale, not HA

This package optimizes for **read throughput** on a trusted LAN. It is the
right choice when:

- The nodes share an L2 network where multicast (mDNS) actually works.
- You want to flash one identical image to N machines and have them
  self-organize.
- Read throughput matters more than write latency, so you want many
  replicas serving local reads behind a small voter quorum.

It is the **wrong** choice for high availability across data centers or
zones. mDNS doesn't cross subnets, the embedded-CA model is wrong for
shared infrastructure, and a large number of voters slows writes. For HA,
use `colmena.New` directly with an explicit seed list of 3 (or 5) voters
spread across failure domains.

| Goal | Use |
|---|---|
| **Read scaling on a LAN** (homelab, edge, appliance) | `colmena/lan` with `VoterQuorum=3` and many learners |
| **High availability across zones / regions** | `colmena.New` directly with 3–5 voters, no LAN discovery |

## Edge cases & known limits

- **Network partition during cold start.** If the LAN splits in half while
  every node is fresh, each half can elect its own bootstrapper → split
  brain. Mitigations: longer `DiscoveryWindow`, or pre-bootstrap a single
  node by running it first in isolation. Once any cluster has formed,
  partitions during runtime behave like normal Raft (the side without
  quorum stalls writes).
- **Bootstrap election ties** are broken by lexicographic NodeID. UUID v4
  collisions are not a real concern.
- **Reflash recovery** has two paths:
  - *Same DHCP IP, new NodeID*: the leader's Join RPC removes the
    stale entry at that address as part of accepting the new node, so
    rejoin is immediate.
  - *Different IP, same role needed*: the new instance joins as a
    non-voter; once `DeadVoterTimeout` passes for the old NodeID, the
    sweeper removes the dead voter and the next promotion tick lifts
    a non-voter into the open slot. Default failover budget: 5 minutes.
- **Failover budget.** While more than `floor(VoterQuorum/2)` voters are
  unreachable, writes stall until the sweeper removes enough dead voters
  for the survivors to form a majority again, plus one promotion tick to
  refill the slot. With defaults that's ~5 minutes after a single voter
  dies in a 3-voter cluster (other 2 still hold quorum, no stall) but
  ~5 minutes of write pause if you lose 2 of 3 voters at once. Tune
  `DeadVoterTimeout` to your tolerance for jitter vs. recovery speed.
- **mDNS is best-effort.** Some home routers / wifi APs filter multicast.
  If discovery doesn't work, fall back to `colmena.New` with an explicit
  seed list — the LAN package does not magically fix broken multicast.

## Testing

```bash
go test ./lan/ -v
```

Unit tests cover NodeID persistence, CA hashing, leaf cert issuance, and
the bootstrap-election decision matrix. The mDNS layer itself is exercised
by the example in `examples/lan-cluster`, which can be run on multiple
terminals to spin up a real cluster on `127.0.0.1`.
