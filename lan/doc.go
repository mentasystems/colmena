// Package lan provides zero-config clustering for Colmena over a local area
// network. It is a thin layer on top of colmena.Node that adds:
//
//   - A persistent, randomly-generated NodeID (created on first boot,
//     stored in DataDir/node_id) so the same image flashed onto many
//     identical machines still produces unique cluster members.
//
//   - mDNS-based peer discovery (no seed list, no static config). Nodes
//     announce themselves under "_colmena._tcp.local." and browse for
//     siblings; the cluster identity is derived from the SHA-256 of the
//     embedded CA cert, so two clusters with different CAs coexist on the
//     same LAN without seeing each other.
//
//   - mTLS by default, using a CA cert + key embedded in the binary
//     (typically via go:embed). Each node generates its own leaf cert
//     signed by the embedded CA on first boot. Possessing the CA key is
//     proof of cluster membership; one image == one cluster.
//
//   - Automatic bootstrap election: when several fresh nodes start at
//     the same time, the one with the lexicographically smallest NodeID
//     bootstraps the cluster and the rest wait to join. A node that finds
//     no peers after DiscoveryWindow bootstraps alone.
//
//   - Voter / non-voter policy: the first VoterQuorum nodes to join
//     become Raft voters (default 3); subsequent nodes join as non-voting
//     learners that replicate the log and serve local reads but do not
//     count toward quorum, so they don't penalize write latency.
//
//   - A leader-side sweeper that drops voters that have been unreachable
//     longer than DeadVoterTimeout, so a reflashed or replaced Pi gets
//     out of the way of its successor.
//
// # When to use this package
//
// This package optimizes for *read throughput* and *operational ease* on a
// trusted LAN — homelabs, edge boxes, on-prem clusters of small machines.
// It is the right choice when:
//
//   - The nodes share an L2 network where multicast (mDNS) actually works.
//   - You want to flash one identical image to N machines and have them
//     self-organize.
//   - Read throughput matters more than write latency, so you want many
//     replicas serving local reads behind a small voter quorum.
//
// # When NOT to use this package
//
// If your goal is fault tolerance across data centers or zones — what
// people usually mean by "high availability" — use the regular
// colmena.New() API directly with an explicit seed list, ideally 3 or 5
// nodes spread across failure domains. mDNS does not cross subnets, the
// embedded-CA model is wrong for shared infrastructure, and a large
// number of voters slows writes. The right tool for HA is a small,
// geographically distributed voter quorum, not a discovery-driven swarm.
package lan
