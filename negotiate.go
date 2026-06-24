package colmena

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
)

// minWriteableFormatVersion is the oldest Command/Snapshot envelope version
// that every Colmena release still in existence can decode. The negotiated
// effective write version never drops below it, and it is the conservative
// value used while a voter's supported version is still unknown — writing the
// floor is always safe because every node can read it.
const minWriteableFormatVersion = 1

// versionNegotiator records, per peer node, the maximum Command/Snapshot
// envelope version that peer advertises it can read. A node's advertised
// CommandFormatVersion is exactly its max readable version: a node that writes
// vN can decode every version up to and including N (see unmarshalCommand's
// version switch). The leader feeds this into effectiveCommandVersion() to pick
// a write version no current voter will choke on.
//
// This is the guardrail for the rolling-deploy footgun: bumping
// CommandFormatVersion and deploying one node at a time used to let an upgraded
// leader replicate vN entries that still-vN-1 voters could not decode, wedging
// them in a tight "fsm apply ... unsupported format version" loop while they
// silently failed to apply committed entries. With negotiation the leader holds
// at the old format until *every* voter reports it can read the new one, so a
// one-at-a-time rolling deploy across a format bump is safe by construction.
type versionNegotiator struct {
	mu    sync.RWMutex
	peers map[string]peerFormat // keyed by NodeID

	// effectiveCommand / effectiveSnapshot cache the last computed
	// min-across-voters write versions so the write hot path reads an atomic
	// instead of walking the Raft configuration on every command.
	effectiveCommand  atomic.Int64
	effectiveSnapshot atomic.Int64
}

// peerFormat is the set of envelope versions a peer advertised in its Hello.
type peerFormat struct {
	command  int
	snapshot int
}

func newVersionNegotiator() *versionNegotiator {
	v := &versionNegotiator{peers: make(map[string]peerFormat)}
	// Start at the floor: until we have confirmed every voter's version we
	// must assume the cluster may include a node that only reads the oldest
	// format.
	v.effectiveCommand.Store(minWriteableFormatVersion)
	v.effectiveSnapshot.Store(minWriteableFormatVersion)
	return v
}

// record stores a peer's advertised envelope versions. Called from both sides
// of the Hello handshake (inbound on the responder, outbound from the probe
// loop) so the leader's view converges from whichever direction connects first.
func (v *versionNegotiator) record(nodeID string, command, snapshot int) {
	if nodeID == "" {
		return
	}
	v.mu.Lock()
	v.peers[nodeID] = peerFormat{command: command, snapshot: snapshot}
	v.mu.Unlock()
}

// peerCommand returns a peer's advertised command version and whether it is
// known. Used by the forward path to avoid handing an old leader a format it
// would blindly replicate to nodes that cannot read it.
func (v *versionNegotiator) peerCommand(nodeID string) (int, bool) {
	v.mu.RLock()
	pf, ok := v.peers[nodeID]
	v.mu.RUnlock()
	if !ok || pf.command <= 0 {
		return 0, false
	}
	return pf.command, true
}

// effectiveCommandVersion returns the command envelope version the leader may
// safely write right now: min(local max, min over all current voters). If any
// voter's version is not yet known it falls back to the floor, because writing
// a newer format to a node that cannot read it is the exact bug this prevents.
func (n *Node) effectiveCommandVersion() int {
	return int(n.versions.effectiveCommand.Load())
}

// effectiveSnapshotVersion mirrors effectiveCommandVersion for the snapshot
// envelope. SnapshotFormatVersion is still 1, so today this always returns 1;
// the machinery is in place so the next snapshot-format bump is safe too.
func (n *Node) effectiveSnapshotVersion() int {
	return int(n.versions.effectiveSnapshot.Load())
}

// forwardWriteVersion is the command version a follower uses when marshaling a
// write to forward to the leader. A new leader re-marshals forwarded commands
// at its own effective version, but an *old* leader applies the forwarded bytes
// verbatim, so the follower must not hand an old leader a format it would
// replicate to voters that cannot read it. Cap at the leader's advertised
// version when known; fall back to the floor when it is not.
func (n *Node) forwardWriteVersion() int {
	_, leaderID := n.raft.LeaderWithID()
	if v, ok := n.versions.peerCommand(string(leaderID)); ok {
		if v < CommandFormatVersion {
			return v
		}
		return CommandFormatVersion
	}
	return minWriteableFormatVersion
}

// recomputeEffectiveVersions recalculates the effective write versions from the
// current voter set and the recorded peer versions, and stores them for the
// write hot path. Safe to call from any node; only the leader's value is acted
// on, but keeping followers current means a freshly elected leader writes the
// right version immediately.
func (n *Node) recomputeEffectiveVersions() {
	cmd := n.computeEffective(func(pf peerFormat) int { return pf.command }, CommandFormatVersion)
	snap := n.computeEffective(func(pf peerFormat) int { return pf.snapshot }, SnapshotFormatVersion)

	prev := int(n.versions.effectiveCommand.Swap(int64(cmd)))
	n.versions.effectiveSnapshot.Store(int64(snap))
	if prev != cmd && n.raft.State() == raft.Leader {
		log.Printf("colmena: effective command write version %d -> %d (voters min, local max %d)",
			prev, cmd, CommandFormatVersion)
	}
}

// computeEffective walks the current voters and returns min(localMax, min over
// voters of their advertised version). Self is assumed to support localMax. Any
// voter whose version is unknown (not yet probed, or pre-Hello peer) forces the
// conservative floor.
func (n *Node) computeEffective(pick func(peerFormat) int, localMax int) int {
	cf := n.raft.GetConfiguration()
	if cf.Error() != nil {
		// Can't enumerate voters — don't risk writing a format someone can't read.
		return minWriteableFormatVersion
	}
	eff := localMax
	n.versions.mu.RLock()
	defer n.versions.mu.RUnlock()
	for _, srv := range cf.Configuration().Servers {
		if srv.Suffrage != raft.Voter {
			continue // non-voters (learners) never block a format bump
		}
		if string(srv.ID) == n.config.NodeID {
			continue // self supports localMax by construction
		}
		pf, ok := n.versions.peers[string(srv.ID)]
		pv := pick(pf)
		if !ok || pv <= 0 {
			return minWriteableFormatVersion // unknown voter — stay safe
		}
		if pv < eff {
			eff = pv
		}
	}
	if eff < minWriteableFormatVersion {
		eff = minWriteableFormatVersion
	}
	return eff
}

// versionLoop periodically probes every voter's advertised format versions and
// recomputes the effective write version. Runs for the life of the node; the
// probe RPCs only fire while this node is the leader (the only node whose
// effective version drives what gets written to the log). Shares leaseStop as
// its shutdown signal.
func (n *Node) versionLoop() {
	interval := max(n.config.HeartbeatTimeout, 500*time.Millisecond)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-n.leaseStop:
			return
		case <-ticker.C:
			if n.raft.State() == raft.Leader {
				n.probeVoters()
			}
			n.recomputeEffectiveVersions()
		}
	}
}

// probeVoters dials every voter's RPC sidecar, runs the Hello handshake, and
// records each peer's advertised format versions. Best-effort: an unreachable
// voter simply stays "unknown" (which keeps the effective version conservative
// until it answers).
func (n *Node) probeVoters() {
	cf := n.raft.GetConfiguration()
	if cf.Error() != nil {
		return
	}
	for _, srv := range cf.Configuration().Servers {
		if srv.Suffrage != raft.Voter || string(srv.ID) == n.config.NodeID {
			continue
		}
		n.probePeerVersion(string(srv.Address))
	}
}

// probePeerVersion runs a single Hello against the peer at raftAddr and records
// the response. Errors are ignored: the peer keeps its previous (or unknown)
// recorded version, and the conservative floor covers the gap.
func (n *Node) probePeerVersion(raftAddr string) {
	client, err := n.rpcPool.get(raftAddr)
	if err != nil {
		return
	}
	req := &RPCHelloRequest{
		NodeID:                n.config.NodeID,
		LibraryVersion:        LibraryVersion,
		ProtocolVersion:       ProtocolVersion,
		CommandFormatVersion:  CommandFormatVersion,
		SnapshotFormatVersion: SnapshotFormatVersion,
	}
	var resp RPCHelloResponse
	if err = client.Call("Colmena.Hello", req, &resp); err != nil {
		n.rpcPool.markFailed(raftAddr)
		return
	}
	n.versions.record(resp.NodeID /* command */, resp.CommandFormatVersion /* snapshot */, resp.SnapshotFormatVersion)
}

// FormatStatus reports the cluster's command-format negotiation state, so a
// health check or dashboard can see "mid-migration" instead of discovering it
// as fsm-apply log spam. Meaningful on the leader (it owns the voter view);
// followers report their local snapshot of it.
type FormatStatus struct {
	// LocalCommandVersion is the newest command format this node writes/reads.
	LocalCommandVersion int
	// EffectiveCommandVersion is the format the leader is actually writing —
	// below LocalCommandVersion while a slower voter is still catching up.
	EffectiveCommandVersion int
	// Skew is true when the effective write version is being held below this
	// node's local max because a voter cannot yet read the newer format, i.e.
	// the cluster is mid-format-migration.
	Skew bool
	// PeersBehind lists voter NodeIDs whose advertised command version is older
	// than this node's local max (they hold the cluster back).
	PeersBehind []string
	// PeersAhead lists voter NodeIDs advertising a newer command version than
	// this node's local max (this node is the one holding things back).
	PeersAhead []string
}

// FormatStatus returns the current command-format negotiation state.
func (n *Node) FormatStatus() FormatStatus {
	st := FormatStatus{
		LocalCommandVersion:     CommandFormatVersion,
		EffectiveCommandVersion: n.effectiveCommandVersion(),
	}
	st.Skew = st.EffectiveCommandVersion < st.LocalCommandVersion
	cf := n.raft.GetConfiguration()
	if cf.Error() != nil {
		return st
	}
	n.versions.mu.RLock()
	defer n.versions.mu.RUnlock()
	for _, srv := range cf.Configuration().Servers {
		if srv.Suffrage != raft.Voter || string(srv.ID) == n.config.NodeID {
			continue
		}
		pf, ok := n.versions.peers[string(srv.ID)]
		if !ok || pf.command <= 0 {
			st.PeersBehind = append(st.PeersBehind, string(srv.ID)) // unknown == treat as behind
			st.Skew = true
			continue
		}
		if pf.command < CommandFormatVersion {
			st.PeersBehind = append(st.PeersBehind, string(srv.ID))
		} else if pf.command > CommandFormatVersion {
			st.PeersAhead = append(st.PeersAhead, string(srv.ID))
		}
	}
	return st
}
