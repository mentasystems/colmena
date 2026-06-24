package colmena

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// Metrics holds structured observability data for a colmena node.
type Metrics struct {
	// Raft state
	RaftState        string
	RaftTerm         uint64
	RaftLastIndex    uint64
	RaftCommitIndex  uint64
	RaftAppliedIndex uint64
	RaftFSMPending   int

	// Snapshot
	SnapshotIndex uint64

	// Throughput
	WritesTotal      uint64
	ReadsTotal       uint64
	RPCForwardsTotal uint64

	// Health
	LastContact time.Duration
	Peers       int

	// Format negotiation
	// FormatSkew is true when this node is holding its write version below its
	// local max because a voter cannot yet read the newer format (or the leader
	// has not yet confirmed every voter's version) — i.e. the cluster is
	// mid-format-migration.
	FormatSkew bool
	// EffectiveCommandVersion is the command-envelope version actually being
	// written; below LocalCommandVersion during a format migration.
	EffectiveCommandVersion int
	// LocalCommandVersion is the newest command format this build supports.
	LocalCommandVersion int
	// FormatRejectsTotal counts log entries this node refused to apply because
	// their envelope version was unknown (newer than this build). A non-zero,
	// growing value means committed entries are NOT being applied here — state
	// is diverging; treat it as a failed-readiness condition, not info.
	FormatRejectsTotal uint64
}

// metricsCounters holds the atomic counters embedded in Node.
type metricsCounters struct {
	writesTotal        atomic.Uint64
	readsTotal         atomic.Uint64
	rpcForwardsTotal   atomic.Uint64
	formatRejectsTotal atomic.Uint64
}

// Metrics returns a snapshot of the node's current observability data.
func (n *Node) Metrics() Metrics {
	stats := n.raft.Stats()
	fs := n.FormatStatus()

	return Metrics{
		RaftState:               stats["state"],
		RaftTerm:                parseUint64(stats["term"]),
		RaftLastIndex:           parseUint64(stats["last_log_index"]),
		RaftCommitIndex:         parseUint64(stats["commit_index"]),
		RaftAppliedIndex:        parseUint64(stats["applied_index"]),
		RaftFSMPending:          parseInt(stats["fsm_pending"]),
		SnapshotIndex:           parseUint64(stats["last_snapshot_index"]),
		WritesTotal:             n.metrics.writesTotal.Load(),
		ReadsTotal:              n.metrics.readsTotal.Load(),
		RPCForwardsTotal:        n.metrics.rpcForwardsTotal.Load(),
		LastContact:             parseDuration(stats["last_contact"]),
		Peers:                   countPeers(stats["latest_configuration"]),
		FormatSkew:              fs.Skew,
		EffectiveCommandVersion: fs.EffectiveCommandVersion,
		LocalCommandVersion:     fs.LocalCommandVersion,
		FormatRejectsTotal:      n.metrics.formatRejectsTotal.Load(),
	}
}

// MetricsHandler returns an http.Handler that serves node metrics in
// Prometheus text exposition format.
func (n *Node) MetricsHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		m := n.Metrics()
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

		var b strings.Builder

		writeGauge(&b, "colmena_raft_state", "Current Raft state (1=leader, 2=follower, 3=candidate, 0=shutdown)", raftStateToInt(m.RaftState))
		writeGauge(&b, "colmena_raft_term", "Current Raft term", m.RaftTerm)
		writeGauge(&b, "colmena_raft_last_index", "Last Raft log index", m.RaftLastIndex)
		writeGauge(&b, "colmena_raft_commit_index", "Raft commit index", m.RaftCommitIndex)
		writeGauge(&b, "colmena_raft_applied_index", "Raft applied index", m.RaftAppliedIndex)
		writeGauge(&b, "colmena_raft_fsm_pending", "Number of pending FSM operations", m.RaftFSMPending)
		writeGauge(&b, "colmena_snapshot_index", "Last snapshot index", m.SnapshotIndex)
		writeCounter(&b, "colmena_writes_total", "Total write operations applied", m.WritesTotal)
		writeCounter(&b, "colmena_reads_total", "Total read operations executed", m.ReadsTotal)
		writeCounter(&b, "colmena_rpc_forwards_total", "Total RPC-forwarded operations", m.RPCForwardsTotal)
		writeGauge(&b, "colmena_last_contact_ms", "Milliseconds since last leader contact", m.LastContact.Milliseconds())
		writeGauge(&b, "colmena_peers", "Number of peers in Raft configuration", m.Peers)
		writeGauge(&b /* name */, "colmena_format_skew" /* help */, "1 when the cluster is mid-format-migration (write version held below local max)", boolToInt(m.FormatSkew))
		writeGauge(&b /* name */, "colmena_command_format_effective" /* help */, "Command envelope version currently being written", m.EffectiveCommandVersion)
		writeGauge(&b /* name */, "colmena_command_format_local" /* help */, "Newest command envelope version this build supports", m.LocalCommandVersion)
		writeCounter(&b /* name */, "colmena_format_rejects_total" /* help */, "Committed log entries refused because their envelope version is unknown (state divergence)", m.FormatRejectsTotal)

		fmt.Fprint(w, b.String())
	})
}

// --- Prometheus text format helpers ---

func writeGauge[T int | int64 | uint64](b *strings.Builder, name, help string, value T) {
	fmt.Fprintf(b, "# HELP %s %s\n", name, help)
	fmt.Fprintf(b, "# TYPE %s gauge\n", name)
	fmt.Fprintf(b, "%s %d\n", name, value)
}

func writeCounter[T uint64](b *strings.Builder, name, help string, value T) {
	fmt.Fprintf(b, "# HELP %s %s\n", name, help)
	fmt.Fprintf(b, "# TYPE %s counter\n", name)
	fmt.Fprintf(b, "%s %d\n", name, value)
}

// --- Parsing helpers ---

func parseUint64(s string) uint64 {
	v, _ := strconv.ParseUint(s, 10, 64)
	return v
}

func parseInt(s string) int {
	v, _ := strconv.Atoi(s)
	return v
}

func parseDuration(s string) time.Duration {
	if s == "" || s == "never" || s == "0" {
		return 0
	}
	d, _ := time.ParseDuration(s)
	return d
}

func countPeers(config string) int {
	if config == "" {
		return 0
	}
	return strings.Count(config, "Suffrage")
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func raftStateToInt(state string) int {
	switch strings.ToLower(state) {
	case "leader":
		return 1
	case "follower":
		return 2
	case "candidate":
		return 3
	default:
		return 0
	}
}
