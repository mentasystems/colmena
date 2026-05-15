package lan

import (
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"github.com/mentasystems/colmena"
)

// Config configures a zero-config LAN cluster node.
type Config struct {
	// DataDir is where Colmena state lives (Raft log, snapshots, SQLite),
	// plus the persistent node_id and per-node TLS material. Required.
	DataDir string

	// Bind is the address Raft (and the RPC sidecar on Bind+1) listens on.
	// Use "0.0.0.0:9000" for "any interface, port 9000".
	// Required.
	Bind string

	// Advertise overrides the address published in mDNS for peers to dial.
	// Empty means "auto-detect the first non-loopback IPv4 and combine it
	// with the Bind port". Set this if auto-detection picks the wrong NIC.
	Advertise string

	// CACert and CAKey are PEM-encoded CA materials, typically embedded in
	// the binary via go:embed. Each node generates its own leaf cert
	// signed by this CA on first boot, and the SHA-256 of CACert is used
	// to derive the cluster identity advertised in mDNS, so different
	// CAs == different clusters on the same LAN.
	//
	// If both fields are empty, the cluster runs in plaintext. Acceptable
	// for trusted home labs; never run multi-tenant or anything reachable
	// outside your LAN without TLS.
	CACert []byte
	CAKey  []byte

	// VoterQuorum is the target number of Raft voters in the cluster.
	// The first VoterQuorum nodes to join become voters; later joiners
	// become non-voting learners that scale read throughput without
	// adding write-path latency. Default: 3. Set to 5 for a larger
	// failure-tolerant core. Set to 1 if you only ever expect a single
	// node (degrades to a single-machine deployment with mDNS support
	// for future expansion).
	VoterQuorum int

	// DiscoveryWindow is how long a starting node listens for peers
	// before deciding to bootstrap or join. Longer windows reduce the
	// risk of two simultaneous starts both believing they're alone, at
	// the cost of slower cold-cluster startup. Default: 8s.
	DiscoveryWindow time.Duration

	// DeadVoterTimeout is how long a peer can be unreachable via mDNS
	// before the leader removes it from the Raft configuration. Once
	// removed, an existing non-voter is auto-promoted to fill the
	// vacated voter slot, so the cluster restores its target voter
	// quorum without manual intervention. This is the failover budget
	// of the cluster: writes stall while a majority of voters is
	// unreachable, so shorter values recover faster but tolerate less
	// jitter in the underlying network. Default: 5m. Set to 0 to
	// disable the sweeper (and therefore auto-promotion).
	DeadVoterTimeout time.Duration

	// Consistency is forwarded to colmena.Config.Consistency. Default:
	// ConsistencyNone, because the typical reason to run a LAN cluster
	// is to scale local reads — None reads from each node's local
	// SQLite at ~6µs without crossing the network. Override if you
	// need stricter freshness.
	Consistency colmena.ConsistencyLevel

	// BatchWindow, BatchMaxSize, OnApply, Backup are passed through to
	// the underlying colmena.Config. Zero values keep colmena defaults.
	BatchWindow  time.Duration
	BatchMaxSize int
	OnApply      func(db string, statements []colmena.Statement, results []colmena.ExecResult)
	Backup       *colmena.BackupConfig

	// LogOutput receives diagnostic output from the lan package and from
	// Colmena's Raft logger. Default: os.Stderr.
	LogOutput io.Writer

	// ServiceName overrides the auto-derived mDNS service instance type.
	// Leave empty in production; useful for tests that need to isolate
	// the discovery namespace.
	ServiceName string
}

func (c *Config) applyDefaults() {
	if c.VoterQuorum == 0 {
		c.VoterQuorum = 3
	}
	if c.DiscoveryWindow == 0 {
		c.DiscoveryWindow = 8 * time.Second
	}
	if c.DeadVoterTimeout == 0 {
		c.DeadVoterTimeout = 5 * time.Minute
	}
	if c.Consistency == 0 {
		c.Consistency = colmena.ConsistencyNone
	}
	if c.LogOutput == nil {
		c.LogOutput = os.Stderr
	}
}

func (c *Config) validate() error {
	if c.DataDir == "" {
		return fmt.Errorf("lan: DataDir is required")
	}
	if c.Bind == "" {
		return fmt.Errorf("lan: Bind is required")
	}
	if _, _, err := net.SplitHostPort(c.Bind); err != nil {
		return fmt.Errorf("lan: invalid Bind %q: %w", c.Bind, err)
	}
	if (len(c.CACert) == 0) != (len(c.CAKey) == 0) {
		return fmt.Errorf("lan: CACert and CAKey must both be set or both empty")
	}
	if c.VoterQuorum < 1 {
		return fmt.Errorf("lan: VoterQuorum must be >= 1")
	}
	return nil
}
