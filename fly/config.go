package fly

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"strconv"
	"time"

	"github.com/mentasystems/colmena"
)

// resolver is the subset of *net.Resolver the fly discovery uses, so tests can
// inject a fake. *net.Resolver satisfies it.
type resolver interface {
	LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error)
	LookupTXT(ctx context.Context, name string) ([]string, error)
}

// Config configures a Colmena node that auto-clusters on Fly.io. The identity
// and topology fields (NodeID, PrivateIP, Region, AppName) are normally filled
// from the Fly machine environment via FromEnv; the caller then sets DataDir
// (and optionally RaftPort, tuning, TLS) before calling Start.
type Config struct {
	// --- identity / topology (FromEnv fills these from the Fly env) ---

	// NodeID is the stable Raft node id. On Fly this is FLY_MACHINE_ID.
	NodeID string
	// PrivateIP is this machine's 6PN IPv6 address (FLY_PRIVATE_IP). It is
	// the host part of the Raft advertise address.
	PrivateIP string
	// Region pins discovery to a single Fly region (FLY_REGION). All voters
	// must share one region — cross-region Raft latency is out of scope.
	Region string
	// AppName is the Fly app name (FLY_APP_NAME); it builds the .internal
	// DNS names used for discovery.
	AppName string

	// --- required by the caller ---

	// DataDir is where Colmena state lives (Raft log, snapshots, SQLite).
	// On Fly this should be a persistent volume mount. Required.
	DataDir string

	// RaftPort is the Raft transport listen port; the RPC sidecar listens on
	// RaftPort+1. The node binds [::]:RaftPort (all IPv6 interfaces) and
	// advertises [PrivateIP]:RaftPort. Default: 9000.
	RaftPort int

	// --- clustering policy ---

	// VoterQuorum is the target number of Raft voters. The first VoterQuorum
	// nodes become voters; later nodes join as non-voting learners. Default: 3.
	VoterQuorum int

	// ExpectedVoters is the cold-start anti-split-brain gate: a node will not
	// bootstrap until it has either observed a formed cluster, seen at least
	// ExpectedVoters-1 peers in DNS, or BootstrapTimeout elapses. Default:
	// max(VoterQuorum, COLMENA_BOOTSTRAP_EXPECT env).
	ExpectedVoters int

	// BootstrapTimeout bounds how long the cold-start gate waits before
	// proceeding with whatever peers are visible. Default: 15s.
	BootstrapTimeout time.Duration

	// DiscoveryInterval is the Fly DNS poll cadence. Default: 3s.
	DiscoveryInterval time.Duration

	// PeerTTL is how long a peer absent from DNS stays in the snapshot before
	// being aged out. Default: 15s (≈5 missed 3s polls; absorbs deploy churn).
	PeerTTL time.Duration

	// DeadVoterTimeout is how long the leader tolerates a voter that has
	// vanished from Fly DNS before removing it (after which a non-voter is
	// promoted to restore quorum). Much shorter than the LAN default because
	// Fly deploys recreate machines routinely. Default: 30s. Set to 0 to
	// disable the sweeper (and auto-promotion).
	DeadVoterTimeout time.Duration

	// Consistency is the default read consistency. Default: ConsistencyNone.
	Consistency colmena.ConsistencyLevel

	// TLSConfig enables mutual TLS on the Raft transport and RPC sidecar,
	// passed straight through to colmena.Config.TLSConfig. Off by default on
	// Fly: the 6PN is already a private WireGuard mesh per org. To enable,
	// build a *tls.Config (e.g. with colmena's lan identity helpers) and set
	// it here; the same config is used for the bootstrap ProbeStatus dials.
	TLSConfig *tls.Config

	// --- colmena pass-throughs (zero values keep colmena defaults) ---
	BatchWindow  time.Duration
	BatchMaxSize int
	OnApply      func(db string, statements []colmena.Statement, results []colmena.ExecResult)
	Backup       *colmena.BackupConfig
	LogOutput    io.Writer

	// Resolver overrides the DNS resolver. Default: net.DefaultResolver (the
	// VM's libc resolver already targets Fly's internal nameserver over 6PN).
	// Tests inject a fake; operators can point it at [fdaa::3]:53 if needed.
	Resolver resolver
}

// FromEnv reads the Fly machine environment (FLY_MACHINE_ID, FLY_PRIVATE_IP,
// FLY_REGION, FLY_APP_NAME) into a Config. The caller must still set DataDir
// (and may override RaftPort and tuning) before calling Start.
func FromEnv() (Config, error) {
	c := Config{
		NodeID:    os.Getenv("FLY_MACHINE_ID"),
		PrivateIP: os.Getenv("FLY_PRIVATE_IP"),
		Region:    os.Getenv("FLY_REGION"),
		AppName:   os.Getenv("FLY_APP_NAME"),
	}
	for k, v := range map[string]string{
		"FLY_MACHINE_ID": c.NodeID,
		"FLY_PRIVATE_IP": c.PrivateIP,
		"FLY_REGION":     c.Region,
		"FLY_APP_NAME":   c.AppName,
	} {
		if v == "" {
			return Config{}, fmt.Errorf("fly: %s not set (not running on a Fly machine?)", k)
		}
	}
	return c, nil
}

func (c *Config) applyDefaults() {
	if c.RaftPort == 0 {
		c.RaftPort = 9000
	}
	if c.VoterQuorum == 0 {
		c.VoterQuorum = 3
	}
	if c.ExpectedVoters == 0 {
		c.ExpectedVoters = c.VoterQuorum
		if v := os.Getenv("COLMENA_BOOTSTRAP_EXPECT"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > c.ExpectedVoters {
				c.ExpectedVoters = n
			}
		}
	}
	if c.BootstrapTimeout == 0 {
		c.BootstrapTimeout = 15 * time.Second
	}
	if c.DiscoveryInterval == 0 {
		c.DiscoveryInterval = 3 * time.Second
	}
	if c.PeerTTL == 0 {
		c.PeerTTL = 15 * time.Second
	}
	if c.DeadVoterTimeout == 0 {
		c.DeadVoterTimeout = 30 * time.Second
	}
	if c.Consistency == 0 {
		c.Consistency = colmena.ConsistencyNone
	}
	if c.LogOutput == nil {
		c.LogOutput = os.Stderr
	}
	if c.Resolver == nil {
		c.Resolver = net.DefaultResolver
	}
	// Canonicalize PrivateIP so string comparisons against resolver output
	// (always canonical) match — e.g. self-exclusion in the degraded path.
	if a, err := netip.ParseAddr(c.PrivateIP); err == nil {
		c.PrivateIP = a.String()
	}
}

func (c *Config) validate() error {
	if c.DataDir == "" {
		return fmt.Errorf("fly: DataDir is required")
	}
	if c.NodeID == "" {
		return fmt.Errorf("fly: NodeID (FLY_MACHINE_ID) is required")
	}
	if c.PrivateIP == "" {
		return fmt.Errorf("fly: PrivateIP (FLY_PRIVATE_IP) is required")
	}
	if _, err := netip.ParseAddr(c.PrivateIP); err != nil {
		return fmt.Errorf("fly: PrivateIP %q is not a valid IP: %w", c.PrivateIP, err)
	}
	if c.Region == "" {
		return fmt.Errorf("fly: Region (FLY_REGION) is required")
	}
	if c.AppName == "" {
		return fmt.Errorf("fly: AppName (FLY_APP_NAME) is required")
	}
	if c.VoterQuorum < 1 {
		return fmt.Errorf("fly: VoterQuorum must be >= 1")
	}
	return nil
}

// advertise is the address peers dial for Raft/RPC: [PrivateIP]:RaftPort.
func (c *Config) advertise() string {
	return net.JoinHostPort(c.PrivateIP, strconv.Itoa(c.RaftPort))
}

// bind is the local listen address: [::]:RaftPort (all IPv6 interfaces).
func (c *Config) bind() string {
	return net.JoinHostPort("::", strconv.Itoa(c.RaftPort))
}

// regionDomain is the AAAA name listing instances in our region.
func (c *Config) regionDomain() string {
	return fmt.Sprintf("%s.%s.internal", c.Region, c.AppName)
}

// vmsDomain is the TXT name listing "<machine_id> <region>" for every instance.
func (c *Config) vmsDomain() string {
	return fmt.Sprintf("vms.%s.internal", c.AppName)
}

// machineDomain is the per-machine AAAA name resolving to one machine's 6PN IP.
func (c *Config) machineDomain(machineID string) string {
	return fmt.Sprintf("%s.vm.%s.internal", machineID, c.AppName)
}
