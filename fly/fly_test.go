package fly

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/mentasystems/colmena"
	"github.com/mentasystems/colmena/cluster"
)

// --- pure decision logic ---

func TestShouldBootstrap(t *testing.T) {
	p := func(ids ...string) []cluster.Peer {
		out := make([]cluster.Peer, len(ids))
		for i, id := range ids {
			out[i] = cluster.Peer{NodeID: id, Advertise: id + ":9000"}
		}
		return out
	}
	cases := []struct {
		name     string
		self     string
		peers    []cluster.Peer
		expected int
		timedOut bool
		want     bool
	}{
		{"alone, gate not ready, no timeout", "a", nil, 3, false, false},
		{"alone, timed out, wins by default", "a", nil, 3, true, true},
		{"lowest id, gate ready", "a", p("b", "c"), 3, false, true},
		{"not lowest, gate ready", "z", p("b", "c"), 3, false, false},
		{"lowest id, gate not ready, no timeout", "a", p("b"), 3, false, false},
		{"lowest id, gate not ready, timed out", "a", p("b"), 3, true, true},
		{"not lowest, timed out", "z", p("a", "b"), 3, true, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			self := cluster.Peer{NodeID: c.self, Advertise: c.self + ":9000"}
			if got := shouldBootstrap(self, c.peers, c.expected, c.timedOut); got != c.want {
				t.Fatalf("shouldBootstrap=%v want %v", got, c.want)
			}
		})
	}
}

func TestWinsElection(t *testing.T) {
	peers := []cluster.Peer{{NodeID: "m", Advertise: "m:9000"}, {NodeID: "z", Advertise: "z:9000"}}
	if !winsElection(cluster.Peer{NodeID: "a", Advertise: "a:9000"}, peers) {
		t.Fatal("a should win over m,z")
	}
	if winsElection(cluster.Peer{NodeID: "n", Advertise: "n:9000"}, peers) {
		t.Fatal("n should not win over m")
	}
}

func TestCaughtUp(t *testing.T) {
	cases := []struct {
		name  string
		stats map[string]string
		want  bool
	}{
		{"never contacted", map[string]string{"last_contact": "never", "applied_index": "5", "commit_index": "5"}, false},
		{"stale contact", map[string]string{"last_contact": "3s", "applied_index": "5", "commit_index": "5"}, false},
		{"fresh, applied < commit", map[string]string{"last_contact": "100ms", "applied_index": "4", "commit_index": "5"}, false},
		{"fresh, applied == commit", map[string]string{"last_contact": "100ms", "applied_index": "5", "commit_index": "5"}, true},
		{"fresh, applied > commit", map[string]string{"last_contact": "100ms", "applied_index": "6", "commit_index": "5"}, true},
		{"leader zero contact", map[string]string{"last_contact": "0", "applied_index": "5", "commit_index": "5"}, true},
		{"unparseable indices", map[string]string{"last_contact": "100ms", "applied_index": "x", "commit_index": "5"}, false},
		{"empty contact", map[string]string{"last_contact": "", "applied_index": "5", "commit_index": "5"}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := caughtUp(c.stats); got != c.want {
				t.Fatalf("caughtUp=%v want %v", got, c.want)
			}
		})
	}
}

func TestProbeForLeaderNoReachablePeers(t *testing.T) {
	cfg := Config{VoterQuorum: 3}
	if _, _, ok := probeForLeader(cfg, nil); ok {
		t.Fatal("no peers → no leader")
	}
	// Unreachable address must be skipped, not block.
	peers := []cluster.Peer{{NodeID: "x", Advertise: "127.0.0.1:1"}}
	if _, _, ok := probeForLeader(cfg, peers); ok {
		t.Fatal("unreachable peer → no leader")
	}
}

func TestConfigDefaultsAndValidate(t *testing.T) {
	c := Config{NodeID: "m1", PrivateIP: "fdaa:0:1::1", Region: "mad", AppName: "app", DataDir: "/data"}
	c.applyDefaults()
	if c.RaftPort != 9000 || c.VoterQuorum != 3 || c.ExpectedVoters != 3 {
		t.Fatalf("unexpected defaults: %+v", c)
	}
	if c.DiscoveryInterval != 3*time.Second || c.PeerTTL != 15*time.Second || c.DeadVoterTimeout != 30*time.Second {
		t.Fatalf("unexpected timing defaults: %+v", c)
	}
	if c.advertise() != "[fdaa:0:1::1]:9000" {
		t.Fatalf("advertise = %q", c.advertise())
	}
	if c.bind() != "[::]:9000" {
		t.Fatalf("bind = %q", c.bind())
	}
	if err := c.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	bad := Config{NodeID: "m1", Region: "mad", AppName: "app", DataDir: "/data", PrivateIP: "not-an-ip"}
	bad.applyDefaults()
	if err := bad.validate(); err == nil {
		t.Fatal("expected invalid PrivateIP to fail validation")
	}
}

// --- lifecycle integration: real colmena nodes over loopback ---

// fakeDiscovery returns a programmable peer set.
type fakeDiscovery struct {
	mu    sync.Mutex
	peers []cluster.Peer
}

func (f *fakeDiscovery) set(peers []cluster.Peer) {
	f.mu.Lock()
	f.peers = peers
	f.mu.Unlock()
}
func (f *fakeDiscovery) Start(ctx context.Context, self cluster.Peer) error { return nil }
func (f *fakeDiscovery) Peers() []cluster.Peer {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]cluster.Peer(nil), f.peers...)
}
func (f *fakeDiscovery) UpdateFlags(bootstrapping, voter bool) error { return nil }
func (f *fakeDiscovery) Close() error                                { return nil }

func freePortPair(t testing.TB) int {
	t.Helper()
	for i := 0; i < 200; i++ {
		ln1, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			continue
		}
		port := ln1.Addr().(*net.TCPAddr).Port
		ln1.Close()
		ln2, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port+1))
		if err != nil {
			continue
		}
		ln2.Close()
		return port
	}
	t.Fatal("no free consecutive port pair")
	return 0
}

func newColmenaNode(t *testing.T, id string, bootstrap bool, joinAddr string, asNonvoter bool) (*colmena.Node, string) {
	t.Helper()
	port := freePortPair(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	cfg := colmena.Config{
		NodeID:           id,
		DataDir:          t.TempDir(),
		Bind:             addr,
		Advertise:        addr,
		HeartbeatTimeout: 200 * time.Millisecond,
		ElectionTimeout:  200 * time.Millisecond,
		ApplyTimeout:     5 * time.Second,
		LogOutput:        io.Discard,
	}
	if bootstrap {
		cfg.Bootstrap = true
	} else {
		cfg.Join = []string{joinAddr}
		if asNonvoter {
			cfg.JoinAs = colmena.JoinAsNonvoter
		}
	}
	n, err := colmena.New(cfg)
	if err != nil {
		t.Fatalf("new node %s: %v", id, err)
	}
	t.Cleanup(func() { n.Close() })
	return n, addr
}

func countVoters(n *colmena.Node) (voters int, ids map[string]bool) {
	servers, err := n.Nodes()
	if err != nil {
		return 0, nil
	}
	ids = make(map[string]bool)
	for _, s := range servers {
		ids[string(s.ID)] = true
		if s.Suffrage.String() == "Voter" {
			voters++
		}
	}
	return voters, ids
}

// TestSweepDeadVoterAndPromote builds a 3-voter + 1-nonvoter cluster, kills a
// voter, and asserts the leader-side sweep removes it and a non-voter is
// promoted to restore the quorum — the core failover the fly lifecycle adds.
func TestSweepDeadVoterAndPromote(t *testing.T) {
	if testing.Short() {
		t.Skip("skips real-cluster lifecycle test in -short")
	}
	a, aAddr := newColmenaNode(t, "a", true, "", false)
	if err := a.WaitForLeader(5 * time.Second); err != nil {
		t.Fatalf("wait leader: %v", err)
	}
	_, bAddr := newColmenaNode(t, "b", false, aAddr, false)
	c, cAddr := newColmenaNode(t, "c", false, aAddr, false)
	_, dAddr := newColmenaNode(t, "d", false, aAddr, true) // non-voter

	// Wait for the configuration to settle at 3 voters + 1 non-voter.
	if !waitFor(10*time.Second, func() bool {
		v, ids := countVoters(a)
		return v == 3 && len(ids) == 4
	}) {
		v, ids := countVoters(a)
		t.Fatalf("cluster did not form 3v+1nv: voters=%d ids=%v", v, ids)
	}

	disc := &fakeDiscovery{}
	cl := &Cluster{
		Node:   a,
		cfg:    Config{VoterQuorum: 3, DeadVoterTimeout: time.Millisecond},
		logger: log.New(io.Discard, "", 0),
		disc:   disc,
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}

	// c dies; discovery only sees b and d (a is self, c is gone).
	c.Close()
	disc.set([]cluster.Peer{
		{NodeID: "b", Advertise: bAddr},
		{NodeID: "d", Advertise: dAddr},
	})

	lastSeen := map[string]time.Time{}
	ok := waitFor(15*time.Second, func() bool {
		cl.sweepDeadVoters(lastSeen)
		cl.promoteIfNeeded()
		v, ids := countVoters(a)
		return !ids["c"] && ids["d"] && v == 3 && isVoter(a, "d")
	})
	_ = cAddr
	if !ok {
		v, ids := countVoters(a)
		t.Fatalf("failover did not restore quorum: voters=%d ids=%v", v, ids)
	}
}

func isVoter(n *colmena.Node, id string) bool {
	servers, err := n.Nodes()
	if err != nil {
		return false
	}
	for _, s := range servers {
		if string(s.ID) == id {
			return s.Suffrage.String() == "Voter"
		}
	}
	return false
}

func waitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return cond()
}
