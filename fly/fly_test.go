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

func TestOrphanVerdict(t *testing.T) {
	cases := []struct {
		name                              string
		isLeader, selfInConfig, hasOthers bool
		peerListsUs, peerExcludesUs       bool
		want                              orphanDecision
	}{
		{"leader is always healthy", true, false, true, false, true, orphanHealthy},
		{"normal follower lists itself", false, true, true, false, false, orphanHealthy},
		{"recovering full restart lists itself", false, true, true, false, false, orphanHealthy},
		{"fresh node knows no other servers", false, false, false, false, false, orphanHealthy},
		// Our own config excludes us but a healthy peer still lists us (e.g. a
		// fresh joiner being added) → not orphaned.
		{"healthy peer still lists us", false, false, true, true, false, orphanHealthy},
		// The orphan: own config excludes us AND a healthy peer confirms the
		// exclusion.
		{"removed: healthy peer excludes us", false, false, true, false, true, orphanConfirmed},
		// Own config excludes us but no healthy peer is reachable to confirm
		// (e.g. a brief partition) → wait, don't wipe.
		{"excluded but no authoritative peer", false, false, true, false, false, orphanInconclusive},
		// "lists us" must win over a stale "excludes us" if both are seen.
		{"listed beats excluded", false, false, true, true, true, orphanHealthy},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := orphanVerdict(c.isLeader, c.selfInConfig, c.hasOthers, c.peerListsUs, c.peerExcludesUs)
			if got != c.want {
				t.Fatalf("orphanVerdict=%d want %d", got, c.want)
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

// freeSpacedPortPairs returns n base ports such that each {p, p+1} raft/RPC pair
// is disjoint from every other — adjacent bases would collide (one node's RPC
// sidecar on p+1 is another node's raft port). Each base differs from the rest
// by at least 2.
func freeSpacedPortPairs(t testing.TB, n int) []int {
	t.Helper()
	abs := func(x int) int {
		if x < 0 {
			return -x
		}
		return x
	}
	var ports []int
	for len(ports) < n {
		p := freePortPair(t)
		ok := true
		for _, q := range ports {
			if abs(p-q) < 2 {
				ok = false
				break
			}
		}
		if ok {
			ports = append(ports, p)
		}
	}
	return ports
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

// TestHasExistingState verifies the recovery gate's discriminator: an empty
// data dir reports no state (a true first cold start), while a dir that has held
// a formed node reports state present (a returning member to be recovered, not
// bootstrapped).
func TestHasExistingState(t *testing.T) {
	dir := t.TempDir()
	if has, err := colmena.HasExistingState(dir); err != nil || has {
		t.Fatalf("fresh dir: has=%v err=%v, want false/nil", has, err)
	}

	port := freePortPair(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	n, err := colmena.New(colmena.Config{
		NodeID:           "s1",
		DataDir:          dir,
		Bind:             addr,
		Advertise:        addr,
		HeartbeatTimeout: 200 * time.Millisecond,
		ElectionTimeout:  200 * time.Millisecond,
		ApplyTimeout:     5 * time.Second,
		LogOutput:        io.Discard,
		Bootstrap:        true,
	})
	if err != nil {
		t.Fatalf("new node: %v", err)
	}
	if err := n.WaitForLeader(5 * time.Second); err != nil {
		t.Fatalf("wait leader: %v", err)
	}
	// Close so the BoltDB lock is released before HasExistingState reopens it.
	if err := n.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if has, err := colmena.HasExistingState(dir); err != nil || !has {
		t.Fatalf("after forming state: has=%v err=%v, want true/nil", has, err)
	}
}

// restartableNode is a colmena node bound to a fixed dataDir+addr so it can be
// closed and brought back up on the same identity — what a Fly machine restart
// in place looks like (FLY_MACHINE_ID and FLY_PRIVATE_IP are stable across a
// restart, so NodeID and advertise address persist).
func newColmenaNodeAt(t *testing.T, id, dataDir, addr string, bootstrap bool, joinAddr string) *colmena.Node {
	t.Helper()
	cfg := colmena.Config{
		NodeID:           id,
		DataDir:          dataDir,
		Bind:             addr,
		Advertise:        addr,
		HeartbeatTimeout: 200 * time.Millisecond,
		ElectionTimeout:  200 * time.Millisecond,
		ApplyTimeout:     5 * time.Second,
		LogOutput:        io.Discard,
	}
	if bootstrap {
		cfg.Bootstrap = true
	} else if joinAddr != "" {
		cfg.Join = []string{joinAddr}
	} else {
		// No bootstrap, no join: recover from persisted state (the fly recovery
		// path), bringing Raft up on the on-disk configuration.
		cfg.Recover = true
	}
	n, err := colmena.New(cfg)
	if err != nil {
		t.Fatalf("new node %s: %v", id, err)
	}
	return n
}

// TestFullRestartReElects is the regression test for the cold-start quorum
// deadlock (BUG.md): a 3-voter cluster whose every member is restarted at once,
// with pre-existing multi-server Raft state and no leader reachable during
// startup, must re-elect a leader on its own. It exercises the recovery
// mechanism the fly layer now uses — bring Raft up on the persisted
// configuration with neither Bootstrap nor Join — rather than gating on a leader
// that cannot exist until a quorum is already listening.
func TestFullRestartReElects(t *testing.T) {
	if testing.Short() {
		t.Skip("skips real-cluster lifecycle test in -short")
	}
	ports := freeSpacedPortPairs(t, 3)
	dirs := [3]string{t.TempDir(), t.TempDir(), t.TempDir()}
	ids := [3]string{"a", "b", "c"}
	addr := func(i int) string { return fmt.Sprintf("127.0.0.1:%d", ports[i]) }

	// Form a 3-voter cluster: a bootstraps, b and c join as voters.
	a := newColmenaNodeAt(t, ids[0], dirs[0], addr(0), true, "")
	if err := a.WaitForLeader(5 * time.Second); err != nil {
		t.Fatalf("wait leader: %v", err)
	}
	b := newColmenaNodeAt(t, ids[1], dirs[1], addr(1), false, addr(0))
	c := newColmenaNodeAt(t, ids[2], dirs[2], addr(2), false, addr(0))
	if !waitFor(10*time.Second, func() bool {
		v, idset := countVoters(a)
		return v == 3 && len(idset) == 3
	}) {
		v, idset := countVoters(a)
		t.Fatalf("cluster did not form 3 voters: voters=%d ids=%v", v, idset)
	}

	// Every node now has a 3-server configuration persisted. Each must report
	// existing state (so the fly layer takes the recovery path on restart).
	nodes := [3]*colmena.Node{a, b, c}
	for i := range nodes {
		if err := nodes[i].Close(); err != nil {
			t.Fatalf("close %s: %v", ids[i], err)
		}
	}
	for i := range dirs {
		if has, err := colmena.HasExistingState(dirs[i]); err != nil || !has {
			t.Fatalf("node %s: HasExistingState=%v err=%v, want true", ids[i], has, err)
		}
	}

	// Full cold restart: every node comes back with NO bootstrap and NO join —
	// exactly the recovery path. No leader exists at startup, yet they must
	// re-elect among the persisted voters without external intervention.
	var restarted [3]*colmena.Node
	for i := range restarted {
		restarted[i] = newColmenaNodeAt(t, ids[i], dirs[i], addr(i), false, "")
	}
	defer func() {
		for _, n := range restarted {
			if n != nil {
				n.Close()
			}
		}
	}()

	leaderEmerged := waitFor(20*time.Second, func() bool {
		for _, n := range restarted {
			if n.IsLeader() {
				return true
			}
		}
		return false
	})
	if !leaderEmerged {
		t.Fatal("no leader re-elected after full-cluster restart with persisted state")
	}

	// And exactly one leader, with all three back as voters — no split-brain.
	leaders := 0
	for _, n := range restarted {
		if n.IsLeader() {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("want exactly 1 leader after recovery, got %d", leaders)
	}
	if !waitFor(10*time.Second, func() bool {
		for _, n := range restarted {
			if n.IsLeader() {
				v, idset := countVoters(n)
				return v == 3 && len(idset) == 3
			}
		}
		return false
	}) {
		t.Fatal("recovered cluster did not settle at 3 voters")
	}
}

// TestOrphanSelfHeal is the regression test for the operational bug where a node
// removed from the cluster configuration while it was offline comes back with
// stale Raft state naming itself a member, loses elections forever ("not in
// configuration"), and only Fly's kill-and-restart breaks the loop — endlessly.
// The fly layer must detect this (a healthy peer's configuration excludes us)
// and self-heal: wipe local state and restart fresh.
func TestOrphanSelfHeal(t *testing.T) {
	if testing.Short() {
		t.Skip("skips real-cluster lifecycle test in -short")
	}
	ports := freeSpacedPortPairs(t, 3)
	dirs := [3]string{t.TempDir(), t.TempDir(), t.TempDir()}
	ids := [3]string{"a", "b", "c"}
	addr := func(i int) string { return fmt.Sprintf("127.0.0.1:%d", ports[i]) }

	// Form a 3-voter cluster: a bootstraps, b and c join.
	a := newColmenaNodeAt(t, ids[0], dirs[0], addr(0), true, "")
	defer a.Close()
	if err := a.WaitForLeader(5 * time.Second); err != nil {
		t.Fatalf("wait leader: %v", err)
	}
	b := newColmenaNodeAt(t, ids[1], dirs[1], addr(1), false, addr(0))
	defer b.Close()
	c := newColmenaNodeAt(t, ids[2], dirs[2], addr(2), false, addr(0))
	if !waitFor(10*time.Second, func() bool {
		v, idset := countVoters(a)
		return v == 3 && len(idset) == 3
	}) {
		v, idset := countVoters(a)
		t.Fatalf("cluster did not form 3 voters: voters=%d ids=%v", v, idset)
	}

	// c goes offline, then the leader removes it from the configuration. c never
	// learns it was removed — its on-disk config still names {a,b,c}.
	if err := c.Close(); err != nil {
		t.Fatalf("close c: %v", err)
	}
	if err := a.RemoveNode("c"); err != nil {
		t.Fatalf("remove c: %v", err)
	}
	if !waitFor(10*time.Second, func() bool {
		_, idset := countVoters(a)
		return !idset["c"] && len(idset) == 2
	}) {
		_, idset := countVoters(a)
		t.Fatalf("leader did not drop c: ids=%v", idset)
	}

	// c restarts from its persisted (stale) state — the fly recovery path. It
	// believes it is still a member but can never win an election, since a and b
	// reject it as "not in configuration".
	cr := newColmenaNodeAt(t, ids[2], dirs[2], addr(2), false, "")
	defer cr.Close()

	healed := make(chan string, 1)
	disc := &fakeDiscovery{}
	disc.set([]cluster.Peer{
		{NodeID: "a", Advertise: addr(0)},
		{NodeID: "b", Advertise: addr(1)},
	})
	cl := &Cluster{
		Node: cr,
		cfg: Config{
			DataDir:             dirs[2],
			VoterQuorum:         3,
			OrphanConfirmations: 2,
			OnSelfHeal:          func(reason string) { healed <- reason },
		},
		logger: log.New(io.Discard, "", 0),
		disc:   disc,
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}

	// Drive the check until self-heal fires. cr must look orphaned: it lists
	// itself, has no leader, and healthy peers a/b exclude it.
	strikes := 0
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if cl.checkOrphaned(&strikes) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	select {
	case reason := <-healed:
		if reason == "" {
			t.Fatal("heal fired with empty reason")
		}
	default:
		selfIn, hasOthers := cl.selfConfigState()
		t.Fatalf("orphaned node did not self-heal (strikes=%d, selfInConfig=%v, hasOthers=%v, leaderID=%q)",
			strikes, selfIn, hasOthers, cr.LeaderID())
	}

	// heal closed the node and wiped its state: the dir is now pristine, so a
	// restart takes the fresh join path instead of recovering the stale config.
	if has, err := colmena.HasExistingState(dirs[2]); err != nil || has {
		t.Fatalf("c's state not wiped after self-heal: HasExistingState=%v err=%v", has, err)
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
