package cluster

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mentasystems/colmena"
)

func TestDecideRoleAlone(t *testing.T) {
	role, addr, boot := DecideRole("aaa-node", nil, 3)
	if !boot {
		t.Fatal("alone node should bootstrap")
	}
	if addr != "" {
		t.Fatalf("alone node should not have a join addr, got %q", addr)
	}
	if role != colmena.JoinAsVoter {
		t.Fatalf("expected voter role, got %v", role)
	}
}

func TestDecideRoleBootstrapElectionWinner(t *testing.T) {
	// "aaa" sees two other bootstrappers with bigger IDs → "aaa" wins.
	peers := []Peer{
		{NodeID: "bbb", Advertise: "10.0.0.2:9000", Bootstrapping: true},
		{NodeID: "ccc", Advertise: "10.0.0.3:9000", Bootstrapping: true},
	}
	_, _, boot := DecideRole("aaa", peers, 3)
	if !boot {
		t.Fatal("smallest ID should bootstrap")
	}
}

func TestDecideRoleBootstrapElectionLoser(t *testing.T) {
	// "ccc" sees a smaller bootstrapper → "ccc" should wait.
	peers := []Peer{
		{NodeID: "aaa", Advertise: "10.0.0.1:9000", Bootstrapping: true},
		{NodeID: "bbb", Advertise: "10.0.0.2:9000", Bootstrapping: true},
	}
	_, addr, boot := DecideRole("ccc", peers, 3)
	if boot {
		t.Fatal("non-smallest ID should not bootstrap")
	}
	if addr != "" {
		t.Fatalf("expected to wait (no join addr), got %q", addr)
	}
}

func TestDecideRoleJoinAsVoterWhenQuorumNotReached(t *testing.T) {
	// Cluster has 2 voters, quorum target is 3 → joiner becomes voter.
	peers := []Peer{
		{NodeID: "aaa", Advertise: "10.0.0.1:9000", Voter: true},
		{NodeID: "bbb", Advertise: "10.0.0.2:9000", Voter: true},
	}
	role, addr, boot := DecideRole("ddd", peers, 3)
	if boot {
		t.Fatal("should join, not bootstrap")
	}
	if addr == "" {
		t.Fatal("expected a join address")
	}
	if role != colmena.JoinAsVoter {
		t.Fatalf("expected voter role, got %v", role)
	}
}

func TestDecideRoleJoinAsNonvoterWhenQuorumFull(t *testing.T) {
	// Cluster already has 3 voters → 4th joiner becomes non-voter.
	peers := []Peer{
		{NodeID: "aaa", Advertise: "10.0.0.1:9000", Voter: true},
		{NodeID: "bbb", Advertise: "10.0.0.2:9000", Voter: true},
		{NodeID: "ccc", Advertise: "10.0.0.3:9000", Voter: true},
	}
	role, addr, boot := DecideRole("ddd", peers, 3)
	if boot {
		t.Fatal("should join, not bootstrap")
	}
	if addr == "" {
		t.Fatal("expected a join address")
	}
	if role != colmena.JoinAsNonvoter {
		t.Fatalf("expected non-voter role, got %v", role)
	}
}

func TestDecideRoleMixedBootstrappingAndFormed(t *testing.T) {
	// One peer is a real cluster member (bootstrapping=false), another is
	// still discovering. The real one wins — we join, never bootstrap.
	peers := []Peer{
		{NodeID: "aaa", Advertise: "10.0.0.1:9000", Voter: true, Bootstrapping: false},
		{NodeID: "bbb", Advertise: "10.0.0.2:9000", Voter: false, Bootstrapping: true},
	}
	_, addr, boot := DecideRole("zzz", peers, 3)
	if boot {
		t.Fatal("should join the formed cluster, not bootstrap")
	}
	if addr != "10.0.0.1:9000" {
		t.Fatalf("expected to join via aaa, got %q", addr)
	}
}

func TestDecideRoleFromPeersFormedFlag(t *testing.T) {
	// Empty list → not formed.
	if _, _, formed := DecideRoleFromPeers(nil, 3); formed {
		t.Fatal("expected formed=false for empty peers")
	}
	// All bootstrapping → not formed.
	all := []Peer{{NodeID: "x", Advertise: "1:1", Bootstrapping: true}}
	if _, _, formed := DecideRoleFromPeers(all, 3); formed {
		t.Fatal("expected formed=false when all peers are bootstrapping")
	}
}

func TestRoleName(t *testing.T) {
	if RoleName(colmena.JoinAsVoter) != "voter" {
		t.Fatal("voter name")
	}
	if RoleName(colmena.JoinAsNonvoter) != "non-voter" {
		t.Fatal("non-voter name")
	}
}

func TestFormedPeerAddrsPreferredFirst(t *testing.T) {
	peers := []Peer{
		{NodeID: "aaa", Advertise: "10.0.0.1:9000"},
		{NodeID: "bbb", Advertise: "10.0.0.2:9000"},
		{NodeID: "ccc", Advertise: "10.0.0.3:9000", Bootstrapping: true}, // excluded
	}
	got := FormedPeerAddrs(peers, "10.0.0.2:9000")
	if len(got) != 2 {
		t.Fatalf("expected 2 formed addrs, got %v", got)
	}
	if got[0] != "10.0.0.2:9000" {
		t.Fatalf("preferred must be first, got %v", got)
	}
	for _, a := range got {
		if a == "10.0.0.3:9000" {
			t.Fatalf("bootstrapping peer must be excluded, got %v", got)
		}
	}
}

// fakeDiscovery is a minimal Discovery whose peer set can be swapped at
// runtime, used to drive WaitForFormedCluster.
type fakeDiscovery struct {
	peers func() []Peer
}

func (f *fakeDiscovery) Start(ctx context.Context, self Peer) error  { return nil }
func (f *fakeDiscovery) Peers() []Peer                               { return f.peers() }
func (f *fakeDiscovery) UpdateFlags(bootstrapping, voter bool) error { return nil }
func (f *fakeDiscovery) Close() error                                { return nil }

func TestWaitForFormedClusterReturnsOnFormed(t *testing.T) {
	var formed atomic.Bool
	d := &fakeDiscovery{peers: func() []Peer {
		if !formed.Load() {
			return []Peer{{NodeID: "a", Advertise: "10.0.0.1:9000", Bootstrapping: true}}
		}
		return []Peer{{NodeID: "a", Advertise: "10.0.0.1:9000", Bootstrapping: false}}
	}}
	go func() {
		time.Sleep(200 * time.Millisecond)
		formed.Store(true)
	}()
	addr, err := WaitForFormedCluster(context.Background(), d, 5*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if addr != "10.0.0.1:9000" {
		t.Fatalf("expected join addr, got %q", addr)
	}
}

func TestWaitForFormedClusterContextCancel(t *testing.T) {
	d := &fakeDiscovery{peers: func() []Peer {
		return []Peer{{NodeID: "a", Advertise: "10.0.0.1:9000", Bootstrapping: true}}
	}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := WaitForFormedCluster(ctx, d, 5*time.Second); err == nil {
		t.Fatal("expected context cancellation error")
	}
}

func TestWaitForFormedClusterTimeout(t *testing.T) {
	d := &fakeDiscovery{peers: func() []Peer {
		return []Peer{{NodeID: "a", Advertise: "10.0.0.1:9000", Bootstrapping: true}}
	}}
	if _, err := WaitForFormedCluster(context.Background(), d, 100*time.Millisecond); err == nil {
		t.Fatal("expected timeout error")
	}
}
