package lan

import (
	"testing"

	"github.com/kidandcat/colmena"
)

func TestDecideRoleAlone(t *testing.T) {
	role, addr, boot := decideRole("aaa-node", nil, 3)
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
	peers := []peer{
		{NodeID: "bbb", Advertise: "10.0.0.2:9000", Bootstrapping: true},
		{NodeID: "ccc", Advertise: "10.0.0.3:9000", Bootstrapping: true},
	}
	_, _, boot := decideRole("aaa", peers, 3)
	if !boot {
		t.Fatal("smallest ID should bootstrap")
	}
}

func TestDecideRoleBootstrapElectionLoser(t *testing.T) {
	// "ccc" sees a smaller bootstrapper → "ccc" should wait.
	peers := []peer{
		{NodeID: "aaa", Advertise: "10.0.0.1:9000", Bootstrapping: true},
		{NodeID: "bbb", Advertise: "10.0.0.2:9000", Bootstrapping: true},
	}
	_, addr, boot := decideRole("ccc", peers, 3)
	if boot {
		t.Fatal("non-smallest ID should not bootstrap")
	}
	if addr != "" {
		t.Fatalf("expected to wait (no join addr), got %q", addr)
	}
}

func TestDecideRoleJoinAsVoterWhenQuorumNotReached(t *testing.T) {
	// Cluster has 2 voters, quorum target is 3 → joiner becomes voter.
	peers := []peer{
		{NodeID: "aaa", Advertise: "10.0.0.1:9000", Voter: true},
		{NodeID: "bbb", Advertise: "10.0.0.2:9000", Voter: true},
	}
	role, addr, boot := decideRole("ddd", peers, 3)
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
	peers := []peer{
		{NodeID: "aaa", Advertise: "10.0.0.1:9000", Voter: true},
		{NodeID: "bbb", Advertise: "10.0.0.2:9000", Voter: true},
		{NodeID: "ccc", Advertise: "10.0.0.3:9000", Voter: true},
	}
	role, addr, boot := decideRole("ddd", peers, 3)
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
	// One peer is a real cluster member (bootstrapping=false), another
	// is still discovering. The real one wins — we join, never bootstrap.
	peers := []peer{
		{NodeID: "aaa", Advertise: "10.0.0.1:9000", Voter: true, Bootstrapping: false},
		{NodeID: "bbb", Advertise: "10.0.0.2:9000", Voter: false, Bootstrapping: true},
	}
	_, addr, boot := decideRole("zzz", peers, 3)
	if boot {
		t.Fatal("should join the formed cluster, not bootstrap")
	}
	if addr != "10.0.0.1:9000" {
		t.Fatalf("expected to join via aaa, got %q", addr)
	}
}

func TestDecideRoleFromPeersFormedFlag(t *testing.T) {
	// Empty list → not formed.
	if _, _, formed := decideRoleFromPeers(nil, 3); formed {
		t.Fatal("expected formed=false for empty peers")
	}
	// All bootstrapping → not formed.
	all := []peer{{NodeID: "x", Advertise: "1:1", Bootstrapping: true}}
	if _, _, formed := decideRoleFromPeers(all, 3); formed {
		t.Fatal("expected formed=false when all peers are bootstrapping")
	}
}

func TestRoleName(t *testing.T) {
	if roleName(colmena.JoinAsVoter) != "voter" {
		t.Fatal("voter name")
	}
	if roleName(colmena.JoinAsNonvoter) != "non-voter" {
		t.Fatal("non-voter name")
	}
}
