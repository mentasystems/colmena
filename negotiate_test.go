package colmena

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"
)

// TestMapLeaderRPCError verifies the leader-side "not the leader" string is
// re-typed as the transient ErrNoLeader sentinel, while genuine server errors
// pass through verbatim.
func TestMapLeaderRPCError(t *testing.T) {
	transient := mapLeaderRPCError(rpcErrNotLeader)
	if !errors.Is(transient, ErrNoLeader) {
		t.Fatalf("expected %q to map to ErrNoLeader, got %v", rpcErrNotLeader, transient)
	}

	real := mapLeaderRPCError("UNIQUE constraint failed: users.email")
	if errors.Is(real, ErrNoLeader) {
		t.Fatalf("a real SQL error must not be ErrNoLeader: %v", real)
	}
	if real.Error() != "UNIQUE constraint failed: users.email" {
		t.Fatalf("real error should pass through verbatim, got %q", real.Error())
	}
}

// TestErrNoLeader_QuorumLoss verifies that when a node loses its leader
// (quorum loss here), a leader-routed ConsistencyWeak read fails with the typed
// ErrNoLeader sentinel — distinguishable from a real error — while a
// ConsistencyNone read stays available from the local replica.
func TestErrNoLeader_QuorumLoss(t *testing.T) {
	leader := testNode(t)
	leaderAddr := leader.config.Bind
	time.Sleep(500 * time.Millisecond)

	follower1 := testJoinNode(t, leaderAddr)
	follower2 := testJoinNode(t, leaderAddr)
	time.Sleep(1 * time.Second)

	if _, err := leader.DB().Exec("CREATE TABLE q (id INTEGER PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := leader.DB().Exec("INSERT INTO q (v) VALUES (?)", "hello"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	time.Sleep(500 * time.Millisecond)

	// Kill the two followers: the leader loses quorum and must step down, so
	// the cluster has no reachable leader.
	follower1.Close()
	follower2.Close()

	// Wait for the ex-leader to step down (no leader known).
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if !leader.IsLeader() && leader.LeaderAddr() == "" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if leader.IsLeader() {
		t.Fatal("leader did not step down after quorum loss")
	}

	// A ConsistencyWeak read now has no leader to forward to → ErrNoLeader.
	ctxWeak := WithConsistency(context.Background(), ConsistencyWeak)
	var got string
	weakErr := leader.DB().QueryRowContext(ctxWeak, "SELECT v FROM q WHERE id = 1").Scan(&got)
	if weakErr == nil {
		t.Fatal("expected ConsistencyWeak read to fail without a leader, got nil")
	}
	if !errors.Is(weakErr, ErrNoLeader) {
		t.Fatalf("expected ErrNoLeader, got %v", weakErr)
	}

	// A ConsistencyNone read stays available from the local replica.
	ctxNone := WithConsistency(context.Background(), ConsistencyNone)
	if err := leader.DB().QueryRowContext(ctxNone, "SELECT v FROM q WHERE id = 1").Scan(&got); err != nil {
		t.Fatalf("ConsistencyNone read must stay available without a leader: %v", err)
	}
	if got != "hello" {
		t.Fatalf("local read got %q, want %q", got, "hello")
	}
}

// TestNegotiate_SingleNodeWritesNewest verifies a bootstrapped single-node
// cluster writes its newest command format immediately (no voter holds it back)
// and reports no format skew.
func TestNegotiate_SingleNodeWritesNewest(t *testing.T) {
	node := testNode(t)

	if got := node.effectiveCommandVersion(); got != CommandFormatVersion {
		t.Fatalf("single node effective command version = %d, want %d", got, CommandFormatVersion)
	}
	fs := node.FormatStatus()
	if fs.Skew {
		t.Fatalf("single node should not report format skew: %+v", fs)
	}
	if len(fs.PeersBehind) != 0 || len(fs.PeersAhead) != 0 {
		t.Fatalf("single node should have no peers behind/ahead: %+v", fs)
	}
}

// TestNegotiate_HoldsBackForOldVoter verifies the leader does NOT write the
// newest format while a voter only supports an older one: effective version is
// pinned to that voter's max, FormatStatus reports the skew, writes still
// round-trip at the held-back format, and the version flips up once every voter
// reports support for the newer format.
func TestNegotiate_HoldsBackForOldVoter(t *testing.T) {
	leader := testNode(t)
	leaderAddr := leader.config.Bind
	time.Sleep(500 * time.Millisecond)

	follower1 := testJoinNode(t, leaderAddr)
	follower2 := testJoinNode(t, leaderAddr)
	time.Sleep(1 * time.Second)

	// All three are this build → effective is the newest format, no skew.
	if got := leader.effectiveCommandVersion(); got != CommandFormatVersion {
		t.Fatalf("all-current cluster effective version = %d, want %d", got, CommandFormatVersion)
	}
	if leader.FormatStatus().Skew {
		t.Fatalf("all-current cluster should not report skew: %+v", leader.FormatStatus())
	}

	// Simulate follower2 being an older node that only reads format v1. Close
	// it first so the probe loop can't overwrite the injected version (a closed
	// peer stays "as recorded").
	oldID := follower2.NodeID()
	follower2.Close()
	leader.versions.record(oldID, 1, 1)
	leader.recomputeEffectiveVersions()

	if got := leader.effectiveCommandVersion(); got != 1 {
		t.Fatalf("with an old voter, effective version = %d, want 1 (held back)", got)
	}
	fs := leader.FormatStatus()
	if !fs.Skew {
		t.Fatalf("expected format skew with a lagging voter: %+v", fs)
	}
	if !slices.Contains(fs.PeersBehind, oldID) {
		t.Fatalf("expected %s in PeersBehind, got %+v", oldID, fs.PeersBehind)
	}

	// A write must still succeed and replicate while held back at v1, and the
	// surviving current-version follower must be able to read it.
	if _, err := leader.DB().Exec("CREATE TABLE held (id INTEGER PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatalf("create table while held back: %v", err)
	}
	if _, err := leader.DB().Exec("INSERT INTO held (v) VALUES (?)", "v1-write"); err != nil {
		t.Fatalf("insert while held back: %v", err)
	}
	time.Sleep(500 * time.Millisecond)

	ctxNone := WithConsistency(context.Background(), ConsistencyNone)
	var v string
	if err := follower1.DB().QueryRowContext(ctxNone, "SELECT v FROM held WHERE id = 1").Scan(&v); err != nil {
		t.Fatalf("current-version follower read of held-back write: %v", err)
	}
	if v != "v1-write" {
		t.Fatalf("follower read got %q, want %q", v, "v1-write")
	}

	// Once the lagging voter reports it can read the newer format, the leader
	// flips up to it.
	leader.versions.record(oldID, CommandFormatVersion, SnapshotFormatVersion)
	leader.recomputeEffectiveVersions()
	if got := leader.effectiveCommandVersion(); got != CommandFormatVersion {
		t.Fatalf("after all voters upgraded, effective version = %d, want %d", got, CommandFormatVersion)
	}
	if leader.FormatStatus().Skew {
		t.Fatalf("no skew expected after all voters upgraded: %+v", leader.FormatStatus())
	}
}
