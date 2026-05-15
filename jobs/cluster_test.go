package jobs

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mentasystems/colmena"
)

// testCluster builds an n-node cluster. Bind-time port conflicts trigger a
// retry of the whole bring-up so a slow port release in CI doesn't fail
// the test.
func testCluster(t *testing.T, n int) []*colmena.Node {
	t.Helper()

	for attempt := 0; attempt < 5; attempt++ {
		nodes, ok := tryStartCluster(t, n)
		if ok {
			time.Sleep(500 * time.Millisecond)
			return nodes
		}
		// All previously-created nodes were already cleaned up via
		// t.Cleanup hooks the failed attempt registered.
	}
	t.Fatal("could not start cluster after 5 attempts")
	return nil
}

func tryStartCluster(t *testing.T, n int) ([]*colmena.Node, bool) {
	t.Helper()

	leaderPort := freePort(t)
	leader, err := colmena.New(colmena.Config{
		NodeID:            fmt.Sprintf("c%d-leader", leaderPort),
		DataDir:           t.TempDir(),
		Bind:              fmt.Sprintf("127.0.0.1:%d", leaderPort),
		Bootstrap:         true,
		HeartbeatTimeout:  200 * time.Millisecond,
		ElectionTimeout:   200 * time.Millisecond,
		SnapshotInterval:  5 * time.Second,
		SnapshotThreshold: 100,
		ApplyTimeout:      5 * time.Second,
		LogOutput:         discardWriter{},
	})
	if err != nil {
		return nil, false
	}
	t.Cleanup(func() { leader.Close() })
	if err := leader.WaitForLeader(5 * time.Second); err != nil {
		return nil, false
	}

	nodes := []*colmena.Node{leader}
	for i := 1; i < n; i++ {
		port := freePort(t)
		node, err := colmena.New(colmena.Config{
			NodeID:           fmt.Sprintf("c%d-follower%d", leaderPort, i),
			DataDir:          t.TempDir(),
			Bind:             fmt.Sprintf("127.0.0.1:%d", port),
			Join:             []string{fmt.Sprintf("127.0.0.1:%d", leaderPort)},
			HeartbeatTimeout: 200 * time.Millisecond,
			ElectionTimeout:  200 * time.Millisecond,
			ApplyTimeout:     5 * time.Second,
			LogOutput:        discardWriter{},
		})
		if err != nil {
			return nil, false
		}
		t.Cleanup(func() { node.Close() })
		nodes = append(nodes, node)
	}
	return nodes, true
}

func attachManager(t *testing.T, node *colmena.Node, configure func(*Config)) *Manager {
	t.Helper()
	cfg := Config{
		Workers:        2,
		PollInterval:   50 * time.Millisecond,
		DefaultTimeout: 2 * time.Second,
		SweepInterval:  100 * time.Millisecond,
		ScheduleInterval: 100 * time.Millisecond,
		DefaultMaxAttempts: 3,
		DefaultBackoff: Backoff{Base: 20 * time.Millisecond, Max: 200 * time.Millisecond},
	}
	if configure != nil {
		configure(&cfg)
	}
	m, err := New(node, cfg)
	if err != nil {
		t.Fatalf("jobs.New: %v", err)
	}
	t.Cleanup(func() { m.Close() })
	return m
}

// TestMultiNodeClaimRace boots three nodes, registers the same handler on
// each, enqueues a single job, and asserts exactly one node runs it.
func TestMultiNodeClaimRace(t *testing.T) {
	nodes := testCluster(t, 3)

	type Args struct{ Tag string }

	var execCount atomic.Int32
	mu := sync.Mutex{}
	runners := map[string]int{}
	managers := make([]*Manager, len(nodes))

	for i, node := range nodes {
		m := attachManager(t, node, nil)
		managers[i] = m
		nodeID := node.NodeID()
		Register(m, "race", func(ctx Context, a Args) error {
			execCount.Add(1)
			mu.Lock()
			runners[nodeID]++
			mu.Unlock()
			return nil
		})
	}

	id, err := Enqueue(managers[0], "race", Args{Tag: "x"})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	waitFor(t, 3*time.Second, "succeeded", func() bool {
		var status string
		_ = managers[0].Node().DB().QueryRow(
			`SELECT status FROM colmena_jobs WHERE id = ?`, id).Scan(&status)
		return status == string(StatusSucceeded)
	})

	// Give stragglers a chance to (incorrectly) re-execute.
	time.Sleep(300 * time.Millisecond)
	if got := execCount.Load(); got != 1 {
		t.Fatalf("execCount = %d, want 1; runners=%v", got, runners)
	}
}

// TestMultiNodeFanout enqueues many jobs and asserts at least two distinct
// nodes share the work. This catches a regression where one node hogs all
// claims.
func TestMultiNodeFanout(t *testing.T) {
	nodes := testCluster(t, 3)

	type Args struct{}
	managers := make([]*Manager, len(nodes))

	mu := sync.Mutex{}
	byNode := map[string]int{}
	for i, node := range nodes {
		m := attachManager(t, node, func(c *Config) { c.Workers = 4 })
		managers[i] = m
		nodeID := node.NodeID()
		Register(m, "fan", func(ctx Context, a Args) error {
			mu.Lock()
			byNode[nodeID]++
			mu.Unlock()
			// Hold the worker briefly so other nodes get a turn.
			time.Sleep(20 * time.Millisecond)
			return nil
		})
	}

	const N = 60
	for i := 0; i < N; i++ {
		if _, err := Enqueue(managers[i%len(managers)], "fan", Args{}); err != nil {
			t.Fatal(err)
		}
	}

	waitFor(t, 10*time.Second, "all done", func() bool {
		var n int
		_ = managers[0].Node().DB().QueryRow(
			`SELECT COUNT(*) FROM colmena_jobs WHERE type='fan' AND status='succeeded'`,
		).Scan(&n)
		return n >= N
	})

	mu.Lock()
	defer mu.Unlock()
	if len(byNode) < 2 {
		t.Errorf("only one node executed jobs: %v", byNode)
	}
	total := 0
	for _, v := range byNode {
		total += v
	}
	if total != N {
		t.Errorf("total executions = %d, want %d (per-node: %v)", total, N, byNode)
	}
}

// TestMultiNodeRateLimit asserts that a cluster-wide Rate{N: x, Per: t}
// caps total executions across all nodes. We verify by reading the
// started_at timestamps after the run and checking that no rolling window
// of size t contains more than x of them — that's the property the rate
// limit promises, independent of enqueue/scheduling timing.
func TestMultiNodeRateLimit(t *testing.T) {
	nodes := testCluster(t, 3)

	type Args struct{}
	managers := make([]*Manager, len(nodes))
	const N = 12

	for i, node := range nodes {
		m := attachManager(t, node, nil)
		managers[i] = m
		Register(m, "limited", func(ctx Context, a Args) error { return nil })
	}

	// 2 per second cluster-wide.
	if err := SetRateLimit(managers[0], "limited", Rate{N: 2, Per: 1 * time.Second}); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < N; i++ {
		if _, err := Enqueue(managers[i%len(managers)], "limited", Args{}); err != nil {
			t.Fatal(err)
		}
	}

	// Wait until they've all completed.
	waitFor(t, 30*time.Second, "all done", func() bool {
		var n int
		_ = managers[0].Node().DB().QueryRow(
			`SELECT COUNT(*) FROM colmena_jobs WHERE type='limited' AND status='succeeded'`,
		).Scan(&n)
		return n >= N
	})

	// Pull the started_at history and verify the sliding-window invariant:
	// for every job, no more than capacity jobs (including itself) have a
	// started_at within the last period_ms.
	rows, err := managers[0].Node().DB().Query(
		`SELECT started_at FROM colmena_jobs WHERE type='limited' ORDER BY started_at`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var starts []int64
	for rows.Next() {
		var s int64
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		starts = append(starts, s)
	}
	if len(starts) != N {
		t.Fatalf("got %d starts, want %d", len(starts), N)
	}
	for i, s := range starts {
		count := 0
		for _, other := range starts {
			if other > s-1000 && other <= s {
				count++
			}
		}
		if count > 2 {
			t.Errorf("at index %d (started=%d), %d jobs started in the prior 1s — capacity is 2", i, s, count)
		}
	}
}

// TestSweeperRecoversAcrossLeaderChange forces a leader change while a
// running-state job is orphaned and asserts the new leader's sweeper picks
// it up.
func TestSweeperRecoversAcrossLeaderChange(t *testing.T) {
	nodes := testCluster(t, 3)

	type Args struct{}
	managers := make([]*Manager, len(nodes))
	var picked atomic.Int32

	for i, node := range nodes {
		m := attachManager(t, node, func(c *Config) {
			c.SweepInterval = 100 * time.Millisecond
		})
		managers[i] = m
		Register(m, "ghost", func(ctx Context, a Args) error {
			picked.Add(1)
			return nil
		})
	}

	// Find current leader and bypass it: insert a stuck running job via
	// the leader's manager, then close that node so a new leader takes
	// over.
	var leader *colmena.Node
	for _, n := range nodes {
		if n.IsLeader() {
			leader = n
			break
		}
	}
	if leader == nil {
		t.Fatal("no leader")
	}

	id := newID()
	old := time.Now().Add(-10 * time.Second).UnixMilli()
	if _, err := leader.DB().Exec(
		`INSERT INTO colmena_jobs
            (id, type, payload, status, priority, attempts, max_attempts,
             enqueued_at, run_at, claimed_at, claimed_by, started_at,
             unique_key, timeout_ms)
         VALUES (?, ?, ?, 'running', 0, 1, 5, ?, ?, ?, ?, ?, NULL, ?)`,
		id, "ghost", "{}", old, old, old, leader.NodeID(), old, int64(100),
	); err != nil {
		t.Fatal(err)
	}

	// Close the leader's manager so its sweeper goes silent. The other
	// two nodes will re-elect a new leader within a few election timeouts.
	for i, n := range nodes {
		if n == leader {
			_ = managers[i].Close()
			_ = n.Close()
			break
		}
	}

	// Wait for a new leader.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n != leader && n.IsLeader() {
				goto leaderElected
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("no new leader elected")
leaderElected:

	waitFor(t, 5*time.Second, "ghost reclaimed and run", func() bool {
		return picked.Load() >= 1
	})
}

// TestEnqueueFromFollower verifies that enqueue works through the leader-
// forwarding path on a follower node.
func TestEnqueueFromFollower(t *testing.T) {
	nodes := testCluster(t, 3)

	var follower *colmena.Node
	for _, n := range nodes {
		if !n.IsLeader() {
			follower = n
			break
		}
	}
	if follower == nil {
		t.Fatal("no follower")
	}

	type Args struct{ X int }
	got := make(chan int, 1)

	managers := make([]*Manager, len(nodes))
	for i, n := range nodes {
		m := attachManager(t, n, nil)
		managers[i] = m
		Register(m, "from_follower", func(ctx Context, a Args) error {
			select {
			case got <- a.X:
			default:
			}
			return nil
		})
	}

	// Enqueue specifically from the follower.
	var followerMgr *Manager
	for i, n := range nodes {
		if n == follower {
			followerMgr = managers[i]
			break
		}
	}

	if _, err := Enqueue(followerMgr, "from_follower", Args{X: 7}); err != nil {
		t.Fatalf("enqueue from follower: %v", err)
	}

	select {
	case v := <-got:
		if v != 7 {
			t.Fatalf("payload = %d, want 7", v)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler never ran (enqueue-from-follower path)")
	}
}

// TestMissingHandlerReleases asserts that a node which receives a job for an
// unknown type releases it back to pending instead of marking it failed.
func TestMissingHandlerReleases(t *testing.T) {
	// Two-node cluster: only node 0 has a handler for "two_phase". When
	// node 1 races to claim, it should release the job and node 0 takes it.
	nodes := testCluster(t, 2)

	type Args struct{}
	ran := make(chan struct{}, 1)
	m0 := attachManager(t, nodes[0], nil)
	m1 := attachManager(t, nodes[1], nil)
	Register(m0, "two_phase", func(ctx Context, a Args) error {
		ran <- struct{}{}
		return nil
	})

	// Enqueue from node 1. Node 1's worker won't see it (no handler), but
	// node 0 will pick it up.
	if _, err := Enqueue(m1, "two_phase", Args{}); err != nil {
		t.Fatal(err)
	}

	select {
	case <-ran:
	case <-time.After(3 * time.Second):
		t.Fatal("never ran on the node that has the handler")
	}

	// Sanity: there should be no jobs in dead status.
	var dead int
	if err := m0.Node().DB().QueryRow(
		`SELECT COUNT(*) FROM colmena_jobs WHERE type='two_phase' AND status='dead'`,
	).Scan(&dead); err != nil {
		t.Fatal(err)
	}
	if dead > 0 {
		t.Errorf("expected 0 dead jobs, got %d", dead)
	}
}

// _ keeps imports honest.
var _ = errors.New
