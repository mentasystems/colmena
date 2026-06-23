package jobs

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mentasystems/colmena"
)

var _ = os.Getpid // keep os import if other usage is conditional

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

// testNode boots a single bootstrapped Colmena node. A small retry loop
// covers the rare case where another process grabs the chosen port pair
// between freePort's probe and colmena.New's bind.
func testNode(t *testing.T) *colmena.Node {
	t.Helper()
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		dir := t.TempDir()
		port := freePort(t)
		node, err := colmena.New(colmena.Config{
			NodeID:            fmt.Sprintf("jobs-test-%d-%d", port, attempt),
			DataDir:           dir,
			Bind:              fmt.Sprintf("127.0.0.1:%d", port),
			Bootstrap:         true,
			HeartbeatTimeout:  200 * time.Millisecond,
			ElectionTimeout:   200 * time.Millisecond,
			SnapshotInterval:  5 * time.Second,
			SnapshotThreshold: 100,
			ApplyTimeout:      5 * time.Second,
			LogOutput:         discardWriter{},
		})
		if err != nil {
			lastErr = err
			continue
		}
		t.Cleanup(func() { node.Close() })
		if err := node.WaitForLeader(5 * time.Second); err != nil {
			t.Fatalf("wait leader: %v", err)
		}
		return node
	}
	t.Fatalf("colmena.New (5 attempts): %v", lastErr)
	return nil
}

func testManager(t *testing.T, opts ...func(*Config)) (*colmena.Node, *Manager) {
	t.Helper()
	node := testNode(t)
	cfg := Config{
		Workers:            2,
		PollInterval:       50 * time.Millisecond,
		DefaultTimeout:     2 * time.Second,
		SweepInterval:      100 * time.Millisecond,
		ScheduleInterval:   100 * time.Millisecond,
		DefaultMaxAttempts: 3,
		DefaultBackoff:     Backoff{Base: 20 * time.Millisecond, Max: 200 * time.Millisecond},
	}
	for _, o := range opts {
		o(&cfg)
	}
	m, err := New(node, cfg)
	if err != nil {
		t.Fatalf("jobs.New: %v", err)
	}
	t.Cleanup(func() { m.Close() })
	return node, m
}

// portAlloc hands out fresh port pairs across the test binary so concurrent
// or back-to-back runs don't collide on the same offset.
var portAlloc atomic.Int32

func freePort(t testing.TB) int {
	t.Helper()
	// Start from a random offset per binary, then advance monotonically.
	// `-count=N` reuses the same binary, so the atomic counter still gives
	// us fresh values between repeated runs.
	if portAlloc.Load() == 0 {
		// Seed with process PID + nanosecond bits so parallel `go test`
		// runs of different packages don't trample each other.
		seed := int32(os.Getpid()*7) ^ int32(time.Now().UnixNano())
		portAlloc.CompareAndSwap(0, (seed%9000+1000)|1)
	}
	for i := 0; i < 200; i++ {
		port := int(portAlloc.Add(2)) % 65000
		if port < 12000 {
			port += 12000
		}
		ln1, e1 := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if e1 != nil {
			continue
		}
		ln2, e2 := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port+1))
		if e2 != nil {
			ln1.Close()
			continue
		}
		// Close listeners just before returning. A small race remains —
		// another process can grab the port between Close and the test's
		// real bind — so testNode and testCluster also retry on bind errors.
		ln1.Close()
		ln2.Close()
		return port
	}
	t.Fatal("no free port pair")
	return 0
}

// waitFor polls fn every 20ms until it returns true or the deadline passes.
func waitFor(t *testing.T, d time.Duration, msg string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("waitFor timeout: %s", msg)
}

// --- Single-node tests ---

func TestEnqueueAndRun(t *testing.T) {
	_, m := testManager(t)

	type Args struct{ N int }
	got := make(chan int, 1)
	Register(m, "echo", func(ctx Context, a Args) error {
		got <- a.N
		return nil
	})

	id, err := Enqueue(m, "echo", Args{N: 42})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if id == "" {
		t.Fatal("empty id")
	}

	select {
	case n := <-got:
		if n != 42 {
			t.Fatalf("payload = %d, want 42", n)
		}
	case <-time.After(2 * time.Second):
		var status string
		var attempts int
		_ = m.Node().DB().QueryRow(
			`SELECT status, attempts FROM colmena_jobs WHERE id = ?`, id,
		).Scan(&status, &attempts)
		t.Logf("job %s status=%q attempts=%d", id, status, attempts)
		t.Fatal("handler never ran")
	}

	waitFor(t, 2*time.Second, "status = succeeded", func() bool {
		var status string
		err := m.Node().DB().QueryRow(
			`SELECT status FROM colmena_jobs WHERE id = ?`, id).Scan(&status)
		return err == nil && status == string(StatusSucceeded)
	})
}

func TestRetryThenSucceed(t *testing.T) {
	_, m := testManager(t)

	type Args struct{}
	var attempts atomic.Int32
	Register(m, "flaky", func(ctx Context, a Args) error {
		n := attempts.Add(1)
		if n < 3 {
			return errors.New("transient")
		}
		return nil
	})

	id, err := Enqueue(m, "flaky", Args{})
	if err != nil {
		t.Fatal(err)
	}

	waitFor(t, 5*time.Second, "succeeded after retries", func() bool {
		var status string
		var n int
		_ = m.Node().DB().QueryRow(
			`SELECT status, attempts FROM colmena_jobs WHERE id = ?`,
			id).Scan(&status, &n)
		return status == string(StatusSucceeded) && n == 3
	})
}

func TestDeadAfterMaxAttempts(t *testing.T) {
	_, m := testManager(t)
	type Args struct{}
	Register(m, "always_fails", func(ctx Context, a Args) error {
		return errors.New("nope")
	})

	id, err := Enqueue(m, "always_fails", Args{}, WithMaxAttempts(2))
	if err != nil {
		t.Fatal(err)
	}

	waitFor(t, 5*time.Second, "marked dead", func() bool {
		var status string
		var n int
		_ = m.Node().DB().QueryRow(
			`SELECT status, attempts FROM colmena_jobs WHERE id = ?`,
			id).Scan(&status, &n)
		return status == string(StatusDead) && n == 2
	})
}

func TestUniqueKeyDedupes(t *testing.T) {
	_, m := testManager(t)
	type Args struct{ K string }
	// Block handler so the first job stays running while we try to enqueue
	// a duplicate.
	release := make(chan struct{})
	Register(m, "slow", func(ctx Context, a Args) error {
		<-release
		return nil
	})

	id1, err := Enqueue(m, "slow", Args{K: "a"}, WithUniqueKey("k1"))
	if err != nil {
		t.Fatal(err)
	}
	// Wait for it to be claimed (status = running) so the dedup must hit
	// the running row.
	waitFor(t, 2*time.Second, "running", func() bool {
		var s string
		_ = m.Node().DB().QueryRow(
			`SELECT status FROM colmena_jobs WHERE id = ?`, id1).Scan(&s)
		return s == string(StatusRunning)
	})

	id2, err := Enqueue(m, "slow", Args{K: "b"}, WithUniqueKey("k1"))
	if err != nil {
		t.Fatalf("dedup enqueue: %v", err)
	}
	if id2 != id1 {
		t.Fatalf("expected dedup to return %s, got %s", id1, id2)
	}
	close(release)
}

func TestSchedulerFiresPeriodic(t *testing.T) {
	_, m := testManager(t, func(c *Config) {
		c.ScheduleInterval = 50 * time.Millisecond
	})

	type Args struct{}
	fired := make(chan struct{}, 10)
	Register(m, "tick", func(ctx Context, a Args) error {
		select {
		case fired <- struct{}{}:
		default:
		}
		return nil
	})

	// Use "* * * * *" — fires every minute. We can't wait a real minute
	// so instead we backdate next_run_at to now and assert one fires.
	if err := Schedule(m, "tick-sched", "tick", "* * * * *", Args{}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Node().DB().Exec(
		`UPDATE colmena_jobs_schedule SET next_run_at = ? WHERE id = ?`,
		time.Now().UnixMilli(), "tick-sched",
	); err != nil {
		t.Fatal(err)
	}

	select {
	case <-fired:
	case <-time.After(3 * time.Second):
		t.Fatal("scheduler never fired")
	}

	// next_run_at should have advanced.
	var nextRun int64
	if err := m.Node().DB().QueryRow(
		`SELECT next_run_at FROM colmena_jobs_schedule WHERE id = ?`, "tick-sched",
	).Scan(&nextRun); err != nil {
		t.Fatal(err)
	}
	if nextRun <= time.Now().UnixMilli() {
		t.Errorf("next_run_at not advanced: %d", nextRun)
	}
}

func TestConcurrencyLimit(t *testing.T) {
	_, m := testManager(t, func(c *Config) { c.Workers = 5 })

	type Args struct{}
	var inflight atomic.Int32
	var maxSeen atomic.Int32
	release := make(chan struct{})
	Register(m, "limited", func(ctx Context, a Args) error {
		cur := inflight.Add(1)
		for {
			old := maxSeen.Load()
			if cur <= old || maxSeen.CompareAndSwap(old, cur) {
				break
			}
		}
		<-release
		inflight.Add(-1)
		return nil
	})
	if err := SetConcurrency(m, "limited", 2); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		if _, err := Enqueue(m, "limited", Args{}); err != nil {
			t.Fatal(err)
		}
	}

	// Wait until at least 2 are running, then ensure no more than 2.
	waitFor(t, 2*time.Second, "2 inflight", func() bool {
		return inflight.Load() == 2
	})
	time.Sleep(300 * time.Millisecond)
	if maxSeen.Load() > 2 {
		t.Fatalf("concurrency violated: max=%d", maxSeen.Load())
	}
	close(release)
}

func TestSweeperReclaimsOrphans(t *testing.T) {
	node, m := testManager(t, func(c *Config) {
		c.SweepInterval = 50 * time.Millisecond
	})

	type Args struct{}
	// Insert a fake job in 'running' state with an old claimed_at so the
	// sweeper will move it back to pending.
	id := newID()
	old := time.Now().Add(-10 * time.Second).UnixMilli()
	if _, err := node.DB().Exec(
		`INSERT INTO colmena_jobs
            (id, type, payload, status, priority, attempts, max_attempts,
             enqueued_at, run_at, claimed_at, claimed_by, started_at,
             unique_key, timeout_ms)
         VALUES (?, ?, ?, 'running', 0, 1, 5, ?, ?, ?, ?, ?, NULL, ?)`,
		id, "ghost", []byte(`{}`), old, old, old, "dead-node", old, int64(100),
	); err != nil {
		t.Fatal(err)
	}

	// Register a no-op handler so the worker would normally pick it up
	// after reclaim. We don't really need it to run; we just need the
	// status to flip back to pending.
	Register(m, "ghost", func(ctx Context, a Args) error { return nil })

	waitFor(t, 3*time.Second, "reclaimed", func() bool {
		var status string
		_ = node.DB().QueryRow(
			`SELECT status FROM colmena_jobs WHERE id = ?`, id).Scan(&status)
		return status == string(StatusPending) || status == string(StatusSucceeded)
	})
}

func TestStats(t *testing.T) {
	_, m := testManager(t)
	type Args struct{}
	Register(m, "ok", func(ctx Context, a Args) error { return nil })

	for i := 0; i < 5; i++ {
		if _, err := Enqueue(m, "ok", Args{}); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, 3*time.Second, "all done", func() bool {
		s, err := m.Stats()
		return err == nil && s.Succeeded >= 5
	})
	s, err := m.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if s.Succeeded < 5 {
		t.Errorf("succeeded = %d, want >= 5", s.Succeeded)
	}
	if s.ByStatus[StatusSucceeded] < 5 {
		t.Errorf("by_status[succeeded] = %d, want >= 5", s.ByStatus[StatusSucceeded])
	}
}

func TestHandlerTimeoutCancelsContext(t *testing.T) {
	_, m := testManager(t)

	type Args struct{}
	caught := make(chan error, 1)
	Register(m, "slow", func(ctx Context, a Args) error {
		<-ctx.Done()
		caught <- ctx.Err()
		return ctx.Err()
	})

	if _, err := Enqueue(m, "slow", Args{},
		WithTimeout(150*time.Millisecond),
		WithMaxAttempts(1),
	); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-caught:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("ctx err = %v, want DeadlineExceeded", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handler never cancelled")
	}
}
