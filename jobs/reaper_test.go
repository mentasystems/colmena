package jobs

import (
	"database/sql"
	"testing"
	"time"

	"github.com/mentasystems/colmena"
)

// insertJob writes a colmena_jobs row directly so the test controls status and
// finished_at precisely, without waiting on a handler.
func insertJob(t *testing.T, db *sql.DB, id, status string, finishedAtMs int64) {
	t.Helper()
	var finished any
	if finishedAtMs != 0 {
		finished = finishedAtMs
	}
	_, err := db.Exec(
		`INSERT INTO colmena_jobs
            (id, type, payload, status, enqueued_at, run_at, finished_at, timeout_ms)
         VALUES (?, 'noop', X'', ?, ?, ?, ?, 0)`,
		id, status, time.Now().UnixMilli(), time.Now().UnixMilli(), finished,
	)
	if err != nil {
		t.Fatalf("insert %s: %v", id, err)
	}
}

func jobExists(t *testing.T, db *sql.DB, id string) bool {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM colmena_jobs WHERE id = ?`, id).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", id, err)
	}
	return n > 0
}

// TestReaperDeletesExpiredTerminalJobs verifies the reaper deletes succeeded and
// dead jobs older than RetainTerminal while leaving recent terminal jobs and
// non-terminal jobs untouched.
func TestReaperDeletesExpiredTerminalJobs(t *testing.T) {
	_, m := testManager(t, func(c *Config) {
		c.RetainTerminal = time.Hour
		c.ReapInterval = 50 * time.Millisecond
	})
	db := m.Node().DB()

	now := time.Now().UnixMilli()
	oldMs := now - (2 * time.Hour).Milliseconds()      // beyond retention
	recentMs := now - (5 * time.Minute).Milliseconds() // within retention

	insertJob(t, db, "old-succeeded", string(StatusSucceeded), oldMs)
	insertJob(t, db, "old-dead", string(StatusDead), oldMs)
	insertJob(t, db, "recent-succeeded", string(StatusSucceeded), recentMs)
	insertJob(t, db, "pending", string(StatusPending), 0) // finished_at NULL

	waitFor(t, 3*time.Second, "old terminal jobs reaped", func() bool {
		return !jobExists(t, db, "old-succeeded") && !jobExists(t, db, "old-dead")
	})

	if !jobExists(t, db, "recent-succeeded") {
		t.Fatal("recent succeeded job was reaped but is within retention")
	}
	if !jobExists(t, db, "pending") {
		t.Fatal("pending job was reaped but is not terminal")
	}
	if got := m.reaped.Load(); got < 2 {
		t.Fatalf("reaped counter = %d, want >= 2", got)
	}
}

// TestReaperDisabled verifies a negative RetainTerminal keeps all history.
func TestReaperDisabled(t *testing.T) {
	_, m := testManager(t, func(c *Config) {
		c.RetainTerminal = -1
		c.ReapInterval = 50 * time.Millisecond
	})
	db := m.Node().DB()

	oldMs := time.Now().UnixMilli() - (48 * time.Hour).Milliseconds()
	insertJob(t, db, "ancient", string(StatusSucceeded), oldMs)

	// Give a reaper (if it were running) several ticks to act.
	time.Sleep(300 * time.Millisecond)

	if !jobExists(t, db, "ancient") {
		t.Fatal("job reaped despite reaping being disabled (RetainTerminal < 0)")
	}
}

// TestReaperReplicatesDeterministically boots a 3-node cluster and asserts that
// after the leader reaps, every node's LOCAL replica agrees on exactly which
// rows survive. The reap DELETE is replicated through Raft and applied on all
// nodes, so a non-deterministic statement (e.g. LIMIT without a total order)
// would silently diverge the cluster — this guards against that.
func TestReaperReplicatesDeterministically(t *testing.T) {
	nodes := testCluster(t, 3)
	leader := nodes[0]
	attachManager(t, leader, func(c *Config) {
		c.RetainTerminal = time.Hour
		c.ReapInterval = 50 * time.Millisecond
	})

	ldb := leader.DB()
	now := time.Now().UnixMilli()
	oldMs := now - (2 * time.Hour).Milliseconds()
	recentMs := now - (5 * time.Minute).Milliseconds()

	insertJob(t, ldb, "c-old-1", string(StatusSucceeded), oldMs)
	insertJob(t, ldb, "c-old-2", string(StatusDead), oldMs)
	insertJob(t, ldb, "c-recent", string(StatusSucceeded), recentMs)
	insertJob(t, ldb, "c-pending", string(StatusPending), 0)

	// Every node's local replica must end up with exactly the survivors.
	localCount := func(n *colmena.Node) int {
		db := n.OpenDB("default", colmena.ConsistencyNone)
		var c int
		if err := db.QueryRow(`SELECT COUNT(*) FROM colmena_jobs`).Scan(&c); err != nil {
			t.Fatalf("local count: %v", err)
		}
		return c
	}

	waitFor(t, 5*time.Second, "all nodes converge to 2 survivors", func() bool {
		for _, n := range nodes {
			if localCount(n) != 2 {
				return false
			}
		}
		return true
	})

	// Confirm the survivors are the right ones on every node, not just the count.
	for i, n := range nodes {
		db := n.OpenDB("default", colmena.ConsistencyNone)
		for _, id := range []string{"c-recent", "c-pending"} {
			var c int
			if err := db.QueryRow(`SELECT COUNT(*) FROM colmena_jobs WHERE id = ?`, id).Scan(&c); err != nil {
				t.Fatalf("node %d survivor query: %v", i, err)
			}
			if c != 1 {
				t.Fatalf("node %d missing survivor %s", i, id)
			}
		}
	}
}
