package colmena

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// testNode creates a bootstrapped single node for testing.
func testNode(t *testing.T, opts ...func(*Config)) *Node {
	t.Helper()
	dir := t.TempDir()
	port := freePort(t)

	cfg := Config{
		NodeID:    fmt.Sprintf("test-node-%d", port),
		DataDir:   dir,
		Bind:      fmt.Sprintf("127.0.0.1:%d", port),
		Bootstrap: true,
		// Speed up Raft for tests.
		HeartbeatTimeout:  200 * time.Millisecond,
		ElectionTimeout:   200 * time.Millisecond,
		SnapshotInterval:  5 * time.Second,
		SnapshotThreshold: 100,
		ApplyTimeout:      5 * time.Second,
		LogOutput:         io.Discard,
	}
	for _, o := range opts {
		o(&cfg)
	}

	node, err := New(cfg)
	if err != nil {
		t.Fatalf("create node: %v", err)
	}
	t.Cleanup(func() { node.Close() })

	if err := node.WaitForLeader(5 * time.Second); err != nil {
		t.Fatalf("wait for leader: %v", err)
	}
	return node
}

// testJoinNode creates a node that joins an existing cluster.
func testJoinNode(t *testing.T, joinAddr string) *Node {
	t.Helper()
	dir := t.TempDir()
	port := freePort(t)

	cfg := Config{
		NodeID:           fmt.Sprintf("test-node-%d", port),
		DataDir:          dir,
		Bind:             fmt.Sprintf("127.0.0.1:%d", port),
		Join:             []string{joinAddr},
		HeartbeatTimeout: 200 * time.Millisecond,
		ElectionTimeout:  200 * time.Millisecond,
		ApplyTimeout:     5 * time.Second,
		LogOutput:        io.Discard,
	}

	node, err := New(cfg)
	if err != nil {
		t.Fatalf("create join node: %v", err)
	}
	t.Cleanup(func() { node.Close() })
	return node
}

func freePort(t testing.TB) int {
	t.Helper()
	// Use port 0 to get a free port, but we need two consecutive ports (Raft + RPC).
	// Find a pair of free ports by trying.
	for i := 0; i < 100; i++ {
		port := 19000 + (os.Getpid()+i*2)%10000
		// Check both port and port+1 are available.
		ln1, err1 := (&net.ListenConfig{}).Listen(context.Background(), "tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err1 != nil {
			continue
		}
		ln1.Close()
		ln2, err2 := (&net.ListenConfig{}).Listen(context.Background(), "tcp", fmt.Sprintf("127.0.0.1:%d", port+1))
		if err2 != nil {
			continue
		}
		ln2.Close()
		return port
	}
	t.Fatal("could not find free port pair")
	return 0
}

// --- Single Node Tests ---

func TestSingleNode_CreateTable(t *testing.T) {
	node := testNode(t)
	db := node.DB()

	_, err := db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Verify table exists.
	var count int
	err = db.QueryRow("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='test'").Scan(&count)
	if err != nil {
		t.Fatalf("query sqlite_master: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 table, got %d", count)
	}
}

func TestSingleNode_InsertAndSelect(t *testing.T) {
	node := testNode(t)
	db := node.DB()

	_, err := db.Exec("CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Insert.
	result, err := db.Exec("INSERT INTO kv (key, value) VALUES (?, ?)", "hello", "world")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	rows, _ := result.RowsAffected()
	if rows != 1 {
		t.Fatalf("expected 1 row affected, got %d", rows)
	}

	// Wait briefly for Raft apply to propagate to reader.
	time.Sleep(100 * time.Millisecond)

	// Select.
	var value string
	err = db.QueryRow("SELECT value FROM kv WHERE key = ?", "hello").Scan(&value)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if value != "world" {
		t.Fatalf("expected 'world', got %q", value)
	}
}

func TestSingleNode_MultipleInserts(t *testing.T) {
	node := testNode(t)
	db := node.DB()

	_, err := db.Exec("CREATE TABLE items (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	for i := 0; i < 50; i++ {
		_, err = db.Exec("INSERT INTO items (name) VALUES (?)", fmt.Sprintf("item-%d", i))
		if err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	time.Sleep(100 * time.Millisecond)

	var count int
	err = db.QueryRow("SELECT count(*) FROM items").Scan(&count)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 50 {
		t.Fatalf("expected 50 rows, got %d", count)
	}
}

func TestSingleNode_Transaction(t *testing.T) {
	node := testNode(t)
	db := node.DB()

	_, err := db.Exec("CREATE TABLE accounts (id TEXT PRIMARY KEY, balance INTEGER)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Use ExecMulti for atomic batch insert.
	_, err = node.ExecMulti([]Statement{
		{SQL: "INSERT INTO accounts (id, balance) VALUES (?, ?)", Args: []any{"alice", 100}},
		{SQL: "INSERT INTO accounts (id, balance) VALUES (?, ?)", Args: []any{"bob", 200}},
		{SQL: "INSERT INTO accounts (id, balance) VALUES (?, ?)", Args: []any{"charlie", 300}},
	})
	if err != nil {
		t.Fatalf("exec multi: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	var total int
	err = db.QueryRow("SELECT sum(balance) FROM accounts").Scan(&total)
	if err != nil {
		t.Fatalf("sum: %v", err)
	}
	if total != 600 {
		t.Fatalf("expected total 600, got %d", total)
	}
}

func TestSingleNode_TransactionViaTx(t *testing.T) {
	node := testNode(t)
	db := node.DB()

	_, err := db.Exec("CREATE TABLE data (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Begin transaction via database/sql.
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	tx.Exec("INSERT INTO data (val) VALUES (?)", "one")
	tx.Exec("INSERT INTO data (val) VALUES (?)", "two")
	tx.Exec("INSERT INTO data (val) VALUES (?)", "three")
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	var count int
	err = db.QueryRow("SELECT count(*) FROM data").Scan(&count)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 3 {
		t.Fatalf("expected 3 rows, got %d", count)
	}
}

func TestSingleNode_TransactionRollback(t *testing.T) {
	node := testNode(t)
	db := node.DB()

	_, err := db.Exec("CREATE TABLE data (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	tx.Exec("INSERT INTO data (val) VALUES (?)", "one")
	tx.Exec("INSERT INTO data (val) VALUES (?)", "two")
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	var count int
	err = db.QueryRow("SELECT count(*) FROM data").Scan(&count)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 rows after rollback, got %d", count)
	}
}

// TestTransaction_RowsAffected guards against the BUG.md regression: an
// UPDATE issued inside *sql.Tx silently returned RowsAffected=0 because the
// driver buffered the statement and applied it at Commit time. The fix makes
// the result lazy: pending until Commit, then populated with the real count.
func TestTransaction_RowsAffected(t *testing.T) {
	node := testNode(t)
	db := node.DB()

	if _, err := db.Exec("CREATE TABLE promo_codes (code TEXT PRIMARY KEY, redeemed_by TEXT, redeemed_at TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := db.Exec("INSERT INTO promo_codes (code) VALUES (?)", "PMC7PPNL37NR"); err != nil {
		t.Fatalf("seed row: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}

	res, err := tx.Exec(
		`UPDATE promo_codes SET redeemed_by = ?, redeemed_at = ?
		 WHERE code = ? AND redeemed_at IS NULL`,
		"user-1", "2026-05-03T00:00:00Z", "PMC7PPNL37NR")
	if err != nil {
		t.Fatalf("tx exec: %v", err)
	}

	// Before Commit the row count must NOT be silently 0 — that was the
	// original bug. It must surface ErrTxResultPending so callers can tell
	// "I haven't run yet" apart from "the WHERE matched nothing".
	if n, err := res.RowsAffected(); err == nil {
		t.Fatalf("RowsAffected before commit: expected ErrTxResultPending, got n=%d err=nil", n)
	} else if !errors.Is(err, ErrTxResultPending) {
		t.Fatalf("RowsAffected before commit: expected ErrTxResultPending, got %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// After Commit the real row count must be available.
	n, err := res.RowsAffected()
	if err != nil {
		t.Fatalf("RowsAffected after commit: %v", err)
	}
	if n != 1 {
		t.Fatalf("RowsAffected after commit: expected 1, got %d", n)
	}
}

// TestTransaction_RowsAffected_Rollback verifies a rolled-back transaction
// surfaces ErrTxRolledBack from buffered results instead of leaving callers
// blocked on ErrTxResultPending forever.
func TestTransaction_RowsAffected_Rollback(t *testing.T) {
	node := testNode(t)
	db := node.DB()

	if _, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	res, err := tx.Exec("INSERT INTO t (v) VALUES (?)", "x")
	if err != nil {
		t.Fatalf("tx exec: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	if _, err := res.RowsAffected(); !errors.Is(err, ErrTxRolledBack) {
		t.Fatalf("RowsAffected after rollback: expected ErrTxRolledBack, got %v", err)
	}
}

func TestSingleNode_ConsistencyStrong(t *testing.T) {
	node := testNode(t)
	db := node.DB()

	_, err := db.Exec("CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	_, err = db.Exec("INSERT INTO kv (key, value) VALUES (?, ?)", "k1", "v1")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Strong consistency read.
	ctx := WithConsistency(context.Background(), ConsistencyStrong)
	var value string
	err = db.QueryRowContext(ctx, "SELECT value FROM kv WHERE key = ?", "k1").Scan(&value)
	if err != nil {
		t.Fatalf("strong read: %v", err)
	}
	if value != "v1" {
		t.Fatalf("expected 'v1', got %q", value)
	}
}

// --- Multi Node Tests ---

func TestMultiNode_ThreeNodeCluster(t *testing.T) {
	// Bootstrap leader.
	leader := testNode(t)
	leaderAddr := leader.config.Bind

	// Give leader time to stabilize.
	time.Sleep(500 * time.Millisecond)

	// Join two followers.
	follower1 := testJoinNode(t, leaderAddr)
	follower2 := testJoinNode(t, leaderAddr)

	// Wait for cluster to stabilize.
	time.Sleep(1 * time.Second)

	// Verify cluster has 3 nodes.
	servers, err := leader.Nodes()
	if err != nil {
		t.Fatalf("get nodes: %v", err)
	}
	if len(servers) != 3 {
		t.Fatalf("expected 3 servers, got %d", len(servers))
	}

	// Write on leader.
	db := leader.DB()
	_, err = db.Exec("CREATE TABLE cluster_test (id INTEGER PRIMARY KEY, node TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	_, err = db.Exec("INSERT INTO cluster_test (node) VALUES (?)", "leader")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Wait for replication.
	time.Sleep(500 * time.Millisecond)

	// Read from each follower (None consistency = local read).
	for i, f := range []*Node{follower1, follower2} {
		fdb := f.DB()
		ctx := WithConsistency(context.Background(), ConsistencyNone)
		var node string
		err = fdb.QueryRowContext(ctx, "SELECT node FROM cluster_test WHERE id = 1").Scan(&node)
		if err != nil {
			t.Fatalf("follower%d read: %v", i+1, err)
		}
		if node != "leader" {
			t.Fatalf("follower%d: expected 'leader', got %q", i+1, node)
		}
	}
}

func TestMultiNode_LeaderForwarding(t *testing.T) {
	// Bootstrap leader.
	leader := testNode(t)
	leaderAddr := leader.config.Bind

	time.Sleep(500 * time.Millisecond)

	// Join a follower.
	follower := testJoinNode(t, leaderAddr)

	time.Sleep(1 * time.Second)

	// Create table on leader.
	_, err := leader.DB().Exec("CREATE TABLE forward_test (id INTEGER PRIMARY KEY, source TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	// Write FROM the follower — should be forwarded to leader.
	fdb := follower.DB()
	_, err = fdb.Exec("INSERT INTO forward_test (source) VALUES (?)", "from-follower")
	if err != nil {
		t.Fatalf("follower write (forwarded): %v", err)
	}

	// Wait for replication.
	time.Sleep(500 * time.Millisecond)

	// Verify data on leader.
	var source string
	err = leader.DB().QueryRow("SELECT source FROM forward_test WHERE id = 1").Scan(&source)
	if err != nil {
		t.Fatalf("leader read: %v", err)
	}
	if source != "from-follower" {
		t.Fatalf("expected 'from-follower', got %q", source)
	}

	// Verify data on follower (local read).
	ctx := WithConsistency(context.Background(), ConsistencyNone)
	err = fdb.QueryRowContext(ctx, "SELECT source FROM forward_test WHERE id = 1").Scan(&source)
	if err != nil {
		t.Fatalf("follower read: %v", err)
	}
	if source != "from-follower" {
		t.Fatalf("expected 'from-follower', got %q", source)
	}
}

func TestMultiNode_SnapshotRestore(t *testing.T) {
	// This test verifies that the FSM snapshot/restore path works correctly.
	// It forces a Raft snapshot on the leader and then joins a follower which
	// must receive the snapshot to catch up (since old logs are compacted).

	// Bootstrap leader with very low snapshot threshold.
	leader := testNode(t, func(cfg *Config) {
		cfg.SnapshotThreshold = 4
		cfg.SnapshotInterval = 1 * time.Second
	})
	leaderAddr := leader.config.Bind
	time.Sleep(500 * time.Millisecond)

	// Write enough data to trigger at least one snapshot.
	db := leader.DB()
	_, err := db.Exec("CREATE TABLE snap_test (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	for i := 0; i < 20; i++ {
		_, err = db.Exec("INSERT INTO snap_test (val) VALUES (?)", fmt.Sprintf("row-%d", i))
		if err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	// Wait for Raft to take a snapshot and compact logs.
	time.Sleep(3 * time.Second)

	// Verify the leader has data.
	var count int
	err = db.QueryRow("SELECT count(*) FROM snap_test").Scan(&count)
	if err != nil {
		t.Fatalf("leader count: %v", err)
	}
	if count != 20 {
		t.Fatalf("leader: expected 20 rows, got %d", count)
	}

	// Now join a follower. Since old logs may be compacted, Raft should send
	// an InstallSnapshot to bring the follower up to date.
	follower := testJoinNode(t, leaderAddr)

	// Wait for snapshot transfer and replication.
	time.Sleep(3 * time.Second)

	// Read from follower.
	fdb := follower.DB()
	ctx := WithConsistency(context.Background(), ConsistencyNone)
	var fCount int
	err = fdb.QueryRowContext(ctx, "SELECT count(*) FROM snap_test").Scan(&fCount)
	if err != nil {
		t.Fatalf("follower count: %v", err)
	}
	if fCount != 20 {
		t.Fatalf("follower: expected 20 rows, got %d", fCount)
	}
}

// --- Backup Tests ---

func TestBackup_LocalBackendSnapshotAndRestore(t *testing.T) {
	backupDir := filepath.Join(t.TempDir(), "backups")
	backend, err := NewLocalBackend(backupDir)
	if err != nil {
		t.Fatalf("create backend: %v", err)
	}

	// Create node with backup enabled.
	node := testNode(t, func(cfg *Config) {
		cfg.Backup = &BackupConfig{
			Backend:          backend,
			SyncInterval:     200 * time.Millisecond,
			SnapshotInterval: 10 * time.Minute, // won't trigger during test
		}
	})

	db := node.DB()

	// Create table and insert data.
	_, err = db.Exec("CREATE TABLE backup_test (id INTEGER PRIMARY KEY, data TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	for i := 0; i < 20; i++ {
		_, err = db.Exec("INSERT INTO backup_test (data) VALUES (?)", fmt.Sprintf("row-%d", i))
		if err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	// Wait for WAL sync to happen.
	time.Sleep(1 * time.Second)

	// Verify a generation exists.
	gens, err := backend.Generations(context.Background())
	if err != nil {
		t.Fatalf("list generations: %v", err)
	}
	if len(gens) == 0 {
		t.Fatal("expected at least 1 generation")
	}

	// Close the original node.
	node.Close()

	// Restore to a new directory.
	restoreDir := t.TempDir()
	if err := Restore(context.Background(), backend, restoreDir); err != nil {
		t.Fatalf("restore: %v", err)
	}

	// Verify restored database has the data.
	restoredDBPath := filepath.Join(restoreDir, "default.db")
	restoredDB, err := sql.Open("sqlite", fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)", restoredDBPath))
	if err != nil {
		t.Fatalf("open restored db: %v", err)
	}
	defer restoredDB.Close()

	var count int
	err = restoredDB.QueryRow("SELECT count(*) FROM backup_test").Scan(&count)
	if err != nil {
		t.Fatalf("count restored: %v", err)
	}
	if count != 20 {
		t.Fatalf("expected 20 rows in restored db, got %d", count)
	}
}

func TestBackup_RestoreAndBootstrapNewNode(t *testing.T) {
	backupDir := filepath.Join(t.TempDir(), "backups")
	backend, err := NewLocalBackend(backupDir)
	if err != nil {
		t.Fatalf("create backend: %v", err)
	}

	// Create original node with data.
	node := testNode(t, func(cfg *Config) {
		cfg.Backup = &BackupConfig{
			Backend:          backend,
			SyncInterval:     200 * time.Millisecond,
			SnapshotInterval: 10 * time.Minute,
		}
	})

	db := node.DB()
	_, err = db.Exec("CREATE TABLE restore_test (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	_, err = db.Exec("INSERT INTO restore_test (val) VALUES (?)", "original-data")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Wait for backup.
	time.Sleep(1 * time.Second)
	node.Close()

	// Restore to new location and bootstrap a new node from it.
	newDataDir := t.TempDir()
	if err := Restore(context.Background(), backend, newDataDir); err != nil {
		t.Fatalf("restore: %v", err)
	}

	port := freePort(t)
	newNode, err := New(Config{
		NodeID:           fmt.Sprintf("restored-%d", port),
		DataDir:          newDataDir,
		Bind:             fmt.Sprintf("127.0.0.1:%d", port),
		Bootstrap:        true,
		HeartbeatTimeout: 200 * time.Millisecond,
		ElectionTimeout:  200 * time.Millisecond,
		ApplyTimeout:     5 * time.Second,
	})
	if err != nil {
		t.Fatalf("create restored node: %v", err)
	}
	defer newNode.Close()

	if err := newNode.WaitForLeader(5 * time.Second); err != nil {
		t.Fatalf("wait for leader: %v", err)
	}

	// Verify data is there.
	time.Sleep(200 * time.Millisecond)
	var val string
	err = newNode.DB().QueryRow("SELECT val FROM restore_test WHERE id = 1").Scan(&val)
	if err != nil {
		t.Fatalf("query restored node: %v", err)
	}
	if val != "original-data" {
		t.Fatalf("expected 'original-data', got %q", val)
	}
}

// --- Node Status Tests ---

func TestNode_IsLeader(t *testing.T) {
	node := testNode(t)
	if !node.IsLeader() {
		t.Fatal("single bootstrapped node should be leader")
	}
}

func TestNode_Stats(t *testing.T) {
	node := testNode(t)
	stats := node.Stats()
	if stats["state"] != "Leader" {
		t.Fatalf("expected state 'Leader', got %q", stats["state"])
	}
}

func TestNode_ClusterInfo(t *testing.T) {
	node := testNode(t)
	servers, err := node.Nodes()
	if err != nil {
		t.Fatalf("get nodes: %v", err)
	}
	if len(servers) != 1 {
		t.Fatalf("expected 1 server, got %d", len(servers))
	}
	if string(servers[0].ID) != node.NodeID() {
		t.Fatalf("expected node ID %q, got %q", node.NodeID(), servers[0].ID)
	}
}

// --- Multi-Database Tests ---

func TestMultiDB_DifferentConsistency(t *testing.T) {
	node := testNode(t)

	// Open two databases with different consistency levels.
	mainDB := node.OpenDB("main", ConsistencyWeak)
	logsDB := node.OpenDB("logs", ConsistencyNone)

	// Create tables in each database.
	_, err := mainDB.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("create users table: %v", err)
	}
	_, err = logsDB.Exec("CREATE TABLE events (id INTEGER PRIMARY KEY, msg TEXT)")
	if err != nil {
		t.Fatalf("create events table: %v", err)
	}

	// Insert into each.
	_, err = mainDB.Exec("INSERT INTO users (name) VALUES (?)", "alice")
	if err != nil {
		t.Fatalf("insert user: %v", err)
	}
	_, err = logsDB.Exec("INSERT INTO events (msg) VALUES (?)", "login")
	if err != nil {
		t.Fatalf("insert event: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Verify data is isolated: users only in mainDB, events only in logsDB.
	var name string
	err = mainDB.QueryRow("SELECT name FROM users WHERE id = 1").Scan(&name)
	if err != nil {
		t.Fatalf("select user: %v", err)
	}
	if name != "alice" {
		t.Fatalf("expected 'alice', got %q", name)
	}

	var msg string
	err = logsDB.QueryRow("SELECT msg FROM events WHERE id = 1").Scan(&msg)
	if err != nil {
		t.Fatalf("select event: %v", err)
	}
	if msg != "login" {
		t.Fatalf("expected 'login', got %q", msg)
	}

	// Verify tables do NOT exist across databases.
	var count int
	err = mainDB.QueryRow("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='events'").Scan(&count)
	if err != nil {
		t.Fatalf("check events in mainDB: %v", err)
	}
	if count != 0 {
		t.Fatal("events table should not exist in mainDB")
	}

	err = logsDB.QueryRow("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='users'").Scan(&count)
	if err != nil {
		t.Fatalf("check users in logsDB: %v", err)
	}
	if count != 0 {
		t.Fatal("users table should not exist in logsDB")
	}

	// Verify node.DB() still works (backward compatibility).
	defaultDB := node.DB()
	_, err = defaultDB.Exec("CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("create kv table on default: %v", err)
	}
	_, err = defaultDB.Exec("INSERT INTO kv (key, value) VALUES (?, ?)", "test", "ok")
	if err != nil {
		t.Fatalf("insert kv on default: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	var val string
	err = defaultDB.QueryRow("SELECT value FROM kv WHERE key = ?", "test").Scan(&val)
	if err != nil {
		t.Fatalf("select kv from default: %v", err)
	}
	if val != "ok" {
		t.Fatalf("expected 'ok', got %q", val)
	}
}

func TestMultiDB_ThreeNodeReplication(t *testing.T) {
	// Bootstrap leader.
	leader := testNode(t)
	leaderAddr := leader.config.Bind
	time.Sleep(500 * time.Millisecond)

	// Join a follower.
	follower := testJoinNode(t, leaderAddr)
	time.Sleep(1 * time.Second)

	// Write to two different databases on the leader.
	mainDB := leader.OpenDB("main", ConsistencyWeak)
	logsDB := leader.OpenDB("logs", ConsistencyNone)

	_, err := mainDB.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("create users: %v", err)
	}
	_, err = logsDB.Exec("CREATE TABLE events (id INTEGER PRIMARY KEY, msg TEXT)")
	if err != nil {
		t.Fatalf("create events: %v", err)
	}
	_, err = mainDB.Exec("INSERT INTO users (name) VALUES (?)", "bob")
	if err != nil {
		t.Fatalf("insert user: %v", err)
	}
	_, err = logsDB.Exec("INSERT INTO events (msg) VALUES (?)", "signup")
	if err != nil {
		t.Fatalf("insert event: %v", err)
	}

	// Wait for replication.
	time.Sleep(500 * time.Millisecond)

	// Read from follower using ConsistencyNone (local reads).
	fMainDB := follower.OpenDB("main", ConsistencyNone)
	fLogsDB := follower.OpenDB("logs", ConsistencyNone)

	ctx := WithConsistency(context.Background(), ConsistencyNone)

	var name string
	err = fMainDB.QueryRowContext(ctx, "SELECT name FROM users WHERE id = 1").Scan(&name)
	if err != nil {
		t.Fatalf("follower read user: %v", err)
	}
	if name != "bob" {
		t.Fatalf("expected 'bob', got %q", name)
	}

	var msg string
	err = fLogsDB.QueryRowContext(ctx, "SELECT msg FROM events WHERE id = 1").Scan(&msg)
	if err != nil {
		t.Fatalf("follower read event: %v", err)
	}
	if msg != "signup" {
		t.Fatalf("expected 'signup', got %q", msg)
	}
}

// --- Config Validation Tests ---

func TestConfig_EmptyNodeID(t *testing.T) {
	_, err := New(Config{
		DataDir:   t.TempDir(),
		Bind:      "127.0.0.1:9999",
		Bootstrap: true,
		LogOutput: io.Discard,
	})
	if err == nil {
		t.Fatal("expected error for empty NodeID")
	}
}

func TestConfig_EmptyDataDir(t *testing.T) {
	_, err := New(Config{
		NodeID:    "test",
		Bind:      "127.0.0.1:9999",
		Bootstrap: true,
		LogOutput: io.Discard,
	})
	if err == nil {
		t.Fatal("expected error for empty DataDir")
	}
}

func TestConfig_EmptyBind(t *testing.T) {
	_, err := New(Config{
		NodeID:    "test",
		DataDir:   t.TempDir(),
		Bootstrap: true,
		LogOutput: io.Discard,
	})
	if err == nil {
		t.Fatal("expected error for empty Bind")
	}
}

func TestConfig_InvalidBind(t *testing.T) {
	_, err := New(Config{
		NodeID:    "test",
		DataDir:   t.TempDir(),
		Bind:      "not-a-valid-address",
		Bootstrap: true,
		LogOutput: io.Discard,
	})
	if err == nil {
		t.Fatal("expected error for invalid Bind")
	}
}

func TestConfig_BootstrapAndJoin(t *testing.T) {
	_, err := New(Config{
		NodeID:    "test",
		DataDir:   t.TempDir(),
		Bind:      "127.0.0.1:9999",
		Bootstrap: true,
		Join:      []string{"127.0.0.1:9000"},
		LogOutput: io.Discard,
	})
	if err == nil {
		t.Fatal("expected error for Bootstrap+Join")
	}
}

func TestConfig_NeitherBootstrapNorJoin(t *testing.T) {
	_, err := New(Config{
		NodeID:    "test",
		DataDir:   t.TempDir(),
		Bind:      "127.0.0.1:9999",
		LogOutput: io.Discard,
	})
	if err == nil {
		t.Fatal("expected error when neither Bootstrap nor Join is set")
	}
}

// --- Driver Prepare + Stmt Tests ---

func TestDriver_PrepareExec(t *testing.T) {
	node := testNode(t)
	db := node.DB()

	_, err := db.Exec("CREATE TABLE prep_test (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Use Prepare+Exec path.
	stmt, err := db.Prepare("INSERT INTO prep_test (val) VALUES (?)")
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	defer stmt.Close()

	result, err := stmt.Exec("hello")
	if err != nil {
		t.Fatalf("stmt exec: %v", err)
	}
	lastID, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("last insert id: %v", err)
	}
	if lastID != 1 {
		t.Fatalf("expected lastID 1, got %d", lastID)
	}

	time.Sleep(100 * time.Millisecond)

	var val string
	err = db.QueryRow("SELECT val FROM prep_test WHERE id = 1").Scan(&val)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if val != "hello" {
		t.Fatalf("expected 'hello', got %q", val)
	}
}

func TestDriver_PrepareQuery(t *testing.T) {
	node := testNode(t)
	db := node.DB()

	_, err := db.Exec("CREATE TABLE prep_q_test (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	_, err = db.Exec("INSERT INTO prep_q_test (val) VALUES (?)", "world")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	// Use Prepare+Query path.
	stmt, err := db.Prepare("SELECT val FROM prep_q_test WHERE id = ?")
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	defer stmt.Close()

	var val string
	err = stmt.QueryRow(1).Scan(&val)
	if err != nil {
		t.Fatalf("stmt query: %v", err)
	}
	if val != "world" {
		t.Fatalf("expected 'world', got %q", val)
	}
}

// --- Driver Open Error Test ---

func TestDriver_OpenReturnsError(t *testing.T) {
	d := &colmenaDriver{}
	_, err := d.Open("anything")
	if err == nil {
		t.Fatal("expected error from Open")
	}
}

// --- Connector Driver Test ---

func TestConnector_Driver(t *testing.T) {
	c := &colmenaConnector{}
	d := c.Driver()
	if d == nil {
		t.Fatal("expected non-nil driver")
	}
}

// --- BeginTx ReadOnly Error Test ---

func TestDriver_BeginTxReadOnly(t *testing.T) {
	node := testNode(t)
	db := node.DB()

	// database/sql doesn't expose BeginTx with ReadOnly directly through the
	// standard interface in a way that triggers the error, so test via connector.
	conn := &colmenaConn{node: node, dbName: "default", consistency: ConsistencyWeak}
	_, err := conn.BeginTx(context.Background(), driver.TxOptions{ReadOnly: true})
	if err == nil {
		t.Fatal("expected error for read-only transaction")
	}
	_ = db // keep db reference
}

// --- rpcRows Test ---

func TestRpcRows(t *testing.T) {
	// Build rpcRows manually and verify iteration.
	row1 := []json.RawMessage{
		json.RawMessage(`"col_a"`),
		json.RawMessage(`42`),
	}
	row2 := []json.RawMessage{
		json.RawMessage(`"col_b"`),
		json.RawMessage(`99`),
	}
	rows := &rpcRows{
		columns: []string{"name", "num"},
		legacy:  [][]json.RawMessage{row1, row2},
	}

	if cols := rows.Columns(); len(cols) != 2 || cols[0] != "name" || cols[1] != "num" {
		t.Fatalf("unexpected columns: %v", cols)
	}

	// First row.
	dest := make([]driver.Value, 2)
	err := rows.Next(dest)
	if err != nil {
		t.Fatalf("next row 1: %v", err)
	}
	if dest[0] != "col_a" {
		t.Fatalf("expected 'col_a', got %v", dest[0])
	}

	// Second row.
	err = rows.Next(dest)
	if err != nil {
		t.Fatalf("next row 2: %v", err)
	}

	// EOF.
	err = rows.Next(dest)
	if err != io.EOF {
		t.Fatalf("expected io.EOF, got %v", err)
	}

	// Close should return nil.
	if err := rows.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// --- Follower Query with Weak and Strong Consistency ---

func TestFollower_WeakConsistencyQuery(t *testing.T) {
	leader := testNode(t)
	leaderAddr := leader.config.Bind
	time.Sleep(500 * time.Millisecond)

	follower := testJoinNode(t, leaderAddr)
	time.Sleep(1 * time.Second)

	// Write on leader.
	_, err := leader.DB().Exec("CREATE TABLE weak_test (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	_, err = leader.DB().Exec("INSERT INTO weak_test (val) VALUES (?)", "weak-val")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	time.Sleep(500 * time.Millisecond)

	// Query from follower with Weak consistency (forwards to leader).
	fdb := follower.OpenDB("default", ConsistencyWeak)
	ctx := WithConsistency(context.Background(), ConsistencyWeak)
	var val string
	err = fdb.QueryRowContext(ctx, "SELECT val FROM weak_test WHERE id = 1").Scan(&val)
	if err != nil {
		t.Fatalf("weak read from follower: %v", err)
	}
	if val != "weak-val" {
		t.Fatalf("expected 'weak-val', got %q", val)
	}
}

func TestFollower_StrongConsistencyQuery(t *testing.T) {
	leader := testNode(t)
	leaderAddr := leader.config.Bind
	time.Sleep(500 * time.Millisecond)

	follower := testJoinNode(t, leaderAddr)
	time.Sleep(1 * time.Second)

	// Write on leader.
	_, err := leader.DB().Exec("CREATE TABLE strong_test (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	_, err = leader.DB().Exec("INSERT INTO strong_test (val) VALUES (?)", "strong-val")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	time.Sleep(500 * time.Millisecond)

	// Query from follower with Strong consistency (forwards to leader).
	fdb := follower.OpenDB("default", ConsistencyStrong)
	ctx := WithConsistency(context.Background(), ConsistencyStrong)
	var val string
	err = fdb.QueryRowContext(ctx, "SELECT val FROM strong_test WHERE id = 1").Scan(&val)
	if err != nil {
		t.Fatalf("strong read from follower: %v", err)
	}
	if val != "strong-val" {
		t.Fatalf("expected 'strong-val', got %q", val)
	}
}

// --- RemoveNode Test ---

func TestMultiNode_RemoveNode(t *testing.T) {
	leader := testNode(t)
	leaderAddr := leader.config.Bind
	time.Sleep(500 * time.Millisecond)

	follower1 := testJoinNode(t, leaderAddr)
	follower2 := testJoinNode(t, leaderAddr)
	time.Sleep(1 * time.Second)

	// Verify 3 nodes.
	servers, err := leader.Nodes()
	if err != nil {
		t.Fatalf("get nodes: %v", err)
	}
	if len(servers) != 3 {
		t.Fatalf("expected 3 servers, got %d", len(servers))
	}

	// Remove follower2.
	err = leader.RemoveNode(follower2.NodeID())
	if err != nil {
		t.Fatalf("remove node: %v", err)
	}
	time.Sleep(500 * time.Millisecond)

	// Verify 2 nodes remain.
	servers, err = leader.Nodes()
	if err != nil {
		t.Fatalf("get nodes after remove: %v", err)
	}
	if len(servers) != 2 {
		t.Fatalf("expected 2 servers after removal, got %d", len(servers))
	}

	// Cluster should still work: write and read.
	_, err = leader.DB().Exec("CREATE TABLE remove_test (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	_, err = leader.DB().Exec("INSERT INTO remove_test (val) VALUES (?)", "still-works")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	time.Sleep(500 * time.Millisecond)

	ctx := WithConsistency(context.Background(), ConsistencyNone)
	var val string
	err = follower1.DB().QueryRowContext(ctx, "SELECT val FROM remove_test WHERE id = 1").Scan(&val)
	if err != nil {
		t.Fatalf("read after removal: %v", err)
	}
	if val != "still-works" {
		t.Fatalf("expected 'still-works', got %q", val)
	}
}

// --- LeaderAddr Test ---

func TestNode_LeaderAddr(t *testing.T) {
	node := testNode(t)
	// On a single-node bootstrap cluster, LeaderAddr should return the node's address.
	addr := node.LeaderAddr()
	if addr == "" {
		t.Fatal("expected non-empty leader addr")
	}
}

// --- FSM Error Paths ---

func TestFSM_UnknownCommandType(t *testing.T) {
	node := testNode(t)

	// Build a command with an unknown type and apply it directly via Raft.
	cmd := &Command{
		Type:       CommandType(99),
		DB:         "default",
		Statements: []Statement{{SQL: "SELECT 1"}},
	}
	data, err := marshalCommand(cmd)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	future := node.raft.Apply(data, 5*time.Second)
	if err := future.Error(); err != nil {
		t.Fatalf("raft apply: %v", err)
	}
	resp, ok := future.Response().(*ApplyResult)
	if !ok {
		t.Fatal("unexpected response type")
	}
	if resp.Error == "" {
		t.Fatal("expected error for unknown command type")
	}
}

func TestFSM_InvalidCommandData(t *testing.T) {
	node := testNode(t)

	// Apply invalid JSON data via Raft to trigger unmarshal error.
	future := node.raft.Apply([]byte("{invalid json"), 5*time.Second)
	if err := future.Error(); err != nil {
		t.Fatalf("raft apply: %v", err)
	}
	resp, ok := future.Response().(*ApplyResult)
	if !ok {
		t.Fatal("unexpected response type")
	}
	if resp.Error == "" {
		t.Fatal("expected error for invalid command data")
	}
}

// --- Store Restore Test ---

func TestStore_SnapshotAndRestore(t *testing.T) {
	dir := t.TempDir()
	s, err := newStoreAt(filepath.Join(dir, "test.db"), 2)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	defer s.close()

	// Create table and insert data.
	_, err = s.execute(Statement{SQL: "CREATE TABLE sr_test (id INTEGER PRIMARY KEY, val TEXT)"})
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	for i := 0; i < 5; i++ {
		_, err = s.execute(Statement{SQL: "INSERT INTO sr_test (val) VALUES (?)", Args: []any{fmt.Sprintf("v%d", i)}})
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	// Take a snapshot.
	var buf bytes.Buffer
	if err := s.snapshot(&buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	// Restore from the snapshot (this replaces the DB in-place).
	if err := s.restore(&buf); err != nil {
		t.Fatalf("restore: %v", err)
	}

	// Verify data is intact after restore.
	rows, err := s.query("SELECT count(*) FROM sr_test")
	if err != nil {
		t.Fatalf("query after restore: %v", err)
	}
	defer rows.Close()
	rows.Next()
	var count int
	if err := rows.Scan(&count); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if count != 5 {
		t.Fatalf("expected 5 rows after restore, got %d", count)
	}
}

// --- StoreManager Restore Test ---

func TestStoreManager_SnapshotAndRestore(t *testing.T) {
	dir1 := t.TempDir()
	sm1 := newStoreManager(dir1, 2)
	defer sm1.close()

	// Create two stores and add data.
	s1, err := sm1.get("db1")
	if err != nil {
		t.Fatalf("get db1: %v", err)
	}
	_, err = s1.execute(Statement{SQL: "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)"})
	if err != nil {
		t.Fatalf("create t1: %v", err)
	}
	_, err = s1.execute(Statement{SQL: "INSERT INTO t1 (val) VALUES (?)", Args: []any{"hello"}})
	if err != nil {
		t.Fatalf("insert t1: %v", err)
	}

	s2, err := sm1.get("db2")
	if err != nil {
		t.Fatalf("get db2: %v", err)
	}
	_, err = s2.execute(Statement{SQL: "CREATE TABLE t2 (id INTEGER PRIMARY KEY, num INTEGER)"})
	if err != nil {
		t.Fatalf("create t2: %v", err)
	}
	_, err = s2.execute(Statement{SQL: "INSERT INTO t2 (num) VALUES (?)", Args: []any{42}})
	if err != nil {
		t.Fatalf("insert t2: %v", err)
	}

	// Snapshot.
	var buf bytes.Buffer
	if err := sm1.snapshot(&buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	// Create a new store manager and restore into it.
	dir2 := t.TempDir()
	sm2 := newStoreManager(dir2, 2)
	defer sm2.close()

	if err := sm2.restore(&buf); err != nil {
		t.Fatalf("restore: %v", err)
	}

	// Verify both databases were restored.
	rs1, err := sm2.get("db1")
	if err != nil {
		t.Fatalf("get restored db1: %v", err)
	}
	rows, err := rs1.query("SELECT val FROM t1 WHERE id = 1")
	if err != nil {
		t.Fatalf("query db1: %v", err)
	}
	defer rows.Close()
	rows.Next()
	var val string
	if err := rows.Scan(&val); err != nil {
		t.Fatalf("scan db1: %v", err)
	}
	if val != "hello" {
		t.Fatalf("expected 'hello', got %q", val)
	}

	rs2, err := sm2.get("db2")
	if err != nil {
		t.Fatalf("get restored db2: %v", err)
	}
	rows2, err := rs2.query("SELECT num FROM t2 WHERE id = 1")
	if err != nil {
		t.Fatalf("query db2: %v", err)
	}
	defer rows2.Close()
	rows2.Next()
	var num int
	if err := rows2.Scan(&num); err != nil {
		t.Fatalf("scan db2: %v", err)
	}
	if num != 42 {
		t.Fatalf("expected 42, got %d", num)
	}
}

// --- OnApply Callback Test ---

func TestNode_OnApplyCallback(t *testing.T) {
	var callbackDB string
	var callbackStmts []Statement
	var callbackResults []ExecResult

	node := testNode(t, func(cfg *Config) {
		cfg.OnApply = func(db string, statements []Statement, results []ExecResult) {
			callbackDB = db
			callbackStmts = statements
			callbackResults = results
		}
	})

	db := node.DB()
	_, err := db.Exec("CREATE TABLE callback_test (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	_, err = db.Exec("INSERT INTO callback_test (val) VALUES (?)", "cb-val")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	if callbackDB != "default" {
		t.Fatalf("expected callback db 'default', got %q", callbackDB)
	}
	if len(callbackStmts) == 0 {
		t.Fatal("expected callback stmts to be non-empty")
	}
	if len(callbackResults) == 0 {
		t.Fatal("expected callback results to be non-empty")
	}
}

// --- Follower Forward Query (leaderQuery + forwardQuery) ---

func TestFollower_ForwardQuery(t *testing.T) {
	leader := testNode(t)
	leaderAddr := leader.config.Bind
	time.Sleep(500 * time.Millisecond)

	follower := testJoinNode(t, leaderAddr)
	time.Sleep(1 * time.Second)

	// Write on leader.
	_, err := leader.DB().Exec("CREATE TABLE fwd_q_test (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	_, err = leader.DB().Exec("INSERT INTO fwd_q_test (val) VALUES (?)", "forwarded-data")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	time.Sleep(500 * time.Millisecond)

	// Use the internal forwardQuery to test the RPC query path directly.
	resp, err := follower.forwardQuery("default", "SELECT val FROM fwd_q_test WHERE id = 1", nil, ConsistencyWeak)
	if err != nil {
		t.Fatalf("forward query: %v", err)
	}
	if len(resp.Columns) == 0 {
		t.Fatal("expected columns in response")
	}
	if len(resp.TaggedRows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(resp.TaggedRows))
	}
}

// --- RPCService Query Handler Direct Test ---

func TestRPCService_QueryHandler(t *testing.T) {
	node := testNode(t)
	db := node.DB()

	_, err := db.Exec("CREATE TABLE rpc_q_test (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	_, err = db.Exec("INSERT INTO rpc_q_test (val) VALUES (?)", "rpc-val")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	svc := &RPCService{node: node}
	req := &RPCQueryRequest{
		DB:  "default",
		SQL: "SELECT val FROM rpc_q_test WHERE id = 1",
	}
	var resp RPCQueryResponse
	err = svc.Query(req, &resp)
	if err != nil {
		t.Fatalf("RPC query: %v", err)
	}
	if resp.Error != "" {
		t.Fatalf("RPC query error: %s", resp.Error)
	}
	if len(resp.Columns) == 0 {
		t.Fatal("expected columns")
	}
	if len(resp.TaggedRows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(resp.TaggedRows))
	}
}

func TestRPCService_QueryEmptyDB(t *testing.T) {
	// Test that RPCService.Query defaults to "default" db when DB is empty.
	node := testNode(t)
	db := node.DB()

	_, err := db.Exec("CREATE TABLE rpc_empty_test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	svc := &RPCService{node: node}
	req := &RPCQueryRequest{
		DB:  "", // empty, should default to "default"
		SQL: "SELECT count(*) FROM rpc_empty_test",
	}
	var resp RPCQueryResponse
	err = svc.Query(req, &resp)
	if err != nil {
		t.Fatalf("RPC query: %v", err)
	}
	if resp.Error != "" {
		t.Fatalf("RPC query error: %s", resp.Error)
	}
}

// --- valuesToNamed Test ---

func TestValuesToNamed(t *testing.T) {
	vals := []driver.Value{"hello", int64(42), 3.14}
	named := valuesToNamed(vals)
	if len(named) != 3 {
		t.Fatalf("expected 3, got %d", len(named))
	}
	if named[0].Ordinal != 1 || named[0].Value != "hello" {
		t.Fatalf("unexpected first: %+v", named[0])
	}
	if named[2].Ordinal != 3 {
		t.Fatalf("unexpected ordinal: %d", named[2].Ordinal)
	}
}

// --- Backup Restore Function Test ---

func TestBackup_RestoreNoGenerations(t *testing.T) {
	backupDir := filepath.Join(t.TempDir(), "empty-backups")
	backend, err := NewLocalBackend(backupDir)
	if err != nil {
		t.Fatalf("create backend: %v", err)
	}

	restoreDir := t.TempDir()
	err = Restore(context.Background(), backend, restoreDir)
	if err == nil {
		t.Fatal("expected error when no backups exist")
	}
}

// --- Backup WAL Sync With No WAL File ---

func TestBackup_SyncWALNoFile(t *testing.T) {
	backupDir := filepath.Join(t.TempDir(), "backups")
	backend, err := NewLocalBackend(backupDir)
	if err != nil {
		t.Fatalf("create backend: %v", err)
	}

	node := testNode(t, func(cfg *Config) {
		cfg.Backup = &BackupConfig{
			Backend:          backend,
			SyncInterval:     10 * time.Minute, // don't auto-sync
			SnapshotInterval: 10 * time.Minute,
		}
	})

	// After snapshot, the WAL is truncated. Force a checkpoint to clear WAL.
	_, _ = node.stores.stores["default"].writer.Exec("PRAGMA wal_checkpoint(TRUNCATE)")

	// Remove WAL file to simulate no-WAL scenario.
	st, _ := node.stores.get("default")
	os.Remove(st.dbPath + "-wal")

	// syncWAL should handle missing WAL gracefully.
	err = node.backup.syncWAL()
	if err != nil {
		t.Fatalf("syncWAL with no WAL file should not error: %v", err)
	}
}

// --- DefaultStore Test ---

func TestStoreManager_DefaultStore(t *testing.T) {
	dir := t.TempDir()
	sm := newStoreManager(dir, 2)
	defer sm.close()

	s, err := sm.defaultStore()
	if err != nil {
		t.Fatalf("defaultStore: %v", err)
	}
	if s == nil {
		t.Fatal("expected non-nil store")
	}
}

// --- NewStore Test ---

func TestNewStore(t *testing.T) {
	dir := t.TempDir()
	s, err := newStore(dir, 2)
	if err != nil {
		t.Fatalf("newStore: %v", err)
	}
	defer s.close()

	// Verify the store works.
	_, err = s.execute(Statement{SQL: "CREATE TABLE ns_test (id INTEGER PRIMARY KEY)"})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
}

// --- colmenaStmt Close and NumInput ---

func TestStmt_CloseAndNumInput(t *testing.T) {
	s := &colmenaStmt{conn: nil, query: "SELECT 1"}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if n := s.NumInput(); n != -1 {
		t.Fatalf("expected -1, got %d", n)
	}
}

// --- LocalBackend Close Test ---

func TestLocalBackend_Close(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "backups")
	backend, err := NewLocalBackend(dir)
	if err != nil {
		t.Fatalf("create backend: %v", err)
	}
	if err := backend.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// --- Backup with SnapshotInterval to trigger applyDefaults fully ---

func TestBackupConfig_ApplyDefaults(t *testing.T) {
	cfg := BackupConfig{}
	cfg.applyDefaults()
	if cfg.SyncInterval != 1*time.Second {
		t.Fatalf("expected SyncInterval 1s, got %v", cfg.SyncInterval)
	}
	if cfg.SnapshotInterval != 1*time.Hour {
		t.Fatalf("expected SnapshotInterval 1h, got %v", cfg.SnapshotInterval)
	}

	// Test with values already set.
	cfg2 := BackupConfig{
		SyncInterval:     5 * time.Second,
		SnapshotInterval: 30 * time.Minute,
	}
	cfg2.applyDefaults()
	if cfg2.SyncInterval != 5*time.Second {
		t.Fatalf("expected SyncInterval 5s, got %v", cfg2.SyncInterval)
	}
	if cfg2.SnapshotInterval != 30*time.Minute {
		t.Fatalf("expected SnapshotInterval 30m, got %v", cfg2.SnapshotInterval)
	}
}

// --- FSM Restore via storeManager restore ---

func TestFSM_Restore(t *testing.T) {
	dir := t.TempDir()
	sm := newStoreManager(dir, 2)

	// Create a store with data.
	s, err := sm.get("default")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	_, err = s.execute(Statement{SQL: "CREATE TABLE fsm_r_test (id INTEGER PRIMARY KEY, val TEXT)"})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, err = s.execute(Statement{SQL: "INSERT INTO fsm_r_test (val) VALUES (?)", Args: []any{"fsm-val"}})
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Snapshot.
	var buf bytes.Buffer
	if err := sm.snapshot(&buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	sm.close()

	// Create a fresh FSM and restore.
	dir2 := t.TempDir()
	sm2 := newStoreManager(dir2, 2)
	f := &fsm{stores: sm2}
	rc := io.NopCloser(&buf)
	if err := f.Restore(rc); err != nil {
		t.Fatalf("fsm restore: %v", err)
	}

	// Verify data after restore.
	s2, err := sm2.get("default")
	if err != nil {
		t.Fatalf("get after restore: %v", err)
	}
	rows, err := s2.query("SELECT val FROM fsm_r_test WHERE id = 1")
	if err != nil {
		t.Fatalf("query after restore: %v", err)
	}
	defer rows.Close()
	rows.Next()
	var val string
	if err := rows.Scan(&val); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if val != "fsm-val" {
		t.Fatalf("expected 'fsm-val', got %q", val)
	}
	sm2.close()
}

// --- fsmSnapshot Release Test ---

func TestFSMSnapshot_Release(t *testing.T) {
	// Just verify Release doesn't panic.
	snap := &fsmSnapshot{stores: nil}
	snap.Release()
}

// --- Exec with empty result ---

func TestExecContext_EmptyResults(t *testing.T) {
	node := testNode(t)
	db := node.DB()

	// CREATE TABLE returns no rows affected in a meaningful way, but the path
	// through ExecContext should work.
	_, err := db.Exec("CREATE TABLE empty_res_test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("exec: %v", err)
	}
}

// --- Weak consistency on leader (should use local query) ---

func TestLeader_WeakConsistencyLocal(t *testing.T) {
	node := testNode(t)
	db := node.DB()

	_, err := db.Exec("CREATE TABLE weak_local (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, err = db.Exec("INSERT INTO weak_local (val) VALUES (?)", "local")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	ctx := WithConsistency(context.Background(), ConsistencyWeak)
	var val string
	err = db.QueryRowContext(ctx, "SELECT val FROM weak_local WHERE id = 1").Scan(&val)
	if err != nil {
		t.Fatalf("weak read: %v", err)
	}
	if val != "local" {
		t.Fatalf("expected 'local', got %q", val)
	}
}

// --- Follower multiple queries via Weak to exercise rpcRows fully ---

func TestFollower_WeakMultipleRows(t *testing.T) {
	leader := testNode(t)
	leaderAddr := leader.config.Bind
	time.Sleep(500 * time.Millisecond)

	follower := testJoinNode(t, leaderAddr)
	time.Sleep(1 * time.Second)

	// Write multiple rows on leader.
	_, err := leader.DB().Exec("CREATE TABLE multi_row (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	for i := 0; i < 5; i++ {
		_, err = leader.DB().Exec("INSERT INTO multi_row (val) VALUES (?)", fmt.Sprintf("row-%d", i))
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	time.Sleep(500 * time.Millisecond)

	// Query from follower with Weak consistency — exercises leaderQuery + rpcRows.
	fdb := follower.OpenDB("default", ConsistencyWeak)
	ctx := WithConsistency(context.Background(), ConsistencyWeak)
	rows, err := fdb.QueryContext(ctx, "SELECT id, val FROM multi_row ORDER BY id")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id int
		var val string
		if err := rows.Scan(&id, &val); err != nil {
			t.Fatalf("scan: %v", err)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}
	if count != 5 {
		t.Fatalf("expected 5 rows, got %d", count)
	}
}

// --- Backup SnapshotOnly restore (no WAL) ---

func TestBackup_RestoreSnapshotOnly(t *testing.T) {
	backupDir := filepath.Join(t.TempDir(), "backups")
	backend, err := NewLocalBackend(backupDir)
	if err != nil {
		t.Fatalf("create backend: %v", err)
	}

	// Create node with backup, using a short snapshot interval so a fresh
	// snapshot is taken after writes.
	node := testNode(t, func(cfg *Config) {
		cfg.Backup = &BackupConfig{
			Backend:          backend,
			SyncInterval:     200 * time.Millisecond,
			SnapshotInterval: 500 * time.Millisecond,
		}
	})

	db := node.DB()
	_, err = db.Exec("CREATE TABLE snap_only (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, err = db.Exec("INSERT INTO snap_only (val) VALUES (?)", "snap-val")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Wait for a new snapshot to be taken (includes the writes above).
	time.Sleep(2 * time.Second)
	node.Close()

	// Get the latest generation and remove the WAL file to simulate
	// a snapshot-only restore.
	gens, err := backend.Generations(context.Background())
	if err != nil {
		t.Fatalf("list gens: %v", err)
	}
	if len(gens) == 0 {
		t.Fatal("expected at least 1 generation")
	}
	walPath := filepath.Join(backupDir, gens[0].ID, "wal.db")
	os.Remove(walPath)

	restoreDir := t.TempDir()
	err = Restore(context.Background(), backend, restoreDir)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}

	// Verify restored DB.
	restoredDB, err := sql.Open("sqlite", fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)", filepath.Join(restoreDir, "default.db")))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer restoredDB.Close()

	var count int
	err = restoredDB.QueryRow("SELECT count(*) FROM snap_only").Scan(&count)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1, got %d", count)
	}
}

// --- Tx double commit / double rollback ---

func TestTx_DoubleCommit(t *testing.T) {
	node := testNode(t)
	db := node.DB()

	_, err := db.Exec("CREATE TABLE tx_dc (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	tx.Exec("INSERT INTO tx_dc (id) VALUES (?)", 1)
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	// Second commit should fail (already completed).
	if err := tx.Commit(); err == nil {
		t.Log("second commit returned nil (database/sql may handle this)")
	}
}

func TestTx_DoubleRollback(t *testing.T) {
	node := testNode(t)
	db := node.DB()

	_, err := db.Exec("CREATE TABLE tx_dr (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	tx.Exec("INSERT INTO tx_dr (id) VALUES (?)", 1)
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	// Second rollback should fail (already completed).
	if err := tx.Rollback(); err == nil {
		t.Log("second rollback returned nil (database/sql may handle this)")
	}
}

// --- Tx with empty stmts commits without error ---

func TestTx_EmptyCommit(t *testing.T) {
	node := testNode(t)
	db := node.DB()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	// Commit with no statements.
	if err := tx.Commit(); err != nil {
		t.Fatalf("empty commit: %v", err)
	}
}

// --- Config applyDefaults full coverage ---

func TestConfig_ApplyDefaults(t *testing.T) {
	cfg := Config{}
	cfg.applyDefaults()

	if cfg.Consistency != ConsistencyWeak {
		t.Fatalf("expected ConsistencyWeak, got %d", cfg.Consistency)
	}
	if cfg.HeartbeatTimeout != 1*time.Second {
		t.Fatalf("expected 1s HeartbeatTimeout, got %v", cfg.HeartbeatTimeout)
	}
	if cfg.ElectionTimeout != 1*time.Second {
		t.Fatalf("expected 1s ElectionTimeout, got %v", cfg.ElectionTimeout)
	}
	if cfg.SnapshotInterval != 2*time.Minute {
		t.Fatalf("expected 2m SnapshotInterval, got %v", cfg.SnapshotInterval)
	}
	if cfg.SnapshotThreshold != 1024 {
		t.Fatalf("expected 1024 SnapshotThreshold, got %d", cfg.SnapshotThreshold)
	}
	if cfg.ApplyTimeout != 10*time.Second {
		t.Fatalf("expected 10s ApplyTimeout, got %v", cfg.ApplyTimeout)
	}
	if cfg.MaxPool != 3 {
		t.Fatalf("expected 3 MaxPool, got %d", cfg.MaxPool)
	}
	if cfg.SQLiteReadConns != 4 {
		t.Fatalf("expected 4 SQLiteReadConns, got %d", cfg.SQLiteReadConns)
	}
	if cfg.LogOutput == nil {
		t.Fatal("expected non-nil LogOutput")
	}
}

// --- RPCService Execute Not Leader Error ---

func TestRPCService_ExecuteNotLeader(t *testing.T) {
	leader := testNode(t)
	leaderAddr := leader.config.Bind
	time.Sleep(500 * time.Millisecond)

	follower := testJoinNode(t, leaderAddr)
	time.Sleep(1 * time.Second)

	// Call Execute on follower's RPCService — it should return "not the leader".
	svc := &RPCService{node: follower}
	cmd := &Command{
		Type:       CommandExecute,
		DB:         "default",
		Statements: []Statement{{SQL: "INSERT INTO x (y) VALUES (?)", Args: []any{"z"}}},
	}
	data, err := marshalCommand(cmd)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	req := &RPCExecuteRequest{Command: data}
	var resp RPCExecuteResponse
	err = svc.Execute(req, &resp)
	if err != nil {
		t.Fatalf("RPC execute: %v", err)
	}
	if resp.Error == "" {
		t.Fatal("expected error for non-leader execute")
	}
}

// --- RPCService Join Not Leader Redirect ---

func TestRPCService_JoinNotLeader(t *testing.T) {
	leader := testNode(t)
	leaderAddr := leader.config.Bind
	time.Sleep(500 * time.Millisecond)

	follower := testJoinNode(t, leaderAddr)
	time.Sleep(1 * time.Second)

	// Call Join on follower's RPCService — should return "not the leader" + leader addr.
	svc := &RPCService{node: follower}
	req := &RPCJoinRequest{NodeID: "fake-node", Address: "127.0.0.1:29999"}
	var resp RPCJoinResponse
	err := svc.Join(req, &resp)
	if err != nil {
		t.Fatalf("RPC join: %v", err)
	}
	if resp.Error == "" {
		t.Fatal("expected error for non-leader join")
	}
	if resp.LeaderAddr == "" {
		t.Fatal("expected non-empty leader addr")
	}
}

// --- RPCService Query with SQL Error ---

func TestRPCService_QueryWithSQLError(t *testing.T) {
	node := testNode(t)

	svc := &RPCService{node: node}
	req := &RPCQueryRequest{
		DB:  "default",
		SQL: "SELECT * FROM nonexistent_table",
	}
	var resp RPCQueryResponse
	err := svc.Query(req, &resp)
	if err != nil {
		t.Fatalf("RPC query: %v", err)
	}
	if resp.Error == "" {
		t.Fatal("expected error for query on nonexistent table")
	}
}

// --- Direct conn.Begin() test (not BeginTx) ---

func TestConn_Begin(t *testing.T) {
	node := testNode(t)
	conn := &colmenaConn{node: node, dbName: "default", consistency: ConsistencyWeak}
	tx, err := conn.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	// Rollback the transaction.
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}
}

// --- Follower write forwarding to exercise forwardExecute paths ---

func TestFollower_WriteForwardingMultiNode(t *testing.T) {
	leader := testNode(t)
	leaderAddr := leader.config.Bind
	time.Sleep(500 * time.Millisecond)

	follower := testJoinNode(t, leaderAddr)
	time.Sleep(1 * time.Second)

	// Create table on leader.
	_, err := leader.DB().Exec("CREATE TABLE fwd_write (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	// ExecMulti from follower — exercises forwarding of multi-statements.
	_, err = follower.ExecMulti([]Statement{
		{SQL: "INSERT INTO fwd_write (val) VALUES (?)", Args: []any{"a"}},
		{SQL: "INSERT INTO fwd_write (val) VALUES (?)", Args: []any{"b"}},
	})
	if err != nil {
		t.Fatalf("exec multi from follower: %v", err)
	}
	time.Sleep(500 * time.Millisecond)

	// Verify on leader.
	var count int
	err = leader.DB().QueryRow("SELECT count(*) FROM fwd_write").Scan(&count)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2, got %d", count)
	}
}

// --- Query with multiple rows from leader strong to exercise Next+Scan fully ---

func TestLeader_StrongQueryMultipleRows(t *testing.T) {
	node := testNode(t)
	db := node.DB()

	_, err := db.Exec("CREATE TABLE strong_multi (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	for i := 0; i < 10; i++ {
		_, err = db.Exec("INSERT INTO strong_multi (val) VALUES (?)", fmt.Sprintf("v%d", i))
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	time.Sleep(100 * time.Millisecond)

	ctx := WithConsistency(context.Background(), ConsistencyStrong)
	rows, err := db.QueryContext(ctx, "SELECT id, val FROM strong_multi ORDER BY id")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id int
		var val string
		if err := rows.Scan(&id, &val); err != nil {
			t.Fatalf("scan: %v", err)
		}
		count++
	}
	if count != 10 {
		t.Fatalf("expected 10, got %d", count)
	}
}

// --- Follower transaction via Tx commit (forwarded) ---

func TestFollower_TransactionForwarded(t *testing.T) {
	leader := testNode(t)
	leaderAddr := leader.config.Bind
	time.Sleep(500 * time.Millisecond)

	follower := testJoinNode(t, leaderAddr)
	time.Sleep(1 * time.Second)

	_, err := leader.DB().Exec("CREATE TABLE fwd_tx (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	// Begin transaction on follower — writes will be forwarded on commit.
	fdb := follower.DB()
	tx, err := fdb.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	tx.Exec("INSERT INTO fwd_tx (val) VALUES (?)", "tx-a")
	tx.Exec("INSERT INTO fwd_tx (val) VALUES (?)", "tx-b")
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	time.Sleep(500 * time.Millisecond)

	var count int
	err = leader.DB().QueryRow("SELECT count(*) FROM fwd_tx").Scan(&count)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2, got %d", count)
	}
}

// --- Store executeMulti error path ---

func TestStore_ExecuteMultiError(t *testing.T) {
	dir := t.TempDir()
	s, err := newStoreAt(filepath.Join(dir, "test.db"), 2)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	defer s.close()

	// Execute multi with an invalid statement — should return error.
	_, err = s.executeMulti([]Statement{
		{SQL: "INSERT INTO nonexistent (val) VALUES (?)", Args: []any{"x"}},
	})
	if err == nil {
		t.Fatal("expected error for invalid statement")
	}
}

// --- Store execute error path ---

func TestStore_ExecuteError(t *testing.T) {
	dir := t.TempDir()
	s, err := newStoreAt(filepath.Join(dir, "test.db"), 2)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	defer s.close()

	// Execute with invalid SQL.
	_, err = s.execute(Statement{SQL: "INSERT INTO nonexistent (val) VALUES (?)", Args: []any{"x"}})
	if err == nil {
		t.Fatal("expected error for invalid SQL")
	}
}

// --- RPC Query with multiple rows to exercise full iteration ---

func TestRPCService_QueryMultipleRows(t *testing.T) {
	node := testNode(t)
	db := node.DB()

	_, err := db.Exec("CREATE TABLE rpc_multi (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	for i := 0; i < 3; i++ {
		_, err = db.Exec("INSERT INTO rpc_multi (val) VALUES (?)", fmt.Sprintf("r%d", i))
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	time.Sleep(100 * time.Millisecond)

	svc := &RPCService{node: node}
	req := &RPCQueryRequest{
		DB:  "default",
		SQL: "SELECT id, val FROM rpc_multi ORDER BY id",
	}
	var resp RPCQueryResponse
	err = svc.Query(req, &resp)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if resp.Error != "" {
		t.Fatalf("query error: %s", resp.Error)
	}
	if len(resp.TaggedRows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(resp.TaggedRows))
	}
	if len(resp.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(resp.Columns))
	}
}

// --- ExecContext in a transaction accumulates statements ---

func TestDriver_ExecInTx(t *testing.T) {
	node := testNode(t)
	conn := &colmenaConn{node: node, dbName: "default", consistency: ConsistencyWeak}

	// Setup table first.
	_, err := conn.ExecContext(context.Background(), "CREATE TABLE tx_acc (id INTEGER PRIMARY KEY, val TEXT)", nil)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	// Begin tx.
	tx, err := conn.BeginTx(context.Background(), driver.TxOptions{})
	if err != nil {
		t.Fatalf("begin: %v", err)
	}

	// ExecContext inside tx should accumulate, not execute immediately.
	_, err = conn.ExecContext(context.Background(), "INSERT INTO tx_acc (val) VALUES (?)", []driver.NamedValue{{Ordinal: 1, Value: "a"}})
	if err != nil {
		t.Fatalf("exec in tx: %v", err)
	}
	_, err = conn.ExecContext(context.Background(), "INSERT INTO tx_acc (val) VALUES (?)", []driver.NamedValue{{Ordinal: 1, Value: "b"}})
	if err != nil {
		t.Fatalf("exec in tx: %v", err)
	}

	// Commit should send all accumulated statements.
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	// Verify data.
	rows, err := conn.QueryContext(context.Background(), "SELECT count(*) FROM tx_acc", nil)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	dest := make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil {
		t.Fatalf("next: %v", err)
	}
	count, ok := dest[0].(int64)
	if !ok {
		t.Fatalf("expected int64, got %T", dest[0])
	}
	if count != 2 {
		t.Fatalf("expected 2, got %d", count)
	}
}

// --- Default consistency (unset context) uses local on leader ---

func TestQuery_DefaultConsistency(t *testing.T) {
	node := testNode(t)
	db := node.DB()

	_, err := db.Exec("CREATE TABLE def_cons (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, err = db.Exec("INSERT INTO def_cons (val) VALUES (?)", "default")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	// Query without any context consistency set — uses the default (Weak).
	// Since this node is the leader, it should read locally.
	var val string
	err = db.QueryRow("SELECT val FROM def_cons WHERE id = 1").Scan(&val)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if val != "default" {
		t.Fatalf("expected 'default', got %q", val)
	}
}

// --- Query with args from follower Weak (exercises full leaderQuery path with args) ---

func TestFollower_WeakQueryWithArgs(t *testing.T) {
	leader := testNode(t)
	leaderAddr := leader.config.Bind
	time.Sleep(500 * time.Millisecond)

	follower := testJoinNode(t, leaderAddr)
	time.Sleep(1 * time.Second)

	_, err := leader.DB().Exec("CREATE TABLE weak_args (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, err = leader.DB().Exec("INSERT INTO weak_args (val) VALUES (?)", "arg-val")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	time.Sleep(500 * time.Millisecond)

	// forwardQuery with args.
	resp, err := follower.forwardQuery("default", "SELECT val FROM weak_args WHERE id = ?", []any{1}, ConsistencyWeak)
	if err != nil {
		t.Fatalf("forward query with args: %v", err)
	}
	if len(resp.TaggedRows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(resp.TaggedRows))
	}
}

// --- RPC Query with Args via RPCService directly ---

func TestRPCService_QueryWithArgs(t *testing.T) {
	node := testNode(t)
	db := node.DB()

	_, err := db.Exec("CREATE TABLE rpc_args (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, err = db.Exec("INSERT INTO rpc_args (val) VALUES (?)", "rpc-arg-val")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	svc := &RPCService{node: node}
	req := &RPCQueryRequest{
		DB:   "default",
		SQL:  "SELECT val FROM rpc_args WHERE id = ?",
		Args: []interface{}{1},
	}
	var resp RPCQueryResponse
	err = svc.Query(req, &resp)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if resp.Error != "" {
		t.Fatalf("query error: %s", resp.Error)
	}
	if len(resp.TaggedRows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(resp.TaggedRows))
	}
}

// --- FSM Apply with invalid SQL error path ---

func TestFSM_ApplyInvalidSQL(t *testing.T) {
	node := testNode(t)

	// Apply a command with invalid SQL to trigger the execute error path.
	cmd := &Command{
		Type:       CommandExecute,
		DB:         "default",
		Statements: []Statement{{SQL: "INSERT INTO nonexistent_table (x) VALUES (1)"}},
	}
	data, err := marshalCommand(cmd)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	future := node.raft.Apply(data, 5*time.Second)
	if err := future.Error(); err != nil {
		t.Fatalf("raft apply: %v", err)
	}
	resp, ok := future.Response().(*ApplyResult)
	if !ok {
		t.Fatal("unexpected response type")
	}
	if resp.Error == "" {
		t.Fatal("expected error for invalid SQL")
	}
}

// --- FSM Apply ExecuteMulti with wrong statement count ---

func TestFSM_ApplyExecuteWrongStmtCount(t *testing.T) {
	node := testNode(t)

	// CommandExecute requires exactly 1 statement. Send 0.
	cmd := &Command{
		Type:       CommandExecute,
		DB:         "default",
		Statements: []Statement{},
	}
	data, err := marshalCommand(cmd)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	future := node.raft.Apply(data, 5*time.Second)
	if err := future.Error(); err != nil {
		t.Fatalf("raft apply: %v", err)
	}
	resp, ok := future.Response().(*ApplyResult)
	if !ok {
		t.Fatal("unexpected response type")
	}
	if resp.Error == "" {
		t.Fatal("expected error for wrong statement count")
	}
}

// --- FSM Apply ExecuteMulti error path ---

func TestFSM_ApplyExecuteMultiError(t *testing.T) {
	node := testNode(t)

	cmd := &Command{
		Type:       CommandExecuteMulti,
		DB:         "default",
		Statements: []Statement{{SQL: "INSERT INTO nonexistent (x) VALUES (1)"}},
	}
	data, err := marshalCommand(cmd)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	future := node.raft.Apply(data, 5*time.Second)
	if err := future.Error(); err != nil {
		t.Fatalf("raft apply: %v", err)
	}
	resp, ok := future.Response().(*ApplyResult)
	if !ok {
		t.Fatal("unexpected response type")
	}
	if resp.Error == "" {
		t.Fatal("expected error for execute multi with invalid SQL")
	}
}

// --- FSM Apply with empty DB name defaults to "default" ---

func TestFSM_ApplyEmptyDBName(t *testing.T) {
	node := testNode(t)

	_, err := node.DB().Exec("CREATE TABLE empty_db_test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	cmd := &Command{
		Type:       CommandExecute,
		DB:         "", // empty — should default to "default"
		Statements: []Statement{{SQL: "INSERT INTO empty_db_test (id) VALUES (1)"}},
	}
	data, err := marshalCommand(cmd)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	future := node.raft.Apply(data, 5*time.Second)
	if err := future.Error(); err != nil {
		t.Fatalf("raft apply: %v", err)
	}
	resp, ok := future.Response().(*ApplyResult)
	if !ok {
		t.Fatal("unexpected response type")
	}
	if resp.Error != "" {
		t.Fatalf("unexpected error: %s", resp.Error)
	}
}

// --- Node Close idempotency ---

func TestNode_CloseIdempotent(t *testing.T) {
	port := freePort(t)
	dir := t.TempDir()

	node, err := New(Config{
		NodeID:           fmt.Sprintf("close-test-%d", port),
		DataDir:          dir,
		Bind:             fmt.Sprintf("127.0.0.1:%d", port),
		Bootstrap:        true,
		HeartbeatTimeout: 200 * time.Millisecond,
		ElectionTimeout:  200 * time.Millisecond,
		ApplyTimeout:     5 * time.Second,
		LogOutput:        io.Discard,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := node.WaitForLeader(5 * time.Second); err != nil {
		t.Fatalf("wait: %v", err)
	}

	// Close twice — should not panic or error.
	if err := node.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if err := node.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

// --- Backup WAL with empty WAL size ---

func TestBackup_EmptyWALSize(t *testing.T) {
	backupDir := filepath.Join(t.TempDir(), "backups")
	backend, err := NewLocalBackend(backupDir)
	if err != nil {
		t.Fatalf("create backend: %v", err)
	}

	node := testNode(t, func(cfg *Config) {
		cfg.Backup = &BackupConfig{
			Backend:          backend,
			SyncInterval:     10 * time.Minute,
			SnapshotInterval: 10 * time.Minute,
		}
	})

	// Force checkpoint to truncate WAL, making it 0 bytes.
	st, _ := node.stores.get("default")
	_, _ = st.writer.Exec("PRAGMA wal_checkpoint(TRUNCATE)")

	// syncWAL should handle empty WAL gracefully (size == 0).
	err = node.backup.syncWAL()
	if err != nil {
		t.Fatalf("syncWAL with empty WAL: %v", err)
	}
}

// --- Exec via node.DB() returns result with LastInsertId ---

func TestExecResult_LastInsertId(t *testing.T) {
	node := testNode(t)
	db := node.DB()

	_, err := db.Exec("CREATE TABLE lid_test (id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	result, err := db.Exec("INSERT INTO lid_test (val) VALUES (?)", "a")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("last insert id: %v", err)
	}
	if id != 1 {
		t.Fatalf("expected 1, got %d", id)
	}

	result, err = db.Exec("INSERT INTO lid_test (val) VALUES (?)", "b")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	id, err = result.LastInsertId()
	if err != nil {
		t.Fatalf("last insert id: %v", err)
	}
	if id != 2 {
		t.Fatalf("expected 2, got %d", id)
	}
}

// --- QueryContext default consistency path (unknown level) ---

func TestQuery_UnknownConsistencyLevel(t *testing.T) {
	node := testNode(t)
	db := node.DB()

	_, err := db.Exec("CREATE TABLE unk_cons (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, err = db.Exec("INSERT INTO unk_cons (val) VALUES (?)", "unk")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	// Use a consistency level value that doesn't match any case (triggers default).
	ctx := WithConsistency(context.Background(), ConsistencyLevel(99))
	var val string
	err = db.QueryRowContext(ctx, "SELECT val FROM unk_cons WHERE id = 1").Scan(&val)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if val != "unk" {
		t.Fatalf("expected 'unk', got %q", val)
	}
}

// --- RPCService Execute on leader with invalid command data ---

func TestRPCService_ExecuteInvalidCommand(t *testing.T) {
	node := testNode(t)
	svc := &RPCService{node: node}

	// Send invalid command data — should propagate the apply error.
	req := &RPCExecuteRequest{Command: []byte("{invalid}")}
	var resp RPCExecuteResponse
	err := svc.Execute(req, &resp)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if resp.Error == "" {
		t.Fatal("expected error for invalid command")
	}
}

// --- Backup: test syncWAL with no generation (early return) ---

func TestBackup_SyncWALNoGeneration(t *testing.T) {
	dir := t.TempDir()
	s, err := newStoreAt(filepath.Join(dir, "test.db"), 2)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	defer s.close()

	backupDir := filepath.Join(t.TempDir(), "backups")
	backend, err := NewLocalBackend(backupDir)
	if err != nil {
		t.Fatalf("create backend: %v", err)
	}

	bm := &backupManager{
		store:   s,
		backend: backend,
		cfg:     BackupConfig{SyncInterval: time.Minute, SnapshotInterval: time.Hour},
		stopCh:  make(chan struct{}),
		doneCh:  make(chan struct{}),
		// generation is empty — syncWAL should return nil early.
	}

	if err := bm.syncWAL(); err != nil {
		t.Fatalf("syncWAL with no generation: %v", err)
	}
}

// --- ExecMulti error from follower (exercises the error return in ExecMulti) ---

func TestFollower_ExecMultiInvalidSQL(t *testing.T) {
	leader := testNode(t)
	leaderAddr := leader.config.Bind
	time.Sleep(500 * time.Millisecond)

	follower := testJoinNode(t, leaderAddr)
	time.Sleep(1 * time.Second)

	// ExecMulti on follower with invalid SQL.
	_, err := follower.ExecMulti([]Statement{
		{SQL: "INSERT INTO nonexistent_table (x) VALUES (?)", Args: []any{"y"}},
	})
	if err == nil {
		t.Fatal("expected error for invalid SQL via ExecMulti")
	}
}

// --- Store close covers both reader and writer close ---

func TestStore_CloseExplicit(t *testing.T) {
	dir := t.TempDir()
	s, err := newStoreAt(filepath.Join(dir, "close_test.db"), 2)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := s.close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// --- StoreManager close with multiple stores ---

func TestStoreManager_CloseMultiple(t *testing.T) {
	dir := t.TempDir()
	sm := newStoreManager(dir, 2)

	_, err := sm.get("a")
	if err != nil {
		t.Fatalf("get a: %v", err)
	}
	_, err = sm.get("b")
	if err != nil {
		t.Fatalf("get b: %v", err)
	}

	if err := sm.close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}
