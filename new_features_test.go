package colmena

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/rpc"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// =============================================================================
// readLease tests
// =============================================================================

func TestReadLease_ExtendAndValid(t *testing.T) {
	l := &readLease{}

	// A fresh lease should not be valid.
	if l.valid() {
		t.Fatal("fresh lease should not be valid")
	}

	// Extend with a generous duration.
	l.extend(500 * time.Millisecond)
	if !l.valid() {
		t.Fatal("lease should be valid immediately after extend")
	}

	// Wait past the expiry.
	time.Sleep(600 * time.Millisecond)
	if l.valid() {
		t.Fatal("lease should be expired after waiting past its duration")
	}
}

func TestReadLease_ExtendResets(t *testing.T) {
	l := &readLease{}

	l.extend(100 * time.Millisecond)
	time.Sleep(60 * time.Millisecond)

	// Re-extend before expiry.
	l.extend(200 * time.Millisecond)
	time.Sleep(60 * time.Millisecond)

	// Should still be valid because we re-extended.
	if !l.valid() {
		t.Fatal("lease should still be valid after re-extension")
	}
}

func TestReadLease_ConcurrentAccess(t *testing.T) {
	l := &readLease{}
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			l.extend(50 * time.Millisecond)
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			l.valid()
		}
	}()

	wg.Wait()
}

// =============================================================================
// metrics helper tests (table-driven)
// =============================================================================

func TestParseUint64(t *testing.T) {
	tests := []struct {
		input string
		want  uint64
	}{
		{"0", 0},
		{"1", 1},
		{"123456789", 123456789},
		{"", 0},
		{"abc", 0},
		{"18446744073709551615", 18446744073709551615},
	}
	for _, tt := range tests {
		got := parseUint64(tt.input)
		if got != tt.want {
			t.Errorf("parseUint64(%q) = %d, want %d", tt.input, got, tt.want)
		}
	}
}

func TestParseInt(t *testing.T) {
	tests := []struct {
		input string
		want  int
	}{
		{"0", 0},
		{"42", 42},
		{"-1", -1},
		{"", 0},
		{"notanumber", 0},
	}
	for _, tt := range tests {
		got := parseInt(tt.input)
		if got != tt.want {
			t.Errorf("parseInt(%q) = %d, want %d", tt.input, got, tt.want)
		}
	}
}

func TestParseDuration(t *testing.T) {
	tests := []struct {
		input string
		want  time.Duration
	}{
		{"", 0},
		{"never", 0},
		{"0", 0},
		{"100ms", 100 * time.Millisecond},
		{"1s", 1 * time.Second},
		{"5m0s", 5 * time.Minute},
		{"garbage", 0},
	}
	for _, tt := range tests {
		got := parseDuration(tt.input)
		if got != tt.want {
			t.Errorf("parseDuration(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestCountPeers(t *testing.T) {
	tests := []struct {
		input string
		want  int
	}{
		{"", 0},
		{"[{Suffrage:Voter ID:node1 Address:127.0.0.1:9000}]", 1},
		{"[{Suffrage:Voter ID:node1 Address:127.0.0.1:9000} {Suffrage:Voter ID:node2 Address:127.0.0.1:9002}]", 2},
		{"[{Suffrage:Voter ID:n1 Address:a} {Suffrage:Voter ID:n2 Address:b} {Suffrage:Voter ID:n3 Address:c}]", 3},
	}
	for _, tt := range tests {
		got := countPeers(tt.input)
		if got != tt.want {
			t.Errorf("countPeers(%q) = %d, want %d", tt.input, got, tt.want)
		}
	}
}

func TestRaftStateToInt(t *testing.T) {
	tests := []struct {
		input string
		want  int
	}{
		{"Leader", 1},
		{"leader", 1},
		{"LEADER", 1},
		{"Follower", 2},
		{"follower", 2},
		{"Candidate", 3},
		{"candidate", 3},
		{"Shutdown", 0},
		{"unknown", 0},
		{"", 0},
	}
	for _, tt := range tests {
		got := raftStateToInt(tt.input)
		if got != tt.want {
			t.Errorf("raftStateToInt(%q) = %d, want %d", tt.input, got, tt.want)
		}
	}
}

func TestWriteGauge(t *testing.T) {
	var b strings.Builder
	writeGauge(&b, "test_metric", "A test metric", uint64(42))
	out := b.String()

	if !strings.Contains(out, "# HELP test_metric A test metric") {
		t.Errorf("missing HELP line in:\n%s", out)
	}
	if !strings.Contains(out, "# TYPE test_metric gauge") {
		t.Errorf("missing TYPE line in:\n%s", out)
	}
	if !strings.Contains(out, "test_metric 42") {
		t.Errorf("missing metric value in:\n%s", out)
	}
}

func TestWriteCounter(t *testing.T) {
	var b strings.Builder
	writeCounter(&b, "ops_total", "Total operations", uint64(99))
	out := b.String()

	if !strings.Contains(out, "# HELP ops_total Total operations") {
		t.Errorf("missing HELP line in:\n%s", out)
	}
	if !strings.Contains(out, "# TYPE ops_total counter") {
		t.Errorf("missing TYPE line in:\n%s", out)
	}
	if !strings.Contains(out, "ops_total 99") {
		t.Errorf("missing metric value in:\n%s", out)
	}
}

// =============================================================================
// Metrics integration tests (require a running node)
// =============================================================================

func TestMetrics_ReturnsPopulatedFields(t *testing.T) {
	node := testNode(t)
	db := node.DB()

	_, err := db.Exec("CREATE TABLE m_test (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	for i := 0; i < 5; i++ {
		_, err = db.Exec("INSERT INTO m_test (val) VALUES (?)", fmt.Sprintf("row-%d", i))
		if err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	time.Sleep(100 * time.Millisecond)

	// Do some reads.
	var count int
	if err := db.QueryRow("SELECT count(*) FROM m_test").Scan(&count); err != nil {
		t.Fatalf("query: %v", err)
	}

	m := node.Metrics()

	if m.RaftState != "Leader" {
		t.Errorf("expected RaftState=Leader, got %q", m.RaftState)
	}
	if m.RaftTerm == 0 {
		t.Error("expected RaftTerm > 0")
	}
	if m.RaftLastIndex == 0 {
		t.Error("expected RaftLastIndex > 0")
	}
	if m.RaftCommitIndex == 0 {
		t.Error("expected RaftCommitIndex > 0")
	}
	if m.RaftAppliedIndex == 0 {
		t.Error("expected RaftAppliedIndex > 0")
	}
	if m.WritesTotal == 0 {
		t.Error("expected WritesTotal > 0")
	}
	if m.ReadsTotal == 0 {
		t.Error("expected ReadsTotal > 0")
	}
	if m.Peers == 0 {
		t.Error("expected Peers > 0")
	}
}

func TestMetricsHandler_PrometheusFormat(t *testing.T) {
	node := testNode(t)
	db := node.DB()

	_, err := db.Exec("CREATE TABLE mh_test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	_, err = db.Exec("INSERT INTO mh_test (id) VALUES (1)")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	handler := node.MetricsHandler()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	ct := rec.Header().Get("Content-Type")
	if !strings.Contains(ct, "text/plain") {
		t.Errorf("expected text/plain content type, got %q", ct)
	}

	body := rec.Body.String()
	expectedMetrics := []string{
		"colmena_raft_state",
		"colmena_raft_term",
		"colmena_raft_last_index",
		"colmena_raft_commit_index",
		"colmena_raft_applied_index",
		"colmena_raft_fsm_pending",
		"colmena_snapshot_index",
		"colmena_writes_total",
		"colmena_reads_total",
		"colmena_rpc_forwards_total",
		"colmena_last_contact_ms",
		"colmena_peers",
	}
	for _, name := range expectedMetrics {
		if !strings.Contains(body, name) {
			t.Errorf("metrics output missing %q", name)
		}
		helpLine := fmt.Sprintf("# HELP %s", name)
		if !strings.Contains(body, helpLine) {
			t.Errorf("metrics output missing HELP for %q", name)
		}
		typeLine := fmt.Sprintf("# TYPE %s", name)
		if !strings.Contains(body, typeLine) {
			t.Errorf("metrics output missing TYPE for %q", name)
		}
	}

	// Verify counter vs gauge TYPE annotations.
	if !strings.Contains(body, "# TYPE colmena_writes_total counter") {
		t.Error("colmena_writes_total should be a counter")
	}
	if !strings.Contains(body, "# TYPE colmena_raft_state gauge") {
		t.Error("colmena_raft_state should be a gauge")
	}
}

// =============================================================================
// migrate tests
// =============================================================================

func TestMigrate_BasicApply(t *testing.T) {
	node := testNode(t)

	migrations := []Migration{
		{
			Version: 1,
			Name:    "create_users",
			SQL:     "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
		},
		{
			Version: 2,
			Name:    "create_orders",
			SQL:     "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, total REAL)",
		},
	}

	if err := node.Migrate(migrations); err != nil {
		t.Fatalf("first migrate: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	db := node.DB()

	// Verify both tables exist.
	var count int
	err := db.QueryRow("SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN ('users','orders')").Scan(&count)
	if err != nil {
		t.Fatalf("query tables: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 tables, got %d", count)
	}

	// Verify _migrations table tracks both.
	err = db.QueryRow("SELECT count(*) FROM _migrations").Scan(&count)
	if err != nil {
		t.Fatalf("query _migrations: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 migration records, got %d", count)
	}
}

func TestMigrate_SkipsAlreadyApplied(t *testing.T) {
	node := testNode(t)

	migrations := []Migration{
		{
			Version: 1,
			Name:    "create_users",
			SQL:     "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
		},
	}

	// Apply once.
	if err := node.Migrate(migrations); err != nil {
		t.Fatalf("first migrate: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Add a second migration and re-apply all.
	migrations = append(migrations, Migration{
		Version: 2,
		Name:    "create_orders",
		SQL:     "CREATE TABLE orders (id INTEGER PRIMARY KEY, total REAL)",
	})

	if err := node.Migrate(migrations); err != nil {
		t.Fatalf("second migrate: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	db := node.DB()
	var count int
	err := db.QueryRow("SELECT count(*) FROM _migrations").Scan(&count)
	if err != nil {
		t.Fatalf("query _migrations: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 migration records (not duplicated), got %d", count)
	}
}

func TestMigrate_MultiStatementMigration(t *testing.T) {
	node := testNode(t)

	migrations := []Migration{
		{
			Version: 1,
			Name:    "create_schema",
			SQL: `CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT);
				  CREATE TABLE categories (id INTEGER PRIMARY KEY, label TEXT)`,
		},
	}

	if err := node.Migrate(migrations); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	db := node.DB()
	var count int
	err := db.QueryRow("SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN ('products','categories')").Scan(&count)
	if err != nil {
		t.Fatalf("query tables: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 tables from multi-statement migration, got %d", count)
	}
}

func TestMigrate_AlterTableDuplicateColumnIgnored(t *testing.T) {
	node := testNode(t)

	migrations := []Migration{
		{
			Version: 1,
			Name:    "create_users",
			SQL:     "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
		},
		{
			Version: 2,
			Name:    "add_email",
			SQL:     "ALTER TABLE users ADD COLUMN email TEXT",
		},
	}

	if err := node.Migrate(migrations); err != nil {
		t.Fatalf("first migrate: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Apply a migration that adds the same column again. This should be silently
	// ignored because Migrate handles ALTER TABLE duplicate column errors.
	migrations2 := []Migration{
		{Version: 1, Name: "create_users", SQL: "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"},
		{Version: 2, Name: "add_email", SQL: "ALTER TABLE users ADD COLUMN email TEXT"},
		{
			Version: 3,
			Name:    "add_email_again",
			SQL:     "ALTER TABLE users ADD COLUMN email TEXT",
		},
	}

	if err := node.Migrate(migrations2); err != nil {
		t.Fatalf("migrate with duplicate column should not fail: %v", err)
	}
}

func TestMigrate_EmptyMigrationList(t *testing.T) {
	node := testNode(t)

	if err := node.Migrate(nil); err != nil {
		t.Fatalf("migrate with nil list: %v", err)
	}
	if err := node.Migrate([]Migration{}); err != nil {
		t.Fatalf("migrate with empty list: %v", err)
	}
}

func TestIsAlterTable(t *testing.T) {
	tests := []struct {
		sql  string
		want bool
	}{
		{"ALTER TABLE users ADD COLUMN email TEXT", true},
		{"alter table users add column email text", true},
		{"  ALTER TABLE foo DROP COLUMN bar", true},
		{"CREATE TABLE test (id INT)", false},
		{"INSERT INTO users VALUES (1)", false},
		{"", false},
		{"SELECT * FROM alter_table_log", false},
	}
	for _, tt := range tests {
		got := isAlterTable(tt.sql)
		if got != tt.want {
			t.Errorf("isAlterTable(%q) = %v, want %v", tt.sql, got, tt.want)
		}
	}
}

func TestIsDuplicateColumn(t *testing.T) {
	tests := []struct {
		err  error
		want bool
	}{
		{errors.New("duplicate column name: email"), true},
		{errors.New("DUPLICATE COLUMN name: email"), true},
		{errors.New("table users already has a column named email"), false},
		{errors.New("some other error"), false},
	}
	for _, tt := range tests {
		got := isDuplicateColumn(tt.err)
		if got != tt.want {
			t.Errorf("isDuplicateColumn(%q) = %v, want %v", tt.err, got, tt.want)
		}
	}
}

// =============================================================================
// batcher tests
// =============================================================================

func TestBatcher_SingleWrite(t *testing.T) {
	node := testNode(t, func(cfg *Config) {
		cfg.BatchWindow = 5 * time.Millisecond
		cfg.BatchMaxSize = 10
	})
	db := node.DB()

	_, err := db.Exec("CREATE TABLE batch_single (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	_, err = db.Exec("INSERT INTO batch_single (val) VALUES (?)", "hello")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	var count int
	if err := db.QueryRow("SELECT count(*) FROM batch_single").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 row, got %d", count)
	}
}

func TestBatcher_ConcurrentWrites(t *testing.T) {
	node := testNode(t, func(cfg *Config) {
		cfg.BatchWindow = 5 * time.Millisecond
		cfg.BatchMaxSize = 10
	})
	db := node.DB()

	_, err := db.Exec("CREATE TABLE batch_conc (id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	const numWriters = 20
	var wg sync.WaitGroup
	errs := make([]error, numWriters)

	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, errs[idx] = db.Exec("INSERT INTO batch_conc (val) VALUES (?)", fmt.Sprintf("w-%d", idx))
		}(i)
	}

	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("writer %d failed: %v", i, err)
		}
	}

	time.Sleep(100 * time.Millisecond)

	var count int
	if err := db.QueryRow("SELECT count(*) FROM batch_conc").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != numWriters {
		t.Fatalf("expected %d rows, got %d", numWriters, count)
	}
}

func TestBatcher_FlushOnMaxBatch(t *testing.T) {
	node := testNode(t, func(cfg *Config) {
		// Very long window, but small batch size to trigger immediate flush.
		cfg.BatchWindow = 5 * time.Second
		cfg.BatchMaxSize = 5
	})
	db := node.DB()

	_, err := db.Exec("CREATE TABLE batch_max (id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Submit exactly maxBatch concurrent writes. They should flush immediately.
	const n = 5
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, err := db.Exec("INSERT INTO batch_max (val) VALUES (?)", fmt.Sprintf("v-%d", idx))
			if err != nil {
				t.Errorf("insert %d: %v", idx, err)
			}
		}(i)
	}

	wg.Wait()

	time.Sleep(100 * time.Millisecond)

	var count int
	if err := db.QueryRow("SELECT count(*) FROM batch_max").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != n {
		t.Fatalf("expected %d rows, got %d", n, count)
	}
}

func TestBatcher_FlushOnWindowTimeout(t *testing.T) {
	node := testNode(t, func(cfg *Config) {
		cfg.BatchWindow = 10 * time.Millisecond
		cfg.BatchMaxSize = 1000 // large enough to never trigger max-batch flush
	})
	db := node.DB()

	_, err := db.Exec("CREATE TABLE batch_window (id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	start := time.Now()
	_, err = db.Exec("INSERT INTO batch_window (val) VALUES (?)", "timeout-flush")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	elapsed := time.Since(start)

	// The write should complete within a reasonable time (batch window + apply).
	if elapsed > 2*time.Second {
		t.Fatalf("expected write to complete quickly, took %v", elapsed)
	}

	time.Sleep(50 * time.Millisecond)

	var count int
	if err := db.QueryRow("SELECT count(*) FROM batch_window").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 row, got %d", count)
	}
}

func TestBatcher_ClosePreventsSubmission(t *testing.T) {
	b := newWriteBatcher(nil, 100*time.Millisecond, 100)
	b.close()

	cmd := &Command{
		Type:       CommandExecute,
		DB:         "default",
		Statements: []Statement{{SQL: "INSERT INTO x VALUES (1)"}},
	}
	_, err := b.submit(cmd)
	if err == nil {
		t.Fatal("expected error when submitting to closed batcher")
	}
}

// =============================================================================
// rpc_pool tests
// =============================================================================

func TestRPCPool_GetDialsNewConnection(t *testing.T) {
	// Start a simple RPC server.
	srv := rpc.NewServer()
	srv.RegisterName("Colmena", &testRPCService{})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go srv.ServeConn(conn)
		}
	}()

	// The pool uses rpcAddrFrom which adds +1 to the Raft port.
	// We need to figure out the "raft addr" that maps to our listener's port.
	_, portStr, _ := net.SplitHostPort(ln.Addr().String())
	var rpcPort int
	fmt.Sscanf(portStr, "%d", &rpcPort)
	raftPort := rpcPort - 1
	raftAddr := fmt.Sprintf("127.0.0.1:%d", raftPort)

	pool := newRPCPool(nil, "test")
	defer pool.close()

	client, err := pool.get(raftAddr)
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	// Verify the connection works.
	var resp RPCPingResponse
	if err := client.Call("Colmena.Ping", &RPCPingRequest{}, &resp); err != nil {
		t.Fatalf("ping: %v", err)
	}
}

func TestRPCPool_CachesConnection(t *testing.T) {
	srv := rpc.NewServer()
	srv.RegisterName("Colmena", &testRPCService{})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go srv.ServeConn(conn)
		}
	}()

	_, portStr, _ := net.SplitHostPort(ln.Addr().String())
	var rpcPort int
	fmt.Sscanf(portStr, "%d", &rpcPort)
	raftAddr := fmt.Sprintf("127.0.0.1:%d", rpcPort-1)

	pool := newRPCPool(nil, "test")
	defer pool.close()

	c1, err := pool.get(raftAddr)
	if err != nil {
		t.Fatalf("first get: %v", err)
	}
	c2, err := pool.get(raftAddr)
	if err != nil {
		t.Fatalf("second get: %v", err)
	}

	// Same pointer — connection was cached.
	if c1 != c2 {
		t.Fatal("expected same client from pool cache")
	}
}

func TestRPCPool_MarkFailedCausesReconnect(t *testing.T) {
	srv := rpc.NewServer()
	srv.RegisterName("Colmena", &testRPCService{})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go srv.ServeConn(conn)
		}
	}()

	_, portStr, _ := net.SplitHostPort(ln.Addr().String())
	var rpcPort int
	fmt.Sscanf(portStr, "%d", &rpcPort)
	raftAddr := fmt.Sprintf("127.0.0.1:%d", rpcPort-1)

	pool := newRPCPool(nil, "test")
	defer pool.close()

	c1, err := pool.get(raftAddr)
	if err != nil {
		t.Fatalf("first get: %v", err)
	}

	pool.markFailed(raftAddr)

	c2, err := pool.get(raftAddr)
	if err != nil {
		t.Fatalf("get after markFailed: %v", err)
	}

	// After marking failed, should get a new client.
	if c1 == c2 {
		t.Fatal("expected different client after markFailed")
	}

	// Verify the new connection works.
	var resp RPCPingResponse
	if err := c2.Call("Colmena.Ping", &RPCPingRequest{}, &resp); err != nil {
		t.Fatalf("ping on new client: %v", err)
	}
}

func TestRPCPool_CloseShutdown(t *testing.T) {
	srv := rpc.NewServer()
	srv.RegisterName("Colmena", &testRPCService{})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go srv.ServeConn(conn)
		}
	}()

	_, portStr, _ := net.SplitHostPort(ln.Addr().String())
	var rpcPort int
	fmt.Sscanf(portStr, "%d", &rpcPort)
	raftAddr := fmt.Sprintf("127.0.0.1:%d", rpcPort-1)

	pool := newRPCPool(nil, "test")

	_, err = pool.get(raftAddr)
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	pool.close()

	// After close, internal map should be empty.
	pool.mu.Lock()
	n := len(pool.clients)
	pool.mu.Unlock()
	if n != 0 {
		t.Fatalf("expected 0 clients after close, got %d", n)
	}
}

func TestRPCPool_GetInvalidAddress(t *testing.T) {
	pool := newRPCPool(nil, "test")
	defer pool.close()

	// Address with no listening server.
	_, err := pool.get("127.0.0.1:19999")
	if err == nil {
		t.Fatal("expected error when dialing non-existent server")
	}
}

func TestRPCPool_MarkFailed_UnknownAddr(t *testing.T) {
	pool := newRPCPool(nil, "test")
	defer pool.close()

	// Marking an unknown address should not panic.
	pool.markFailed("127.0.0.1:12345")
}

// testRPCService is a minimal RPC service for pool tests.
type testRPCService struct{}

func (s *testRPCService) Ping(req *RPCPingRequest, resp *RPCPingResponse) error {
	return nil
}

// =============================================================================
// Additional coverage tests for node, driver, config
// =============================================================================

func TestRPCAddrFrom(t *testing.T) {
	tests := []struct {
		input   string
		want    string
		wantErr bool
	}{
		{"127.0.0.1:9000", "127.0.0.1:9001", false},
		{"0.0.0.0:5000", "0.0.0.0:5001", false},
		{"invalid", "", true},
	}
	for _, tt := range tests {
		got, err := rpcAddrFrom(tt.input)
		if (err != nil) != tt.wantErr {
			t.Errorf("rpcAddrFrom(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			continue
		}
		if got != tt.want {
			t.Errorf("rpcAddrFrom(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestConfig_ValidateErrors(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr string
	}{
		{"empty NodeID", Config{DataDir: "/tmp", Bind: "0.0.0.0:9000", Bootstrap: true}, "NodeID"},
		{"empty DataDir", Config{NodeID: "n1", Bind: "0.0.0.0:9000", Bootstrap: true}, "DataDir"},
		{"empty Bind", Config{NodeID: "n1", DataDir: "/tmp", Bootstrap: true}, "Bind"},
		{"invalid Bind", Config{NodeID: "n1", DataDir: "/tmp", Bind: "invalid", Bootstrap: true}, "invalid Bind"},
		{"both Bootstrap and Join", Config{NodeID: "n1", DataDir: "/tmp", Bind: "0.0.0.0:9000", Bootstrap: true, Join: []string{"a"}}, "mutually exclusive"},
		{"neither Bootstrap nor Join nor Recover", Config{NodeID: "n1", DataDir: "/tmp", Bind: "0.0.0.0:9000"}, "either Bootstrap, Join, or Recover"},
		{"Recover with Bootstrap", Config{NodeID: "n1", DataDir: "/tmp", Bind: "0.0.0.0:9000", Recover: true, Bootstrap: true}, "Recover cannot be combined"},
		{"Recover with Join", Config{NodeID: "n1", DataDir: "/tmp", Bind: "0.0.0.0:9000", Recover: true, Join: []string{"a"}}, "Recover cannot be combined"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.validate()
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error %q should contain %q", err, tt.wantErr)
			}
		})
	}
}

func TestConfig_ApplyDefaultsAll(t *testing.T) {
	cfg := Config{}
	cfg.applyDefaults()

	if cfg.HeartbeatTimeout == 0 {
		t.Error("HeartbeatTimeout should have default")
	}
	if cfg.ElectionTimeout == 0 {
		t.Error("ElectionTimeout should have default")
	}
	if cfg.SnapshotInterval == 0 {
		t.Error("SnapshotInterval should have default")
	}
	if cfg.SnapshotThreshold == 0 {
		t.Error("SnapshotThreshold should have default")
	}
	if cfg.ApplyTimeout == 0 {
		t.Error("ApplyTimeout should have default")
	}
	if cfg.MaxPool == 0 {
		t.Error("MaxPool should have default")
	}
	if cfg.SQLiteReadConns == 0 {
		t.Error("SQLiteReadConns should have default")
	}
	if cfg.BatchMaxSize == 0 {
		t.Error("BatchMaxSize should have default")
	}
	if cfg.Advertise != "" {
		t.Error("Advertise should still be empty when Bind is empty")
	}
	if cfg.LogOutput == nil {
		t.Error("LogOutput should have default")
	}
}

func TestNode_MetricsCountersIncrement(t *testing.T) {
	node := testNode(t)
	db := node.DB()

	_, err := db.Exec("CREATE TABLE counter_test (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Record writes before our operations.
	beforeWrites := node.metrics.writesTotal.Load()

	_, err = db.Exec("INSERT INTO counter_test (val) VALUES (?)", "a")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	_, err = db.Exec("INSERT INTO counter_test (val) VALUES (?)", "b")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	afterWrites := node.metrics.writesTotal.Load()
	if afterWrites <= beforeWrites {
		t.Errorf("expected writesTotal to increase, before=%d after=%d", beforeWrites, afterWrites)
	}

	// Record reads before our operations.
	beforeReads := node.metrics.readsTotal.Load()

	var count int
	_ = db.QueryRow("SELECT count(*) FROM counter_test").Scan(&count)

	afterReads := node.metrics.readsTotal.Load()
	if afterReads <= beforeReads {
		t.Errorf("expected readsTotal to increase, before=%d after=%d", beforeReads, afterReads)
	}
}

func TestConsistencyFromContext(t *testing.T) {
	tests := []struct {
		name     string
		setLevel *ConsistencyLevel
		defLevel ConsistencyLevel
		want     ConsistencyLevel
	}{
		{"uses default when no context value", nil, ConsistencyWeak, ConsistencyWeak},
		{"uses context value when set", ptr(ConsistencyStrong), ConsistencyWeak, ConsistencyStrong},
		{"context None overrides default", ptr(ConsistencyNone), ConsistencyStrong, ConsistencyNone},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			if tt.setLevel != nil {
				ctx = WithConsistency(ctx, *tt.setLevel)
			}
			got := consistencyFromContext(ctx, tt.defLevel)
			if got != tt.want {
				t.Errorf("consistencyFromContext = %d, want %d", got, tt.want)
			}
		})
	}
}

func ptr(l ConsistencyLevel) *ConsistencyLevel { return &l }

func TestMarshalUnmarshalCommand(t *testing.T) {
	cmd := &Command{
		Type: CommandExecuteMulti,
		DB:   "default",
		Statements: []Statement{
			{SQL: "INSERT INTO t VALUES (?)", Args: []any{42}},
			{SQL: "INSERT INTO t VALUES (?)", Args: []any{"hello"}},
		},
	}

	data, err := marshalCommand(cmd)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	cmd2, err := unmarshalCommand(data)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if cmd2.Type != cmd.Type {
		t.Errorf("Type: got %d, want %d", cmd2.Type, cmd.Type)
	}
	if cmd2.DB != cmd.DB {
		t.Errorf("DB: got %q, want %q", cmd2.DB, cmd.DB)
	}
	if len(cmd2.Statements) != len(cmd.Statements) {
		t.Fatalf("Statements len: got %d, want %d", len(cmd2.Statements), len(cmd.Statements))
	}
}

func TestNode_StatsAndNodeID(t *testing.T) {
	node := testNode(t)

	if node.NodeID() == "" {
		t.Error("NodeID should not be empty")
	}

	stats := node.Stats()
	if stats == nil {
		t.Fatal("Stats should not be nil")
	}
	if _, ok := stats["state"]; !ok {
		t.Error("Stats should contain 'state' key")
	}
}

func TestNode_CloseTwice(t *testing.T) {
	node := testNode(t)

	// The testNode helper already registers a cleanup that calls Close.
	// Calling Close explicitly first should be safe (idempotent).
	if err := node.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	// Second close should be a no-op.
	if err := node.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

// =============================================================================
// TLS stream layer tests
// =============================================================================

// generateTestCert creates a self-signed CA + server certificate/key for tests.
func generateTestTLSConfig(t *testing.T) *tls.Config {
	t.Helper()

	// CA key and cert.
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{Organization: []string{"Test CA"}},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caCertDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create CA cert: %v", err)
	}
	caCert, err := x509.ParseCertificate(caCertDER)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}

	// Server key and cert.
	srvKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate server key: %v", err)
	}
	srvTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:     []string{"localhost"},
	}
	srvCertDER, err := x509.CreateCertificate(rand.Reader, srvTemplate, caCert, &srvKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create server cert: %v", err)
	}

	pool := x509.NewCertPool()
	pool.AddCert(caCert)

	return &tls.Config{
		Certificates: []tls.Certificate{{
			Certificate: [][]byte{srvCertDER},
			PrivateKey:  srvKey,
		}},
		RootCAs:    pool,
		ClientCAs:  pool,
		ClientAuth: tls.RequireAndVerifyClientCert,
	}
}

func TestTLSStreamLayer_AcceptCloseAddr(t *testing.T) {
	tlsCfg := generateTestTLSConfig(t)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	serverTLS := tlsCfg.Clone()
	tlsLn := tls.NewListener(ln, serverTLS)

	advertise, _ := net.ResolveTCPAddr("tcp", tlsLn.Addr().String())

	layer := &tlsStreamLayer{
		listener:  tlsLn,
		advertise: advertise,
		tlsConfig: tlsCfg,
	}

	// Test Addr() with advertise set.
	addr := layer.Addr()
	if addr == nil {
		t.Fatal("Addr() should not return nil")
	}
	if addr.String() != advertise.String() {
		t.Errorf("Addr() = %q, want %q", addr.String(), advertise.String())
	}

	// Test Addr() with nil advertise falls back to listener.
	layer2 := &tlsStreamLayer{listener: tlsLn, tlsConfig: tlsCfg}
	addr2 := layer2.Addr()
	if addr2 == nil {
		t.Fatal("Addr() with nil advertise should not return nil")
	}

	// Test Close.
	if err := layer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Accept should fail after close.
	_, err = layer.Accept()
	if err == nil {
		t.Fatal("Accept after Close should return error")
	}
}

func TestTLSStreamLayer_DialInvalidAddr(t *testing.T) {
	tlsCfg := generateTestTLSConfig(t)
	clientTLS := tlsCfg.Clone()
	clientTLS.ServerName = "localhost"

	layer := &tlsStreamLayer{
		tlsConfig: clientTLS,
	}

	// Dial to an invalid address should fail.
	conn, err := layer.Dial(raft.ServerAddress("127.0.0.1:1"), 200*time.Millisecond)
	if err == nil {
		conn.Close()
		t.Fatal("Dial to invalid address should fail")
	}
}

// =============================================================================
// FSM Release test
// =============================================================================

func TestFSMSnapshot_ReleaseNoop(t *testing.T) {
	// Release is a no-op but must not panic.
	snap := &fsmSnapshot{}
	snap.Release()
}

// =============================================================================
// Lease-based consistency test
// =============================================================================

func TestConsistencyLease_LocalReadWhenValid(t *testing.T) {
	node := testNode(t)
	db := node.DB()

	_, err := db.Exec("CREATE TABLE lease_test (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	_, err = db.Exec("INSERT INTO lease_test (val) VALUES (?)", "leased")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Extend the lease so it's valid.
	node.lease.extend(5 * time.Second)

	ctx := WithConsistency(context.Background(), ConsistencyLease)
	var val string
	err = db.QueryRowContext(ctx, "SELECT val FROM lease_test WHERE id = 1").Scan(&val)
	if err != nil {
		t.Fatalf("lease read: %v", err)
	}
	if val != "leased" {
		t.Fatalf("expected 'leased', got %q", val)
	}
}

// =============================================================================
// RPCPool with stale connection (maxIdle expired)
// =============================================================================

func TestRPCPool_StaleConnectionReconnects(t *testing.T) {
	srv := rpc.NewServer()
	srv.RegisterName("Colmena", &testRPCService{})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go srv.ServeConn(conn)
		}
	}()

	_, portStr, _ := net.SplitHostPort(ln.Addr().String())
	var rpcPort int
	fmt.Sscanf(portStr, "%d", &rpcPort)
	raftAddr := fmt.Sprintf("127.0.0.1:%d", rpcPort-1)

	pool := newRPCPool(nil, "test")
	defer pool.close()

	// Override maxIdle to a tiny value to simulate staleness.
	pool.maxIdle = 1 * time.Nanosecond

	c1, err := pool.get(raftAddr)
	if err != nil {
		t.Fatalf("first get: %v", err)
	}

	// Sleep briefly to ensure the entry is considered stale.
	time.Sleep(1 * time.Millisecond)

	c2, err := pool.get(raftAddr)
	if err != nil {
		t.Fatalf("second get (after stale): %v", err)
	}

	if c1 == c2 {
		t.Fatal("expected different client after maxIdle expiry")
	}
}

// =============================================================================
// RPCPool dial with TLS
// =============================================================================

func TestRPCPool_DialWithTLS(t *testing.T) {
	tlsCfg := generateTestTLSConfig(t)

	serverTLS := tlsCfg.Clone()
	serverTLS.ClientAuth = tls.RequireAndVerifyClientCert
	ln, err := tls.Listen("tcp", "127.0.0.1:0", serverTLS)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	srv := rpc.NewServer()
	srv.RegisterName("Colmena", &testRPCService{})
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go srv.ServeConn(conn)
		}
	}()

	_, portStr, _ := net.SplitHostPort(ln.Addr().String())
	var rpcPort int
	fmt.Sscanf(portStr, "%d", &rpcPort)
	raftAddr := fmt.Sprintf("127.0.0.1:%d", rpcPort-1)

	clientTLS := tlsCfg.Clone()
	clientTLS.ClientAuth = tls.NoClientCert
	pool := newRPCPool(clientTLS, "test")
	defer pool.close()

	client, err := pool.get(raftAddr)
	if err != nil {
		t.Fatalf("get with TLS: %v", err)
	}

	var resp RPCPingResponse
	if err := client.Call("Colmena.Ping", &RPCPingRequest{}, &resp); err != nil {
		t.Fatalf("ping via TLS: %v", err)
	}
}

// =============================================================================
// Additional batcher coverage: multiple concurrent batches that exceed maxBatch
// =============================================================================

func TestBatcher_MultipleBatchesConcurrent(t *testing.T) {
	node := testNode(t, func(cfg *Config) {
		cfg.BatchWindow = 2 * time.Millisecond
		cfg.BatchMaxSize = 5
	})
	db := node.DB()

	_, err := db.Exec("CREATE TABLE batch_multi (id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Submit more than maxBatch, triggering multiple flush cycles.
	const numWriters = 25
	var wg sync.WaitGroup
	errCh := make(chan error, numWriters)

	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, err := db.Exec("INSERT INTO batch_multi (val) VALUES (?)", fmt.Sprintf("v-%d", idx))
			if err != nil {
				errCh <- fmt.Errorf("writer %d: %w", idx, err)
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Error(err)
	}

	time.Sleep(100 * time.Millisecond)

	var count int
	if err := db.QueryRow("SELECT count(*) FROM batch_multi").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != numWriters {
		t.Fatalf("expected %d rows, got %d", numWriters, count)
	}
}

// =============================================================================
// Batcher ExecMulti (transaction) with batching enabled
// =============================================================================

func TestBatcher_ExecMultiWithBatching(t *testing.T) {
	node := testNode(t, func(cfg *Config) {
		cfg.BatchWindow = 5 * time.Millisecond
		cfg.BatchMaxSize = 10
	})
	db := node.DB()

	_, err := db.Exec("CREATE TABLE batch_tx (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	results, err := node.ExecMulti([]Statement{
		{SQL: "INSERT INTO batch_tx (id, val) VALUES (1, 'a')"},
		{SQL: "INSERT INTO batch_tx (id, val) VALUES (2, 'b')"},
		{SQL: "INSERT INTO batch_tx (id, val) VALUES (3, 'c')"},
	})
	if err != nil {
		t.Fatalf("exec multi: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	time.Sleep(50 * time.Millisecond)

	var count int
	if err := db.QueryRow("SELECT count(*) FROM batch_tx").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 3 {
		t.Fatalf("expected 3, got %d", count)
	}
}

// =============================================================================
// Node.New with TLS config (exercises the TLS transport branch)
// =============================================================================

func TestNode_NewWithTLSConfig(t *testing.T) {
	tlsCfg := generateTestTLSConfig(t)

	node := testNode(t, func(cfg *Config) {
		cfg.TLSConfig = tlsCfg
	})

	// Verify node is operational.
	if !node.IsLeader() {
		t.Error("node should be leader")
	}

	db := node.DB()
	_, err := db.Exec("CREATE TABLE tls_test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("create table via TLS node: %v", err)
	}
}

// =============================================================================
// Additional driver coverage
// =============================================================================

func TestQueryContext_AllConsistencyLevels(t *testing.T) {
	node := testNode(t)
	db := node.DB()

	_, err := db.Exec("CREATE TABLE cons_test (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	_, err = db.Exec("INSERT INTO cons_test (id, val) VALUES (1, 'x')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	levels := []ConsistencyLevel{
		ConsistencyNone,
		ConsistencyWeak,
		ConsistencyStrong,
		ConsistencyLease,
		ConsistencyLevel(99), // unknown level - should fall through to default
	}

	node.lease.extend(5 * time.Second)

	for _, level := range levels {
		ctx := WithConsistency(context.Background(), level)
		var val string
		err := db.QueryRowContext(ctx, "SELECT val FROM cons_test WHERE id = 1").Scan(&val)
		if err != nil {
			t.Errorf("query with level %d: %v", level, err)
			continue
		}
		if val != "x" {
			t.Errorf("level %d: expected 'x', got %q", level, val)
		}
	}
}

// =============================================================================
// Migrate error: bad SQL
// =============================================================================

func TestMigrate_BadSQLFails(t *testing.T) {
	node := testNode(t)

	migrations := []Migration{
		{
			Version: 1,
			Name:    "bad_sql",
			SQL:     "THIS IS NOT VALID SQL",
		},
	}

	err := node.Migrate(migrations)
	if err == nil {
		t.Fatal("expected error for bad SQL migration")
	}
	if !strings.Contains(err.Error(), "migration 1") {
		t.Errorf("error should reference migration number: %v", err)
	}
}

// =============================================================================
// RPCPing direct test (covers the Ping method on RPCService)
// =============================================================================

func TestRPCService_PingDirect(t *testing.T) {
	node := testNode(t)
	svc := &RPCService{node: node}

	req := &RPCPingRequest{}
	resp := &RPCPingResponse{}
	err := svc.Ping(req, resp)
	if err != nil {
		t.Fatalf("Ping: %v", err)
	}
}

// =============================================================================
// Handler forwarding tests
// =============================================================================

func TestHandler_LeaderLocalExecution(t *testing.T) {
	type PingReq struct{ Msg string }
	type PingResp struct{ Reply string }

	node := testNode(t)

	key := NewHandlerKey[PingReq, PingResp]("test.ping")
	RegisterHandler(node, key, func(req PingReq) (PingResp, error) {
		return PingResp{Reply: "pong: " + req.Msg}, nil
	})

	resp, err := Forward(node, key, PingReq{Msg: "hello"})
	if err != nil {
		t.Fatalf("forward: %v", err)
	}
	if resp.Reply != "pong: hello" {
		t.Fatalf("expected 'pong: hello', got %q", resp.Reply)
	}
}

func TestHandler_ErrorPropagation(t *testing.T) {
	type Req struct{}
	type Resp struct{}

	node := testNode(t)

	key := NewHandlerKey[Req, Resp]("test.fail")
	RegisterHandler(node, key, func(req Req) (Resp, error) {
		return Resp{}, errors.New("handler error")
	})

	_, err := Forward(node, key, Req{})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "handler error") {
		t.Fatalf("expected 'handler error', got %q", err)
	}
}

func TestHandler_UnknownKey(t *testing.T) {
	type Req struct{}
	type Resp struct{}

	node := testNode(t)

	key := NewHandlerKey[Req, Resp]("test.nonexistent")
	_, err := Forward(node, key, Req{})
	if err == nil {
		t.Fatal("expected error for unknown handler")
	}
	if !strings.Contains(err.Error(), "unknown handler") {
		t.Fatalf("expected 'unknown handler', got %q", err)
	}
}

func TestHandler_DuplicateRegistrationPanics(t *testing.T) {
	type Req struct{}
	type Resp struct{}

	node := testNode(t)

	key := NewHandlerKey[Req, Resp]("test.dup")
	RegisterHandler(node, key, func(req Req) (Resp, error) { return Resp{}, nil })

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on duplicate registration")
		}
		msg := fmt.Sprintf("%v", r)
		if !strings.Contains(msg, "already registered") {
			t.Fatalf("unexpected panic: %v", r)
		}
	}()

	RegisterHandler(node, key, func(req Req) (Resp, error) { return Resp{}, nil })
}

func TestHandler_ForwardFromFollower(t *testing.T) {
	type AddReq struct{ A, B int }
	type AddResp struct{ Sum int }

	leader := testNode(t)
	follower := testJoinNode(t, leader.config.Advertise)

	key := NewHandlerKey[AddReq, AddResp]("test.add")

	// Register on both nodes (as you would in a real app).
	RegisterHandler(leader, key, func(req AddReq) (AddResp, error) {
		return AddResp{Sum: req.A + req.B}, nil
	})
	RegisterHandler(follower, key, func(req AddReq) (AddResp, error) {
		return AddResp{Sum: req.A + req.B}, nil
	})

	// Wait for the follower to see the leader.
	if err := follower.WaitForLeader(5 * time.Second); err != nil {
		t.Fatalf("follower wait for leader: %v", err)
	}
	time.Sleep(500 * time.Millisecond)

	// Forward from follower — should be routed to the leader.
	resp, err := Forward(follower, key, AddReq{A: 3, B: 7})
	if err != nil {
		t.Fatalf("forward from follower: %v", err)
	}
	if resp.Sum != 10 {
		t.Fatalf("expected 10, got %d", resp.Sum)
	}
}

func TestHandler_RPCServiceForwardDirect(t *testing.T) {
	type Req struct{ X int }
	type Resp struct{ Y int }

	node := testNode(t)
	svc := &RPCService{node: node}

	key := NewHandlerKey[Req, Resp]("test.double")
	RegisterHandler(node, key, func(req Req) (Resp, error) {
		return Resp{Y: req.X * 2}, nil
	})

	rpcReq := &RPCForwardRequest{Handler: "test.double", Payload: []byte(`{"X":21}`)}
	var rpcResp RPCForwardResponse
	if err := svc.Forward(rpcReq, &rpcResp); err != nil {
		t.Fatalf("RPC Forward: %v", err)
	}
	if rpcResp.Error != "" {
		t.Fatalf("RPC Forward error: %s", rpcResp.Error)
	}
	if string(rpcResp.Payload) != `{"Y":42}` {
		t.Fatalf("expected {\"Y\":42}, got %s", rpcResp.Payload)
	}
}
