package colmena

// Regression tests for the bugs fixed in v0.11.0 (see BUGS.md):
//   #1 Query RPC not leadership-gated (stale forwarded reads)
//   #2 WriteBatcher merging statements from different DBs into one command
//   #3 rpcPool leaking failed connections
//   #8 write args JSON-coerced (int64 precision loss, []byte → base64 TEXT)
//   plus: LeaderAddr returning the node ID, time.Time args failing gob.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestBatcher_CrossDBIsolation verifies that concurrent writes to different
// databases inside the same batch window are never merged into a single
// command (which would execute one DB's statements against the other).
func TestBatcher_CrossDBIsolation(t *testing.T) {
	node := testNode(t, func(c *Config) {
		c.BatchWindow = 30 * time.Millisecond // wide window to force co-batching
	})

	dbA := node.OpenDB("batch_a", ConsistencyNone)
	dbB := node.OpenDB("batch_b", ConsistencyNone)

	// Distinct table names so a mis-routed statement fails loudly.
	if _, err := dbA.Exec("CREATE TABLE only_a (id INTEGER PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatalf("create only_a: %v", err)
	}
	if _, err := dbB.Exec("CREATE TABLE only_b (id INTEGER PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatalf("create only_b: %v", err)
	}

	const perDB = 20
	var wg sync.WaitGroup
	errs := make(chan error, perDB*2)
	for i := 0; i < perDB; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			if _, err := dbA.Exec("INSERT INTO only_a (v) VALUES (?)", fmt.Sprintf("a%d", i)); err != nil {
				errs <- fmt.Errorf("insert a%d: %w", i, err)
			}
		}(i)
		go func(i int) {
			defer wg.Done()
			if _, err := dbB.Exec("INSERT INTO only_b (v) VALUES (?)", fmt.Sprintf("b%d", i)); err != nil {
				errs <- fmt.Errorf("insert b%d: %w", i, err)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("cross-DB batch write failed: %v", err)
	}

	var countA, countB int
	if err := dbA.QueryRow("SELECT COUNT(*) FROM only_a").Scan(&countA); err != nil {
		t.Fatalf("count only_a: %v", err)
	}
	if err := dbB.QueryRow("SELECT COUNT(*) FROM only_b").Scan(&countB); err != nil {
		t.Fatalf("count only_b: %v", err)
	}
	if countA != perDB || countB != perDB {
		t.Fatalf("expected %d rows in each DB, got only_a=%d only_b=%d", perDB, countA, countB)
	}
}

// TestRPCQuery_NotLeaderGated verifies that the Query RPC handler refuses to
// serve on a non-leader instead of silently answering from local SQLite.
func TestRPCQuery_NotLeaderGated(t *testing.T) {
	leader := testNode(t)
	follower := testJoinNode(t, leader.config.Bind)
	time.Sleep(1 * time.Second)

	svc := &RPCService{node: follower}
	var resp RPCQueryResponse
	if err := svc.Query(&RPCQueryRequest{DB: "default", SQL: "SELECT 1"}, &resp); err != nil {
		t.Fatalf("query rpc: %v", err)
	}
	if resp.Error != "not the leader" {
		t.Fatalf("expected 'not the leader' from follower Query handler, got %q", resp.Error)
	}
}

// TestStrongRead_FromFollower verifies the end-to-end Strong path: the
// follower forwards with the consistency level on the wire and the leader
// re-verifies quorum before reading.
func TestStrongRead_FromFollower(t *testing.T) {
	leader := testNode(t)
	follower := testJoinNode(t, leader.config.Bind)
	time.Sleep(1 * time.Second)

	if _, err := leader.DB().Exec("CREATE TABLE strong_t (id INTEGER PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := leader.DB().Exec("INSERT INTO strong_t (v) VALUES ('x')"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	db := follower.OpenDB("default", ConsistencyStrong)
	var v string
	if err := db.QueryRow("SELECT v FROM strong_t WHERE id = 1").Scan(&v); err != nil {
		t.Fatalf("strong read from follower: %v", err)
	}
	if v != "x" {
		t.Fatalf("expected 'x', got %q", v)
	}
}

// TestNode_LeaderAddrReturnsAddress pins the LeaderAddr regression: it used
// to return the leader's node ID instead of its advertise address.
func TestNode_LeaderAddrReturnsAddress(t *testing.T) {
	node := testNode(t)
	if got := node.LeaderAddr(); got != node.config.Advertise {
		t.Fatalf("LeaderAddr() = %q, want advertise address %q", got, node.config.Advertise)
	}
	if got := node.LeaderID(); got != node.config.NodeID {
		t.Fatalf("LeaderID() = %q, want node ID %q", got, node.config.NodeID)
	}
}

// TestWriteArgs_TypePreservation verifies that int64 args above 2^53 and
// []byte args survive replication intact (command format v2). With v1 they
// were JSON-coerced: big ints lost precision as float64 and blobs were
// stored as base64 TEXT.
func TestWriteArgs_TypePreservation(t *testing.T) {
	node := testNode(t)
	db := node.DB()

	if _, err := db.Exec("CREATE TABLE typed (id INTEGER PRIMARY KEY, big INTEGER, blob BLOB)"); err != nil {
		t.Fatalf("create: %v", err)
	}

	bigInt := int64(1)<<60 + 7 // not representable as float64
	blob := []byte{0x00, 0xff, 0x10, 0x80}

	// Plain Exec path (CommandExecute).
	if _, err := db.Exec("INSERT INTO typed (id, big, blob) VALUES (1, ?, ?)", bigInt, blob); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Buffered transaction path (CommandExecuteMulti).
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := tx.Exec("INSERT INTO typed (id, big, blob) VALUES (2, ?, ?)", bigInt, blob); err != nil {
		t.Fatalf("tx insert: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// ExecMulti path.
	if _, err := node.ExecMulti([]Statement{
		{SQL: "INSERT INTO typed (id, big, blob) VALUES (3, ?, ?)", Args: []any{bigInt, blob}},
	}); err != nil {
		t.Fatalf("execmulti: %v", err)
	}

	for id := 1; id <= 3; id++ {
		var gotInt int64
		var gotBlob []byte
		if err := db.QueryRow("SELECT big, blob FROM typed WHERE id = ?", id).Scan(&gotInt, &gotBlob); err != nil {
			t.Fatalf("select id=%d: %v", id, err)
		}
		if gotInt != bigInt {
			t.Errorf("id=%d: big = %d, want %d (precision lost)", id, gotInt, bigInt)
		}
		if !bytes.Equal(gotBlob, blob) {
			t.Errorf("id=%d: blob = %x, want %x (corrupted)", id, gotBlob, blob)
		}
	}
}

// TestCommandV1_DecodeCompat pins the v1 decoding behavior: numeric args in
// old log entries must keep decoding as float64 — a replica that already
// applied them did so with float64 bindings, and a fresh replay must
// reproduce the same bytes.
func TestCommandV1_DecodeCompat(t *testing.T) {
	payload, err := json.Marshal(&Command{
		Type: CommandExecute,
		DB:   "default",
		Statements: []Statement{
			{SQL: "INSERT INTO t (x) VALUES (?)", Args: []any{int64(42)}},
		},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	data := encodeEnvelope(FormatKindCommand, 1, payload)

	cmd, err := unmarshalCommand(data)
	if err != nil {
		t.Fatalf("unmarshal v1: %v", err)
	}
	arg := cmd.Statements[0].Args[0]
	if _, ok := arg.(float64); !ok {
		t.Fatalf("v1 numeric arg decoded as %T, want float64 (legacy replay determinism)", arg)
	}
}

// TestCommandV2_RoundTrip verifies the current envelope round-trips typed
// args exactly.
func TestCommandV2_RoundTrip(t *testing.T) {
	now := time.Date(2026, 6, 10, 12, 30, 45, 123456789, time.UTC)
	cmd := &Command{
		Type: CommandExecuteMulti,
		DB:   "typed",
		Statements: []Statement{
			{SQL: "INSERT INTO t VALUES (?, ?, ?, ?, ?)", Args: []any{
				int64(1)<<60 + 7, []byte{0, 1, 2}, "s", true, now,
			}},
		},
	}
	data, err := marshalCommand(cmd)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if data[9] != 2 {
		t.Fatalf("expected command format v2 on the wire, got v%d", data[9])
	}
	got, err := unmarshalCommand(data)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	args := got.Statements[0].Args
	if v := args[0].(int64); v != int64(1)<<60+7 {
		t.Errorf("int64 arg = %d", v)
	}
	if v := args[1].([]byte); !bytes.Equal(v, []byte{0, 1, 2}) {
		t.Errorf("bytes arg = %x", v)
	}
	if v := args[2].(string); v != "s" {
		t.Errorf("string arg = %q", v)
	}
	if v := args[3].(bool); !v {
		t.Errorf("bool arg = %v", v)
	}
	if v := args[4].(time.Time); !v.Equal(now) {
		t.Errorf("time arg = %v, want %v", v, now)
	}
}

// TestForwardQuery_TimeArg verifies a forwarded read with a time.Time
// argument encodes over gob (time.Time needs explicit gob registration).
func TestForwardQuery_TimeArg(t *testing.T) {
	leader := testNode(t)
	follower := testJoinNode(t, leader.config.Bind)
	time.Sleep(1 * time.Second)

	if _, err := leader.DB().Exec("CREATE TABLE time_args (id INTEGER PRIMARY KEY, created TEXT)"); err != nil {
		t.Fatalf("create: %v", err)
	}

	_, err := follower.forwardQuery("default",
		"SELECT COUNT(*) FROM time_args WHERE created < ?",
		[]any{time.Now()}, ConsistencyWeak)
	if err != nil {
		t.Fatalf("forwarded query with time.Time arg: %v", err)
	}
}

// TestRPCPool_MarkFailedEvicts verifies markFailed closes and evicts the
// entry immediately instead of leaving the dead client (and its fd/reader
// goroutine) cached forever.
func TestRPCPool_MarkFailedEvicts(t *testing.T) {
	node := testNode(t)
	addr := node.config.Bind

	pool := newRPCPool(nil, "pool-test")
	defer pool.close()

	if _, err := pool.get(addr); err != nil {
		t.Fatalf("pool get: %v", err)
	}
	pool.mu.Lock()
	cached := len(pool.clients)
	pool.mu.Unlock()
	if cached != 1 {
		t.Fatalf("expected 1 cached client, got %d", cached)
	}

	pool.markFailed(addr)

	pool.mu.Lock()
	cached = len(pool.clients)
	pool.mu.Unlock()
	if cached != 0 {
		t.Fatalf("expected failed client to be evicted, still %d cached", cached)
	}
}
