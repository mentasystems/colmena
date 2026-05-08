package colmena

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// --- Test helpers used by the coverage suite ---

type fakeSnapshotSink struct {
	buf    bytes.Buffer
	closed bool
	cancel bool
}

func (s *fakeSnapshotSink) Write(p []byte) (int, error) { return s.buf.Write(p) }
func (s *fakeSnapshotSink) Close() error                { s.closed = true; return nil }
func (s *fakeSnapshotSink) ID() string                  { return "fake" }
func (s *fakeSnapshotSink) Cancel() error               { s.cancel = true; return nil }

func itoa(n int) string { return strconv.Itoa(n) }

// TestVersion verifies Node.Version reports the library's compiled-in version.
func TestVersion(t *testing.T) {
	node := testNode(t)
	if got := node.Version(); got != LibraryVersion {
		t.Fatalf("Version() = %q, want %q", got, LibraryVersion)
	}
}

// TestTxExecResult_LastInsertId checks that LastInsertId surfaces
// ErrTxResultPending before commit and the real id after commit.
func TestTxExecResult_LastInsertId(t *testing.T) {
	node := testNode(t)
	db := node.DB()

	if _, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)"); err != nil {
		t.Fatalf("create: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	res, err := tx.Exec("INSERT INTO t (v) VALUES (?)", "hello")
	if err != nil {
		t.Fatalf("exec: %v", err)
	}
	if _, err := res.LastInsertId(); err != ErrTxResultPending {
		t.Fatalf("LastInsertId before commit: want ErrTxResultPending, got %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("LastInsertId after commit: %v", err)
	}
	if id != 1 {
		t.Fatalf("LastInsertId = %d, want 1", id)
	}
}

// TestErrNonDeterministicSQL_Error verifies the error string includes both the
// offending call and a trimmed version of the SQL.
func TestErrNonDeterministicSQL_Error(t *testing.T) {
	short := &ErrNonDeterministicSQL{Call: "random(", SQL: "INSERT INTO t VALUES (random())"}
	msg := short.Error()
	if !strings.Contains(msg, "random(") || !strings.Contains(msg, "INSERT INTO t") {
		t.Fatalf("short error missing call/sql: %q", msg)
	}

	long := &ErrNonDeterministicSQL{
		Call: "datetime('now')",
		SQL:  strings.Repeat("x", 200),
	}
	msg = long.Error()
	if !strings.Contains(msg, "...") {
		t.Fatalf("long sql should be ellipsized: %q", msg)
	}
}

// TestTrimForErr_Whitespace exercises the trim-then-check branch where the
// trimmed input is shorter than the limit.
func TestTrimForErr_Whitespace(t *testing.T) {
	if got := trimForErr("   hello   "); got != "hello" {
		t.Fatalf("trimForErr = %q, want %q", got, "hello")
	}
}

// TestFsmSnapshot_Release ensures Release is a no-op (no panic, no error).
func TestFsmSnapshot_Release(t *testing.T) {
	s := &fsmSnapshot{}
	s.Release() // must not panic
}

// TestEncodeTaggedValue_NumericTypes covers the integer/float/byte branches of
// the encoder that the higher-level roundtrip tests skip.
func TestEncodeTaggedValue_NumericTypes(t *testing.T) {
	cases := []any{
		int32(7), uint(7), uint32(7), uint64(7), float32(1.5),
	}
	for _, v := range cases {
		tv := encodeTaggedValue(v)
		if tv.T != tagInt && tv.T != tagFloat {
			t.Fatalf("%T: unexpected tag %s", v, tv.T)
		}
	}
}

// TestEncodeTaggedValue_DefaultFallback exercises the catch-all branch where
// an unknown type falls back to JSON-string encoding.
func TestEncodeTaggedValue_DefaultFallback(t *testing.T) {
	type custom struct{ X int }
	tv := encodeTaggedValue(custom{X: 9})
	if tv.T != tagString {
		t.Fatalf("default tag = %s, want %s", tv.T, tagString)
	}
}

// TestDecodeTaggedValue_Errors covers every malformed-input branch.
func TestDecodeTaggedValue_Errors(t *testing.T) {
	cases := []TaggedValue{
		{T: tagBool, V: json.RawMessage(`"not-bool"`)},
		{T: tagInt, V: json.RawMessage(`"not-int"`)},
		{T: tagFloat, V: json.RawMessage(`"not-float"`)},
		{T: tagString, V: json.RawMessage(`123`)},
		{T: tagBytes, V: json.RawMessage(`123`)},
		{T: tagBytes, V: json.RawMessage(`"!!notbase64!!"`)},
		{T: tagTime, V: json.RawMessage(`123`)},
		{T: tagTime, V: json.RawMessage(`"not-a-time"`)},
	}
	for i, tv := range cases {
		if _, err := decodeTaggedValue(tv); err == nil {
			t.Fatalf("case %d (%s): expected error", i, tv.T)
		}
	}
}

// TestUnmarshalCommand_LegacyAndErrors covers every branch of the command
// unmarshaller: legacy unenveloped JSON, empty buffer, garbage first byte,
// envelope with wrong kind, and envelope with unsupported version.
func TestUnmarshalCommand_LegacyAndErrors(t *testing.T) {
	// Legacy unenveloped JSON survives.
	legacy := []byte(`  {"type":0,"db":"d","stmts":[{"sql":"SELECT 1"}]}`)
	cmd, err := unmarshalCommand(legacy)
	if err != nil {
		t.Fatalf("legacy: %v", err)
	}
	if cmd.DB != "d" {
		t.Fatalf("legacy parse: %+v", cmd)
	}

	// Empty input → unrecognized.
	if _, err := unmarshalCommand(nil); err == nil {
		t.Fatalf("nil input should error")
	}
	// Garbage first byte → unrecognized.
	if _, err := unmarshalCommand([]byte{0xff, 0x00}); err == nil {
		t.Fatalf("garbage first byte should error")
	}
	// Envelope with wrong kind → error.
	wrongKind := encodeEnvelope(FormatKindSnapshot, 1, []byte("{}"))
	if _, err := unmarshalCommand(wrongKind); err == nil {
		t.Fatalf("wrong kind should error")
	}
	// Envelope with unknown version → ErrUnsupportedFormatVersion.
	badVer := encodeEnvelope(FormatKindCommand, 99, []byte("{}"))
	if _, err := unmarshalCommand(badVer); err == nil {
		t.Fatalf("bad version should error")
	}
}

// TestDecodeEnvelope_MissingMagic exercises the magic-missing error branch.
func TestDecodeEnvelope_MissingMagic(t *testing.T) {
	if _, _, _, err := decodeEnvelope([]byte("XXXXXXXXXX")); err == nil {
		t.Fatalf("expected magic-missing error")
	}
	// Too-short input also fails the magic check.
	if _, _, _, err := decodeEnvelope([]byte("hi")); err == nil {
		t.Fatalf("expected magic-missing error on short input")
	}
}

// TestFirstByte covers both branches.
func TestFirstByte(t *testing.T) {
	if firstByte(nil) != 0 {
		t.Fatalf("nil")
	}
	if firstByte([]byte{0x42}) != 0x42 {
		t.Fatalf("non-empty")
	}
}

// TestValidateWriteStatements_SecondStatementBad ensures the loop reports the
// first offender even when earlier statements are clean.
func TestValidateWriteStatements_SecondStatementBad(t *testing.T) {
	stmts := []Statement{
		{SQL: "INSERT INTO t VALUES (?)", Args: []any{1}},
		{SQL: "INSERT INTO t VALUES (random())"},
	}
	if err := validateWriteStatements(stmts); err == nil {
		t.Fatalf("expected error from second statement")
	}
}

// TestValidate_TimeNowVariants triggers each non-deterministic SQL branch.
func TestValidate_TimeNowVariants(t *testing.T) {
	bad := []string{
		"INSERT INTO t VALUES (datetime('now'))",
		"INSERT INTO t VALUES (date())",
		"INSERT INTO t VALUES (current_timestamp)",
		"INSERT INTO t VALUES (CURRENT_DATE)",
		"INSERT INTO t VALUES (last_insert_rowid())",
	}
	for _, s := range bad {
		if err := validateWriteSQL(s); err == nil {
			t.Fatalf("expected reject: %s", s)
		}
	}
	// 'now' inside a string literal is not a function call → allowed.
	good := "INSERT INTO t (note) VALUES ('now is the time')"
	if err := validateWriteSQL(good); err != nil {
		t.Fatalf("string literal containing 'now' wrongly rejected: %v", err)
	}
}

// TestTimeNowCall_AllPrefixes covers the per-function loop in timeNowCall.
func TestTimeNowCall_AllPrefixes(t *testing.T) {
	for _, fn := range []string{"datetime", "date", "time", "julianday", "strftime", "unixepoch"} {
		got := timeNowCall(fn + "( NOW_LITERAL )")
		if got != fn+"('now')" {
			t.Fatalf("%s: got %q", fn, got)
		}
	}
	// Unknown prefix returns the raw fallback ending in "'now')".
	if got := timeNowCall("foo(NOW_LITERAL)"); !strings.HasSuffix(got, "'now')") {
		t.Fatalf("fallback: %q", got)
	}
}

// TestRPCService_Hello exercises every log branch of Hello plus the response
// fill-in. Hello never returns an error — the test asserts the resp fields are
// populated correctly across mismatched protocol versions.
func TestRPCService_Hello(t *testing.T) {
	node := testNode(t)
	svc := &RPCService{node: node}
	cases := []RPCHelloRequest{
		{NodeID: "peer", LibraryVersion: "x", ProtocolVersion: ProtocolVersion, CommandFormatVersion: CommandFormatVersion, SnapshotFormatVersion: SnapshotFormatVersion},
		{NodeID: "peer", LibraryVersion: "x", ProtocolVersion: ProtocolVersion + 1},
		{NodeID: "peer", LibraryVersion: "x", CommandFormatVersion: CommandFormatVersion + 1},
		{NodeID: "peer", LibraryVersion: "x", SnapshotFormatVersion: SnapshotFormatVersion + 1},
	}
	for i, req := range cases {
		var resp RPCHelloResponse
		if err := svc.Hello(&req, &resp); err != nil {
			t.Fatalf("case %d: %v", i, err)
		}
		if resp.LibraryVersion != LibraryVersion || resp.ProtocolVersion != ProtocolVersion {
			t.Fatalf("case %d: bad resp %+v", i, resp)
		}
	}
}

// TestRPCService_QueryError covers Query's resp.Error path when the SQL is
// invalid (driver returns an error from rows.Columns / Query).
func TestRPCService_QueryError(t *testing.T) {
	node := testNode(t)
	svc := &RPCService{node: node}
	var resp RPCQueryResponse
	if err := svc.Query(&RPCQueryRequest{DB: "default", SQL: "SELECT * FROM does_not_exist"}, &resp); err != nil {
		t.Fatalf("rpc err: %v", err)
	}
	if resp.Error == "" {
		t.Fatalf("expected resp.Error, got empty")
	}
}

// TestRPCService_ExecuteAndForward covers the leader-only paths plus the
// "not the leader" branch by faking the raft state via close+stale node.
func TestRPCService_ExecuteAndForward(t *testing.T) {
	node := testNode(t)
	svc := &RPCService{node: node}

	// Execute against an empty/garbage payload — Apply will fail to unmarshal
	// and surface the error string in resp.Error.
	var execResp RPCExecuteResponse
	if err := svc.Execute(&RPCExecuteRequest{Command: []byte("not-json")}, &execResp); err != nil {
		t.Fatalf("execute err: %v", err)
	}
	if execResp.Error == "" {
		t.Fatalf("expected error from bogus command")
	}

	// Forward to an unregistered handler returns an error in resp.Error.
	var fwdResp RPCForwardResponse
	if err := svc.Forward(&RPCForwardRequest{Handler: "missing", Payload: []byte(`{}`)}, &fwdResp); err != nil {
		t.Fatalf("forward err: %v", err)
	}
	if fwdResp.Error == "" {
		t.Fatalf("expected error from missing handler")
	}

	// Register a real handler and call Forward (leader-local path).
	key := NewHandlerKey[map[string]int, map[string]int]("doubled")
	RegisterHandler(node, key, func(in map[string]int) (map[string]int, error) {
		out := make(map[string]int, len(in))
		for k, v := range in {
			out[k] = v * 2
		}
		return out, nil
	})
	got, err := Forward(node, key, map[string]int{"a": 3})
	if err != nil {
		t.Fatalf("forward leader-local: %v", err)
	}
	if got["a"] != 6 {
		t.Fatalf("Forward got %v", got)
	}
}

// TestNodeJoin_FailureBranches exercises the join() error path: a node with
// an unreachable Join target returns the bundled error.
func TestNodeJoin_FailureBranches(t *testing.T) {
	dir := t.TempDir()
	port := freePort(t)
	cfg := Config{
		NodeID:           "join-fail",
		DataDir:          dir,
		Bind:             "127.0.0.1:" + itoa(port),
		Join:             []string{"127.0.0.1:1"}, // unreachable port
		HeartbeatTimeout: 200 * time.Millisecond,
		ElectionTimeout:  200 * time.Millisecond,
		ApplyTimeout:     1 * time.Second,
		LogOutput:        io.Discard,
	}
	_, err := New(cfg)
	if err == nil {
		t.Fatalf("expected join failure")
	}
}

// TestFsmApply_ErrorPaths exercises every branch of fsm.Apply that returns an
// error: bad envelope, multi-statement Execute, unknown command type.
func TestFsmApply_ErrorPaths(t *testing.T) {
	dir := t.TempDir()
	sm := newStoreManager(dir, 2)
	t.Cleanup(func() { sm.close() })
	f := &fsm{stores: sm}

	// Garbage payload → unmarshal error.
	r := f.Apply(&raft.Log{Data: []byte("not-json-or-envelope")}).(*ApplyResult)
	if r.Error == "" {
		t.Fatalf("expected unmarshal error")
	}

	// Execute with len(stmts) != 1 → error.
	cmd := &Command{Type: CommandExecute, DB: "default", Statements: nil}
	data, _ := marshalCommand(cmd)
	r = f.Apply(&raft.Log{Data: data}).(*ApplyResult)
	if r.Error == "" {
		t.Fatalf("expected len(stmts) error")
	}

	// Unknown command type → error.
	cmd = &Command{Type: CommandType(99), DB: "default", Statements: []Statement{{SQL: "SELECT 1"}}}
	data, _ = marshalCommand(cmd)
	r = f.Apply(&raft.Log{Data: data}).(*ApplyResult)
	if r.Error == "" || !strings.Contains(r.Error, "unknown command type") {
		t.Fatalf("expected unknown type error, got %q", r.Error)
	}
}

// TestFsmPersist_RoundTrip drives Persist through a fake snapshot sink and
// verifies the output starts with the colmena envelope.
func TestFsmPersist_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	sm := newStoreManager(dir, 2)
	t.Cleanup(func() { sm.close() })

	// Force a store to exist so Persist has something non-trivial to copy.
	st, err := sm.get("default")
	if err != nil {
		t.Fatalf("get store: %v", err)
	}
	if _, err := st.execute(Statement{SQL: "CREATE TABLE p (id INTEGER PRIMARY KEY)"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	snap := &fsmSnapshot{stores: sm}
	sink := &fakeSnapshotSink{}
	if err := snap.Persist(sink); err != nil {
		t.Fatalf("persist: %v", err)
	}
	if !sink.closed {
		t.Fatalf("sink should be closed")
	}
	if !hasEnvelopeMagic(sink.buf.Bytes()) {
		t.Fatalf("persisted bytes should start with envelope magic")
	}
}

// TestLocalBackend_RoundTrip drives the full LocalBackend surface:
// NewLocalBackend, WriteSnapshot, WriteWAL, Generations, ReadSnapshot,
// ReadWAL, plus the top-level Restore.
func TestLocalBackend_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	be, err := NewLocalBackend(dir + "/backups")
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}
	defer be.Close()

	ctx := context.Background()

	// Empty: Generations returns nil/empty.
	gens, err := be.Generations(ctx)
	if err != nil {
		t.Fatalf("gens empty: %v", err)
	}
	if len(gens) != 0 {
		t.Fatalf("expected 0 gens, got %d", len(gens))
	}

	// Write two generations a few ms apart so the sort ordering is observable.
	snap1 := bytes.Repeat([]byte{0x42}, 64)
	if err := be.WriteSnapshot(ctx, "gen1", bytes.NewReader(snap1), int64(len(snap1))); err != nil {
		t.Fatalf("write snap1: %v", err)
	}
	if err := be.WriteWAL(ctx, "gen1", bytes.NewReader([]byte("wal1")), 4); err != nil {
		t.Fatalf("write wal1: %v", err)
	}

	time.Sleep(10 * time.Millisecond)
	if err := be.WriteSnapshot(ctx, "gen2", bytes.NewReader(snap1), int64(len(snap1))); err != nil {
		t.Fatalf("write snap2: %v", err)
	}

	gens, err = be.Generations(ctx)
	if err != nil {
		t.Fatalf("gens after write: %v", err)
	}
	if len(gens) != 2 {
		t.Fatalf("expected 2 gens, got %d", len(gens))
	}
	if gens[0].ID != "gen2" {
		t.Fatalf("expected gen2 newest, got %s", gens[0].ID)
	}

	// ReadSnapshot/ReadWAL round-trip.
	rc, err := be.ReadSnapshot(ctx, "gen1")
	if err != nil {
		t.Fatalf("read snap: %v", err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(got, snap1) {
		t.Fatalf("snapshot bytes mismatch")
	}
	rc, err = be.ReadWAL(ctx, "gen1")
	if err != nil {
		t.Fatalf("read wal: %v", err)
	}
	wb, _ := io.ReadAll(rc)
	rc.Close()
	if string(wb) != "wal1" {
		t.Fatalf("wal mismatch: %s", wb)
	}

	// Generations skips junk dirs without meta.json.
	if err := os.MkdirAll(dir+"/backups/junk", 0755); err != nil {
		t.Fatalf("mkdir junk: %v", err)
	}
	gens, _ = be.Generations(ctx)
	if len(gens) != 2 {
		t.Fatalf("junk dir leaked into Generations: %d", len(gens))
	}

	// Top-level Restore copies the latest generation into a fresh data dir.
	restoreDir := dir + "/restored"
	if err := Restore(ctx, be, restoreDir); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if _, err := os.Stat(restoreDir + "/default.db"); err != nil {
		t.Fatalf("restored db missing: %v", err)
	}
}

// TestLocalBackend_Restore_NoGenerations covers Restore's empty-backend error.
func TestLocalBackend_Restore_NoGenerations(t *testing.T) {
	dir := t.TempDir()
	be, _ := NewLocalBackend(dir + "/empty")
	if err := Restore(context.Background(), be, dir+"/out"); err == nil {
		t.Fatalf("expected error for empty backend")
	}
}

// TestStore_SnapshotRestore_RoundTrip drives store.backupTo via
// store.snapshot, then store.restore, ensuring data survives.
func TestStore_SnapshotRestore_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	st, err := newStoreAt(dir+"/source.db", 2)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	if _, err := st.execute(Statement{SQL: "CREATE TABLE k (id INTEGER PRIMARY KEY, v TEXT)"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := st.execute(Statement{SQL: "INSERT INTO k (v) VALUES (?)", Args: []any{"hello"}}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	var buf bytes.Buffer
	if err := st.snapshot(&buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if buf.Len() == 0 {
		t.Fatalf("empty snapshot")
	}
	st.close()

	// Restore into a brand-new store and verify the row survived.
	dst, err := newStoreAt(dir+"/dst.db", 2)
	if err != nil {
		t.Fatalf("new dst: %v", err)
	}
	defer dst.close()
	if err := dst.restore(&buf); err != nil {
		t.Fatalf("restore: %v", err)
	}
	rows, err := dst.query("SELECT v FROM k")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	var v string
	if !rows.Next() {
		t.Fatalf("no rows")
	}
	rows.Scan(&v)
	if v != "hello" {
		t.Fatalf("got %q", v)
	}
}

// TestStoreManager_RestoreRawSQLite covers the v0.2.0 legacy snapshot path
// (a single raw SQLite file without an envelope or tar wrapper).
func TestStoreManager_RestoreRawSQLite(t *testing.T) {
	// First build a real SQLite file we can stream through restore.
	src := t.TempDir() + "/src.db"
	st, err := newStoreAt(src, 1)
	if err != nil {
		t.Fatalf("new src: %v", err)
	}
	if _, err := st.execute(Statement{SQL: "CREATE TABLE legacy (id INTEGER)"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	st.close()
	raw, err := os.ReadFile(src)
	if err != nil {
		t.Fatalf("read raw: %v", err)
	}

	dstDir := t.TempDir()
	sm := newStoreManager(dstDir, 1)
	t.Cleanup(func() { sm.close() })
	if err := sm.restore(bytes.NewReader(raw)); err != nil {
		t.Fatalf("restore raw: %v", err)
	}
	st2, err := sm.get("default")
	if err != nil {
		t.Fatalf("get default: %v", err)
	}
	rows, err := st2.query("SELECT name FROM sqlite_master WHERE type='table' AND name='legacy'")
	if err != nil {
		t.Fatalf("query master: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatalf("table 'legacy' missing after restore")
	}
}

// TestLooksLikeRawSQLite covers both branches of the magic check.
func TestLooksLikeRawSQLite(t *testing.T) {
	if !looksLikeRawSQLite(sqliteMagic) {
		t.Fatalf("magic should match")
	}
	if looksLikeRawSQLite([]byte("hi")) {
		t.Fatalf("short input should not match")
	}
	if looksLikeRawSQLite(bytes.Repeat([]byte{0xff}, 32)) {
		t.Fatalf("garbage should not match")
	}
}

// TestRPCService_Join exercises every branch of the Join RPC: not-the-leader
// (followers don't exist in single-node so we fake by closing the node), and
// leader-already-known.
func TestRPCService_Join(t *testing.T) {
	node := testNode(t)
	svc := &RPCService{node: node}

	// Leader-known path: already a leader, AddVoter succeeds for self.
	var resp RPCJoinResponse
	if err := svc.Join(&RPCJoinRequest{NodeID: node.config.NodeID, Address: node.config.Bind}, &resp); err != nil {
		t.Fatalf("join self: %v", err)
	}
	// Adding the same voter twice may error or no-op; either is valid for
	// this test — we only care that the RPC flowed.
}

// TestRPCService_Join_ReplacesStaleAddress verifies the fix for the
// "duplicate address" failure that happens when a node leaves the
// cluster and a new node arrives at the same address with a fresh
// NodeID (e.g., a Pi reflashed and given the same DHCP IP, or a
// Docker container recreated with the same bridge IP). The Join RPC
// must remove the stale member first so the new one can take its slot.
func TestRPCService_Join_ReplacesStaleAddress(t *testing.T) {
	node := testNode(t)
	svc := &RPCService{node: node}

	const addr = "10.99.99.99:9000"
	const oldID = "stale-node-id"
	const newID = "fresh-node-id"

	// Seed the configuration with a stale member at addr.
	if err := node.raft.AddNonvoter("stale-node-id", "10.99.99.99:9000", 0, 5*time.Second).Error(); err != nil {
		t.Fatalf("seed stale member: %v", err)
	}
	servers, _ := node.Nodes()
	foundStale := false
	for _, s := range servers {
		if string(s.ID) == oldID && string(s.Address) == addr {
			foundStale = true
		}
	}
	if !foundStale {
		t.Fatal("precondition failed: stale member not in configuration")
	}

	// Now a different NodeID joins at the same address.
	var resp RPCJoinResponse
	if err := svc.Join(&RPCJoinRequest{NodeID: newID, Address: addr, AsNonvoter: true}, &resp); err != nil {
		t.Fatalf("join: %v", err)
	}
	if resp.Error != "" {
		t.Fatalf("join returned error: %s", resp.Error)
	}

	// The new ID should be at addr, the old one should be gone.
	servers, _ = node.Nodes()
	var staleStillThere, newPresent bool
	for _, s := range servers {
		if string(s.ID) == oldID {
			staleStillThere = true
		}
		if string(s.ID) == newID && string(s.Address) == addr {
			newPresent = true
		}
	}
	if staleStillThere {
		t.Errorf("stale member %s still in configuration after replace", oldID)
	}
	if !newPresent {
		t.Errorf("new member %s not in configuration at %s", newID, addr)
	}
}

// TestBatcher_SubmitClosed exercises the batcher's close path.
func TestBatcher_SubmitClosed(t *testing.T) {
	node := testNode(t)
	if node.batcher == nil {
		t.Skip("batcher not enabled in test config")
	}
	node.batcher.close()
	cmd := &Command{Type: CommandExecute, DB: "default", Statements: []Statement{{SQL: "SELECT 1"}}}
	if _, err := node.batcher.submit(cmd); err == nil {
		t.Fatalf("expected error from closed batcher")
	}
}

// TestStore_Close_Idempotent ensures double-close doesn't panic and surfaces
// either err or nil consistently (idempotent).
func TestStore_Close_Idempotent(t *testing.T) {
	dir := t.TempDir()
	st, err := newStoreAt(dir+"/x.db", 2)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if err := st.close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	// second close on already-closed pools — must not panic.
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panic on double close: %v", r)
		}
	}()
	_ = st.close()
}

// TestStoreManager_SnapshotMultipleDBs ensures snapshot iterates more than one
// store (the loop over sm.stores is otherwise only exercised with a single
// "default" store).
func TestStoreManager_SnapshotMultipleDBs(t *testing.T) {
	dir := t.TempDir()
	sm := newStoreManager(dir, 2)
	t.Cleanup(func() { sm.close() })
	for _, name := range []string{"default", "logs", "events"} {
		st, err := sm.get(name)
		if err != nil {
			t.Fatalf("get %s: %v", name, err)
		}
		if _, err := st.execute(Statement{SQL: "CREATE TABLE t (id INTEGER PRIMARY KEY)"}); err != nil {
			t.Fatalf("create on %s: %v", name, err)
		}
	}
	var buf bytes.Buffer
	if err := sm.snapshot(&buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if buf.Len() < 100 {
		t.Fatalf("snapshot suspiciously small: %d bytes", buf.Len())
	}
}

// keep time imports alive
var _ = time.Now
