package colmena

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"
)

func openTestNode(t *testing.T) (*Node, *sql.DB) {
	t.Helper()
	node, err := New(Config{DataDir: t.TempDir(), LogOutput: os.Stderr})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { node.Close() })
	return node, node.DB()
}

func TestOpenExecQuery(t *testing.T) {
	node, db := openTestNode(t)

	if _, err := db.Exec(`CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	res, err := db.Exec(`INSERT INTO kv (k, v) VALUES (?, ?)`, "hello", "world")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		t.Fatalf("rows affected = %d, want 1", n)
	}
	var v string
	if err := db.QueryRow(`SELECT v FROM kv WHERE k = ?`, "hello").Scan(&v); err != nil {
		t.Fatalf("select: %v", err)
	}
	if v != "world" {
		t.Fatalf("v = %q", v)
	}

	// Named databases live side by side.
	other := node.OpenDB("other", ConsistencyWeak)
	if _, err := other.Exec(`CREATE TABLE t (id INTEGER)`); err != nil {
		t.Fatalf("named db create: %v", err)
	}
	if _, err := os.Stat(filepath.Join(node.config.DataDir, "other.db")); err != nil {
		t.Fatalf("named db file: %v", err)
	}

	// Deprecated shims stay callable.
	if err := node.WaitForLeader(0); err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}
	if !node.IsLeader() {
		t.Fatal("IsLeader = false")
	}
}

func TestTransactions(t *testing.T) {
	_, db := openTestNode(t)
	if _, err := db.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`); err != nil {
		t.Fatal(err)
	}

	// Reads inside a transaction observe its own writes (v1 could not).
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	res, err := tx.Exec(`INSERT INTO t (v) VALUES ('a')`)
	if err != nil {
		t.Fatal(err)
	}
	id, err := res.LastInsertId()
	if err != nil || id != 1 {
		t.Fatalf("LastInsertId = %d, %v", id, err)
	}
	var count int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM t`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("read-your-writes count = %d, want 1", count)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Rollback discards.
	tx2, _ := db.Begin()
	tx2.Exec(`INSERT INTO t (v) VALUES ('b')`)
	tx2.Rollback()
	db.QueryRow(`SELECT COUNT(*) FROM t`).Scan(&count)
	if count != 1 {
		t.Fatalf("after rollback count = %d, want 1", count)
	}
}

func TestWALParser(t *testing.T) {
	node, db := openTestNode(t)
	if _, err := db.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB)`); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if _, err := db.Exec(`INSERT INTO t (v) VALUES (randomblob(100))`); err != nil {
			t.Fatal(err)
		}
	}
	walPath := filepath.Join(node.config.DataDir, "default.db") + "-wal"
	hdr, committed, err := walCommittedSize(walPath)
	if err != nil {
		t.Fatalf("walCommittedSize: %v", err)
	}
	st, err := os.Stat(walPath)
	if err != nil {
		t.Fatal(err)
	}
	if committed != st.Size() {
		t.Fatalf("committed = %d, file = %d (all frames were commits)", committed, st.Size())
	}
	if (committed-walHeaderSize)%hdr.frameSize() != 0 {
		t.Fatalf("committed %d not frame-aligned (frame size %d)", committed, hdr.frameSize())
	}

	// A torn tail (half a frame of garbage) must not move the boundary.
	f, _ := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0)
	f.Write(make([]byte, hdr.frameSize()/2))
	f.Close()
	_, committed2, err := walCommittedSize(walPath)
	if err != nil {
		t.Fatal(err)
	}
	if committed2 != committed {
		t.Fatalf("torn tail moved boundary: %d != %d", committed2, committed)
	}
}
