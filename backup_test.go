package colmena

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// fakeClock advances deterministically so PITR boundaries are testable.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
	return c.t
}

// testBackup wires a node + local backend + manual-tick backup manager.
type testBackup struct {
	node    *Node
	db      *sql.DB
	backend *LocalBackend
	bm      *backupManager
	clock   *fakeClock
}

func newTestBackup(t *testing.T, cfg BackupConfig) *testBackup {
	t.Helper()
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	backend, err := NewLocalBackend(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	node, err := New(Config{DataDir: t.TempDir(), LogOutput: os.Stderr})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { node.Close() })
	st, err := node.stores.get("default")
	if err != nil {
		t.Fatal(err)
	}
	cfg.NewBackend = func(db string) (BackupBackend, error) { return backend, nil }
	cfg.now = clock.now
	cfg.applyDefaults()
	cfg.now = clock.now // applyDefaults keeps it, but be explicit
	bm, err := newBackupManager("default", st, cfg, node.logger.Printf)
	if err != nil {
		t.Fatal(err)
	}
	// Manual control: disable auto-checkpoint + initial snapshot, no goroutine.
	if _, err := st.writer.Exec("PRAGMA wal_autocheckpoint = 0"); err != nil {
		t.Fatal(err)
	}
	tb := &testBackup{node: node, db: node.DB(), backend: backend, bm: bm, clock: clock}
	if _, err := tb.db.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`); err != nil {
		t.Fatal(err)
	}
	if err := bm.takeSnapshot(); err != nil {
		t.Fatal(err)
	}
	return tb
}

func (tb *testBackup) insert(t *testing.T, v string) {
	t.Helper()
	if _, err := tb.db.Exec(`INSERT INTO t (v) VALUES (?)`, v); err != nil {
		t.Fatal(err)
	}
}

func (tb *testBackup) sync(t *testing.T) {
	t.Helper()
	if err := tb.bm.sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
}

// restoreValues restores (optionally at ts) and returns t's rows in order.
func (tb *testBackup) restoreValues(t *testing.T, ts time.Time) []string {
	t.Helper()
	dir := t.TempDir()
	err := Restore(context.Background(), tb.backend, dir, RestoreOptions{Timestamp: ts})
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	db, err := sql.Open("sqlite", "file:"+filepath.Join(dir, "default.db")+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query(`SELECT v FROM t ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		out = append(out, v)
	}
	return out
}

func TestBackupRestoreLatest(t *testing.T) {
	tb := newTestBackup(t, BackupConfig{})
	tb.insert(t, "a")
	tb.insert(t, "b")
	tb.clock.advance(time.Second)
	tb.sync(t)

	got := tb.restoreValues(t, time.Time{})
	if fmt.Sprint(got) != "[a b]" {
		t.Fatalf("restored %v, want [a b]", got)
	}

	// More writes + sync → restore picks them up too.
	tb.insert(t, "c")
	tb.clock.advance(time.Second)
	tb.sync(t)
	got = tb.restoreValues(t, time.Time{})
	if fmt.Sprint(got) != "[a b c]" {
		t.Fatalf("restored %v, want [a b c]", got)
	}
}

func TestBackupPITR(t *testing.T) {
	tb := newTestBackup(t, BackupConfig{})

	tb.insert(t, "v1")
	tb.clock.advance(time.Second)
	tb.sync(t)
	t1 := tb.clock.advance(time.Second) // point-in-time target: after v1, before v2

	tb.clock.advance(time.Second)
	tb.insert(t, "v2")
	tb.clock.advance(time.Second)
	tb.sync(t)

	if got := tb.restoreValues(t, time.Time{}); fmt.Sprint(got) != "[v1 v2]" {
		t.Fatalf("latest = %v, want [v1 v2]", got)
	}
	if got := tb.restoreValues(t, t1); fmt.Sprint(got) != "[v1]" {
		t.Fatalf("PITR@t1 = %v, want [v1]", got)
	}
}

func TestBackupCheckpointCycles(t *testing.T) {
	// Tiny threshold: every synced batch triggers a TRUNCATE → many indexes.
	tb := newTestBackup(t, BackupConfig{CheckpointThreshold: 1})

	var want []string
	for i := 0; i < 5; i++ {
		v := fmt.Sprintf("row%d", i)
		tb.insert(t, v)
		want = append(want, v)
		tb.clock.advance(time.Second)
		tb.sync(t) // ships + truncates → next insert starts a new WAL index
	}
	if tb.bm.walIndex == 0 {
		t.Fatalf("expected WAL index rotation, still at 0")
	}
	got := tb.restoreValues(t, time.Time{})
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("restored %v, want %v", got, want)
	}
}

func TestBackupNewGenerationAndPrune(t *testing.T) {
	tb := newTestBackup(t, BackupConfig{Retention: time.Hour})

	tb.insert(t, "old")
	tb.clock.advance(time.Second)
	tb.sync(t)

	// Second generation two hours later: the first falls out of retention.
	tb.clock.advance(2 * time.Hour)
	if err := tb.bm.takeSnapshot(); err != nil {
		t.Fatal(err)
	}
	gens, err := tb.backend.Generations(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(gens) != 1 {
		t.Fatalf("generations after prune = %d, want 1", len(gens))
	}
	tb.insert(t, "new")
	tb.clock.advance(time.Second)
	tb.sync(t)
	got := tb.restoreValues(t, time.Time{})
	if fmt.Sprint(got) != "[old new]" {
		t.Fatalf("restored %v, want [old new]", got)
	}
}

func TestRestoreEmptyBackend(t *testing.T) {
	backend, _ := NewLocalBackend(t.TempDir())
	err := Restore(context.Background(), backend, t.TempDir())
	if err == nil {
		t.Fatal("expected error on empty backend")
	}
}

func TestBackupStatusAndOnError(t *testing.T) {
	var mu sync.Mutex
	var reported []string
	tb := newTestBackup(t, BackupConfig{OnError: func(db string, err error) {
		mu.Lock()
		reported = append(reported, db+": "+err.Error())
		mu.Unlock()
	}})
	tb.insert(t, "x")
	tb.clock.advance(time.Second)
	tb.sync(t)
	st := tb.bm.Status()
	if st.Generation == "" || st.LastSyncAt.IsZero() {
		t.Fatalf("status incomplete: %+v", st)
	}
	tb.bm.report(fmt.Errorf("boom"))
	mu.Lock()
	n := len(reported)
	mu.Unlock()
	if n != 1 {
		t.Fatalf("OnError calls = %d, want 1", n)
	}
	if tb.bm.Status().LastError != "boom" {
		t.Fatalf("LastError = %q", tb.bm.Status().LastError)
	}
}

// TestNodeWithBackupIntegration exercises the real wiring: New() with a
// Backup config, the ticker goroutine, Close's final sync, and Restore.
func TestNodeWithBackupIntegration(t *testing.T) {
	backend, _ := NewLocalBackend(t.TempDir())
	node, err := New(Config{
		DataDir:   t.TempDir(),
		LogOutput: os.Stderr,
		Backup: &BackupConfig{
			NewBackend:   func(db string) (BackupBackend, error) { return backend, nil },
			SyncInterval: 50 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	db := node.DB()
	if _, err := db.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO t (v) VALUES ('live')`); err != nil {
		t.Fatal(err)
	}
	if st := node.BackupStatus()["default"]; st.Generation == "" {
		t.Fatalf("no generation in status: %+v", st)
	}
	if err := node.Close(); err != nil { // final sync ships the insert
		t.Fatal(err)
	}

	dir := t.TempDir()
	if err := Restore(context.Background(), backend, dir); err != nil {
		t.Fatal(err)
	}
	rdb, err := sql.Open("sqlite", "file:"+filepath.Join(dir, "default.db")+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer rdb.Close()
	var v string
	if err := rdb.QueryRow(`SELECT v FROM t`).Scan(&v); err != nil {
		t.Fatal(err)
	}
	if v != "live" {
		t.Fatalf("v = %q", v)
	}
}

// TestBackupFragmentedDB reproduces the production corruption: a database
// with free pages (from deletes) whose page numbering must survive the
// snapshot verbatim. A compacting snapshot (VACUUM INTO-style) renumbers
// pages and makes WAL replay corrupt indexes — this test catches that.
func TestBackupFragmentedDB(t *testing.T) {
	tb := newTestBackup(t, BackupConfig{})

	// Interleave two btrees, then punch holes in one: the free pages sit in
	// the middle of the file, so any compacting snapshot renumbers the pages
	// of the surviving btree.
	if _, err := tb.db.Exec(`CREATE TABLE u (id INTEGER PRIMARY KEY, v TEXT)`); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 500; i++ {
		tb.insert(t, fmt.Sprintf("bulk%04d", i))
		if _, err := tb.db.Exec(`INSERT INTO u (v) VALUES (?)`, fmt.Sprintf("keep%04d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tb.db.Exec(`DELETE FROM t WHERE v LIKE 'bulk0%'`); err != nil {
		t.Fatal(err)
	}
	tb.clock.advance(time.Second)
	tb.sync(t)

	// New generation on the fragmented file: snapshot must be page-faithful.
	tb.clock.advance(time.Hour)
	if err := tb.bm.takeSnapshot(); err != nil {
		t.Fatal(err)
	}

	// Writes after the snapshot arrive via WAL replay on restore. Touch both
	// btrees so replayed page images land on renumbered pages if the
	// snapshot compacted the file.
	tb.insert(t, "after-snapshot")
	for i := 0; i < 50; i++ {
		if _, err := tb.db.Exec(`INSERT INTO u (v) VALUES (?)`, fmt.Sprintf("post%04d", i)); err != nil {
			t.Fatal(err)
		}
	}
	tb.clock.advance(time.Second)
	tb.sync(t)

	got := tb.restoreValues(t, time.Time{}) // Restore runs integrity_check
	if len(got) == 0 || got[len(got)-1] != "after-snapshot" {
		t.Fatalf("restored tail = %v", got[max(0, len(got)-2):])
	}

	// And the source still matches the restore row-for-row.
	var srcCount int
	tb.db.QueryRow(`SELECT COUNT(*) FROM t`).Scan(&srcCount)
	if srcCount != len(got) {
		t.Fatalf("restored %d rows, source has %d", len(got), srcCount)
	}
}
