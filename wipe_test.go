package colmena

import (
	"os"
	"path/filepath"
	"testing"
)

// TestWipeLocalState verifies that wiping returns a data dir to a pristine
// "never joined" condition (HasExistingState false) by removing the Raft log,
// snapshots, and replicated SQLite stores, while leaving unrelated files and
// the directory itself intact.
func TestWipeLocalState(t *testing.T) {
	dir := t.TempDir()

	// Lay down a realistic Colmena footprint plus an unrelated file.
	files := []string{
		"raft.db",
		"colmena.db", "colmena.db-wal", "colmena.db-shm",
		"users.db", "users.db-wal",
		"keep.txt",
	}
	for _, f := range files {
		if err := os.WriteFile(filepath.Join(dir, f), []byte("x"), 0644); err != nil {
			t.Fatalf("write %s: %v", f, err)
		}
	}
	if err := os.MkdirAll(filepath.Join(dir, "snapshots", "1-2-3"), 0755); err != nil {
		t.Fatalf("mkdir snapshots: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "snapshots", "1-2-3", "state.bin"), []byte("s"), 0644); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}

	if err := WipeLocalState(dir); err != nil {
		t.Fatalf("WipeLocalState: %v", err)
	}

	// Raft state, snapshots, and every <name>.db (+ sidecars) must be gone.
	gone := []string{
		"raft.db", "snapshots",
		"colmena.db", "colmena.db-wal", "colmena.db-shm",
		"users.db", "users.db-wal",
	}
	for _, f := range gone {
		if _, err := os.Stat(filepath.Join(dir, f)); !os.IsNotExist(err) {
			t.Errorf("%s should have been removed (stat err=%v)", f, err)
		}
	}
	// Unrelated files and the dir itself survive.
	if _, err := os.Stat(filepath.Join(dir, "keep.txt")); err != nil {
		t.Errorf("keep.txt should survive: %v", err)
	}
	if _, err := os.Stat(dir); err != nil {
		t.Errorf("data dir should survive: %v", err)
	}

	// And the gate now reports a fresh node.
	if has, err := HasExistingState(dir); err != nil || has {
		t.Fatalf("HasExistingState after wipe = %v (err %v), want false", has, err)
	}
}

// TestWipeLocalStateMissingDir tolerates a non-existent directory (nothing to
// wipe is not an error).
func TestWipeLocalStateMissingDir(t *testing.T) {
	if err := WipeLocalState(filepath.Join(t.TempDir(), "does-not-exist")); err != nil {
		t.Fatalf("WipeLocalState on missing dir: %v", err)
	}
}
