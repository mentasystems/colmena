package colmena

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"
)

// Node is an open Colmena store: one data directory of SQLite databases with
// optional continuous backup. The name is inherited from v1 (where a Node was
// a raft cluster member); v2 is single-process, no cluster.
type Node struct {
	config  Config
	stores  *storeManager
	logger  *log.Logger
	backups map[string]*backupManager
}

// New opens (or creates) the store rooted at cfg.DataDir.
func New(cfg Config) (*Node, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	cfg.applyDefaults()

	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("colmena: create data dir: %w", err)
	}

	n := &Node{
		config:  cfg,
		stores:  newStoreManager(cfg.DataDir, cfg.SQLiteReadConns),
		logger:  log.New(cfg.LogOutput, "", log.LstdFlags),
		backups: map[string]*backupManager{},
	}

	// Attach the backup engine to every store as it opens (including the
	// lazily-created named ones).
	if cfg.Backup != nil {
		n.stores.onOpen = func(name string, st *store) error {
			bm, err := newBackupManager(name, st, *cfg.Backup, n.logger.Printf)
			if err != nil {
				return err
			}
			if err := bm.start(); err != nil {
				return fmt.Errorf("colmena: start backup for %q: %w", name, err)
			}
			n.backups[name] = bm
			return nil
		}
	}

	return n, nil
}

// DB returns a standard *sql.DB for the default database. Writes go through
// a single writer connection; reads through a read-only pool.
func (n *Node) DB() *sql.DB {
	return n.OpenDB("default", ConsistencyNone)
}

// OpenDB returns a *sql.DB for the named database (created on first use as
// <DataDir>/<name>.db). The consistency argument is a v1 leftover and has no
// effect — reads are always local.
func (n *Node) OpenDB(name string, consistency ConsistencyLevel) *sql.DB {
	return sql.OpenDB(&colmenaConnector{node: n, dbName: name})
}

// BackupStatus reports the state of the backup engine per database. Empty
// when backups are not configured. Wire it into a health endpoint to notice
// stuck replication.
func (n *Node) BackupStatus() map[string]BackupStatus {
	out := map[string]BackupStatus{}
	for name, bm := range n.backups {
		out[name] = bm.Status()
	}
	return out
}

// Close stops the backup engines (after a final sync) and closes every store.
func (n *Node) Close() error {
	for _, bm := range n.backups {
		bm.stop()
	}
	for _, bm := range n.backups {
		bm.backend.Close()
	}
	return n.stores.close()
}

// ── Deprecated v1 compatibility surface ─────────────────────────────────────

// WaitForLeader is a no-op kept for v1 callers; there is no cluster.
//
// Deprecated: remove the call; it returns immediately.
func (n *Node) WaitForLeader(timeout time.Duration) error { return nil }

// IsLeader always reports true; there is no cluster.
//
// Deprecated: remove the call.
func (n *Node) IsLeader() bool { return true }

// NodeID returns the configured (and otherwise unused) node id.
//
// Deprecated: NodeID is ignored by colmena v2.
func (n *Node) NodeID() string { return n.config.NodeID }

// Version returns the library version.
func (n *Node) Version() string { return LibraryVersion }
