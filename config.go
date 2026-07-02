package colmena

import (
	"fmt"
	"io"
	"os"
)

// Config holds the configuration for a Colmena store.
type Config struct {
	// DataDir is the directory where the SQLite databases live. The default
	// database is <DataDir>/default.db; OpenDB("foo", …) uses <DataDir>/foo.db.
	// v1 raft deployments used the same layout, so existing data dirs open
	// as-is (leftover raft.db / snapshots/ are ignored).
	DataDir string

	// SQLiteReadConns is the size of the read-only connection pool per
	// database. Default: 4.
	SQLiteReadConns int

	// Backup enables continuous backup when set. The engine streams committed
	// WAL frames to the backend in small segments (point-in-time restore) and
	// starts each generation with a fresh snapshot. See BackupConfig.
	Backup *BackupConfig

	// LogOutput is where colmena writes its logs. Default: os.Stderr.
	LogOutput io.Writer

	// ── Deprecated v1 (raft) fields ─────────────────────────────────────────
	// Kept so v1 callers compile unchanged; all of them are ignored. Colmena
	// v2 is single-node: SQLite + continuous backup, no consensus.

	// Deprecated: ignored. Colmena no longer runs a cluster.
	NodeID string
	// Deprecated: ignored.
	Bind string
	// Deprecated: ignored.
	Advertise string
	// Deprecated: ignored.
	Bootstrap bool
	// Deprecated: ignored.
	Join []string
	// Deprecated: ignored. Reads are always local.
	Consistency ConsistencyLevel
}

func (c *Config) validate() error {
	if c.DataDir == "" {
		return fmt.Errorf("colmena: DataDir is required")
	}
	return nil
}

func (c *Config) applyDefaults() {
	if c.SQLiteReadConns <= 0 {
		c.SQLiteReadConns = 4
	}
	if c.LogOutput == nil {
		c.LogOutput = os.Stderr
	}
}

// ConsistencyLevel is kept for source compatibility with v1 callers.
// Reads are always local now, so every level behaves identically.
//
// Deprecated: has no effect in colmena v2.
type ConsistencyLevel int

const (
	// Deprecated: has no effect in colmena v2.
	ConsistencyNone ConsistencyLevel = iota
	// Deprecated: has no effect in colmena v2.
	ConsistencyWeak
	// Deprecated: has no effect in colmena v2.
	ConsistencyStrong
	// Deprecated: has no effect in colmena v2.
	ConsistencyLease
)
