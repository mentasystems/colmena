package colmena

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"
)

// BackupBackend is the storage interface for continuous backups.
// Implementations store database snapshots and WAL files.
type BackupBackend interface {
	// WriteSnapshot stores a full database snapshot for a generation.
	WriteSnapshot(ctx context.Context, generation string, r io.Reader, size int64) error

	// WriteWAL stores the current WAL file for a generation, replacing any previous one.
	WriteWAL(ctx context.Context, generation string, r io.Reader, size int64) error

	// Generations lists available backup generations, newest first.
	Generations(ctx context.Context) ([]Generation, error)

	// ReadSnapshot retrieves a snapshot for the given generation.
	ReadSnapshot(ctx context.Context, generation string) (io.ReadCloser, error)

	// ReadWAL retrieves the WAL file for the given generation.
	ReadWAL(ctx context.Context, generation string) (io.ReadCloser, error)

	// Close releases any resources held by the backend.
	Close() error
}

// Generation represents a backup generation (period between full snapshots).
type Generation struct {
	ID        string    `json:"id"`
	CreatedAt time.Time `json:"created_at"`
}

// BackupConfig configures the continuous backup engine.
type BackupConfig struct {
	// Backend is the storage backend for backups.
	Backend BackupBackend

	// SyncInterval is how often the WAL is backed up. Default: 1s.
	SyncInterval time.Duration

	// SnapshotInterval is how often full snapshots are taken. Default: 1h.
	// Each snapshot starts a new generation and checkpoints the WAL.
	SnapshotInterval time.Duration
}

func (c *BackupConfig) applyDefaults() {
	if c.SyncInterval == 0 {
		c.SyncInterval = 1 * time.Second
	}
	if c.SnapshotInterval == 0 {
		c.SnapshotInterval = 1 * time.Hour
	}
}

// backupManager runs the continuous backup loop.
type backupManager struct {
	store   *store
	backend BackupBackend
	cfg     BackupConfig

	generation string
	mu         sync.Mutex

	stopCh   chan struct{}
	doneCh   chan struct{}
	stopOnce sync.Once
}

func newBackupManager(store *store, cfg BackupConfig) *backupManager {
	cfg.applyDefaults()
	return &backupManager{
		store:   store,
		backend: cfg.Backend,
		cfg:     cfg,
		stopCh:  make(chan struct{}),
		doneCh:  make(chan struct{}),
	}
}

// start begins the backup loop in a goroutine.
func (b *backupManager) start() error {
	// Disable auto-checkpoint so we control when the WAL is compacted.
	if _, err := b.store.writer.Exec("PRAGMA wal_autocheckpoint = 0"); err != nil {
		return fmt.Errorf("colmena: disable auto-checkpoint: %w", err)
	}

	// Take initial full snapshot.
	if err := b.takeSnapshot(); err != nil {
		return fmt.Errorf("colmena: initial snapshot: %w", err)
	}

	go b.run()
	return nil
}

func (b *backupManager) run() {
	defer close(b.doneCh)

	syncTicker := time.NewTicker(b.cfg.SyncInterval)
	defer syncTicker.Stop()

	snapTicker := time.NewTicker(b.cfg.SnapshotInterval)
	defer snapTicker.Stop()

	for {
		select {
		case <-syncTicker.C:
			if err := b.syncWAL(); err != nil {
				log.Printf("colmena: backup WAL sync error: %v", err)
			}
		case <-snapTicker.C:
			if err := b.takeSnapshot(); err != nil {
				log.Printf("colmena: backup snapshot error: %v", err)
			}
		case <-b.stopCh:
			// Final WAL sync before stopping.
			if err := b.syncWAL(); err != nil {
				log.Printf("colmena: backup final WAL sync error: %v", err)
			}
			return
		}
	}
}

// stop gracefully stops the backup loop and waits for it to finish.
// Safe to call multiple times.
func (b *backupManager) stop() {
	b.stopOnce.Do(func() {
		close(b.stopCh)
	})
	<-b.doneCh
}

// takeSnapshot takes a full database snapshot, starts a new generation,
// and checkpoints the WAL to compact it.
//
// Ordering and mechanism are both load-bearing:
//
//   - The checkpoint runs BEFORE the snapshot. SQLite resets the WAL at the
//     first write after a completed checkpoint, and every frame written
//     before the snapshot is covered by the snapshot itself — so a reset can
//     never drop a frame the backend still needs. (Checkpointing after the
//     snapshot opened a window where frames written between the WAL upload
//     and the reset vanished from the backend until the next generation.)
//   - The snapshot uses the SQLite Online Backup API (store.backupTo), not a
//     raw copy of the main DB file. In WAL mode the main file alone is not
//     the current state, and a concurrent checkpoint can rewrite it mid-copy,
//     tearing the streamed snapshot.
//
// Restore correctness: the snapshot is complete up to its start; the WAL
// uploaded afterwards may overlap it, but WAL frames are page images, so
// replaying already-applied frames on restore is idempotent.
func (b *backupManager) takeSnapshot() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	ctx := context.Background()

	// PASSIVE checkpoint compacts the WAL without blocking writers.
	if _, err := b.store.writer.Exec("PRAGMA wal_checkpoint(PASSIVE)"); err != nil {
		log.Printf("colmena: checkpoint before snapshot (non-fatal): %v", err)
	}

	// Generate new generation ID.
	b.generation = generateID()

	tmpPath := b.store.dbPath + ".backup-snapshot"
	_ = os.Remove(tmpPath) // safe-ignore: best-effort pre-clean (a leftover would break VACUUM INTO); backupTo surfaces any real problem
	defer os.Remove(tmpPath)

	if err := b.store.backupTo(tmpPath); err != nil {
		return fmt.Errorf("backup snapshot: %w", err)
	}

	dbFile, err := os.Open(tmpPath)
	if err != nil {
		return fmt.Errorf("open snapshot file: %w", err)
	}
	defer dbFile.Close()

	stat, err := dbFile.Stat()
	if err != nil {
		return fmt.Errorf("stat snapshot file: %w", err)
	}

	if err := b.backend.WriteSnapshot(ctx, b.generation, dbFile, stat.Size()); err != nil {
		return fmt.Errorf("write snapshot: %w", err)
	}

	// Also back up the current WAL so writes since the snapshot are covered.
	if err := b.writeWALToBackend(ctx); err != nil {
		// WAL might not exist yet if no writes happened. That's OK.
		log.Printf("colmena: backup WAL after snapshot (non-fatal): %v", err)
	}

	return nil
}

// syncWAL copies the current WAL file to the backup backend.
func (b *backupManager) syncWAL() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.generation == "" {
		return nil // no snapshot taken yet
	}

	ctx := context.Background()
	return b.writeWALToBackend(ctx)
}

func (b *backupManager) writeWALToBackend(ctx context.Context) error {
	walPath := b.store.dbPath + "-wal"

	walFile, err := os.Open(walPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // no WAL file, nothing to sync
		}
		return fmt.Errorf("open WAL: %w", err)
	}
	defer walFile.Close()

	stat, err := walFile.Stat()
	if err != nil {
		return fmt.Errorf("stat WAL: %w", err)
	}

	if stat.Size() == 0 {
		return nil // empty WAL, nothing to sync
	}

	return b.backend.WriteWAL(ctx, b.generation, walFile, stat.Size())
}

// Restore downloads the latest backup and places it in dataDir.
// Call this before creating a Node to restore from a backup.
//
//	err := colmena.Restore(ctx, backend, "./data/node1")
//	node, err := colmena.New(cfg) // now starts with restored data
func Restore(ctx context.Context, backend BackupBackend, dataDir string) error {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return fmt.Errorf("colmena: create data dir: %w", err)
	}

	// Find latest generation.
	generations, err := backend.Generations(ctx)
	if err != nil {
		return fmt.Errorf("colmena: list generations: %w", err)
	}
	if len(generations) == 0 {
		return fmt.Errorf("colmena: no backups found")
	}

	latest := generations[0]
	dbPath := fmt.Sprintf("%s/%s", dataDir, "default.db")
	walPath := dbPath + "-wal"

	// Remove any existing database files.
	os.Remove(dbPath)
	os.Remove(walPath)
	os.Remove(dbPath + "-shm")

	// Download and write snapshot.
	snapReader, err := backend.ReadSnapshot(ctx, latest.ID)
	if err != nil {
		return fmt.Errorf("colmena: read snapshot: %w", err)
	}
	defer snapReader.Close()

	dbFile, err := os.Create(dbPath)
	if err != nil {
		return fmt.Errorf("colmena: create db file: %w", err)
	}
	if _, err := io.Copy(dbFile, snapReader); err != nil {
		dbFile.Close()
		return fmt.Errorf("colmena: write snapshot: %w", err)
	}
	dbFile.Close()

	// Download and write WAL (if available).
	walReader, err := backend.ReadWAL(ctx, latest.ID)
	if err != nil {
		// WAL might not exist. That's OK — snapshot alone is valid.
		log.Printf("colmena: no WAL for generation %s (snapshot-only restore)", latest.ID)
		return nil
	}
	defer walReader.Close()

	walFile, err := os.Create(walPath)
	if err != nil {
		return fmt.Errorf("colmena: create WAL file: %w", err)
	}
	if _, err := io.Copy(walFile, walReader); err != nil {
		walFile.Close()
		return fmt.Errorf("colmena: write WAL: %w", err)
	}
	walFile.Close()

	return nil
}

func generateID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}
