package colmena

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// BackupBackend is the storage interface for continuous backups. A backend
// instance stores the history of ONE database (implementations namespace by
// prefix). All payloads are opaque (gzip) blobs produced by the engine.
type BackupBackend interface {
	// WriteSnapshot stores the full database snapshot that opens a generation.
	WriteSnapshot(ctx context.Context, generation string, r io.Reader, size int64) error

	// WriteWALSegment stores one WAL segment for a generation.
	WriteWALSegment(ctx context.Context, generation string, seg WALSegmentInfo, r io.Reader, size int64) error

	// Generations lists generations, newest first (by CreatedAt).
	Generations(ctx context.Context) ([]Generation, error)

	// WALSegments lists a generation's WAL segments sorted by (Index, Offset).
	WALSegments(ctx context.Context, generation string) ([]WALSegmentInfo, error)

	// ReadSnapshot retrieves the snapshot for the given generation.
	ReadSnapshot(ctx context.Context, generation string) (io.ReadCloser, error)

	// ReadWALSegment retrieves one WAL segment.
	ReadWALSegment(ctx context.Context, generation string, seg WALSegmentInfo) (io.ReadCloser, error)

	// DeleteGeneration removes a generation and all its segments (retention).
	DeleteGeneration(ctx context.Context, generation string) error

	// Close releases any resources held by the backend.
	Close() error
}

// Generation represents one backup generation: a full snapshot plus the WAL
// segments recorded after it. Its ID embeds the creation time so generations
// sort chronologically by ID.
type Generation struct {
	ID        string    `json:"id"`
	CreatedAt time.Time `json:"created_at"`
}

// WALSegmentInfo identifies one uploaded WAL segment. Index is the WAL cycle
// within the generation (bumped every checkpoint/reset); Offset is the byte
// offset of the segment within that WAL file (0 = includes the WAL header).
type WALSegmentInfo struct {
	Index     int64     `json:"index"`
	Offset    int64     `json:"offset"`
	Size      int64     `json:"size"` // compressed size as stored
	CreatedAt time.Time `json:"created_at"`
}

// segmentKey renders the canonical object name for a segment. The timestamp
// is embedded so point-in-time restore never depends on backend metadata.
func (s WALSegmentInfo) segmentKey() string {
	return fmt.Sprintf("%016x-%016x-%016x.seg.gz", s.Index, s.Offset, s.CreatedAt.UnixNano())
}

// parseSegmentKey inverts segmentKey.
func parseSegmentKey(name string) (WALSegmentInfo, error) {
	base := strings.TrimSuffix(name, ".seg.gz")
	parts := strings.Split(base, "-")
	if len(parts) != 3 {
		return WALSegmentInfo{}, fmt.Errorf("colmena: bad segment name %q", name)
	}
	idx, err1 := strconv.ParseInt(parts[0], 16, 64)
	off, err2 := strconv.ParseInt(parts[1], 16, 64)
	ts, err3 := strconv.ParseInt(parts[2], 16, 64)
	if err1 != nil || err2 != nil || err3 != nil {
		return WALSegmentInfo{}, fmt.Errorf("colmena: bad segment name %q", name)
	}
	return WALSegmentInfo{Index: idx, Offset: off, CreatedAt: time.Unix(0, ts)}, nil
}

// NewGenerationID returns a time-sortable generation id: <unixnano>-<random>.
func NewGenerationID(now time.Time) string {
	b := make([]byte, 4)
	rand.Read(b)
	return fmt.Sprintf("%016x-%s", now.UnixNano(), hex.EncodeToString(b))
}

// ParseGenerationID extracts the creation time embedded in a generation id.
func ParseGenerationID(id string) (time.Time, error) {
	head, _, ok := strings.Cut(id, "-")
	if !ok {
		return time.Time{}, fmt.Errorf("colmena: bad generation id %q", id)
	}
	ns, err := strconv.ParseInt(head, 16, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("colmena: bad generation id %q", id)
	}
	return time.Unix(0, ns), nil
}

// BackupConfig configures the continuous backup engine.
type BackupConfig struct {
	// NewBackend returns the storage backend for the named database
	// ("default" for Node.DB()). Required. Called once per opened database.
	NewBackend func(db string) (BackupBackend, error)

	// SyncInterval is how often new committed WAL frames are shipped.
	// It bounds the restore point granularity. Default: 1s.
	SyncInterval time.Duration

	// SnapshotInterval is how often a fresh generation (full snapshot) is
	// started. Old generations beyond Retention are pruned. Default: 24h.
	SnapshotInterval time.Duration

	// Retention is how far back generations are kept. Point-in-time restore
	// works within this window. 0 keeps everything. Default: 30 days.
	Retention time.Duration

	// CheckpointThreshold is the WAL size (bytes) that, once fully shipped,
	// triggers a TRUNCATE checkpoint to keep the WAL bounded. Default: 4 MiB.
	CheckpointThreshold int64

	// OnError, when set, is invoked on every backup engine error with the
	// database name. Use it to surface backup failures (alerts, health).
	// Called from the engine goroutine; keep it fast.
	OnError func(db string, err error)

	// now is injectable for tests.
	now func() time.Time
}

func (c *BackupConfig) applyDefaults() {
	if c.SyncInterval == 0 {
		c.SyncInterval = time.Second
	}
	if c.SnapshotInterval == 0 {
		c.SnapshotInterval = 24 * time.Hour
	}
	if c.Retention == 0 {
		c.Retention = 30 * 24 * time.Hour
	}
	if c.CheckpointThreshold == 0 {
		c.CheckpointThreshold = 4 << 20
	}
	if c.now == nil {
		c.now = time.Now
	}
}

// BackupStatus is a point-in-time view of one database's backup engine,
// suitable for health endpoints.
type BackupStatus struct {
	DB             string    `json:"db"`
	Generation     string    `json:"generation"`
	LastSyncAt     time.Time `json:"last_sync_at"`
	LastSnapshotAt time.Time `json:"last_snapshot_at"`
	WALIndex       int64     `json:"wal_index"`
	WALOffset      int64     `json:"wal_offset"`
	LastError      string    `json:"last_error,omitempty"`
	LastErrorAt    time.Time `json:"last_error_at,omitzero"`
}

// backupManager runs the continuous backup loop for one store.
type backupManager struct {
	db      string
	store   *store
	backend BackupBackend
	cfg     BackupConfig
	logf    func(format string, args ...any)

	mu         sync.Mutex
	generation string
	walIndex   int64
	walOffset  int64 // committed bytes already shipped in the current index
	salt1      uint32
	salt2      uint32
	status     BackupStatus

	stopCh   chan struct{}
	doneCh   chan struct{}
	stopOnce sync.Once
}

func newBackupManager(db string, st *store, cfg BackupConfig, logf func(string, ...any)) (*backupManager, error) {
	cfg.applyDefaults()
	backend, err := cfg.NewBackend(db)
	if err != nil {
		return nil, fmt.Errorf("colmena: backup backend for %q: %w", db, err)
	}
	return &backupManager{
		db:      db,
		store:   st,
		backend: backend,
		cfg:     cfg,
		logf:    logf,
		status:  BackupStatus{DB: db},
		stopCh:  make(chan struct{}),
		doneCh:  make(chan struct{}),
	}, nil
}

// start disables SQLite auto-checkpointing (the engine owns checkpoints),
// takes the opening snapshot and launches the sync loop. A failed initial
// snapshot (e.g. transient S3 outage at boot) does not fail the store: it is
// reported and retried on the sync ticker until a generation exists.
func (b *backupManager) start() error {
	if _, err := b.store.writer.Exec("PRAGMA wal_autocheckpoint = 0"); err != nil {
		return fmt.Errorf("colmena: disable auto-checkpoint: %w", err)
	}
	if err := b.takeSnapshot(); err != nil {
		b.report(fmt.Errorf("initial snapshot (will retry): %w", err))
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
			// No generation yet (initial snapshot failed): retry it first,
			// nothing can ship without one.
			b.mu.Lock()
			needSnap := b.generation == ""
			b.mu.Unlock()
			if needSnap {
				b.report(b.takeSnapshot())
				continue
			}
			b.report(b.sync())
		case <-snapTicker.C:
			b.report(b.takeSnapshot())
		case <-b.stopCh:
			b.report(b.sync()) // final ship before stopping
			return
		}
	}
}

// stop gracefully stops the loop after a final sync. Safe to call repeatedly.
func (b *backupManager) stop() {
	b.stopOnce.Do(func() { close(b.stopCh) })
	<-b.doneCh
}

// report records and surfaces an engine error (nil is a no-op).
func (b *backupManager) report(err error) {
	if err == nil {
		return
	}
	b.mu.Lock()
	b.status.LastError = err.Error()
	b.status.LastErrorAt = b.cfg.now()
	b.mu.Unlock()
	b.logf("colmena: backup %s: %v", b.db, err)
	if b.cfg.OnError != nil {
		b.cfg.OnError(b.db, err)
	}
}

// Status returns a copy of the engine status.
func (b *backupManager) Status() BackupStatus {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := b.status
	s.Generation = b.generation
	s.WALIndex = b.walIndex
	s.WALOffset = b.walOffset
	return s
}

// sync ships any new committed WAL frames as one segment, then checkpoints
// when the WAL has grown past the threshold and is fully shipped.
func (b *backupManager) sync() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.generation == "" {
		return nil // no snapshot yet
	}

	walPath := b.store.dbPath + "-wal"
	hdr, committed, err := walCommittedSize(walPath)
	if os.IsNotExist(err) {
		return nil // no WAL (yet, or right after TRUNCATE)
	}
	if err != nil {
		return fmt.Errorf("scan wal: %w", err)
	}

	// A salt change means SQLite reset the WAL (post-checkpoint): new index.
	if b.walOffset > 0 && (hdr.salt1 != b.salt1 || hdr.salt2 != b.salt2) {
		b.walIndex++
		b.walOffset = 0
	}
	if b.walOffset == 0 {
		b.salt1, b.salt2 = hdr.salt1, hdr.salt2
	}
	if committed <= b.walOffset {
		return b.maybeCheckpoint(committed)
	}

	// Read the new committed byte range [walOffset, committed). A segment at
	// offset 0 includes the 32-byte WAL header, so each index replays whole.
	start := b.walOffset
	f, err := os.Open(walPath)
	if err != nil {
		return fmt.Errorf("open wal: %w", err)
	}
	defer f.Close()
	if _, err := f.Seek(start, io.SeekStart); err != nil {
		return fmt.Errorf("seek wal: %w", err)
	}
	data := make([]byte, committed-start)
	if _, err := io.ReadFull(f, data); err != nil {
		return fmt.Errorf("read wal: %w", err)
	}

	seg := WALSegmentInfo{Index: b.walIndex, Offset: start, CreatedAt: b.cfg.now()}
	gz, size, err := gzipBytes(data)
	if err != nil {
		return err
	}
	seg.Size = size
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := b.backend.WriteWALSegment(ctx, b.generation, seg, gz, size); err != nil {
		return fmt.Errorf("write segment: %w", err)
	}
	b.walOffset = committed
	b.status.LastSyncAt = b.cfg.now()
	return b.maybeCheckpoint(committed)
}

// maybeCheckpoint TRUNCATEs the WAL once it is big and fully shipped. The
// next write cycle re-creates it with fresh salts, which sync detects as a
// new index. b.mu must be held.
func (b *backupManager) maybeCheckpoint(committed int64) error {
	if committed < b.cfg.CheckpointThreshold || b.walOffset != committed {
		return nil
	}
	if _, err := b.store.writer.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		return fmt.Errorf("checkpoint: %w", err)
	}
	// TRUNCATE can be a no-op when readers pin the WAL; only advance the
	// index if the file actually shrank to zero. (If it didn't, the WAL keeps
	// growing under the current index and we retry next time.)
	if st, err := os.Stat(b.store.dbPath + "-wal"); err == nil && st.Size() > 0 {
		return nil
	}
	b.walIndex++
	b.walOffset = 0
	return nil
}

// takeSnapshot opens a new generation: checkpoint (bounded WAL), snapshot via
// the online backup API, upload, reset WAL tracking, prune old generations.
func (b *backupManager) takeSnapshot() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Best-effort compaction so the snapshot covers everything and the new
	// generation starts with an empty (or tiny) WAL.
	if _, err := b.store.writer.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		b.logf("colmena: backup %s: checkpoint before snapshot (non-fatal): %v", b.db, err)
	}

	// The snapshot is a literal copy of the main database file. This is safe
	// and page-faithful by construction: auto-checkpointing is disabled and
	// this engine (which holds b.mu) is the only checkpointer, so the main
	// file cannot change while we read it — concurrent writes land in the
	// WAL, and the new generation ships that WAL from byte 0, so replay
	// covers anything the checkpoint above left behind. A compacted copy
	// (VACUUM INTO / online backup into a fresh file) would renumber pages
	// and corrupt WAL replay — do not "optimize" this back.
	raw, err := os.ReadFile(b.store.dbPath)
	if err != nil {
		return fmt.Errorf("read snapshot: %w", err)
	}
	gz, size, err := gzipBytes(raw)
	if err != nil {
		return err
	}

	gen := NewGenerationID(b.cfg.now())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	if err := b.backend.WriteSnapshot(ctx, gen, gz, size); err != nil {
		return fmt.Errorf("write snapshot: %w", err)
	}

	b.generation = gen
	b.walIndex = 0
	b.walOffset = 0
	b.salt1, b.salt2 = 0, 0
	b.status.LastSnapshotAt = b.cfg.now()

	b.pruneLocked(ctx)
	return nil
}

// pruneLocked deletes generations older than Retention, always keeping the
// current one and at least one older-than-window anchor is unnecessary since
// the current generation always starts with a full snapshot.
func (b *backupManager) pruneLocked(ctx context.Context) {
	if b.cfg.Retention <= 0 {
		return
	}
	gens, err := b.backend.Generations(ctx)
	if err != nil {
		b.logf("colmena: backup %s: list generations for prune: %v", b.db, err)
		return
	}
	cutoff := b.cfg.now().Add(-b.cfg.Retention)
	for _, g := range gens {
		if g.ID == b.generation || !g.CreatedAt.Before(cutoff) {
			continue
		}
		if err := b.backend.DeleteGeneration(ctx, g.ID); err != nil {
			b.logf("colmena: backup %s: prune %s: %v", b.db, g.ID, err)
		}
	}
}

func gzipBytes(data []byte) (io.Reader, int64, error) {
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	if _, err := gw.Write(data); err != nil {
		return nil, 0, err
	}
	if err := gw.Close(); err != nil {
		return nil, 0, err
	}
	return bytes.NewReader(buf.Bytes()), int64(buf.Len()), nil
}

// ── Restore ─────────────────────────────────────────────────────────────────

// RestoreOptions selects what to restore.
type RestoreOptions struct {
	// DB is the database name ("default" when empty).
	DB string
	// Timestamp requests point-in-time restore: the state as of that moment
	// (generation snapshot + WAL segments recorded up to it). Zero = latest.
	Timestamp time.Time
}

// Restore rebuilds a database from the backend into dataDir/<db>.db.
// The target file must not be in use. Typical boot-time use:
//
//	if _, err := os.Stat(filepath.Join(dataDir, "default.db")); os.IsNotExist(err) {
//	    colmena.Restore(ctx, backend, dataDir)
//	}
//	node, err := colmena.New(cfg)
func Restore(ctx context.Context, backend BackupBackend, dataDir string, opts ...RestoreOptions) error {
	var opt RestoreOptions
	if len(opts) > 0 {
		opt = opts[0]
	}
	if opt.DB == "" {
		opt.DB = "default"
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return fmt.Errorf("colmena: create data dir: %w", err)
	}
	dbPath := filepath.Join(dataDir, opt.DB+".db")

	gens, err := backend.Generations(ctx)
	if err != nil {
		return fmt.Errorf("colmena: list generations: %w", err)
	}
	if len(gens) == 0 {
		return fmt.Errorf("colmena: no backups found")
	}
	sort.Slice(gens, func(i, j int) bool { return gens[i].CreatedAt.After(gens[j].CreatedAt) })
	var gen *Generation
	for i := range gens {
		if opt.Timestamp.IsZero() || !gens[i].CreatedAt.After(opt.Timestamp) {
			gen = &gens[i]
			break
		}
	}
	if gen == nil {
		return fmt.Errorf("colmena: no generation at or before %s", opt.Timestamp)
	}

	// Lay down the snapshot.
	for _, suffix := range []string{"", "-wal", "-shm"} {
		os.Remove(dbPath + suffix)
	}
	snap, err := backend.ReadSnapshot(ctx, gen.ID)
	if err != nil {
		return fmt.Errorf("colmena: read snapshot: %w", err)
	}
	if err := writeGunzipped(dbPath, snap); err != nil {
		snap.Close()
		return fmt.Errorf("colmena: write snapshot: %w", err)
	}
	snap.Close()

	// Roll the WAL segments forward, one index (= one WAL cycle) at a time.
	segs, err := backend.WALSegments(ctx, gen.ID)
	if err != nil {
		return fmt.Errorf("colmena: list segments: %w", err)
	}
	sort.Slice(segs, func(i, j int) bool {
		if segs[i].Index != segs[j].Index {
			return segs[i].Index < segs[j].Index
		}
		return segs[i].Offset < segs[j].Offset
	})

	byIndex := map[int64][]WALSegmentInfo{}
	var indexes []int64
	for _, s := range segs {
		if !opt.Timestamp.IsZero() && s.CreatedAt.After(opt.Timestamp) {
			continue
		}
		if _, ok := byIndex[s.Index]; !ok {
			indexes = append(indexes, s.Index)
		}
		byIndex[s.Index] = append(byIndex[s.Index], s)
	}
	sort.Slice(indexes, func(i, j int) bool { return indexes[i] < indexes[j] })

	for _, idx := range indexes {
		if err := applyWALIndex(ctx, backend, gen.ID, dbPath, byIndex[idx]); err != nil {
			return fmt.Errorf("colmena: apply wal index %d: %w", idx, err)
		}
	}

	// Final sanity: the restored database must pass an integrity check.
	if err := verifySQLite(dbPath); err != nil {
		return fmt.Errorf("colmena: restored db failed verification: %w", err)
	}
	return nil
}

// applyWALIndex reconstructs one WAL cycle from its contiguous segments and
// lets SQLite recover + checkpoint it into the database file.
func applyWALIndex(ctx context.Context, backend BackupBackend, gen, dbPath string, segs []WALSegmentInfo) error {
	if len(segs) == 0 {
		return nil
	}
	if segs[0].Offset != 0 {
		// The start of this WAL cycle is missing (e.g. PITR cut the first
		// segment): nothing from this index can be applied safely.
		return nil
	}
	walPath := dbPath + "-wal"
	f, err := os.Create(walPath)
	if err != nil {
		return err
	}
	written := int64(0)
	for _, s := range segs {
		if s.Offset != written {
			break // gap: apply the contiguous prefix only
		}
		rc, err := backend.ReadWALSegment(ctx, gen, s)
		if err != nil {
			f.Close()
			return err
		}
		gr, err := gzip.NewReader(rc)
		if err != nil {
			rc.Close()
			f.Close()
			return err
		}
		n, err := io.Copy(f, gr)
		gr.Close()
		rc.Close()
		if err != nil {
			f.Close()
			return err
		}
		written += n
	}
	if err := f.Close(); err != nil {
		return err
	}
	if written == 0 {
		os.Remove(walPath)
		return nil
	}

	// Open the database so SQLite recovers the WAL, then force a TRUNCATE
	// checkpoint to fold the frames into the main file before the next index.
	db, err := sql.Open("sqlite", "file:"+dbPath+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		return err
	}
	var user int
	if err := db.QueryRow("PRAGMA user_version").Scan(&user); err != nil { // forces WAL recovery
		db.Close()
		return err
	}
	if _, err := db.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		db.Close()
		return err
	}
	if err := db.Close(); err != nil {
		return err
	}
	os.Remove(walPath)
	os.Remove(dbPath + "-shm")
	return nil
}

func writeGunzipped(path string, r io.Reader) error {
	gr, err := gzip.NewReader(r)
	if err != nil {
		return err
	}
	defer gr.Close()
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, gr); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func verifySQLite(dbPath string) error {
	db, err := sql.Open("sqlite", "file:"+dbPath+"?mode=ro")
	if err != nil {
		return err
	}
	defer db.Close()
	var res string
	if err := db.QueryRow("PRAGMA integrity_check").Scan(&res); err != nil {
		return err
	}
	if res != "ok" {
		return fmt.Errorf("integrity_check: %s", res)
	}
	return nil
}
