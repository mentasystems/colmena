package colmena

import (
	"archive/tar"
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	_ "modernc.org/sqlite"
)

// sqliteBackup is the interface exposed by modernc.org/sqlite's internal conn
// for the online backup API.
type sqliteBackup interface {
	NewBackup(dstUri string) (sqliteBackupHandle, error)
}

// sqliteBackupHandle represents an in-progress backup.
type sqliteBackupHandle interface {
	Step(n int32) (bool, error)
	Finish() error
}

const dbFileName = "colmena.db"

// store manages the local SQLite database with separate writer and reader pools.
type store struct {
	dbPath    string
	writer    *sql.DB
	reader    *sql.DB
	readConns int
	mu        sync.RWMutex // protects writer/reader during restore
}

func newStore(dataDir string, readConns int) (*store, error) {
	return newStoreAt(filepath.Join(dataDir, dbFileName), readConns)
}

func newStoreAt(dbPath string, readConns int) (*store, error) {

	// Writer: single connection, WAL mode, immediate transactions.
	writerDSN := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)&_txlock=immediate", dbPath)
	writer, err := sql.Open("sqlite", writerDSN)
	if err != nil {
		return nil, fmt.Errorf("colmena: open writer: %w", err)
	}
	writer.SetMaxOpenConns(1)

	// Verify WAL mode is active.
	var journalMode string
	if err := writer.QueryRow("PRAGMA journal_mode").Scan(&journalMode); err != nil {
		writer.Close()
		return nil, fmt.Errorf("colmena: check journal_mode: %w", err)
	}
	if journalMode != "wal" {
		writer.Close()
		return nil, fmt.Errorf("colmena: expected WAL mode, got %q", journalMode)
	}

	// Reader: multiple connections, read-only.
	readerDSN := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&mode=ro", dbPath)
	reader, err := sql.Open("sqlite", readerDSN)
	if err != nil {
		writer.Close()
		return nil, fmt.Errorf("colmena: open reader: %w", err)
	}
	reader.SetMaxOpenConns(readConns)

	return &store{
		dbPath:    dbPath,
		writer:    writer,
		reader:    reader,
		readConns: readConns,
	}, nil
}

// execute runs a write statement on the writer connection.
func (s *store) execute(stmt Statement) (ExecResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result, err := s.writer.Exec(stmt.SQL, stmt.Args...)
	if err != nil {
		return ExecResult{}, err
	}
	lastID, _ := result.LastInsertId()
	rows, _ := result.RowsAffected()
	return ExecResult{LastInsertID: lastID, RowsAffected: rows}, nil
}

// executeMulti runs multiple statements atomically in a single transaction.
func (s *store) executeMulti(stmts []Statement) ([]ExecResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	tx, err := s.writer.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	results := make([]ExecResult, len(stmts))
	for i, stmt := range stmts {
		result, err := tx.Exec(stmt.SQL, stmt.Args...)
		if err != nil {
			return nil, fmt.Errorf("statement %d: %w", i, err)
		}
		lastID, _ := result.LastInsertId()
		rows, _ := result.RowsAffected()
		results[i] = ExecResult{LastInsertID: lastID, RowsAffected: rows}
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return results, nil
}

// query runs a read query on the reader pool.
func (s *store) query(sqlStr string, args ...any) (*sql.Rows, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.reader.Query(sqlStr, args...)
}

// snapshot writes a full copy of the database to w using SQLite's Online Backup API.
// This copies only used pages incrementally, doesn't block concurrent readers,
// and avoids the temporary disk doubling of VACUUM INTO.
func (s *store) snapshot(w io.Writer) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tmpPath := s.dbPath + ".snapshot"
	defer os.Remove(tmpPath)

	if err := s.backupTo(tmpPath); err != nil {
		return err
	}

	f, err := os.Open(tmpPath)
	if err != nil {
		return fmt.Errorf("colmena: snapshot open: %w", err)
	}
	defer f.Close()

	if _, err := io.Copy(w, f); err != nil {
		return fmt.Errorf("colmena: snapshot copy: %w", err)
	}
	return nil
}

// backupTo uses the SQLite Online Backup API to create an incremental copy
// of the database at dstPath. Falls back to VACUUM INTO if the backup API
// is not accessible (e.g., the driver connection doesn't expose it).
func (s *store) backupTo(dstPath string) error {
	conn, err := s.reader.Conn(context.Background())
	if err != nil {
		return fmt.Errorf("colmena: get conn for backup: %w", err)
	}
	defer conn.Close()

	var backupErr error
	err = conn.Raw(func(driverConn any) error {
		bc, ok := driverConn.(sqliteBackup)
		if !ok {
			// Driver doesn't expose backup API — fall back to VACUUM INTO.
			backupErr = errBackupNotSupported
			return nil
		}

		dstURI := fmt.Sprintf("file:%s", dstPath)
		backup, err := bc.NewBackup(dstURI)
		if err != nil {
			return fmt.Errorf("colmena: init backup: %w", err)
		}

		// Copy all pages in one step.
		for {
			more, err := backup.Step(-1)
			if err != nil {
				backup.Finish()
				return fmt.Errorf("colmena: backup step: %w", err)
			}
			if !more {
				break
			}
		}

		return backup.Finish()
	})

	if err != nil {
		return err
	}

	if backupErr == errBackupNotSupported {
		// Fallback to VACUUM INTO.
		if _, err := s.reader.Exec(fmt.Sprintf("VACUUM INTO '%s'", dstPath)); err != nil {
			return fmt.Errorf("colmena: snapshot vacuum: %w", err)
		}
	}

	return nil
}

var errBackupNotSupported = fmt.Errorf("backup API not supported")

// restore replaces the database with data from r.
func (s *store) restore(r io.Reader) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Close existing connections.
	s.writer.Close()
	s.reader.Close()

	// Write the snapshot to the database file.
	f, err := os.Create(s.dbPath)
	if err != nil {
		return fmt.Errorf("colmena: restore create: %w", err)
	}
	if _, err := io.Copy(f, r); err != nil {
		f.Close()
		return fmt.Errorf("colmena: restore copy: %w", err)
	}
	f.Close()

	// Remove any leftover WAL/SHM files.
	os.Remove(s.dbPath + "-wal")
	os.Remove(s.dbPath + "-shm")

	// Re-open connections with the same readConns as original.
	ns, err := newStoreAt(s.dbPath, s.readConns)
	if err != nil {
		return fmt.Errorf("colmena: restore reopen: %w", err)
	}
	s.writer = ns.writer
	s.reader = ns.reader
	return nil
}

func (s *store) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var firstErr error
	if err := s.reader.Close(); err != nil {
		firstErr = err
	}
	if err := s.writer.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// storeManager manages multiple named SQLite stores sharing one Raft cluster.
type storeManager struct {
	mu        sync.RWMutex
	stores    map[string]*store
	dataDir   string
	readConns int
}

func newStoreManager(dataDir string, readConns int) *storeManager {
	return &storeManager{
		stores:    make(map[string]*store),
		dataDir:   dataDir,
		readConns: readConns,
	}
}

// get returns the named store, creating it if it does not already exist.
// The SQLite file for database "foo" lives at dataDir/foo.db.
func (sm *storeManager) get(name string) (*store, error) {
	sm.mu.RLock()
	if s, ok := sm.stores[name]; ok {
		sm.mu.RUnlock()
		return s, nil
	}
	sm.mu.RUnlock()

	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Double-check after acquiring write lock.
	if s, ok := sm.stores[name]; ok {
		return s, nil
	}

	dbPath := filepath.Join(sm.dataDir, name+".db")
	s, err := newStoreAt(dbPath, sm.readConns)
	if err != nil {
		return nil, fmt.Errorf("colmena: open store %q: %w", name, err)
	}
	sm.stores[name] = s
	return s, nil
}

// snapshot writes a versioned snapshot to w:
//
//	[10-byte envelope: magic|kind=Snapshot|version=1] [tar archive of .db files]
//
// Pre-v0.6 Colmena wrote the tar archive without an envelope (and v0.2.0
// wrote a single raw SQLite file). Both shapes are still accepted by
// restore() so rolling upgrades and old backups keep working.
func (sm *storeManager) snapshot(w io.Writer) error {
	return sm.snapshotVersioned(w, SnapshotFormatVersion)
}

// snapshotVersioned writes the snapshot at an explicit envelope version. The
// FSM passes the leader's negotiated effectiveSnapshotVersion() so a future
// snapshot-format bump never produces an envelope a lagging voter cannot
// restore — the same guardrail marshalCommandVersion gives the Raft log.
func (sm *storeManager) snapshotVersioned(w io.Writer, version int) error {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if err := writeEnvelopeHeader(w /* kind */, FormatKindSnapshot /* version */, uint8(version)); err != nil {
		return fmt.Errorf("colmena: snapshot header: %w", err)
	}

	tw := tar.NewWriter(w)

	for name, st := range sm.stores {
		var buf bytes.Buffer
		if err := st.snapshot(&buf); err != nil {
			tw.Close()
			return fmt.Errorf("colmena: snapshot store %q: %w", name, err)
		}
		data := buf.Bytes()
		hdr := &tar.Header{
			Name: name + ".db",
			Size: int64(len(data)),
			Mode: 0644,
		}
		if err := tw.WriteHeader(hdr); err != nil {
			tw.Close()
			return fmt.Errorf("colmena: tar header %q: %w", name, err)
		}
		if _, err := tw.Write(data); err != nil {
			tw.Close()
			return fmt.Errorf("colmena: tar write %q: %w", name, err)
		}
	}
	return tw.Close()
}

// restore closes all stores and rebuilds them from a snapshot stream. It
// accepts three historical shapes (detected by sniffing the first bytes):
//
//  1. v0.6+ enveloped tar: magic|kind=Snapshot|version=1 followed by tar.
//  2. v0.3..v0.5 unenveloped tar: the tar archive directly (stream starts
//     with a POSIX tar header, not a SQLite page).
//  3. v0.2.0 raw SQLite file: a single database written directly, restored
//     as the "default" store.
//
// Unknown envelope versions return ErrUnsupportedFormatVersion so the node
// refuses to load a snapshot it can't interpret.
func (sm *storeManager) restore(r io.Reader) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Close all existing stores.
	for _, st := range sm.stores {
		st.close()
	}
	sm.stores = make(map[string]*store)

	// Buffered reader lets us peek without consuming.
	br := bufio.NewReader(r)

	// Peek enough bytes to identify all three legacy shapes plus our envelope.
	// 16 bytes is enough: envelope magic is 8, raw SQLite magic is 16.
	const peekSize = 16
	peek, err := br.Peek(peekSize)
	if err != nil && err != io.EOF && !errors.Is(err, bufio.ErrBufferFull) {
		return fmt.Errorf("colmena: snapshot peek: %w", err)
	}

	switch {
	case hasEnvelopeMagic(peek):
		// Consume the 10-byte header, then extract tar.
		var hdr [envelopeHeaderSize]byte
		if _, err := io.ReadFull(br, hdr[:]); err != nil {
			return fmt.Errorf("colmena: snapshot header: %w", err)
		}
		kind := FormatKind(hdr[8])
		version := hdr[9]
		if kind != FormatKindSnapshot {
			return fmt.Errorf("colmena: snapshot: unexpected envelope kind %d", kind)
		}
		switch version {
		case 1:
			return sm.restoreTar(br)
		default:
			return fmt.Errorf("colmena: snapshot version %d: %w", version, ErrUnsupportedFormatVersion)
		}

	case looksLikeRawSQLite(peek):
		// v0.2.0 legacy: single raw SQLite file, becomes the "default" store.
		return sm.restoreRawSQLite(br)

	default:
		// Assume legacy unenveloped tar (v0.3..v0.5).
		return sm.restoreTar(br)
	}
}

// restoreTar extracts a tar archive of <name>.db entries into the data dir
// and reopens every discovered store.
func (sm *storeManager) restoreTar(r io.Reader) error {
	tr := tar.NewReader(r)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("colmena: tar read: %w", err)
		}

		name := strings.TrimSuffix(hdr.Name, ".db")
		dbPath := filepath.Join(sm.dataDir, hdr.Name)

		os.Remove(dbPath + "-wal")
		os.Remove(dbPath + "-shm")

		f, err := os.Create(dbPath)
		if err != nil {
			return fmt.Errorf("colmena: restore create %q: %w", name, err)
		}
		if _, err := io.Copy(f, tr); err != nil {
			f.Close()
			return fmt.Errorf("colmena: restore write %q: %w", name, err)
		}
		f.Close()
	}

	return sm.reopenAllFromDir()
}

// restoreRawSQLite handles v0.2.0 snapshots, which are a single SQLite
// database file with no wrapper. The file is written as "default.db".
func (sm *storeManager) restoreRawSQLite(r io.Reader) error {
	dbPath := filepath.Join(sm.dataDir, "default.db")
	os.Remove(dbPath + "-wal")
	os.Remove(dbPath + "-shm")

	f, err := os.Create(dbPath)
	if err != nil {
		return fmt.Errorf("colmena: restore legacy create: %w", err)
	}
	if _, err := io.Copy(f, r); err != nil {
		f.Close()
		return fmt.Errorf("colmena: restore legacy copy: %w", err)
	}
	f.Close()

	return sm.reopenAllFromDir()
}

// reopenAllFromDir scans the data directory and opens every *.db file
// (except raft.db) as a store. Called at the end of each restore path.
func (sm *storeManager) reopenAllFromDir() error {
	entries, err := os.ReadDir(sm.dataDir)
	if err != nil {
		return fmt.Errorf("colmena: readdir after restore: %w", err)
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".db") {
			continue
		}
		if e.Name() == "raft.db" {
			continue
		}
		name := strings.TrimSuffix(e.Name(), ".db")
		dbPath := filepath.Join(sm.dataDir, e.Name())
		s, err := newStoreAt(dbPath, sm.readConns)
		if err != nil {
			return fmt.Errorf("colmena: restore reopen %q: %w", name, err)
		}
		sm.stores[name] = s
	}
	return nil
}

// sqliteMagic is the fixed first 16 bytes of every SQLite 3 database file.
var sqliteMagic = []byte("SQLite format 3\x00")

func looksLikeRawSQLite(b []byte) bool {
	return len(b) >= len(sqliteMagic) && bytes.Equal(b[:len(sqliteMagic)], sqliteMagic)
}

// close closes all managed stores.
func (sm *storeManager) close() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	var firstErr error
	for _, st := range sm.stores {
		if err := st.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// defaultStore returns the store named "default", creating it if needed.
// This preserves backward compatibility.
func (sm *storeManager) defaultStore() (*store, error) {
	return sm.get("default")
}
