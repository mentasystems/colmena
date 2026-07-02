package colmena

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
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

// store manages one local SQLite database with separate writer and reader pools.
type store struct {
	dbPath    string
	writer    *sql.DB
	reader    *sql.DB
	readConns int
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

// backupTo uses the SQLite Online Backup API to create a consistent copy of
// the database at dstPath (used pages only, doesn't block readers). Falls
// back to VACUUM INTO if the driver connection doesn't expose the API.
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
		if _, err := s.reader.Exec(fmt.Sprintf("VACUUM INTO '%s'", dstPath)); err != nil {
			return fmt.Errorf("colmena: snapshot vacuum: %w", err)
		}
	}

	return nil
}

var errBackupNotSupported = fmt.Errorf("backup API not supported")

func (s *store) close() error {
	var firstErr error
	if err := s.reader.Close(); err != nil {
		firstErr = err
	}
	if err := s.writer.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// storeManager manages the named SQLite stores of one data directory.
type storeManager struct {
	mu        sync.RWMutex
	stores    map[string]*store
	dataDir   string
	readConns int
	onOpen    func(name string, st *store) error // hook: backup engine attach
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
	if s, ok := sm.stores[name]; ok {
		return s, nil
	}

	dbPath := filepath.Join(sm.dataDir, name+".db")
	s, err := newStoreAt(dbPath, sm.readConns)
	if err != nil {
		return nil, fmt.Errorf("colmena: open store %q: %w", name, err)
	}
	if sm.onOpen != nil {
		if hookErr := sm.onOpen(name, s); hookErr != nil {
			s.close()
			return nil, hookErr
		}
	}
	sm.stores[name] = s
	return s, nil
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
