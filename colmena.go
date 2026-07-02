// Package colmena is an embedded SQLite store for Go services, with
// continuous backup to object storage and point-in-time restore.
//
// v2 note: colmena started as "SQLite + litestream, embedded", grew a raft
// cluster layer (v0.x), and v2 removed it again — every real deployment was
// single-node. What remains is the useful core: a standard database/sql
// interface over a single-writer/multi-reader SQLite, and a litestream-style
// backup engine that ships committed WAL frames to S3-compatible storage.
//
// Quick start:
//
//	node, err := colmena.New(colmena.Config{
//	    DataDir: "./data",
//	    Backup: &colmena.BackupConfig{
//	        NewBackend: func(db string) (colmena.BackupBackend, error) {
//	            return s3.NewBackend(s3.Config{ /* bucket, creds… */ , Prefix: "myapp/" + db})
//	        },
//	        OnError: func(db string, err error) { alert(db, err) },
//	    },
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer node.Close()
//
//	db := node.DB() // *sql.DB
//	db.Exec("CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT)")
//
// Restore (disaster recovery or point-in-time):
//
//	colmena.Restore(ctx, backend, "./data")                                  // latest
//	colmena.Restore(ctx, backend, "./data", colmena.RestoreOptions{Timestamp: t}) // as of t
package colmena
