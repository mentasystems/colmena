<p align="center">
  <img src="banner.png" alt="Colmena" width="700">
</p>

<h1 align="center">Colmena</h1>

<p align="center">
  Embedded SQLite for Go services, with continuous backup and point-in-time restore. No CGo, no external processes — just <code>import</code> and go.
</p>

---

Colmena gives a Go service a production-ready SQLite setup in one call:

- **Standard `database/sql`** — a single-writer / multi-reader pool split
  behind a normal `*sql.DB`. WAL mode, sane pragmas, real transactions.
- **Continuous backup, litestream-style** — committed WAL frames are
  verified (salt + checksum chain) and shipped every second as immutable
  segments to S3-compatible storage; each generation opens with a full
  snapshot. **Point-in-time restore** within the retention window.
- **Restore-on-boot** — rebuild the database from the backend on a fresh
  machine with one call before `New`.
- **Backup observability** — `BackupStatus()` for health endpoints and an
  `OnError` hook for alerting; backups that stop working never fail silent.
- **Pure Go** — `modernc.org/sqlite`; the S3 backend has zero dependencies.

> **v2 note.** Colmena v0.x was a raft-replicated distributed SQLite. v2
> removed the cluster layer — every real deployment was single-node — and
> kept the useful core. Single-node v0.x callers compile unchanged (cluster
> config fields are deprecated no-ops). The last raft release is tagged
> `v0.13-raft-final`. See [UPGRADING.md](UPGRADING.md).

## Quick start

```go
import (
    "github.com/mentasystems/colmena"
    "github.com/mentasystems/colmena/backup/s3"
)

node, err := colmena.New(colmena.Config{
    DataDir: "./data",
    Backup: &colmena.BackupConfig{
        NewBackend: func(db string) (colmena.BackupBackend, error) {
            return s3.NewBackend(s3.Config{
                Endpoint:  "https://s3.gra.io.cloud.ovh.net",
                Region:    "gra",
                Bucket:    "myapp-backups",
                Prefix:    "myapp/" + db,
                AccessKey: os.Getenv("AWS_ACCESS_KEY_ID"),
                SecretKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
            })
        },
        OnError: func(db string, err error) { alerting.Notify(db, err) },
    },
})
if err != nil {
    log.Fatal(err)
}
defer node.Close()

db := node.DB() // *sql.DB
db.Exec(`CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT)`)
```

Multiple databases per data dir: `node.OpenDB("analytics", 0)` →
`./data/analytics.db`, each with its own backup stream.

## Restore

```go
// Latest state:
colmena.Restore(ctx, backend, "./data")

// Point-in-time (e.g. right before an accidental delete):
colmena.Restore(ctx, backend, "./data", colmena.RestoreOptions{
    Timestamp: time.Date(2026, 7, 2, 14, 30, 0, 0, time.UTC),
})
```

Restore-on-boot pattern for disaster recovery:

```go
if _, err := os.Stat(filepath.Join(dataDir, "default.db")); os.IsNotExist(err) {
    if err := colmena.Restore(ctx, backend, dataDir); err != nil {
        log.Fatalf("restore: %v", err)
    }
}
node, err := colmena.New(cfg)
```

## How the backup works

- The engine owns checkpointing (`wal_autocheckpoint=0`; colmena controls
  every connection, so nothing else resets the WAL under it).
- Every `SyncInterval` it scans the WAL, validates frames against the header
  salts and the cumulative checksum chain, and ships the bytes up to the last
  commit frame as one gzip segment. Torn or uncommitted tails never ship.
- Once the WAL passes `CheckpointThreshold` and is fully shipped, a TRUNCATE
  checkpoint folds it into the main file; the next WAL cycle becomes a new
  segment index.
- Every `SnapshotInterval` a new generation starts with a full snapshot
  (SQLite online backup API — consistent, non-blocking), and generations
  older than `Retention` are pruned.
- Restore lays down the snapshot, replays each WAL index in order (SQLite's
  own WAL recovery does the applying), and finishes with `integrity_check`.

Defaults: sync 1s · snapshot 24h · retention 30d · checkpoint 4 MiB.

## License

MIT
