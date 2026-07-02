# Upgrading to Colmena v2

v2 removes the raft cluster layer entirely. Colmena is now what it started
as: embedded SQLite + continuous backup (litestream-style, with point-in-time
restore). The last raft release is tagged `v0.13-raft-final`.

## Source compatibility

Existing single-node callers compile unchanged:

- `colmena.New(colmena.Config{NodeID, DataDir, Bind, Bootstrap, Join, …})` —
  all cluster fields are now deprecated no-ops; only `DataDir` matters.
- `node.DB()` / `node.OpenDB(name, consistency)` still return a `*sql.DB`
  (consistency is ignored; reads are always local).
- `node.WaitForLeader(…)` / `node.IsLeader()` are deprecated no-ops.
- Data layout is unchanged (`<DataDir>/default.db`), so existing data dirs
  open as-is. Leftover `raft.db` / `snapshots/` are ignored — delete them.

## Behavior changes

- **Transactions are real SQLite transactions.** Queries inside a `*sql.Tx`
  now see the transaction's own writes, and `LastInsertId`/`RowsAffected`
  work immediately (v1 buffered writes until Commit and returned
  `ErrTxResultPending`).
- Removed: jobs subpackage, cluster/RPC/TLS/LAN discovery, consistency
  levels, write batching, `ExecMulti`, `Migrate`, metrics handlers.

## New: continuous backup with PITR

```go
node, err := colmena.New(colmena.Config{
    DataDir: dataDir,
    Backup: &colmena.BackupConfig{
        NewBackend: func(db string) (colmena.BackupBackend, error) {
            return s3.NewBackend(s3.Config{
                Endpoint: "https://s3.gra.io.cloud.ovh.net", Region: "gra",
                Bucket: "myapp-backups", Prefix: "myapp/" + db,
                AccessKey: os.Getenv("AWS_ACCESS_KEY_ID"),
                SecretKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
            })
        },
        OnError: func(db string, err error) { /* alert */ },
    },
})
```

- Committed WAL frames ship every `SyncInterval` (default 1s) as immutable
  segments; each generation opens with a full snapshot (default every 24h,
  retention 30 days).
- Restore: `colmena.Restore(ctx, backend, dataDir)` for latest, or pass
  `RestoreOptions{Timestamp: t}` for point-in-time.
- Health: `node.BackupStatus()` per database; `OnError` fires on every
  engine failure — wire it to your alerting.
