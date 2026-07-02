package jobs

import (
	"database/sql"
	"fmt"
)

// schemaStatements are applied once per database when the manager starts.
// They are idempotent (CREATE IF NOT EXISTS) so re-running is harmless,
// and they are written directly via node.DB().Exec so they replicate
// through Raft on the first node that boots up.
var schemaStatements = []string{
	`CREATE TABLE IF NOT EXISTS colmena_jobs (
        id           TEXT PRIMARY KEY,
        type         TEXT NOT NULL,
        payload      BLOB NOT NULL,
        status       TEXT NOT NULL,
        priority     INTEGER NOT NULL DEFAULT 0,
        attempts     INTEGER NOT NULL DEFAULT 0,
        max_attempts INTEGER NOT NULL DEFAULT 5,
        enqueued_at  INTEGER NOT NULL,
        run_at       INTEGER NOT NULL,
        claimed_at   INTEGER,
        claimed_by   TEXT,
        started_at   INTEGER,
        finished_at  INTEGER,
        last_error   TEXT,
        unique_key   TEXT,
        timeout_ms   INTEGER NOT NULL
    )`,
	`CREATE INDEX IF NOT EXISTS idx_colmena_jobs_pending
        ON colmena_jobs(status, run_at, priority)
        WHERE status = 'pending'`,
	`CREATE INDEX IF NOT EXISTS idx_colmena_jobs_running_type
        ON colmena_jobs(type)
        WHERE status = 'running'`,
	`CREATE INDEX IF NOT EXISTS idx_colmena_jobs_claimed
        ON colmena_jobs(claimed_at)
        WHERE status IN ('running')`,
	`CREATE UNIQUE INDEX IF NOT EXISTS idx_colmena_jobs_unique
        ON colmena_jobs(unique_key)
        WHERE unique_key IS NOT NULL AND status IN ('pending','running')`,

	`CREATE TABLE IF NOT EXISTS colmena_jobs_schedule (
        id            TEXT PRIMARY KEY,
        job_type      TEXT NOT NULL,
        cron_expr     TEXT NOT NULL,
        payload       BLOB NOT NULL,
        next_run_at   INTEGER NOT NULL,
        last_run_at   INTEGER,
        enabled       INTEGER NOT NULL DEFAULT 1
    )`,
	`CREATE INDEX IF NOT EXISTS idx_colmena_schedule_next
        ON colmena_jobs_schedule(next_run_at)
        WHERE enabled = 1`,

	// Sliding-window rate limit per job type: at most "capacity" jobs may
	// have started within the last "period_ms" milliseconds, cluster-wide.
	// We measure against colmena_jobs.started_at directly so the check is
	// part of the atomic claim UPDATE — no separate token bucket to keep
	// in sync.
	`CREATE TABLE IF NOT EXISTS colmena_jobs_ratelimit (
        type       TEXT PRIMARY KEY,
        capacity   INTEGER NOT NULL,
        period_ms  INTEGER NOT NULL
    )`,
	// Index on started_at filtered by type makes the rate-limit subquery
	// in the claim UPDATE O(log N) per type.
	`CREATE INDEX IF NOT EXISTS idx_colmena_jobs_started
        ON colmena_jobs(type, started_at)
        WHERE started_at IS NOT NULL`,

	// Per-type concurrency cap. Read by the claim transaction. NULL/missing
	// row means unlimited.
	`CREATE TABLE IF NOT EXISTS colmena_jobs_concurrency (
        type   TEXT PRIMARY KEY,
        cap    INTEGER NOT NULL
    )`,
}

// migrate applies the jobs schema. Safe to call from any node — writes are
// forwarded to the leader through Colmena's standard write path.
func migrate(db *sql.DB) error {
	for i, stmt := range schemaStatements {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("colmena/jobs: schema statement %d: %w", i, err)
		}
	}
	return nil
}
