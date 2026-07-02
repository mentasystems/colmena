package jobs

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// EnqueueOption customises a single Enqueue call.
type EnqueueOption func(*enqueueOpts)

type enqueueOpts struct {
	priority    Priority
	runAt       time.Time
	maxAttempts int
	uniqueKey   string
	timeout     time.Duration
}

// WithPriority sets the priority. Higher priorities run first.
func WithPriority(p Priority) EnqueueOption {
	return func(o *enqueueOpts) { o.priority = p }
}

// WithRunAt schedules the job to run no earlier than the given time.
func WithRunAt(t time.Time) EnqueueOption {
	return func(o *enqueueOpts) { o.runAt = t }
}

// WithMaxAttempts overrides the configured default attempt cap.
func WithMaxAttempts(n int) EnqueueOption {
	return func(o *enqueueOpts) { o.maxAttempts = n }
}

// WithUniqueKey deduplicates jobs: while another job with the same key is
// pending or running, Enqueue returns the existing ID instead of creating a
// duplicate. Once the prior job finishes (succeeded/failed/dead), a new
// enqueue with the same key starts fresh.
func WithUniqueKey(k string) EnqueueOption {
	return func(o *enqueueOpts) { o.uniqueKey = k }
}

// WithTimeout overrides the configured default per-attempt timeout.
func WithTimeout(d time.Duration) EnqueueOption {
	return func(o *enqueueOpts) { o.timeout = d }
}

// ErrDuplicateUnique is returned by Enqueue (as the error result) when the
// underlying unique-key index rejects the insert because another job with the
// same key is already pending or running. Callers usually treat this as
// "fine, the job is already on its way" and ignore it.
var ErrDuplicateUnique = errors.New("colmena/jobs: duplicate unique key")

// Enqueue inserts a new job. The args value is JSON-encoded and stored in the
// payload column. The returned ID is the canonical reference; if a job with
// the same unique key is already pending/running, that prior ID is returned.
func Enqueue[Args any](m *Manager, jobType string, args Args, opts ...EnqueueOption) (string, error) {
	o := enqueueOpts{
		priority:    PriorityNormal,
		runAt:       time.Now(),
		maxAttempts: m.config.DefaultMaxAttempts,
		timeout:     m.config.DefaultTimeout,
	}
	for _, fn := range opts {
		fn(&o)
	}

	payload, err := json.Marshal(args)
	if err != nil {
		return "", fmt.Errorf("colmena/jobs: marshal args: %w", err)
	}

	// If a unique key is set, check for an existing pending/running job
	// first. We do this on the local replica; under contention the unique
	// index will catch racing inserts and we surface the existing row.
	if o.uniqueKey != "" {
		var existingID string
		err := m.node.DB().QueryRow(
			`SELECT id FROM colmena_jobs
              WHERE unique_key = ? AND status IN ('pending','running')
              LIMIT 1`,
			o.uniqueKey,
		).Scan(&existingID)
		if err == nil {
			return existingID, nil
		}
	}

	id := newID()
	now := time.Now().UnixMilli()
	runAtMs := o.runAt.UnixMilli()
	if runAtMs < now {
		runAtMs = now
	}

	var uniqueArg any
	if o.uniqueKey != "" {
		uniqueArg = o.uniqueKey
	} else {
		uniqueArg = nil
	}

	// Store payload as TEXT (string) — passing []byte goes through JSON
	// marshal in the Raft envelope and arrives base64-encoded, which is
	// not what we want. JSON is text anyway, so a TEXT-affinity column
	// holds it correctly.
	_, err = m.node.DB().Exec(
		`INSERT INTO colmena_jobs
            (id, type, payload, status, priority, attempts, max_attempts,
             enqueued_at, run_at, unique_key, timeout_ms)
         VALUES (?, ?, ?, 'pending', ?, 0, ?, ?, ?, ?, ?)`,
		id, jobType, string(payload), int(o.priority), o.maxAttempts,
		now, runAtMs, uniqueArg, o.timeout.Milliseconds(),
	)
	if err != nil {
		// SQLite unique-index violation: another node won the race.
		// Look up and return the live id rather than failing the caller.
		if o.uniqueKey != "" && isUniqueViolation(err) {
			var existingID string
			if e2 := m.node.DB().QueryRow(
				`SELECT id FROM colmena_jobs
                  WHERE unique_key = ? AND status IN ('pending','running')
                  LIMIT 1`,
				o.uniqueKey,
			).Scan(&existingID); e2 == nil {
				return existingID, nil
			}
			return "", ErrDuplicateUnique
		}
		return "", fmt.Errorf("colmena/jobs: insert: %w", err)
	}

	// Best-effort wakeup so a local idle worker picks the job up immediately
	// rather than waiting for the next poll tick.
	m.poke()
	return id, nil
}

// Schedule registers (or updates) a recurring job. The cron expression
// follows the standard 5-field format: "minute hour day-of-month month
// day-of-week". The schedule survives restarts because it lives in the
// replicated colmena_jobs_schedule table.
//
// If a schedule with the same id already exists, its expression and payload
// are updated and next_run_at is recomputed. Use the same id across restarts
// to avoid duplicating schedules.
func Schedule[Args any](m *Manager, id, jobType, cronExpr string, args Args) error {
	sched, err := parseCron(cronExpr)
	if err != nil {
		return fmt.Errorf("colmena/jobs: parse cron %q: %w", cronExpr, err)
	}

	payload, err := json.Marshal(args)
	if err != nil {
		return fmt.Errorf("colmena/jobs: marshal schedule args: %w", err)
	}

	now := time.Now()
	next := sched.Next(now).UnixMilli()

	// Use INSERT ... ON CONFLICT to update existing schedules in place
	// without an extra round-trip.
	_, err = m.node.DB().Exec(
		`INSERT INTO colmena_jobs_schedule
            (id, job_type, cron_expr, payload, next_run_at, last_run_at, enabled)
         VALUES (?, ?, ?, ?, ?, NULL, 1)
         ON CONFLICT(id) DO UPDATE SET
            job_type    = excluded.job_type,
            cron_expr   = excluded.cron_expr,
            payload     = excluded.payload,
            next_run_at = excluded.next_run_at,
            enabled     = 1`,
		id, jobType, cronExpr, string(payload), next,
	)
	if err != nil {
		return fmt.Errorf("colmena/jobs: upsert schedule: %w", err)
	}
	return nil
}

// Unschedule disables a recurring job. The row is kept for audit; future
// ticks will skip it because enabled=0.
func Unschedule(m *Manager, id string) error {
	_, err := m.node.DB().Exec(
		`UPDATE colmena_jobs_schedule SET enabled = 0 WHERE id = ?`,
		id,
	)
	if err != nil {
		return fmt.Errorf("colmena/jobs: unschedule: %w", err)
	}
	return nil
}

// SetConcurrency caps cluster-wide simultaneous executions of a job type.
// A cap of 0 removes the cap entirely.
func SetConcurrency(m *Manager, jobType string, cap int) error {
	if cap <= 0 {
		_, err := m.node.DB().Exec(
			`DELETE FROM colmena_jobs_concurrency WHERE type = ?`, jobType,
		)
		return err
	}
	_, err := m.node.DB().Exec(
		`INSERT INTO colmena_jobs_concurrency (type, cap) VALUES (?, ?)
         ON CONFLICT(type) DO UPDATE SET cap = excluded.cap`,
		jobType, cap,
	)
	return err
}

// SetRateLimit installs a sliding-window rate limit for the given job type.
// At most r.N executions may start within any rolling r.Per window across
// the cluster. Setting r.N to 0 removes the limit.
func SetRateLimit(m *Manager, jobType string, r Rate) error {
	if r.N <= 0 || r.Per <= 0 {
		_, err := m.node.DB().Exec(
			`DELETE FROM colmena_jobs_ratelimit WHERE type = ?`, jobType,
		)
		return err
	}
	_, err := m.node.DB().Exec(
		`INSERT INTO colmena_jobs_ratelimit (type, capacity, period_ms)
         VALUES (?, ?, ?)
         ON CONFLICT(type) DO UPDATE SET
            capacity  = excluded.capacity,
            period_ms = excluded.period_ms`,
		jobType, r.N, r.Per.Milliseconds(),
	)
	return err
}

// isUniqueViolation reports whether err is a SQLite unique-constraint failure.
// We match on the message because the modernc.org/sqlite driver does not
// expose the result code reliably across versions.
func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return contains(s, "UNIQUE constraint failed") || contains(s, "constraint failed: UNIQUE")
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
