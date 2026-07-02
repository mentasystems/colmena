package jobs

import (
	"database/sql"
	"errors"
	"log"
	"math/rand"
	"time"
)

// workerLoop is the main poll-claim-run-finalise cycle running on each
// worker goroutine. Workers don't have a fixed share of the queue: every
// worker on every node races for every eligible job. The leader serialises
// claim writes through Raft so exactly one worker wins per job.
func (m *Manager) workerLoop(_ int) {
	defer m.wg.Done()

	// Spread initial polls so all workers don't hit the leader at the
	// same instant on startup.
	jitter := time.Duration(rand.Int63n(int64(m.config.PollInterval)))
	timer := time.NewTimer(jitter)
	defer timer.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-timer.C:
		case <-m.pokeCh:
		}

		ran := m.tryClaimAndRun()

		// If we ran something, immediately try again — there might be a
		// burst. Otherwise back off to PollInterval.
		next := m.config.PollInterval
		if ran {
			next = 0
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(next)
	}
}

// tryClaimAndRun finds one eligible pending job and attempts to claim it.
// Returns true when a job was actually executed (regardless of success).
func (m *Manager) tryClaimAndRun() bool {
	types := m.handlers.types()
	if len(types) == 0 {
		// No handlers registered — nothing to do.
		return false
	}

	// Find candidate IDs from the local replica. Read consistency is
	// "weak" by default which is fine: an out-of-date row just means the
	// claim UPDATE will return RowsAffected=0 and we'll move on.
	candidate, err := m.findCandidate(types)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			log.Printf("colmena/jobs: find candidate: %v", err)
		}
		return false
	}

	// Atomic claim: status flip happens through Raft (leader-serialised),
	// so the COUNT subqueries for concurrency and rate-limit are evaluated
	// at the same moment as the WHERE id=? AND status='pending' guard.
	// That gives us cluster-wide exactly-once semantics for both limits
	// without a separate token bucket to keep in sync.
	now := time.Now().UnixMilli()
	res, err := m.node.DB().Exec(
		`UPDATE colmena_jobs
            SET status     = 'running',
                claimed_at = ?,
                claimed_by = ?,
                started_at = ?,
                attempts   = attempts + 1
          WHERE id = ?
            AND status = 'pending'
            AND run_at <= ?
            AND (
                NOT EXISTS (SELECT 1 FROM colmena_jobs_concurrency c WHERE c.type = colmena_jobs.type)
                OR (SELECT cap FROM colmena_jobs_concurrency WHERE type = colmena_jobs.type)
                   > (SELECT COUNT(*) FROM colmena_jobs WHERE type = colmena_jobs.type AND status = 'running')
            )
            AND (
                NOT EXISTS (SELECT 1 FROM colmena_jobs_ratelimit r WHERE r.type = colmena_jobs.type)
                OR (SELECT capacity FROM colmena_jobs_ratelimit WHERE type = colmena_jobs.type)
                   > (SELECT COUNT(*) FROM colmena_jobs
                       WHERE type = colmena_jobs.type
                         AND started_at IS NOT NULL
                         AND started_at > ? - (SELECT period_ms FROM colmena_jobs_ratelimit WHERE type = colmena_jobs.type))
            )`,
		now, m.node.NodeID(), now, candidate.ID, now, now,
	)
	if err != nil {
		log.Printf("colmena/jobs: claim %s: %v", candidate.ID, err)
		return false
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		// Lost the race, hit a limit, or run_at moved into the future.
		return false
	}

	// Reflect the claim on our local copy so the handler context sees the
	// post-claim attempt count.
	candidate.Attempts++
	candidate.Status = StatusRunning

	m.executed.Add(1)
	handlerErr := m.runHandler(candidate)
	m.finalise(candidate, handlerErr)
	return true
}

// findCandidate returns the highest-priority pending job whose type matches
// one of the locally-registered handlers. We keep this read on the local
// replica because consistency hazards are caught by the claim UPDATE.
func (m *Manager) findCandidate(types []string) (*Job, error) {
	q := `SELECT id, type, payload, attempts, max_attempts,
                 enqueued_at, run_at, timeout_ms
            FROM colmena_jobs
           WHERE status = 'pending'
             AND run_at <= ?
             AND type IN (` + placeholders(len(types)) + `)
        ORDER BY priority DESC, run_at ASC
           LIMIT 1`
	args := make([]any, 0, len(types)+1)
	args = append(args, time.Now().UnixMilli())
	for _, t := range types {
		args = append(args, t)
	}
	row := m.node.DB().QueryRow(q, args...)

	j := &Job{}
	var enqueuedMs, runAtMs, timeoutMs int64
	if err := row.Scan(&j.ID, &j.Type, &j.Payload, &j.Attempts, &j.MaxAttempts,
		&enqueuedMs, &runAtMs, &timeoutMs); err != nil {
		return nil, err
	}
	j.EnqueuedAt = time.UnixMilli(enqueuedMs)
	j.RunAt = time.UnixMilli(runAtMs)
	j.Timeout = time.Duration(timeoutMs) * time.Millisecond
	j.Status = StatusPending
	return j, nil
}

// finalise records the outcome of a handler run: success, retry, or dead.
func (m *Manager) finalise(j *Job, handlerErr error) {
	now := time.Now().UnixMilli()

	if handlerErr == nil {
		_, err := m.node.DB().Exec(
			`UPDATE colmena_jobs
                SET status = 'succeeded',
                    finished_at = ?,
                    last_error = NULL
              WHERE id = ?`,
			now, j.ID,
		)
		if err != nil {
			log.Printf("colmena/jobs: mark succeeded %s: %v", j.ID, err)
		}
		m.succeeded.Add(1)
		return
	}

	if errors.Is(handlerErr, errNoHandler) {
		// Release the job back to pending so another node can take it.
		// We also rewind attempts so this aborted try doesn't count.
		_, err := m.node.DB().Exec(
			`UPDATE colmena_jobs
                SET status = 'pending',
                    claimed_at = NULL,
                    claimed_by = NULL,
                    started_at = NULL,
                    attempts = MAX(0, attempts - 1)
              WHERE id = ?`,
			j.ID,
		)
		if err != nil {
			log.Printf("colmena/jobs: release %s: %v", j.ID, err)
		}
		return
	}

	// True handler failure.
	if j.Attempts >= j.MaxAttempts {
		_, err := m.node.DB().Exec(
			`UPDATE colmena_jobs
                SET status = 'dead',
                    finished_at = ?,
                    last_error = ?
              WHERE id = ?`,
			now, handlerErr.Error(), j.ID,
		)
		if err != nil {
			log.Printf("colmena/jobs: mark dead %s: %v", j.ID, err)
		}
		m.failed.Add(1)
		m.dead.Add(1)
		return
	}

	// Retry: schedule next run with exponential backoff + jitter.
	entry, _ := m.handlers.lookup(j.Type)
	bo := m.config.DefaultBackoff
	if entry != nil && entry.backoff.Base > 0 {
		bo = entry.backoff
	}
	delay := nextBackoff(bo, j.Attempts)
	nextRun := time.Now().Add(delay).UnixMilli()

	_, err := m.node.DB().Exec(
		`UPDATE colmena_jobs
            SET status = 'pending',
                run_at = ?,
                claimed_at = NULL,
                claimed_by = NULL,
                started_at = NULL,
                last_error = ?
          WHERE id = ?`,
		nextRun, handlerErr.Error(), j.ID,
	)
	if err != nil {
		log.Printf("colmena/jobs: schedule retry %s: %v", j.ID, err)
	}
	m.failed.Add(1)
	m.retried.Add(1)
}

// placeholders returns "?, ?, ?" for the given count.
func placeholders(n int) string {
	if n <= 0 {
		return ""
	}
	out := make([]byte, 0, n*3)
	for i := 0; i < n; i++ {
		if i > 0 {
			out = append(out, ',', ' ')
		}
		out = append(out, '?')
	}
	return string(out)
}
