package jobs

import (
	"log"
	"time"
)

// schedulerLoop runs on the leader and fires recurring jobs. On each tick it
// reads colmena_jobs_schedule rows whose next_run_at is due, enqueues a
// concrete job, and advances next_run_at using the same cron expression.
//
// A single goroutine is fine because the work per tick is small and the
// leader-only check guarantees no double-firing across the cluster.
func (m *Manager) schedulerLoop() {
	defer m.wg.Done()

	tick := time.NewTicker(m.config.ScheduleInterval)
	defer tick.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-tick.C:
		}
		if !m.node.IsLeader() {
			continue
		}
		m.schedulerOnce()
	}
}

func (m *Manager) schedulerOnce() {
	now := time.Now()
	rows, err := m.node.DB().Query(
		`SELECT id, job_type, cron_expr, payload, next_run_at
            FROM colmena_jobs_schedule
           WHERE enabled = 1 AND next_run_at <= ?`,
		now.UnixMilli(),
	)
	if err != nil {
		log.Printf("colmena/jobs: scheduler query: %v", err)
		return
	}
	type due struct {
		id, jobType, cronExpr string
		payload               string
		nextRunAt             int64
	}
	var dues []due
	for rows.Next() {
		var d due
		if err := rows.Scan(&d.id, &d.jobType, &d.cronExpr, &d.payload, &d.nextRunAt); err != nil {
			log.Printf("colmena/jobs: scheduler scan: %v", err)
			continue
		}
		dues = append(dues, d)
	}
	rows.Close()

	for _, d := range dues {
		sched, err := parseCron(d.cronExpr)
		if err != nil {
			log.Printf("colmena/jobs: schedule %s: bad cron %q: %v", d.id, d.cronExpr, err)
			continue
		}

		// Enqueue a one-off run. We don't fan out missed ticks: if the
		// scheduler was paused, only one job is created when it wakes.
		jobID := newID()
		fireAt := time.UnixMilli(d.nextRunAt)
		_, err = m.node.DB().Exec(
			`INSERT INTO colmena_jobs
                (id, type, payload, status, priority, attempts, max_attempts,
                 enqueued_at, run_at, unique_key, timeout_ms)
             VALUES (?, ?, ?, 'pending', 0, 0, ?, ?, ?, NULL, ?)`,
			jobID, d.jobType, d.payload, m.config.DefaultMaxAttempts, // payload is already a string
			fireAt.UnixMilli(), fireAt.UnixMilli(),
			m.config.DefaultTimeout.Milliseconds(),
		)
		if err != nil {
			log.Printf("colmena/jobs: scheduler enqueue %s: %v", d.id, err)
			continue
		}

		// Compute the next fire time strictly *after* the one we just
		// fired so we don't immediately re-fire on the next tick.
		next := sched.Next(fireAt).UnixMilli()
		_, err = m.node.DB().Exec(
			`UPDATE colmena_jobs_schedule
                SET last_run_at = ?,
                    next_run_at = ?
              WHERE id = ?`,
			fireAt.UnixMilli(), next, d.id,
		)
		if err != nil {
			log.Printf("colmena/jobs: scheduler advance %s: %v", d.id, err)
		}
	}
}
