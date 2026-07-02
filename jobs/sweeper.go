package jobs

import (
	"log"
	"time"
)

// sweeperLoop runs on the leader and reclaims jobs whose worker died. A job
// counts as orphaned when its claimed_at + timeout_ms is in the past while
// it's still flagged as running. We push it back to pending and increment
// attempts only at retry-time — sweeping is "no progress was made", which we
// already counted at claim time.
func (m *Manager) sweeperLoop() {
	defer m.wg.Done()

	tick := time.NewTicker(m.config.SweepInterval)
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
		m.sweepOnce()
	}
}

func (m *Manager) sweepOnce() {
	now := time.Now().UnixMilli()
	// timeout_ms == 0 means "no per-attempt timeout"; skip those (we have no
	// signal to know they've crashed). Most jobs use the manager default.
	res, err := m.node.DB().Exec(
		`UPDATE colmena_jobs
            SET status = 'pending',
                claimed_at = NULL,
                claimed_by = NULL,
                started_at = NULL,
                last_error = COALESCE(last_error, '') || ' (orphaned: reclaimed by sweeper)'
          WHERE status = 'running'
            AND timeout_ms > 0
            AND claimed_at IS NOT NULL
            AND claimed_at + timeout_ms < ?`,
		now,
	)
	if err != nil {
		log.Printf("colmena/jobs: sweep: %v", err)
		return
	}
	if rows, _ := res.RowsAffected(); rows > 0 {
		log.Printf("colmena/jobs: reclaimed %d orphaned job(s)", rows)
	}
}
