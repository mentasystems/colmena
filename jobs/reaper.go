package jobs

import (
	"log"
	"time"
)

// reaperLoop runs on the leader and deletes terminal jobs (succeeded/dead)
// older than config.RetainTerminal. finalise() only ever flips a completed job
// to 'succeeded'/'dead' — it never deletes — so without this loop colmena_jobs
// grows without bound and drags every Raft snapshot down with it.
//
// We only need to bound the row COUNT: a Raft snapshot copies the store with
// SQLite's VACUUM INTO / Online Backup path, which already emits a compacted
// image, so deleting the rows is enough to shrink every future snapshot. The
// physical file self-heals too — freed pages are reused by later inserts, and a
// node that restores from a snapshot installs the compacted copy. We do NOT run
// VACUUM here: colmena's writer opens with _txlock=immediate, so a replicated
// `VACUUM` fails with "cannot VACUUM from within a transaction" under load, and
// it would be redundant anyway.
func (m *Manager) reaperLoop() {
	defer m.wg.Done()

	tick := time.NewTicker(m.config.ReapInterval)
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
		m.reapOnce()
	}
}

// reapOnce deletes all terminal jobs whose finished_at is older than the
// retention cutoff. The DELETE is replicated through Raft and applied on every
// node, so it MUST be deterministic: a bare `LIMIT` without a total order would
// let different nodes delete different rows and diverge. We therefore delete
// the whole eligible set in one statement (the set is identical on every node)
// rather than batching. The cutoff is computed on the leader and travels as a
// bound argument, so every node filters against the same instant.
func (m *Manager) reapOnce() {
	cutoff := time.Now().Add(-m.config.RetainTerminal).UnixMilli()

	res, err := m.node.DB().Exec(
		`DELETE FROM colmena_jobs
          WHERE status IN ('succeeded', 'dead')
            AND finished_at IS NOT NULL
            AND finished_at < ?`,
		cutoff,
	)
	if err != nil {
		log.Printf("colmena/jobs: reap: %v", err)
		return
	}
	rows, _ := res.RowsAffected()
	if rows <= 0 {
		return
	}
	m.reaped.Add(uint64(rows))
	log.Printf("colmena/jobs: reaped %d terminal job(s)", rows)
}
