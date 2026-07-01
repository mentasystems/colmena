package jobs

import (
	"log"
	"time"
)

// vacuumFreeRatio is the free-page fraction above which reapOnce compacts the
// store with VACUUM. Deleting jobs leaves free pages behind: SQLite reuses them
// for later inserts but never shrinks the file on its own, and every Raft
// snapshot copies the full page count — so a store that once grew large stays
// large (slow and memory-hungry to snapshot) until it is vacuumed.
const vacuumFreeRatio = 0.25

// reaperLoop runs on the leader and deletes terminal jobs (succeeded/dead)
// older than config.RetainTerminal, then compacts the store when it has
// accumulated enough free space. finalise() only ever flips a completed job to
// 'succeeded'/'dead' — it never deletes — so without this loop colmena_jobs
// grows without bound and drags every snapshot down with it.
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
	m.maybeCompact()
}

// maybeCompact runs VACUUM when free pages exceed vacuumFreeRatio of the file.
// VACUUM changes no logical content, so replicating it through the write path
// is safe — every node reclaims its own copy. It briefly blocks that node's
// apply loop, so we only pay it when there is meaningful space to reclaim
// (typically once, right after a large backlog is first reaped).
func (m *Manager) maybeCompact() {
	var freelist, pageCount int64
	if err := m.node.DB().QueryRow(`PRAGMA freelist_count`).Scan(&freelist); err != nil {
		log.Printf("colmena/jobs: freelist_count: %v", err)
		return
	}
	if err := m.node.DB().QueryRow(`PRAGMA page_count`).Scan(&pageCount); err != nil {
		log.Printf("colmena/jobs: page_count: %v", err)
		return
	}
	if pageCount == 0 || float64(freelist)/float64(pageCount) < vacuumFreeRatio {
		return
	}
	log.Printf("colmena/jobs: compacting store (%d of %d pages free)", freelist, pageCount)
	if _, err := m.node.DB().Exec(`VACUUM`); err != nil {
		log.Printf("colmena/jobs: vacuum: %v", err)
	}
}
