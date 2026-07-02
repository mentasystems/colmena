package jobs

// Stats is a point-in-time snapshot of jobs activity.
type Stats struct {
	// Process-local counters since this manager started.
	Executed  uint64 `json:"executed"`
	Succeeded uint64 `json:"succeeded"`
	Failed    uint64 `json:"failed"`
	Retried   uint64 `json:"retried"`
	Dead      uint64 `json:"dead"`
	Reaped    uint64 `json:"reaped"`

	// Cluster-wide queue depth by status. These are read from the
	// replicated colmena_jobs table so all nodes see the same numbers
	// (modulo replication lag).
	ByStatus map[Status]int64 `json:"by_status"`

	// Pending jobs broken down by type. Useful for debugging starvation.
	PendingByType map[string]int64 `json:"pending_by_type"`
}

// Stats returns counters for this node and replicated queue depth.
// It issues a few read-only queries against the local replica; consistency
// is the manager's underlying node consistency setting.
func (m *Manager) Stats() (*Stats, error) {
	s := &Stats{
		Executed:      m.executed.Load(),
		Succeeded:     m.succeeded.Load(),
		Failed:        m.failed.Load(),
		Retried:       m.retried.Load(),
		Dead:          m.dead.Load(),
		Reaped:        m.reaped.Load(),
		ByStatus:      make(map[Status]int64),
		PendingByType: make(map[string]int64),
	}

	rows, err := m.node.DB().Query(
		`SELECT status, COUNT(*) FROM colmena_jobs GROUP BY status`,
	)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var status string
		var n int64
		if err := rows.Scan(&status, &n); err != nil {
			rows.Close()
			return nil, err
		}
		s.ByStatus[Status(status)] = n
	}
	rows.Close()

	rows, err = m.node.DB().Query(
		`SELECT type, COUNT(*) FROM colmena_jobs
          WHERE status = 'pending' GROUP BY type`,
	)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var t string
		var n int64
		if err := rows.Scan(&t, &n); err != nil {
			rows.Close()
			return nil, err
		}
		s.PendingByType[t] = n
	}
	rows.Close()
	return s, nil
}
