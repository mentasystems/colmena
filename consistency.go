package colmena

import (
	"context"
	"sync"
	"time"
)

// ConsistencyLevel defines the read consistency guarantee.
type ConsistencyLevel int

const (
	// consistencyUnset is the zero value of ConsistencyLevel. It is not
	// a valid consistency level; applyDefaults() replaces it with
	// ConsistencyWeak so existing callers that leave Config.Consistency
	// at zero still get the documented default. Real levels start at 1
	// so a caller can pass ConsistencyNone (= 1) and have it survive
	// applyDefaults instead of being silently upgraded to Weak.
	consistencyUnset ConsistencyLevel = iota

	// ConsistencyNone reads from the local SQLite on this node, with no
	// communication to other nodes. Fastest option (~8µs) but the data
	// may be stale if this node is a follower behind on replication or
	// is partitioned from the cluster.
	// Use for: dashboards, analytics, data where momentary staleness is OK.
	ConsistencyNone

	// ConsistencyWeak reads from the leader. If this node is the leader,
	// it reads locally. If not, it forwards the query to the leader.
	// This ensures you always read from the node that processes writes,
	// so data is fresh. However, there is a small window (~1 heartbeat
	// timeout) where a just-deposed leader still believes it is the leader
	// and serves a stale local read before stepping down.
	// Use for: most applications — fresh data with minimal overhead.
	ConsistencyWeak

	// ConsistencyStrong provides linearizable reads. The leader contacts
	// a quorum of nodes to confirm it still holds leadership before
	// reading. If this node is not the leader, the query is forwarded.
	// Guarantees you read the latest committed state — impossible to get
	// stale data, even during leadership transitions.
	// Use for: financial transactions, uniqueness checks, anything where
	// reading stale data would cause incorrect behavior.
	ConsistencyStrong

	// ConsistencyLease reads locally while the local read lease is valid,
	// falling back to ConsistencyWeak (leader forwarding) when it expires.
	//
	// The lease is granted locally, not coordinated with the leader: a
	// background loop polls Raft's last-contact stat every
	// HeartbeatTimeout/4 and extends the lease by HeartbeatTimeout/2 while
	// the leader heartbeat is fresh. That makes Lease a freshness
	// *heuristic*, not a linearizability guarantee: worst-case staleness is
	// ~0.75 × HeartbeatTimeout (poll age + grant window), and a follower
	// that silently lost its leader can keep serving local reads for up to
	// the remaining lease window before falling back to forwarding.
	// This gives ~6µs reads with staleness bounded by ~HeartbeatTimeout.
	// Use for: read-heavy paths that want local-read speed and can tolerate
	// up to a heartbeat of staleness; use Strong when correctness depends
	// on reading the latest committed state.
	ConsistencyLease
)

type contextKey int

const consistencyKey contextKey = 0

// WithConsistency returns a context that carries the specified consistency level.
// Use this with QueryContext to override the node's default consistency.
//
//	ctx := colmena.WithConsistency(ctx, colmena.ConsistencyStrong)
//	rows, err := db.QueryContext(ctx, "SELECT ...")
func WithConsistency(ctx context.Context, level ConsistencyLevel) context.Context {
	return context.WithValue(ctx, consistencyKey, level)
}

func consistencyFromContext(ctx context.Context, defaultLevel ConsistencyLevel) ConsistencyLevel {
	if v, ok := ctx.Value(consistencyKey).(ConsistencyLevel); ok {
		return v
	}
	return defaultLevel
}

// readLease tracks a time-based lease granted by the Raft leader.
// While the lease is valid, followers can serve reads locally without
// an RPC roundtrip to the leader.
type readLease struct {
	mu         sync.RWMutex
	validUntil time.Time
}

// valid returns true if the lease has not expired.
func (l *readLease) valid() bool {
	l.mu.RLock()
	v := time.Now().Before(l.validUntil)
	l.mu.RUnlock()
	return v
}

// extend sets the lease expiry to now + duration.
func (l *readLease) extend(d time.Duration) {
	l.mu.Lock()
	l.validUntil = time.Now().Add(d)
	l.mu.Unlock()
}
