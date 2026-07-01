package jobs

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mentasystems/colmena"
)

// Manager owns the worker pool and the leader-only background loops
// (sweeper, scheduler) for one Colmena node.
type Manager struct {
	node     *colmena.Node
	config   Config
	handlers *handlerRegistry

	// pokeCh wakes idle workers when a new job is enqueued locally so we
	// don't have to wait for the next poll tick. Buffered=1 means a single
	// pending poke is preserved while many writers fan in.
	pokeCh chan struct{}

	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup

	// Stats counters, updated locally on this node.
	executed  atomic.Uint64
	succeeded atomic.Uint64
	failed    atomic.Uint64
	retried   atomic.Uint64
	dead      atomic.Uint64
	reaped    atomic.Uint64
}

// New starts a jobs manager bound to the given node. Schema is applied
// idempotently via the node (so it replicates across the cluster) and worker
// goroutines plus background loops start immediately. Call Close to shut
// them down before closing the node.
//
// Multiple concurrent New calls on the same node would race on schema setup;
// callers are expected to start the manager once during application startup.
func New(node *colmena.Node, cfg Config) (*Manager, error) {
	cfg.applyDefaults()

	if node == nil {
		return nil, fmt.Errorf("colmena/jobs: nil node")
	}

	// Schema migration is a write — on followers it is forwarded to the
	// leader. We retry briefly to give the cluster time to elect a leader
	// when New is called immediately after colmena.New on bootstrap.
	if err := waitMigrate(node); err != nil {
		return nil, err
	}

	m := &Manager{
		node:     node,
		config:   cfg,
		handlers: newHandlerRegistry(),
		pokeCh:   make(chan struct{}, 1),
		stopCh:   make(chan struct{}),
	}

	m.wg.Add(cfg.Workers)
	for i := 0; i < cfg.Workers; i++ {
		go m.workerLoop(i)
	}

	m.wg.Add(2)
	go m.sweeperLoop()
	go m.schedulerLoop()

	// Reaping is on by default (24h retention); a negative RetainTerminal
	// opts out and keeps all terminal jobs forever.
	if m.config.RetainTerminal >= 0 {
		m.wg.Add(1)
		go m.reaperLoop()
	}

	return m, nil
}

func waitMigrate(node *colmena.Node) error {
	deadline := time.Now().Add(10 * time.Second)
	for {
		if err := migrate(node.DB()); err == nil {
			return nil
		} else if time.Now().After(deadline) {
			return fmt.Errorf("colmena/jobs: schema migration: %w", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Close stops the worker pool and background loops. Jobs already in flight
// are given a short grace period to finish; jobs whose handler ignores
// context cancellation will be reclaimed by the sweeper after they exceed
// their timeout.
func (m *Manager) Close() error {
	m.stopOnce.Do(func() {
		close(m.stopCh)
	})
	done := make(chan struct{})
	go func() { m.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		log.Printf("colmena/jobs: manager close: timed out waiting for workers")
	}
	return nil
}

// poke wakes up to one idle worker without blocking the caller.
func (m *Manager) poke() {
	select {
	case m.pokeCh <- struct{}{}:
	default:
	}
}

// Node returns the underlying Colmena node. Useful for tests and admin code
// that needs to issue queries directly.
func (m *Manager) Node() *colmena.Node { return m.node }

// runHandler invokes the handler with the appropriate context derived from
// the job's timeout. It is called from the worker loop after a successful
// claim.
func (m *Manager) runHandler(j *Job) (err error) {
	entry, ok := m.handlers.lookup(j.Type)
	if !ok {
		// No registered handler on this node: release the job back to
		// pending so a node that does have the handler picks it up.
		// We don't retry locally.
		return errNoHandler
	}

	timeout := j.Timeout
	if timeout <= 0 {
		timeout = m.config.DefaultTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	jc := &jobCtx{
		Context:    ctx,
		jobID:      j.ID,
		jobType:    j.Type,
		attempt:    j.Attempts, // already incremented at claim time
		enqueuedAt: j.EnqueuedAt,
		nodeID:     m.node.NodeID(),
	}

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panic: %v", r)
		}
	}()

	return entry.fn(jc, []byte(j.Payload))
}

// errNoHandler is a sentinel — the worker treats it specially (releases the
// job rather than counting it as a normal failure).
var errNoHandler = fmt.Errorf("colmena/jobs: no handler registered on this node")
