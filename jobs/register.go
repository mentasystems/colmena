package jobs

import (
	"encoding/json"
	"fmt"
	"sync"
)

// HandlerFunc is the internal, type-erased handler invoked by the worker
// loop. The type-safe Register[Args] adapter wraps a generic handler with
// JSON unmarshalling.
type HandlerFunc func(ctx Context, payload []byte) error

// handlerEntry stores per-type configuration alongside the handler itself.
type handlerEntry struct {
	fn      HandlerFunc
	backoff Backoff // zero means use Manager.config.DefaultBackoff
}

type handlerRegistry struct {
	mu      sync.RWMutex
	entries map[string]*handlerEntry
}

func newHandlerRegistry() *handlerRegistry {
	return &handlerRegistry{entries: make(map[string]*handlerEntry)}
}

func (r *handlerRegistry) register(jobType string, fn HandlerFunc, opts []HandlerOption) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.entries[jobType]; exists {
		panic(fmt.Sprintf("colmena/jobs: handler %q already registered", jobType))
	}
	e := &handlerEntry{fn: fn}
	for _, o := range opts {
		o(e)
	}
	r.entries[jobType] = e
}

func (r *handlerRegistry) lookup(jobType string) (*handlerEntry, bool) {
	r.mu.RLock()
	e, ok := r.entries[jobType]
	r.mu.RUnlock()
	return e, ok
}

func (r *handlerRegistry) types() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.entries))
	for k := range r.entries {
		out = append(out, k)
	}
	return out
}

// HandlerOption customises handler-level behaviour at registration time.
type HandlerOption func(*handlerEntry)

// WithBackoff overrides the manager's default backoff for this handler.
func WithBackoff(b Backoff) HandlerOption {
	return func(e *handlerEntry) { e.backoff = b }
}

// Register binds a typed handler to a job type on the given manager. The
// args type must be JSON-marshalable; the same type must be used by both
// Register and Enqueue.
//
// Calling Register twice for the same job type panics — handler registration
// is meant to happen once at startup.
func Register[Args any](m *Manager, jobType string, handler func(ctx Context, args Args) error, opts ...HandlerOption) {
	m.handlers.register(jobType, func(ctx Context, payload []byte) error {
		var args Args
		if len(payload) > 0 {
			if err := json.Unmarshal(payload, &args); err != nil {
				return fmt.Errorf("unmarshal args: %w", err)
			}
		}
		return handler(ctx, args)
	}, opts)
}
