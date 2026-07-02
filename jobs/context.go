package jobs

import (
	"context"
	"time"
)

// Context is what a job handler receives. It embeds context.Context so
// existing code (HTTP clients, db calls) inherits cancellation when the
// job times out or the manager shuts down.
type Context interface {
	context.Context

	// JobID returns the unique identifier of the running job.
	JobID() string

	// Type returns the registered job type ("scrape_anilist", etc.).
	Type() string

	// Attempt returns the 1-indexed attempt number (1 on first run).
	Attempt() int

	// EnqueuedAt is when the job was first put on the queue.
	EnqueuedAt() time.Time

	// NodeID is the local node executing this attempt.
	NodeID() string
}

// jobCtx is the concrete implementation of Context.
type jobCtx struct {
	context.Context
	jobID      string
	jobType    string
	attempt    int
	enqueuedAt time.Time
	nodeID     string
}

func (c *jobCtx) JobID() string         { return c.jobID }
func (c *jobCtx) Type() string          { return c.jobType }
func (c *jobCtx) Attempt() int          { return c.attempt }
func (c *jobCtx) EnqueuedAt() time.Time { return c.enqueuedAt }
func (c *jobCtx) NodeID() string        { return c.nodeID }
