package jobs

import "time"

// Config configures the jobs subsystem attached to a Colmena node.
type Config struct {
	// Workers is the number of in-process worker goroutines per node.
	// Default: max(2, GOMAXPROCS).
	Workers int

	// PollInterval is how often each worker polls for new jobs when idle.
	// Default: 1s.
	PollInterval time.Duration

	// DefaultTimeout is applied to jobs enqueued without an explicit timeout.
	// Default: 5m.
	DefaultTimeout time.Duration

	// DefaultMaxAttempts is applied to jobs enqueued without an explicit value.
	// Default: 5.
	DefaultMaxAttempts int

	// SweepInterval controls how often the leader scans for orphaned jobs
	// (claimed_at + timeout_ms < now and status in claimed/running). Default: 30s.
	SweepInterval time.Duration

	// ScheduleInterval is the cadence of the cron scheduler tick. Default: 30s.
	ScheduleInterval time.Duration

	// DefaultBackoff is the base backoff for retries when a handler does not
	// register a custom backoff. Default: 5s base, 1h cap.
	DefaultBackoff Backoff

	// RetainTerminal is how long a job in a terminal state (succeeded or dead)
	// is kept before the reaper deletes it. Enqueue/claim/finalise never remove
	// rows — a completed job just flips to 'succeeded' — so without reaping the
	// colmena_jobs table grows without bound. That bloats the store and, because
	// every Raft snapshot copies the whole database, inflates snapshot size and
	// memory until nodes OOM. Default: 24h. Set to a negative duration to
	// disable reaping and keep all history.
	RetainTerminal time.Duration

	// ReapInterval is how often the leader deletes expired terminal jobs (and
	// compacts the store once it holds enough free pages to be worth it).
	// Default: 10m.
	ReapInterval time.Duration
}

func (c *Config) applyDefaults() {
	if c.Workers <= 0 {
		c.Workers = 4
	}
	if c.PollInterval <= 0 {
		c.PollInterval = 1 * time.Second
	}
	if c.DefaultTimeout <= 0 {
		c.DefaultTimeout = 5 * time.Minute
	}
	if c.DefaultMaxAttempts <= 0 {
		c.DefaultMaxAttempts = 5
	}
	if c.SweepInterval <= 0 {
		c.SweepInterval = 30 * time.Second
	}
	if c.ScheduleInterval <= 0 {
		c.ScheduleInterval = 30 * time.Second
	}
	if c.DefaultBackoff.Base <= 0 {
		c.DefaultBackoff.Base = 5 * time.Second
	}
	if c.DefaultBackoff.Max <= 0 {
		c.DefaultBackoff.Max = 1 * time.Hour
	}
	// Zero means "unset" → default. A negative value is meaningful (reaping
	// disabled), so only rewrite the zero case.
	if c.RetainTerminal == 0 {
		c.RetainTerminal = 24 * time.Hour
	}
	if c.ReapInterval <= 0 {
		c.ReapInterval = 10 * time.Minute
	}
}
