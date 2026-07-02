package jobs

import "time"

// Status is the lifecycle state of a job.
type Status string

const (
	StatusPending   Status = "pending"
	StatusRunning   Status = "running"
	StatusSucceeded Status = "succeeded"
	StatusFailed    Status = "failed"
	StatusDead      Status = "dead"
)

// Priority controls ordering among ready-to-run jobs. Higher values run first.
type Priority int

const (
	PriorityLow    Priority = -10
	PriorityNormal Priority = 0
	PriorityHigh   Priority = 10
)

// Job is the persisted record for a single unit of work.
type Job struct {
	ID          string
	Type        string
	Payload     string
	Status      Status
	Priority    Priority
	Attempts    int
	MaxAttempts int
	EnqueuedAt  time.Time
	RunAt       time.Time
	ClaimedAt   *time.Time
	ClaimedBy   string
	StartedAt   *time.Time
	FinishedAt  *time.Time
	LastError   string
	UniqueKey   string
	Timeout     time.Duration
}

// Backoff describes exponential backoff between retries.
type Backoff struct {
	Base time.Duration
	Max  time.Duration
}

// Rate is a sliding-window rate limit: at most N jobs of a given type may
// have started within any rolling window of duration Per, cluster-wide.
// The check is part of the atomic claim UPDATE, so the limit is honoured
// exactly across nodes — no token-bucket reservation to keep in sync.
type Rate struct {
	N   int
	Per time.Duration
}
