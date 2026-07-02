package jobs

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestJobCtx_Getters verifies the Context implementation returns the values
// set when the worker constructed it.
func TestJobCtx_Getters(t *testing.T) {
	enq := time.Unix(1234567890, 0).UTC()
	c := &jobCtx{
		Context:    context.Background(),
		jobID:      "job-1",
		jobType:    "scrape",
		attempt:    3,
		enqueuedAt: enq,
		nodeID:     "node-a",
	}
	if c.JobID() != "job-1" {
		t.Fatalf("JobID")
	}
	if c.Type() != "scrape" {
		t.Fatalf("Type")
	}
	if c.Attempt() != 3 {
		t.Fatalf("Attempt")
	}
	if !c.EnqueuedAt().Equal(enq) {
		t.Fatalf("EnqueuedAt")
	}
	if c.NodeID() != "node-a" {
		t.Fatalf("NodeID")
	}
}

// TestEnqueueOptions exercises the option setters that aren't already covered
// by higher-level integration tests.
func TestEnqueueOptions(t *testing.T) {
	o := &enqueueOpts{}
	WithPriority(PriorityHigh)(o)
	if o.priority != PriorityHigh {
		t.Fatalf("WithPriority")
	}
	when := time.Now().Add(1 * time.Hour)
	WithRunAt(when)(o)
	if !o.runAt.Equal(when) {
		t.Fatalf("WithRunAt")
	}
}

// TestWithBackoff installs a custom backoff via the handler option path.
func TestWithBackoff(t *testing.T) {
	e := &handlerEntry{}
	want := Backoff{Base: 5 * time.Second, Max: 30 * time.Second}
	WithBackoff(want)(e)
	if e.backoff != want {
		t.Fatalf("WithBackoff = %+v, want %+v", e.backoff, want)
	}
}

func TestStringHelpers(t *testing.T) {
	if !contains("hello world", "world") {
		t.Fatalf("contains: positive")
	}
	if contains("hi", "longer") {
		t.Fatalf("contains: short haystack")
	}
	if contains("abcdef", "xyz") {
		t.Fatalf("contains: missing")
	}
	if indexOf("abcdef", "cd") != 2 {
		t.Fatalf("indexOf: position")
	}
	if indexOf("abc", "xyz") != -1 {
		t.Fatalf("indexOf: missing")
	}
}

func TestIsUniqueViolation(t *testing.T) {
	if isUniqueViolation(nil) {
		t.Fatalf("nil err")
	}
	if !isUniqueViolation(errors.New("UNIQUE constraint failed: x.id")) {
		t.Fatalf("classic form")
	}
	if !isUniqueViolation(errors.New("constraint failed: UNIQUE")) {
		t.Fatalf("alt form")
	}
	if isUniqueViolation(errors.New("disk full")) {
		t.Fatalf("unrelated err")
	}
}

func TestUnschedule(t *testing.T) {
	_, m := testManager(t)
	if err := Schedule(m, "sched-1", "noop", "* * * * *", struct{}{}); err != nil {
		t.Fatalf("schedule: %v", err)
	}
	if err := Unschedule(m, "sched-1"); err != nil {
		t.Fatalf("unschedule: %v", err)
	}
	// Unscheduling a missing id is a no-op (UPDATE matches no rows).
	if err := Unschedule(m, "nope"); err != nil {
		t.Fatalf("unschedule missing: %v", err)
	}
}

// TestAdminHandler hits the three admin endpoints to cover the HTTP layer.
func TestAdminHandler(t *testing.T) {
	_, m := testManager(t)
	Register(m, "noop", func(ctx Context, _ struct{}) error { return nil })
	if _, err := Enqueue(m, "noop", struct{}{}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	srv := httptest.NewServer(http.StripPrefix("/admin", AdminHandler(m)))
	t.Cleanup(srv.Close)

	cases := []struct {
		path   string
		needle string
	}{
		{"/admin/", "colmena jobs"},
		{"/admin/stats.json", `"`},
		{"/admin/jobs.json", `[`},
		{"/admin/jobs.json?status=pending", `[`},
	}
	for _, tc := range cases {
		resp, err := http.Get(srv.URL + tc.path)
		if err != nil {
			t.Fatalf("GET %s: %v", tc.path, err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("GET %s: status %d body=%s", tc.path, resp.StatusCode, body)
		}
		if !strings.Contains(string(body), tc.needle) {
			t.Fatalf("GET %s: body missing %q: %s", tc.path, tc.needle, body)
		}
	}
}

// TestConfig_ApplyDefaults verifies every field falls back to its default
// when the caller leaves it zero.
func TestConfig_ApplyDefaults(t *testing.T) {
	c := &Config{}
	c.applyDefaults()
	if c.Workers <= 0 || c.PollInterval <= 0 || c.DefaultTimeout <= 0 ||
		c.DefaultMaxAttempts <= 0 || c.SweepInterval <= 0 ||
		c.ScheduleInterval <= 0 || c.DefaultBackoff.Base <= 0 ||
		c.DefaultBackoff.Max <= 0 {
		t.Fatalf("applyDefaults left zero fields: %+v", c)
	}

	// Pre-set values must be preserved.
	c2 := &Config{
		Workers: 99, PollInterval: 7 * time.Second,
		DefaultTimeout: 7 * time.Second, DefaultMaxAttempts: 7,
		SweepInterval: 7 * time.Second, ScheduleInterval: 7 * time.Second,
		DefaultBackoff: Backoff{Base: 7 * time.Second, Max: 7 * time.Second},
	}
	c2.applyDefaults()
	if c2.Workers != 99 || c2.DefaultMaxAttempts != 7 {
		t.Fatalf("applyDefaults overwrote explicit values: %+v", c2)
	}
}

// TestSetConcurrencyAndRateLimit exercises both the install and remove paths.
func TestSetConcurrencyAndRateLimit(t *testing.T) {
	_, m := testManager(t)

	if err := SetConcurrency(m, "noop", 5); err != nil {
		t.Fatalf("set concurrency: %v", err)
	}
	if err := SetConcurrency(m, "noop", 0); err != nil {
		t.Fatalf("clear concurrency: %v", err)
	}
	if err := SetRateLimit(m, "noop", Rate{N: 10, Per: 1 * time.Second}); err != nil {
		t.Fatalf("set rate: %v", err)
	}
	if err := SetRateLimit(m, "noop", Rate{N: 0, Per: 0}); err != nil {
		t.Fatalf("clear rate: %v", err)
	}
}

// TestFinalise_DeadAndRetry covers the dead-letter and retry branches that
// the happy-path tests don't reach.
func TestFinalise_DeadAndRetry(t *testing.T) {
	_, m := testManager(t)
	Register(m, "boom", func(ctx Context, _ struct{}) error { return errors.New("explode") })

	// Dead path: attempts already at max, finalise marks it dead.
	if _, err := m.node.DB().Exec(
		`INSERT INTO colmena_jobs (id, type, payload, status, priority, attempts, max_attempts,
		 enqueued_at, run_at, timeout_ms)
		 VALUES ('dead-1', 'boom', '{}', 'running', 0, 3, 3, ?, ?, 1000)`,
		time.Now().UnixMilli(), time.Now().UnixMilli(),
	); err != nil {
		t.Fatalf("seed dead: %v", err)
	}
	m.finalise(&Job{ID: "dead-1", Type: "boom", Attempts: 3, MaxAttempts: 3}, errors.New("kaboom"))
	var status string
	m.node.DB().QueryRow(`SELECT status FROM colmena_jobs WHERE id='dead-1'`).Scan(&status)
	if status != "dead" {
		t.Fatalf("dead path: status = %s", status)
	}

	// Retry path: attempts < max, finalise reschedules as pending.
	if _, err := m.node.DB().Exec(
		`INSERT INTO colmena_jobs (id, type, payload, status, priority, attempts, max_attempts,
		 enqueued_at, run_at, timeout_ms)
		 VALUES ('retry-1', 'boom', '{}', 'running', 0, 1, 5, ?, ?, 1000)`,
		time.Now().UnixMilli(), time.Now().UnixMilli(),
	); err != nil {
		t.Fatalf("seed retry: %v", err)
	}
	m.finalise(&Job{ID: "retry-1", Type: "boom", Attempts: 1, MaxAttempts: 5}, errors.New("transient"))
	m.node.DB().QueryRow(`SELECT status FROM colmena_jobs WHERE id='retry-1'`).Scan(&status)
	if status != "pending" {
		t.Fatalf("retry path: status = %s", status)
	}

	// errNoHandler path: a job whose handler isn't registered locally is
	// released back to pending without consuming an attempt.
	if _, err := m.node.DB().Exec(
		`INSERT INTO colmena_jobs (id, type, payload, status, priority, attempts, max_attempts,
		 enqueued_at, run_at, timeout_ms)
		 VALUES ('noh-1', 'unknown', '{}', 'running', 0, 1, 5, ?, ?, 1000)`,
		time.Now().UnixMilli(), time.Now().UnixMilli(),
	); err != nil {
		t.Fatalf("seed noh: %v", err)
	}
	m.finalise(&Job{ID: "noh-1", Type: "unknown", Attempts: 1, MaxAttempts: 5}, errNoHandler)
	m.node.DB().QueryRow(`SELECT status FROM colmena_jobs WHERE id='noh-1'`).Scan(&status)
	if status != "pending" {
		t.Fatalf("no-handler path: status = %s", status)
	}
}

// TestPlaceholders covers both branches of the helper.
func TestPlaceholders(t *testing.T) {
	if placeholders(0) != "" {
		t.Fatalf("zero")
	}
	if placeholders(3) != "?, ?, ?" {
		t.Fatalf("three: %q", placeholders(3))
	}
}

// TestRunHandler_NoHandler exercises runHandler's missing-handler branch.
func TestRunHandler_NoHandler(t *testing.T) {
	_, m := testManager(t)
	if err := m.runHandler(&Job{ID: "x", Type: "nope"}); !errors.Is(err, errNoHandler) {
		t.Fatalf("expected errNoHandler, got %v", err)
	}
}

// TestSweepOnce_ReclaimsOrphan inserts a job whose claim has expired and
// verifies sweepOnce moves it back to pending.
func TestSweepOnce_ReclaimsOrphan(t *testing.T) {
	_, m := testManager(t)
	old := time.Now().Add(-1 * time.Hour).UnixMilli()
	if _, err := m.node.DB().Exec(
		`INSERT INTO colmena_jobs (id, type, payload, status, priority, attempts, max_attempts,
		 enqueued_at, run_at, claimed_at, claimed_by, timeout_ms)
		 VALUES ('orph', 't', '{}', 'running', 0, 1, 3, ?, ?, ?, 'dead-node', 1000)`,
		old, old, old,
	); err != nil {
		t.Fatalf("insert orphan: %v", err)
	}
	m.sweepOnce()
	var status string
	if err := m.node.DB().QueryRow(`SELECT status FROM colmena_jobs WHERE id = 'orph'`).Scan(&status); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if status != "pending" {
		t.Fatalf("status = %s, want pending", status)
	}
}

// TestSchedulerOnce_FiresAndAdvances inserts a due schedule row and verifies
// schedulerOnce enqueues a job and advances next_run_at.
func TestSchedulerOnce_FiresAndAdvances(t *testing.T) {
	_, m := testManager(t)
	past := time.Now().Add(-1 * time.Hour).UnixMilli()
	if _, err := m.node.DB().Exec(
		`INSERT INTO colmena_jobs_schedule
		 (id, job_type, cron_expr, payload, next_run_at, enabled)
		 VALUES ('s1', 'noop', '* * * * *', '{}', ?, 1)`, past,
	); err != nil {
		t.Fatalf("insert schedule: %v", err)
	}
	// Also insert a row with a bad cron to exercise the parse-error branch.
	if _, err := m.node.DB().Exec(
		`INSERT INTO colmena_jobs_schedule
		 (id, job_type, cron_expr, payload, next_run_at, enabled)
		 VALUES ('s-bad', 'noop', 'not-a-cron', '{}', ?, 1)`, past,
	); err != nil {
		t.Fatalf("insert bad: %v", err)
	}

	m.schedulerOnce()

	var pending int
	if err := m.node.DB().QueryRow(`SELECT COUNT(*) FROM colmena_jobs WHERE type='noop'`).Scan(&pending); err != nil {
		t.Fatalf("count: %v", err)
	}
	if pending != 1 {
		t.Fatalf("expected 1 enqueued, got %d", pending)
	}
	var nextRun int64
	m.node.DB().QueryRow(`SELECT next_run_at FROM colmena_jobs_schedule WHERE id='s1'`).Scan(&nextRun)
	if nextRun <= past {
		t.Fatalf("next_run_at not advanced: %d <= %d", nextRun, past)
	}
}

// TestMetricsHandler verifies the Prometheus exposition output.
func TestMetricsHandler(t *testing.T) {
	_, m := testManager(t)
	Register(m, "noop", func(ctx Context, _ struct{}) error { return nil })
	if _, err := Enqueue(m, "noop", struct{}{}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	srv := httptest.NewServer(MetricsHandler(m))
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d body=%s", resp.StatusCode, body)
	}
	for _, want := range []string{
		"colmena_jobs_executed_total",
		"colmena_jobs_succeeded_total",
		"colmena_jobs_queue",
		"# TYPE colmena_jobs_queue gauge",
	} {
		if !strings.Contains(string(body), want) {
			t.Fatalf("metrics missing %q in output:\n%s", want, body)
		}
	}
}
