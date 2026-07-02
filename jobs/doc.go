// Package jobs adds a distributed background-job system on top of a
// Colmena node. Jobs are persisted in the same replicated SQLite store and
// claimed through Raft, so a job runs on exactly one node across the
// cluster, survives leader changes, and is restored from snapshot/backup
// like any other row.
//
// # Quick start
//
// Bring up a Colmena node, attach a manager, register a typed handler,
// and enqueue work:
//
//	node, _ := colmena.New(colmena.Config{...})
//	defer node.Close()
//
//	jm, _ := jobs.New(node, jobs.Config{Workers: 16})
//	defer jm.Close()
//
//	type ScrapeArgs struct{ Page int }
//
//	jobs.Register(jm, "scrape", func(ctx jobs.Context, a ScrapeArgs) error {
//	    return doScrape(ctx, a.Page)
//	})
//
//	id, _ := jobs.Enqueue(jm, "scrape", ScrapeArgs{Page: 1})
//
// # Recurring jobs
//
// Schedule installs a cron-style recurring job. The schedule lives in a
// replicated table, so passing the same id from a fresh process updates
// the existing entry instead of duplicating it. The cron parser supports
// the standard 5-field syntax (minute, hour, day-of-month, month,
// day-of-week) with `*`, `*/N`, `N`, `N-M`, and lists.
//
//	jobs.Schedule(jm, "refresh-airing", "refresh_airing", "0 */6 * * *", RefreshArgs{})
//
// # Cluster-wide limits
//
// SetConcurrency caps simultaneous executions of a job type cluster-wide;
// SetRateLimit caps starts within a rolling window. Both checks are part
// of the atomic claim UPDATE, so they are race-safe across nodes:
//
//	jobs.SetConcurrency(jm, "scrape_justwatch", 2)
//	jobs.SetRateLimit(jm,   "scrape_justwatch", jobs.Rate{N: 30, Per: time.Minute})
//
// # Reliability
//
// Failed handlers retry with exponential backoff plus jitter. After
// MaxAttempts, the job is moved to status "dead" for inspection. A
// leader-only sweeper reclaims jobs whose worker died (no progress past
// timeout_ms) so a crashed node never strands work.
//
// Use WithUniqueKey to dedupe pending/running jobs:
//
//	jobs.Enqueue(jm, "scrape", ScrapeArgs{Page: 2}, jobs.WithUniqueKey("scrape:2"))
//
// # Observability
//
// Manager.Stats returns counters and queue depth. MetricsHandler exposes
// them as Prometheus text. AdminHandler serves a small read-only HTML
// dashboard plus JSON endpoints (mount it behind your own auth):
//
//	http.Handle("/admin/jobs/", http.StripPrefix("/admin/jobs", jobs.AdminHandler(jm)))
//	http.Handle("/jobs/metrics", jobs.MetricsHandler(jm))
package jobs
