package jobs_test

import (
	"fmt"
	"time"

	"github.com/mentasystems/colmena"
	"github.com/mentasystems/colmena/jobs"
)

// ExampleNew shows the minimum wiring: bring up a Colmena node, attach a
// jobs manager, register a typed handler, and enqueue work.
func ExampleNew() {
	node, _ := colmena.New(colmena.Config{
		NodeID:    "node-1",
		DataDir:   "/tmp/colmena-jobs-example",
		Bind:      "127.0.0.1:9000",
		Bootstrap: true,
	})
	defer node.Close()

	jm, _ := jobs.New(node, jobs.Config{
		Workers:        8,
		PollInterval:   1 * time.Second,
		DefaultTimeout: 5 * time.Minute,
	})
	defer jm.Close()

	type ScrapeArgs struct{ Page int }

	jobs.Register(jm, "scrape", func(ctx jobs.Context, a ScrapeArgs) error {
		fmt.Printf("scraping page %d on %s (attempt %d)\n",
			a.Page, ctx.NodeID(), ctx.Attempt())
		return nil
	})

	_, _ = jobs.Enqueue(jm, "scrape", ScrapeArgs{Page: 1})
}

// ExampleSchedule shows installing a recurring job. The schedule lives in
// colmena_jobs_schedule and survives restarts, so passing the same id from
// a fresh process updates the existing row in place rather than creating a
// duplicate.
func ExampleSchedule() {
	var jm *jobs.Manager // assume jm is initialised

	type RefreshArgs struct{}

	// Every 6 hours.
	_ = jobs.Schedule(jm, "refresh-airing", "refresh_airing", "0 */6 * * *", RefreshArgs{})

	// Every 15 minutes.
	_ = jobs.Schedule(jm, "scrape-news", "scrape_news", "*/15 * * * *", RefreshArgs{})
}

// ExampleSetRateLimit shows installing a cluster-wide rate cap. At most 30
// "scrape_justwatch" jobs may *start* across the whole cluster within any
// rolling 1-minute window.
func ExampleSetRateLimit() {
	var jm *jobs.Manager

	_ = jobs.SetRateLimit(jm, "scrape_justwatch", jobs.Rate{N: 30, Per: time.Minute})
	_ = jobs.SetConcurrency(jm, "scrape_justwatch", 2)
}
