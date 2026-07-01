package jobs

import (
	"fmt"
	"net/http"
	"strings"
)

// MetricsHandler returns an http.Handler that exposes jobs metrics in
// Prometheus text format. Mount it under any path you like, separately
// from colmena's own MetricsHandler:
//
//	http.Handle("/metrics", node.MetricsHandler())
//	http.Handle("/jobs/metrics", jobs.MetricsHandler(m))
//
// The exported names are prefixed with colmena_jobs_ to avoid colliding
// with the existing colmena_* metrics.
func MetricsHandler(m *Manager) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s, err := m.Stats()
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		var b strings.Builder

		writeCounter(&b, "colmena_jobs_executed_total", "Jobs handlers invoked on this node since start", s.Executed)
		writeCounter(&b, "colmena_jobs_succeeded_total", "Job runs that returned nil error", s.Succeeded)
		writeCounter(&b, "colmena_jobs_failed_total", "Job runs that returned a non-nil error", s.Failed)
		writeCounter(&b, "colmena_jobs_retried_total", "Job runs that were rescheduled for retry", s.Retried)
		writeCounter(&b, "colmena_jobs_dead_total", "Jobs that reached max_attempts and were marked dead", s.Dead)
		writeCounter(&b, "colmena_jobs_reaped_total", "Terminal jobs deleted by the retention reaper on this node", s.Reaped)

		fmt.Fprintln(&b, "# HELP colmena_jobs_queue Cluster-wide job count by status")
		fmt.Fprintln(&b, "# TYPE colmena_jobs_queue gauge")
		for status, count := range s.ByStatus {
			fmt.Fprintf(&b, "colmena_jobs_queue{status=%q} %d\n", string(status), count)
		}

		fmt.Fprintln(&b, "# HELP colmena_jobs_pending_by_type Pending jobs per type")
		fmt.Fprintln(&b, "# TYPE colmena_jobs_pending_by_type gauge")
		for t, count := range s.PendingByType {
			fmt.Fprintf(&b, "colmena_jobs_pending_by_type{type=%q} %d\n", t, count)
		}

		_, _ = w.Write([]byte(b.String()))
	})
}

func writeCounter(b *strings.Builder, name, help string, value uint64) {
	fmt.Fprintf(b, "# HELP %s %s\n", name, help)
	fmt.Fprintf(b, "# TYPE %s counter\n", name)
	fmt.Fprintf(b, "%s %d\n", name, value)
}
