package jobs

import (
	"encoding/json"
	"fmt"
	"html"
	"net/http"
	"strings"
	"time"
)

// AdminHandler returns a read-only HTTP handler that surfaces queue state.
// Mount it under any prefix you like:
//
//	http.Handle("/admin/jobs/", jobs.AdminHandler(m))
//
// The handler only serves GETs. Anything destructive lives elsewhere; the
// host app is expected to wrap this handler in its own auth middleware.
func AdminHandler(m *Manager) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/stats.json", func(w http.ResponseWriter, r *http.Request) {
		s, err := m.Stats()
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(s)
	})

	mux.HandleFunc("/jobs.json", func(w http.ResponseWriter, r *http.Request) {
		status := r.URL.Query().Get("status")
		jobs, err := listJobs(m, status, 200)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(jobs)
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Default route: render a tiny HTML dashboard.
		s, err := m.Stats()
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		recent, _ := listJobs(m, "", 50)
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		renderDashboard(w, s, recent)
	})

	return http.StripPrefix(stripPrefix(mux), mux)
}

// stripPrefix is a no-op wrapper kept so the mux works regardless of how
// callers mount it. http.StripPrefix(...) is applied by the host's mux,
// not here.
func stripPrefix(_ *http.ServeMux) string { return "" }

type adminJob struct {
	ID          string `json:"id"`
	Type        string `json:"type"`
	Status      string `json:"status"`
	Priority    int    `json:"priority"`
	Attempts    int    `json:"attempts"`
	MaxAttempts int    `json:"max_attempts"`
	EnqueuedAt  int64  `json:"enqueued_at"`
	RunAt       int64  `json:"run_at"`
	ClaimedBy   string `json:"claimed_by"`
	LastError   string `json:"last_error"`
}

func listJobs(m *Manager, status string, limit int) ([]adminJob, error) {
	jobs := []adminJob{} // non-nil: the JSON API returns [] for an empty list
	q := `SELECT id, type, status, priority, attempts, max_attempts,
                 enqueued_at, run_at,
                 COALESCE(claimed_by, ''), COALESCE(last_error, '')
            FROM colmena_jobs`
	args := []any{}
	if status != "" {
		q += ` WHERE status = ?`
		args = append(args, status)
	}
	q += ` ORDER BY enqueued_at DESC LIMIT ?`
	args = append(args, limit)

	rows, err := m.node.DB().Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := jobs
	for rows.Next() {
		var j adminJob
		if err := rows.Scan(
			&j.ID, &j.Type, &j.Status, &j.Priority, &j.Attempts, &j.MaxAttempts,
			&j.EnqueuedAt, &j.RunAt, &j.ClaimedBy, &j.LastError,
		); err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, nil
}

func renderDashboard(w http.ResponseWriter, s *Stats, recent []adminJob) {
	var b strings.Builder
	b.WriteString(`<!doctype html><html><head><meta charset="utf-8">`)
	b.WriteString(`<title>colmena jobs</title>`)
	b.WriteString(`<style>
        body{font:14px/1.4 -apple-system,BlinkMacSystemFont,sans-serif;margin:2em;color:#222}
        h1,h2{font-weight:600}
        table{border-collapse:collapse;width:100%;margin-top:1em}
        th,td{text-align:left;padding:6px 10px;border-bottom:1px solid #eee}
        th{background:#f5f5f5}
        td.err{color:#a00;max-width:30em;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
        .pill{padding:2px 6px;border-radius:6px;font-size:12px}
        .s-pending{background:#eef}
        .s-running{background:#ffe;color:#660}
        .s-succeeded{background:#efe;color:#060}
        .s-failed{background:#fee;color:#900}
        .s-dead{background:#222;color:#fff}
    </style></head><body>`)

	fmt.Fprintf(&b, "<h1>colmena jobs</h1>")
	fmt.Fprintf(&b, "<p>executed=%d succeeded=%d failed=%d retried=%d dead=%d</p>",
		s.Executed, s.Succeeded, s.Failed, s.Retried, s.Dead)

	b.WriteString("<h2>Queue depth</h2><table><tr><th>status</th><th>count</th></tr>")
	for _, st := range []Status{StatusPending, StatusRunning, StatusSucceeded, StatusFailed, StatusDead} {
		fmt.Fprintf(&b, "<tr><td><span class=\"pill s-%s\">%s</span></td><td>%d</td></tr>",
			st, st, s.ByStatus[st])
	}
	b.WriteString("</table>")

	b.WriteString("<h2>Recent jobs</h2><table>")
	b.WriteString("<tr><th>id</th><th>type</th><th>status</th><th>attempts</th><th>enqueued</th><th>last error</th></tr>")
	for _, j := range recent {
		fmt.Fprintf(&b,
			"<tr><td><code>%s</code></td><td>%s</td><td><span class=\"pill s-%s\">%s</span></td><td>%d/%d</td><td>%s</td><td class=\"err\">%s</td></tr>",
			html.EscapeString(j.ID),
			html.EscapeString(j.Type),
			html.EscapeString(j.Status), html.EscapeString(j.Status),
			j.Attempts, j.MaxAttempts,
			time.UnixMilli(j.EnqueuedAt).Format(time.RFC3339),
			html.EscapeString(j.LastError),
		)
	}
	b.WriteString("</table></body></html>")

	_, _ = w.Write([]byte(b.String()))
}
