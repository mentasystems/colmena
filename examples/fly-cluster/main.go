// Command fly-cluster demonstrates zero-config Colmena clustering on Fly.io
// using the colmena/fly subpackage.
//
// Deploy the same image to N machines in ONE Fly region. The machines discover
// each other over Fly's internal 6PN DNS, elect a bootstrapper on cold start,
// and form a Raft cluster automatically. The first VoterQuorum machines (default
// 3) become voters; later machines join as non-voting learners that scale read
// throughput without slowing down writes.
//
// The node reads its identity from the Fly machine environment (FLY_MACHINE_ID,
// FLY_PRIVATE_IP, FLY_REGION, FLY_APP_NAME), so there is nothing to configure
// per machine. It exposes an HTTP health check at /health that only reports
// healthy once the node has joined and caught up — wire it into fly.toml so a
// rolling deploy keeps quorum. On SIGTERM it gracefully leaves the cluster.
//
// See ../../fly/README.md for the matching fly.toml.
package main

import (
	"context"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mentasystems/colmena"
	"github.com/mentasystems/colmena/fly"
)

func main() {
	cfg, err := fly.FromEnv()
	if err != nil {
		log.Fatal(err)
	}
	cfg.DataDir = envOr("COLMENA_DATA_DIR", "/data")
	cfg.RaftPort = 9000
	cfg.VoterQuorum = 3

	cluster, err := fly.Start(cfg)
	if err != nil {
		log.Fatal(err)
	}

	// Health endpoint for fly.toml's [[http_service.checks]]. Returns 200 only
	// once this node is in the configuration and (if a follower) caught up.
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if cluster.Healthy() {
			w.WriteHeader(http.StatusOK)
			io.WriteString(w, "ok")
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		io.WriteString(w, "not ready")
	})
	go func() {
		if err := http.ListenAndServe("[::]:8080", nil); err != nil {
			log.Printf("health server: %v", err)
		}
	}()

	// Use the cluster: a simple replicated key/value table.
	db := cluster.Node.OpenDB("default", colmena.ConsistencyNone)
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT)`); err != nil {
		log.Printf("create table: %v", err)
	}
	if _, err := db.Exec(`INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)`,
		"machine:"+cfg.NodeID, time.Now().Format(time.RFC3339)); err != nil {
		log.Printf("write: %v", err)
	}
	log.Printf("ready: node %s in region %s", cfg.NodeID, cfg.Region)

	// Graceful leave on SIGTERM (Fly sends it before SIGKILL). The timeout must
	// be shorter than the Fly machine's shutdown grace period.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if err := cluster.GracefulLeave(ctx); err != nil {
		log.Printf("graceful leave: %v", err)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
