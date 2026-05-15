package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mentasystems/colmena"
)

func main() {
	if len(os.Args) < 4 {
		fmt.Fprintf(os.Stderr, "Usage: %s <node-id> <bind-addr> <bootstrap|join-addr>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  # Bootstrap first node:\n")
		fmt.Fprintf(os.Stderr, "  %s node1 127.0.0.1:9000 bootstrap\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  # Join second node:\n")
		fmt.Fprintf(os.Stderr, "  %s node2 127.0.0.1:9002 127.0.0.1:9000\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  # Join third node:\n")
		fmt.Fprintf(os.Stderr, "  %s node3 127.0.0.1:9004 127.0.0.1:9000\n", os.Args[0])
		os.Exit(1)
	}

	nodeID := os.Args[1]
	bindAddr := os.Args[2]
	joinOrBootstrap := os.Args[3]

	cfg := colmena.Config{
		NodeID:  nodeID,
		DataDir: fmt.Sprintf("./data/%s", nodeID),
		Bind:    bindAddr,
	}

	if joinOrBootstrap == "bootstrap" {
		cfg.Bootstrap = true
	} else {
		cfg.Join = []string{joinOrBootstrap}
	}

	node, err := colmena.New(cfg)
	if err != nil {
		log.Fatalf("Failed to create node: %v", err)
	}
	defer node.Close()

	log.Printf("Node %s started on %s", nodeID, bindAddr)

	// Wait for leader election.
	if err := node.WaitForLeader(10 * time.Second); err != nil {
		log.Fatalf("No leader elected: %v", err)
	}
	log.Printf("Leader elected. This node is leader: %v", node.IsLeader())

	// Get database handle.
	db := node.DB()

	// Create table (only succeeds on leader, forwarded automatically from followers).
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT)")
	if err != nil {
		log.Fatalf("Create table: %v", err)
	}

	// Insert data.
	_, err = db.Exec("INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)",
		fmt.Sprintf("from-%s", nodeID),
		fmt.Sprintf("hello from %s at %s", nodeID, time.Now().Format(time.RFC3339)),
	)
	if err != nil {
		log.Fatalf("Insert: %v", err)
	}
	log.Printf("Inserted data from %s", nodeID)

	// Query data.
	rows, err := db.Query("SELECT key, value FROM kv")
	if err != nil {
		log.Fatalf("Query: %v", err)
	}
	defer rows.Close()

	log.Println("--- Data in cluster ---")
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			log.Fatalf("Scan: %v", err)
		}
		log.Printf("  %s = %s", key, value)
	}

	// Wait for shutdown signal.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	log.Printf("Node %s running. Press Ctrl+C to stop.", nodeID)
	<-sig
	log.Printf("Shutting down node %s...", nodeID)
}
