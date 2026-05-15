// Command lan-cluster demonstrates zero-config Colmena clustering over a
// local network using the colmena/lan subpackage.
//
// Build the same binary, flash it onto every machine, and run it. The
// nodes will discover each other via mDNS, elect a bootstrapper, and
// form a cluster automatically. The first VoterQuorum nodes (default 3)
// become Raft voters; later nodes join as non-voting learners that
// scale read throughput without slowing down writes.
//
//	go run ./examples/lan-cluster
//
// To simulate multiple nodes on a single machine, run the binary in
// several terminals with different DataDir/Bind values:
//
//	./lan-cluster --data ./data/n1 --bind 127.0.0.1:9000
//	./lan-cluster --data ./data/n2 --bind 127.0.0.1:9100
//	./lan-cluster --data ./data/n3 --bind 127.0.0.1:9200
//
// The example uses a CA + key generated at runtime — for a real
// deployment, embed a fixed CA in the binary with go:embed so the same
// image identifies the same cluster everywhere.
package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"encoding/pem"
	"flag"
	"fmt"
	"log"
	"math/big"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/mentasystems/colmena"
	"github.com/mentasystems/colmena/lan"
)

func main() {
	dataDir := flag.String("data", "./data/lan", "data directory")
	bind := flag.String("bind", "0.0.0.0:9000", "bind address (host:port)")
	caDir := flag.String("ca", "./data/ca", "directory holding ca.crt + ca.key (created on first run)")
	benchWriters := flag.Int("bench-writers", 0, "if >0, run a write benchmark with this many concurrent writers and exit")
	benchReaders := flag.Int("bench-readers", 0, "if >0, run a local read benchmark with this many concurrent readers and exit")
	benchDuration := flag.Duration("bench-duration", 10*time.Second, "duration of each benchmark phase")
	benchTxStmts := flag.Int("bench-tx-stmts", 0, "if >0, each write op is a transaction with this many statements")
	deadVoterTimeout := flag.Duration("dead-voter-timeout", 0, "override DeadVoterTimeout (0 = default 5m); set short for failover testing")
	flag.Parse()

	caPEM, caKeyPEM, err := loadOrGenCA(*caDir)
	if err != nil {
		log.Fatal(err)
	}

	cluster, err := lan.Start(lan.Config{
		DataDir:          *dataDir,
		Bind:             *bind,
		CACert:           caPEM,
		CAKey:            caKeyPEM,
		VoterQuorum:      3,
		DiscoveryWindow:  5 * time.Second,
		DeadVoterTimeout: *deadVoterTimeout,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer cluster.Close()

	if err := cluster.Node.WaitForLeader(15 * time.Second); err != nil {
		log.Fatal(err)
	}

	// Open with explicit ConsistencyNone so reads stay local even during
	// leader transitions. (The node-level default in colmena.Config is
	// pinned to Weak when left at zero; OpenDB lets us bypass that.)
	db := cluster.Node.OpenDB("default", colmena.ConsistencyNone)
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT)`); err != nil {
		log.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS bench (id INTEGER PRIMARY KEY, payload TEXT)`); err != nil {
		log.Fatal(err)
	}

	if *benchWriters > 0 {
		runWriteBench(db, *benchWriters, *benchDuration, *benchTxStmts)
		return
	}
	if *benchReaders > 0 {
		runReadBench(db, *benchReaders, *benchDuration)
		return
	}

	hostname, _ := os.Hostname()
	if _, err := db.Exec(`INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)`, "host:"+hostname, time.Now().Format(time.RFC3339)); err != nil {
		log.Printf("write: %v", err)
	}

	fmt.Printf("ready on %s as node %s\n", *bind, cluster.Node.NodeID())

	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	for {
		select {
		case <-stop:
			return
		case <-tick.C:
			rows, err := db.Query(`SELECT key, value FROM kv ORDER BY key`)
			if err != nil {
				fmt.Printf("--- %s read failed: %v ---\n", time.Now().Format(time.RFC3339), err)
				continue
			}
			fmt.Printf("--- %s contents ---\n", time.Now().Format(time.RFC3339))
			for rows.Next() {
				var k, v string
				_ = rows.Scan(&k, &v)
				fmt.Printf("  %s = %s\n", k, v)
			}
			rows.Close()
		}
	}
}

// runWriteBench fires N concurrent goroutines that each loop on INSERT
// statements for the configured duration. Writes go through Colmena's
// usual leader-forwarding path, so the throughput reflects what the
// real cluster can sustain (Raft fsync + replication + apply on every
// node). Reports total ops, ops/sec, and p50/p95/p99 latency.
func runWriteBench(db *sql.DB, writers int, dur time.Duration, txStmts int) {
	if txStmts > 0 {
		fmt.Printf("write bench: %d writers x txn(%d stmts) for %s\n", writers, txStmts, dur)
	} else {
		fmt.Printf("write bench: %d concurrent writers for %s\n", writers, dur)
	}
	deadline := time.Now().Add(dur)
	var ops atomic.Int64
	var stmtsApplied atomic.Int64

	latsCh := make(chan time.Duration, writers*1024)
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			i := 0
			for time.Now().Before(deadline) {
				start := time.Now()
				if txStmts > 0 {
					tx, err := db.Begin()
					if err != nil {
						i++
						continue
					}
					for k := 0; k < txStmts; k++ {
						_, _ = tx.Exec(
							`INSERT INTO bench (id, payload) VALUES (?, ?)`,
							int64(id)*1_000_000_000_000+int64(i)*int64(txStmts+1)+int64(k),
							"x",
						)
					}
					_ = tx.Commit()
					stmtsApplied.Add(int64(txStmts))
				} else {
					_, _ = db.Exec(
						`INSERT INTO bench (id, payload) VALUES (?, ?)`,
						int64(id)*1_000_000_000+int64(i),
						"x",
					)
					stmtsApplied.Add(1)
				}
				lat := time.Since(start)
				ops.Add(1)
				select {
				case latsCh <- lat:
				default:
				}
				i++
			}
		}(w)
	}
	wg.Wait()
	close(latsCh)

	lats := make([]time.Duration, 0, writers*1024)
	for l := range latsCh {
		lats = append(lats, l)
	}
	total := ops.Load()
	stmts := stmtsApplied.Load()
	fmt.Printf("  total: %d ops\n", total)
	fmt.Printf("  rate:  %.0f ops/sec\n", float64(total)/dur.Seconds())
	if txStmts > 0 {
		fmt.Printf("  stmts: %d applied (%.0f stmts/sec)\n", stmts, float64(stmts)/dur.Seconds())
	}
	fmt.Printf("  p50:   %s\n", percentile(lats, 0.50))
	fmt.Printf("  p95:   %s\n", percentile(lats, 0.95))
	fmt.Printf("  p99:   %s\n", percentile(lats, 0.99))
}

// runReadBench fires N concurrent goroutines doing SELECTs against the
// node's local SQLite (ConsistencyNone — no network hop). Useful to
// confirm that a non-voter learner can saturate local reads at the same
// rate as a single SQLite process.
func runReadBench(db *sql.DB, readers int, dur time.Duration) {
	if _, err := db.Exec(`INSERT OR IGNORE INTO bench (id, payload) VALUES (?, ?)`, int64(-1), "seed"); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("read bench: %d concurrent readers for %s (local, ConsistencyNone)\n", readers, dur)
	deadline := time.Now().Add(dur)
	var ops atomic.Int64

	var wg sync.WaitGroup
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				var n int
				_ = db.QueryRow(`SELECT COUNT(*) FROM bench`).Scan(&n)
				ops.Add(1)
			}
		}()
	}
	wg.Wait()
	total := ops.Load()
	fmt.Printf("  total: %d ops\n", total)
	fmt.Printf("  rate:  %.0f ops/sec\n", float64(total)/dur.Seconds())
}

func percentile(lats []time.Duration, p float64) time.Duration {
	if len(lats) == 0 {
		return 0
	}
	// crude sort via insertion (small enough)
	cp := append([]time.Duration(nil), lats...)
	sort := func(a []time.Duration) {
		for i := 1; i < len(a); i++ {
			for j := i; j > 0 && a[j-1] > a[j]; j-- {
				a[j-1], a[j] = a[j], a[j-1]
			}
		}
	}
	if len(cp) > 4096 {
		// downsample to keep the inline sort cheap
		step := len(cp) / 4096
		out := make([]time.Duration, 0, 4096)
		for i := 0; i < len(cp); i += step {
			out = append(out, cp[i])
		}
		cp = out
	}
	sort(cp)
	idx := int(float64(len(cp)) * p)
	if idx >= len(cp) {
		idx = len(cp) - 1
	}
	return cp[idx]
}

// loadOrGenCA loads a CA from caDir, or generates a fresh self-signed
// CA there on first call. In a real deployment you would embed
// (ca.crt, ca.key) in the binary with go:embed and ship one image per
// cluster.
func loadOrGenCA(caDir string) (caPEM, caKeyPEM []byte, err error) {
	crtPath := filepath.Join(caDir, "ca.crt")
	keyPath := filepath.Join(caDir, "ca.key")
	if c, err := os.ReadFile(crtPath); err == nil {
		if k, err := os.ReadFile(keyPath); err == nil {
			return c, k, nil
		}
	}
	if err := os.MkdirAll(caDir, 0o755); err != nil {
		return nil, nil, err
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "colmena-lan-example"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(10 * 365 * 24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, nil, err
	}
	caPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, nil, err
	}
	caKeyPEM = pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(crtPath, caPEM, 0o644); err != nil {
		return nil, nil, err
	}
	if err := os.WriteFile(keyPath, caKeyPEM, 0o600); err != nil {
		return nil, nil, err
	}
	return caPEM, caKeyPEM, nil
}
