package lan

import (
	"context"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/grandcat/zeroconf"
	"github.com/mentasystems/colmena/cluster"
)

// peer aliases cluster.Peer so the lan package and the shared election helpers
// agree on a single model. Field names match 1:1 (NodeID, Advertise,
// Bootstrapping, Voter); cluster.Peer's LastSeen field is unused on the LAN —
// peerRecord.lastSeen tracks mDNS sightings instead.
type peer = cluster.Peer

// peerRecord wraps a peer with the timestamp of its last mDNS sighting.
// Used to age entries out of the discovery cache, so a node that
// disappears (crashed, partitioned, reflashed) eventually stops showing
// up in snapshot().
type peerRecord struct {
	peer
	lastSeen time.Time
}

// peerStaleness is how long since the last mDNS sighting we keep a peer
// in snapshots. The browse loop refreshes every ~3s, so 15s tolerates
// 4–5 missed cycles before treating the peer as gone.
const peerStaleness = 15 * time.Second

const (
	mdnsServiceFmt = "_colmena-%s._tcp" // %s is the short cluster ID
	mdnsDomain     = "local."
)

// discovery announces this node over mDNS and continuously browses for
// peers in the same cluster. Peer state is racy by design — the
// caller is expected to use snapshots as a hint, not as a source of
// truth, and to fall back to the existing Colmena Join RPC for the
// authoritative cluster configuration.
type discovery struct {
	cfg         *Config
	serviceType string
	logger      *log.Logger

	// announce state
	announceMu sync.Mutex
	server     *zeroconf.Server
	nodeID     string
	advertise  string
	port       int
	host       string
	bootFlag   bool
	voterFlag  bool

	// peer cache
	mu    sync.Mutex
	peers map[string]peerRecord
}

func newDiscovery(cfg *Config, logger *log.Logger) *discovery {
	svc := cfg.ServiceName
	if svc == "" {
		svc = fmt.Sprintf(mdnsServiceFmt, clusterID(cfg.CACert))
	}
	return &discovery{
		cfg:         cfg,
		serviceType: svc,
		logger:      logger,
		peers:       make(map[string]peerRecord),
	}
}

// start begins announcing this node and browsing for peers. Browsing
// continues in a background goroutine until ctx is cancelled or close()
// is called.
func (d *discovery) start(ctx context.Context, nodeID, advertise string, bootstrapping, voter bool) error {
	host, portStr, err := net.SplitHostPort(advertise)
	if err != nil {
		return fmt.Errorf("lan: split advertise %q: %w", advertise, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return fmt.Errorf("lan: parse advertise port: %w", err)
	}

	d.announceMu.Lock()
	d.nodeID = nodeID
	d.advertise = advertise
	d.host = host
	d.port = port
	d.bootFlag = bootstrapping
	d.voterFlag = voter
	d.announceMu.Unlock()

	if err := d.republish(); err != nil {
		return err
	}

	go d.browseLoop(ctx)
	return nil
}

// republish (re)announces the service with the current TXT records.
// zeroconf doesn't expose live TXT updates, so we tear down and re-register
// — cheap, and the multicast traffic is bounded by the cluster size.
func (d *discovery) republish() error {
	d.announceMu.Lock()
	defer d.announceMu.Unlock()

	if d.server != nil {
		d.server.Shutdown()
		d.server = nil
	}

	txt := []string{
		"node=" + d.nodeID,
		"advertise=" + d.advertise,
		"cluster=" + clusterID(d.cfg.CACert),
		"bootstrapping=" + strconv.FormatBool(d.bootFlag),
		"voter=" + strconv.FormatBool(d.voterFlag),
	}

	srv, err := zeroconf.Register(d.nodeID, d.serviceType, mdnsDomain, d.port, txt, nil)
	if err != nil {
		return fmt.Errorf("lan: mdns register: %w", err)
	}
	d.server = srv
	return nil
}

// updateFlags refreshes the bootstrapping/voter TXT fields. Cheap to
// call; if the values haven't changed it's a no-op.
func (d *discovery) updateFlags(bootstrapping, voter bool) error {
	d.announceMu.Lock()
	if d.bootFlag == bootstrapping && d.voterFlag == voter {
		d.announceMu.Unlock()
		return nil
	}
	d.bootFlag = bootstrapping
	d.voterFlag = voter
	d.announceMu.Unlock()
	return d.republish()
}

// browseLoop continuously runs zeroconf.Browse to keep the peer cache
// fresh. zeroconf returns each entry once per browse run, so we re-run
// every ~3s with a 2s collect window.
func (d *discovery) browseLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		resolver, err := zeroconf.NewResolver(nil)
		if err != nil {
			d.logger.Printf("lan: mdns resolver: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}

		entries := make(chan *zeroconf.ServiceEntry, 16)
		browseCtx, cancel := context.WithTimeout(ctx, 2*time.Second)

		go d.collect(entries)
		if err := resolver.Browse(browseCtx, d.serviceType, mdnsDomain, entries); err != nil {
			d.logger.Printf("lan: mdns browse: %v", err)
		}
		<-browseCtx.Done()
		cancel()
		// give collect() a moment to drain
		time.Sleep(50 * time.Millisecond)

		select {
		case <-ctx.Done():
			return
		case <-time.After(1 * time.Second):
		}
	}
}

func (d *discovery) collect(entries <-chan *zeroconf.ServiceEntry) {
	d.announceMu.Lock()
	selfID := d.nodeID
	d.announceMu.Unlock()
	for e := range entries {
		p, ok := parseEntry(e)
		if !ok {
			continue
		}
		// Filter by cluster ID — defensive, in case the mDNS service
		// type ever collides between clusters (shouldn't, but TXT is
		// the source of truth).
		expected := clusterID(d.cfg.CACert)
		if pCluster := txtValue(e.Text, "cluster"); pCluster != expected {
			continue
		}
		if p.NodeID == selfID {
			continue // skip our own announcement
		}
		d.mu.Lock()
		d.peers[p.NodeID] = peerRecord{peer: p, lastSeen: time.Now()}
		d.mu.Unlock()
	}
}

func parseEntry(e *zeroconf.ServiceEntry) (peer, bool) {
	p := peer{
		NodeID:        txtValue(e.Text, "node"),
		Advertise:     txtValue(e.Text, "advertise"),
		Bootstrapping: txtBool(e.Text, "bootstrapping", true),
		Voter:         txtBool(e.Text, "voter", false),
	}
	if p.NodeID == "" || p.Advertise == "" {
		return peer{}, false
	}
	return p, true
}

func txtValue(records []string, key string) string {
	prefix := key + "="
	for _, r := range records {
		if v, ok := strings.CutPrefix(r, prefix); ok {
			return v
		}
	}
	return ""
}

func txtBool(records []string, key string, def bool) bool {
	v := txtValue(records, key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return b
}

// snapshot returns a copy of peers seen via mDNS within peerStaleness.
// Stale entries (peers we haven't heard from in too many browse cycles)
// are dropped from the cache here so the sweeper can correctly detect
// nodes that have actually gone away.
func (d *discovery) snapshot() []peer {
	d.mu.Lock()
	defer d.mu.Unlock()
	cutoff := time.Now().Add(-peerStaleness)
	out := make([]peer, 0, len(d.peers))
	for id, r := range d.peers {
		if r.lastSeen.Before(cutoff) {
			delete(d.peers, id)
			continue
		}
		out = append(out, r.peer)
	}
	return out
}

// close stops the announcement and unblocks browseLoop on next iteration.
func (d *discovery) close() {
	d.announceMu.Lock()
	defer d.announceMu.Unlock()
	if d.server != nil {
		d.server.Shutdown()
		d.server = nil
	}
}
