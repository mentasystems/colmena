package fly

import (
	"context"
	"log"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mentasystems/colmena/cluster"
)

// discovery implements cluster.Discovery over Fly.io's internal 6PN DNS.
//
// Fly has no multicast, so mDNS is dead; instead the topology is published via
// internal DNS. We poll:
//   - vms.<app>.internal      (TXT: "<machine_id> <region>,…") for the stable
//     machine-id set in our region, and
//   - <id>.vm.<app>.internal  (AAAA) to map each machine_id to its 6PN IP.
//
// Keying peers by FLY_MACHINE_ID (== the Raft NodeID each node joins with)
// keeps the leader-side sweep correct: a Raft server id always matches the
// discovery NodeID. If the vms TXT is unavailable we fall back to the region
// AAAA keyed by IP (degraded: ids are not stable, but liveness still works).
//
// The Peer.Bootstrapping / Peer.Voter flags are NOT populated here — unlike
// mDNS, Fly DNS carries no such signal. The fly bootstrap path detects a
// formed cluster via colmena.ProbeStatus instead, and the lifecycle reads
// voter state from the authoritative Raft configuration.
type discovery struct {
	cfg    Config
	logger *log.Logger
	res    resolver
	self   cluster.Peer
	now    func() time.Time

	mu    sync.Mutex
	peers map[string]peerRecord // keyed by NodeID (machine_id, or IP when degraded)

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

type peerRecord struct {
	cluster.Peer
	lastSeen time.Time
}

func newDiscovery(cfg Config, logger *log.Logger) *discovery {
	res := cfg.Resolver
	if res == nil {
		res = net.DefaultResolver
	}
	return &discovery{
		cfg:    cfg,
		logger: logger,
		res:    res,
		now:    time.Now,
		peers:  make(map[string]peerRecord),
		done:   make(chan struct{}),
	}
}

func (d *discovery) Start(ctx context.Context, self cluster.Peer) error {
	d.self = self
	d.ctx, d.cancel = context.WithCancel(ctx)
	d.refresh() // prime the cache so callers see peers immediately after Start
	go d.pollLoop()
	return nil
}

func (d *discovery) pollLoop() {
	defer close(d.done)
	t := time.NewTicker(d.cfg.DiscoveryInterval)
	defer t.Stop()
	for {
		select {
		case <-d.ctx.Done():
			return
		case <-t.C:
			d.refresh()
		}
	}
}

func (d *discovery) refresh() {
	ctx, cancel := context.WithTimeout(d.ctx, 5*time.Second)
	defer cancel()

	found := d.resolvePeers(ctx)
	now := d.now()

	d.mu.Lock()
	defer d.mu.Unlock()
	seen := make(map[string]bool, len(found))
	for _, p := range found {
		seen[p.NodeID] = true
		p.LastSeen = now
		d.peers[p.NodeID] = peerRecord{Peer: p, lastSeen: now}
	}
	cutoff := now.Add(-d.cfg.PeerTTL)
	for id, rec := range d.peers {
		if !seen[id] && rec.lastSeen.Before(cutoff) {
			delete(d.peers, id)
		}
	}
}

// resolvePeers returns the current peer set, preferring the stable machine-id
// path (vms TXT + per-machine AAAA) and falling back to the region AAAA.
func (d *discovery) resolvePeers(ctx context.Context) []cluster.Peer {
	ids := d.machineIDsInRegion(ctx)
	if len(ids) > 0 {
		out := make([]cluster.Peer, 0, len(ids))
		for _, id := range ids {
			ip := d.lookupMachineIP(ctx, id)
			if ip == "" {
				continue
			}
			out = append(out, cluster.Peer{
				NodeID:    id,
				Advertise: net.JoinHostPort(ip, strconv.Itoa(d.cfg.RaftPort)),
			})
		}
		if len(out) > 0 {
			return out
		}
	}
	return d.resolveByRegionAAAA(ctx)
}

// machineIDsInRegion parses vms.<app>.internal TXT ("<id> <region>,…") and
// returns the machine_ids whose region == ours, excluding self. Sorted for a
// deterministic order. Best-effort: returns nil on error.
func (d *discovery) machineIDsInRegion(ctx context.Context) []string {
	txts, err := d.res.LookupTXT(ctx, d.cfg.vmsDomain())
	if err != nil {
		d.logger.Printf("fly: lookup TXT %s: %v", d.cfg.vmsDomain(), err)
		return nil
	}
	var ids []string
	for _, t := range txts {
		for _, entry := range strings.Split(t, ",") {
			f := strings.Fields(strings.TrimSpace(entry))
			if len(f) == 2 && f[1] == d.cfg.Region && f[0] != d.self.NodeID {
				ids = append(ids, f[0])
			}
		}
	}
	sort.Strings(ids)
	return ids
}

// lookupMachineIP resolves a single machine's 6PN IP via <id>.vm.<app>.internal.
func (d *discovery) lookupMachineIP(ctx context.Context, machineID string) string {
	addrs, err := d.res.LookupNetIP(ctx, "ip6", d.cfg.machineDomain(machineID))
	if err != nil || len(addrs) == 0 {
		return ""
	}
	return addrs[0].String()
}

// resolveByRegionAAAA is the degraded fallback: peers keyed by IP, no stable id.
func (d *discovery) resolveByRegionAAAA(ctx context.Context) []cluster.Peer {
	addrs, err := d.res.LookupNetIP(ctx, "ip6", d.cfg.regionDomain())
	if err != nil {
		d.logger.Printf("fly: lookup AAAA %s: %v", d.cfg.regionDomain(), err)
		return nil
	}
	selfIP := d.cfg.PrivateIP
	out := make([]cluster.Peer, 0, len(addrs))
	for _, a := range addrs {
		ip := a.String()
		if ip == selfIP {
			continue
		}
		out = append(out, cluster.Peer{
			NodeID:    ip,
			Advertise: net.JoinHostPort(ip, strconv.Itoa(d.cfg.RaftPort)),
		})
	}
	return out
}

func (d *discovery) Peers() []cluster.Peer {
	d.mu.Lock()
	defer d.mu.Unlock()
	cutoff := d.now().Add(-d.cfg.PeerTTL)
	out := make([]cluster.Peer, 0, len(d.peers))
	for id, rec := range d.peers {
		if rec.lastSeen.Before(cutoff) {
			delete(d.peers, id)
			continue
		}
		out = append(out, rec.Peer)
	}
	return out
}

// UpdateFlags is a no-op on Fly: the internal DNS is the registry and carries
// no per-node bootstrapping/voter flags.
func (d *discovery) UpdateFlags(bootstrapping, voter bool) error { return nil }

func (d *discovery) Close() error {
	if d.cancel != nil {
		d.cancel()
		<-d.done
	}
	return nil
}
