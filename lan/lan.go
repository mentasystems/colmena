package lan

import (
	"context"
	"fmt"
	"log"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mentasystems/colmena"
	"github.com/mentasystems/colmena/cluster"
)

// Cluster is the live state of a node started via Start. It wraps the
// underlying *colmena.Node and owns the mDNS announcer plus the
// background goroutines that drive bootstrap election and dead-voter
// sweeping. Close() stops everything in the right order.
type Cluster struct {
	Node *colmena.Node // the underlying Colmena node — use this for SQL

	cfg    Config
	logger *log.Logger

	disc *discovery

	stop     chan struct{}
	done     chan struct{}
	closeMu  sync.Mutex
	closed   bool
	isVoter  atomic.Bool
}

// Start brings up a Colmena node with LAN-based zero-config clustering.
// On first boot it generates a persistent NodeID and a per-node TLS
// cert signed by the embedded CA, then announces itself over mDNS,
// listens for peers, and either bootstraps the cluster (if alone) or
// joins an existing leader.
//
// Start blocks for up to cfg.DiscoveryWindow + a few seconds while the
// initial bootstrap/join decision is made. Once it returns, the node is
// ready: cluster.Node.DB() can be used immediately.
func Start(cfg Config) (*Cluster, error) {
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	logger := log.New(cfg.LogOutput, "lan: ", log.LstdFlags|log.Lmsgprefix)

	nodeID, err := loadOrCreateNodeID(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	logger.Printf("node id: %s", nodeID)

	advertise := cfg.Advertise
	if advertise == "" {
		_, port, err := net.SplitHostPort(cfg.Bind)
		if err != nil {
			return nil, fmt.Errorf("lan: split bind: %w", err)
		}
		advertise = net.JoinHostPort(firstNonLoopbackIPv4(), port)
	}
	logger.Printf("advertise: %s", advertise)

	colmenaCfg := colmena.Config{
		NodeID:       nodeID,
		DataDir:      cfg.DataDir,
		Bind:         cfg.Bind,
		Advertise:    advertise,
		Consistency:  cfg.Consistency,
		BatchWindow:  cfg.BatchWindow,
		BatchMaxSize: cfg.BatchMaxSize,
		OnApply:      cfg.OnApply,
		Backup:       cfg.Backup,
		LogOutput:    cfg.LogOutput,
	}

	if len(cfg.CACert) > 0 {
		cert, err := loadOrIssueCert(cfg.DataDir, cfg.CACert, cfg.CAKey, nodeID)
		if err != nil {
			return nil, err
		}
		tc, err := buildTLSConfig(cert, cfg.CACert)
		if err != nil {
			return nil, err
		}
		colmenaCfg.TLSConfig = tc
	}

	disc := newDiscovery(&cfg, logger)

	// Start announcing ourselves as bootstrapping=true so other nodes
	// know we haven't decided yet. We update this flag once we either
	// bootstrap or successfully join.
	ctx, cancel := context.WithCancel(context.Background())
	if err := disc.start(ctx, nodeID, advertise, true, false); err != nil {
		cancel()
		return nil, err
	}

	logger.Printf("listening for peers for %s", cfg.DiscoveryWindow)
	time.Sleep(cfg.DiscoveryWindow)

	peers := disc.snapshot()
	logger.Printf("discovered %d peer(s) in this cluster", len(peers))

	role, joinAddr, doBootstrap := decideRole(nodeID, peers, cfg.VoterQuorum)

	switch {
	case doBootstrap:
		logger.Printf("no formed cluster found — bootstrapping as voter")
		colmenaCfg.Bootstrap = true
	case joinAddr != "":
		colmenaCfg.Join = formedPeerAddrs(peers, joinAddr)
		colmenaCfg.JoinAs = role
		logger.Printf("joining existing cluster via %v as %s", colmenaCfg.Join, roleName(role))
	default:
		// All visible peers are still bootstrapping; we lost the
		// election. Wait for the winner to publish bootstrapping=false.
		logger.Printf("waiting for elected bootstrapper to come online")
		joinAddr, err = waitForFormedCluster(ctx, disc, 60*time.Second)
		if err != nil {
			cancel()
			disc.close()
			return nil, err
		}
		_, role, _ = decideRoleFromPeers(disc.snapshot(), cfg.VoterQuorum)
		colmenaCfg.Join = formedPeerAddrs(disc.snapshot(), joinAddr)
		colmenaCfg.JoinAs = role
		logger.Printf("joining via %v as %s", colmenaCfg.Join, roleName(role))
	}

	node, err := colmena.New(colmenaCfg)
	if err != nil {
		cancel()
		disc.close()
		return nil, fmt.Errorf("lan: start colmena: %w", err)
	}

	// On the bootstrapper, wait for Raft to actually elect us before
	// flipping bootstrapping=false in mDNS. Otherwise joiners can race
	// in during the heartbeat-timeout window and get "not the leader"
	// from a node that hasn't promoted itself yet.
	if doBootstrap {
		if err := node.WaitForLeader(15 * time.Second); err != nil {
			logger.Printf("warning: leader election did not complete in time: %v", err)
		}
	}

	c := &Cluster{
		Node:   node,
		cfg:    cfg,
		logger: logger,
		disc:   disc,
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
	c.isVoter.Store(role == colmena.JoinAsVoter || doBootstrap)

	// Update mDNS to reflect our final state.
	_ = disc.updateFlags(false, c.isVoter.Load())

	go c.run(ctx, cancel)
	return c, nil
}

// run keeps mDNS announcements in sync with the live Raft configuration
// and (on the leader) sweeps voters that have been unreachable for too
// long.
func (c *Cluster) run(ctx context.Context, cancel context.CancelFunc) {
	defer close(c.done)
	defer cancel()
	defer c.disc.close()

	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()

	lastSeen := make(map[string]time.Time)

	for {
		select {
		case <-c.stop:
			return
		case <-tick.C:
			c.refreshAnnouncement()
			if c.Node.IsLeader() {
				if c.cfg.DeadVoterTimeout > 0 {
					c.sweepDeadVoters(lastSeen)
				}
				c.promoteIfNeeded()
			}
		}
	}
}

// refreshAnnouncement re-publishes our mDNS TXT records when our voter
// status changes (e.g., we were promoted from non-voter to voter, or a
// voter slot opened up and we should advertise availability).
func (c *Cluster) refreshAnnouncement() {
	servers, err := c.Node.Nodes()
	if err != nil {
		return
	}
	myID := c.Node.NodeID()
	wasVoter := c.isVoter.Load()
	nowVoter := false
	for _, s := range servers {
		if string(s.ID) == myID && s.Suffrage.String() == "Voter" {
			nowVoter = true
			break
		}
	}
	if wasVoter != nowVoter {
		c.isVoter.Store(nowVoter)
		_ = c.disc.updateFlags(false, nowVoter)
	}
}

// sweepDeadVoters removes voters that have been continuously absent
// from mDNS for longer than DeadVoterTimeout. The "last seen" timestamp
// is kept in memory only — on a leader change the new leader starts its
// own clock, which is the conservative choice (it can never remove a
// voter prematurely).
func (c *Cluster) sweepDeadVoters(lastSeen map[string]time.Time) {
	servers, err := c.Node.Nodes()
	if err != nil {
		return
	}
	visible := make(map[string]bool)
	for _, p := range c.disc.snapshot() {
		visible[p.NodeID] = true
	}
	myID := c.Node.NodeID()
	now := time.Now()
	for _, s := range servers {
		id := string(s.ID)
		if id == myID {
			continue
		}
		if visible[id] {
			lastSeen[id] = now
			continue
		}
		first, ok := lastSeen[id]
		if !ok {
			lastSeen[id] = now
			continue
		}
		if now.Sub(first) >= c.cfg.DeadVoterTimeout {
			c.logger.Printf("removing dead peer %s (unreachable for %s)", id, now.Sub(first).Round(time.Second))
			if err := c.Node.RemoveNode(id); err != nil {
				c.logger.Printf("remove dead peer %s: %v", id, err)
			} else {
				delete(lastSeen, id)
			}
		}
	}
	// drop entries for peers no longer in the configuration
	configured := make(map[string]bool, len(servers))
	for _, s := range servers {
		configured[string(s.ID)] = true
	}
	for id := range lastSeen {
		if !configured[id] {
			delete(lastSeen, id)
		}
	}
}

// promoteIfNeeded restores the target voter quorum by promoting an
// existing non-voter to voter when the cluster has fewer voters than
// VoterQuorum. The non-voter with the lexicographically smallest
// NodeID is chosen so leaders that swap during the same outage agree
// on the same candidate (no race, no pongs back and forth).
//
// Promotion only happens AFTER sweepDeadVoters has actually removed
// the dead member from the configuration — that's when the voter slot
// becomes available. Until then, voter count == quorum and this
// function is a no-op.
func (c *Cluster) promoteIfNeeded() {
	servers, err := c.Node.Nodes()
	if err != nil {
		return
	}
	voters := 0
	var nonvoterIDs []string
	addrByID := make(map[string]string, len(servers))
	for _, s := range servers {
		addrByID[string(s.ID)] = string(s.Address)
		if s.Suffrage.String() == "Voter" {
			voters++
		} else {
			nonvoterIDs = append(nonvoterIDs, string(s.ID))
		}
	}
	if voters >= c.cfg.VoterQuorum || len(nonvoterIDs) == 0 {
		return
	}
	sort.Strings(nonvoterIDs)
	candidate := nonvoterIDs[0]
	c.logger.Printf("promoting non-voter %s to voter (%d/%d voters before promote)", candidate, voters, c.cfg.VoterQuorum)
	if err := c.Node.AddVoter(candidate, addrByID[candidate]); err != nil {
		c.logger.Printf("promote: %v", err)
	}
}

// Close gracefully shuts down the cluster: stops mDNS, stops the
// background loop, and shuts down the underlying Colmena node.
func (c *Cluster) Close() error {
	c.closeMu.Lock()
	if c.closed {
		c.closeMu.Unlock()
		return nil
	}
	c.closed = true
	c.closeMu.Unlock()

	close(c.stop)
	<-c.done
	if c.Node != nil {
		return c.Node.Close()
	}
	return nil
}

// decideRole / decideRoleFromPeers / formedPeerAddrs / roleName are thin
// wrappers over the shared cluster package so the lan and fly transports run
// the exact same bootstrap-vs-join election logic. The lowercase names are
// kept so the existing lan tests (bootstrap_test.go) compile unchanged.
func decideRole(myID string, peers []peer, voterQuorum int) (colmena.JoinRole, string, bool) {
	return cluster.DecideRole(myID, peers, voterQuorum)
}

func decideRoleFromPeers(peers []peer, voterQuorum int) (joinAddr string, role colmena.JoinRole, formed bool) {
	return cluster.DecideRoleFromPeers(peers, voterQuorum)
}

// waitForFormedCluster blocks until at least one peer advertises
// bootstrapping=false, then returns its address.
func waitForFormedCluster(ctx context.Context, d *discovery, timeout time.Duration) (string, error) {
	deadline := time.After(timeout)
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	for {
		for _, p := range d.snapshot() {
			if !p.Bootstrapping {
				return p.Advertise, nil
			}
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-deadline:
			return "", fmt.Errorf("lan: timed out waiting for elected bootstrapper")
		case <-tick.C:
		}
	}
}

func formedPeerAddrs(peers []peer, preferred string) []string {
	return cluster.FormedPeerAddrs(peers, preferred)
}

func roleName(r colmena.JoinRole) string {
	return cluster.RoleName(r)
}
