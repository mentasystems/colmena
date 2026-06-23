// Package fly provides zero-config Colmena clustering on Fly.io. It is the 6PN
// counterpart of the lan package: instead of mDNS (which needs multicast, which
// Fly's WireGuard mesh does not have), nodes discover each other through Fly's
// internal DNS and form a Raft cluster automatically, surviving the ephemeral
// machines that every deploy recreates.
//
// A node started via Start auto-elects a bootstrapper on cold start (guarded by
// ExpectedVoters to avoid split-brain), joins an existing cluster otherwise,
// reaps voters that vanish from Fly DNS, auto-promotes a non-voter to restore
// quorum, and exposes a health check the consumer can wire into fly.toml so a
// rolling deploy only advances once a replacement has rejoined and caught up.
//
// Pin all nodes to a single region: Raft is latency-sensitive and cross-region
// quorum is out of scope.
package fly

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mentasystems/colmena"
	"github.com/mentasystems/colmena/cluster"
)

// Cluster is the live state of a node started via Start. It wraps the
// underlying *colmena.Node and owns the Fly DNS discovery plus the background
// loop that reaps dead voters and promotes non-voters. Close stops everything.
type Cluster struct {
	Node *colmena.Node // the underlying Colmena node — use this for SQL

	cfg    Config
	logger *log.Logger
	disc   cluster.Discovery

	stop    chan struct{}
	done    chan struct{}
	closeMu sync.Mutex
	closed  bool
	isVoter atomic.Bool
}

// startPlan is the outcome of the cold-start decision: recover an existing
// member from persisted state, bootstrap a new cluster, or join an existing one
// via joinAddrs as role. recover takes precedence and bypasses the cold-start
// gate entirely (see Start).
type startPlan struct {
	recover   bool
	bootstrap bool
	joinAddrs []string
	role      colmena.JoinRole
}

// Start brings up a Colmena node that auto-clusters on Fly.io. It blocks while
// the bootstrap-vs-join decision is made (bounded by BootstrapTimeout, plus a
// grace period for election losers to find the new leader). Once it returns the
// node is ready: cluster.Node.DB() can be used immediately.
func Start(cfg Config) (*Cluster, error) {
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	logger := log.New(cfg.LogOutput, "fly: ", log.LstdFlags|log.Lmsgprefix)

	self := cluster.Peer{NodeID: cfg.NodeID, Advertise: cfg.advertise()}
	logger.Printf("node %s advertising %s in region %s", self.NodeID, self.Advertise, cfg.Region)

	disc := newDiscovery(cfg, logger)
	ctx, cancel := context.WithCancel(context.Background())
	if err := disc.Start(ctx, self); err != nil {
		cancel()
		return nil, err
	}

	// A node that already holds persisted Raft state is a returning member of a
	// formed cluster, not a fresh one — so skip the cold-start gate entirely and
	// bring Raft up on the persisted configuration. Routing recovery through the
	// gate is exactly the cold-start deadlock this guards against: the gate won't
	// let an election loser bind its Raft listener until a leader exists, but on
	// a full-cluster restart no leader can exist until a quorum of members are
	// listening — circular, so the cluster never recovers. Real Raft members
	// re-electing among themselves is the supported recovery path and is
	// split-brain-safe by Raft's own quorum rule (a majority of the persisted
	// voters must agree). The gate is only needed for the first-ever cold start
	// (no state), where it prevents split-brain while force-installing a
	// single-server configuration.
	var plan startPlan
	if hasState, stErr := colmena.HasExistingState(cfg.DataDir); stErr != nil {
		logger.Printf("warning: could not inspect existing raft state: %v — treating as a fresh node", stErr)
	} else if hasState {
		plan.recover = true
	}

	if !plan.recover {
		p, err := decideStart(ctx, cfg, self, disc, logger)
		if err != nil {
			cancel()
			_ = disc.Close()
			return nil, err
		}
		plan = p
	}

	colmenaCfg := colmena.Config{
		NodeID:       cfg.NodeID,
		DataDir:      cfg.DataDir,
		Bind:         cfg.bind(),
		Advertise:    cfg.advertise(),
		Consistency:  cfg.Consistency,
		TLSConfig:    cfg.TLSConfig,
		BatchWindow:  cfg.BatchWindow,
		BatchMaxSize: cfg.BatchMaxSize,
		OnApply:      cfg.OnApply,
		Backup:       cfg.Backup,
		LogOutput:    cfg.LogOutput,
	}
	switch {
	case plan.recover:
		// Recover brings Raft up on the persisted configuration (no Bootstrap,
		// no Join) and joins the normal election among the members already
		// recorded on disk.
		colmenaCfg.Recover = true
		logger.Printf("existing raft state found — recovering and electing among persisted voters")
	case plan.bootstrap:
		colmenaCfg.Bootstrap = true
		logger.Printf("no formed cluster found — bootstrapping as voter")
	default:
		colmenaCfg.Join = plan.joinAddrs
		colmenaCfg.JoinAs = plan.role
		logger.Printf("joining existing cluster via %v as %s", plan.joinAddrs, cluster.RoleName(plan.role))
	}

	node, err := colmena.New(colmenaCfg)
	if err != nil {
		cancel()
		_ = disc.Close()
		return nil, fmt.Errorf("fly: start colmena: %w", err)
	}

	// On the bootstrapper, wait for Raft to elect us before serving so joiners
	// don't race in during the election window and get "not the leader".
	if plan.bootstrap {
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
	// On recovery the suffrage comes from the persisted configuration, not the
	// plan; assume voter (the common case) and let refreshVoterFlag reconcile a
	// recovering non-voter on the first discovery tick.
	c.isVoter.Store(plan.recover || plan.bootstrap || plan.role == colmena.JoinAsVoter)

	go c.run(ctx, cancel)
	return c, nil
}

// decideStart runs the cold-start gate. Each round it probes visible peers for
// a leader (join if found), otherwise decides whether to bootstrap (gate + win
// the election) or wait for the elected bootstrapper's leader to appear.
func decideStart(ctx context.Context, cfg Config, self cluster.Peer, disc cluster.Discovery, logger *log.Logger) (startPlan, error) {
	start := time.Now()
	bootstrapDeadline := start.Add(cfg.BootstrapTimeout)
	joinDeadline := start.Add(cfg.BootstrapTimeout + 60*time.Second)

	for {
		peers := disc.Peers()

		// 1) Is there an existing cluster? Probe peers for a leader.
		if leader, role, ok := probeForLeader(cfg, peers); ok {
			return startPlan{joinAddrs: cluster.FormedPeerAddrs(peers, leader), role: role}, nil
		}

		// 2) No leader reachable. Bootstrap iff the gate is satisfied and we
		//    win the deterministic election.
		timedOut := time.Now().After(bootstrapDeadline)
		if shouldBootstrap(self, peers, cfg.ExpectedVoters, timedOut) {
			if timedOut && len(peers) < cfg.ExpectedVoters-1 {
				logger.Printf("bootstrap gate timeout: proceeding with %d of %d expected nodes", len(peers)+1, cfg.ExpectedVoters)
			}
			return startPlan{bootstrap: true}, nil
		}

		// 3) We lost the election (or the gate isn't ready yet) and no leader
		//    exists. Wait for the winner to come up, bounded by joinDeadline.
		if time.Now().After(joinDeadline) {
			return startPlan{}, fmt.Errorf("fly: no leader appeared and did not win bootstrap election within %s", cfg.BootstrapTimeout+60*time.Second)
		}
		select {
		case <-ctx.Done():
			return startPlan{}, ctx.Err()
		case <-time.After(pollDelay(cfg)):
		}
	}
}

// shouldBootstrap is the pure cold-start decision used when no leader was found.
// It returns true only if the gate is satisfied — enough peers observed
// (>= ExpectedVoters-1) or the bootstrap timeout elapsed — AND this node wins
// the deterministic election. This is what prevents split-brain: every
// candidate observes (a superset of) the same set before electing, so exactly
// one bootstraps.
func shouldBootstrap(self cluster.Peer, peers []cluster.Peer, expectedVoters int, timedOut bool) bool {
	gateReady := len(peers) >= expectedVoters-1
	if !gateReady && !timedOut {
		return false
	}
	return winsElection(self, peers)
}

// winsElection reports whether self is the election winner — the node with the
// lexicographically smallest advertise address in the set {self} ∪ peers.
//
// The election keys on Advertise ("[ip]:port"), not NodeID, on purpose: in the
// degraded discovery mode (Fly's vms.<app>.internal TXT unavailable) peers are
// keyed by IP while self is keyed by FLY_MACHINE_ID, so a NodeID comparison
// would mix two namespaces and could elect more than one bootstrapper. Every
// node sees the same set of advertise addresses in both healthy and degraded
// modes, so an Advertise-keyed election is always within one namespace and
// agreed by all nodes.
func winsElection(self cluster.Peer, peers []cluster.Peer) bool {
	keys := make([]string, 0, len(peers)+1)
	keys = append(keys, self.Advertise)
	for _, p := range peers {
		keys = append(keys, p.Advertise)
	}
	sort.Strings(keys)
	return keys[0] == self.Advertise
}

// probeForLeader dials each visible peer's RPC status endpoint. If any reports a
// leader (itself or via redirect), it returns the leader's advertise address
// and the role to join as (voter while the cluster has fewer than VoterQuorum
// voters, else non-voter).
func probeForLeader(cfg Config, peers []cluster.Peer) (leaderAddr string, role colmena.JoinRole, ok bool) {
	for _, p := range peers {
		if p.Advertise == "" {
			continue
		}
		st, err := colmena.ProbeStatus(p.Advertise, cfg.TLSConfig, 2*time.Second)
		if err != nil {
			continue
		}
		la := st.LeaderAddr
		if st.IsLeader {
			la = p.Advertise
		}
		if la == "" {
			continue
		}
		role := colmena.JoinAsNonvoter
		if st.Voters < cfg.VoterQuorum {
			role = colmena.JoinAsVoter
		}
		return la, role, true
	}
	return "", 0, false
}

func pollDelay(cfg Config) time.Duration {
	if cfg.DiscoveryInterval < 500*time.Millisecond {
		return cfg.DiscoveryInterval
	}
	return 500 * time.Millisecond
}

// run reaps voters that vanish from Fly DNS and promotes a non-voter to restore
// the target quorum. Only the leader acts.
func (c *Cluster) run(ctx context.Context, cancel context.CancelFunc) {
	defer close(c.done)
	defer cancel()
	defer c.disc.Close()

	tick := time.NewTicker(c.cfg.DiscoveryInterval)
	defer tick.Stop()

	lastSeen := make(map[string]time.Time)
	for {
		select {
		case <-c.stop:
			return
		case <-tick.C:
			c.refreshVoterFlag()
			if c.Node.IsLeader() {
				if c.cfg.DeadVoterTimeout > 0 {
					c.sweepDeadVoters(lastSeen)
				}
				c.promoteIfNeeded()
			}
		}
	}
}

// refreshVoterFlag keeps the in-memory voter flag in sync with the Raft config,
// for logging/health introspection.
func (c *Cluster) refreshVoterFlag() {
	servers, err := c.Node.Nodes()
	if err != nil {
		return
	}
	myID := c.Node.NodeID()
	nowVoter := false
	for _, s := range servers {
		if string(s.ID) == myID && s.Suffrage.String() == "Voter" {
			nowVoter = true
			break
		}
	}
	c.isVoter.Store(nowVoter)
}

// sweepDeadVoters removes voters absent from Fly DNS for longer than
// DeadVoterTimeout. A peer counts as visible if either its NodeID (machine id)
// or its advertise address matches the discovery snapshot — the address match
// keeps the degraded IP-keyed discovery mode (when the vms TXT is unavailable)
// from reaping live members. The "first missing" timestamp is in-memory only;
// a new leader starts its own clock, which can only delay removal, never cause
// a premature one.
func (c *Cluster) sweepDeadVoters(lastSeen map[string]time.Time) {
	servers, err := c.Node.Nodes()
	if err != nil {
		return
	}
	visibleID := make(map[string]bool)
	visibleAddr := make(map[string]bool)
	for _, p := range c.disc.Peers() {
		visibleID[p.NodeID] = true
		if p.Advertise != "" {
			visibleAddr[p.Advertise] = true
		}
	}
	myID := c.Node.NodeID()
	now := time.Now()
	for _, s := range servers {
		id := string(s.ID)
		if id == myID {
			continue
		}
		if visibleID[id] || visibleAddr[string(s.Address)] {
			lastSeen[id] = now
			continue
		}
		first, ok := lastSeen[id]
		if !ok {
			lastSeen[id] = now
			continue
		}
		if now.Sub(first) >= c.cfg.DeadVoterTimeout {
			c.logger.Printf("removing dead peer %s (absent from Fly DNS for %s)", id, now.Sub(first).Round(time.Second))
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

// promoteIfNeeded restores the target voter quorum by promoting a non-voter
// when the cluster has fewer voters than VoterQuorum. Candidates are tried in
// NodeID order (deterministic, so leaders that swap during the same outage
// agree), but each is probed first and a caught-up candidate is preferred: a
// brand-new machine still replaying the log would join the quorum without
// being able to ack appends, stalling commits until it catches up. If no
// candidate reports caught-up (e.g. all run pre-v0.11 and don't send the
// flag), the first reachable one is promoted anyway — a temporarily slow
// quorum beats an indefinitely degraded one.
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

	candidate := ""
	for _, id := range nonvoterIDs {
		st, probeErr := colmena.ProbeStatus(addrByID[id], c.cfg.TLSConfig, 2*time.Second)
		if probeErr != nil {
			c.logger.Printf("promote: candidate %s unreachable: %v", id, probeErr)
			continue
		}
		if st.CaughtUp {
			candidate = id
			break
		}
		if candidate == "" {
			candidate = id // reachable fallback if nobody reports caught-up
		}
		c.logger.Printf("promote: candidate %s reachable but not caught up", id)
	}
	if candidate == "" {
		c.logger.Printf("promote: no reachable non-voter candidate (%d/%d voters)", voters, c.cfg.VoterQuorum)
		return
	}
	c.logger.Printf("promoting non-voter %s to voter (%d/%d voters before promote)", candidate, voters, c.cfg.VoterQuorum)
	if err := c.Node.AddVoter(candidate, addrByID[candidate]); err != nil {
		c.logger.Printf("promote: %v", err)
	}
}

// Healthy reports whether this node is part of the Raft configuration and, if a
// follower, has recently heard from the leader and applied everything the
// leader has committed to it. Fly's health check should gate a rolling deploy on
// this so a new machine only reports healthy once it has truly rejoined and
// caught up — keeping quorum intact during the deploy.
func (c *Cluster) Healthy() bool {
	servers, err := c.Node.Nodes()
	if err != nil {
		return false
	}
	me := c.Node.NodeID()
	in := false
	for _, s := range servers {
		if string(s.ID) == me {
			in = true
			break
		}
	}
	if !in {
		return false
	}
	if c.Node.IsLeader() {
		return true
	}
	return caughtUp(c.Node.Stats())
}

// caughtUp reports whether a follower has fresh leader contact and has applied
// everything committed to it. Uses only confirmed Raft Stats keys.
func caughtUp(stats map[string]string) bool {
	lc := stats["last_contact"]
	if lc == "" || lc == "never" {
		return false
	}
	if lc != "0" {
		if d, err := time.ParseDuration(lc); err != nil || d > 2*time.Second {
			return false
		}
	}
	applied, e1 := strconv.ParseUint(stats["applied_index"], 10, 64)
	commit, e2 := strconv.ParseUint(stats["commit_index"], 10, 64)
	if e1 != nil || e2 != nil {
		return false
	}
	return applied >= commit
}

// GracefulLeave hands off leadership (if leader) and best-effort removes this
// node from the configuration before shutting down, so a rolling deploy doesn't
// have to wait for the dead-voter sweep. Wire it into the consumer's SIGTERM
// handler with a timeout shorter than Fly's shutdown grace period. Always calls
// Close, so the node is fully shut down on return.
func (c *Cluster) GracefulLeave(ctx context.Context) error {
	myID := c.Node.NodeID()

	if c.Node.IsLeader() {
		c.logger.Printf("transferring leadership before leave")
		if err := c.Node.TransferLeadership(); err != nil {
			c.logger.Printf("leadership transfer: %v (continuing)", err)
		}
		// Wait until we have actually stepped down, bounded by ctx/timeout.
		waitForStepDown(ctx, c.Node, 3*time.Second)
	}

	// Best-effort self-removal. RemoveServer only works on the leader: if we
	// successfully handed off we're no longer leader and skip it, leaving the
	// new leader's sweep to reap us once we vanish from DNS. If the transfer
	// failed we may still be leader — try anyway, UNLESS we are the only
	// voter: removing the last voter leaves an empty configuration that can
	// never elect a leader again, bricking the data dir as a cluster member
	// (the single-machine Fly rolling-deploy case).
	if c.Node.IsLeader() {
		if c.voterCount() <= 1 {
			c.logger.Printf("sole voter: skipping self-removal so the cluster can restart")
		} else if err := c.Node.RemoveNode(myID); err != nil {
			c.logger.Printf("remove self: %v (sweeper will reap)", err)
		}
	}

	return c.Close()
}

// voterCount returns the number of voters in this node's view of the Raft
// configuration. Returns 1 on error: self-removal is the destructive act, so
// when the configuration can't be read the guard errs toward skipping it
// (peers just wait out the dead-voter sweep) rather than risking the removal
// of the last voter.
func (c *Cluster) voterCount() int {
	servers, err := c.Node.Nodes()
	if err != nil {
		return 1
	}
	voters := 0
	for _, s := range servers {
		if s.Suffrage.String() == "Voter" {
			voters++
		}
	}
	return voters
}

// waitForStepDown blocks until this node is no longer the Raft leader, or the
// timeout/context elapses.
func waitForStepDown(ctx context.Context, node *colmena.Node, timeout time.Duration) {
	deadline := time.After(timeout)
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for node.IsLeader() {
		select {
		case <-ctx.Done():
			return
		case <-deadline:
			return
		case <-tick.C:
		}
	}
}

// Close stops discovery and the background loop, then shuts down the underlying
// Colmena node. Safe to call multiple times.
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
