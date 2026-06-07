// Package cluster holds the transport-agnostic membership primitives shared
// by the lan (mDNS) and fly (Fly.io 6PN DNS) discovery implementations: the
// Peer model, the Discovery interface, and the pure bootstrap-vs-join election
// helpers. It imports colmena only for the JoinRole enum.
//
// The two transport packages (lan, fly) each keep their own thin Cluster
// lifecycle (the run loop, dead-voter sweep, and non-voter promotion) because
// that wiring is short and order-sensitive; only the pure, well-tested
// decision logic lives here so both transports agree on how a cluster forms.
package cluster

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/mentasystems/colmena"
)

// Peer is a normalized view of a discovered Colmena node, independent of the
// discovery transport (mDNS TXT records on the LAN, Fly internal DNS on Fly).
type Peer struct {
	NodeID        string    // persistent identifier (UUID on LAN, FLY_MACHINE_ID on Fly)
	Advertise     string    // host:port to dial for Raft/RPC ([fdaa::..]:port on Fly)
	Bootstrapping bool      // true while the peer is still in the discovery phase
	Voter         bool      // true if currently a Raft voter (best-effort, eventually consistent)
	LastSeen      time.Time // last sighting; used by impls for TTL aging (not by the pure helpers)
}

// Discovery is a pluggable peer source. Both mDNS (lan) and Fly DNS (fly)
// implement it. Implementations are responsible for announcing self (where
// applicable), aging out stale peers by TTL, and returning best-effort
// snapshots. Snapshots are hints, never the source of truth — the Raft Join
// RPC remains authoritative for cluster configuration.
type Discovery interface {
	// Start begins announcing self (a no-op on Fly, where DNS is the
	// registry) and observing peers. It runs until ctx is cancelled or
	// Close is called.
	Start(ctx context.Context, self Peer) error

	// Peers returns the current peer snapshot, excluding self.
	Peers() []Peer

	// UpdateFlags publishes a change to this node's bootstrapping/voter
	// state. On Fly this is a no-op (DNS carries no such flags).
	UpdateFlags(bootstrapping, voter bool) error

	// Close stops announcing and discovery.
	Close() error
}

// DecideRole inspects discovered peers and decides whether this node should
// bootstrap, join an existing cluster (and as what role), or wait for another
// node to bootstrap first.
//
// Returns:
//   - role:      the JoinRole to use (ignored when bootstrap==true).
//   - joinAddr:  a peer to join via, or "" if we should bootstrap or wait.
//   - bootstrap: true if this node should bootstrap a new cluster itself.
func DecideRole(myID string, peers []Peer, voterQuorum int) (role colmena.JoinRole, joinAddr string, bootstrap bool) {
	addr, role, formed := DecideRoleFromPeers(peers, voterQuorum)
	if formed {
		return role, addr, false
	}
	// No formed cluster. Either bootstrap (if we win the election) or wait
	// for someone else to bootstrap. The lexicographically smallest NodeID
	// among the bootstrapping candidates wins, so every node converges on
	// the same single bootstrapper without coordination.
	candidates := []string{myID}
	for _, p := range peers {
		if p.Bootstrapping {
			candidates = append(candidates, p.NodeID)
		}
	}
	sort.Strings(candidates)
	if candidates[0] == myID {
		return colmena.JoinAsVoter, "", true
	}
	return colmena.JoinAsVoter, "", false
}

// DecideRoleFromPeers scans peers for one that has finished bootstrapping
// (Bootstrapping==false). If found, the cluster is "formed": it returns a join
// address and the role this node should join as (voter while the visible voter
// count is below voterQuorum, otherwise non-voter).
func DecideRoleFromPeers(peers []Peer, voterQuorum int) (joinAddr string, role colmena.JoinRole, formed bool) {
	voters := 0
	for _, p := range peers {
		if p.Bootstrapping {
			continue
		}
		formed = true
		if p.Voter {
			voters++
		}
		if joinAddr == "" {
			joinAddr = p.Advertise
		}
	}
	if !formed {
		return "", colmena.JoinAsVoter, false
	}
	if voters < voterQuorum {
		return joinAddr, colmena.JoinAsVoter, true
	}
	return joinAddr, colmena.JoinAsNonvoter, true
}

// WaitForFormedCluster blocks until at least one peer reports
// Bootstrapping==false, then returns its advertise address.
func WaitForFormedCluster(ctx context.Context, d Discovery, timeout time.Duration) (string, error) {
	deadline := time.After(timeout)
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	for {
		for _, p := range d.Peers() {
			if !p.Bootstrapping {
				return p.Advertise, nil
			}
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-deadline:
			return "", fmt.Errorf("cluster: timed out waiting for elected bootstrapper")
		case <-tick.C:
		}
	}
}

// FormedPeerAddrs returns every formed peer's advertise address with
// `preferred` first, so colmena's join logic can fall through to the next
// candidate if the first one happens to be a follower whose leader redirect
// fails (e.g. during a brief leadership transition).
func FormedPeerAddrs(peers []Peer, preferred string) []string {
	out := []string{}
	if preferred != "" {
		out = append(out, preferred)
	}
	for _, p := range peers {
		if p.Bootstrapping || p.Advertise == "" || p.Advertise == preferred {
			continue
		}
		out = append(out, p.Advertise)
	}
	return out
}

// RoleName returns the human-readable name of a JoinRole.
func RoleName(r colmena.JoinRole) string {
	if r == colmena.JoinAsNonvoter {
		return "non-voter"
	}
	return "voter"
}
