package colmena

import (
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"
	"time"
)

// rpcPool manages RPC client connections to other nodes with automatic
// reconnection, health checks, and idle eviction.
type rpcPool struct {
	mu        sync.Mutex
	clients   map[string]*rpcEntry
	tlsConfig *tls.Config
	maxIdle   time.Duration

	// localNodeID is sent in the Hello handshake so the peer can log which
	// node is connecting. Zero value is fine (the handshake is log-only).
	localNodeID string

	// onHello, if set, receives every successful Hello response so the version
	// negotiator learns a peer's advertised formats the moment this node dials
	// it — not only via the leader's probe loop. Set by Node after construction.
	onHello func(resp *RPCHelloResponse)
}

type rpcEntry struct {
	client   *rpc.Client
	lastUsed time.Time

	// peerVersion is populated by the Hello handshake after dial. Used for
	// metrics/introspection; not consulted on the hot path.
	peerVersion string
}

func newRPCPool(tlsConfig *tls.Config, localNodeID string) *rpcPool {
	return &rpcPool{
		clients:     make(map[string]*rpcEntry),
		tlsConfig:   tlsConfig,
		maxIdle:     5 * time.Minute,
		localNodeID: localNodeID,
	}
}

// get returns a healthy RPC client for the given Raft address.
// It reconnects if the cached client has failed or is stale.
func (p *rpcPool) get(raftAddr string) (*rpc.Client, error) {
	rpcAddr, err := rpcAddrFrom(raftAddr)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Opportunistically evict idle entries for *other* addresses. Without
	// this, a connection to an address we never dial again (e.g. a deposed
	// leader, or a recycled machine IP on Fly) would keep its fd and reader
	// goroutine alive for the life of the node.
	now := time.Now()
	for addr, e := range p.clients {
		if addr != rpcAddr && now.Sub(e.lastUsed) >= p.maxIdle {
			_ = e.client.Close() // safe-ignore: evicting an idle client; nothing to do on error
			delete(p.clients, addr)
		}
	}

	if entry, ok := p.clients[rpcAddr]; ok {
		if now.Sub(entry.lastUsed) < p.maxIdle {
			entry.lastUsed = now
			return entry.client, nil
		}
		// Stale connection — close and reconnect.
		entry.client.Close()
		delete(p.clients, rpcAddr)
	}

	client, err := p.dial(rpcAddr)
	if err != nil {
		return nil, err
	}

	entry := &rpcEntry{
		client:   client,
		lastUsed: time.Now(),
	}
	// Best-effort version handshake. Failures are logged but don't tear down
	// the connection — the peer might be an older Colmena that doesn't know
	// the Hello method, and we still want its Execute/Query/Join calls to work.
	p.sayHello(rpcAddr, entry)
	p.clients[rpcAddr] = entry
	return client, nil
}

// sayHello runs the version handshake after a successful dial. Updates the
// entry's peerVersion on success; logs a single warning on failure.
func (p *rpcPool) sayHello(rpcAddr string, entry *rpcEntry) {
	req := &RPCHelloRequest{
		NodeID:                p.localNodeID,
		LibraryVersion:        LibraryVersion,
		ProtocolVersion:       ProtocolVersion,
		CommandFormatVersion:  CommandFormatVersion,
		SnapshotFormatVersion: SnapshotFormatVersion,
	}
	var resp RPCHelloResponse
	if err := entry.client.Call("Colmena.Hello", req, &resp); err != nil {
		// Older peer doesn't know Colmena.Hello. Log once, keep the connection.
		log.Printf("colmena: handshake with %s failed (likely older peer): %v", rpcAddr, err)
		return
	}
	entry.peerVersion = resp.LibraryVersion
	if p.onHello != nil {
		p.onHello(&resp)
	}
	if resp.ProtocolVersion != ProtocolVersion {
		log.Printf("colmena: peer %s runs protocol v%d, local v%d — expect issues",
			rpcAddr, resp.ProtocolVersion, ProtocolVersion)
	}
}

// markFailed closes and evicts the cached connection for this address so the
// next get() dials fresh. Closing eagerly matters: after a leader change the
// failed address may never be get() again, and a merely-flagged entry would
// leak its fd and reader goroutine for the life of the node.
func (p *rpcPool) markFailed(raftAddr string) {
	rpcAddr, err := rpcAddrFrom(raftAddr)
	if err != nil {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if entry, ok := p.clients[rpcAddr]; ok {
		_ = entry.client.Close() // safe-ignore: evicting a failed client; nothing to do on error
		delete(p.clients, rpcAddr)
	}
}

func (p *rpcPool) dial(addr string) (*rpc.Client, error) {
	if p.tlsConfig != nil {
		conn, err := tls.DialWithDialer(
			&net.Dialer{Timeout: 5 * time.Second},
			"tcp", addr, p.tlsConfig,
		)
		if err != nil {
			return nil, fmt.Errorf("colmena: TLS dial RPC %s: %w", addr, err)
		}
		return rpc.NewClient(conn), nil
	}
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("colmena: dial RPC %s: %w", addr, err)
	}
	return rpc.NewClient(conn), nil
}

// close shuts down all cached connections.
func (p *rpcPool) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for addr, entry := range p.clients {
		entry.client.Close()
		delete(p.clients, addr)
	}
}

// RPCPing is a lightweight health check method. Returns nil on success.
type RPCPingRequest struct{}
type RPCPingResponse struct{}

func (s *RPCService) Ping(req *RPCPingRequest, resp *RPCPingResponse) error {
	return nil
}
