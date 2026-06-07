package colmena

import (
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// ErrNotLeader is returned when a write is attempted on a non-leader node
// and the leader address is unknown for forwarding.
var ErrNotLeader = errors.New("colmena: not the leader")

// Node is a single member of a Colmena distributed SQLite cluster.
type Node struct {
	config Config
	stores *storeManager
	raft   *raft.Raft
	fsm    *fsm

	rpcServer   *rpc.Server
	rpcListener net.Listener
	rpcPool     *rpcPool

	batcher  *WriteBatcher
	lease    *readLease
	leaseStop chan struct{}
	metrics  metricsCounters
	backup   *backupManager
	handlers handlerRegistry
	dbs      map[string]*sql.DB
	dbsMu    sync.Mutex
	closed   bool
	closedMu sync.Mutex
}

// New creates and starts a new Colmena node.
func New(cfg Config) (*Node, error) {
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("colmena: create data dir: %w", err)
	}

	sm := newStoreManager(cfg.DataDir, cfg.SQLiteReadConns)
	f := &fsm{stores: sm, onApply: cfg.OnApply}

	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(cfg.NodeID)
	raftConfig.Logger = hclog.New(&hclog.LoggerOptions{Output: cfg.LogOutput, Level: hclog.Warn})
	raftConfig.HeartbeatTimeout = cfg.HeartbeatTimeout
	raftConfig.ElectionTimeout = cfg.ElectionTimeout
	raftConfig.LeaderLeaseTimeout = cfg.HeartbeatTimeout
	raftConfig.SnapshotInterval = cfg.SnapshotInterval
	raftConfig.SnapshotThreshold = cfg.SnapshotThreshold

	advertise, err := net.ResolveTCPAddr("tcp", cfg.Advertise)
	if err != nil {
		sm.close()
		return nil, fmt.Errorf("colmena: resolve advertise addr: %w", err)
	}
	var transport raft.Transport
	if cfg.TLSConfig != nil {
		serverTLS := cfg.TLSConfig.Clone()
		serverTLS.ClientAuth = tls.RequireAndVerifyClientCert
		if serverTLS.ClientCAs == nil {
			serverTLS.ClientCAs = serverTLS.RootCAs
		}
		ln, err := net.Listen("tcp", cfg.Bind)
		if err != nil {
			sm.close()
			return nil, fmt.Errorf("colmena: listen raft: %w", err)
		}
		tlsLn := tls.NewListener(ln, serverTLS)
		layer := &tlsStreamLayer{listener: tlsLn, advertise: advertise, tlsConfig: cfg.TLSConfig}
		transport = raft.NewNetworkTransport(layer, cfg.MaxPool, 10*time.Second, cfg.LogOutput)
	} else {
		transport, err = raft.NewTCPTransport(cfg.Bind, advertise, cfg.MaxPool, 10*time.Second, cfg.LogOutput)
		if err != nil {
			sm.close()
			return nil, fmt.Errorf("colmena: create transport: %w", err)
		}
	}

	logStore, err := raftboltdb.New(raftboltdb.Options{
		Path:   filepath.Join(cfg.DataDir, "raft.db"),
		NoSync: cfg.UnsafeNoRaftLogFsync,
	})
	if err != nil {
		sm.close()
		return nil, fmt.Errorf("colmena: create log store: %w", err)
	}

	snapshotStore, err := raft.NewFileSnapshotStore(cfg.DataDir, 2, cfg.LogOutput)
	if err != nil {
		sm.close()
		return nil, fmt.Errorf("colmena: create snapshot store: %w", err)
	}

	r, err := raft.NewRaft(raftConfig, f, logStore, logStore, snapshotStore, transport)
	if err != nil {
		sm.close()
		return nil, fmt.Errorf("colmena: create raft: %w", err)
	}

	node := &Node{
		config:     cfg,
		stores:     sm,
		raft:       r,
		fsm:        f,
		rpcPool:    newRPCPool(cfg.TLSConfig, cfg.NodeID),
		handlers:   handlerRegistry{handlers: make(map[string]func([]byte) ([]byte, error))},
		dbs:        make(map[string]*sql.DB),
	}

	if cfg.Bootstrap {
		configuration := raft.Configuration{
			Servers: []raft.Server{{
				ID:      raft.ServerID(cfg.NodeID),
				Address: raft.ServerAddress(cfg.Advertise),
			}},
		}
		bf := r.BootstrapCluster(configuration)
		if err := bf.Error(); err != nil && err != raft.ErrCantBootstrap {
			node.Close()
			return nil, fmt.Errorf("colmena: bootstrap: %w", err)
		}
	}

	if err := node.startRPC(); err != nil {
		node.Close()
		return nil, err
	}

	if len(cfg.Join) > 0 {
		if err := node.join(); err != nil {
			node.Close()
			return nil, err
		}
	}

	if cfg.BatchWindow > 0 {
		node.batcher = newWriteBatcher(node, cfg.BatchWindow, cfg.BatchMaxSize)
	}
	// Negative BatchWindow disables batching entirely (opt-out escape hatch).

	node.lease = &readLease{}
	node.leaseStop = make(chan struct{})
	go node.leaseLoop()

	if cfg.Backup != nil && cfg.Backup.Backend != nil {
		defStore, err := sm.get("default")
		if err != nil {
			node.Close()
			return nil, fmt.Errorf("colmena: init default store for backup: %w", err)
		}
		bm := newBackupManager(defStore, *cfg.Backup)
		if err := bm.start(); err != nil {
			node.Close()
			return nil, fmt.Errorf("colmena: start backup: %w", err)
		}
		node.backup = bm
	}

	return node, nil
}

// DB returns a *sql.DB for the "default" database with the node's default consistency.
func (n *Node) DB() *sql.DB {
	return n.OpenDB("default", n.config.Consistency)
}

// OpenDB returns a *sql.DB for the named database with the given default consistency.
// Each database maps to a separate SQLite file. Cached: same name returns same instance.
func (n *Node) OpenDB(name string, consistency ConsistencyLevel) *sql.DB {
	n.dbsMu.Lock()
	defer n.dbsMu.Unlock()
	if db, ok := n.dbs[name]; ok {
		return db
	}
	db := sql.OpenDB(&colmenaConnector{node: n, dbName: name, consistency: consistency})
	n.dbs[name] = db
	return db
}

func (n *Node) IsLeader() bool            { return n.raft.State() == raft.Leader }
func (n *Node) LeaderAddr() string         { _, id := n.raft.LeaderWithID(); return string(id) }
func (n *Node) NodeID() string             { return n.config.NodeID }
func (n *Node) Stats() map[string]string   { return n.raft.Stats() }

// Close shuts down the node gracefully. Safe to call multiple times.
func (n *Node) Close() error {
	n.closedMu.Lock()
	if n.closed { n.closedMu.Unlock(); return nil }
	n.closed = true
	n.closedMu.Unlock()

	var firstErr error
	if n.leaseStop != nil { close(n.leaseStop) }
	if n.batcher != nil { n.batcher.close() }
	if n.backup != nil { n.backup.stop() }

	n.dbsMu.Lock()
	for _, db := range n.dbs {
		if err := db.Close(); err != nil && firstErr == nil { firstErr = err }
	}
	n.dbsMu.Unlock()

	if n.rpcListener != nil { n.rpcListener.Close() }
	if n.rpcPool != nil { n.rpcPool.close() }

	if n.raft != nil {
		if err := n.raft.Shutdown().Error(); err != nil && firstErr == nil { firstErr = err }
	}
	if n.stores != nil {
		if err := n.stores.close(); err != nil && firstErr == nil { firstErr = err }
	}
	return firstErr
}

func (n *Node) execute(cmd *Command) (*ApplyResult, error) {
	if err := validateWriteStatements(cmd.Statements); err != nil {
		return nil, err
	}
	var result *ApplyResult
	var err error
	if n.raft.State() == raft.Leader {
		if n.batcher != nil {
			result, err = n.batcher.submit(cmd)
		} else {
			var data []byte
			data, err = marshalCommand(cmd)
			if err != nil {
				return nil, err
			}
			result, err = n.applyRaft(data)
		}
	} else {
		var data []byte
		data, err = marshalCommand(cmd)
		if err != nil {
			return nil, err
		}
		result, err = n.forwardExecute(data)
	}
	if err == nil {
		n.metrics.writesTotal.Add(1)
	}
	return result, err
}

func (n *Node) applyRaft(data []byte) (*ApplyResult, error) {
	future := n.raft.Apply(data, n.config.ApplyTimeout)
	if err := future.Error(); err != nil {
		return nil, fmt.Errorf("colmena: raft apply: %w", err)
	}
	resp, ok := future.Response().(*ApplyResult)
	if !ok { return nil, fmt.Errorf("colmena: unexpected apply response type") }
	if resp.Error != "" { return nil, errors.New(resp.Error) }
	return resp, nil
}

func (n *Node) verifyLeader() error { return n.raft.VerifyLeader().Error() }

func (n *Node) forwardExecute(data []byte) (*ApplyResult, error) {
	leaderAddr, _ := n.raft.LeaderWithID()
	if leaderAddr == "" { return nil, ErrNotLeader }
	addr := string(leaderAddr)
	client, err := n.rpcPool.get(addr)
	if err != nil { return nil, fmt.Errorf("colmena: connect to leader: %w", err) }
	n.metrics.rpcForwardsTotal.Add(1)
	req := &RPCExecuteRequest{Command: data}
	var resp RPCExecuteResponse
	if err := client.Call("Colmena.Execute", req, &resp); err != nil {
		n.rpcPool.markFailed(addr)
		return nil, fmt.Errorf("colmena: forward execute: %w", err)
	}
	if resp.Error != "" { return nil, errors.New(resp.Error) }
	return &ApplyResult{Results: resp.Results}, nil
}

func (n *Node) forwardQuery(dbName, sqlStr string, args []any) (*RPCQueryResponse, error) {
	leaderAddr, _ := n.raft.LeaderWithID()
	if leaderAddr == "" { return nil, ErrNotLeader }
	addr := string(leaderAddr)
	client, err := n.rpcPool.get(addr)
	if err != nil { return nil, fmt.Errorf("colmena: connect to leader: %w", err) }
	n.metrics.rpcForwardsTotal.Add(1)
	iArgs := make([]any, len(args))
	copy(iArgs, args)
	req := &RPCQueryRequest{DB: dbName, SQL: sqlStr, Args: iArgs}
	var resp RPCQueryResponse
	if err := client.Call("Colmena.Query", req, &resp); err != nil {
		n.rpcPool.markFailed(addr)
		return nil, fmt.Errorf("colmena: forward query: %w", err)
	}
	if resp.Error != "" { return nil, errors.New(resp.Error) }
	return &resp, nil
}

func (n *Node) forwardHandler(name string, data []byte) ([]byte, error) {
	leaderAddr, _ := n.raft.LeaderWithID()
	if leaderAddr == "" { return nil, ErrNotLeader }
	addr := string(leaderAddr)
	client, err := n.rpcPool.get(addr)
	if err != nil { return nil, fmt.Errorf("colmena: connect to leader: %w", err) }
	n.metrics.rpcForwardsTotal.Add(1)
	req := &RPCForwardRequest{Handler: name, Payload: data}
	var resp RPCForwardResponse
	if err := client.Call("Colmena.Forward", req, &resp); err != nil {
		n.rpcPool.markFailed(addr)
		return nil, fmt.Errorf("colmena: forward handler: %w", err)
	}
	if resp.Error != "" { return nil, errors.New(resp.Error) }
	return resp.Payload, nil
}

func (n *Node) WaitForLeader(timeout time.Duration) error {
	deadline := time.After(timeout)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-deadline:
			return fmt.Errorf("colmena: timeout waiting for leader")
		case <-ticker.C:
			if addr, _ := n.raft.LeaderWithID(); addr != "" { return nil }
		}
	}
}

// ExecMulti executes multiple statements atomically on the "default" database.
func (n *Node) ExecMulti(stmts []Statement) ([]ExecResult, error) {
	cmd := &Command{Type: CommandExecuteMulti, DB: "default", Statements: stmts}
	result, err := n.execute(cmd)
	if err != nil { return nil, err }
	return result.Results, nil
}

func (n *Node) Nodes() ([]raft.Server, error) {
	f := n.raft.GetConfiguration()
	if err := f.Error(); err != nil { return nil, err }
	return f.Configuration().Servers, nil
}

func (n *Node) RemoveNode(nodeID string) error {
	return n.raft.RemoveServer(raft.ServerID(nodeID), 0, n.config.ApplyTimeout).Error()
}

// AddNonvoter adds a non-voting learner to the cluster. Must be called on
// the leader. Non-voters replicate the Raft log and can serve local reads
// but do not count toward quorum, so they don't add latency to writes.
//
// Use this when scaling out for read throughput rather than fault tolerance:
// keep a small voter core (3 or 5 nodes) and add as many non-voters as you
// need behind it.
func (n *Node) AddNonvoter(nodeID, address string) error {
	return n.raft.AddNonvoter(raft.ServerID(nodeID), raft.ServerAddress(address), 0, n.config.ApplyTimeout).Error()
}

// AddVoter adds a full Raft voter to the cluster. Must be called on the leader.
// If the node already exists as a non-voter, it is promoted to voter.
func (n *Node) AddVoter(nodeID, address string) error {
	return n.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(address), 0, n.config.ApplyTimeout).Error()
}

// DemoteVoter turns a voter into a non-voting learner without removing it.
// Must be called on the leader. Useful for shrinking the voter quorum
// (e.g., reducing 5 voters to 3) without losing the replica entirely.
func (n *Node) DemoteVoter(nodeID string) error {
	return n.raft.DemoteVoter(raft.ServerID(nodeID), 0, n.config.ApplyTimeout).Error()
}

// TransferLeadership asks Raft to move leadership to another up-to-date voter.
// It is used on graceful shutdown so a node can hand off before it leaves,
// avoiding an election timeout. Safe to call on a non-leader (Raft returns an
// error the caller may ignore). Returns once the transfer completes or fails.
func (n *Node) TransferLeadership() error {
	return n.raft.LeadershipTransfer().Error()
}

// --- RPC ---

type RPCJoinRequest struct {
	NodeID, Address string
	// AsNonvoter, when true, requests the leader to add this node as a
	// non-voting learner via raft.AddNonvoter. Default false (= AddVoter)
	// preserves backwards compatibility with pre-v0.9 clients.
	AsNonvoter bool
}
type RPCJoinResponse struct{ Error, LeaderAddr string }

// RPCHelloRequest is sent by a node when it first opens an RPC connection
// to another node. It lets peers detect version skew *early* (before a
// command with an unreadable envelope reaches the log) instead of at
// apply time.
type RPCHelloRequest struct {
	NodeID                string
	LibraryVersion        string
	ProtocolVersion       int
	CommandFormatVersion  int
	SnapshotFormatVersion int
}

// RPCHelloResponse mirrors the fields so the caller can log/diff them.
type RPCHelloResponse struct {
	NodeID                string
	LibraryVersion        string
	ProtocolVersion       int
	CommandFormatVersion  int
	SnapshotFormatVersion int
}

// RPCStatusRequest/RPCStatusResponse back a read-only cluster-status probe.
// Discovery layers that cannot observe peer Raft state out-of-band (e.g. the
// fly package, where Fly's internal DNS carries no bootstrapping/voter flags)
// use it to detect an already-formed cluster before deciding whether to
// bootstrap a new one — avoiding split-brain when a fresh, low-id machine
// boots into an existing cluster.
type RPCStatusRequest struct{}
type RPCStatusResponse struct {
	NodeID     string
	IsLeader   bool
	LeaderAddr string // current Raft leader's advertise addr, "" if none is known
	Voters     int    // number of voters in this node's view of the configuration
}

type RPCService struct{ node *Node }

// Status reports this node's leadership view. Read-only and always succeeds.
func (s *RPCService) Status(req *RPCStatusRequest, resp *RPCStatusResponse) error {
	resp.NodeID = s.node.config.NodeID
	resp.IsLeader = s.node.raft.State() == raft.Leader
	addr, _ := s.node.raft.LeaderWithID()
	resp.LeaderAddr = string(addr)
	if cf := s.node.raft.GetConfiguration(); cf.Error() == nil {
		for _, srv := range cf.Configuration().Servers {
			if srv.Suffrage == raft.Voter {
				resp.Voters++
			}
		}
	}
	return nil
}

// ProbeStatus dials the RPC sidecar of the node at raftAddr (host:port; the RPC
// server listens on port+1) and returns its cluster status. It lets a starting
// node detect an already-formed cluster before deciding to bootstrap. Pass the
// same TLS config the cluster uses, or nil for plaintext. A non-nil error means
// the peer was unreachable or not yet serving RPC.
func ProbeStatus(raftAddr string, tlsConfig *tls.Config, timeout time.Duration) (RPCStatusResponse, error) {
	rpcAddr, err := rpcAddrFrom(raftAddr)
	if err != nil {
		return RPCStatusResponse{}, err
	}
	dialer := &net.Dialer{Timeout: timeout}
	var conn net.Conn
	if tlsConfig != nil {
		conn, err = tls.DialWithDialer(dialer, "tcp", rpcAddr, tlsConfig)
	} else {
		conn, err = dialer.Dial("tcp", rpcAddr)
	}
	if err != nil {
		return RPCStatusResponse{}, err
	}
	defer conn.Close()
	if timeout > 0 {
		_ = conn.SetDeadline(time.Now().Add(timeout))
	}
	client := rpc.NewClient(conn)
	defer client.Close()
	var resp RPCStatusResponse
	if err := client.Call("Colmena.Status", &RPCStatusRequest{}, &resp); err != nil {
		return RPCStatusResponse{}, err
	}
	return resp, nil
}

// Hello is a version handshake. It never fails: the goal is to surface
// incompatibilities through logs and metrics without breaking clusters that
// are intentionally running mixed versions during a rolling upgrade. The
// dialer is responsible for deciding what to do with the response.
func (s *RPCService) Hello(req *RPCHelloRequest, resp *RPCHelloResponse) error {
	if req.ProtocolVersion != ProtocolVersion {
		log.Printf("colmena: peer %s (v%s) has protocol=%d, local=%d — expect RPC incompatibility",
			req.NodeID, req.LibraryVersion, req.ProtocolVersion, ProtocolVersion)
	}
	if req.CommandFormatVersion > CommandFormatVersion {
		log.Printf("colmena: peer %s (v%s) writes command format v%d, local max v%d — will reject its log entries",
			req.NodeID, req.LibraryVersion, req.CommandFormatVersion, CommandFormatVersion)
	}
	if req.SnapshotFormatVersion > SnapshotFormatVersion {
		log.Printf("colmena: peer %s (v%s) writes snapshot format v%d, local max v%d — will reject its snapshots",
			req.NodeID, req.LibraryVersion, req.SnapshotFormatVersion, SnapshotFormatVersion)
	}
	resp.NodeID = s.node.config.NodeID
	resp.LibraryVersion = LibraryVersion
	resp.ProtocolVersion = ProtocolVersion
	resp.CommandFormatVersion = CommandFormatVersion
	resp.SnapshotFormatVersion = SnapshotFormatVersion
	return nil
}

func (s *RPCService) Execute(req *RPCExecuteRequest, resp *RPCExecuteResponse) error {
	if s.node.raft.State() != raft.Leader { resp.Error = "not the leader"; return nil }
	result, err := s.node.applyRaft(req.Command)
	if err != nil { resp.Error = err.Error(); return nil }
	resp.Results = result.Results
	return nil
}

func (s *RPCService) Query(req *RPCQueryRequest, resp *RPCQueryResponse) error {
	dbName := req.DB
	if dbName == "" { dbName = "default" }
	st, err := s.node.stores.get(dbName)
	if err != nil { resp.Error = err.Error(); return nil }
	rows, err := st.query(req.SQL, req.Args...)
	if err != nil { resp.Error = err.Error(); return nil }
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil { resp.Error = err.Error(); return nil }
	resp.Columns = cols
	for rows.Next() {
		values := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values { ptrs[i] = &values[i] }
		if err := rows.Scan(ptrs...); err != nil { resp.Error = err.Error(); return nil }
		tagged := make([]TaggedValue, len(cols))
		legacy := make([]json.RawMessage, len(cols))
		for i, v := range values {
			tagged[i] = encodeTaggedValue(v)
			b, _ := json.Marshal(v)
			legacy[i] = b
		}
		resp.TaggedRows = append(resp.TaggedRows, tagged)
		resp.Rows = append(resp.Rows, legacy)
	}
	if err := rows.Err(); err != nil { resp.Error = err.Error() }
	return nil
}

func (s *RPCService) Forward(req *RPCForwardRequest, resp *RPCForwardResponse) error {
	if s.node.raft.State() != raft.Leader { resp.Error = "not the leader"; return nil }
	data, err := s.node.handlers.call(req.Handler, req.Payload)
	if err != nil { resp.Error = err.Error(); return nil }
	resp.Payload = data
	return nil
}

func (s *RPCService) Join(req *RPCJoinRequest, resp *RPCJoinResponse) error {
	if s.node.raft.State() != raft.Leader {
		leaderAddr, _ := s.node.raft.LeaderWithID()
		resp.Error = "not the leader"
		resp.LeaderAddr = string(leaderAddr)
		return nil
	}
	id := raft.ServerID(req.NodeID)
	addr := raft.ServerAddress(req.Address)
	timeout := s.node.config.ApplyTimeout

	// If a different NodeID is already registered at this address (e.g.,
	// a node was reflashed and rejoined with a fresh NodeID but kept its
	// DHCP IP, or a container was recreated with the same IP),
	// AddVoter/AddNonvoter would fail with "found duplicate address in
	// configuration". Remove the stale member first so the new one can
	// take its slot.
	if cf := s.node.raft.GetConfiguration(); cf.Error() == nil {
		for _, srv := range cf.Configuration().Servers {
			if string(srv.Address) == req.Address && string(srv.ID) != req.NodeID {
				if err := s.node.raft.RemoveServer(srv.ID, 0, timeout).Error(); err != nil {
					resp.Error = fmt.Sprintf("replace stale peer %s at %s: %v", srv.ID, srv.Address, err)
					return nil
				}
				break
			}
		}
	}

	var f raft.IndexFuture
	if req.AsNonvoter {
		f = s.node.raft.AddNonvoter(id, addr, 0, timeout)
	} else {
		f = s.node.raft.AddVoter(id, addr, 0, timeout)
	}
	if err := f.Error(); err != nil { resp.Error = err.Error() }
	return nil
}

func (n *Node) join() error {
	for _, addr := range n.config.Join {
		client, err := n.rpcPool.get(addr)
		if err != nil { log.Printf("colmena: failed to connect to %s: %v", addr, err); continue }
		req := &RPCJoinRequest{
			NodeID:     n.config.NodeID,
			Address:    n.config.Advertise,
			AsNonvoter: n.config.JoinAs == JoinAsNonvoter,
		}
		var resp RPCJoinResponse
		if err := client.Call("Colmena.Join", req, &resp); err != nil {
			n.rpcPool.markFailed(addr)
			log.Printf("colmena: join RPC to %s failed: %v", addr, err)
			continue
		}
		if resp.Error != "" {
			if resp.LeaderAddr != "" {
				if c2, err := n.rpcPool.get(resp.LeaderAddr); err == nil {
					var r2 RPCJoinResponse
					if err := c2.Call("Colmena.Join", req, &r2); err == nil && r2.Error == "" { return nil }
				}
			}
			log.Printf("colmena: join via %s: %s", addr, resp.Error); continue
		}
		return nil
	}
	return fmt.Errorf("colmena: failed to join cluster via any address")
}

func (n *Node) startRPC() error {
	rpcAddr, err := rpcAddrFrom(n.config.Bind)
	if err != nil { return err }
	n.rpcServer = rpc.NewServer()
	if err := n.rpcServer.RegisterName("Colmena", &RPCService{node: n}); err != nil {
		return fmt.Errorf("colmena: register RPC: %w", err)
	}
	var ln net.Listener
	if n.config.TLSConfig != nil {
		serverTLS := n.config.TLSConfig.Clone()
		serverTLS.ClientAuth = tls.RequireAndVerifyClientCert
		if serverTLS.ClientCAs == nil {
			serverTLS.ClientCAs = serverTLS.RootCAs
		}
		ln, err = tls.Listen("tcp", rpcAddr, serverTLS)
	} else {
		ln, err = net.Listen("tcp", rpcAddr)
	}
	if err != nil { return fmt.Errorf("colmena: listen RPC on %s: %w", rpcAddr, err) }
	n.rpcListener = ln
	go func() {
		for { conn, err := ln.Accept(); if err != nil { return }; go n.rpcServer.ServeConn(conn) }
	}()
	return nil
}

func rpcAddrFrom(raftAddr string) (string, error) {
	host, portStr, err := net.SplitHostPort(raftAddr)
	if err != nil { return "", fmt.Errorf("colmena: parse addr %q: %w", raftAddr, err) }
	port, err := strconv.Atoi(portStr)
	if err != nil { return "", fmt.Errorf("colmena: parse port %q: %w", portStr, err) }
	return net.JoinHostPort(host, strconv.Itoa(port+1)), nil
}

// leaseLoop periodically checks Raft's last_contact stat and extends the
// read lease when the leader heartbeat is fresh. On leaders, the lease is
// always valid. On followers, it tracks leader heartbeat freshness.
func (n *Node) leaseLoop() {
	ticker := time.NewTicker(n.config.HeartbeatTimeout / 4)
	defer ticker.Stop()
	for {
		select {
		case <-n.leaseStop:
			return
		case <-ticker.C:
			if n.raft.State() == raft.Leader {
				// Leaders always have fresh data.
				n.lease.extend(n.config.HeartbeatTimeout)
				continue
			}
			stats := n.raft.Stats()
			lastContact := stats["last_contact"]
			if lastContact == "never" || lastContact == "0" {
				continue
			}
			d, err := time.ParseDuration(lastContact)
			if err != nil {
				continue
			}
			// If last contact is within half the heartbeat timeout, extend the lease.
			if d < n.config.HeartbeatTimeout/2 {
				n.lease.extend(n.config.HeartbeatTimeout / 2)
			}
		}
	}
}
