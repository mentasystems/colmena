package colmena

import (
	"encoding/json"
	"fmt"
	"io"
	"log"

	"github.com/hashicorp/raft"
)

// fsm implements the raft.FSM interface, applying replicated commands to the local SQLite store.
type fsm struct {
	stores  *storeManager
	onApply func(db string, statements []Statement, results []ExecResult)
}

// Apply is called by Raft when a log entry is committed by a quorum.
// It executes the SQL statement(s) against the local SQLite database.
func (f *fsm) Apply(l *raft.Log) interface{} {
	cmd, err := unmarshalCommand(l.Data)
	if err != nil {
		log.Printf("colmena: fsm apply unmarshal error: %v", err)
		return &ApplyResult{Error: err.Error()}
	}

	dbName := cmd.DB
	if dbName == "" {
		dbName = "default"
	}

	st, err := f.stores.get(dbName)
	if err != nil {
		return &ApplyResult{Error: err.Error()}
	}

	var applyResult *ApplyResult

	switch cmd.Type {
	case CommandExecute:
		if len(cmd.Statements) != 1 {
			return &ApplyResult{Error: "execute command must have exactly 1 statement"}
		}
		result, err := st.execute(cmd.Statements[0])
		if err != nil {
			return &ApplyResult{Error: err.Error()}
		}
		applyResult = &ApplyResult{Results: []ExecResult{result}}

	case CommandExecuteMulti:
		results, err := st.executeMulti(cmd.Statements)
		if err != nil {
			return &ApplyResult{Error: err.Error()}
		}
		applyResult = &ApplyResult{Results: results}

	default:
		return &ApplyResult{Error: fmt.Sprintf("unknown command type: %d", cmd.Type)}
	}

	// Fire OnApply callback if set and command succeeded.
	if f.onApply != nil && applyResult.Error == "" {
		f.onApply(dbName, cmd.Statements, applyResult.Results)
	}

	return applyResult
}

// Snapshot returns an FSM snapshot for Raft log compaction.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	return &fsmSnapshot{stores: f.stores}, nil
}

// Restore replaces all local databases with the contents of a snapshot.
func (f *fsm) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	return f.stores.restore(rc)
}

// fsmSnapshot implements raft.FSMSnapshot using a tar archive of all stores.
type fsmSnapshot struct {
	stores *storeManager
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if err := s.stores.snapshot(sink); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}

// --- RPC types for leader forwarding ---

// RPCExecuteRequest is sent from a follower to the leader to execute a write.
type RPCExecuteRequest struct {
	Command []byte // JSON-encoded Command
}

// RPCExecuteResponse is the leader's response to a forwarded write.
type RPCExecuteResponse struct {
	Results []ExecResult
	Error   string
}

// RPCQueryRequest is sent from a follower to the leader for forwarded reads.
type RPCQueryRequest struct {
	DB   string
	SQL  string
	Args []interface{}
	// Consistency is the read level the originating node was asked for, so
	// the leader can re-verify quorum for Strong reads. Zero means the
	// request came from a pre-v0.11 peer (gob omits unknown fields) and is
	// treated as Weak: leadership-gated, no quorum round-trip.
	Consistency ConsistencyLevel
}

// RPCQueryResponse is the leader's response to a forwarded query.
//
// Rows (legacy, pre-0.6.1) carries JSON-marshaled driver values. String-
// serialized time.Time values lose their type there and fail to Scan into
// *time.Time on the caller. TaggedRows (0.6.1+) preserves Go type via a
// per-value discriminator.
//
// The leader cannot cheaply know the peer's version here, so it fills BOTH
// fields on every response (doubling the payload — acceptable until v0.6.0
// peers are extinct). A v0.6.1+ reader prefers TaggedRows when present and
// falls back to Rows for compatibility with v0.6.0 leaders.
type RPCQueryResponse struct {
	Columns    []string
	Rows       [][]json.RawMessage
	TaggedRows [][]TaggedValue
	Error      string
}
