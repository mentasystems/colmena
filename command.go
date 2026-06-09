package colmena

import (
	"encoding/json"
	"fmt"
)

// CommandType identifies the type of command in the Raft log.
type CommandType uint8

const (
	// CommandExecute is a write operation (INSERT, UPDATE, DELETE, DDL).
	CommandExecute CommandType = iota
	// CommandExecuteMulti is an atomic batch of write operations (transaction).
	CommandExecuteMulti
)

// Command is the unit of replication in the Raft log.
type Command struct {
	Type       CommandType `json:"type"`
	DB         string      `json:"db"`
	Statements []Statement `json:"stmts"`
}

// Statement is a single SQL statement with optional arguments.
type Statement struct {
	SQL  string        `json:"sql"`
	Args []interface{} `json:"args,omitempty"`
}

// ExecResult holds the result of an executed statement.
type ExecResult struct {
	LastInsertID int64 `json:"last_id"`
	RowsAffected int64 `json:"rows"`
}

// ApplyResult is returned by the FSM after applying a command.
type ApplyResult struct {
	Results []ExecResult `json:"results,omitempty"`
	Error   string       `json:"error,omitempty"`
}

// wireStatementV2/wireCommandV2 are the v2 wire shape of a Command. Statement
// args are encoded as TaggedValues instead of bare JSON values: plain JSON has
// no integer type (int64 args above 2^53 lost precision and bound as REAL)
// and no byte type ([]byte args were silently stored as base64 TEXT). The
// tagged encoding is the same one query responses already use.
type wireStatementV2 struct {
	SQL  string        `json:"sql"`
	Args []TaggedValue `json:"args,omitempty"`
}

type wireCommandV2 struct {
	Type       CommandType       `json:"type"`
	DB         string            `json:"db"`
	Statements []wireStatementV2 `json:"stmts"`
}

// marshalCommand serializes cmd with the v2 envelope:
//
//	[10-byte header: magic|kind=Command|version=2] [JSON payload, tagged args]
//
// Older Colmena versions (<= v0.5.x) wrote raw JSON with no envelope, and
// v0.6–v0.10 wrote the v1 envelope (plain JSON args). Both can still be read
// back by unmarshalCommand, so a cluster's existing Raft log survives an
// upgrade — only new entries get the v2 format. Note: v2 entries are rejected
// by pre-v0.11 nodes, so upgrade every node before resuming writes.
func marshalCommand(cmd *Command) ([]byte, error) {
	wire := wireCommandV2{
		Type:       cmd.Type,
		DB:         cmd.DB,
		Statements: make([]wireStatementV2, len(cmd.Statements)),
	}
	for i, st := range cmd.Statements {
		ws := wireStatementV2{SQL: st.SQL}
		if len(st.Args) > 0 {
			ws.Args = make([]TaggedValue, len(st.Args))
			for j, a := range st.Args {
				ws.Args[j] = encodeTaggedValue(a)
			}
		}
		wire.Statements[i] = ws
	}
	payload, err := json.Marshal(wire)
	if err != nil {
		return nil, fmt.Errorf("colmena: marshal command: %w", err)
	}
	return encodeEnvelope(FormatKindCommand, CommandFormatVersion, payload), nil
}

// unmarshalCommand parses a Raft log entry written by any Colmena version.
// It recognises:
//   - v2 envelope (current): magic + kind=Command + version=2 + JSON with
//     TaggedValue args
//   - v1 envelope (v0.6–v0.10): magic + kind=Command + version=1 + plain JSON
//   - legacy unenveloped JSON (<= v0.5.x): raw `{...}` bytes
//
// v1/legacy entries are decoded exactly as before (numeric args become
// float64): replicas that already applied those entries did so with float64
// bindings, and a fresh replica replaying the log must reproduce the same
// bytes or it would silently diverge.
//
// An envelope with kind=Command but an unknown version returns
// ErrUnsupportedFormatVersion so the node refuses to apply garbage rather
// than silently diverging from its peers.
func unmarshalCommand(data []byte) (*Command, error) {
	payload := data
	if hasEnvelopeMagic(data) {
		kind, version, p, err := decodeEnvelope(data)
		if err != nil {
			return nil, fmt.Errorf("colmena: unmarshal command: %w", err)
		}
		if kind != FormatKindCommand {
			return nil, fmt.Errorf("colmena: unmarshal command: unexpected envelope kind %d", kind)
		}
		switch version {
		case 1:
			payload = p
		case 2:
			return unmarshalCommandV2(p)
		default:
			return nil, fmt.Errorf("colmena: unmarshal command version %d: %w", version, ErrUnsupportedFormatVersion)
		}
	} else if !looksLikeLegacyCommand(data) {
		return nil, fmt.Errorf("colmena: unmarshal command: unrecognized format (first byte 0x%02x)", firstByte(data))
	}

	var cmd Command
	if err := json.Unmarshal(payload, &cmd); err != nil {
		return nil, fmt.Errorf("colmena: unmarshal command: %w", err)
	}
	return &cmd, nil
}

func unmarshalCommandV2(payload []byte) (*Command, error) {
	var wire wireCommandV2
	if err := json.Unmarshal(payload, &wire); err != nil {
		return nil, fmt.Errorf("colmena: unmarshal command v2: %w", err)
	}
	cmd := &Command{
		Type:       wire.Type,
		DB:         wire.DB,
		Statements: make([]Statement, len(wire.Statements)),
	}
	for i, ws := range wire.Statements {
		st := Statement{SQL: ws.SQL}
		if len(ws.Args) > 0 {
			st.Args = make([]interface{}, len(ws.Args))
			for j, tv := range ws.Args {
				v, err := decodeTaggedValue(tv)
				if err != nil {
					return nil, fmt.Errorf("colmena: unmarshal command v2 arg %d/%d: %w", i, j, err)
				}
				st.Args[j] = v
			}
		}
		cmd.Statements[i] = st
	}
	return cmd, nil
}

// looksLikeLegacyCommand returns true if data looks like the pre-v0.6
// unenveloped JSON Command (starts with '{' after optional whitespace).
func looksLikeLegacyCommand(data []byte) bool {
	for _, b := range data {
		switch b {
		case ' ', '\t', '\r', '\n':
			continue
		case '{':
			return true
		default:
			return false
		}
	}
	return false
}

func firstByte(data []byte) byte {
	if len(data) == 0 {
		return 0
	}
	return data[0]
}
