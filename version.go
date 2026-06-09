package colmena

// LibraryVersion is the semver string for the current Colmena release.
// Bump on every tagged release. Used by Version() and exposed in Stats().
const LibraryVersion = "0.11.0"

// Wire format versions. Each envelope type has its own monotonically
// increasing version. A node that reads an envelope with an unknown (newer)
// version returns ErrUnsupportedFormatVersion — never silently ignores it.
//
// When bumping a version:
//  1. Add a new case to the decoder that handles the new format.
//  2. Keep old-version handling in place for at least one release so
//     rolling upgrades can mix N and N+1 nodes.
//  3. Add a fixture in testdata/fixtures to pin wire-level compatibility.
const (
	// CommandFormatVersion is the current version of the Command envelope
	// written to the Raft log. v1 is JSON-encoded Command wrapped with the
	// 10-byte header (see format.go). v2 (0.11.0) encodes statement args as
	// TaggedValues so int64/[]byte/time.Time survive replication intact
	// (plain JSON coerced every number to float64 and []byte to base64 TEXT).
	CommandFormatVersion = 2

	// SnapshotFormatVersion is the current version of the FSM snapshot
	// envelope. v1 is a tar archive of SQLite files wrapped with the header.
	SnapshotFormatVersion = 1

	// ProtocolVersion is the RPC handshake version. Bumped when the RPC
	// method set or argument shapes change in an incompatible way.
	//
	// v2 (0.6.1): RPCQueryResponse.TaggedRows carries type-preserving values
	// so forwarded reads can reconstruct time.Time and other driver-specific
	// types. v1 peers still fill RPCQueryResponse.Rows; v2 readers fall back
	// to Rows when TaggedRows is empty.
	ProtocolVersion = 2
)

// Version returns the library version this node is running.
func (n *Node) Version() string { return LibraryVersion }
