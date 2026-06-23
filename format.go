package colmena

import (
	"errors"
	"fmt"
	"io"
)

// Wire envelope format.
//
// Every persisted/replicated blob is wrapped in a fixed 10-byte header so
// future format changes can be detected and rejected (instead of silently
// corrupting state). The header is intentionally impossible to confuse with
// any legacy payload Colmena has produced:
//
//	bytes 0..6  magic  "COLMENA"
//	byte  7     0x00 (reserved; future envelope rev)
//	byte  8     kind  (see FormatKind*)
//	byte  9     version (monotonic per kind)
//	bytes 10..  payload
//
// Decoders try the magic first; if absent, they fall back to the legacy
// sniffers registered for that kind (e.g. "{" for JSON commands, "SQLite
// format 3\x00" for v0.2.0 raw-db snapshots, tar header for v0.3..v0.5
// snapshots).

const envelopeHeaderSize = 10

var envelopeMagic = [8]byte{'C', 'O', 'L', 'M', 'E', 'N', 'A', 0x00}

// FormatKind identifies which envelope sub-format the payload uses.
type FormatKind uint8

const (
	FormatKindCommand  FormatKind = 1
	FormatKindSnapshot FormatKind = 2
)

// ErrUnsupportedFormatVersion is returned when a reader sees an envelope
// with a kind/version combination it doesn't know how to decode.
var ErrUnsupportedFormatVersion = errors.New("colmena: unsupported format version")

// encodeEnvelope prepends the 10-byte header to payload.
func encodeEnvelope(kind FormatKind, version uint8, payload []byte) []byte {
	out := make([]byte, envelopeHeaderSize+len(payload))
	copy(out[0:8], envelopeMagic[:])
	out[8] = byte(kind)
	out[9] = version
	copy(out[envelopeHeaderSize:], payload)
	return out
}

// writeEnvelopeHeader writes the 10-byte header to w. Used by streaming
// encoders (e.g. snapshot persist) that want to produce the payload directly
// into w without materializing it in memory first.
func writeEnvelopeHeader(w io.Writer, kind FormatKind, version uint8) error {
	var hdr [envelopeHeaderSize]byte
	copy(hdr[0:8], envelopeMagic[:])
	hdr[8] = byte(kind)
	hdr[9] = version
	_, err := w.Write(hdr[:])
	return err
}

// hasEnvelopeMagic returns true if b starts with the envelope magic.
func hasEnvelopeMagic(b []byte) bool {
	if len(b) < envelopeHeaderSize {
		return false
	}
	for i, m := range envelopeMagic {
		if b[i] != m {
			return false
		}
	}
	return true
}

// decodeEnvelope parses the header and returns (kind, version, payload).
// Returns an error if the magic is wrong; does NOT validate the version.
func decodeEnvelope(b []byte) (FormatKind, uint8, []byte, error) {
	if !hasEnvelopeMagic(b) {
		return 0, 0, nil, fmt.Errorf("colmena: envelope magic missing")
	}
	return FormatKind(b[8]), b[9], b[envelopeHeaderSize:], nil
}
