package colmena

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// SQLite WAL file format (https://sqlite.org/walformat.html):
//
//	32-byte header:  magic(4) version(4) pageSize(4) checkpointSeq(4)
//	                 salt1(4) salt2(4) checksum1(4) checksum2(4)
//	frames:          24-byte frame header + pageSize bytes of page data
//	frame header:    pageNo(4) dbSizeAfterCommit(4) salt1(4) salt2(4)
//	                 checksum1(4) checksum2(4)
//
// A frame belongs to the current WAL iff its salts match the header salts and
// the cumulative checksum chain (seeded by the header checksum) is valid up to
// and including the frame. A frame with dbSizeAfterCommit > 0 is a commit
// frame; everything up to the last valid commit frame is durable, committed
// state — that is the exact prefix the backup engine ships.

const (
	walHeaderSize      = 32
	walFrameHeaderSize = 24

	walMagicBE = 0x377f0683 // checksums computed big-endian
	walMagicLE = 0x377f0682 // checksums computed little-endian
)

// walHeader is the parsed 32-byte WAL header.
type walHeader struct {
	bigEndian bool
	pageSize  int64
	salt1     uint32
	salt2     uint32
	cks1      uint32
	cks2      uint32
}

// frameSize returns the on-disk size of one WAL frame for this header.
func (h walHeader) frameSize() int64 { return walFrameHeaderSize + h.pageSize }

func parseWALHeader(b []byte) (walHeader, error) {
	if len(b) < walHeaderSize {
		return walHeader{}, fmt.Errorf("colmena: short WAL header (%d bytes)", len(b))
	}
	magic := binary.BigEndian.Uint32(b[0:4])
	if magic != walMagicBE && magic != walMagicLE {
		return walHeader{}, fmt.Errorf("colmena: bad WAL magic %#x", magic)
	}
	h := walHeader{
		bigEndian: magic == walMagicBE,
		pageSize:  int64(binary.BigEndian.Uint32(b[8:12])),
		salt1:     binary.BigEndian.Uint32(b[16:20]),
		salt2:     binary.BigEndian.Uint32(b[20:24]),
		cks1:      binary.BigEndian.Uint32(b[24:28]),
		cks2:      binary.BigEndian.Uint32(b[28:32]),
	}
	if h.pageSize < 512 || h.pageSize > 65536 || h.pageSize&(h.pageSize-1) != 0 {
		return walHeader{}, fmt.Errorf("colmena: bad WAL page size %d", h.pageSize)
	}
	// Verify the header's own checksum (over its first 24 bytes) so a torn
	// header write is treated as "no WAL yet" instead of garbage salts.
	s1, s2 := walChecksum(h.bigEndian, 0, 0, b[:24])
	if s1 != h.cks1 || s2 != h.cks2 {
		return walHeader{}, fmt.Errorf("colmena: WAL header checksum mismatch")
	}
	return h, nil
}

// walChecksum implements SQLite's WAL checksum: a running pair over 8-byte
// chunks, byte order chosen by the WAL magic. len(b) must be a multiple of 8.
func walChecksum(bigEndian bool, s1, s2 uint32, b []byte) (uint32, uint32) {
	for i := 0; i+8 <= len(b); i += 8 {
		var x0, x1 uint32
		if bigEndian {
			x0 = binary.BigEndian.Uint32(b[i:])
			x1 = binary.BigEndian.Uint32(b[i+4:])
		} else {
			x0 = binary.LittleEndian.Uint32(b[i:])
			x1 = binary.LittleEndian.Uint32(b[i+4:])
		}
		s1 += x0 + s2
		s2 += x1 + s1
	}
	return s1, s2
}

// walCommittedSize scans the WAL file and returns the parsed header plus the
// byte offset just past the last valid committed frame. Offset walHeaderSize
// means "valid header, no committed frames yet". A missing or headerless file
// returns os.ErrNotExist.
func walCommittedSize(path string) (walHeader, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return walHeader{}, 0, err
	}
	defer f.Close()

	hdrBuf := make([]byte, walHeaderSize)
	if _, err := io.ReadFull(f, hdrBuf); err != nil {
		return walHeader{}, 0, os.ErrNotExist // empty or truncated: no WAL yet
	}
	hdr, err := parseWALHeader(hdrBuf)
	if err != nil {
		return walHeader{}, 0, os.ErrNotExist
	}

	frame := make([]byte, hdr.frameSize())
	s1, s2 := hdr.cks1, hdr.cks2
	offset := int64(walHeaderSize)
	committed := offset
	for {
		if _, err := io.ReadFull(f, frame); err != nil {
			break // clean EOF or torn tail — stop at last commit
		}
		salt1 := binary.BigEndian.Uint32(frame[8:12])
		salt2 := binary.BigEndian.Uint32(frame[12:16])
		if salt1 != hdr.salt1 || salt2 != hdr.salt2 {
			break // stale frame from a previous WAL cycle
		}
		fcks1 := binary.BigEndian.Uint32(frame[16:20])
		fcks2 := binary.BigEndian.Uint32(frame[20:24])
		// Chain: first 8 bytes of the frame header, then the page data.
		s1, s2 = walChecksum(hdr.bigEndian, s1, s2, frame[:8])
		s1, s2 = walChecksum(hdr.bigEndian, s1, s2, frame[walFrameHeaderSize:])
		if s1 != fcks1 || s2 != fcks2 {
			break // torn or corrupt frame
		}
		offset += hdr.frameSize()
		if dbSize := binary.BigEndian.Uint32(frame[4:8]); dbSize > 0 {
			committed = offset // commit frame: durable boundary
		}
	}
	return hdr, committed, nil
}
