package jobs

import (
	"crypto/rand"
	"encoding/binary"
	"time"
)

// crockfordAlphabet is Crockford's base32 alphabet used by ULID. It is
// case-insensitive, removes ambiguous characters (I, L, O, U), and preserves
// lexicographic order: encoded bytes sort the same as the underlying integers.
const crockfordAlphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

// newID returns a 26-character lexicographically-sortable identifier.
//
// Layout matches ULID:
//   - 48 bits unsigned millisecond timestamp (big-endian)
//   - 80 bits cryptographic randomness
//
// 26 base32 characters cover 130 bits; the top 2 bits are always zero so the
// first character is one of {0..7}. We do not depend on the oklog/ulid
// package because crypto/rand plus a tiny encoder is enough.
func newID() string {
	var raw [16]byte
	ms := uint64(time.Now().UnixMilli())
	binary.BigEndian.PutUint64(raw[0:8], ms<<16) // shift so only top 6 bytes carry the ms
	if _, err := rand.Read(raw[6:]); err != nil {
		// crypto/rand failing on a healthy host is unrecoverable; the
		// alternative is silently issuing predictable IDs, which we won't.
		panic("colmena/jobs: crypto/rand failed: " + err.Error())
	}

	// Encode 16 bytes (128 bits) as 26 base32 chars by treating the input
	// as a 130-bit integer with two leading zero bits.
	var enc [26]byte
	enc[0] = crockfordAlphabet[(raw[0]&0xE0)>>5] // top 3 bits of byte 0
	enc[1] = crockfordAlphabet[raw[0]&0x1F]
	enc[2] = crockfordAlphabet[(raw[1]&0xF8)>>3]
	enc[3] = crockfordAlphabet[((raw[1]&0x07)<<2)|((raw[2]&0xC0)>>6)]
	enc[4] = crockfordAlphabet[(raw[2]&0x3E)>>1]
	enc[5] = crockfordAlphabet[((raw[2]&0x01)<<4)|((raw[3]&0xF0)>>4)]
	enc[6] = crockfordAlphabet[((raw[3]&0x0F)<<1)|((raw[4]&0x80)>>7)]
	enc[7] = crockfordAlphabet[(raw[4]&0x7C)>>2]
	enc[8] = crockfordAlphabet[((raw[4]&0x03)<<3)|((raw[5]&0xE0)>>5)]
	enc[9] = crockfordAlphabet[raw[5]&0x1F]
	enc[10] = crockfordAlphabet[(raw[6]&0xF8)>>3]
	enc[11] = crockfordAlphabet[((raw[6]&0x07)<<2)|((raw[7]&0xC0)>>6)]
	enc[12] = crockfordAlphabet[(raw[7]&0x3E)>>1]
	enc[13] = crockfordAlphabet[((raw[7]&0x01)<<4)|((raw[8]&0xF0)>>4)]
	enc[14] = crockfordAlphabet[((raw[8]&0x0F)<<1)|((raw[9]&0x80)>>7)]
	enc[15] = crockfordAlphabet[(raw[9]&0x7C)>>2]
	enc[16] = crockfordAlphabet[((raw[9]&0x03)<<3)|((raw[10]&0xE0)>>5)]
	enc[17] = crockfordAlphabet[raw[10]&0x1F]
	enc[18] = crockfordAlphabet[(raw[11]&0xF8)>>3]
	enc[19] = crockfordAlphabet[((raw[11]&0x07)<<2)|((raw[12]&0xC0)>>6)]
	enc[20] = crockfordAlphabet[(raw[12]&0x3E)>>1]
	enc[21] = crockfordAlphabet[((raw[12]&0x01)<<4)|((raw[13]&0xF0)>>4)]
	enc[22] = crockfordAlphabet[((raw[13]&0x0F)<<1)|((raw[14]&0x80)>>7)]
	enc[23] = crockfordAlphabet[(raw[14]&0x7C)>>2]
	enc[24] = crockfordAlphabet[((raw[14]&0x03)<<3)|((raw[15]&0xE0)>>5)]
	enc[25] = crockfordAlphabet[raw[15]&0x1F]
	return string(enc[:])
}
