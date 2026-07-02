package jobs

import (
	"math/rand"
	"time"
)

// nextBackoff returns base*2^(attempt-1) with full jitter, capped at b.Max.
// attempt is 1-indexed (the just-finished attempt).
func nextBackoff(b Backoff, attempt int) time.Duration {
	if b.Base <= 0 {
		b.Base = 5 * time.Second
	}
	if b.Max <= 0 {
		b.Max = 1 * time.Hour
	}
	if attempt < 1 {
		attempt = 1
	}
	// Compute base * 2^(attempt-1) without overflowing.
	mult := uint(1) << uint(attempt-1)
	if mult == 0 || mult > 1<<20 { // ~17 minutes at base=1s, plenty before cap
		mult = 1 << 20
	}
	d := time.Duration(mult) * b.Base
	if d > b.Max {
		d = b.Max
	}
	// Full jitter: random value in [base, d].
	if d > b.Base {
		d = b.Base + time.Duration(rand.Int63n(int64(d-b.Base)+1))
	}
	return d
}
