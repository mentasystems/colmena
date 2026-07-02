package jobs

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// cronSchedule represents a parsed 5-field cron expression. Each field is a
// 64-bit mask of the values it permits. Day-of-week is 7 bits (0=Sunday).
type cronSchedule struct {
	minute     uint64 // bits 0-59
	hour       uint64 // bits 0-23
	dom        uint64 // bits 1-31
	month      uint64 // bits 1-12
	dow        uint64 // bits 0-6 (Sunday=0)
	domStarred bool
	dowStarred bool
}

// parseCron parses a "minute hour day-of-month month day-of-week" expression.
// Supported syntax:
//
//   - any value
//     N           literal N
//     N-M         range (inclusive)
//     */S         step from start of range
//     N-M/S       step from N to M
//     N,M,...     comma-separated list of any of the above
//
// Unlike Vixie cron we don't support textual month/weekday names because the
// cron expressions live in the database and avoiding aliases keeps the
// canonical form unambiguous.
func parseCron(expr string) (*cronSchedule, error) {
	fields := strings.Fields(expr)
	if len(fields) != 5 {
		return nil, fmt.Errorf("expected 5 fields, got %d", len(fields))
	}
	s := &cronSchedule{}
	var err error
	if s.minute, err = parseField(fields[0], 0, 59); err != nil {
		return nil, fmt.Errorf("minute: %w", err)
	}
	if s.hour, err = parseField(fields[1], 0, 23); err != nil {
		return nil, fmt.Errorf("hour: %w", err)
	}
	if s.dom, err = parseField(fields[2], 1, 31); err != nil {
		return nil, fmt.Errorf("day-of-month: %w", err)
	}
	if s.month, err = parseField(fields[3], 1, 12); err != nil {
		return nil, fmt.Errorf("month: %w", err)
	}
	if s.dow, err = parseField(fields[4], 0, 6); err != nil {
		return nil, fmt.Errorf("day-of-week: %w", err)
	}
	s.domStarred = fields[2] == "*"
	s.dowStarred = fields[4] == "*"
	return s, nil
}

func parseField(field string, lo, hi int) (uint64, error) {
	if field == "" {
		return 0, fmt.Errorf("empty")
	}
	var mask uint64
	for _, part := range strings.Split(field, ",") {
		bits, err := parseRange(part, lo, hi)
		if err != nil {
			return 0, err
		}
		mask |= bits
	}
	return mask, nil
}

func parseRange(part string, lo, hi int) (uint64, error) {
	step := 1
	if i := strings.Index(part, "/"); i >= 0 {
		var err error
		step, err = strconv.Atoi(part[i+1:])
		if err != nil || step < 1 {
			return 0, fmt.Errorf("bad step %q", part)
		}
		part = part[:i]
	}

	rangeLo, rangeHi := lo, hi
	switch {
	case part == "*":
		// keep [lo, hi]
	case strings.Contains(part, "-"):
		idx := strings.Index(part, "-")
		a, errA := strconv.Atoi(part[:idx])
		b, errB := strconv.Atoi(part[idx+1:])
		if errA != nil || errB != nil {
			return 0, fmt.Errorf("bad range %q", part)
		}
		if a < lo || b > hi || a > b {
			return 0, fmt.Errorf("range %d-%d out of bounds [%d,%d]", a, b, lo, hi)
		}
		rangeLo, rangeHi = a, b
	default:
		v, err := strconv.Atoi(part)
		if err != nil {
			return 0, fmt.Errorf("bad value %q", part)
		}
		if v < lo || v > hi {
			return 0, fmt.Errorf("value %d out of bounds [%d,%d]", v, lo, hi)
		}
		// A single value with /step is "from v to hi step S"
		// (matches Vixie cron behaviour).
		if step > 1 {
			rangeLo, rangeHi = v, hi
		} else {
			rangeLo, rangeHi = v, v
		}
	}

	var mask uint64
	for i := rangeLo; i <= rangeHi; i += step {
		mask |= 1 << uint(i)
	}
	return mask, nil
}

// Next returns the next time strictly after t at which the schedule fires.
// Resolution is one minute (cron's natural granularity); seconds are zeroed.
func (s *cronSchedule) Next(t time.Time) time.Time {
	// Start at the next minute boundary after t.
	t = t.Add(time.Minute).Truncate(time.Minute)

	// Bound the search at five years to defend against expressions that
	// would never match (e.g. "0 0 31 2 *"). Five years covers leap-day
	// corner cases without looping forever.
	deadline := t.Add(5 * 365 * 24 * time.Hour)

	for !t.After(deadline) {
		if s.month&(1<<uint(t.Month())) == 0 {
			// Skip to first day of next month.
			y, m, _ := t.Date()
			t = time.Date(y, m+1, 1, 0, 0, 0, 0, t.Location())
			continue
		}
		if !s.dayMatches(t) {
			// Skip to next day at 00:00.
			y, m, d := t.Date()
			t = time.Date(y, m, d+1, 0, 0, 0, 0, t.Location())
			continue
		}
		if s.hour&(1<<uint(t.Hour())) == 0 {
			// Skip to next hour at :00.
			y, m, d := t.Date()
			t = time.Date(y, m, d, t.Hour()+1, 0, 0, 0, t.Location())
			continue
		}
		if s.minute&(1<<uint(t.Minute())) == 0 {
			t = t.Add(time.Minute)
			continue
		}
		return t
	}
	// Expression never matches in the bounded window. Returning the
	// deadline keeps the scheduler from busy-looping; the caller can
	// detect this with a separate sentinel if it cares.
	return deadline
}

// dayMatches handles the cron quirk that day-of-month and day-of-week are
// OR'd when both are restricted, but AND-like (any unrestricted star
// matches) when one is "*".
func (s *cronSchedule) dayMatches(t time.Time) bool {
	domMatch := s.dom&(1<<uint(t.Day())) != 0
	dowMatch := s.dow&(1<<uint(t.Weekday())) != 0
	switch {
	case s.domStarred && s.dowStarred:
		return true
	case s.domStarred:
		return dowMatch
	case s.dowStarred:
		return domMatch
	default:
		// Both restricted: traditional cron OR-combine.
		return domMatch || dowMatch
	}
}
