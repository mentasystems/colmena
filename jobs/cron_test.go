package jobs

import (
	"testing"
	"time"
)

func TestParseCron_Errors(t *testing.T) {
	cases := []string{
		"",
		"* * * *",     // 4 fields
		"* * * * * *", // 6 fields
		"60 * * * *",  // minute out of range
		"* 24 * * *",  // hour out of range
		"* * 32 * *",  // dom out of range
		"* * * 13 *",  // month out of range
		"* * * * 7",   // dow out of range
		"a * * * *",   // bad number
		"5-3 * * * *", // inverted range
		"*/0 * * * *", // bad step
	}
	for _, expr := range cases {
		if _, err := parseCron(expr); err == nil {
			t.Errorf("parseCron(%q): expected error, got nil", expr)
		}
	}
}

func TestParseCron_Next(t *testing.T) {
	loc := time.UTC
	at := func(y, mo, d, h, mi int) time.Time {
		return time.Date(y, time.Month(mo), d, h, mi, 0, 0, loc)
	}

	cases := []struct {
		expr string
		from time.Time
		want time.Time
	}{
		// every minute
		{"* * * * *", at(2026, 4, 26, 12, 30), at(2026, 4, 26, 12, 31)},
		// hourly at :00
		{"0 * * * *", at(2026, 4, 26, 12, 30), at(2026, 4, 26, 13, 0)},
		// every 6 hours
		{"0 */6 * * *", at(2026, 4, 26, 5, 30), at(2026, 4, 26, 6, 0)},
		// every 15 minutes
		{"*/15 * * * *", at(2026, 4, 26, 12, 7), at(2026, 4, 26, 12, 15)},
		// daily at 07:00 UTC
		{"0 7 * * *", at(2026, 4, 26, 6, 59), at(2026, 4, 26, 7, 0)},
		{"0 7 * * *", at(2026, 4, 26, 7, 0), at(2026, 4, 27, 7, 0)},
		// specific weekday: Monday at 09:00 (Mon=1)
		{"0 9 * * 1", at(2026, 4, 26, 0, 0), at(2026, 4, 27, 9, 0)}, // 2026-04-26 is Sunday
		// list of minutes
		{"0,30 * * * *", at(2026, 4, 26, 12, 10), at(2026, 4, 26, 12, 30)},
		{"0,30 * * * *", at(2026, 4, 26, 12, 30), at(2026, 4, 26, 13, 0)},
		// range with step
		{"10-50/20 * * * *", at(2026, 4, 26, 12, 0), at(2026, 4, 26, 12, 10)},
		{"10-50/20 * * * *", at(2026, 4, 26, 12, 30), at(2026, 4, 26, 12, 50)},
	}

	for _, c := range cases {
		s, err := parseCron(c.expr)
		if err != nil {
			t.Fatalf("parseCron(%q): %v", c.expr, err)
		}
		got := s.Next(c.from)
		if !got.Equal(c.want) {
			t.Errorf("Next(%q from %s) = %s, want %s", c.expr, c.from, got, c.want)
		}
	}
}

func TestParseCron_DayOfMonthAndWeekOR(t *testing.T) {
	// Both restricted: Vixie cron OR-combines. "0 0 13 * 5" fires on the
	// 13th OR any Friday.
	s, err := parseCron("0 0 13 * 5")
	if err != nil {
		t.Fatalf("parseCron: %v", err)
	}
	loc := time.UTC
	// 2026-02-12 is a Thursday — next match should be Fri 2026-02-13
	from := time.Date(2026, 2, 12, 0, 0, 0, 0, loc)
	got := s.Next(from)
	want := time.Date(2026, 2, 13, 0, 0, 0, 0, loc)
	if !got.Equal(want) {
		t.Errorf("Next = %s, want %s", got, want)
	}
}

func TestNextBackoff_Bounds(t *testing.T) {
	b := Backoff{Base: 1 * time.Second, Max: 1 * time.Minute}
	for attempt := 1; attempt < 30; attempt++ {
		d := nextBackoff(b, attempt)
		if d < b.Base || d > b.Max {
			t.Errorf("attempt %d: %s out of [%s, %s]", attempt, d, b.Base, b.Max)
		}
	}
}

func TestNewID_SortableAndUnique(t *testing.T) {
	const N = 1000
	ids := make(map[string]struct{}, N)
	prev := ""
	for i := 0; i < N; i++ {
		id := newID()
		if len(id) != 26 {
			t.Fatalf("id length %d, want 26", len(id))
		}
		if _, ok := ids[id]; ok {
			t.Fatalf("collision: %s", id)
		}
		ids[id] = struct{}{}
		if i > 0 && id < prev {
			// IDs in the same millisecond may not be ordered strictly,
			// but the timestamp prefix should monotonically advance.
			// We allow same-prefix randomness to vary.
			if id[:10] < prev[:10] {
				t.Fatalf("timestamp prefix went backwards: %s -> %s", prev, id)
			}
		}
		prev = id
	}
}
