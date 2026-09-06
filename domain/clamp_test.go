package domain

import (
	"testing"
	"time"
)

func TestClampElapsedBoundsClockJumps(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	const window = time.Second

	cases := []struct {
		name string
		prev time.Time
		now  time.Time
		want time.Duration
	}{
		{"normal progress", base, base.Add(250 * time.Millisecond), 250 * time.Millisecond},
		{"backward clock yields zero", base, base.Add(-5 * time.Second), 0},
		{"forward jump caps at one window", base, base.Add(time.Hour), window},
		{"exactly one window", base, base.Add(window), window},
		{"no progress", base, base, 0},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := clampElapsed(c.prev, c.now, window); got != c.want {
				t.Fatalf("clampElapsed = %v, want %v", got, c.want)
			}
		})
	}
}

func TestTruncateFromEpochMatchesUnixBoundaries(t *testing.T) {
	base := time.Unix(1_700_000_000, 0).UTC()

	cases := []struct {
		name   string
		window time.Duration
		want   time.Time
	}{
		{"second", time.Second, time.Unix(1_700_000_000, 0).UTC()},
		{"minute", time.Minute, time.Unix(1_699_999_980, 0).UTC()},
		{"hour", time.Hour, time.Unix(1_699_999_200, 0).UTC()},
		{"day", 24 * time.Hour, time.Unix(1_699_920_000, 0).UTC()},
		{"week", 7 * 24 * time.Hour, time.Unix(1_699_488_000, 0).UTC()},
		{"thirty days", 30 * 24 * time.Hour, time.Unix(1_697_760_000, 0).UTC()},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := truncateFromEpoch(base, c.window)
			if !got.Equal(c.want) {
				t.Fatalf("truncateFromEpoch = %v, want %v", got.UTC(), c.want)
			}
			if got.UnixNano()%int64(c.window) != 0 {
				t.Fatalf("result %v is not a whole number of %v from the epoch", got.UTC(), c.window)
			}
		})
	}
}

// TestTruncateFromEpochDivergesFromTruncateOnlyForMultiDayWindows records why
// this helper exists. Windows that divide the offset between the zero time and
// the epoch land identically either way; longer ones do not.
func TestTruncateFromEpochDivergesFromTruncateOnlyForMultiDayWindows(t *testing.T) {
	base := time.Unix(1_700_000_000, 0).UTC()

	for _, w := range []time.Duration{time.Second, time.Minute, time.Hour, 24 * time.Hour} {
		if got, want := truncateFromEpoch(base, w), base.Truncate(w); !got.Equal(want) {
			t.Fatalf("window %v: truncateFromEpoch = %v, time.Truncate = %v, want agreement", w, got, want)
		}
	}

	for _, w := range []time.Duration{7 * 24 * time.Hour, 30 * 24 * time.Hour} {
		if truncateFromEpoch(base, w).Equal(base.Truncate(w)) {
			t.Fatalf("window %v: expected the two truncations to differ", w)
		}
	}
}

func TestTruncateFromEpochHandlesNonPositiveWindow(t *testing.T) {
	base := time.Unix(1_700_000_000, 0).UTC()
	if got := truncateFromEpoch(base, 0); !got.Equal(base) {
		t.Fatalf("truncateFromEpoch with a zero window = %v, want %v unchanged", got, base)
	}
}
