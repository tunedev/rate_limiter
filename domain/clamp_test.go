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
