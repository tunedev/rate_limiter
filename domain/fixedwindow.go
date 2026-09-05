package domain

import "time"

// FixedWindowState is a count and the start of the window it counts. The zero
// value is an expired window, which resets on first use.
type FixedWindowState struct {
	Start time.Time
	Count int64
}

// FixedWindowLimiter grants Params.Limit permits per window, where windows are
// aligned to absolute time so every node agrees on the boundaries.
type FixedWindowLimiter struct{}

// Lifetime reports how long a window's count must outlive its last use: its
// own window plus slack, after which the count applies to no live window.
func (FixedWindowLimiter) Lifetime(p Params) time.Duration { return 2 * p.Window }

// Apply resets the count when now falls outside the stored window, then takes
// cost from it.
func (FixedWindowLimiter) Apply(s FixedWindowState, now time.Time, p Params, cost int64) (FixedWindowState, Outcome) {
	if p.Window <= 0 {
		return s, Outcome{}
	}

	start := now.Truncate(p.Window)
	if !s.Start.Equal(start) {
		s = FixedWindowState{Start: start}
	}

	resetAt := start.Add(p.Window)
	if s.Count+cost > p.Limit {
		return s, Outcome{
			Remaining:  max(0, p.Limit-s.Count),
			RetryAfter: resetAt.Sub(now),
			ResetAt:    resetAt,
		}
	}

	s.Count += cost
	return s, Outcome{
		Allowed:   true,
		Remaining: p.Limit - s.Count,
		ResetAt:   resetAt,
	}
}
