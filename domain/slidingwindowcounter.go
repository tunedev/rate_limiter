package domain

import (
	"math"
	"time"
)

// SlidingWindowCounterState is the current window's start and the counts for it
// and the window before it. The zero value is an expired pair, which resets on
// first use.
type SlidingWindowCounterState struct {
	Start time.Time
	Prev  int64
	Curr  int64
}

// SlidingWindowCounterLimiter estimates the count in the trailing window as the
// current window's count plus the previous window's, weighted by how much of it
// the trailing window still covers. It costs two counters instead of one
// timestamp per permit, and is exact only when the previous window's traffic
// was spread evenly through it.
type SlidingWindowCounterLimiter struct{}

// Lifetime reports how long a counter pair must outlive its last use. Prev
// still carries weight through the whole of the current window, so the pair
// matters for two windows, plus slack.
func (SlidingWindowCounterLimiter) Lifetime(p Params) time.Duration { return 3 * p.Window }

// Apply rolls the window forward if now has left the stored one, then takes
// cost against the weighted estimate.
func (SlidingWindowCounterLimiter) Apply(s SlidingWindowCounterState, now time.Time, p Params, cost int64) (SlidingWindowCounterState, Outcome) {
	if p.Window <= 0 {
		return s, Outcome{}
	}

	start := truncateFromEpoch(now, p.Window)
	switch {
	case s.Start.Equal(start):
	case s.Start.Add(p.Window).Equal(start):
		s = SlidingWindowCounterState{Start: start, Prev: s.Curr}
	default:
		s = SlidingWindowCounterState{Start: start}
	}

	overlap := p.Window - now.Sub(start)
	weighted := int64(float64(s.Prev) * float64(overlap) / float64(p.Window))
	estimate := s.Curr + weighted

	if estimate+cost > p.Limit {
		return s, Outcome{
			Remaining:  max(0, p.Limit-estimate),
			RetryAfter: counterRetryAfter(s, now, start, p, cost),
			ResetAt:    counterResetAt(s, start, now, p),
		}
	}

	s.Curr += cost
	return s, Outcome{
		Allowed:   true,
		Remaining: p.Limit - (estimate + cost),
		ResetAt:   counterResetAt(s, start, now, p),
	}
}

// counterRetryAfter reports the first instant at which cost would be admitted
// against s: the first t with estimate(t)+cost <= Limit. The estimate decays
// from Curr+Prev to Curr across the current window, then from Curr to 0 across
// the next, so which span holds the solution depends on where the target
// falls against Curr. Computed in float64 like the weighted term above, for
// the same overflow reason, and rounded up to the next whole nanosecond so the
// returned instant is never one truncation short of admission.
func counterRetryAfter(s SlidingWindowCounterState, now, start time.Time, p Params, cost int64) time.Duration {
	target := p.Limit - cost
	windowNs := float64(p.Window)

	var offset float64
	switch {
	case target >= s.Curr:
		// Prev > 0 here: with Prev == 0 the estimate is the constant Curr, and
		// target >= Curr would already have been admitted.
		offset = windowNs - float64(target-s.Curr)*windowNs/float64(s.Prev)
	case target <= 0:
		offset = 2 * windowNs
	default:
		offset = 2*windowNs - float64(target)*windowNs/float64(s.Curr)
	}

	t := start.Add(time.Duration(math.Ceil(offset)))
	return t.Sub(now)
}

// counterResetAt reports when capacity is genuinely full again: two windows
// out while the current window still holds a count, one window out while only
// the previous one does, and now when both are already empty.
func counterResetAt(s SlidingWindowCounterState, start, now time.Time, p Params) time.Time {
	switch {
	case s.Curr > 0:
		return start.Add(2 * p.Window)
	case s.Prev > 0:
		return start.Add(p.Window)
	default:
		return now
	}
}
