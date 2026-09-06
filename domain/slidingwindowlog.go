package domain

import "time"

// SlidingWindowLogState is the instants of the admitted requests still inside
// the window, oldest first. The zero value is an empty log. Every state Apply
// returns has cap(Events) == len(Events), which is what lets the deny path
// share Events with the input instead of copying it.
type SlidingWindowLogState struct {
	Events []time.Time
}

// SlidingWindowLogLimiter admits while fewer than Params.Limit requests fall in
// the window ending now. It counts a true sliding window rather than estimating
// one, and holds one instant per admitted permit to do it.
type SlidingWindowLogLimiter struct{}

// Lifetime reports how long a log must outlive its last use: its window plus
// slack, after which every event it holds has fallen out.
func (SlidingWindowLogLimiter) Lifetime(p Params) time.Duration { return 2 * p.Window }

// Apply drops the events that have fallen out of the window, then appends cost
// instants if there is room. Denied requests are not recorded, so the log never
// grows past Limit. The state passed in is left unchanged.
func (SlidingWindowLogLimiter) Apply(s SlidingWindowLogState, now time.Time, p Params, cost int64) (SlidingWindowLogState, Outcome) {
	if p.Window <= 0 {
		return s, Outcome{}
	}

	cutoff := now.Add(-p.Window)
	first := 0
	for first < len(s.Events) && !s.Events[first].After(cutoff) {
		first++
	}
	live := s.Events[first:]

	if int64(len(live))+cost > p.Limit {
		// A denial appends nothing, so the pruned view is shared rather than
		// copied. Copying here would let a flood allocate a slice per rejected
		// request.
		return SlidingWindowLogState{Events: live}, Outcome{
			Remaining:  max(0, p.Limit-int64(len(live))),
			RetryAfter: logRetryAfter(live, now, p, cost),
			ResetAt:    logResetAt(live, now, p),
		}
	}

	next := make([]time.Time, len(live), len(live)+int(cost))
	copy(next, live)
	for range cost {
		next = append(next, now)
	}
	return SlidingWindowLogState{Events: next}, Outcome{
		Allowed:   true,
		Remaining: p.Limit - int64(len(next)),
		ResetAt:   logResetAt(next, now, p),
	}
}

// logRetryAfter reports when enough of the oldest events will have fallen out
// to make room for cost. A cost larger than Limit can never fit, so it waits
// for the whole window.
func logRetryAfter(live []time.Time, now time.Time, p Params, cost int64) time.Duration {
	if len(live) == 0 {
		return p.Window
	}
	need := min(int64(len(live)), int64(len(live))+cost-p.Limit)
	return live[need-1].Add(p.Window).Sub(now)
}

// logResetAt reports when the log is empty again, which is one window past its
// newest event.
func logResetAt(events []time.Time, now time.Time, p Params) time.Time {
	if len(events) == 0 {
		return now
	}
	return events[len(events)-1].Add(p.Window)
}
