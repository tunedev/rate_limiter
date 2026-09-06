package domain

import (
	"testing"
	"time"
)

var swcParams = Params{Limit: 10, Window: time.Second}

func TestSlidingWindowCounterAllowsLimitThenDenies(t *testing.T) {
	var l SlidingWindowCounterLimiter
	now := time.Unix(1_700_000_000, 0)

	s := SlidingWindowCounterState{}
	for i := range 10 {
		var out Outcome
		s, out = l.Apply(s, now, swcParams, 1)
		if !out.Allowed {
			t.Fatalf("request %d denied, want allowed", i)
		}
	}

	_, out := l.Apply(s, now, swcParams, 1)
	if out.Allowed {
		t.Fatal("request 11 allowed, want denied")
	}
	if out.Remaining != 0 {
		t.Fatalf("Remaining = %d, want 0", out.Remaining)
	}
}

func TestSlidingWindowCounterRollsCurrentIntoPrevious(t *testing.T) {
	var l SlidingWindowCounterLimiter
	base := time.Unix(1_700_000_000, 0)

	s := SlidingWindowCounterState{}
	for range 10 {
		s, _ = l.Apply(s, base, swcParams, 1)
	}

	s, _ = l.Apply(s, base.Add(time.Second), swcParams, 0)
	if s.Prev != 10 {
		t.Fatalf("Prev = %d in the next window, want 10", s.Prev)
	}
	if s.Curr != 0 {
		t.Fatalf("Curr = %d in the next window, want 0", s.Curr)
	}
}

func TestSlidingWindowCounterDropsAStaleWindow(t *testing.T) {
	var l SlidingWindowCounterLimiter
	base := time.Unix(1_700_000_000, 0)

	s := SlidingWindowCounterState{}
	for range 10 {
		s, _ = l.Apply(s, base, swcParams, 1)
	}

	// Two windows on, the old count weighs nothing.
	s, out := l.Apply(s, base.Add(2*time.Second), swcParams, 1)
	if !out.Allowed {
		t.Fatal("denied two windows later, want allowed")
	}
	if s.Prev != 0 {
		t.Fatalf("Prev = %d two windows later, want 0", s.Prev)
	}
}

// TestSlidingWindowCounterUnderCountsATrailingBurst is evidence, not a bug
// report. The estimate weights the previous window uniformly. Cluster that
// window's traffic at its end and the weighting discounts requests that are in
// fact recent, so more than Limit are admitted in one window-long span.
func TestSlidingWindowCounterUnderCountsATrailingBurst(t *testing.T) {
	var l SlidingWindowCounterLimiter
	base := time.Unix(1_700_000_000, 0)
	burstAt := base.Add(900 * time.Millisecond)

	s := SlidingWindowCounterState{}
	for range 10 {
		s, _ = l.Apply(s, burstAt, swcParams, 1)
	}

	// Halfway through the next window, the estimate discounts the burst by half
	// even though every one of those requests is inside the trailing second.
	at := base.Add(1500 * time.Millisecond)
	admitted := 0
	for range 10 {
		var out Outcome
		s, out = l.Apply(s, at, swcParams, 1)
		if out.Allowed {
			admitted++
		}
	}

	if admitted != 5 {
		t.Fatalf("admitted = %d at the half-window mark, want 5", admitted)
	}
	// Everything above is inside [at-Window, at], so the true count is 15.
	if total := 10 + admitted; total <= 10 {
		t.Fatalf("total in the trailing window = %d, want more than the limit of 10", total)
	}
}

// TestSlidingWindowCounterOverCountsALeadingBurst is the same flaw in the other
// direction. Cluster the previous window's traffic at its start and the
// weighting charges for requests that have already left the trailing window, so
// requests are denied while the true count is under the limit.
func TestSlidingWindowCounterOverCountsALeadingBurst(t *testing.T) {
	var l SlidingWindowCounterLimiter
	base := time.Unix(1_700_000_000, 0)

	s := SlidingWindowCounterState{}
	for range 10 {
		s, _ = l.Apply(s, base, swcParams, 1)
	}

	// At 1500ms the trailing second is [500ms, 1500ms]. None of the ten
	// requests at 0ms fall in it, so the true count is whatever we admit now.
	at := base.Add(1500 * time.Millisecond)
	admitted := 0
	for range 10 {
		var out Outcome
		s, out = l.Apply(s, at, swcParams, 1)
		if out.Allowed {
			admitted++
		}
	}

	if admitted != 5 {
		t.Fatalf("admitted = %d, want 5", admitted)
	}
	if admitted >= 10 {
		t.Fatalf("admitted = %d, want fewer than the limit of 10: the estimate charges for requests that have left the window", admitted)
	}
}

func TestSlidingWindowCounterLifetimeCoversThePreviousWindow(t *testing.T) {
	var l SlidingWindowCounterLimiter
	p := Params{Limit: 10, Window: time.Second}

	if got, want := l.Lifetime(p), 2*p.Window; got < want {
		t.Fatalf("Lifetime = %v, want at least %v: Prev carries weight for a second window", got, want)
	}
}

// TestSlidingWindowCounterHandlesLongWindowQuotas pins the weighted term
// against overflow. At a monthly window the product of Prev and the overlap in
// nanoseconds passes MaxInt64, and a wrapped estimate reads far below the real
// count, admitting traffic a saturated window should deny.
func TestSlidingWindowCounterHandlesLongWindowQuotas(t *testing.T) {
	var l SlidingWindowCounterLimiter
	p := Params{Limit: 10_000, Window: 30 * 24 * time.Hour}
	base := truncateFromEpoch(time.Unix(1_700_000_000, 0), p.Window)

	// A saturated window rolls into the next one, where it still weighs fully.
	s := SlidingWindowCounterState{Start: base, Curr: p.Limit}

	_, out := l.Apply(s, base.Add(p.Window), p, 1)
	if out.Allowed {
		t.Fatal("allowed at the roll of a saturated month, want denied: the weighted estimate overflowed")
	}
	if out.Remaining != 0 {
		t.Fatalf("Remaining = %d, want 0", out.Remaining)
	}
}

// TestSlidingWindowCounterRetryAfterAdmitsExactlyThen checks RetryAfter against
// its own promise: retrying at exactly that instant must be admitted. Denying
// at RetryAfter would point every rejected client at the same unusable
// instant, which is the retry storm RetryAfter exists to prevent.
func TestSlidingWindowCounterRetryAfterAdmitsExactlyThen(t *testing.T) {
	var l SlidingWindowCounterLimiter
	p := Params{Limit: 10, Window: time.Second}
	base := time.Unix(1_700_000_000, 0).Truncate(p.Window)

	tests := []struct {
		name string
		s    SlidingWindowCounterState
		now  time.Time
		cost int64
	}{
		{
			name: "fresh state saturated by successive requests",
			s:    SlidingWindowCounterState{},
			now:  base,
			cost: 1, // applied 11 times below, saturating Curr at the limit
		},
		{
			name: "current window already saturated",
			s:    SlidingWindowCounterState{Start: base, Curr: p.Limit},
			now:  base.Add(400 * time.Millisecond),
			cost: 1,
		},
		{
			name: "previous window already saturated",
			s:    SlidingWindowCounterState{Start: base.Add(p.Window), Prev: p.Limit},
			now:  base.Add(p.Window),
			cost: 1,
		},
		{
			name: "partially decayed mix of both windows",
			s:    SlidingWindowCounterState{Start: base, Curr: 9, Prev: p.Limit},
			now:  base.Add(300 * time.Millisecond),
			cost: 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := tc.s
			if tc.name == "fresh state saturated by successive requests" {
				for range 10 {
					s, _ = l.Apply(s, tc.now, p, tc.cost)
				}
			}

			s, out := l.Apply(s, tc.now, p, tc.cost)
			if out.Allowed {
				t.Fatalf("allowed at %v, want denied to set up the retry", tc.now)
			}

			retryAt := tc.now.Add(out.RetryAfter)
			_, retry := l.Apply(s, retryAt, p, tc.cost)
			if !retry.Allowed {
				t.Fatalf("retry at exactly RetryAfter (%v) still denied, want admitted", retryAt)
			}
		})
	}
}
