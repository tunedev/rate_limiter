package domain

import (
	"testing"
	"time"
)

var swlParams = Params{Limit: 5, Window: time.Second}

func TestSlidingWindowLogAllowsLimitThenDenies(t *testing.T) {
	var l SlidingWindowLogLimiter
	now := time.Unix(1_700_000_000, 0)

	s := SlidingWindowLogState{}
	for i := range 5 {
		var out Outcome
		s, out = l.Apply(s, now, swlParams, 1)
		if !out.Allowed {
			t.Fatalf("request %d denied, want allowed", i)
		}
	}

	_, out := l.Apply(s, now, swlParams, 1)
	if out.Allowed {
		t.Fatal("request 6 allowed, want denied")
	}
	if out.RetryAfter != time.Second {
		t.Fatalf("RetryAfter = %v, want 1s (until the oldest event falls out)", out.RetryAfter)
	}
}

func TestSlidingWindowLogSlidesRatherThanResetting(t *testing.T) {
	var l SlidingWindowLogLimiter
	now := time.Unix(1_700_000_000, 0)

	s := SlidingWindowLogState{}
	for i := range 5 {
		s, _ = l.Apply(s, now.Add(time.Duration(i)*100*time.Millisecond), swlParams, 1)
	}

	// 600ms in, the oldest event is 600ms old and still inside the window.
	_, out := l.Apply(s, now.Add(600*time.Millisecond), swlParams, 1)
	if out.Allowed {
		t.Fatal("allowed at 600ms, want denied: all five events are still live")
	}
	if want := 400 * time.Millisecond; out.RetryAfter != want {
		t.Fatalf("RetryAfter = %v, want %v", out.RetryAfter, want)
	}

	// At 1050ms the first event has fallen out and exactly one slot is free.
	s, out = l.Apply(s, now.Add(1050*time.Millisecond), swlParams, 1)
	if !out.Allowed {
		t.Fatal("denied at 1050ms, want allowed: the oldest event has fallen out")
	}
	if out.Remaining != 0 {
		t.Fatalf("Remaining = %d, want 0", out.Remaining)
	}
}

func TestSlidingWindowLogDoesNotRecordDenials(t *testing.T) {
	var l SlidingWindowLogLimiter
	now := time.Unix(1_700_000_000, 0)

	s := SlidingWindowLogState{}
	for range 5 {
		s, _ = l.Apply(s, now, swlParams, 1)
	}
	for range 1000 {
		s, _ = l.Apply(s, now, swlParams, 1)
	}

	if len(s.Events) != 5 {
		t.Fatalf("len(Events) = %d after 1000 denials, want 5: a flood must not grow the log", len(s.Events))
	}
}

func TestSlidingWindowLogDoesNotMutateInputState(t *testing.T) {
	var l SlidingWindowLogLimiter
	now := time.Unix(1_700_000_000, 0)

	// Spare capacity so an implementation that appended to the input slice in
	// place, rather than copying it, would not need to reallocate and so
	// would go undetected by the checks below alone.
	backing := make([]time.Time, 1, 8)
	backing[0] = now
	before := SlidingWindowLogState{Events: backing}
	beyond := backing[len(backing):cap(backing)]
	kept := before.Events[0]

	if _, _ = l.Apply(before, now.Add(10*time.Millisecond), swlParams, 1); !before.Events[0].Equal(kept) {
		t.Fatalf("Apply mutated the state it was given: Events[0] = %v, want %v", before.Events[0], kept)
	}
	if len(before.Events) != 1 {
		t.Fatalf("len(Events) = %d after Apply, want 1", len(before.Events))
	}
	if !beyond[0].IsZero() {
		t.Fatalf("Apply wrote into the input slice's spare capacity: got %v, want the zero value", beyond[0])
	}
}

// TestSlidingWindowLogCostsOneTimestampPerPermit is evidence, not a bug report.
// State grows with Limit, where fixed window holds a single counter whatever
// the limit is. That is what a sliding window log buys accuracy with.
func TestSlidingWindowLogCostsOneTimestampPerPermit(t *testing.T) {
	var l SlidingWindowLogLimiter
	now := time.Unix(1_700_000_000, 0)

	for _, limit := range []int64{10, 1000, 10_000} {
		p := Params{Limit: limit, Window: time.Hour}

		s := SlidingWindowLogState{}
		for range limit {
			s, _ = l.Apply(s, now, p, 1)
		}

		if int64(len(s.Events)) != limit {
			t.Fatalf("limit %d: len(Events) = %d, want %d", limit, len(s.Events), limit)
		}
	}
}
