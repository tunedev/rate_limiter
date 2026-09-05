package domain

import (
	"testing"
	"time"
)

var fwParams = Params{Limit: 5, Window: time.Second}

func TestFixedWindowAllowsLimitThenDenies(t *testing.T) {
	var l FixedWindowLimiter
	now := time.Unix(1_700_000_000, 0)

	s := FixedWindowState{}
	for i := range 5 {
		var out Outcome
		s, out = l.Apply(s, now, fwParams, 1)
		if !out.Allowed {
			t.Fatalf("request %d denied, want allowed", i)
		}
	}

	_, out := l.Apply(s, now, fwParams, 1)
	if out.Allowed {
		t.Fatal("request 6 allowed, want denied")
	}
	if out.RetryAfter != time.Second {
		t.Fatalf("RetryAfter = %v, want 1s (to the next boundary)", out.RetryAfter)
	}
}

func TestFixedWindowResetsOnBoundary(t *testing.T) {
	var l FixedWindowLimiter
	now := time.Unix(1_700_000_000, 0)

	s := FixedWindowState{}
	for range 5 {
		s, _ = l.Apply(s, now, fwParams, 1)
	}

	s, out := l.Apply(s, now.Add(time.Second), fwParams, 1)
	if !out.Allowed {
		t.Fatal("denied in the next window, want allowed")
	}
	if s.Count != 1 {
		t.Fatalf("Count = %d, want 1 after reset", s.Count)
	}
}

func TestFixedWindowAlignsToAbsoluteTime(t *testing.T) {
	var l FixedWindowLimiter
	// First use lands mid-window; the window still starts at the second mark.
	now := time.Unix(1_700_000_000, 0).Add(700 * time.Millisecond)

	s, out := l.Apply(FixedWindowState{}, now, fwParams, 1)
	if want := time.Unix(1_700_000_000, 0); !s.Start.Equal(want) {
		t.Fatalf("Start = %v, want %v", s.Start, want)
	}
	if want := now.Add(300 * time.Millisecond); !out.ResetAt.Equal(want) {
		t.Fatalf("ResetAt = %v, want %v", out.ResetAt, want)
	}
}

// TestFixedWindowAdmitsTwiceTheLimitAtABoundary is evidence, not a bug report.
// Clustering requests either side of a seam admits 2x Limit inside a span
// shorter than one window. This is the weakness sliding window algorithms fix.
func TestFixedWindowAdmitsTwiceTheLimitAtABoundary(t *testing.T) {
	var l FixedWindowLimiter
	boundary := time.Unix(1_700_000_001, 0)

	s := FixedWindowState{}
	allowed := 0
	for range 5 {
		var out Outcome
		s, out = l.Apply(s, boundary.Add(-100*time.Millisecond), fwParams, 1)
		if out.Allowed {
			allowed++
		}
	}
	for range 5 {
		var out Outcome
		s, out = l.Apply(s, boundary.Add(100*time.Millisecond), fwParams, 1)
		if out.Allowed {
			allowed++
		}
	}

	if allowed != 10 {
		t.Fatalf("allowed = %d across the seam, want 10 (twice a limit of 5)", allowed)
	}
}
