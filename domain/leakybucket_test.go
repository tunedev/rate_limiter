package domain

import (
	"testing"
	"time"
)

var lbParams = Params{Limit: 10, Window: time.Second}

func TestLeakyBucketFillsThenRejects(t *testing.T) {
	var l LeakyBucketLimiter
	now := time.Unix(1_700_000_000, 0)

	s := LeakyBucketState{}
	for i := range 10 {
		var out Outcome
		s, out = l.Apply(s, now, lbParams, 1)
		if !out.Allowed {
			t.Fatalf("request %d denied, want allowed", i)
		}
	}
	if s.Depth != 10 {
		t.Fatalf("Depth = %d, want 10", s.Depth)
	}

	_, out := l.Apply(s, now, lbParams, 1)
	if out.Allowed {
		t.Fatal("request 11 allowed, want denied")
	}
	if out.RetryAfter != 100*time.Millisecond {
		t.Fatalf("RetryAfter = %v, want 100ms", out.RetryAfter)
	}
}

func TestLeakyBucketDrainsAtRate(t *testing.T) {
	var l LeakyBucketLimiter
	now := time.Unix(1_700_000_000, 0)

	s := LeakyBucketState{Depth: 10, At: now}
	s, out := l.Apply(s, now.Add(350*time.Millisecond), lbParams, 1)
	if !out.Allowed {
		t.Fatal("denied after 350ms of draining, want allowed")
	}
	if s.Depth != 8 {
		t.Fatalf("Depth = %d, want 8 (3 drained, 1 added)", s.Depth)
	}
}

func TestLeakyBucketCarriesSubUnitRemainder(t *testing.T) {
	var l LeakyBucketLimiter
	now := time.Unix(1_700_000_000, 0)
	s := LeakyBucketState{Depth: 10, At: now}

	// At 100ms per unit, 150ms drains one and leaves 50ms owed. Snapping At to
	// now would drop that 50ms and the second step would drain nothing.
	s, _ = l.Apply(s, now.Add(150*time.Millisecond), lbParams, 0)
	if s.Depth != 9 {
		t.Fatalf("Depth = %d after 150ms, want 9", s.Depth)
	}

	s, _ = l.Apply(s, now.Add(200*time.Millisecond), lbParams, 0)
	if s.Depth != 8 {
		t.Fatalf("Depth = %d after 200ms, want 8: the 50ms remainder was dropped", s.Depth)
	}
}

func TestLeakyBucketBackwardClockDrainsNothing(t *testing.T) {
	var l LeakyBucketLimiter
	now := time.Unix(1_700_000_000, 0)

	s, out := l.Apply(LeakyBucketState{Depth: 10, At: now}, now.Add(-time.Hour), lbParams, 1)
	if out.Allowed {
		t.Fatal("allowed after a backward clock jump, want denied")
	}
	if s.Depth != 10 {
		t.Fatalf("Depth = %d, want 10", s.Depth)
	}
}

// TestLeakyBucketMatchesTokenBucket is the evidence for docs/decisions/0001.
// Under Depth = capacity - Tokens the two transitions are the same function,
// so one event sequence must produce identical Outcomes from both. A
// divergence means one of them has a bug.
func TestLeakyBucketMatchesTokenBucket(t *testing.T) {
	var leaky LeakyBucketLimiter
	var token TokenBucketLimiter

	base := time.Unix(1_700_000_000, 0)
	steps := []struct {
		after time.Duration
		cost  int64
	}{
		{0, 1}, {0, 3}, {0, 6}, {0, 1},
		{120 * time.Millisecond, 1},
		{0, 1},
		{450 * time.Millisecond, 4},
		{time.Second, 10},
		{0, 1},
		{-time.Hour, 1},
		{24 * time.Hour, 10},
		{0, 1},
	}

	for _, p := range []Params{
		{Limit: 10, Window: time.Second},
		{Limit: 10, Window: time.Second, Burst: 5},
		{Limit: 3, Window: 300 * time.Millisecond},
	} {
		ls := LeakyBucketState{}
		ts := TokenBucketState{}
		now := base

		for i, step := range steps {
			now = now.Add(step.after)

			var lo, to Outcome
			ls, lo = leaky.Apply(ls, now, p, step.cost)
			ts, to = token.Apply(ts, now, p, step.cost)

			if lo != to {
				t.Fatalf("params %+v step %d (cost %d): leaky = %+v, token = %+v", p, i, step.cost, lo, to)
			}
			if got, want := ls.Depth, p.Limit+p.Burst-ts.Tokens; got != want {
				t.Fatalf("params %+v step %d: Depth = %d, want capacity-Tokens = %d", p, i, got, want)
			}
		}
	}
}

func TestLeakyBucketLifetimeCoversDrain(t *testing.T) {
	var l LeakyBucketLimiter
	p := Params{Limit: 10, Window: time.Second, Burst: 90}

	if got, want := l.Lifetime(p), time.Duration(p.Limit+p.Burst)*p.Rate(); got < want {
		t.Fatalf("Lifetime = %v, want at least %v to drain from full", got, want)
	}
}
