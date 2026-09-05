package domain

import (
	"testing"
	"time"
)

var tbParams = Params{Limit: 10, Window: time.Second}

func TestTokenBucketFreshBucketStartsFull(t *testing.T) {
	var l TokenBucketLimiter
	now := time.Unix(1_700_000_000, 0)

	s := TokenBucketState{}
	for i := range 10 {
		var out Outcome
		s, out = l.Apply(s, now, tbParams, 1)
		if !out.Allowed {
			t.Fatalf("request %d denied, want allowed", i)
		}
	}

	_, out := l.Apply(s, now, tbParams, 1)
	if out.Allowed {
		t.Fatal("request 11 allowed, want denied")
	}
	if out.Remaining != 0 {
		t.Fatalf("Remaining = %d, want 0", out.Remaining)
	}
	if out.RetryAfter != 100*time.Millisecond {
		t.Fatalf("RetryAfter = %v, want 100ms", out.RetryAfter)
	}
}

func TestTokenBucketRefillsAtRate(t *testing.T) {
	var l TokenBucketLimiter
	now := time.Unix(1_700_000_000, 0)

	s, _ := l.Apply(TokenBucketState{Tokens: 0, At: now}, now, tbParams, 1)
	if s.Tokens != 0 {
		t.Fatalf("Tokens = %d, want 0 after denied take", s.Tokens)
	}

	s, out := l.Apply(s, now.Add(350*time.Millisecond), tbParams, 1)
	if !out.Allowed {
		t.Fatal("denied after 350ms, want allowed")
	}
	if s.Tokens != 2 {
		t.Fatalf("Tokens = %d, want 2 (3 accrued, 1 taken)", s.Tokens)
	}
}

func TestTokenBucketCarriesSubTokenRemainder(t *testing.T) {
	var l TokenBucketLimiter
	now := time.Unix(1_700_000_000, 0)
	s := TokenBucketState{Tokens: 0, At: now}

	// At 100ms per token, 150ms accrues one token and leaves 50ms owed. If At
	// advanced to now instead of by the one token's worth, that 50ms would be
	// lost and the second step would accrue nothing.
	s, _ = l.Apply(s, now.Add(150*time.Millisecond), tbParams, 0)
	if s.Tokens != 1 {
		t.Fatalf("Tokens = %d after 150ms, want 1", s.Tokens)
	}

	s, _ = l.Apply(s, now.Add(200*time.Millisecond), tbParams, 0)
	if s.Tokens != 2 {
		t.Fatalf("Tokens = %d after 200ms, want 2: the 50ms remainder was dropped", s.Tokens)
	}
}

func TestTokenBucketBurstAddsCapacity(t *testing.T) {
	var l TokenBucketLimiter
	p := Params{Limit: 10, Window: time.Second, Burst: 5}
	now := time.Unix(1_700_000_000, 0)

	s := TokenBucketState{}
	allowed := 0
	for range 20 {
		var out Outcome
		s, out = l.Apply(s, now, p, 1)
		if out.Allowed {
			allowed++
		}
	}
	if allowed != 15 {
		t.Fatalf("allowed = %d, want 15 (Limit 10 plus Burst 5)", allowed)
	}
}

func TestTokenBucketBackwardClockAddsNoTokens(t *testing.T) {
	var l TokenBucketLimiter
	now := time.Unix(1_700_000_000, 0)
	s := TokenBucketState{Tokens: 0, At: now}

	s, out := l.Apply(s, now.Add(-time.Hour), tbParams, 1)
	if out.Allowed {
		t.Fatal("allowed after backward clock jump, want denied")
	}
	if s.Tokens != 0 {
		t.Fatalf("Tokens = %d, want 0", s.Tokens)
	}
}

func TestTokenBucketForwardJumpGrantsAtMostOneWindow(t *testing.T) {
	var l TokenBucketLimiter
	now := time.Unix(1_700_000_000, 0)
	s := TokenBucketState{Tokens: 0, At: now}

	s, _ = l.Apply(s, now.Add(24*time.Hour), tbParams, 0)
	if s.Tokens != 10 {
		t.Fatalf("Tokens = %d, want 10 (capacity), not unbounded", s.Tokens)
	}
}
