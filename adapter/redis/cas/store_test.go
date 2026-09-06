package cas

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/tunedev/rate_limiter/domain"
	"github.com/tunedev/rate_limiter/internal/redistest"
	"github.com/tunedev/rate_limiter/port"
	"github.com/tunedev/rate_limiter/storetest"
)

func TestStoreConformance(t *testing.T) {
	storetest.RunConformance(t, func(t *testing.T) storetest.Harness {
		// The attempt budget must exceed the number of concurrent contenders
		// on a key: under WATCH only one contender's EXEC commits per round,
		// so serializing N contenders takes on the order of N attempts.
		s := New(redistest.Start(t), WithMaxAttempts(2000))
		return storetest.Harness{
			Store:   s,
			Now:     func() time.Time { return storeNow(t, s) },
			Advance: func(d time.Duration) { time.Sleep(d) },
		}
	}, domain.TokenBucket, domain.FixedWindow)
}

func storeNow(t *testing.T, s *Store) time.Time {
	t.Helper()
	now, err := s.now(context.Background())
	if err != nil {
		t.Fatalf("reading the store's time: %v", err)
	}
	return now
}

func TestStoreRetriesUnderContention(t *testing.T) {
	s := New(redistest.Start(t), WithMaxAttempts(500))
	ctx := context.Background()

	req := port.Request{
		RuleID:    "rule",
		Key:       "subject",
		Algorithm: domain.TokenBucket,
		Params:    domain.Params{Limit: 200, Window: time.Hour},
		Cost:      1,
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	allowed := 0

	for range 40 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, err := s.Apply(ctx, req)
			if err != nil {
				t.Errorf("Apply: %v", err)
				return
			}
			if out.Allowed {
				mu.Lock()
				allowed++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if allowed != 40 {
		t.Fatalf("allowed = %d, want 40: every take is within the limit", allowed)
	}
	if s.Retries() == 0 {
		t.Fatal("Retries = 0 after 40 concurrent takes on one key, want the retry loop to have engaged")
	}
}

func TestStoreGivesUpAfterMaxAttempts(t *testing.T) {
	s := New(redistest.Start(t), WithMaxAttempts(0))

	out, err := s.Apply(context.Background(), port.Request{
		RuleID:    "rule",
		Key:       "subject",
		Algorithm: domain.TokenBucket,
		Params:    domain.Params{Limit: 10, Window: time.Second},
		Cost:      1,
	})
	if err == nil {
		t.Fatal("Apply with zero attempts returned nil error, want an error")
	}
	if out != (domain.Outcome{}) {
		t.Fatalf("Outcome = %+v on error, want the zero value", out)
	}
}

// TestDefaultAttemptBudgetSurvivesAHotKey drives the contention the
// conformance suite drives, 400 concurrent takes on one key, through a store
// built with no options. A default budget under that is a default that fails
// on the traffic the adapter exists for.
func TestDefaultAttemptBudgetSurvivesAHotKey(t *testing.T) {
	s := New(redistest.Start(t))
	ctx := context.Background()

	req := port.Request{
		RuleID:    "rule",
		Key:       "subject",
		Algorithm: domain.TokenBucket,
		Params:    domain.Params{Limit: 100, Window: time.Hour},
		Cost:      1,
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	allowed := 0

	for range 400 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, err := s.Apply(ctx, req)
			if err != nil {
				t.Errorf("Apply: %v", err)
				return
			}
			if out.Allowed {
				mu.Lock()
				allowed++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if allowed != 100 {
		t.Fatalf("allowed = %d, want exactly 100", allowed)
	}
}
