package memory

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tunedev/rate_limiter/adapter/clock"
	"github.com/tunedev/rate_limiter/domain"
	"github.com/tunedev/rate_limiter/port"
	"github.com/tunedev/rate_limiter/storetest"
)

func TestStoreConformance(t *testing.T) {
	storetest.RunConformance(t, func(t *testing.T) storetest.Harness {
		clk := clock.NewFake(time.Unix(1_700_000_000, 0))
		s := New(clk)
		t.Cleanup(func() { _ = s.Close() })
		return storetest.Harness{Store: s, Now: clk.Now, Advance: clk.Advance}
	})
}

func TestStoreRejectsUnknownAlgorithm(t *testing.T) {
	s := New(clock.NewFake(time.Unix(1_700_000_000, 0)))
	t.Cleanup(func() { _ = s.Close() })

	out, err := s.Apply(context.Background(), port.Request{
		Key:       "subject",
		Algorithm: domain.Algorithm("nonexistent"),
		Params:    domain.Params{Limit: 1, Window: time.Second},
		Cost:      1,
	})
	if err == nil {
		t.Fatal("Apply with an unknown algorithm returned nil error, want an error")
	}
	if out != (domain.Outcome{}) {
		t.Fatalf("Outcome = %+v on error, want the zero value", out)
	}
}

// TestStorePanicDoesNotWedgeKey registers a transition that panics and shows a
// later Apply on the same key still completes. Apply releases the key's mutex
// with defer, so a panicking transition leaves the key usable.
func TestStorePanicDoesNotWedgeKey(t *testing.T) {
	clk := clock.NewFake(time.Unix(1_700_000_000, 0))
	s := New(clk, WithSweepInterval(0))
	t.Cleanup(func() { _ = s.Close() })

	const panicky = domain.Algorithm("panics")
	s.algorithms[panicky] = algorithm{
		apply: func(any, time.Time, domain.Params, int64) (any, domain.Outcome) {
			panic("boom")
		},
		lifetime: func(domain.Params) time.Duration { return time.Second },
	}

	req := port.Request{
		RuleID:    "rule",
		Key:       "subject",
		Algorithm: panicky,
		Params:    domain.Params{Limit: 1, Window: time.Second},
		Cost:      1,
	}

	func() {
		defer func() { recover() }()
		_, _ = s.Apply(context.Background(), req)
	}()

	done := make(chan struct{})
	go func() {
		defer close(done)
		req.Algorithm = domain.FixedWindow
		if _, err := s.Apply(context.Background(), req); err != nil {
			t.Errorf("Apply after a panicking transition: %v", err)
		}
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Apply on the same key never returned: the panic wedged it")
	}
}

func TestStoreSweepsExpiredEntries(t *testing.T) {
	clk := clock.NewFake(time.Unix(1_700_000_000, 0))
	s := New(clk, WithSweepInterval(0))
	t.Cleanup(func() { _ = s.Close() })

	req := port.Request{
		Key:       "subject",
		Algorithm: domain.FixedWindow,
		Params:    domain.Params{Limit: 10, Window: time.Second},
		Cost:      1,
	}
	if _, err := s.Apply(context.Background(), req); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if s.Len() != 1 {
		t.Fatalf("Len = %d, want 1", s.Len())
	}

	clk.Advance(3 * time.Second)
	s.Sweep()
	if s.Len() != 0 {
		t.Fatalf("Len = %d after sweeping an expired entry, want 0", s.Len())
	}
}

// TestStoreKeepsBurstStateUntilItCouldRefill drains a bucket whose capacity is
// ten windows of refill and idles well past the window. Expiring on the window
// alone would return a full bucket and admit ten times the rule's rate.
func TestStoreKeepsBurstStateUntilItCouldRefill(t *testing.T) {
	clk := clock.NewFake(time.Unix(1_700_000_000, 0))
	s := New(clk, WithSweepInterval(0))
	t.Cleanup(func() { _ = s.Close() })

	req := port.Request{
		RuleID:    "burst",
		Key:       "subject",
		Algorithm: domain.TokenBucket,
		Params:    domain.Params{Limit: 10, Window: time.Second, Burst: 90},
		Cost:      1,
	}

	if drain(t, s, req) != 100 {
		t.Fatal("a fresh bucket did not hold its full 100 permits")
	}

	idle := 3 * time.Second
	clk.Advance(idle)
	s.Sweep()
	if s.Len() != 1 {
		t.Fatalf("Len = %d after idling %v, want the drained bucket kept", s.Len(), idle)
	}

	accrued := drain(t, s, req)
	if want := int64(idle/req.Params.Rate()) + 1; accrued > want {
		t.Fatalf("allowed = %d after idling %v on a drained bucket, want at most %d accrued", accrued, idle, want)
	}
}

// drain takes permits until one is denied and reports how many were allowed.
func drain(t *testing.T, s *Store, req port.Request) int64 {
	t.Helper()
	var allowed int64
	for {
		out, err := s.Apply(context.Background(), req)
		if err != nil {
			t.Fatalf("Apply: %v", err)
		}
		if !out.Allowed {
			return allowed
		}
		allowed++
		if allowed > req.Params.Limit+req.Params.Burst {
			t.Fatalf("allowed %d permits from a bucket holding %d", allowed, req.Params.Limit+req.Params.Burst)
		}
	}
}

// TestStoreSweepDoesNotOverAdmit hammers Apply against Sweep on a key that has
// just expired. An entry deleted between the lookup and the lock takes a permit
// nobody counts, so another caller takes the same permit from a fresh entry.
func TestStoreSweepDoesNotOverAdmit(t *testing.T) {
	clk := clock.NewFake(time.Unix(1_700_000_000, 0))
	s := New(clk, WithSweepInterval(0))
	t.Cleanup(func() { _ = s.Close() })

	req := port.Request{
		RuleID:    "one-per-window",
		Key:       "subject",
		Algorithm: domain.FixedWindow,
		Params:    domain.Params{Limit: 1, Window: time.Second},
		Cost:      1,
	}

	const rounds, callers = 300, 8
	var allowed atomic.Int64

	for range rounds {
		clk.Advance(3 * time.Second) // past the window and past the entry's lifetime

		sweeping := make(chan struct{})
		go func() {
			for {
				select {
				case <-sweeping:
					return
				default:
					s.Sweep()
				}
			}
		}()

		var wg sync.WaitGroup
		for range callers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				out, err := s.Apply(context.Background(), req)
				if err != nil {
					t.Errorf("Apply: %v", err)
					return
				}
				if out.Allowed {
					allowed.Add(1)
				}
			}()
		}
		wg.Wait()
		close(sweeping)
	}

	if got := allowed.Load(); got != rounds {
		t.Fatalf("allowed = %d over %d rounds of one permit each, want %d", got, rounds, rounds)
	}
}

func TestStoreCloseIsIdempotent(t *testing.T) {
	s := New(clock.NewFake(time.Unix(1_700_000_000, 0)))

	if err := s.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}
