// Package storetest holds the contract every port.Store adapter must satisfy.
// An adapter supplies a Harness so the suite can drive it without assuming how
// the store learns the time.
package storetest

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/tunedev/rate_limiter/domain"
	"github.com/tunedev/rate_limiter/port"
)

// Harness is one empty store plus access to the time that store answers from.
// A memory adapter supplies a fake clock's Now and Advance; a Redis adapter
// supplies a real clock read and a real sleep.
type Harness struct {
	Store   port.Store
	Now     func() time.Time      // the store's own notion of now
	Advance func(d time.Duration) // moves the store's clock forward
}

// window is the window every conformance request uses.
const window = time.Second

func req(key domain.Key) port.Request {
	return port.Request{
		Key:       key,
		Algorithm: domain.FixedWindow,
		Params:    domain.Params{Limit: 100, Window: window},
		Cost:      1,
	}
}

// boundary returns the end of the fixed window containing t.
func boundary(t time.Time) time.Time { return t.Truncate(window).Add(window) }

// RunConformance asserts the port.Store contract against newHarness.
func RunConformance(t *testing.T, newHarness func(t *testing.T) Harness) {
	t.Helper()
	t.Run("clause 1: atomic per key", func(t *testing.T) { clauseAtomicPerKey(t, newHarness) })
	t.Run("clause 2: keys are independent", func(t *testing.T) { clauseKeysIndependent(t, newHarness) })
	t.Run("clause 3: store owns the clock", func(t *testing.T) { clauseStoreOwnsClock(t, newHarness) })
	t.Run("clause 4: missing state is full capacity", func(t *testing.T) { clauseMissingStateIsFull(t, newHarness) })
	t.Run("clause 5: errors carry no policy", func(t *testing.T) { clauseErrorsCarryNoPolicy(t, newHarness) })
}

// clauseAtomicPerKey drives 400 concurrent takes at a limit of 100 and asserts
// exactly 100 are allowed. A lost update shows up as more than 100.
func clauseAtomicPerKey(t *testing.T, newHarness func(t *testing.T) Harness) {
	h := newHarness(t)
	ctx := context.Background()

	var mu sync.Mutex
	allowed := 0

	var wg sync.WaitGroup
	for range 400 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, err := h.Store.Apply(ctx, req("subject"))
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

func clauseKeysIndependent(t *testing.T, newHarness func(t *testing.T) Harness) {
	h := newHarness(t)
	ctx := context.Background()

	for range 100 {
		if _, err := h.Store.Apply(ctx, req("a")); err != nil {
			t.Fatalf("Apply: %v", err)
		}
	}

	out, err := h.Store.Apply(ctx, req("a"))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if out.Allowed {
		t.Fatal("key a still allowed after 100 takes, want denied")
	}

	out, err = h.Store.Apply(ctx, req("b"))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if !out.Allowed {
		t.Fatal("key b denied, want allowed: exhausting one key must not affect another")
	}
}

// clauseStoreOwnsClock asserts ResetAt lands on the window boundary derived
// from the store's own time, and moves when that time moves. A caller's clock
// never enters the calculation.
func clauseStoreOwnsClock(t *testing.T, newHarness func(t *testing.T) Harness) {
	h := newHarness(t)
	ctx := context.Background()

	before := h.Now()
	first, err := h.Store.Apply(ctx, req("subject"))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if lo, hi := boundary(before), boundary(h.Now()); first.ResetAt.Before(lo) || first.ResetAt.After(hi) {
		t.Fatalf("ResetAt = %v, want a window boundary derived from the store's own time, within [%v, %v]", first.ResetAt, lo, hi)
	}

	h.Advance(2 * window)

	before = h.Now()
	second, err := h.Store.Apply(ctx, req("subject"))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if lo, hi := boundary(before), boundary(h.Now()); second.ResetAt.Before(lo) || second.ResetAt.After(hi) {
		t.Fatalf("ResetAt = %v after advancing the store's time, want a boundary within [%v, %v]", second.ResetAt, lo, hi)
	}
	if !second.ResetAt.After(first.ResetAt) {
		t.Fatalf("ResetAt = %v after advancing the store's time by %v, want later than %v", second.ResetAt, 2*window, first.ResetAt)
	}
}

func clauseMissingStateIsFull(t *testing.T, newHarness func(t *testing.T) Harness) {
	h := newHarness(t)
	ctx := context.Background()

	out, err := h.Store.Apply(ctx, req("never-seen"))
	if err != nil {
		t.Fatalf("Apply on an unknown key: %v", err)
	}
	if !out.Allowed {
		t.Fatal("unknown key denied, want allowed: missing state is full capacity")
	}
	if out.Remaining != 99 {
		t.Fatalf("Remaining = %d, want 99", out.Remaining)
	}
}

// clauseErrorsCarryNoPolicy asserts a cancelled context yields an error and a
// zero Outcome, never an Allowed one.
func clauseErrorsCarryNoPolicy(t *testing.T, newHarness func(t *testing.T) Harness) {
	h := newHarness(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	out, err := h.Store.Apply(ctx, req("subject"))
	if err == nil {
		t.Fatal("Apply with a cancelled context returned nil error, want an error")
	}
	if out != (domain.Outcome{}) {
		t.Fatalf("Outcome = %+v on error, want the zero value", out)
	}
}
