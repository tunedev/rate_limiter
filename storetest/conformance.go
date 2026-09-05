// Package storetest holds the contract every port.Store adapter must satisfy.
// It sits outside port/ because it needs a controllable clock adapter, and
// nothing under port/ may depend outward.
package storetest

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/tunedev/rate_limiter/adapter/clock"
	"github.com/tunedev/rate_limiter/domain"
	"github.com/tunedev/rate_limiter/port"
)

// Factory builds a store bound to clk. Each call returns an empty store.
type Factory func(t *testing.T, clk port.Clock) port.Store

var base = time.Unix(1_700_000_000, 0)

func req(key domain.Key) port.Request {
	return port.Request{
		Key:       key,
		Algorithm: domain.FixedWindow,
		Params:    domain.Params{Limit: 100, Window: time.Second},
		Cost:      1,
	}
}

// RunConformance asserts the port.Store contract against newStore.
func RunConformance(t *testing.T, newStore Factory) {
	t.Helper()
	t.Run("clause 1: atomic per key", func(t *testing.T) { clauseAtomicPerKey(t, newStore) })
	t.Run("clause 2: keys are independent", func(t *testing.T) { clauseKeysIndependent(t, newStore) })
	t.Run("clause 3: store owns the clock", func(t *testing.T) { clauseStoreOwnsClock(t, newStore) })
	t.Run("clause 4: missing state is full capacity", func(t *testing.T) { clauseMissingStateIsFull(t, newStore) })
	t.Run("clause 5: errors carry no policy", func(t *testing.T) { clauseErrorsCarryNoPolicy(t, newStore) })
}

// clauseAtomicPerKey drives 400 concurrent takes at a limit of 100 and asserts
// exactly 100 are allowed. A lost update shows up as more than 100.
func clauseAtomicPerKey(t *testing.T, newStore Factory) {
	s := newStore(t, clock.NewFake(base))
	ctx := context.Background()

	var mu sync.Mutex
	allowed := 0

	var wg sync.WaitGroup
	for range 400 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, err := s.Apply(ctx, req("subject"))
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

func clauseKeysIndependent(t *testing.T, newStore Factory) {
	s := newStore(t, clock.NewFake(base))
	ctx := context.Background()

	for range 100 {
		if _, err := s.Apply(ctx, req("a")); err != nil {
			t.Fatalf("Apply: %v", err)
		}
	}

	out, err := s.Apply(ctx, req("a"))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if out.Allowed {
		t.Fatal("key a still allowed after 100 takes, want denied")
	}

	out, err = s.Apply(ctx, req("b"))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if !out.Allowed {
		t.Fatal("key b denied, want allowed: exhausting one key must not affect another")
	}
}

// clauseStoreOwnsClock asserts ResetAt is derived from the store's clock and
// not from the caller's, by moving the store's clock and nothing else.
func clauseStoreOwnsClock(t *testing.T, newStore Factory) {
	clk := clock.NewFake(base)
	s := newStore(t, clk)
	ctx := context.Background()

	out, err := s.Apply(ctx, req("subject"))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if want := base.Add(time.Second); !out.ResetAt.Equal(want) {
		t.Fatalf("ResetAt = %v, want %v", out.ResetAt, want)
	}

	clk.Advance(30 * time.Second)
	out, err = s.Apply(ctx, req("subject"))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if want := base.Add(31 * time.Second); !out.ResetAt.Equal(want) {
		t.Fatalf("ResetAt = %v, want %v after advancing the store clock", out.ResetAt, want)
	}
}

func clauseMissingStateIsFull(t *testing.T, newStore Factory) {
	s := newStore(t, clock.NewFake(base))
	ctx := context.Background()

	out, err := s.Apply(ctx, req("never-seen"))
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
func clauseErrorsCarryNoPolicy(t *testing.T, newStore Factory) {
	s := newStore(t, clock.NewFake(base))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	out, err := s.Apply(ctx, req("subject"))
	if err == nil {
		t.Fatal("Apply with a cancelled context returned nil error, want an error")
	}
	if out != (domain.Outcome{}) {
		t.Fatalf("Outcome = %+v on error, want the zero value", out)
	}
}
