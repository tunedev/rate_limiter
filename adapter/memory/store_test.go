package memory

import (
	"context"
	"testing"
	"time"

	"github.com/tunedev/rate_limiter/adapter/clock"
	"github.com/tunedev/rate_limiter/domain"
	"github.com/tunedev/rate_limiter/port"
	"github.com/tunedev/rate_limiter/storetest"
)

func TestStoreConformance(t *testing.T) {
	storetest.RunConformance(t, func(t *testing.T, clk port.Clock) port.Store {
		s := New(clk)
		t.Cleanup(func() { _ = s.Close() })
		return s
	})
}

func TestStoreDispatchesOnAlgorithm(t *testing.T) {
	clk := clock.NewFake(time.Unix(1_700_000_000, 0))
	s := New(clk)
	t.Cleanup(func() { _ = s.Close() })

	req := port.Request{
		Key:       "subject",
		Algorithm: domain.TokenBucket,
		Params:    domain.Params{Limit: 2, Window: time.Second},
		Cost:      1,
	}

	for i := range 2 {
		out, err := s.Apply(context.Background(), req)
		if err != nil {
			t.Fatalf("Apply: %v", err)
		}
		if !out.Allowed {
			t.Fatalf("take %d denied, want allowed", i)
		}
	}

	out, err := s.Apply(context.Background(), req)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if out.Allowed {
		t.Fatal("take 3 allowed, want denied")
	}
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
