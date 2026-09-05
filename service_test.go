package ratelimit

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tunedev/rate_limiter/adapter/clock"
	"github.com/tunedev/rate_limiter/adapter/memory"
	"github.com/tunedev/rate_limiter/domain"
	"github.com/tunedev/rate_limiter/port"
)

type failingStore struct{ err error }

func (f failingStore) Apply(context.Context, port.Request) (domain.Outcome, error) {
	return domain.Outcome{}, f.err
}

type recordingObserver struct {
	decisions []domain.Decision
	stores    int
}

func (r *recordingObserver) BeginDecision(ctx context.Context, _ domain.RuleID, _ domain.Algorithm) (context.Context, port.EndDecision) {
	return ctx, func(d domain.Decision, _ error) { r.decisions = append(r.decisions, d) }
}

func (r *recordingObserver) BeginStore(ctx context.Context, _ domain.Algorithm) (context.Context, port.EndStore) {
	r.stores++
	return ctx, func(error) {}
}

func (r *recordingObserver) RulesReloaded(context.Context, error) {}

func checkReq() CheckRequest {
	return CheckRequest{
		RuleID:    "per-ip",
		Key:       "203.0.113.7",
		Algorithm: domain.FixedWindow,
		Params:    domain.Params{Limit: 2, Window: time.Second},
		Cost:      1,
	}
}

func TestCheckStampsRuleAndLimit(t *testing.T) {
	clk := clock.NewFake(time.Unix(1_700_000_000, 0))
	store := memory.New(clk)
	t.Cleanup(func() { _ = store.Close() })

	d, err := New(store).Check(context.Background(), checkReq())
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if !d.Allowed {
		t.Fatal("Allowed = false, want true")
	}
	if d.RuleID != "per-ip" {
		t.Fatalf("RuleID = %q, want per-ip", d.RuleID)
	}
	if d.Limit != 2 {
		t.Fatalf("Limit = %d, want 2", d.Limit)
	}
	if d.Remaining != 1 {
		t.Fatalf("Remaining = %d, want 1", d.Remaining)
	}
}

func TestCheckReturnsZeroDecisionOnStoreError(t *testing.T) {
	want := errors.New("store unreachable")

	d, err := New(failingStore{err: want}).Check(context.Background(), checkReq())
	if !errors.Is(err, want) {
		t.Fatalf("err = %v, want %v", err, want)
	}
	if d.Allowed {
		t.Fatal("Allowed = true on a store error; the service must not decide policy")
	}
}

func TestCheckReportsToObserver(t *testing.T) {
	clk := clock.NewFake(time.Unix(1_700_000_000, 0))
	store := memory.New(clk)
	t.Cleanup(func() { _ = store.Close() })

	obs := &recordingObserver{}
	if _, err := New(store, WithObserver(obs)).Check(context.Background(), checkReq()); err != nil {
		t.Fatalf("Check: %v", err)
	}

	if len(obs.decisions) != 1 {
		t.Fatalf("recorded %d decisions, want 1", len(obs.decisions))
	}
	if obs.decisions[0].RuleID != "per-ip" {
		t.Fatalf("recorded RuleID = %q, want per-ip", obs.decisions[0].RuleID)
	}
	if obs.stores != 1 {
		t.Fatalf("recorded %d store calls, want 1", obs.stores)
	}
}
