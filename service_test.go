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

// storeSpanKey marks the context BeginStore returns, so a store can report
// whether it was handed that context rather than the caller's.
type storeSpanKey struct{}

type failingStore struct {
	err             error
	sawStoreContext bool
}

func (f *failingStore) Apply(ctx context.Context, _ port.Request) (domain.Outcome, error) {
	f.sawStoreContext = ctx.Value(storeSpanKey{}) != nil
	return domain.Outcome{}, f.err
}

type recordingObserver struct {
	decisions    []domain.Decision
	decisionErrs []error
	storeErrs    []error
}

func (r *recordingObserver) BeginDecision(ctx context.Context, _ domain.RuleID, _ domain.Algorithm) (context.Context, port.EndDecision) {
	return ctx, func(d domain.Decision, err error) {
		r.decisions = append(r.decisions, d)
		r.decisionErrs = append(r.decisionErrs, err)
	}
}

func (r *recordingObserver) BeginStore(ctx context.Context, _ domain.Algorithm) (context.Context, port.EndStore) {
	return context.WithValue(ctx, storeSpanKey{}, true), func(err error) {
		r.storeErrs = append(r.storeErrs, err)
	}
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

	d, err := New(&failingStore{err: want}).Check(context.Background(), checkReq())
	if !errors.Is(err, want) {
		t.Fatalf("err = %v, want %v", err, want)
	}
	if d != (domain.Decision{}) {
		t.Fatalf("d = %+v, want zero Decision on a store error; the service must not decide policy", d)
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
	if len(obs.storeErrs) != 1 || obs.storeErrs[0] != nil {
		t.Fatalf("recorded store results %v, want exactly one nil", obs.storeErrs)
	}
}

// TestCheckReportsStoreErrorToObserver pins the two things the store round trip
// owes the observer: the error it failed with, and the context BeginStore
// returned, without which a span cannot nest under the decision.
func TestCheckReportsStoreErrorToObserver(t *testing.T) {
	want := errors.New("store unreachable")
	store := &failingStore{err: want}
	obs := &recordingObserver{}

	if _, err := New(store, WithObserver(obs)).Check(context.Background(), checkReq()); !errors.Is(err, want) {
		t.Fatalf("err = %v, want %v", err, want)
	}

	if len(obs.storeErrs) != 1 || !errors.Is(obs.storeErrs[0], want) {
		t.Fatalf("recorded store results %v, want exactly [%v]", obs.storeErrs, want)
	}
	if len(obs.decisionErrs) != 1 || !errors.Is(obs.decisionErrs[0], want) {
		t.Fatalf("recorded decision results %v, want exactly [%v]", obs.decisionErrs, want)
	}
	if !store.sawStoreContext {
		t.Fatal("the store was called with the caller's context, want the one BeginStore returned")
	}
}

// TestCheckRejectsInvalidParams pins that a rule with no window is an error
// rather than a denial with nothing to retry after, which an HTTP adapter would
// map to a 429 with Retry-After 0.
func TestCheckRejectsInvalidParams(t *testing.T) {
	store := memory.New(clock.NewFake(time.Unix(1_700_000_000, 0)))
	t.Cleanup(func() { _ = store.Close() })

	req := checkReq()
	req.Params = domain.Params{Limit: 10}

	obs := &recordingObserver{}
	d, err := New(store, WithObserver(obs)).Check(context.Background(), req)
	if err == nil {
		t.Fatal("Check with no window returned nil error, want an error")
	}
	if d != (domain.Decision{}) {
		t.Fatalf("d = %+v, want zero Decision on invalid params", d)
	}
	if len(obs.decisionErrs) != 1 || obs.decisionErrs[0] == nil {
		t.Fatalf("recorded decision results %v, want exactly one error", obs.decisionErrs)
	}
	if len(obs.storeErrs) != 0 {
		t.Fatalf("recorded %d store round trips, want none: invalid params never reach the store", len(obs.storeErrs))
	}
}

// TestCheckRejectsNegativeCost pins that a negative Cost is an error rather
// than a nonsensical result: sliding_window_log panics on a negative Cost, and
// the other four algorithms would silently grant or deny the wrong thing.
func TestCheckRejectsNegativeCost(t *testing.T) {
	store := memory.New(clock.NewFake(time.Unix(1_700_000_000, 0)))
	t.Cleanup(func() { _ = store.Close() })

	req := checkReq()
	req.Cost = -1

	obs := &recordingObserver{}
	d, err := New(store, WithObserver(obs)).Check(context.Background(), req)
	if err == nil {
		t.Fatal("Check with a negative Cost returned nil error, want an error")
	}
	if d != (domain.Decision{}) {
		t.Fatalf("d = %+v, want zero Decision on a negative Cost", d)
	}
	if len(obs.storeErrs) != 0 {
		t.Fatalf("recorded %d store round trips, want none: a negative Cost never reaches the store", len(obs.storeErrs))
	}
}
