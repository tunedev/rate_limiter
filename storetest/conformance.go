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

// window is the short window clauseStoreOwnsClock needs cheap boundaries for.
const window = time.Second

// counting is the params for clauses that only count admitted permits rather
// than test window boundaries. The window is long enough that a real clock's
// round trips never accrue a permit or cross a boundary while a clause runs.
var counting = domain.Params{Limit: 100, Window: time.Hour}

// algorithms is what RunConformance drives when a caller names none.
var algorithms = []domain.Algorithm{
	domain.TokenBucket,
	domain.LeakyBucket,
	domain.FixedWindow,
	domain.SlidingWindowLog,
	domain.SlidingWindowCounter,
}

func req(algo domain.Algorithm, key domain.Key, p domain.Params) port.Request {
	return ruleReq(algo, "rule-a", key, p)
}

func ruleReq(algo domain.Algorithm, rule domain.RuleID, key domain.Key, p domain.Params) port.Request {
	return port.Request{
		RuleID:    rule,
		Key:       key,
		Algorithm: algo,
		Params:    p,
		Cost:      1,
	}
}

// boundary returns the end of the fixed window containing t.
func boundary(t time.Time) time.Time { return t.Truncate(window).Add(window) }

// RunConformance asserts the port.Store contract against newHarness, for each
// algorithm the adapter claims to support. Naming none means all five.
//
// Clauses 1, 2 and 4 hold whatever the algorithm, and run for each. Clause 4's
// second statement, that expiry cannot return more capacity than idle time has
// accrued, only has an observable effect where Burst raises capacity above the
// window's own limit, so it runs for token_bucket and leaky_bucket alone.
// Clause 3 asserts a fixed window's boundary, which is the tightest statement
// of "the store owns the clock" available, and clause 5 concerns errors rather
// than arithmetic; both run once.
func RunConformance(t *testing.T, newHarness func(t *testing.T) Harness, algos ...domain.Algorithm) {
	t.Helper()
	if len(algos) == 0 {
		algos = algorithms
	}

	for _, algo := range algos {
		t.Run(string(algo), func(t *testing.T) {
			t.Run("clause 1: atomic per key", func(t *testing.T) { clauseAtomicPerKey(t, newHarness, algo) })
			t.Run("clause 2: keys are independent", func(t *testing.T) { clauseKeysIndependent(t, newHarness, algo) })
			t.Run("clause 4: missing state is full capacity", func(t *testing.T) { clauseMissingStateIsFull(t, newHarness, algo) })
			if algo == domain.TokenBucket || algo == domain.LeakyBucket {
				t.Run("clause 4: state outlives what it could refill", func(t *testing.T) {
					clauseStateOutlivesRefill(t, newHarness, algo)
				})
			}
		})
	}

	t.Run("clause 3: store owns the clock", func(t *testing.T) { clauseStoreOwnsClock(t, newHarness) })
	t.Run("clause 5: errors carry no policy", func(t *testing.T) { clauseErrorsCarryNoPolicy(t, newHarness) })
}

// clauseAtomicPerKey drives 400 concurrent takes at a limit of 100 and asserts
// exactly 100 are allowed. A lost update shows up as more than 100.
func clauseAtomicPerKey(t *testing.T, newHarness func(t *testing.T) Harness, algo domain.Algorithm) {
	h := newHarness(t)
	ctx := context.Background()

	var mu sync.Mutex
	allowed := 0

	var wg sync.WaitGroup
	for range 400 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, err := h.Store.Apply(ctx, req(algo, "subject", counting))
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

func clauseKeysIndependent(t *testing.T, newHarness func(t *testing.T) Harness, algo domain.Algorithm) {
	h := newHarness(t)
	ctx := context.Background()

	for range 100 {
		if _, err := h.Store.Apply(ctx, req(algo, "a", counting)); err != nil {
			t.Fatalf("Apply: %v", err)
		}
	}

	out, err := h.Store.Apply(ctx, req(algo, "a", counting))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if out.Allowed {
		t.Fatal("key a still allowed after 100 takes, want denied")
	}

	out, err = h.Store.Apply(ctx, req(algo, "b", counting))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if !out.Allowed {
		t.Fatal("key b denied, want allowed: exhausting one key must not affect another")
	}

	out, err = h.Store.Apply(ctx, ruleReq(algo, "rule-b", "a", counting))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if !out.Allowed {
		t.Fatal("key a under rule-b denied, want allowed: two rules over one subject hold separate state")
	}
}

// clauseStoreOwnsClock asserts ResetAt lands on the window boundary derived
// from the store's own time, and moves when that time moves. A caller's clock
// never enters the calculation.
func clauseStoreOwnsClock(t *testing.T, newHarness func(t *testing.T) Harness) {
	h := newHarness(t)
	ctx := context.Background()

	shortWindow := domain.Params{Limit: 100, Window: window}

	before := h.Now()
	first, err := h.Store.Apply(ctx, req(domain.FixedWindow, "subject", shortWindow))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if lo, hi := boundary(before), boundary(h.Now()); first.ResetAt.Before(lo) || first.ResetAt.After(hi) {
		t.Fatalf("ResetAt = %v, want a window boundary derived from the store's own time, within [%v, %v]", first.ResetAt, lo, hi)
	}

	h.Advance(2 * window)

	before = h.Now()
	second, err := h.Store.Apply(ctx, req(domain.FixedWindow, "subject", shortWindow))
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

func clauseMissingStateIsFull(t *testing.T, newHarness func(t *testing.T) Harness, algo domain.Algorithm) {
	h := newHarness(t)
	ctx := context.Background()

	out, err := h.Store.Apply(ctx, req(algo, "never-seen", counting))
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

// clauseStateOutlivesRefill asserts clause 4's second statement: draining a
// bucket then idling must not admit more than the idle time could refill.
// Missing state answers full capacity, so an entry expiring before the rule
// could have refilled it would hand back Burst permits for free.
func clauseStateOutlivesRefill(t *testing.T, newHarness func(t *testing.T) Harness, algo domain.Algorithm) {
	h := newHarness(t)
	ctx := context.Background()

	p := domain.Params{Limit: 10, Window: time.Second, Burst: 90}
	r := ruleReq(algo, "rule-a", "subject", p)

	drain := func() int64 {
		var allowed int64
		for {
			out, err := h.Store.Apply(ctx, r)
			if err != nil {
				t.Fatalf("Apply: %v", err)
			}
			if !out.Allowed {
				return allowed
			}
			allowed++
			if allowed > p.Limit+p.Burst {
				t.Fatalf("allowed %d permits from a bucket holding %d", allowed, p.Limit+p.Burst)
			}
		}
	}

	if got, want := drain(), p.Limit+p.Burst; got != want {
		t.Fatalf("drained %d permits from a fresh bucket, want %d", got, want)
	}

	idle := 3 * time.Second
	h.Advance(idle)

	// One take accrues at most a window of elapsed time, so however long the
	// store idled, the first take back cannot answer with more than a window's
	// worth of permits. A store that accrued the whole idle period at once
	// would answer with three windows' worth here.
	first, err := h.Store.Apply(ctx, r)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if !first.Allowed {
		t.Fatalf("first take after idling %v denied, want allowed: the idle time accrued permits", idle)
	}
	if want := int64(min(idle, p.Window) / p.Rate()); first.Remaining > want {
		t.Fatalf("Remaining = %d on the first take after idling %v, want at most %d: a single take accrues at most one window of %v", first.Remaining, idle, want, p.Window)
	}

	// Across takes the whole idle period accrues, a window at a time, and
	// stops there: what the rule granted while idle and nothing the state's
	// expiry handed back.
	if got, want := drain()+1, int64(idle/p.Rate())+1; got > want {
		t.Fatalf("drained %d permits after idling %v, want at most %d accrued", got, idle, want)
	}
}

// clauseErrorsCarryNoPolicy asserts a cancelled context yields an error and a
// zero Outcome, never an Allowed one.
func clauseErrorsCarryNoPolicy(t *testing.T, newHarness func(t *testing.T) Harness) {
	h := newHarness(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	out, err := h.Store.Apply(ctx, req(domain.FixedWindow, "subject", counting))
	if err == nil {
		t.Fatal("Apply with a cancelled context returned nil error, want an error")
	}
	if out != (domain.Outcome{}) {
		t.Fatalf("Outcome = %+v on error, want the zero value", out)
	}
}
