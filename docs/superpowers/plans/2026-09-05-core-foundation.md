# Core Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A working single-node rate limiter: two algorithms as pure transitions, an in-memory store that satisfies the atomicity contract, and the ports every later adapter plugs into.

**Architecture:** Hexagonal, dependencies pointing inward. `domain` holds pure transitions and value types and imports nothing but the standard library. `port` declares the four interfaces and imports `domain`. Adapters import both. A store adapter is correct exactly when it passes the shared conformance suite, so every later adapter — Redis included — is a drop-in against tests that already exist.

**Tech Stack:** Go 1.27.0, standard library only. No third-party dependencies in this plan.

**Spec:** `docs/superpowers/specs/2026-09-05-rate-limiter-design.md`

## Global Constraints

- Module path: `github.com/tunedev/rate_limiter`. Go directive: `go 1.27.0`.
- `domain/` and `port/` import the standard library only. No dependency may reach inward.
- Transitions are pure: no I/O, no clock reads, no goroutines. `now` is always a parameter.
- The zero state means full capacity (contract clause 4). Never treat missing state as an error.
- Every transition clamps elapsed time to `[0, Params.Window]` (contract clause 3).
- `Outcome` is zero-valued whenever a store returns a non-nil error (contract clause 5). An adapter never decides to allow traffic.
- No emojis in code, comments, logs, or commit messages.
- Comments describe current behaviour only. No history, no rationale narratives.
- Test command for the whole repo: `go test ./...`

---

### Task 1: Module and domain value types

**Files:**
- Create: `go.mod`
- Create: `domain/types.go`
- Create: `domain/clamp.go`
- Test: `domain/clamp_test.go`
- Test: `domain/types_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `domain.Algorithm` (string) with constants `TokenBucket`, `LeakyBucket`, `FixedWindow`, `SlidingWindowLog`, `SlidingWindowCounter`; `domain.Key` (string); `domain.RuleID` (string); `domain.GroupID` (string); `domain.Params{Limit int64; Window time.Duration; Burst int64}` with method `Rate() time.Duration`; `domain.Outcome{Allowed bool; Remaining int64; RetryAfter time.Duration; ResetAt time.Time}`; `domain.Decision{Allowed bool; Limit int64; Remaining int64; RetryAfter time.Duration; ResetAt time.Time; RuleID RuleID; Group GroupID}`; `domain.Limiter[S any]` interface with `Apply(s S, now time.Time, p Params, cost int64) (S, Outcome)`; unexported `clampElapsed(prev, now time.Time, window time.Duration) time.Duration`.

- [ ] **Step 1: Initialise the module**

```bash
cd /mnt/cs-home/go_learning/src/github.com/tunedev/rate_limiter
go mod init github.com/tunedev/rate_limiter
```

Confirm `go.mod` reads `go 1.27.0`. If it does not, set it: `go mod edit -go=1.27.0`.

- [ ] **Step 2: Write the failing tests**

`domain/clamp_test.go`:

```go
package domain

import (
	"testing"
	"time"
)

func TestClampElapsedBoundsClockJumps(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	const window = time.Second

	cases := []struct {
		name string
		prev time.Time
		now  time.Time
		want time.Duration
	}{
		{"normal progress", base, base.Add(250 * time.Millisecond), 250 * time.Millisecond},
		{"backward clock yields zero", base, base.Add(-5 * time.Second), 0},
		{"forward jump caps at one window", base, base.Add(time.Hour), window},
		{"exactly one window", base, base.Add(window), window},
		{"no progress", base, base, 0},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := clampElapsed(c.prev, c.now, window); got != c.want {
				t.Fatalf("clampElapsed = %v, want %v", got, c.want)
			}
		})
	}
}
```

`domain/types_test.go`:

```go
package domain

import (
	"testing"
	"time"
)

func TestParamsRate(t *testing.T) {
	cases := []struct {
		name string
		p    Params
		want time.Duration
	}{
		{"ten per second", Params{Limit: 10, Window: time.Second}, 100 * time.Millisecond},
		{"one per minute", Params{Limit: 1, Window: time.Minute}, time.Minute},
		{"zero limit is not a rate", Params{Limit: 0, Window: time.Second}, 0},
		{"negative limit is not a rate", Params{Limit: -1, Window: time.Second}, 0},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.p.Rate(); got != c.want {
				t.Fatalf("Rate = %v, want %v", got, c.want)
			}
		})
	}
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `go test ./domain/ -run 'TestClampElapsed|TestParamsRate' -v`
Expected: FAIL to build, with `undefined: clampElapsed` and `undefined: Params`.

- [ ] **Step 4: Write the implementation**

`domain/types.go`:

```go
// Package domain holds the rate limiter's pure value types and state
// transitions. It performs no I/O and reads no clock.
package domain

import "time"

// Algorithm names a rate limiting strategy. Stores dispatch on it to pick a
// state representation and a transition.
type Algorithm string

const (
	TokenBucket          Algorithm = "token_bucket"
	LeakyBucket          Algorithm = "leaky_bucket"
	FixedWindow          Algorithm = "fixed_window"
	SlidingWindowLog     Algorithm = "sliding_window_log"
	SlidingWindowCounter Algorithm = "sliding_window_counter"
)

// Key identifies the subject a limit applies to, such as an IP or a tenant.
type Key string

// RuleID names a rule within the rule set.
type RuleID string

// GroupID names a group of rules evaluated together.
type GroupID string

// Params holds every algorithm's knobs. Limit permits are granted per Window.
// Burst is extra capacity above Limit and applies to bucket algorithms only.
type Params struct {
	Limit  int64
	Window time.Duration
	Burst  int64
}

// Rate returns the interval between two permits, or zero when Limit is not
// positive.
func (p Params) Rate() time.Duration {
	if p.Limit <= 0 {
		return 0
	}
	return time.Duration(int64(p.Window) / p.Limit)
}

// Outcome is what a store can answer without knowing which rule asked.
type Outcome struct {
	Allowed    bool
	Remaining  int64
	RetryAfter time.Duration
	ResetAt    time.Time
}

// Decision is an Outcome attributed to the rule and group that produced it.
type Decision struct {
	Allowed    bool
	Limit      int64
	Remaining  int64
	RetryAfter time.Duration
	ResetAt    time.Time
	RuleID     RuleID
	Group      GroupID
}

// Limiter is a pure state transition over an algorithm's own state type. The
// zero value of S means full capacity.
type Limiter[S any] interface {
	Apply(s S, now time.Time, p Params, cost int64) (S, Outcome)
}
```

`domain/clamp.go`:

```go
package domain

import "time"

// clampElapsed bounds now-prev to [0, window]. A clock that moved backward
// yields zero; a forward jump yields at most one window of credit.
func clampElapsed(prev, now time.Time, window time.Duration) time.Duration {
	d := now.Sub(prev)
	switch {
	case d < 0:
		return 0
	case d > window:
		return window
	default:
		return d
	}
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./domain/ -v`
Expected: PASS for both tests.

- [ ] **Step 6: Commit**

```bash
git add go.mod domain/
git commit -m "Add domain value types and elapsed-time clamp

The clamp bounds elapsed to [0, Window] so a backward clock adds no credit
and a forward jump adds at most one window, which is what lets the store own
the clock without a failover corrupting bucket state."
```

---

### Task 2: Token bucket transition

**Files:**
- Create: `domain/tokenbucket.go`
- Test: `domain/tokenbucket_test.go`

**Interfaces:**
- Consumes: `domain.Params`, `domain.Outcome`, `clampElapsed` from Task 1.
- Produces: `domain.TokenBucketState{Tokens int64; At time.Time}`; `domain.TokenBucketLimiter` (empty struct) with `Apply(s TokenBucketState, now time.Time, p Params, cost int64) (TokenBucketState, Outcome)`. It satisfies `domain.Limiter[TokenBucketState]`.

Capacity is `Limit + Burst`. A fresh bucket starts full. Tokens accrue one per `Params.Rate()`; `At` advances only by the time whole tokens consumed, so the sub-token remainder carries to the next call rather than being lost to integer division.

- [ ] **Step 1: Write the failing test**

```go
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./domain/ -run TestTokenBucket -v`
Expected: FAIL to build, with `undefined: TokenBucketLimiter`.

- [ ] **Step 3: Write the implementation**

```go
package domain

import "time"

// TokenBucketState is a token count and the instant those tokens were counted.
// The zero value is a fresh bucket, filled on first use.
type TokenBucketState struct {
	Tokens int64
	At     time.Time
}

// TokenBucketLimiter grants Params.Limit permits per Params.Window, allowing a
// burst of up to Limit plus Burst permits held in reserve.
type TokenBucketLimiter struct{}

// Apply accrues tokens for the clamped elapsed time, then takes cost of them.
func (TokenBucketLimiter) Apply(s TokenBucketState, now time.Time, p Params, cost int64) (TokenBucketState, Outcome) {
	capacity := p.Limit + p.Burst
	rate := p.Rate()
	if rate <= 0 {
		return s, Outcome{}
	}

	if s.At.IsZero() {
		s = TokenBucketState{Tokens: capacity, At: now}
	}

	if accrued := int64(clampElapsed(s.At, now, p.Window) / rate); accrued > 0 {
		s.Tokens = min(capacity, s.Tokens+accrued)
		s.At = s.At.Add(time.Duration(accrued) * rate)
	}
	if s.Tokens >= capacity {
		s.At = now
	}

	full := time.Duration(capacity-s.Tokens) * rate
	if s.Tokens < cost {
		return s, Outcome{
			Remaining:  s.Tokens,
			RetryAfter: time.Duration(cost-s.Tokens) * rate,
			ResetAt:    now.Add(full),
		}
	}

	s.Tokens -= cost
	return s, Outcome{
		Allowed:   true,
		Remaining: s.Tokens,
		ResetAt:   now.Add(time.Duration(capacity-s.Tokens) * rate),
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./domain/ -run TestTokenBucket -v`
Expected: PASS for all six tests.

- [ ] **Step 5: Commit**

```bash
git add domain/tokenbucket.go domain/tokenbucket_test.go
git commit -m "Add token bucket transition

Advancing At by the time whole tokens consumed carries the sub-token
remainder, so a caller polling faster than the emission interval still
accrues at the configured rate instead of losing every fraction to integer
division."
```

---

### Task 3: Fixed window transition and its boundary weakness

**Files:**
- Create: `domain/fixedwindow.go`
- Test: `domain/fixedwindow_test.go`

**Interfaces:**
- Consumes: `domain.Params`, `domain.Outcome` from Task 1.
- Produces: `domain.FixedWindowState{Start time.Time; Count int64}`; `domain.FixedWindowLimiter` (empty struct) with `Apply(s FixedWindowState, now time.Time, p Params, cost int64) (FixedWindowState, Outcome)`. It satisfies `domain.Limiter[FixedWindowState]`.

Windows are aligned to absolute time (`now.Truncate(Window)`) rather than to first use, so every node computes the same boundary from the same clock. This task also lands the charter's evidence requirement: a test that demonstrates the algorithm admitting twice its limit across a window seam.

- [ ] **Step 1: Write the failing test**

```go
package domain

import (
	"testing"
	"time"
)

var fwParams = Params{Limit: 5, Window: time.Second}

func TestFixedWindowAllowsLimitThenDenies(t *testing.T) {
	var l FixedWindowLimiter
	now := time.Unix(1_700_000_000, 0)

	s := FixedWindowState{}
	for i := range 5 {
		var out Outcome
		s, out = l.Apply(s, now, fwParams, 1)
		if !out.Allowed {
			t.Fatalf("request %d denied, want allowed", i)
		}
	}

	_, out := l.Apply(s, now, fwParams, 1)
	if out.Allowed {
		t.Fatal("request 6 allowed, want denied")
	}
	if out.RetryAfter != time.Second {
		t.Fatalf("RetryAfter = %v, want 1s (to the next boundary)", out.RetryAfter)
	}
}

func TestFixedWindowResetsOnBoundary(t *testing.T) {
	var l FixedWindowLimiter
	now := time.Unix(1_700_000_000, 0)

	s := FixedWindowState{}
	for range 5 {
		s, _ = l.Apply(s, now, fwParams, 1)
	}

	s, out := l.Apply(s, now.Add(time.Second), fwParams, 1)
	if !out.Allowed {
		t.Fatal("denied in the next window, want allowed")
	}
	if s.Count != 1 {
		t.Fatalf("Count = %d, want 1 after reset", s.Count)
	}
}

func TestFixedWindowAlignsToAbsoluteTime(t *testing.T) {
	var l FixedWindowLimiter
	// First use lands mid-window; the window still starts at the second mark.
	now := time.Unix(1_700_000_000, 0).Add(700 * time.Millisecond)

	s, out := l.Apply(FixedWindowState{}, now, fwParams, 1)
	if want := time.Unix(1_700_000_000, 0); !s.Start.Equal(want) {
		t.Fatalf("Start = %v, want %v", s.Start, want)
	}
	if want := now.Add(300 * time.Millisecond); !out.ResetAt.Equal(want) {
		t.Fatalf("ResetAt = %v, want %v", out.ResetAt, want)
	}
}

// TestFixedWindowAdmitsTwiceTheLimitAtABoundary is evidence, not a bug report.
// Clustering requests either side of a seam admits 2x Limit inside a span
// shorter than one window. This is the weakness sliding window algorithms fix.
func TestFixedWindowAdmitsTwiceTheLimitAtABoundary(t *testing.T) {
	var l FixedWindowLimiter
	boundary := time.Unix(1_700_000_001, 0)

	s := FixedWindowState{}
	allowed := 0
	for range 5 {
		var out Outcome
		s, out = l.Apply(s, boundary.Add(-100*time.Millisecond), fwParams, 1)
		if out.Allowed {
			allowed++
		}
	}
	for range 5 {
		var out Outcome
		s, out = l.Apply(s, boundary.Add(100*time.Millisecond), fwParams, 1)
		if out.Allowed {
			allowed++
		}
	}

	if allowed != 10 {
		t.Fatalf("allowed = %d across the seam, want 10 (twice a limit of 5)", allowed)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./domain/ -run TestFixedWindow -v`
Expected: FAIL to build, with `undefined: FixedWindowLimiter`.

- [ ] **Step 3: Write the implementation**

```go
package domain

import "time"

// FixedWindowState is a count and the start of the window it counts. The zero
// value is an expired window, which resets on first use.
type FixedWindowState struct {
	Start time.Time
	Count int64
}

// FixedWindowLimiter grants Params.Limit permits per window, where windows are
// aligned to absolute time so every node agrees on the boundaries.
type FixedWindowLimiter struct{}

// Apply resets the count when now falls outside the stored window, then takes
// cost from it.
func (FixedWindowLimiter) Apply(s FixedWindowState, now time.Time, p Params, cost int64) (FixedWindowState, Outcome) {
	if p.Window <= 0 {
		return s, Outcome{}
	}

	start := now.Truncate(p.Window)
	if !s.Start.Equal(start) {
		s = FixedWindowState{Start: start}
	}

	resetAt := start.Add(p.Window)
	if s.Count+cost > p.Limit {
		return s, Outcome{
			Remaining:  max(0, p.Limit-s.Count),
			RetryAfter: resetAt.Sub(now),
			ResetAt:    resetAt,
		}
	}

	s.Count += cost
	return s, Outcome{
		Allowed:   true,
		Remaining: p.Limit - s.Count,
		ResetAt:   resetAt,
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./domain/ -run TestFixedWindow -v`
Expected: PASS for all four tests, including the boundary evidence test.

- [ ] **Step 5: Commit**

```bash
git add domain/fixedwindow.go domain/fixedwindow_test.go
git commit -m "Add fixed window transition with boundary evidence

Windows align to absolute time so nodes agree on boundaries without
coordinating. The boundary test records the algorithm admitting 2x its limit
across a seam; it is evidence for the algorithm comparison, not a defect."
```

---

### Task 4: Clock port and adapters

**Files:**
- Create: `port/clock.go`
- Create: `adapter/clock/clock.go`
- Test: `adapter/clock/clock_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `port.Clock` interface with `Now() time.Time`; `clock.System` (empty struct) satisfying it; `clock.Fake` with `clock.NewFake(t time.Time) *Fake`, `(*Fake).Now() time.Time`, `(*Fake).Advance(d time.Duration)`, `(*Fake).Set(t time.Time)`. `*Fake` is safe for concurrent use, which the conformance suite in Task 5 depends on.

- [ ] **Step 1: Write the failing test**

```go
package clock

import (
	"sync"
	"testing"
	"time"
)

func TestFakeAdvancesAndSets(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	f := NewFake(base)

	if !f.Now().Equal(base) {
		t.Fatalf("Now = %v, want %v", f.Now(), base)
	}

	f.Advance(time.Second)
	if want := base.Add(time.Second); !f.Now().Equal(want) {
		t.Fatalf("Now = %v, want %v", f.Now(), want)
	}

	f.Set(base)
	if !f.Now().Equal(base) {
		t.Fatalf("Now = %v, want %v after Set", f.Now(), base)
	}
}

func TestFakeIsRaceFree(t *testing.T) {
	f := NewFake(time.Unix(1_700_000_000, 0))

	var wg sync.WaitGroup
	for range 50 {
		wg.Add(2)
		go func() { defer wg.Done(); f.Advance(time.Millisecond) }()
		go func() { defer wg.Done(); _ = f.Now() }()
	}
	wg.Wait()

	if want := time.Unix(1_700_000_000, 0).Add(50 * time.Millisecond); !f.Now().Equal(want) {
		t.Fatalf("Now = %v, want %v", f.Now(), want)
	}
}

func TestSystemAdvances(t *testing.T) {
	var c System
	first := c.Now()
	if second := c.Now(); second.Before(first) {
		t.Fatalf("Now went backward: %v then %v", first, second)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./adapter/clock/ -v`
Expected: FAIL to build, with `undefined: NewFake`.

- [ ] **Step 3: Write the implementation**

`port/clock.go`:

```go
// Package port declares the interfaces the domain depends on. Adapters
// implement them; nothing here performs I/O.
package port

import "time"

// Clock reports the current instant. Stores own the clock, so this is the only
// place time enters the system.
type Clock interface {
	Now() time.Time
}
```

`adapter/clock/clock.go`:

```go
// Package clock provides the real and test implementations of port.Clock.
package clock

import (
	"sync"
	"time"
)

// System reads the host clock.
type System struct{}

// Now returns the host's current time.
func (System) Now() time.Time { return time.Now() }

// Fake is a clock that only moves when a test moves it. It is safe for
// concurrent use.
type Fake struct {
	mu  sync.RWMutex
	now time.Time
}

// NewFake returns a Fake reading t.
func NewFake(t time.Time) *Fake { return &Fake{now: t} }

// Now returns the current fake time.
func (f *Fake) Now() time.Time {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.now
}

// Advance moves the clock forward by d.
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}

// Set moves the clock to t, forward or backward.
func (f *Fake) Set(t time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = t
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./adapter/clock/ -race -v`
Expected: PASS for all three tests, with no race detected.

- [ ] **Step 5: Commit**

```bash
git add port/clock.go adapter/clock/
git commit -m "Add Clock port with system and fake adapters

Fake is mutex-guarded because the store conformance suite drives it from the
goroutines that exercise per-key atomicity."
```

---

### Task 5: Store port and conformance suite

**Files:**
- Create: `port/store.go`
- Create: `storetest/conformance.go`
- Test: none of its own. The suite is exercised by Task 6.

**Interfaces:**
- Consumes: `domain.Key`, `domain.Algorithm`, `domain.Params`, `domain.Outcome` from Task 1; `port.Clock` from Task 4.
- Produces: `port.Store` interface with `Apply(ctx context.Context, req Request) (domain.Outcome, error)`; `port.Request{Key domain.Key; Algorithm domain.Algorithm; Params domain.Params; Cost int64}`; `storetest.Factory` type `func(t *testing.T, clk port.Clock) port.Store`; `storetest.RunConformance(t *testing.T, newStore Factory)`.

The suite is the contract. Every store adapter in every later plan calls `RunConformance` and must pass unmodified. Clauses 1 through 4 and the observable half of clause 5 are covered here; clause 6 (non-idempotence under timeout) is a caller obligation with no generic observable, and is asserted per-adapter in the Redis plan instead.

- [ ] **Step 1: Write the port**

`port/store.go`:

```go
package port

import (
	"context"

	"github.com/tunedev/rate_limiter/domain"
)

// Request is one take against one key.
type Request struct {
	Key       domain.Key
	Algorithm domain.Algorithm
	Params    domain.Params
	Cost      int64
}

// Store applies an algorithm's transition atomically per key.
//
// The contract, asserted by storetest.RunConformance:
//
//  1. Apply is atomic per key. Concurrent calls on one key serialize.
//  2. Apply is not atomic across keys. There are no multi-key transactions.
//  3. The store owns the clock. Request carries no instant, and the returned
//     Outcome's ResetAt and RetryAfter come from the store's own time.
//  4. Missing state is full capacity, never an error.
//  5. Errors carry no policy. A non-nil error returns a zero Outcome; whether
//     that allows or denies is the caller's rule to decide.
//  6. Apply is not idempotent. A timed-out call may or may not have applied.
type Store interface {
	Apply(ctx context.Context, req Request) (domain.Outcome, error)
}
```

- [ ] **Step 2: Write the conformance suite**

`storetest/conformance.go`:

```go
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
```

- [ ] **Step 3: Verify the packages compile**

Run: `go build ./... && go vet ./...`
Expected: no output. `storetest` has no test of its own yet; Task 6 supplies the adapter that runs it.

- [ ] **Step 4: Commit**

```bash
git add port/store.go storetest/
git commit -m "Add Store port and its conformance suite

The suite is the contract: every adapter calls RunConformance unmodified, so
the Redis adapters arrive against tests that already exist. Clause 6 has no
generic observable and is asserted per-adapter instead."
```

---

### Task 6: In-memory store adapter

**Files:**
- Create: `adapter/memory/store.go`
- Test: `adapter/memory/store_test.go`

**Interfaces:**
- Consumes: `port.Store`, `port.Request`, `port.Clock`, `storetest.RunConformance`, `domain.TokenBucketLimiter`, `domain.FixedWindowLimiter`.
- Produces: `memory.Store` with `memory.New(clk port.Clock, opts ...Option) *Store`, `(*Store).Apply(ctx context.Context, req port.Request) (domain.Outcome, error)`, `(*Store).Close() error`, `(*Store).Len() int`; `memory.Option` with `memory.WithSweepInterval(d time.Duration) Option`.

Atomicity comes from one mutex per key, held across the read-transition-write. Generic transitions are erased into a per-algorithm function at construction: a failed type assertion on absent state yields the zero state, which is exactly clause 4. Entries expire one window past last use, and a sweeper reclaims them so idle keys do not accumulate.

- [ ] **Step 1: Write the failing test**

```go
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./adapter/memory/ -v`
Expected: FAIL to build, with `undefined: New`.

- [ ] **Step 3: Write the implementation**

```go
// Package memory holds an in-process port.Store. It is the reference adapter:
// the transitions it runs are the same pure functions the domain exports.
package memory

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/tunedev/rate_limiter/domain"
	"github.com/tunedev/rate_limiter/port"
)

// transition erases a generic domain.Limiter to a function over any. A failed
// assertion yields the zero state, which is contract clause 4.
type transition func(prev any, now time.Time, p domain.Params, cost int64) (any, domain.Outcome)

func erase[S any](l domain.Limiter[S]) transition {
	return func(prev any, now time.Time, p domain.Params, cost int64) (any, domain.Outcome) {
		s, _ := prev.(S)
		return l.Apply(s, now, p, cost)
	}
}

type entry struct {
	mu      sync.Mutex
	state   any
	expires time.Time
}

// Option configures a Store.
type Option func(*Store)

// WithSweepInterval sets how often expired entries are reclaimed. Zero disables
// the background sweeper, leaving Sweep to be called directly.
func WithSweepInterval(d time.Duration) Option {
	return func(s *Store) { s.sweepEvery = d }
}

// Store applies transitions under one mutex per key.
type Store struct {
	clk         port.Clock
	transitions map[domain.Algorithm]transition
	sweepEvery  time.Duration

	mu      sync.RWMutex
	entries map[domain.Key]*entry

	stop chan struct{}
	done sync.WaitGroup
}

// New returns an empty Store reading time from clk.
func New(clk port.Clock, opts ...Option) *Store {
	s := &Store{
		clk: clk,
		transitions: map[domain.Algorithm]transition{
			domain.TokenBucket: erase[domain.TokenBucketState](domain.TokenBucketLimiter{}),
			domain.FixedWindow: erase[domain.FixedWindowState](domain.FixedWindowLimiter{}),
		},
		sweepEvery: time.Minute,
		entries:    make(map[domain.Key]*entry),
		stop:       make(chan struct{}),
	}
	for _, o := range opts {
		o(s)
	}
	if s.sweepEvery > 0 {
		s.done.Add(1)
		go s.sweepLoop()
	}
	return s
}

// Apply runs req's transition atomically for req.Key.
func (s *Store) Apply(ctx context.Context, req port.Request) (domain.Outcome, error) {
	if err := ctx.Err(); err != nil {
		return domain.Outcome{}, err
	}

	apply, ok := s.transitions[req.Algorithm]
	if !ok {
		return domain.Outcome{}, fmt.Errorf("memory: unsupported algorithm %q", req.Algorithm)
	}

	e := s.entryFor(req.Key)

	e.mu.Lock()
	defer e.mu.Unlock()

	now := s.clk.Now()
	if now.After(e.expires) {
		e.state = nil
	}

	state, out := apply(e.state, now, req.Params, req.Cost)
	e.state = state
	e.expires = now.Add(2 * req.Params.Window)
	return out, nil
}

func (s *Store) entryFor(k domain.Key) *entry {
	s.mu.RLock()
	e, ok := s.entries[k]
	s.mu.RUnlock()
	if ok {
		return e
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.entries[k]; ok {
		return e
	}
	e = &entry{}
	s.entries[k] = e
	return e
}

// Len reports how many keys hold state.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.entries)
}

// Sweep reclaims entries whose state has expired.
func (s *Store) Sweep() {
	now := s.clk.Now()

	s.mu.Lock()
	defer s.mu.Unlock()
	for k, e := range s.entries {
		if e.mu.TryLock() {
			if now.After(e.expires) {
				delete(s.entries, k)
			}
			e.mu.Unlock()
		}
	}
}

func (s *Store) sweepLoop() {
	defer s.done.Done()
	t := time.NewTicker(s.sweepEvery)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			s.Sweep()
		case <-s.stop:
			return
		}
	}
}

// Close stops the background sweeper.
func (s *Store) Close() error {
	close(s.stop)
	s.done.Wait()
	return nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./adapter/memory/ -race -v`
Expected: PASS, including all five `TestStoreConformance` subtests and no race detected.

- [ ] **Step 5: Commit**

```bash
git add adapter/memory/
git commit -m "Add in-memory store adapter passing the conformance suite

State is erased to any at the registry boundary so each algorithm keeps its
own state type. A failed type assertion yields the zero state, which is how
clause 4 falls out of the design rather than needing a check."
```

---

### Task 7: Observer port and noop adapter

**Files:**
- Create: `port/observer.go`
- Create: `adapter/observer/noop/noop.go`
- Test: `adapter/observer/noop/noop_test.go`

**Interfaces:**
- Consumes: `domain.Decision`, `domain.RuleID`, `domain.Algorithm` from Task 1.
- Produces: `port.Observer` interface with `BeginDecision(ctx context.Context, rule domain.RuleID, algo domain.Algorithm) (context.Context, EndDecision)`, `BeginStore(ctx context.Context, algo domain.Algorithm) (context.Context, EndStore)`, `RulesReloaded(ctx context.Context, err error)`; `port.EndDecision` type `func(domain.Decision, error)`; `port.EndStore` type `func(error)`; `noop.Observer` (empty struct) satisfying `port.Observer`.

Noop is the library default so an embedding consumer never acquires an exporter it did not ask for. It returns the context unchanged and functions that do nothing.

- [ ] **Step 1: Write the failing test**

```go
package noop

import (
	"context"
	"errors"
	"testing"

	"github.com/tunedev/rate_limiter/domain"
	"github.com/tunedev/rate_limiter/port"
)

var _ port.Observer = Observer{}

func TestObserverIsInert(t *testing.T) {
	var o Observer
	ctx := context.Background()

	got, end := o.BeginDecision(ctx, "rule", domain.TokenBucket)
	if got != ctx {
		t.Fatal("BeginDecision changed the context, want it returned unchanged")
	}
	end(domain.Decision{}, errors.New("ignored"))

	got, endStore := o.BeginStore(ctx, domain.TokenBucket)
	if got != ctx {
		t.Fatal("BeginStore changed the context, want it returned unchanged")
	}
	endStore(nil)

	o.RulesReloaded(ctx, nil)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./adapter/observer/noop/ -v`
Expected: FAIL to build, with `undefined: Observer`.

- [ ] **Step 3: Write the implementation**

`port/observer.go`:

```go
package port

import (
	"context"

	"github.com/tunedev/rate_limiter/domain"
)

// EndDecision closes a decision's span and records its result.
type EndDecision func(domain.Decision, error)

// EndStore closes a store round trip's span and records its result.
type EndStore func(error)

// Observer receives the traces, metrics and logs a decision produces. It
// returns a context so spans propagate into the store call beneath.
//
// The limit key is never passed here: it is unbounded, and a metric label
// built from it would be unbounded too.
type Observer interface {
	BeginDecision(ctx context.Context, rule domain.RuleID, algo domain.Algorithm) (context.Context, EndDecision)
	BeginStore(ctx context.Context, algo domain.Algorithm) (context.Context, EndStore)
	RulesReloaded(ctx context.Context, err error)
}
```

`adapter/observer/noop/noop.go`:

```go
// Package noop holds the inert port.Observer used by default, so a library
// consumer acquires no exporter it did not ask for.
package noop

import (
	"context"

	"github.com/tunedev/rate_limiter/domain"
	"github.com/tunedev/rate_limiter/port"
)

// Observer discards everything it is given.
type Observer struct{}

// BeginDecision returns ctx unchanged and a function that does nothing.
func (Observer) BeginDecision(ctx context.Context, _ domain.RuleID, _ domain.Algorithm) (context.Context, port.EndDecision) {
	return ctx, func(domain.Decision, error) {}
}

// BeginStore returns ctx unchanged and a function that does nothing.
func (Observer) BeginStore(ctx context.Context, _ domain.Algorithm) (context.Context, port.EndStore) {
	return ctx, func(error) {}
}

// RulesReloaded does nothing.
func (Observer) RulesReloaded(context.Context, error) {}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./adapter/observer/noop/ -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add port/observer.go adapter/observer/noop/
git commit -m "Add Observer port with a noop adapter

The port takes a rule id and an algorithm but never a key, so an unbounded
label cannot reach a metrics backend by accident."
```

---

### Task 8: Service, and the per-algorithm benchmark

**Files:**
- Create: `service.go`
- Test: `service_test.go`
- Test: `bench_test.go`
- Create: `docs/benchmarks.md`

**Interfaces:**
- Consumes: everything from Tasks 1 through 7.
- Produces: `ratelimit.Service` with `ratelimit.New(store port.Store, opts ...Option) *Service`, `(*Service).Check(ctx context.Context, req CheckRequest) (domain.Decision, error)`; `ratelimit.CheckRequest{RuleID domain.RuleID; Key domain.Key; Algorithm domain.Algorithm; Params domain.Params; Cost int64}`; `ratelimit.Option` with `ratelimit.WithObserver(o port.Observer) Option`.

`Check` composes the store's `Outcome` into a `Decision` by adding `Limit` from `Params` and stamping `RuleID`. `Group` stays zero until the rule engine lands in plan 4. The service is where instrumentation lives, so the store adapters stay free of it.

- [ ] **Step 1: Write the failing test**

`service_test.go`:

```go
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test . -v`
Expected: FAIL to build, with `undefined: New`.

- [ ] **Step 3: Write the implementation**

`service.go`:

```go
// Package ratelimit is the rate limiter's public entry point. It composes a
// store's Outcome into an attributed Decision and owns instrumentation, so
// store adapters stay free of it.
package ratelimit

import (
	"context"

	"github.com/tunedev/rate_limiter/adapter/observer/noop"
	"github.com/tunedev/rate_limiter/domain"
	"github.com/tunedev/rate_limiter/port"
)

// CheckRequest is one take against one key under one rule.
type CheckRequest struct {
	RuleID    domain.RuleID
	Key       domain.Key
	Algorithm domain.Algorithm
	Params    domain.Params
	Cost      int64
}

// Option configures a Service.
type Option func(*Service)

// WithObserver sends traces, metrics and logs to o. The default discards them.
func WithObserver(o port.Observer) Option {
	return func(s *Service) { s.obs = o }
}

// Service answers rate limit questions against a store.
type Service struct {
	store port.Store
	obs   port.Observer
}

// New returns a Service backed by store.
func New(store port.Store, opts ...Option) *Service {
	s := &Service{store: store, obs: noop.Observer{}}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Check applies req and returns the decision. On a store error the Decision is
// zero-valued: whether that allows or denies is the caller's policy.
func (s *Service) Check(ctx context.Context, req CheckRequest) (domain.Decision, error) {
	ctx, endDecision := s.obs.BeginDecision(ctx, req.RuleID, req.Algorithm)

	storeCtx, endStore := s.obs.BeginStore(ctx, req.Algorithm)
	out, err := s.store.Apply(storeCtx, port.Request{
		Key:       req.Key,
		Algorithm: req.Algorithm,
		Params:    req.Params,
		Cost:      req.Cost,
	})
	endStore(err)

	if err != nil {
		endDecision(domain.Decision{}, err)
		return domain.Decision{}, err
	}

	d := domain.Decision{
		Allowed:    out.Allowed,
		Limit:      req.Params.Limit,
		Remaining:  out.Remaining,
		RetryAfter: out.RetryAfter,
		ResetAt:    out.ResetAt,
		RuleID:     req.RuleID,
	}
	endDecision(d, nil)
	return d, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./... -race`
Expected: PASS for every package.

- [ ] **Step 5: Write the benchmark**

`bench_test.go`:

```go
package ratelimit

import (
	"context"
	"testing"
	"time"

	"github.com/tunedev/rate_limiter/adapter/clock"
	"github.com/tunedev/rate_limiter/adapter/memory"
	"github.com/tunedev/rate_limiter/domain"
)

// BenchmarkCheck measures per-algorithm decision cost against the in-memory
// store, on one hot key so the per-key mutex is contended.
func BenchmarkCheck(b *testing.B) {
	algos := []domain.Algorithm{domain.TokenBucket, domain.FixedWindow}

	for _, algo := range algos {
		b.Run(string(algo), func(b *testing.B) {
			store := memory.New(clock.System{})
			b.Cleanup(func() { _ = store.Close() })
			svc := New(store)

			req := CheckRequest{
				RuleID:    "bench",
				Key:       "subject",
				Algorithm: algo,
				Params:    domain.Params{Limit: 1_000_000_000, Window: time.Hour},
				Cost:      1,
			}

			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				ctx := context.Background()
				for pb.Next() {
					if _, err := svc.Check(ctx, req); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}
```

- [ ] **Step 6: Run the benchmark and record it**

Run: `go test -bench=BenchmarkCheck -benchmem -run '^$' . | tee /tmp/bench.txt`

Create `docs/benchmarks.md` with the real numbers from that run. Use this shape, replacing the placeholder row values with the measured output. Do not invent numbers:

```markdown
# Benchmarks

Every table below is produced by a command in this file. Nothing is asserted
here that was not measured.

## Per-algorithm decision cost, in-memory store

Contended on one key, so the figure includes the per-key mutex.

    go test -bench=BenchmarkCheck -benchmem -run '^$' .

| Algorithm | ns/op | B/op | allocs/op |
|---|---|---|---|
| token_bucket | | | |
| fixed_window | | | |

Machine: `go version go1.27.0 linux/amd64`.
```

- [ ] **Step 7: Commit**

```bash
git add service.go service_test.go bench_test.go docs/benchmarks.md
git commit -m "Add the service composing Outcome into Decision, and benchmark 2

Instrumentation lives in the service rather than in store adapters, so a new
adapter inherits it. A store error yields a zero Decision: the service reports
the failure and leaves fail-open or fail-closed to the caller's rule."
```

---

## Definition of Done

- [ ] `go test ./... -race` passes.
- [ ] `go vet ./...` is clean.
- [ ] `adapter/memory` passes all five `RunConformance` subtests.
- [ ] `docs/benchmarks.md` holds measured numbers and the command that produced them.
- [ ] `domain/` and `port/` import only the standard library: `go list -deps ./domain/ ./port/ | grep -v '^github.com/tunedev/rate_limiter' | grep '\.' ` returns nothing.

## What this plan deliberately leaves out

- The other three algorithms. They implement `domain.Limiter[S]` and register in `memory.New`'s map, which plan 2 does with the seams already proven.
- Redis. Both adapters call `storetest.RunConformance` unchanged; that is the point of Task 5.
- Rules, groups, matching and precedence. `CheckRequest` carries an explicit algorithm and params until plan 4 replaces that with rule resolution, and `Decision.Group` stays zero until then.
- The otel observer, the HTTP middleware, and the standalone service.
