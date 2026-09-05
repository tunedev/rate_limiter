# Remaining Algorithms Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The other three algorithms — leaky bucket, sliding window log, sliding window counter — behind the seams plan 1 proved, each with a test that demonstrates its weakness.

**Architecture:** Each algorithm is a `domain.Limiter[S]` with its own state type, plus a `Lifetime(Params)` saying how long that state must outlive its last use. Adding one means writing those two methods and adding one registry entry to `memory.New`. Nothing outside `domain/` and the registry changes.

**Tech Stack:** Go 1.27.0, standard library only.

**Spec:** `docs/superpowers/specs/2026-09-05-rate-limiter-design.md`

## Global Constraints

- `domain/` and `port/` import the standard library only. No dependency may reach inward.
- Transitions are pure: no I/O, no clock reads, no goroutines, and no mutation of the state passed in. `now` is always a parameter.
- The zero state means full capacity. Never treat missing state as an error.
- Transitions that accrue over elapsed time clamp it with the existing unexported `clampElapsed`. Window-based algorithms derive boundaries from `now.Truncate(p.Window)` instead and need no clamp.
- `Lifetime` must be long enough that expiry can never hand back more capacity than the rule has accrued. Expiring early is the over-admission bug plan 1 closed.
- `min` and `max` are Go builtins. Do not write your own.
- No emojis in code, comments, logs, or commit messages.
- Comments describe current behaviour only. No history, no rationale narratives.
- Test command for the whole repo: `go test ./...`

## Existing interfaces these tasks build on

Already on disk, do not recreate:

- `domain.Params{Limit int64; Window time.Duration; Burst int64}`, `Rate() time.Duration` returning `Window/Limit`, `Validate(Algorithm) error`.
- `domain.Outcome{Allowed bool; Remaining int64; RetryAfter time.Duration; ResetAt time.Time}`.
- `domain.Limiter[S any]` with `Apply(s S, now time.Time, p Params, cost int64) (S, Outcome)`.
- `clampElapsed(prev, now time.Time, window time.Duration) time.Duration`, unexported, bounds to `[0, window]`.
- `domain.TokenBucketLimiter` over `TokenBucketState{Tokens int64; At time.Time}`, with `Lifetime`.
- `domain.FixedWindowLimiter` over `FixedWindowState{Start time.Time; Count int64}`, with `Lifetime(p) = 2*p.Window`.
- `memory.New`'s registry: `map[domain.Algorithm]algorithm` where `algorithm{apply transition; lifetime func(domain.Params) time.Duration}` and `apply` comes from `erase[S](limiter)`.
- `storetest.Harness{Store port.Store; Now func() time.Time; Advance func(time.Duration)}` and `RunConformance(t, newHarness func(t *testing.T) storetest.Harness)`.

The `Algorithm` constants `LeakyBucket`, `SlidingWindowLog` and `SlidingWindowCounter` already exist in `domain/types.go` and are already accepted by `Params.Validate`. No change to that file is needed.

---

### Task 1: Leaky bucket, and proof that it is token bucket

**Files:**
- Create: `domain/leakybucket.go`
- Test: `domain/leakybucket_test.go`
- Create: `docs/decisions/0001-leaky-bucket-is-token-bucket.md`

**Interfaces:**
- Consumes: `domain.Params`, `domain.Outcome`, `clampElapsed`, `domain.TokenBucketLimiter`, `domain.TokenBucketState`.
- Produces: `domain.LeakyBucketState{Depth int64; At time.Time}`; `domain.LeakyBucketLimiter` (empty struct) with `Lifetime(p Params) time.Duration` and `Apply(s LeakyBucketState, now time.Time, p Params, cost int64) (LeakyBucketState, Outcome)`. It satisfies `domain.Limiter[LeakyBucketState]`.

Depth rises by `cost` on admission and drains one unit per `Params.Rate()`. This is the book's leaky bucket as a meter, and it is token bucket in different coordinates: substitute `Depth = capacity - Tokens` and the two transitions are the same function. The differential test in step 1 is what makes that a measured claim rather than an assertion, and it is the reason this algorithm is worth having in the comparison at all.

- [ ] **Step 1: Write the failing test**

```go
package domain

import (
	"testing"
	"time"
)

var lbParams = Params{Limit: 10, Window: time.Second}

func TestLeakyBucketFillsThenRejects(t *testing.T) {
	var l LeakyBucketLimiter
	now := time.Unix(1_700_000_000, 0)

	s := LeakyBucketState{}
	for i := range 10 {
		var out Outcome
		s, out = l.Apply(s, now, lbParams, 1)
		if !out.Allowed {
			t.Fatalf("request %d denied, want allowed", i)
		}
	}
	if s.Depth != 10 {
		t.Fatalf("Depth = %d, want 10", s.Depth)
	}

	_, out := l.Apply(s, now, lbParams, 1)
	if out.Allowed {
		t.Fatal("request 11 allowed, want denied")
	}
	if out.RetryAfter != 100*time.Millisecond {
		t.Fatalf("RetryAfter = %v, want 100ms", out.RetryAfter)
	}
}

func TestLeakyBucketDrainsAtRate(t *testing.T) {
	var l LeakyBucketLimiter
	now := time.Unix(1_700_000_000, 0)

	s := LeakyBucketState{Depth: 10, At: now}
	s, out := l.Apply(s, now.Add(350*time.Millisecond), lbParams, 1)
	if !out.Allowed {
		t.Fatal("denied after 350ms of draining, want allowed")
	}
	if s.Depth != 8 {
		t.Fatalf("Depth = %d, want 8 (3 drained, 1 added)", s.Depth)
	}
}

func TestLeakyBucketCarriesSubUnitRemainder(t *testing.T) {
	var l LeakyBucketLimiter
	now := time.Unix(1_700_000_000, 0)
	s := LeakyBucketState{Depth: 10, At: now}

	// At 100ms per unit, 150ms drains one and leaves 50ms owed. Snapping At to
	// now would drop that 50ms and the second step would drain nothing.
	s, _ = l.Apply(s, now.Add(150*time.Millisecond), lbParams, 0)
	if s.Depth != 9 {
		t.Fatalf("Depth = %d after 150ms, want 9", s.Depth)
	}

	s, _ = l.Apply(s, now.Add(200*time.Millisecond), lbParams, 0)
	if s.Depth != 8 {
		t.Fatalf("Depth = %d after 200ms, want 8: the 50ms remainder was dropped", s.Depth)
	}
}

func TestLeakyBucketBackwardClockDrainsNothing(t *testing.T) {
	var l LeakyBucketLimiter
	now := time.Unix(1_700_000_000, 0)

	s, out := l.Apply(LeakyBucketState{Depth: 10, At: now}, now.Add(-time.Hour), lbParams, 1)
	if out.Allowed {
		t.Fatal("allowed after a backward clock jump, want denied")
	}
	if s.Depth != 10 {
		t.Fatalf("Depth = %d, want 10", s.Depth)
	}
}

// TestLeakyBucketMatchesTokenBucket is the evidence for docs/decisions/0001.
// Under Depth = capacity - Tokens the two transitions are the same function,
// so one event sequence must produce identical Outcomes from both. A
// divergence means one of them has a bug.
func TestLeakyBucketMatchesTokenBucket(t *testing.T) {
	var leaky LeakyBucketLimiter
	var token TokenBucketLimiter

	base := time.Unix(1_700_000_000, 0)
	steps := []struct {
		after time.Duration
		cost  int64
	}{
		{0, 1}, {0, 3}, {0, 6}, {0, 1},
		{120 * time.Millisecond, 1},
		{0, 1},
		{450 * time.Millisecond, 4},
		{time.Second, 10},
		{0, 1},
		{-time.Hour, 1},
		{24 * time.Hour, 10},
		{0, 1},
	}

	for _, p := range []Params{
		{Limit: 10, Window: time.Second},
		{Limit: 10, Window: time.Second, Burst: 5},
		{Limit: 3, Window: 300 * time.Millisecond},
	} {
		ls := LeakyBucketState{}
		ts := TokenBucketState{}
		now := base

		for i, step := range steps {
			now = now.Add(step.after)

			var lo, to Outcome
			ls, lo = leaky.Apply(ls, now, p, step.cost)
			ts, to = token.Apply(ts, now, p, step.cost)

			if lo != to {
				t.Fatalf("params %+v step %d (cost %d): leaky = %+v, token = %+v", p, i, step.cost, lo, to)
			}
			if got, want := ls.Depth, p.Limit+p.Burst-ts.Tokens; got != want {
				t.Fatalf("params %+v step %d: Depth = %d, want capacity-Tokens = %d", p, i, got, want)
			}
		}
	}
}

func TestLeakyBucketLifetimeCoversDrain(t *testing.T) {
	var l LeakyBucketLimiter
	p := Params{Limit: 10, Window: time.Second, Burst: 90}

	if got, want := l.Lifetime(p), time.Duration(p.Limit+p.Burst)*p.Rate(); got < want {
		t.Fatalf("Lifetime = %v, want at least %v to drain from full", got, want)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./domain/ -run TestLeakyBucket -v`
Expected: FAIL to build, with `undefined: LeakyBucketLimiter`.

- [ ] **Step 3: Write the implementation**

```go
package domain

import "time"

// LeakyBucketState is a queue depth and the instant it was measured. The zero
// value is an empty bucket.
type LeakyBucketState struct {
	Depth int64
	At    time.Time
}

// LeakyBucketLimiter admits while the bucket holds room. Depth rises by cost on
// admission and drains one unit per Params.Rate(), so the bucket empties at the
// configured rate no matter how fast requests arrive.
type LeakyBucketLimiter struct{}

// Lifetime reports how long a bucket's state must outlive its last use: long
// enough to drain from full, plus a window of slack. Expiring sooner would hand
// back room the rule has not drained.
func (LeakyBucketLimiter) Lifetime(p Params) time.Duration {
	drain := p.Window + time.Duration(p.Burst)*p.Rate()
	return drain + p.Window
}

// Apply drains the bucket for the clamped elapsed time, then adds cost to it.
func (LeakyBucketLimiter) Apply(s LeakyBucketState, now time.Time, p Params, cost int64) (LeakyBucketState, Outcome) {
	capacity := p.Limit + p.Burst
	rate := p.Rate()
	if rate <= 0 {
		return s, Outcome{}
	}

	if s.At.IsZero() {
		s = LeakyBucketState{At: now}
	}

	if drained := int64(clampElapsed(s.At, now, p.Window) / rate); drained > 0 {
		s.Depth = max(0, s.Depth-drained)
		s.At = s.At.Add(time.Duration(drained) * rate)
	}
	if s.Depth <= 0 {
		s.At = now
	}

	if s.Depth+cost > capacity {
		return s, Outcome{
			Remaining:  capacity - s.Depth,
			RetryAfter: time.Duration(s.Depth+cost-capacity) * rate,
			ResetAt:    now.Add(time.Duration(s.Depth) * rate),
		}
	}

	s.Depth += cost
	return s, Outcome{
		Allowed:   true,
		Remaining: capacity - s.Depth,
		ResetAt:   now.Add(time.Duration(s.Depth) * rate),
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./domain/ -run TestLeakyBucket -v`
Expected: PASS for all six tests, including `TestLeakyBucketMatchesTokenBucket`.

- [ ] **Step 5: Write the decision record**

Create `docs/decisions/0001-leaky-bucket-is-token-bucket.md`:

```markdown
# 0001: Leaky bucket ships as a meter, and equals token bucket

## Decision

`LeakyBucketLimiter` implements the leaky bucket as a meter: depth rises on
admission and drains at `Params.Rate()`. It is kept despite being equivalent to
`TokenBucketLimiter`, and the equivalence is stated rather than hidden.

## The equivalence

Substitute `Depth = capacity - Tokens`, where `capacity = Limit + Burst`:

| Token bucket | Leaky bucket |
|---|---|
| fresh state is `Tokens = capacity` | fresh state is `Depth = 0` |
| `Tokens = min(capacity, Tokens + accrued)` | `Depth = max(0, Depth - drained)` |
| deny when `Tokens < cost` | deny when `Depth + cost > capacity` |
| `Remaining = Tokens` | `Remaining = capacity - Depth` |
| `RetryAfter = (cost - Tokens) * rate` | `RetryAfter = (Depth + cost - capacity) * rate` |

Each row is the same statement in the other's coordinates. `TestLeakyBucketMatchesTokenBucket`
drives one event sequence through both across three parameter sets and asserts
the `Outcome`s are equal field for field, so the claim is checked on every run.

## Consequences

The algorithm comparison has four distinct behaviours, not five. Benchmarks that
show these two costing the same are measuring that fact, not noise, and no
weakness test can separate them.

A leaky bucket that shaped traffic rather than metering it would differ: it would
hold a request and hand back a scheduled time instead of allow or deny. That
does not fit `Outcome` and belongs to the delivery layer, not to an algorithm.
```

- [ ] **Step 6: Run the full suite and commit**

Run: `go test ./... -race`
Expected: PASS in every package.

```bash
git add domain/leakybucket.go domain/leakybucket_test.go docs/decisions/
git commit -m "Add leaky bucket, and a test proving it equals token bucket

Depth = capacity - Tokens turns one transition into the other, so a single
event sequence must produce identical Outcomes from both. The test drives
three parameter sets through both and compares field for field, which makes
the equivalence something the suite checks rather than something a comment
claims."
```

---

### Task 2: Sliding window log

**Files:**
- Create: `domain/slidingwindowlog.go`
- Test: `domain/slidingwindowlog_test.go`

**Interfaces:**
- Consumes: `domain.Params`, `domain.Outcome`.
- Produces: `domain.SlidingWindowLogState{Events []time.Time}`; `domain.SlidingWindowLogLimiter` (empty struct) with `Lifetime(p Params) time.Duration` and `Apply(s SlidingWindowLogState, now time.Time, p Params, cost int64) (SlidingWindowLogState, Outcome)`. It satisfies `domain.Limiter[SlidingWindowLogState]`.

Only admitted requests are logged, so the slice length is bounded by `Params.Limit` rather than by traffic. Logging denials would make an unbounded slice out of exactly the flood the limiter exists to reject.

`Apply` never mutates the slice it is given: it builds a new one. A caller holding the previous state must still see it unchanged, and the store keeps only what `Apply` returns.

- [ ] **Step 1: Write the failing test**

```go
package domain

import (
	"testing"
	"time"
)

var swlParams = Params{Limit: 5, Window: time.Second}

func TestSlidingWindowLogAllowsLimitThenDenies(t *testing.T) {
	var l SlidingWindowLogLimiter
	now := time.Unix(1_700_000_000, 0)

	s := SlidingWindowLogState{}
	for i := range 5 {
		var out Outcome
		s, out = l.Apply(s, now, swlParams, 1)
		if !out.Allowed {
			t.Fatalf("request %d denied, want allowed", i)
		}
	}

	_, out := l.Apply(s, now, swlParams, 1)
	if out.Allowed {
		t.Fatal("request 6 allowed, want denied")
	}
	if out.RetryAfter != time.Second {
		t.Fatalf("RetryAfter = %v, want 1s (until the oldest event falls out)", out.RetryAfter)
	}
}

func TestSlidingWindowLogSlidesRatherThanResetting(t *testing.T) {
	var l SlidingWindowLogLimiter
	now := time.Unix(1_700_000_000, 0)

	s := SlidingWindowLogState{}
	for i := range 5 {
		s, _ = l.Apply(s, now.Add(time.Duration(i)*100*time.Millisecond), swlParams, 1)
	}

	// 600ms in, the oldest event is 600ms old and still inside the window.
	_, out := l.Apply(s, now.Add(600*time.Millisecond), swlParams, 1)
	if out.Allowed {
		t.Fatal("allowed at 600ms, want denied: all five events are still live")
	}
	if want := 400 * time.Millisecond; out.RetryAfter != want {
		t.Fatalf("RetryAfter = %v, want %v", out.RetryAfter, want)
	}

	// At 1050ms the first event has fallen out and exactly one slot is free.
	s, out = l.Apply(s, now.Add(1050*time.Millisecond), swlParams, 1)
	if !out.Allowed {
		t.Fatal("denied at 1050ms, want allowed: the oldest event has fallen out")
	}
	if out.Remaining != 0 {
		t.Fatalf("Remaining = %d, want 0", out.Remaining)
	}
}

func TestSlidingWindowLogDoesNotRecordDenials(t *testing.T) {
	var l SlidingWindowLogLimiter
	now := time.Unix(1_700_000_000, 0)

	s := SlidingWindowLogState{}
	for range 5 {
		s, _ = l.Apply(s, now, swlParams, 1)
	}
	for range 1000 {
		s, _ = l.Apply(s, now, swlParams, 1)
	}

	if len(s.Events) != 5 {
		t.Fatalf("len(Events) = %d after 1000 denials, want 5: a flood must not grow the log", len(s.Events))
	}
}

func TestSlidingWindowLogDoesNotMutateInputState(t *testing.T) {
	var l SlidingWindowLogLimiter
	now := time.Unix(1_700_000_000, 0)

	before := SlidingWindowLogState{Events: []time.Time{now}}
	kept := before.Events[0]

	if _, _ = l.Apply(before, now.Add(10*time.Millisecond), swlParams, 1); !before.Events[0].Equal(kept) {
		t.Fatalf("Apply mutated the state it was given: Events[0] = %v, want %v", before.Events[0], kept)
	}
	if len(before.Events) != 1 {
		t.Fatalf("len(Events) = %d after Apply, want 1", len(before.Events))
	}
}

// TestSlidingWindowLogCostsOneTimestampPerPermit is evidence, not a bug report.
// State grows with Limit, where fixed window holds a single counter whatever
// the limit is. That is what a sliding window log buys accuracy with.
func TestSlidingWindowLogCostsOneTimestampPerPermit(t *testing.T) {
	var l SlidingWindowLogLimiter
	now := time.Unix(1_700_000_000, 0)

	for _, limit := range []int64{10, 1000, 10_000} {
		p := Params{Limit: limit, Window: time.Hour}

		s := SlidingWindowLogState{}
		for range limit {
			s, _ = l.Apply(s, now, p, 1)
		}

		if int64(len(s.Events)) != limit {
			t.Fatalf("limit %d: len(Events) = %d, want %d", limit, len(s.Events), limit)
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./domain/ -run TestSlidingWindowLog -v`
Expected: FAIL to build, with `undefined: SlidingWindowLogLimiter`.

- [ ] **Step 3: Write the implementation**

```go
package domain

import "time"

// SlidingWindowLogState is the instants of the admitted requests still inside
// the window, oldest first. The zero value is an empty log.
type SlidingWindowLogState struct {
	Events []time.Time
}

// SlidingWindowLogLimiter admits while fewer than Params.Limit requests fall in
// the window ending now. It counts a true sliding window rather than estimating
// one, and holds one instant per admitted permit to do it.
type SlidingWindowLogLimiter struct{}

// Lifetime reports how long a log must outlive its last use: its window plus
// slack, after which every event it holds has fallen out.
func (SlidingWindowLogLimiter) Lifetime(p Params) time.Duration { return 2 * p.Window }

// Apply drops the events that have fallen out of the window, then appends cost
// instants if there is room. Denied requests are not recorded, so the log never
// grows past Limit. The state passed in is left unchanged.
func (SlidingWindowLogLimiter) Apply(s SlidingWindowLogState, now time.Time, p Params, cost int64) (SlidingWindowLogState, Outcome) {
	if p.Window <= 0 {
		return s, Outcome{}
	}

	cutoff := now.Add(-p.Window)
	first := 0
	for first < len(s.Events) && !s.Events[first].After(cutoff) {
		first++
	}
	live := s.Events[first:]

	if int64(len(live))+cost > p.Limit {
		// A denial appends nothing, so the pruned view is shared rather than
		// copied. Copying here would let a flood allocate a slice per rejected
		// request.
		return SlidingWindowLogState{Events: live}, Outcome{
			Remaining:  max(0, p.Limit-int64(len(live))),
			RetryAfter: logRetryAfter(live, now, p, cost),
			ResetAt:    logResetAt(live, now, p),
		}
	}

	next := make([]time.Time, len(live), len(live)+int(cost))
	copy(next, live)
	for range cost {
		next = append(next, now)
	}
	return SlidingWindowLogState{Events: next}, Outcome{
		Allowed:   true,
		Remaining: p.Limit - int64(len(next)),
		ResetAt:   logResetAt(next, now, p),
	}
}

// logRetryAfter reports when enough of the oldest events will have fallen out
// to make room for cost. A cost larger than Limit can never fit, so it waits
// for the whole window.
func logRetryAfter(live []time.Time, now time.Time, p Params, cost int64) time.Duration {
	if len(live) == 0 {
		return p.Window
	}
	need := min(int64(len(live)), int64(len(live))+cost-p.Limit)
	return live[need-1].Add(p.Window).Sub(now)
}

// logResetAt reports when the log is empty again, which is one window past its
// newest event.
func logResetAt(events []time.Time, now time.Time, p Params) time.Time {
	if len(events) == 0 {
		return now
	}
	return events[len(events)-1].Add(p.Window)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./domain/ -run TestSlidingWindowLog -v`
Expected: PASS for all five tests.

- [ ] **Step 5: Run the full suite and commit**

Run: `go test ./... -race`
Expected: PASS in every package.

```bash
git add domain/slidingwindowlog.go domain/slidingwindowlog_test.go
git commit -m "Add sliding window log

Only admitted requests are logged, so the slice is bounded by Limit rather
than by traffic: recording denials would let the flood the limiter exists to
reject grow the state without bound. The cost test records the tradeoff, one
timestamp per permit against fixed window's single counter."
```

---

### Task 3: Sliding window counter, and both directions of its error

**Files:**
- Create: `domain/slidingwindowcounter.go`
- Test: `domain/slidingwindowcounter_test.go`

**Interfaces:**
- Consumes: `domain.Params`, `domain.Outcome`.
- Produces: `domain.SlidingWindowCounterState{Start time.Time; Prev int64; Curr int64}`; `domain.SlidingWindowCounterLimiter` (empty struct) with `Lifetime(p Params) time.Duration` and `Apply(s SlidingWindowCounterState, now time.Time, p Params, cost int64) (SlidingWindowCounterState, Outcome)`. It satisfies `domain.Limiter[SlidingWindowCounterState]`.

The estimate is `Curr + Prev * overlap / Window`, where `overlap` is how much of the previous window the trailing window still covers. That weighting assumes the previous window's traffic was spread evenly through it. It was not, in general, and the two weakness tests pin the consequence in both directions.

`Lifetime` is `3 * Window`: `Prev` still carries weight for a full window after the current one begins, so state matters for two windows, plus slack.

- [ ] **Step 1: Write the failing test**

```go
package domain

import (
	"testing"
	"time"
)

var swcParams = Params{Limit: 10, Window: time.Second}

func TestSlidingWindowCounterAllowsLimitThenDenies(t *testing.T) {
	var l SlidingWindowCounterLimiter
	now := time.Unix(1_700_000_000, 0)

	s := SlidingWindowCounterState{}
	for i := range 10 {
		var out Outcome
		s, out = l.Apply(s, now, swcParams, 1)
		if !out.Allowed {
			t.Fatalf("request %d denied, want allowed", i)
		}
	}

	_, out := l.Apply(s, now, swcParams, 1)
	if out.Allowed {
		t.Fatal("request 11 allowed, want denied")
	}
	if out.Remaining != 0 {
		t.Fatalf("Remaining = %d, want 0", out.Remaining)
	}
}

func TestSlidingWindowCounterRollsCurrentIntoPrevious(t *testing.T) {
	var l SlidingWindowCounterLimiter
	base := time.Unix(1_700_000_000, 0)

	s := SlidingWindowCounterState{}
	for range 10 {
		s, _ = l.Apply(s, base, swcParams, 1)
	}

	s, _ = l.Apply(s, base.Add(time.Second), swcParams, 0)
	if s.Prev != 10 {
		t.Fatalf("Prev = %d in the next window, want 10", s.Prev)
	}
	if s.Curr != 0 {
		t.Fatalf("Curr = %d in the next window, want 0", s.Curr)
	}
}

func TestSlidingWindowCounterDropsAStaleWindow(t *testing.T) {
	var l SlidingWindowCounterLimiter
	base := time.Unix(1_700_000_000, 0)

	s := SlidingWindowCounterState{}
	for range 10 {
		s, _ = l.Apply(s, base, swcParams, 1)
	}

	// Two windows on, the old count weighs nothing.
	s, out := l.Apply(s, base.Add(2*time.Second), swcParams, 1)
	if !out.Allowed {
		t.Fatal("denied two windows later, want allowed")
	}
	if s.Prev != 0 {
		t.Fatalf("Prev = %d two windows later, want 0", s.Prev)
	}
}

// TestSlidingWindowCounterUnderCountsATrailingBurst is evidence, not a bug
// report. The estimate weights the previous window uniformly. Cluster that
// window's traffic at its end and the weighting discounts requests that are in
// fact recent, so more than Limit are admitted in one window-long span.
func TestSlidingWindowCounterUnderCountsATrailingBurst(t *testing.T) {
	var l SlidingWindowCounterLimiter
	base := time.Unix(1_700_000_000, 0)
	burstAt := base.Add(900 * time.Millisecond)

	s := SlidingWindowCounterState{}
	for range 10 {
		s, _ = l.Apply(s, burstAt, swcParams, 1)
	}

	// Halfway through the next window, the estimate discounts the burst by half
	// even though every one of those requests is inside the trailing second.
	at := base.Add(1500 * time.Millisecond)
	admitted := 0
	for range 10 {
		var out Outcome
		s, out = l.Apply(s, at, swcParams, 1)
		if out.Allowed {
			admitted++
		}
	}

	if admitted != 5 {
		t.Fatalf("admitted = %d at the half-window mark, want 5", admitted)
	}
	// Everything above is inside [at-Window, at], so the true count is 15.
	if total := 10 + admitted; total <= 10 {
		t.Fatalf("total in the trailing window = %d, want more than the limit of 10", total)
	}
}

// TestSlidingWindowCounterOverCountsALeadingBurst is the same flaw in the other
// direction. Cluster the previous window's traffic at its start and the
// weighting charges for requests that have already left the trailing window, so
// requests are denied while the true count is under the limit.
func TestSlidingWindowCounterOverCountsALeadingBurst(t *testing.T) {
	var l SlidingWindowCounterLimiter
	base := time.Unix(1_700_000_000, 0)

	s := SlidingWindowCounterState{}
	for range 10 {
		s, _ = l.Apply(s, base, swcParams, 1)
	}

	// At 1500ms the trailing second is [500ms, 1500ms]. None of the ten
	// requests at 0ms fall in it, so the true count is whatever we admit now.
	at := base.Add(1500 * time.Millisecond)
	admitted := 0
	for range 10 {
		var out Outcome
		s, out = l.Apply(s, at, swcParams, 1)
		if out.Allowed {
			admitted++
		}
	}

	if admitted != 5 {
		t.Fatalf("admitted = %d, want 5", admitted)
	}
	if admitted >= 10 {
		t.Fatalf("admitted = %d, want fewer than the limit of 10: the estimate charges for requests that have left the window", admitted)
	}
}

func TestSlidingWindowCounterLifetimeCoversThePreviousWindow(t *testing.T) {
	var l SlidingWindowCounterLimiter
	p := Params{Limit: 10, Window: time.Second}

	if got, want := l.Lifetime(p), 2*p.Window; got < want {
		t.Fatalf("Lifetime = %v, want at least %v: Prev carries weight for a second window", got, want)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./domain/ -run TestSlidingWindowCounter -v`
Expected: FAIL to build, with `undefined: SlidingWindowCounterLimiter`.

- [ ] **Step 3: Write the implementation**

```go
package domain

import "time"

// SlidingWindowCounterState is the current window's start and the counts for it
// and the window before it. The zero value is an expired pair, which resets on
// first use.
type SlidingWindowCounterState struct {
	Start time.Time
	Prev  int64
	Curr  int64
}

// SlidingWindowCounterLimiter estimates the count in the trailing window as the
// current window's count plus the previous window's, weighted by how much of it
// the trailing window still covers. It costs two counters instead of one
// timestamp per permit, and is exact only when the previous window's traffic
// was spread evenly through it.
type SlidingWindowCounterLimiter struct{}

// Lifetime reports how long a counter pair must outlive its last use. Prev
// still carries weight through the whole of the current window, so the pair
// matters for two windows, plus slack.
func (SlidingWindowCounterLimiter) Lifetime(p Params) time.Duration { return 3 * p.Window }

// Apply rolls the window forward if now has left the stored one, then takes
// cost against the weighted estimate.
func (SlidingWindowCounterLimiter) Apply(s SlidingWindowCounterState, now time.Time, p Params, cost int64) (SlidingWindowCounterState, Outcome) {
	if p.Window <= 0 {
		return s, Outcome{}
	}

	start := now.Truncate(p.Window)
	switch {
	case s.Start.Equal(start):
	case s.Start.Add(p.Window).Equal(start):
		s = SlidingWindowCounterState{Start: start, Prev: s.Curr}
	default:
		s = SlidingWindowCounterState{Start: start}
	}

	overlap := p.Window - now.Sub(start)
	estimate := s.Curr + s.Prev*int64(overlap)/int64(p.Window)
	resetAt := start.Add(p.Window)

	if estimate+cost > p.Limit {
		return s, Outcome{
			Remaining:  max(0, p.Limit-estimate),
			RetryAfter: resetAt.Sub(now),
			ResetAt:    resetAt,
		}
	}

	s.Curr += cost
	return s, Outcome{
		Allowed:   true,
		Remaining: p.Limit - (estimate + cost),
		ResetAt:   resetAt,
	}
}
```

`RetryAfter` waits for the window boundary. The estimate in fact decays continuously as `overlap` shrinks, so a caller may be admitted sooner; the boundary is an upper bound, never an under-estimate.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./domain/ -run TestSlidingWindowCounter -v`
Expected: PASS for all six tests, including both weakness tests.

- [ ] **Step 5: Run the full suite and commit**

Run: `go test ./... -race`
Expected: PASS in every package.

```bash
git add domain/slidingwindowcounter.go domain/slidingwindowcounter_test.go
git commit -m "Add sliding window counter with both directions of its error

The weighted estimate assumes the previous window's traffic was spread evenly
through it. The two evidence tests pin what happens when it was not: a burst
at that window's end is discounted and admits 15 against a limit of 10, and a
burst at its start is charged for after leaving the trailing window and denies
at 5. Neither is a defect to fix; both are the algorithm."
```

---

### Task 4: Register the three, and widen the conformance suite

**Files:**
- Modify: `adapter/memory/store.go` — the `algorithms` map in `New`
- Modify: `storetest/conformance.go`
- Modify: `adapter/memory/store_test.go`

**Interfaces:**
- Consumes: the three limiters from tasks 1 through 3.
- Produces: `storetest.RunConformance(t *testing.T, newHarness func(t *testing.T) Harness, algos ...domain.Algorithm)` — the algorithms an adapter claims to support. Passing none runs the clauses against all five.

Today every clause runs against `domain.FixedWindow` only, so an adapter that implements one algorithm correctly and another incorrectly passes the whole suite. That stops being theoretical with five algorithms and two Redis adapters coming.

Clauses 1, 2 and 4 are algorithm-agnostic and run per algorithm. Clause 3 keeps its fixed-window boundary assertion, which is the tightest available and is what the clause is really about; clause 5 concerns errors and is algorithm-irrelevant. Both run once.

At `Params{Limit: 100, Window: time.Second}` with `Burst: 0` and a frozen clock, all five algorithms admit exactly 100 and report `Remaining: 99` after one take, so the existing assertions hold unchanged for each.

- [ ] **Step 1: Write the failing test**

In `adapter/memory/store_test.go`, replace the body of `TestStoreConformance` and add a dispatch test:

```go
func TestStoreConformance(t *testing.T) {
	storetest.RunConformance(t, func(t *testing.T) storetest.Harness {
		clk := clock.NewFake(time.Unix(1_700_000_000, 0))
		s := New(clk)
		t.Cleanup(func() { _ = s.Close() })
		return storetest.Harness{Store: s, Now: clk.Now, Advance: clk.Advance}
	})
}

func TestStoreDispatchesOnEveryAlgorithm(t *testing.T) {
	algos := []domain.Algorithm{
		domain.TokenBucket,
		domain.LeakyBucket,
		domain.FixedWindow,
		domain.SlidingWindowLog,
		domain.SlidingWindowCounter,
	}

	for _, algo := range algos {
		t.Run(string(algo), func(t *testing.T) {
			clk := clock.NewFake(time.Unix(1_700_000_000, 0))
			s := New(clk)
			t.Cleanup(func() { _ = s.Close() })

			req := port.Request{
				RuleID:    "rule",
				Key:       "subject",
				Algorithm: algo,
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
		})
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./adapter/memory/ -run 'TestStoreDispatchesOnEveryAlgorithm' -v`
Expected: FAIL, with `memory: unsupported algorithm "leaky_bucket"` for the three unregistered algorithms.

- [ ] **Step 3: Register the three algorithms**

In `adapter/memory/store.go`, extend the `algorithms` map in `New` with three entries following the existing pattern exactly:

```go
			domain.LeakyBucket: {
				apply:    erase[domain.LeakyBucketState](domain.LeakyBucketLimiter{}),
				lifetime: domain.LeakyBucketLimiter{}.Lifetime,
			},
			domain.SlidingWindowLog: {
				apply:    erase[domain.SlidingWindowLogState](domain.SlidingWindowLogLimiter{}),
				lifetime: domain.SlidingWindowLogLimiter{}.Lifetime,
			},
			domain.SlidingWindowCounter: {
				apply:    erase[domain.SlidingWindowCounterState](domain.SlidingWindowCounterLimiter{}),
				lifetime: domain.SlidingWindowCounterLimiter{}.Lifetime,
			},
```

- [ ] **Step 4: Run the dispatch test to verify it passes**

Run: `go test ./adapter/memory/ -run 'TestStoreDispatchesOnEveryAlgorithm' -v`
Expected: PASS for all five subtests.

- [ ] **Step 5: Widen the conformance suite across algorithms**

In `storetest/conformance.go`, thread an algorithm through the request helpers and run the agnostic clauses per algorithm.

Replace the `req`/`ruleReq` helpers and `RunConformance` with:

```go
// algorithms is what RunConformance drives when a caller names none.
var algorithms = []domain.Algorithm{
	domain.TokenBucket,
	domain.LeakyBucket,
	domain.FixedWindow,
	domain.SlidingWindowLog,
	domain.SlidingWindowCounter,
}

func req(algo domain.Algorithm, key domain.Key) port.Request {
	return ruleReq(algo, "rule-a", key)
}

func ruleReq(algo domain.Algorithm, rule domain.RuleID, key domain.Key) port.Request {
	return port.Request{
		RuleID:    rule,
		Key:       key,
		Algorithm: algo,
		Params:    domain.Params{Limit: 100, Window: window},
		Cost:      1,
	}
}

// RunConformance asserts the port.Store contract against newHarness, for each
// algorithm the adapter claims to support. Naming none means all five.
//
// Clauses 1, 2 and 4 hold whatever the algorithm, and run for each. Clause 3
// asserts a fixed window's boundary, which is the tightest statement of "the
// store owns the clock" available, and clause 5 concerns errors rather than
// arithmetic; both run once.
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
		})
	}

	t.Run("clause 3: store owns the clock", func(t *testing.T) { clauseStoreOwnsClock(t, newHarness) })
	t.Run("clause 5: errors carry no policy", func(t *testing.T) { clauseErrorsCarryNoPolicy(t, newHarness) })
}
```

Then give `clauseAtomicPerKey`, `clauseKeysIndependent` and `clauseMissingStateIsFull` a trailing `algo domain.Algorithm` parameter and pass it into every `req(...)` and `ruleReq(...)` call inside them. Leave their assertions exactly as they are. `clauseStoreOwnsClock` and `clauseErrorsCarryNoPolicy` keep their signatures and call `req(domain.FixedWindow, ...)`.

- [ ] **Step 6: Run the suite to verify every algorithm conforms**

Run: `go test ./adapter/memory/ -race -v -run TestStoreConformance`
Expected: PASS, with clause 1, 2 and 4 subtests under each of the five algorithm names, plus clauses 3 and 5 once.

- [ ] **Step 7: Run the full suite and commit**

Run: `go test ./... -race`
Expected: PASS in every package.

```bash
git add adapter/memory/store.go adapter/memory/store_test.go storetest/conformance.go
git commit -m "Register the remaining algorithms and widen the conformance suite

Every clause ran against fixed window alone, so an adapter implementing one
algorithm correctly and another incorrectly passed the whole suite. The
agnostic clauses now run per algorithm, which is what the two Redis adapters
will be held to."
```

---

### Task 5: Benchmark what each algorithm costs

**Files:**
- Modify: `bench_test.go`
- Modify: `docs/benchmarks.md`

**Interfaces:**
- Consumes: `ratelimit.Service`, `memory.Store`, all five algorithms.
- Produces: `BenchmarkCheckUncontended`, `BenchmarkStateBytesPerKey`. The existing `BenchmarkCheck` keeps its name and behaviour.

`BenchmarkCheck` contends every goroutine on one key, so mutex contention swamps the difference between algorithms — which is why its table refuses to rank them. An uncontended variant gives each iteration its own key and measures what the algorithm itself costs. Both are worth having: one is the cost under a hot key, the other the cost of the arithmetic.

`BenchmarkStateBytesPerKey` measures the heap held by saturated state across many keys, which is the sliding window log's cost claim expressed as a number.

Both new benchmarks measure a saturated limiter: each goroutine's key fills to `Limit` within the first iterations and denies thereafter, so the figures are the steady-state cost under sustained load rather than the cost of the first thousand requests. That is the state a limiter spends its life in, and it is the one where the sliding window log's per-request prune actually shows up. Say so in the recorded table.

- [ ] **Step 1: Write the benchmarks**

Add to `bench_test.go`, keeping the existing `BenchmarkCheck` as it is:

```go
var benchAlgorithms = []domain.Algorithm{
	domain.TokenBucket,
	domain.LeakyBucket,
	domain.FixedWindow,
	domain.SlidingWindowLog,
	domain.SlidingWindowCounter,
}

// BenchmarkCheckUncontended gives every iteration its own key, so the figure is
// the algorithm's own cost rather than the cost of waiting for one key's mutex.
func BenchmarkCheckUncontended(b *testing.B) {
	for _, algo := range benchAlgorithms {
		b.Run(string(algo), func(b *testing.B) {
			store := memory.New(clock.System{}, memory.WithSweepInterval(0))
			b.Cleanup(func() { _ = store.Close() })
			svc := New(store)

			var n atomic.Int64

			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				// One key per goroutine: no two goroutines share a mutex, and
				// the map holds GOMAXPROCS entries rather than one per
				// iteration.
				req := CheckRequest{
					RuleID:    "bench",
					Key:       domain.Key(strconv.FormatInt(n.Add(1), 10)),
					Algorithm: algo,
					Params:    domain.Params{Limit: 1000, Window: time.Hour},
					Cost:      1,
				}
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

// BenchmarkStateBytesPerKey reports the heap held per saturated key. It is the
// memory cost of accuracy: the sliding window log keeps one instant per permit
// where the counter algorithms keep two integers.
func BenchmarkStateBytesPerKey(b *testing.B) {
	const (
		keys  = 2_000
		limit = 64
	)

	for _, algo := range benchAlgorithms {
		b.Run(string(algo), func(b *testing.B) {
			b.ReportAllocs()

			var bytesPerKey float64
			for b.Loop() {
				store := memory.New(clock.System{}, memory.WithSweepInterval(0))
				svc := New(store)
				ctx := context.Background()

				runtime.GC()
				var before runtime.MemStats
				runtime.ReadMemStats(&before)

				for k := range keys {
					key := domain.Key(strconv.Itoa(k))
					for range limit {
						if _, err := svc.Check(ctx, CheckRequest{
							RuleID:    "bench",
							Key:       key,
							Algorithm: algo,
							Params:    domain.Params{Limit: limit, Window: time.Hour},
							Cost:      1,
						}); err != nil {
							b.Fatal(err)
						}
					}
				}

				runtime.GC()
				var after runtime.MemStats
				runtime.ReadMemStats(&after)

				bytesPerKey = float64(after.HeapAlloc-before.HeapAlloc) / keys
				runtime.KeepAlive(store)
				_ = store.Close()
			}
			b.ReportMetric(bytesPerKey, "bytes/key")
		})
	}
}
```

Add `"runtime"`, `"strconv"` and `"sync/atomic"` to the file's imports.

- [ ] **Step 2: Run the benchmarks**

Run: `go test -bench='BenchmarkCheckUncontended|BenchmarkStateBytesPerKey' -benchmem -run '^$' -count=10 . | tee /tmp/bench2.txt`
Expected: results for five algorithms in each benchmark.

If `BenchmarkStateBytesPerKey` reports a negative or wildly unstable `bytes/key`, the GC is reclaiming between the two reads. Say so in your report and publish only the benchmark that gave stable numbers rather than publishing an unstable one.

- [ ] **Step 3: Record the measurements**

Rewrite `docs/benchmarks.md` to hold three sections, keeping the file's existing style and its opening two lines. The first section is the current contended table, unchanged. Add:

```markdown
## Per-algorithm decision cost, uncontended

Every iteration takes its own key, so this is the algorithm's own cost rather
than the cost of waiting for one key's mutex.

    go test -bench=BenchmarkCheckUncontended -benchmem -run '^$' -count=10 .

| Algorithm | ns/op (min) | ns/op (median) | ns/op (max) | B/op | allocs/op |
|---|---|---|---|---|---|

## State held per saturated key

Each key is driven to its limit of 64 permits, then the heap is measured.

    go test -bench=BenchmarkStateBytesPerKey -benchmem -run '^$' -count=10 .

| Algorithm | bytes/key (median) |
|---|---|
```

Fill every cell from the run in step 2. Compute min, median and max from the ten samples yourself; do not install a tool. Publish no number you did not measure, and state no ranking the samples' ranges do not support — where two algorithms' ranges overlap, say so rather than ordering them.

Leaky bucket and token bucket are the same transition in different coordinates, per `docs/decisions/0001-leaky-bucket-is-token-bucket.md`. If their figures match, note that this is the equivalence rather than a coincidence.

- [ ] **Step 4: Run the full suite and commit**

Run: `go test ./... -race && go vet ./...`
Expected: PASS in every package, vet silent.

```bash
git add bench_test.go docs/benchmarks.md
git commit -m "Measure per-algorithm cost without contention, and state per key

The existing benchmark contends every goroutine on one key, so it measures the
mutex rather than the algorithm. The uncontended variant gives each iteration
its own key, and the memory benchmark puts a number on what the sliding window
log's accuracy costs."
```

---

## Definition of Done

- [ ] `go test ./... -race` passes.
- [ ] `go vet ./...` is clean.
- [ ] `adapter/memory` passes `RunConformance` for all five algorithms.
- [ ] `TestLeakyBucketMatchesTokenBucket` passes, and `docs/decisions/0001-leaky-bucket-is-token-bucket.md` states the equivalence it checks.
- [ ] Each new algorithm has a test demonstrating its weakness: the log's one-timestamp-per-permit cost, the counter's error in both directions.
- [ ] `docs/benchmarks.md` holds three tables of measured numbers, each with the command that reproduces it.
- [ ] `domain/` still imports only the standard library: `go list -deps ./domain/ ./port/ | grep -v '^github.com/tunedev/rate_limiter' | grep '\.'` returns nothing.

## What this plan deliberately leaves out

- Redis. Plan 3 adds `redis/lua` and `redis/cas` against the conformance suite this plan widens.
- Rules, groups and precedence. `CheckRequest` still carries an explicit algorithm and params until plan 4.
- The otel observer, the HTTP middleware, and the standalone service.
