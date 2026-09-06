# Redis Adapters Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Two Redis store adapters — one shipping Lua, one doing a WATCH retry loop — measured against each other so an ADR can recommend one on evidence.

**Architecture:** Both implement `port.Store` and pass `storetest.RunConformance` unmodified. `redis/lua` duplicates each transition as a script and answers in one round trip; `redis/cas` calls the same Go transition the memory adapter does and pays retries. The comparison is the deliverable: the spec ships both so the hot-key contention benchmark, not an argument, picks the recommendation.

**Tech Stack:** Go 1.27.0, `github.com/redis/go-redis/v9` v9.22.0 (already in `go.mod`), Redis 8.2.2 (`redis-server` on PATH; tests spawn their own).

**Spec:** `docs/superpowers/specs/2026-09-05-rate-limiter-design.md`

## Global Constraints

- `domain/` and `port/` import the standard library only. No dependency may reach inward — go-redis is imported by `adapter/redis/...` and nothing else.
- Transitions stay pure. Nothing in `domain/` learns that Redis exists.
- Both adapters call `storetest.RunConformance` **unmodified**. If a clause fails, the adapter is wrong or the finding is real — do not edit the suite to accommodate an adapter.
- Scope is **token bucket and fixed window only**, in both adapters. `RunConformance`'s variadic `algos` exists for exactly this. The other three algorithms are out of scope.
- The store owns the clock: both adapters take `now` from Redis `TIME`, never from a Go clock.
- **Redis Lua numbers are IEEE doubles, exact only below 2^53.** A Unix nanosecond timestamp is ~1.7e18 and loses precision; microseconds are ~1.7e15 and are exact with 5.3x headroom. Every Lua script and every value crossing the Go/Lua boundary works in **microseconds**. Redis-backed limits therefore have microsecond resolution — say so where it is user-visible.
- `min` and `max` are Go builtins.
- No emojis. Comments describe current behaviour only — no history, no rationale narratives.
- Test command for the whole repo: `go test ./...`

## Existing interfaces these tasks build on

- `domain.Params{Limit int64; Window time.Duration; Burst int64}`, `Rate() time.Duration`, `Validate(Algorithm) error`.
- `domain.Outcome{Allowed bool; Remaining int64; RetryAfter time.Duration; ResetAt time.Time}`.
- `domain.Limiter[S any]` with `Apply(s S, now time.Time, p Params, cost int64) (S, Outcome)` **and** `Lifetime(Params) time.Duration`.
- `domain.TokenBucketLimiter` over `TokenBucketState{Tokens int64; At time.Time}`; `domain.FixedWindowLimiter` over `FixedWindowState{Start time.Time; Count int64}`.
- `port.Store` with `Apply(ctx, Request) (domain.Outcome, error)` and its six-clause contract; `port.Request{RuleID, Key, Algorithm, Params, Cost}`.
- `storetest.Harness{Store port.Store; Now func() time.Time; Advance func(time.Duration)}` and `RunConformance(t, newHarness, algos ...domain.Algorithm)`.
- `adapter/memory.New(clk port.Clock, opts ...Option) *Store` — the reference adapter and the differential test's control.

---

### Task 1: Truncate windows from the Unix epoch

**Files:**
- Modify: `domain/clamp.go` (add the helper beside `clampElapsed`)
- Modify: `domain/fixedwindow.go`
- Modify: `domain/slidingwindowcounter.go`
- Test: `domain/clamp_test.go`

**Interfaces:**
- Produces: unexported `truncateFromEpoch(t time.Time, window time.Duration) time.Time`.

`time.Truncate` rounds down to a multiple of the window measured from the zero time (January 1, year 1), not from the Unix epoch. For second, minute, hour and day windows the two agree, so nothing on disk changes behaviour. For a 7-day window they differ by 96 hours and for a 30-day window by 48 hours — and a Lua script truncating on `TIME`'s Unix seconds computes the epoch-based boundary. Aligning Go to the epoch is what lets the two languages agree by construction.

- [ ] **Step 1: Write the failing test**

Add to `domain/clamp_test.go`:

```go
func TestTruncateFromEpochMatchesUnixBoundaries(t *testing.T) {
	base := time.Unix(1_700_000_000, 0).UTC()

	cases := []struct {
		name   string
		window time.Duration
		want   time.Time
	}{
		{"second", time.Second, time.Unix(1_700_000_000, 0).UTC()},
		{"minute", time.Minute, time.Unix(1_699_999_980, 0).UTC()},
		{"hour", time.Hour, time.Unix(1_699_999_200, 0).UTC()},
		{"day", 24 * time.Hour, time.Unix(1_699_920_000, 0).UTC()},
		{"week", 7 * 24 * time.Hour, time.Unix(1_699_574_400, 0).UTC()},
		{"thirty days", 30 * 24 * time.Hour, time.Unix(1_697_760_000, 0).UTC()},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := truncateFromEpoch(base, c.window)
			if !got.Equal(c.want) {
				t.Fatalf("truncateFromEpoch = %v, want %v", got.UTC(), c.want)
			}
			if got.UnixNano()%int64(c.window) != 0 {
				t.Fatalf("result %v is not a whole number of %v from the epoch", got.UTC(), c.window)
			}
		})
	}
}

// TestTruncateFromEpochDivergesFromTruncateOnlyForMultiDayWindows records why
// this helper exists. Windows that divide the offset between the zero time and
// the epoch land identically either way; longer ones do not.
func TestTruncateFromEpochDivergesFromTruncateOnlyForMultiDayWindows(t *testing.T) {
	base := time.Unix(1_700_000_000, 0).UTC()

	for _, w := range []time.Duration{time.Second, time.Minute, time.Hour, 24 * time.Hour} {
		if got, want := truncateFromEpoch(base, w), base.Truncate(w); !got.Equal(want) {
			t.Fatalf("window %v: truncateFromEpoch = %v, time.Truncate = %v, want agreement", w, got, want)
		}
	}

	for _, w := range []time.Duration{7 * 24 * time.Hour, 30 * 24 * time.Hour} {
		if truncateFromEpoch(base, w).Equal(base.Truncate(w)) {
			t.Fatalf("window %v: expected the two truncations to differ", w)
		}
	}
}

func TestTruncateFromEpochHandlesNonPositiveWindow(t *testing.T) {
	base := time.Unix(1_700_000_000, 0).UTC()
	if got := truncateFromEpoch(base, 0); !got.Equal(base) {
		t.Fatalf("truncateFromEpoch with a zero window = %v, want %v unchanged", got, base)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./domain/ -run TestTruncateFromEpoch -v`
Expected: FAIL to build, `undefined: truncateFromEpoch`.

- [ ] **Step 3: Write the helper**

Add to `domain/clamp.go`:

```go
// truncateFromEpoch rounds t down to a whole number of windows measured from
// the Unix epoch. time.Truncate measures from the zero time instead, which for
// windows longer than a day lands on a different boundary than any system
// computing from Unix seconds.
func truncateFromEpoch(t time.Time, window time.Duration) time.Time {
	if window <= 0 {
		return t
	}
	n, w := t.UnixNano(), int64(window)
	q := n / w
	if n < 0 && q*w != n {
		q--
	}
	return time.Unix(0, q*w).UTC()
}
```

- [ ] **Step 4: Use it in both window algorithms**

In `domain/fixedwindow.go`'s `Apply`, replace `start := now.Truncate(p.Window)` with `start := truncateFromEpoch(now, p.Window)`.

In `domain/slidingwindowcounter.go`'s `Apply`, replace `start := now.Truncate(p.Window)` with `start := truncateFromEpoch(now, p.Window)`.

Change nothing else in either file. Their existing tests use second-scale windows and must keep their current expectations.

- [ ] **Step 5: Run the full suite**

Run: `go test ./... -race`
Expected: PASS in every package, with no existing expectation changed. If a pre-existing test now fails, stop and report it — that would mean a window somewhere is longer than a day and the change is user-visible.

- [ ] **Step 6: Commit**

```bash
git add domain/
git commit -m "Truncate windows from the Unix epoch

Window boundaries are now a whole number of windows from the epoch rather
than from the zero time, so a script computing on Unix seconds lands on the
same boundary. Windows of a day or less are unaffected."
```

---

### Task 2: Redis test server and the shared key layout

**Files:**
- Create: `internal/redistest/server.go`
- Create: `adapter/redis/keys.go`
- Test: `adapter/redis/keys_test.go`
- Test: `internal/redistest/server_test.go`
- Modify: `go.mod`, `go.sum` (via `go mod tidy`)

**Interfaces:**
- Produces: `redistest.Start(t *testing.T) *redis.Client` — a client bound to a fresh `redis-server` subprocess, flushed and closed on cleanup; skips the test when `redis-server` is not on PATH. `rediskey.Key(rule domain.RuleID, key domain.Key) string`.

The server binds a unix socket inside `t.TempDir()` rather than a TCP port, so there is no port-allocation race between parallel packages.

- [ ] **Step 1: Write the failing tests**

`adapter/redis/keys_test.go`:

```go
package rediskey

import (
	"strings"
	"testing"

	"github.com/tunedev/rate_limiter/domain"
)

func TestKeyMatchesTheDocumentedLayout(t *testing.T) {
	got := Key("per-ip", "203.0.113.7")
	if want := "rl:v1:{per-ip/203.0.113.7}"; got != want {
		t.Fatalf("Key = %q, want %q", got, want)
	}
}

func TestKeyPutsRuleAndSubjectInOneHashTag(t *testing.T) {
	got := Key("per-ip", "203.0.113.7")

	open, close := strings.Index(got, "{"), strings.Index(got, "}")
	if open < 0 || close < 0 || close < open {
		t.Fatalf("Key = %q, want a single hash tag", got)
	}
	if tag := got[open+1 : close]; tag != "per-ip/203.0.113.7" {
		t.Fatalf("hash tag = %q, want the rule and subject together", tag)
	}
}

func TestKeySeparatesRulesOverOneSubject(t *testing.T) {
	if Key("route", "user:42") == Key("tenant", "user:42") {
		t.Fatal("two rules over one subject share a key, want them separate")
	}
}
```

`internal/redistest/server_test.go`:

```go
package redistest

import (
	"context"
	"testing"
)

func TestStartGivesAWorkingEmptyServer(t *testing.T) {
	c := Start(t)
	ctx := context.Background()

	if err := c.Ping(ctx).Err(); err != nil {
		t.Fatalf("Ping: %v", err)
	}

	n, err := c.DBSize(ctx).Result()
	if err != nil {
		t.Fatalf("DBSize: %v", err)
	}
	if n != 0 {
		t.Fatalf("DBSize = %d on a fresh server, want 0", n)
	}
}

func TestStartGivesEachCallerItsOwnServer(t *testing.T) {
	ctx := context.Background()

	a := Start(t)
	if err := a.Set(ctx, "shared", "1", 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}

	b := Start(t)
	if n, err := b.Exists(ctx, "shared").Result(); err != nil {
		t.Fatalf("Exists: %v", err)
	} else if n != 0 {
		t.Fatal("a key written to one server is visible from another, want isolation")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/redistest/ ./adapter/redis/ -v`
Expected: FAIL to build, `undefined: Start` and `undefined: Key`.

- [ ] **Step 3: Write the implementations**

`adapter/redis/keys.go`:

```go
// Package rediskey builds the keys both Redis store adapters use.
package rediskey

import "github.com/tunedev/rate_limiter/domain"

// Key returns the key holding one rule's state for one subject. The hash tag
// wraps both, so a rule and subject land in one cluster slot; nothing needs to
// colocate beyond that, because the store contract forbids cross-key atomicity.
func Key(rule domain.RuleID, key domain.Key) string {
	return "rl:v1:{" + string(rule) + "/" + string(key) + "}"
}
```

`internal/redistest/server.go`:

```go
// Package redistest runs a real redis-server for tests that need one.
package redistest

import (
	"context"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Start runs a redis-server bound to a unix socket in the test's temp
// directory and returns a client for it. The server is killed and the client
// closed when the test finishes. A test calling this is skipped when
// redis-server is not installed.
func Start(t *testing.T) *redis.Client {
	t.Helper()

	bin, err := exec.LookPath("redis-server")
	if err != nil {
		t.Skip("redis-server not on PATH")
	}

	sock := filepath.Join(t.TempDir(), "redis.sock")
	cmd := exec.Command(bin,
		"--port", "0",
		"--unixsocket", sock,
		"--save", "",
		"--appendonly", "no",
	)
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting redis-server: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	})

	client := redis.NewClient(&redis.Options{Network: "unix", Addr: sock})
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for {
		if err := client.Ping(ctx).Err(); err == nil {
			return client
		}
		select {
		case <-ctx.Done():
			t.Fatalf("redis-server did not become ready on %s", sock)
		case <-time.After(10 * time.Millisecond):
		}
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go mod tidy && go test ./internal/redistest/ ./adapter/redis/ -v`
Expected: PASS. `go.mod` should now list go-redis as a direct dependency rather than indirect.

- [ ] **Step 5: Confirm the inward-dependency rule still holds**

Run: `go list -deps ./domain/ ./port/ | grep -v '^github.com/tunedev/rate_limiter' | grep '\.'`
Expected: no output. go-redis must not have reached `domain/` or `port/`.

- [ ] **Step 6: Commit**

```bash
git add go.mod go.sum internal/redistest/ adapter/redis/
git commit -m "Add a Redis test server and the shared key layout

Tests spawn their own redis-server on a unix socket in the test temp
directory, so there is no port race and no Docker dependency. Key wraps the
rule and subject in one hash tag, which is the layout the design names."
```

---

### Task 3: The Lua adapter

**Files:**
- Create: `adapter/redis/lua/store.go`
- Create: `adapter/redis/lua/tokenbucket.lua.go`
- Create: `adapter/redis/lua/fixedwindow.lua.go`
- Test: `adapter/redis/lua/store_test.go`

**Interfaces:**
- Consumes: `port.Store`, `port.Request`, `rediskey.Key`, `redistest.Start`, `domain.TokenBucketLimiter{}.Lifetime`, `domain.FixedWindowLimiter{}.Lifetime`.
- Produces: `lua.Store` with `lua.New(client *redis.Client) *Store` and `(*Store).Apply(ctx, port.Request) (domain.Outcome, error)`.

Each script reads `TIME` itself, so a decision costs one round trip and every node agrees on the instant. Scripts work in microseconds throughout. `HMGET` returning one field but not the other means a partially written hash, which the script rejects rather than treating as fresh — a fresh token bucket is a full one, so silently accepting it would hand back capacity.

`redis.NewScript` sends `EVALSHA` and falls back to `EVAL` on `NOSCRIPT`, so no explicit `SCRIPT LOAD` is needed.

- [ ] **Step 1: Write the failing test**

```go
package lua

import (
	"context"
	"testing"
	"time"

	"github.com/tunedev/rate_limiter/domain"
	"github.com/tunedev/rate_limiter/internal/redistest"
	"github.com/tunedev/rate_limiter/port"
	"github.com/tunedev/rate_limiter/storetest"
)

func TestStoreConformance(t *testing.T) {
	storetest.RunConformance(t, func(t *testing.T) storetest.Harness {
		s := New(redistest.Start(t))
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

func TestStoreRejectsUnsupportedAlgorithm(t *testing.T) {
	s := New(redistest.Start(t))

	out, err := s.Apply(context.Background(), port.Request{
		RuleID:    "rule",
		Key:       "subject",
		Algorithm: domain.SlidingWindowLog,
		Params:    domain.Params{Limit: 10, Window: time.Second},
		Cost:      1,
	})
	if err == nil {
		t.Fatal("Apply with an unsupported algorithm returned nil error, want an error")
	}
	if out != (domain.Outcome{}) {
		t.Fatalf("Outcome = %+v on error, want the zero value", out)
	}
}

func TestStoreRejectsPartiallyWrittenState(t *testing.T) {
	client := redistest.Start(t)
	s := New(client)
	ctx := context.Background()

	req := port.Request{
		RuleID:    "rule",
		Key:       "subject",
		Algorithm: domain.TokenBucket,
		Params:    domain.Params{Limit: 10, Window: time.Second},
		Cost:      1,
	}
	if _, err := s.Apply(ctx, req); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	// Half of the hash survives. Treating that as fresh state would hand back
	// a full bucket.
	if err := client.HDel(ctx, "rl:v1:{rule/subject}", "at").Err(); err != nil {
		t.Fatalf("HDel: %v", err)
	}

	out, err := s.Apply(ctx, req)
	if err == nil {
		t.Fatal("Apply over partial state returned nil error, want an error")
	}
	if out != (domain.Outcome{}) {
		t.Fatalf("Outcome = %+v on error, want the zero value", out)
	}
}

func TestStoreSetsATTLFromLifetime(t *testing.T) {
	client := redistest.Start(t)
	s := New(client)
	ctx := context.Background()

	p := domain.Params{Limit: 10, Window: time.Second, Burst: 90}
	if _, err := s.Apply(ctx, port.Request{
		RuleID: "rule", Key: "subject", Algorithm: domain.TokenBucket, Params: p, Cost: 1,
	}); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	ttl, err := client.PTTL(ctx, "rl:v1:{rule/subject}").Result()
	if err != nil {
		t.Fatalf("PTTL: %v", err)
	}

	want := domain.TokenBucketLimiter{}.Lifetime(p)
	if ttl <= 0 || ttl > want {
		t.Fatalf("PTTL = %v, want a positive value no greater than Lifetime %v", ttl, want)
	}
	if ttl < want/2 {
		t.Fatalf("PTTL = %v, want close to Lifetime %v: a TTL from Window alone would expire early", ttl, want)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./adapter/redis/lua/ -v`
Expected: FAIL to build, `undefined: New`.

- [ ] **Step 3: Write the scripts**

`adapter/redis/lua/tokenbucket.lua.go`:

```go
package lua

// tokenBucketSrc mirrors domain.TokenBucketLimiter.Apply. All time is in
// microseconds: Lua numbers are doubles, exact only below 2^53, and a Unix
// nanosecond timestamp is past that.
//
// KEYS[1] state hash
// ARGV: limit, burst, window_us, cost, ttl_ms
// returns: allowed, remaining, retry_after_us, reset_at_us
const tokenBucketSrc = `
local limit  = tonumber(ARGV[1])
local burst  = tonumber(ARGV[2])
local window = tonumber(ARGV[3])
local cost   = tonumber(ARGV[4])
local ttl    = tonumber(ARGV[5])

local capacity = limit + burst
local rate = math.floor(window / limit)
if rate <= 0 then
  return redis.error_reply('rate_limiter: Limit over Window leaves no time between permits')
end

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])

local h = redis.call('HMGET', KEYS[1], 'tokens', 'at')
if (h[1] == false) ~= (h[2] == false) then
  return redis.error_reply('rate_limiter: partial token bucket state')
end

local tokens, at
if h[1] == false then
  tokens, at = capacity, now
else
  tokens, at = tonumber(h[1]), tonumber(h[2])
end

local elapsed = now - at
if elapsed < 0 then
  elapsed = 0
elseif elapsed > window then
  elapsed = window
end

local accrued = math.floor(elapsed / rate)
if accrued > 0 then
  tokens = math.min(capacity, tokens + accrued)
  at = at + accrued * rate
end
if tokens >= capacity then
  at = now
end

local allowed, retry = 1, 0
if tokens < cost then
  allowed = 0
  retry = (cost - tokens) * rate
else
  tokens = tokens - cost
end

redis.call('HSET', KEYS[1], 'tokens', tokens, 'at', at)
redis.call('PEXPIRE', KEYS[1], ttl)

return {allowed, tokens, retry, now + (capacity - tokens) * rate}
`
```

`adapter/redis/lua/fixedwindow.lua.go`:

```go
package lua

// fixedWindowSrc mirrors domain.FixedWindowLimiter.Apply. Windows are whole
// multiples of the window measured from the Unix epoch, which is what
// domain.truncateFromEpoch computes.
//
// KEYS[1] state hash
// ARGV: limit, window_us, cost, ttl_ms
// returns: allowed, remaining, retry_after_us, reset_at_us
const fixedWindowSrc = `
local limit  = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local cost   = tonumber(ARGV[3])
local ttl    = tonumber(ARGV[4])

if window <= 0 then
  return redis.error_reply('rate_limiter: Window must be positive')
end

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])

local start = math.floor(now / window) * window
local reset = start + window

local h = redis.call('HMGET', KEYS[1], 'start', 'count')
if (h[1] == false) ~= (h[2] == false) then
  return redis.error_reply('rate_limiter: partial fixed window state')
end

local count = 0
if h[1] ~= false and tonumber(h[1]) == start then
  count = tonumber(h[2])
end

local allowed, retry, remaining = 1, 0, 0
if count + cost > limit then
  allowed = 0
  remaining = math.max(0, limit - count)
  retry = reset - now
else
  count = count + cost
  remaining = limit - count
end

redis.call('HSET', KEYS[1], 'start', start, 'count', count)
redis.call('PEXPIRE', KEYS[1], ttl)

return {allowed, remaining, retry, reset}
`
```

- [ ] **Step 4: Write the store**

`adapter/redis/lua/store.go`:

```go
// Package lua holds a port.Store that runs each algorithm's transition as a
// Redis script. One round trip per decision, and the script reads TIME itself
// so every node agrees on the instant.
package lua

import (
	"context"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"

	rediskey "github.com/tunedev/rate_limiter/adapter/redis"
	"github.com/tunedev/rate_limiter/domain"
	"github.com/tunedev/rate_limiter/port"
)

// script is one algorithm's transition plus how its arguments are built.
type script struct {
	run      *goredis.Script
	args     func(p domain.Params, cost int64, ttl time.Duration) []any
	lifetime func(domain.Params) time.Duration
}

// Store applies transitions as Redis scripts.
type Store struct {
	client  *goredis.Client
	scripts map[domain.Algorithm]script
}

// New returns a Store issuing commands through client.
func New(client *goredis.Client) *Store {
	return &Store{
		client: client,
		scripts: map[domain.Algorithm]script{
			domain.TokenBucket: {
				run: goredis.NewScript(tokenBucketSrc),
				args: func(p domain.Params, cost int64, ttl time.Duration) []any {
					return []any{p.Limit, p.Burst, micros(p.Window), cost, ttl.Milliseconds()}
				},
				lifetime: domain.TokenBucketLimiter{}.Lifetime,
			},
			domain.FixedWindow: {
				run: goredis.NewScript(fixedWindowSrc),
				args: func(p domain.Params, cost int64, ttl time.Duration) []any {
					return []any{p.Limit, micros(p.Window), cost, ttl.Milliseconds()}
				},
				lifetime: domain.FixedWindowLimiter{}.Lifetime,
			},
		},
	}
}

// Apply runs req's script, which is atomic for req's key by construction.
func (s *Store) Apply(ctx context.Context, req port.Request) (domain.Outcome, error) {
	sc, ok := s.scripts[req.Algorithm]
	if !ok {
		return domain.Outcome{}, fmt.Errorf("lua: unsupported algorithm %q", req.Algorithm)
	}

	key := rediskey.Key(req.RuleID, req.Key)
	raw, err := sc.run.Run(ctx, s.client, []string{key}, sc.args(req.Params, req.Cost, sc.lifetime(req.Params))...).Slice()
	if err != nil {
		return domain.Outcome{}, fmt.Errorf("lua: %w", err)
	}
	if len(raw) != 4 {
		return domain.Outcome{}, fmt.Errorf("lua: script returned %d values, want 4", len(raw))
	}

	fields := make([]int64, 4)
	for i, v := range raw {
		n, ok := v.(int64)
		if !ok {
			return domain.Outcome{}, fmt.Errorf("lua: return value %d is %T, want an integer", i, v)
		}
		fields[i] = n
	}

	return domain.Outcome{
		Allowed:    fields[0] == 1,
		Remaining:  fields[1],
		RetryAfter: time.Duration(fields[2]) * time.Microsecond,
		ResetAt:    time.UnixMicro(fields[3]),
	}, nil
}

// now reports the store's own time, which is the Redis server's.
func (s *Store) now(ctx context.Context) (time.Time, error) {
	return s.client.Time(ctx).Result()
}

// micros converts a duration to whole microseconds, the unit every script
// works in.
func micros(d time.Duration) int64 { return int64(d / time.Microsecond) }
```

- [ ] **Step 5: Run the tests**

Run: `go test ./adapter/redis/lua/ -race -v`
Expected: PASS, including `TestStoreConformance` with clauses 1, 2 and both halves of clause 4 under `token_bucket` and `fixed_window`, plus clauses 3 and 5 once.

Clause 3 and clause 4's second half both call `Advance`, which is a real sleep here, so this package is slower than the memory adapter's. That is expected.

If a conformance clause fails, do not edit the suite. Report which clause and what the adapter returned.

- [ ] **Step 6: Commit**

```bash
git add adapter/redis/lua/
git commit -m "Add the Lua Redis store adapter

Each transition ships as a script that reads TIME itself, so a decision is
one round trip and every node agrees on the instant. Scripts work in
microseconds because Lua numbers are doubles and a Unix nanosecond timestamp
is past 2^53. A partially written hash is rejected rather than read as fresh
state, which for a token bucket would hand back a full bucket."
```

---

### Task 4: The CAS adapter

**Files:**
- Create: `adapter/redis/cas/store.go`
- Create: `adapter/redis/cas/codec.go`
- Test: `adapter/redis/cas/store_test.go`

**Interfaces:**
- Consumes: the same as task 3, plus `domain.TokenBucketLimiter{}.Apply` and `domain.FixedWindowLimiter{}.Apply`.
- Produces: `cas.Store` with `cas.New(client *goredis.Client, opts ...Option) *Store`, `(*Store).Apply(ctx, port.Request) (domain.Outcome, error)`, `(*Store).Retries() int64`; `cas.Option` with `cas.WithMaxAttempts(n int) Option`.

This adapter runs the same Go transitions the memory adapter does, so there is one implementation of each algorithm rather than two. Atomicity comes from `WATCH`: the key is watched, state and `TIME` are read, the transition runs in Go, and the write goes through `TxPipelined`. Redis fails the `EXEC` if the key changed, go-redis reports `redis.TxFailedErr`, and the attempt is retried.

`Retries()` exists for the contention benchmark in task 6 — the retry count under a hot key is the number that decides the ADR.

**Before writing:** confirm how `TIME` behaves inside a watched connection. If it cannot be pipelined with the read, issue it as a separate call on `tx` and say so in your report; the extra round trip is a real cost of this strategy and belongs in the benchmark, not hidden.

- [ ] **Step 1: Write the failing test**

```go
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
		s := New(redistest.Start(t), WithMaxAttempts(200))
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./adapter/redis/cas/ -v`
Expected: FAIL to build, `undefined: New`.

- [ ] **Step 3: Write the codec**

`adapter/redis/cas/codec.go` holds, per algorithm, how its state is read from and written to a Redis hash, and how its transition is called. Follow the shape the memory adapter's registry uses — a struct of functions built once in `New`, keyed by `domain.Algorithm`.

Each entry needs: `decode(map[string]string) (any, error)` returning the zero state when the hash is empty and an error when it is partially written; `encode(any) []any` producing the `HSET` field/value pairs; `apply(state any, now time.Time, p domain.Params, cost int64) (any, domain.Outcome)` erasing the generic transition exactly as `adapter/memory`'s `erase[S]` does; and `lifetime(domain.Params) time.Duration`.

Reject partial state for the same reason the Lua adapter does: a fresh token bucket is a full one.

- [ ] **Step 4: Write the store**

`Apply` builds the key, then loops up to `maxAttempts`:

```go
	err := s.client.Watch(ctx, func(tx *goredis.Tx) error {
		now, state, err := a.load(ctx, tx, key)
		if err != nil {
			return err
		}
		next, o := a.apply(state, now, req.Params, req.Cost)
		_, err = tx.TxPipelined(ctx, func(pipe goredis.Pipeliner) error {
			pipe.HSet(ctx, key, a.encode(next)...)
			pipe.PExpire(ctx, key, a.lifetime(req.Params))
			return nil
		})
		out = o
		return err
	}, key)
```

On `nil`, return `out`. On `errors.Is(err, goredis.TxFailedErr)`, increment the retry counter and try again. On any other error, return a zero `Outcome` and the error. After the last attempt, return a zero `Outcome` and an error naming the attempt count.

`load` reads `TIME` and the hash. Pipeline them if that works on a watched connection; otherwise issue two calls and note it.

Note the identifier: this version of go-redis exports **`redis.TxFailedErr`**. Some published examples show `redis.TxFailure`, which does not exist here — check the symbol resolves before building the loop around it.

- [ ] **Step 5: Run the tests**

Run: `go test ./adapter/redis/cas/ -race -v`
Expected: PASS, including the same conformance subtests the Lua adapter produced.

`TestStoreRetriesUnderContention` asserts the retry path actually engages. If `Retries()` is zero, the loop is not doing what the benchmark will measure — investigate rather than relaxing the assertion.

- [ ] **Step 6: Commit**

```bash
git add adapter/redis/cas/
git commit -m "Add the CAS Redis store adapter

Runs the same Go transitions the memory adapter does, so each algorithm has
one implementation rather than two. WATCH supplies the version check and
go-redis reports a lost race as TxFailedErr, which the loop retries. Retries
is exported because the retry count under a hot key is what the contention
benchmark measures."
```

---

### Task 5: Differential and clock-skew tests

**Files:**
- Create: `adapter/redis/equivalence_test.go`
- Test: same file

**Interfaces:**
- Consumes: `memory.New`, `lua.New`, `cas.New`, `redistest.Start`, `clock.NewFake`.

Two properties, neither provable inside one adapter.

**Differential.** The Lua adapter duplicates each transition in a second language. Nothing but a test comparing them keeps the copies honest. Replaying one event sequence through memory, `redis/lua` and `redis/cas` must produce the same `Allowed` and `Remaining` at every step.

`ResetAt` and `RetryAfter` cannot be compared exactly: each store derives them from its own clock, and the Redis stores are microsecond-resolution while the memory store is nanosecond. Compare those within a tolerance and say so.

Choose params where no permit accrues during the test — `Limit: 100, Window: time.Hour` gives a permit every 36 seconds — so all three stores see effectively one instant and any divergence is the arithmetic rather than timing.

**Clock skew.** The design's clause 3 claims the store owning the clock removes per-node skew rather than tolerating it. Two store instances sharing one Redis, each handed a wildly wrong local clock, must together admit exactly `Limit`. That is the claim made observable.

- [ ] **Step 1: Write the tests**

```go
package rediskey_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/tunedev/rate_limiter/adapter/clock"
	"github.com/tunedev/rate_limiter/adapter/memory"
	"github.com/tunedev/rate_limiter/adapter/redis/cas"
	"github.com/tunedev/rate_limiter/adapter/redis/lua"
	"github.com/tunedev/rate_limiter/domain"
	"github.com/tunedev/rate_limiter/internal/redistest"
	"github.com/tunedev/rate_limiter/port"
)

// TestAdaptersAgreeOnOneEventSequence is the guard on the Lua duplication.
// Each transition exists twice, in Go and in a script; a divergence here means
// one of the two is wrong.
func TestAdaptersAgreeOnOneEventSequence(t *testing.T) {
	client := redistest.Start(t)
	ctx := context.Background()

	mem := memory.New(clock.System{}, memory.WithSweepInterval(0))
	t.Cleanup(func() { _ = mem.Close() })

	stores := map[string]port.Store{
		"memory":    mem,
		"redis/lua": lua.New(client),
		"redis/cas": cas.New(client, cas.WithMaxAttempts(200)),
	}

	// A permit is worth 36 seconds at these params, so elapsed test time
	// accrues none and every store sees effectively one instant.
	params := domain.Params{Limit: 100, Window: time.Hour}
	costs := []int64{1, 1, 5, 20, 1, 40, 30, 1, 10, 1}

	for _, algo := range []domain.Algorithm{domain.TokenBucket, domain.FixedWindow} {
		t.Run(string(algo), func(t *testing.T) {
			type result struct {
				allowed   bool
				remaining int64
			}
			got := map[string][]result{}

			for name, store := range stores {
				for i, cost := range costs {
					out, err := store.Apply(ctx, port.Request{
						RuleID:    domain.RuleID("equiv-" + string(algo)),
						Key:       domain.Key(name),
						Algorithm: algo,
						Params:    params,
						Cost:      cost,
					})
					if err != nil {
						t.Fatalf("%s step %d: %v", name, i, err)
					}
					got[name] = append(got[name], result{out.Allowed, out.Remaining})
				}
			}

			want := got["memory"]
			for name, seq := range got {
				for i := range seq {
					if seq[i] != want[i] {
						t.Fatalf("%s step %d = %+v, memory = %+v", name, i, seq[i], want[i])
					}
				}
			}
		})
	}
}

// TestNodesShareOneClockThroughRedis makes the design's clause 3 observable.
// Two independent stores over one Redis admit exactly Limit between them. No
// local clock is injected because neither adapter has one to inject: the
// instant comes from Redis, so there is no per-node clock left to disagree.
func TestNodesShareOneClockThroughRedis(t *testing.T) {
	client := redistest.Start(t)
	ctx := context.Background()

	for _, tc := range []struct {
		name  string
		build func() (port.Store, port.Store)
	}{
		{"redis/lua", func() (port.Store, port.Store) { return lua.New(client), lua.New(client) }},
		{"redis/cas", func() (port.Store, port.Store) {
			return cas.New(client, cas.WithMaxAttempts(500)), cas.New(client, cas.WithMaxAttempts(500))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nodeA, nodeB := tc.build()

			req := port.Request{
				RuleID:    domain.RuleID("skew-" + tc.name),
				Key:       "shared-subject",
				Algorithm: domain.TokenBucket,
				Params:    domain.Params{Limit: 50, Window: time.Hour},
				Cost:      1,
			}

			var wg sync.WaitGroup
			var mu sync.Mutex
			allowed := 0

			for _, node := range []struct {
				name  string
				store port.Store
			}{{"a", nodeA}, {"b", nodeB}} {
				for range 40 {
					wg.Add(1)
					go func() {
						defer wg.Done()
						out, err := node.store.Apply(ctx, req)
						if err != nil {
							t.Errorf("node %s: %v", node.name, err)
							return
						}
						if out.Allowed {
							mu.Lock()
							allowed++
							mu.Unlock()
						}
					}()
				}
			}
			wg.Wait()

			if allowed != 50 {
				t.Fatalf("allowed = %d across two nodes, want exactly the limit of 50", allowed)
			}
		})
	}
}
```

- [ ] **Step 2: Run the tests**

Run: `go test ./adapter/redis/ -race -run 'TestAdaptersAgree|TestNodesShareOneClock' -v`
Expected: PASS.

A differential failure is the most valuable signal this plan can produce. If one appears, report the step, the algorithm, and both values — do not adjust the sequence to avoid it.

- [ ] **Step 3: Commit**

```bash
git add adapter/redis/equivalence_test.go
git commit -m "Compare the adapters against each other and against node skew

The Lua adapter holds a second copy of each transition, so one event sequence
replayed through all three stores is what keeps the copies honest. The skew
test puts two stores with wrong local clocks over one Redis and asserts they
admit exactly the limit between them, which is the claim clause 3 makes."
```

---

### Task 6: The contention benchmark and the atomicity decision

**Files:**
- Create: `adapter/redis/bench_test.go`
- Create: `docs/decisions/0002-store-atomicity-strategy.md`
- Modify: `docs/benchmarks.md`

**Interfaces:**
- Consumes: `lua.New`, `cas.New`, `redistest.Start`, `(*cas.Store).Retries`.

This is the benchmark the spec ships both adapters for. One hot key, rising concurrency, measuring decision throughput and — for CAS — how many attempts were thrown away. The ADR is written from the numbers, not before them.

- [ ] **Step 1: Write the benchmark**

```go
package rediskey_test

import (
	"context"
	"testing"
	"time"

	"github.com/tunedev/rate_limiter/adapter/redis/cas"
	"github.com/tunedev/rate_limiter/adapter/redis/lua"
	"github.com/tunedev/rate_limiter/domain"
	"github.com/tunedev/rate_limiter/internal/redistest"
	"github.com/tunedev/rate_limiter/port"
)

// BenchmarkHotKey drives every goroutine at one key, which is the traffic a
// rate limiter exists to handle and the case where the two atomicity
// strategies diverge. For CAS it also reports retries per operation.
func BenchmarkHotKey(b *testing.B) {
	for _, conc := range []int{1, 8, 64} {
		b.Run("lua/parallelism="+strconv.Itoa(conc), func(b *testing.B) {
			client := redistest.StartB(b)
			runHotKey(b, lua.New(client), conc, nil)
		})

		b.Run("cas/parallelism="+strconv.Itoa(conc), func(b *testing.B) {
			client := redistest.StartB(b)
			s := cas.New(client, cas.WithMaxAttempts(1000))
			runHotKey(b, s, conc, s.Retries)
		})
	}
}

func runHotKey(b *testing.B, store port.Store, conc int, retries func() int64) {
	b.Helper()

	req := port.Request{
		RuleID:    "bench",
		Key:       "hot",
		Algorithm: domain.TokenBucket,
		Params:    domain.Params{Limit: 1_000_000, Window: time.Hour},
		Cost:      1,
	}

	var before int64
	if retries != nil {
		before = retries()
	}

	b.SetParallelism(conc)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		ctx := context.Background()
		for pb.Next() {
			if _, err := store.Apply(ctx, req); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.StopTimer()

	if retries != nil {
		b.ReportMetric(float64(retries()-before)/float64(b.N), "retries/op")
	}
}
```

`redistest.StartB` is the `*testing.B` counterpart of `Start`. Add it to `internal/redistest` by extracting the shared body behind a small interface both `*testing.T` and `*testing.B` satisfy (`Helper`, `TempDir`, `Cleanup`, `Skip`, `Fatalf`), rather than duplicating the function.

`b.SetParallelism` multiplies GOMAXPROCS rather than setting an absolute goroutine count, so the label says parallelism, not concurrency. Record what GOMAXPROCS was when you publish the table.

- [ ] **Step 2: Run the benchmark and read the real output**

Run: `go test ./adapter/redis/ -bench=BenchmarkHotKey -benchmem -run '^$' -count=5 | tee /tmp/redisbench.txt`

Expect six rows: two adapters at three parallelism levels. The `retries/op` metric appears on the CAS rows only.

If the benchmark takes more than a few minutes, reduce `-count` and record the command you actually ran. If `redis-server` is unavailable the benchmark skips; report that rather than publishing nothing silently.

- [ ] **Step 3: Record the measurements**

Add a section to `docs/benchmarks.md` in its existing style, with the command you ran and a table of min/median/max ns/op per adapter per concurrency level, plus retries/op for CAS. State no ranking the sample ranges do not support. Note that these figures come from a Redis on a unix socket on the same machine, so they understate network latency and therefore understate CAS's extra round trip — a real deployment's gap is wider, not narrower.

- [ ] **Step 4: Write the decision record**

Create `docs/decisions/0002-store-atomicity-strategy.md`. Structure it as: the decision, the measurement that produced it, and the consequences. It must recommend one adapter for production and say what the other is kept for.

Write it from the numbers you measured. If the two adapters' ranges overlap at every concurrency level, say the measurement does not separate them and recommend on the other grounds the spec names — one implementation of each algorithm versus two, against one round trip versus a retry loop. Do not manufacture a difference the data does not show.

Record the microsecond resolution floor here too: it applies to both Redis adapters and is a real user-visible constraint on `Params` that the memory adapter does not have.

- [ ] **Step 5: Run the full suite and commit**

Run: `go test ./... -race && go vet ./...`
Expected: PASS in every package, vet silent.

```bash
git add adapter/redis/bench_test.go internal/redistest/ docs/decisions/ docs/benchmarks.md
git commit -m "Measure the two atomicity strategies under a hot key

One key, rising concurrency, which is where the strategies diverge and the
traffic a rate limiter exists to handle. The CAS rows carry retries per
operation, since thrown-away attempts are what the strategy costs. ADR 0002
records the recommendation and the numbers behind it."
```

---

## Definition of Done

- [ ] `go test ./... -race` passes.
- [ ] `go vet ./...` is clean.
- [ ] Both Redis adapters pass `storetest.RunConformance` unmodified for `token_bucket` and `fixed_window`.
- [ ] The differential test replays one sequence through memory, `redis/lua` and `redis/cas` with matching `Allowed` and `Remaining`.
- [ ] The skew test shows two stores with wrong local clocks admitting exactly `Limit` between them.
- [ ] `docs/benchmarks.md` holds the hot-key table with the command that reproduces it.
- [ ] `docs/decisions/0002-store-atomicity-strategy.md` recommends one adapter, from the measured numbers.
- [ ] `go list -deps ./domain/ ./port/ | grep -v '^github.com/tunedev/rate_limiter' | grep '\.'` returns nothing — go-redis has not reached inward.

## What this plan deliberately leaves out

- Leaky bucket, sliding window log and sliding window counter in Redis. `RunConformance`'s variadic `algos` carries a partial adapter deliberately; once ADR 0002 picks a strategy, the remaining three may only need writing once.
- Redis Cluster, Sentinel, and connection failover.
- Rules, groups and precedence — plan 4.
- The otel observer and HTTP delivery — plans 5 and 6.
