# Rate Limiter — Design

Concrete shapes behind the charter in `CLAUDE.md`. Scope: the domain types, the
four ports, and the three decisions that constrain everything downstream —
store atomicity, rule precedence, Redis key layout.

## Domain

```go
type Decision struct {
    Allowed    bool
    Limit      int64          // the rule's configured limit
    Remaining  int64          // permits left, never negative
    RetryAfter time.Duration  // zero when Allowed
    ResetAt    time.Time      // when capacity is full again
    RuleID     RuleID         // which rule decided
    Group      GroupID        // which group denied, when denied
}
```

The field set is exactly what the IETF `RateLimit` header fields draft needs, so
the HTTP adapter is a mapping with no arithmetic of its own. Algorithm is not a
field: `RuleID` resolves to it, and two copies would drift.

`ResetAt` is an absolute instant because the store owns the clock (port clause 3);
a duration computed against a skewed caller clock would be wrong.

```go
type Scope int  // Route < Client < Tenant < Global, rank fixed in code

type Group struct {
    ID    GroupID
    Scope Scope
    Rules []Rule  // tried in declared order
}

type Rule struct {
    ID        RuleID
    Group     GroupID
    Match     Matcher          // predicate over request attributes
    Subject   SubjectSelector  // which attribute forms the limit key
    Algorithm Algorithm
    Params    Params           // limit, window, burst, refill rate
    OnError   ErrorPolicy      // Allow | Deny
}
```

Each algorithm is a pure transition over its own state type, generic in that
state so no algorithm pays for another's fields:

```go
type Outcome struct {
    Allowed    bool
    Remaining  int64
    RetryAfter time.Duration
    ResetAt    time.Time
}

type Limiter[S any] interface {
    Apply(s S, now time.Time, p Params, cost int64) (S, Outcome)
}
```

No I/O, no clock reads. The zero `S` is full capacity (clause 4). Transitions
that accrue over elapsed time clamp it to `[0, Params.Window]`, so a clock that
moves backward or jumps forward cannot corrupt bucket state. The window
algorithms need no clamp: they derive boundaries by truncating absolute time,
which bounds a jump by construction.

`Outcome` is what a store can answer on its own. The domain composes it into a
`Decision` by adding `Limit` from `Params` and stamping `RuleID` and `Group`,
which the store has no reason to know.

## Ports

```go
type Store interface {
    Apply(ctx context.Context, req Request) (Outcome, error)
}

type Request struct {
    RuleID    RuleID
    Key       Key
    Algorithm Algorithm
    Params    Params
    Cost      int64
}
```

`RuleID` and `Key` together identify the state, matching the key layout below:
one rule's limit on a subject never shares state with another's.

`Store` dispatches on `Algorithm` to a per-algorithm handler, which persists the
new state and returns the `Outcome`. State is per-algorithm and never leaves the
adapter; the port promises atomic application, never a single blob encoding —
sliding window log is a sorted set, not a serialized struct.

`Params` spans every algorithm's knobs, so `RuleSource` validates each rule's
params against its algorithm at load, never on the request path.

```go
type RuleSource interface {
    Rules() *RuleSet  // current immutable set; never blocks on I/O
}

type Clock interface {
    Now() time.Time
}

type Observer interface {
    BeginDecision(ctx context.Context, rule RuleID, algo Algorithm) (context.Context, EndDecision)
    BeginStore(ctx context.Context, algo Algorithm) (context.Context, EndStore)
    RulesReloaded(ctx context.Context, err error)
}

type EndDecision func(Decision, error)
type EndStore func(error)
```

`Observer` returns a context so spans propagate into store calls and metrics can
carry exemplars. The noop implementation returns the input context and empty funcs.

## Store atomicity contract

Every adapter passes one shared conformance suite asserting these clauses.

1. **Atomic per key.** Concurrent `Apply` calls on one key serialize. No lost updates.
2. **Not atomic across keys.** No multi-key transactions. Layered limits are
   separate calls, which is what keeps Redis Cluster viable.
3. **The store owns the clock.** `Request` carries no `now`. `Outcome.ResetAt`
   and `RetryAfter` are computed from the instant the store applied the
   transition, and callers use those rather than their own clock. Redis answers
   from `TIME` so all nodes agree on window boundaries; the memory adapter
   answers from the `Clock` port so tests stay deterministic. This costs no extra
   round trip: `redis/lua` calls `TIME` inside the script, `redis/cas` pipelines
   it with the read it already makes. The residual risk is a failover to a
   replica whose clock disagrees, which the transition clamp bounds.
4. **Missing state is full capacity.** Every key carries a TTL derived from the
   algorithm and its params: long enough that expiry can never hand back more
   capacity than the rule has actually accrued, which for a bucket with a burst
   reserve is the time to refill from empty rather than one window. Expiry is
   the normal path, not an error path.
5. **Errors carry no policy.** `Apply` returns an error and nothing else. Fail-open
   versus fail-closed is `Rule.OnError`, resolved in the domain. An adapter never
   decides to allow traffic. `Apply` honours context cancellation, returning
   `ctx.Err()` without applying anything.
6. **Not idempotent.** A timed-out `Apply` may or may not have been applied.
   Callers do not blindly retry; the timeout routes through clause 5.

### Two Redis adapters

Redis cannot run the Go transition, so the adapter satisfies clause 1 one of two
ways. Both ship, because the comparison is a headline result rather than an
assumption.

- `redis/lua` — the transition also exists as a Lua script. One round trip, no
  retries, correct under any contention. The duplication is held safe by the
  conformance suite plus differential tests replaying identical event sequences
  through memory and Redis and asserting identical output.
- `redis/cas` — loads state, calls the same pure Go transition, writes back under
  a version check, retries on conflict. One implementation of each algorithm, at
  the cost of extra round trips and retries that peak on hot keys.

`redis/cas` exists to be measured. Recommendation follows the numbers, in an ADR.

## Rule precedence

Ordered groups, most specific first; first match wins inside a group; every group
must pass.

Each group declares a `Scope`, whose rank is fixed in code rather than inferred
from a config-author's name. `RuleSet` sorts groups by that rank ascending at
load, so evaluation order is a property of the type and cannot be misconfigured.
Within a group, rules are tried in declared order and the first match decides, so
order is explicit and there is no specificity-scoring surprise. Across groups it
is a conjunction, which is what expresses "50/s on this endpoint and 1000/s per
tenant".

Cost is one store round trip per group, bounded by config and known at load time.

Evaluation short-circuits on first denial, and permits consumed by earlier groups
are not refunded. Ordering most specific first is what makes that safe: a flood
against one route is rejected by the route rule before the global bucket is
touched, so rejected traffic cannot drain the budget protecting other tenants.
The reverse order reaches that amplification, which is why the order comes from
`Scope` rank and not from the config file.

The trade this makes explicit: a global limit counts admitted requests, not
arrivals. Limiting arrivals is an edge concern and a different tool.

`Decision.Group` reports the denying group, which makes a 429 debuggable and
keeps the metric label bounded.

Rejected as v1 designs: peek-then-commit pays double the round trips and still
races between the two phases, since clause 2 forbids cross-group atomicity;
compensating refunds race with each other and leak a permit if a process dies
mid-pair. A single Lua script spanning every group is the strongest answer — one
round trip, genuinely atomic — but needs all of a tenant's groups in one hash
slot, so it is a v2 mode behind an ADR rather than a default.

### Reload

A reload validates the whole set, then swaps an immutable `*RuleSet` pointer
atomically. Consumers never see a half-applied change and never parse anything on
the request path. A failed reload keeps the last good set and reports through
`Observer.RulesReloaded`.

## Redis key layout

```
rl:v1:{<ruleID>/<subject>}
```

- `v1` is an encoding version, so the state representation can change without a
  migration.
- The hash tag wraps `ruleID/subject` so one rule's state for one subject is in
  one slot. Nothing else needs colocating, per clause 2.
- One key per (rule, subject), typed to its algorithm: hash for token and leaky
  bucket, counter for fixed window, sorted set for sliding window log, hash of two
  counters for sliding window counter.
- No key is written without a TTL.

## Observability

Per the charter. Concretely: one span per decision with `rule.id`, `algorithm`
and outcome; store round trips as child spans so network cost separates from
algorithm cost; decisions, decision latency, store latency and errors, and rule
reload success and age as metrics; structured logs correlated by trace id for
errors and reloads only. The limit key never appears as a metric label.

## Evidence

Headline benchmarks:

1. Hot-key contention: `redis/lua` versus `redis/cas` — retries and p99 against
   concurrency on a shared key.
2. Per-algorithm cost and memory per key.
3. Fixed window boundary spike: a test that fails without burst-at-boundary
   tolerance, showing twice the limit across the seam.
4. Sliding window log memory growth against limit.
5. Instrumentation overhead: noop versus otel.
6. Quota error under injected clock skew: store-authoritative time versus
   caller-supplied, across window sizes.

ADRs: store atomicity strategy (written after benchmark 1), rule precedence and
group ordering, Redis key layout, the pinned IETF header draft revision, the
Redis version whose effect replication permits `TIME` in scripts, the fail-open
default.

## Out of scope

Quota billing, auth, per-tenant admin UI, transports beyond HTTP and gRPC,
permit refunds on cross-group denial, single-script cross-group atomicity.
