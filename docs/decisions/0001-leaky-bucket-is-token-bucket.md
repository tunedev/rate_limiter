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
