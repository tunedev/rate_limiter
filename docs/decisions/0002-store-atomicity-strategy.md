# 0002: redis/lua is the recommended store, redis/cas is kept for algorithms it has not yet earned

## Decision

`adapter/redis/lua` is the recommended `port.Store` for production use against
Redis. `adapter/redis/cas` is kept in the tree, not deprecated: it is the
lower-cost place to add the three algorithms `redis/lua` does not yet
implement (`leaky_bucket`, `sliding_window_log`, `sliding_window_counter`),
because it shares its transitions with `adapter/memory` instead of requiring
a second, hand-written implementation in Lua.

## The measurement that produced it

`BenchmarkHotKey` (`adapter/redis/bench_test.go`) drives both adapters against
one key at three parallelism levels, five samples each:

    go test ./adapter/redis/ -bench=BenchmarkHotKey -benchmem -run '^$' -count=5

The parallelism column is `b.SetParallelism(n)`, which runs `n * GOMAXPROCS`
goroutines rather than `n`. GOMAXPROCS on the measuring machine was 32, so the
three rows are 32, 256 and 2048 goroutines contending on one key; there is no
uncontended datapoint here. Every request in the run is allowed - the params
are `Limit: 1_000_000, Window: time.Hour` at cost 1, chosen so no run
saturates and no denial path is measured - and `redis/cas` ran with
`cas.WithMaxAttempts(1000)`, the budget the retries/op column is spent
against.

| Adapter | Parallelism | ns/op (min) | ns/op (median) | ns/op (max) | retries/op (median) |
|---|---|---|---|---|---|
| redis/lua | 1 | 8643 | 8984 | 9282 | - |
| redis/cas | 1 | 237282 | 257962 | 273284 | 18.34 |
| redis/lua | 8 | 10644 | 11303 | 12561 | - |
| redis/cas | 8 | 1113692 | 1299827 | 1391932 | 116.3 |
| redis/lua | 64 | 9703 | 10340 | 11034 | - |
| redis/cas | 64 | 1489840 | 1605595 | 1728580 | 149.1 |

Full samples and machine details are in `docs/benchmarks.md`. The ranges do
not overlap at any parallelism level: `redis/lua`'s worst sample across all
three levels (12561 ns/op) is below `redis/cas`'s best sample across all
three levels (237282 ns/op). Comparing matched quantiles at each level (min
against min, median against median, max against max), the gap widens as
parallelism rises: roughly 27-29x at parallelism 1, roughly 105-115x at
parallelism 8, roughly 154-157x at parallelism 64, tracking the retries/op
column. This is measured on Redis over a unix socket on the same machine as
the client, which understates network latency and therefore understates
`redis/cas`'s disadvantage — its cost is one extra round trip per retried
attempt, and a real network makes each round trip more expensive, not less.
A deployment against a network Redis should expect a wider gap than the one
measured here, not a narrower one.

This measurement alone would be enough to recommend `redis/lua`. Two further
facts, established while building the adapters, back the same conclusion and
explain what `redis/cas` costs to keep and what it buys:

1. **CAS needs an attempt budget exceeding the concurrent contenders on a
   key.** Under `WATCH`, exactly one contender's `EXEC` commits per round, so
   serialising N contenders takes on the order of N attempts. The conformance
   suite drives 400 concurrent takes on one key; `redis/cas` failed 8 runs out
   of 8 at 200 attempts and passed at 2000. `redis/lua` passes the same test
   with no such tuning parameter at all — there is no attempt budget to get
   wrong, because there is no retry loop.
2. **The Lua duplication is real and currently paid for, not free.** Each
   algorithm `redis/lua` implements exists twice: once as the Go transition
   `adapter/memory` and `redis/cas` share, once as a hand-written Lua script.
   `TestAdaptersAgreeOnOneEventSequence` replays two event sequences through
   the memory store and both Redis adapters — one at a rate slow enough that
   no permit accrues, one that refills the bucket and rolls the fixed window
   across a boundary — and finds no divergence in `Allowed`, `Remaining`,
   `RetryAfter` or `ResetAt` across all ten steps of either, for both
   algorithms currently ported. That test shows the duplication is currently
   correct.
   It does not show the duplication is free: every future algorithm ported to
   `redis/lua` is a second implementation to write and keep in sync, where
   `redis/cas` gets the same algorithm by reusing the transition that already
   exists.

## Consequences

- New production traffic against Redis should use `redis/lua`. Its cost is
  one round trip per decision with no tunable attempt budget, and at every
  contention level measured - 32, 256 and 2048 goroutines on one key -
  `redis/cas` is at least an order of magnitude slower. The benchmark
  measures no uncontended case, so it supports no claim about a key only one
  caller ever touches.
- `redis/cas` stays in the tree as the adapter to extend first for
  `leaky_bucket`, `sliding_window_log`, and `sliding_window_counter`, since it
  needs no new Lua script to gain an algorithm — only whatever memory-side
  transition already exists. Once an algorithm is ported to `redis/cas` and
  proven correct there, porting it to `redis/lua` as a script is the
  follow-on work implied by this decision, not a substitute for it.
- Both adapters hold instants to the microsecond where `adapter/memory` holds
  them to the nanosecond. Redis Lua numbers are IEEE doubles, exact only below
  2^53, and a Unix nanosecond timestamp is past that threshold, so `redis/lua`
  passes every instant to a script in microseconds. `redis/cas` never runs a
  script, but its codec (`adapter/redis/cas/codec.go`) writes the same
  microsecond-timestamp fields to the same hash layout `redis/lua`'s scripts
  use, so the floor is a property of the shared state layout and does not go
  away by choosing one Redis adapter over the other. It bounds the resolution
  of a stored instant by roughly a microsecond, which shows up as `ResetAt`
  and `RetryAfter` being that much coarser than `adapter/memory`'s.
- The refill rate is not subject to that floor. `redis/lua` passes
  `domain.Params.Rate` in nanoseconds as its own argument and scales the
  microsecond elapsed time up to meet it, so a rate that is not a whole
  number of microseconds (`Limit: 300_000, Window: time.Second` is 3333ns) or
  is under one (`Limit: 2_000_000, Window: time.Second` is 500ns) accrues the
  same permits on either Redis adapter as on `adapter/memory`. A nanosecond
  rate stays exact for the same reason a microsecond timestamp does: even a
  30-day window is 2.59e15 nanoseconds, under 2^53. Every set of `Params`
  that `domain.Params.Validate` accepts is therefore usable on both Redis
  adapters; `TestAdaptersAgreeOnRatesFinerThanAMicrosecond` is the guard.
- `redis/cas`'s attempt budget (`cas.WithMaxAttempts`) is a parameter a
  caller must size to the expected number of concurrent contenders on a
  single key. Exhausting it returns an error and nothing else, which carries
  no policy: clause 5 leaves fail-open against fail-closed to `Rule.OnError`
  in the domain, and the adapter never decides to admit traffic. That is
  better than silently corrupting state, but it is a tuning burden
  `redis/lua` does not impose. The default budget carries the 400 concurrent
  takes the conformance suite drives.
- `redis/cas`'s `algorithm` struct is hash-shaped: `decode(map[string]string)`
  and `encode() []any` around a fixed `HGETALL`/`HSET` pair. `leaky_bucket`
  and `sliding_window_counter` are small integer states and fit it as it
  stands. `sliding_window_log` does not: it needs a sorted set, so adding it
  means widening that struct to own its own read and write commands rather
  than reusing the hash pair. That widening is part of the cost of the
  migration path above, not a surprise to discover mid-task.
- The two adapters diverge on params `domain.Params.Validate` rejects, which
  reach a store only when a caller skips validation. A `Limit` over `Window`
  leaving no time between permits makes `redis/lua` return an error and write
  nothing, where `redis/cas` runs the domain transition, which answers a zero
  `Outcome` with no error, and then persists the zero state: `at` encodes as
  the zero `time.Time`, a year-1 timestamp of -62135596800000000
  microseconds. Rules are validated at load, so no validated rule set reaches
  this; it is recorded rather than fixed.
