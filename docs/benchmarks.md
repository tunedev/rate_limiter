# Benchmarks

Every table below is produced by a command in this file. Nothing is asserted
here that was not measured.

## Per-algorithm decision cost, in-memory store

Every `RunParallel` goroutine contends on one key's mutex, so this measures
contended decision cost, not isolated per-operation cost. Across the ten
samples below, the two algorithms' ranges overlap; the spread between them
is within run-to-run noise, and the table implies no ranking between them.

This benchmark deliberately covers two of the five algorithms rather than
all five: its `Limit` never saturates, and running sliding_window_log
against a window that never saturates would allocate one timestamp per
iteration and grow the heap without bound over the run.

    go test -bench=BenchmarkCheck -benchmem -run '^$' -count=10 .

| Algorithm | ns/op (min) | ns/op (median) | ns/op (max) | B/op | allocs/op |
|---|---|---|---|---|---|
| token_bucket | 316.0 | 608.05 | 681.0 | 32 | 1 |
| fixed_window | 327.7 | 367.6 | 645.6 | 32 | 1 |

Machine: `go version go1.27.0 linux/amd64`, `13th Gen Intel(R) Core(TM) i9-13900HX`.

## Per-algorithm decision cost, uncontended

Every iteration takes its own key, so this is the algorithm's own cost rather
than the cost of waiting for one key's mutex. Each key is driven to its limit
of 1000 permits within the first iterations and denies thereafter, so the
figures are steady-state cost under sustained load, not the cost of the first
requests.

    go test -bench=BenchmarkCheckUncontended -benchmem -run '^$' -count=10 .

| Algorithm | ns/op (min) | ns/op (median) | ns/op (max) | B/op | allocs/op |
|---|---|---|---|---|---|
| token_bucket | 83.82 | 127.15 | 200.8 | 32 | 1 |
| leaky_bucket | 100.4 | 128.5 | 153.7 | 32 | 1 |
| fixed_window | 96.08 | 130.1 | 144.6 | 32 | 1 |
| sliding_window_log | 108.8 | 139.65 | 332.7 | 64.5 (62-115) | 1 |
| sliding_window_counter | 109.3 | 122.0 | 155.3 | 48 | 1 |

All five ns/op ranges overlap; the samples support no ranking between any
pair of algorithms. token_bucket and leaky_bucket's ranges overlap fully,
consistent with `docs/decisions/0001-leaky-bucket-is-token-bucket.md`: they
are the same transition in different coordinates, so no run-to-run noise
here separates them. B/op is a stable single value for every algorithm
except sliding_window_log, whose slice grows across allocator size classes
as it fills with permit timestamps; the range shown (62-115 bytes, median
64.5) is the ten samples' spread, not measurement noise.

## State held per saturated key

Each key is driven to its limit of 64 permits, then the heap is measured.

    go test -bench=BenchmarkStateBytesPerKey -benchmem -run '^$' -count=10 .

| Algorithm | bytes/key (median) |
|---|---|
| token_bucket | 197.8 |
| leaky_bucket | 197.8 |
| fixed_window | 197.8 |
| sliding_window_log | 1982 |
| sliding_window_counter | 213.8 |

The heap delta behind each sample is a signed subtraction, so a GC that freed
more than the loop allocated would show as a negative figure rather than
wrapping to a huge unsigned one; all ten samples per algorithm came back
positive and consistent in magnitude, so every figure above is a direct
measurement. token_bucket and
leaky_bucket report the identical 197.8 bytes/key across all ten samples
each — the equivalence in
`docs/decisions/0001-leaky-bucket-is-token-bucket.md` showing up as a
measurement, not a coincidence. sliding_window_log holds roughly ten times
the state of every counter-based algorithm: it keeps one timestamp per
admitted permit, where the others keep two integers, and that per-permit
cost is exactly the accuracy trade the algorithm makes.

## Redis adapters under a hot key

One key, `token_bucket`, `Limit` high enough that no run saturates it. Every
`RunParallel` goroutine contends on that one key, which is the traffic a rate
limiter exists to handle and the case where the two atomicity strategies
diverge: `redis/lua` commits in one round trip per decision; `redis/cas`
retries its `WATCH` transaction whenever another goroutine's write lands
first.

`b.SetParallelism(n)` runs `n * GOMAXPROCS` goroutines, not `n` goroutines, so
the row labels below name parallelism, not an absolute goroutine count.
GOMAXPROCS on the measuring machine was 32, matching the `-32` suffix Go
prints on each benchmark name.

Redis ran on a unix socket on the same machine as the client, which is the
cheapest possible path between them. That understates real network latency,
and therefore understates the cost of `redis/cas`'s extra round trips: a
client talking to Redis over a network should expect a wider gap than the one
below, not a narrower one.

    go test ./adapter/redis/ -bench=BenchmarkHotKey -benchmem -run '^$' -count=5

| Adapter | Parallelism | ns/op (min) | ns/op (median) | ns/op (max) | retries/op (min) | retries/op (median) | retries/op (max) |
|---|---|---|---|---|---|---|---|
| redis/lua | 1 | 8643 | 8984 | 9282 | - | - | - |
| redis/cas | 1 | 237282 | 257962 | 273284 | 18.24 | 18.34 | 18.38 |
| redis/lua | 8 | 10644 | 11303 | 12561 | - | - | - |
| redis/cas | 8 | 1113692 | 1299827 | 1391932 | 112.3 | 116.3 | 118.5 |
| redis/lua | 64 | 9703 | 10340 | 11034 | - | - | - |
| redis/cas | 64 | 1489840 | 1605595 | 1728580 | 143.6 | 149.1 | 153.4 |

Machine: `go version go1.27.0 linux/amd64`, `13th Gen Intel(R) Core(TM) i9-13900HX`.

`redis/lua`'s ns/op range sits under 13 microseconds at every parallelism
level measured. `redis/cas`'s range sits at least an order of magnitude
above it at every level, with no overlap: `redis/lua`'s worst sample (12561
ns/op, at parallelism 8) is still below `redis/cas`'s best sample at any
level (237282 ns/op, at parallelism 1). Comparing matched quantiles at each
level (min against min, median against median, max against max), the gap
widens as parallelism rises: roughly 27-29x at parallelism 1, roughly
105-115x at parallelism 8, roughly 154-157x at parallelism 64. That
widening tracks the retries/op column: under `WATCH`, exactly one contender's
`EXEC` commits per round, so serialising more concurrent contenders on the
same key costs more thrown-away attempts, and each attempt is a full round
trip. `redis/lua` needs no such retry loop and reports no retries/op figure.

`redis/lua`'s own ns/op range is lowest at parallelism 1 (8643-9282) and does
not overlap either the parallelism-8 range (10644-12561) or the
parallelism-64 range (9703-11034); those two higher-parallelism ranges
overlap each other. The samples therefore separate parallelism 1 from the
other two but do not support ranking parallelism 8 against parallelism 64.
The median is not monotonic across all three points either (8984 at
parallelism 1, 11303 at 8, 10340 at 64).
