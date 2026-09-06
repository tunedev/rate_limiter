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
