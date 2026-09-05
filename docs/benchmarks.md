# Benchmarks

Every table below is produced by a command in this file. Nothing is asserted
here that was not measured.

## Per-algorithm decision cost, in-memory store

Every `RunParallel` goroutine contends on one key's mutex, so this measures
contended decision cost, not isolated per-operation cost. Across the ten
samples below, the two algorithms' ranges overlap; the spread between them
is within run-to-run noise, and the table implies no ranking between them.

    go test -bench=BenchmarkCheck -benchmem -run '^$' -count=10 .

| Algorithm | ns/op (min) | ns/op (median) | ns/op (max) | B/op | allocs/op |
|---|---|---|---|---|---|
| token_bucket | 296.6 | 310.65 | 639.7 | 32 | 1 |
| fixed_window | 315.6 | 562.35 | 624.8 | 32 | 1 |

Machine: `go version go1.27.0 linux/amd64`, `13th Gen Intel(R) Core(TM) i9-13900HX`.
