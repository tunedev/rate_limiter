# Benchmarks

Every table below is produced by a command in this file. Nothing is asserted
here that was not measured.

## Per-algorithm decision cost, in-memory store

Contended on one key, so the figure includes the per-key mutex.

    go test -bench=BenchmarkCheck -benchmem -run '^$' .

| Algorithm | ns/op | B/op | allocs/op |
|---|---|---|---|
| token_bucket | 558.9 | 32 | 1 |
| fixed_window | 571.0 | 32 | 1 |

Machine: `go version go1.27.0 linux/amd64`, `13th Gen Intel(R) Core(TM) i9-13900HX`.
