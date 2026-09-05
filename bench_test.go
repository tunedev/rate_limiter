package ratelimit

import (
	"context"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tunedev/rate_limiter/adapter/clock"
	"github.com/tunedev/rate_limiter/adapter/memory"
	"github.com/tunedev/rate_limiter/domain"
)

// BenchmarkCheck measures per-algorithm decision cost against the in-memory
// store, on one hot key so the per-key mutex is contended. It deliberately
// covers two of the five algorithms: its Limit never saturates, and running
// sliding_window_log against a window that never saturates would allocate one
// timestamp per iteration and grow the heap without bound over the run.
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

				// A signed subtraction so a GC that frees more than the loop
				// allocated shows as a negative delta rather than wrapping to
				// a huge unsigned one.
				delta := int64(after.HeapAlloc) - int64(before.HeapAlloc)
				bytesPerKey = float64(delta) / keys
				runtime.KeepAlive(store)
				_ = store.Close()
			}
			b.ReportMetric(bytesPerKey, "bytes/key")
		})
	}
}
