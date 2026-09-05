package ratelimit

import (
	"context"
	"testing"
	"time"

	"github.com/tunedev/rate_limiter/adapter/clock"
	"github.com/tunedev/rate_limiter/adapter/memory"
	"github.com/tunedev/rate_limiter/domain"
)

// BenchmarkCheck measures per-algorithm decision cost against the in-memory
// store, on one hot key so the per-key mutex is contended.
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
