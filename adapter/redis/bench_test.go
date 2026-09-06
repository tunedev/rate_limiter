package rediskey_test

import (
	"context"
	"strconv"
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
