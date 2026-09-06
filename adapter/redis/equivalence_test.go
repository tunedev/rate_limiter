package rediskey_test

import (
	"context"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/tunedev/rate_limiter/adapter/clock"
	"github.com/tunedev/rate_limiter/adapter/memory"
	"github.com/tunedev/rate_limiter/adapter/redis/cas"
	"github.com/tunedev/rate_limiter/adapter/redis/lua"
	"github.com/tunedev/rate_limiter/domain"
	"github.com/tunedev/rate_limiter/internal/redistest"
	"github.com/tunedev/rate_limiter/port"
)

// tolerance bounds how far two stores' clock-derived fields may differ while
// their arithmetic still agrees. Two things put them apart: the stores are
// applied one after another and each reads its own clock, and the Redis
// adapters answer in whole microseconds where the memory store answers in
// nanoseconds. Both are round trips and rounding, tens of microseconds at
// most; an arithmetic divergence between a script and its Go transition moves
// these fields by a large fraction of a window.
const tolerance = 20 * time.Millisecond

// rollingWindow is short enough that permits accrue and windows roll while a
// test runs.
const rollingWindow = 400 * time.Millisecond

type namedStore struct {
	name  string
	store port.Store
}

// newStores returns the memory reference store and both Redis adapters, in a
// fixed order so every step reaches them in the same sequence. The memory
// store is first, and is what the others are compared against.
func newStores(t *testing.T, client *goredis.Client) []namedStore {
	t.Helper()

	mem := memory.New(clock.System{}, memory.WithSweepInterval(0))
	t.Cleanup(func() { _ = mem.Close() })

	return []namedStore{
		{"memory", mem},
		{"redis/lua", lua.New(client)},
		{"redis/cas", cas.New(client, cas.WithMaxAttempts(200))},
	}
}

// sameOutcome compares two stores' answers: the counting fields exactly, the
// clock-derived ones within tolerance.
func sameOutcome(a, b domain.Outcome) bool {
	return a.Allowed == b.Allowed &&
		a.Remaining == b.Remaining &&
		within(a.RetryAfter-b.RetryAfter) &&
		within(a.ResetAt.Sub(b.ResetAt))
}

func within(d time.Duration) bool { return d < tolerance && d > -tolerance }

// TestAdaptersAgreeOnOneEventSequence is the guard on the Lua duplication.
// Each transition exists twice, in Go and in a script; a divergence here means
// one of the two is wrong. A step reaches every store before the next step
// starts, so all three see the same instant to within a few round trips.
func TestAdaptersAgreeOnOneEventSequence(t *testing.T) {
	client := redistest.Start(t)
	ctx := context.Background()

	for _, tc := range []struct {
		name       string
		params     domain.Params
		costs      []int64
		start      func()
		pauseAfter int
		pause      time.Duration
	}{
		{
			// A permit is worth 36 seconds at these params, so elapsed test
			// time accrues none and every store sees effectively one instant.
			name:       "one-instant",
			params:     domain.Params{Limit: 100, Window: time.Hour},
			costs:      []int64{1, 1, 5, 20, 1, 40, 30, 1, 10, 1},
			pauseAfter: -1,
		},
		{
			// A permit is worth 40ms here and the window rolls every 400ms, so
			// the pause refills the bucket and starts a new fixed window. The
			// first five steps run half a window in and the pause is just
			// short of a whole window, so every call lands mid-window with
			// exactly one boundary between the two halves.
			name:   "accruing-and-rolling",
			params: domain.Params{Limit: 10, Window: rollingWindow},
			costs:  []int64{3, 3, 3, 3, 2, 4, 4, 4, 1, 1},
			start: func() {
				now := time.Now()
				time.Sleep(now.Truncate(rollingWindow).Add(rollingWindow + rollingWindow/2).Sub(now))
			},
			pauseAfter: 4,
			pause:      rollingWindow - rollingWindow/20,
		},
	} {
		for _, algo := range []domain.Algorithm{domain.TokenBucket, domain.FixedWindow} {
			t.Run(tc.name+"/"+string(algo), func(t *testing.T) {
				stores := newStores(t, client)
				got := map[string][]domain.Outcome{}

				if tc.start != nil {
					tc.start()
				}

				for i, cost := range tc.costs {
					for _, s := range stores {
						out, err := s.store.Apply(ctx, port.Request{
							RuleID:    domain.RuleID("equiv-" + tc.name + "-" + string(algo)),
							Key:       domain.Key(s.name),
							Algorithm: algo,
							Params:    tc.params,
							Cost:      cost,
						})
						if err != nil {
							t.Fatalf("%s step %d: %v", s.name, i, err)
						}
						got[s.name] = append(got[s.name], out)
					}
					if i == tc.pauseAfter {
						time.Sleep(tc.pause)
					}
				}

				want := got["memory"]
				for _, s := range stores {
					for i, out := range got[s.name] {
						if !sameOutcome(out, want[i]) {
							t.Fatalf("%s step %d = %+v, memory = %+v", s.name, i, out, want[i])
						}
					}
				}
			})
		}
	}
}

// TestAdaptersAgreeOnRatesFinerThanAMicrosecond covers the rates where a
// script working in whole microseconds parts company with domain.Params.Rate,
// which is in nanoseconds: 3333ns is not a whole microsecond, and 500ns is
// under one. Both pass domain.Params.Validate, so both can reach a store from
// a loaded rule set.
func TestAdaptersAgreeOnRatesFinerThanAMicrosecond(t *testing.T) {
	client := redistest.Start(t)
	ctx := context.Background()

	for _, tc := range []struct {
		name   string
		params domain.Params
		cost   int64
	}{
		{"3333ns-allowed", domain.Params{Limit: 300_000, Window: time.Second}, 299_000},
		{"3333ns-denied", domain.Params{Limit: 300_000, Window: time.Second}, 900_000},
		{"500ns-allowed", domain.Params{Limit: 2_000_000, Window: time.Second}, 1_999_000},
		{"500ns-denied", domain.Params{Limit: 2_000_000, Window: time.Second}, 6_000_000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.params.Validate(domain.TokenBucket); err != nil {
				t.Fatalf("Validate: %v, want params a rule set would accept", err)
			}

			// One call per store, each on its own untouched key. A fresh
			// bucket is full at the instant it is read, so nothing here
			// depends on how long the calls take, only on the rate each store
			// computes: RetryAfter and ResetAt are that rate times a count
			// both stores agree on.
			stores := newStores(t, client)
			var want domain.Outcome

			for i, s := range stores {
				out, err := s.store.Apply(ctx, port.Request{
					RuleID:    domain.RuleID("rate-" + tc.name),
					Key:       domain.Key(s.name),
					Algorithm: domain.TokenBucket,
					Params:    tc.params,
					Cost:      tc.cost,
				})
				if err != nil {
					t.Fatalf("%s: %v", s.name, err)
				}
				if i == 0 {
					want = out
					continue
				}
				if !sameOutcome(out, want) {
					t.Fatalf("%s = %+v, memory = %+v", s.name, out, want)
				}
			}
		})
	}
}

// TestAdaptersShareOneStateLayout drives both Redis adapters over one key.
// Each writes the state the other reads, which is what lets a deployment move
// between them, and what a renamed field or a changed unit would break.
func TestAdaptersShareOneStateLayout(t *testing.T) {
	client := redistest.Start(t)
	ctx := context.Background()

	newLua := func() port.Store { return lua.New(client) }
	newCAS := func() port.Store { return cas.New(client, cas.WithMaxAttempts(200)) }

	for _, order := range []struct {
		name          string
		first, second func() port.Store
	}{
		{"lua-then-cas", newLua, newCAS},
		{"cas-then-lua", newCAS, newLua},
	} {
		for _, algo := range []domain.Algorithm{domain.TokenBucket, domain.FixedWindow} {
			t.Run(order.name+"/"+string(algo), func(t *testing.T) {
				req := port.Request{
					RuleID:    domain.RuleID("shared-" + order.name + "-" + string(algo)),
					Key:       "subject",
					Algorithm: algo,
					Params:    domain.Params{Limit: 10, Window: time.Hour},
					Cost:      1,
				}

				first, err := order.first().Apply(ctx, req)
				if err != nil {
					t.Fatalf("first adapter: %v", err)
				}
				if first.Remaining != 9 {
					t.Fatalf("first adapter Remaining = %d, want 9", first.Remaining)
				}

				second, err := order.second().Apply(ctx, req)
				if err != nil {
					t.Fatalf("second adapter: %v", err)
				}
				if second.Remaining != 8 {
					t.Fatalf("second adapter Remaining = %d, want 8: it must continue from the state the first adapter wrote", second.Remaining)
				}
			})
		}
	}
}

// TestNodesShareOneClockThroughRedis makes the design's clause 3 observable.
// Two independent stores over one Redis admit exactly Limit between them. No
// local clock is injected because neither adapter has one to inject: the
// instant comes from Redis, so there is no per-node clock left to disagree.
func TestNodesShareOneClockThroughRedis(t *testing.T) {
	client := redistest.Start(t)
	ctx := context.Background()

	for _, tc := range []struct {
		name  string
		build func() (port.Store, port.Store)
	}{
		{"redis/lua", func() (port.Store, port.Store) { return lua.New(client), lua.New(client) }},
		{"redis/cas", func() (port.Store, port.Store) {
			return cas.New(client, cas.WithMaxAttempts(500)), cas.New(client, cas.WithMaxAttempts(500))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nodeA, nodeB := tc.build()

			req := port.Request{
				RuleID:    domain.RuleID("skew-" + tc.name),
				Key:       "shared-subject",
				Algorithm: domain.TokenBucket,
				Params:    domain.Params{Limit: 50, Window: time.Hour},
				Cost:      1,
			}

			var wg sync.WaitGroup
			var mu sync.Mutex
			allowed := 0

			for _, node := range []struct {
				name  string
				store port.Store
			}{{"a", nodeA}, {"b", nodeB}} {
				for range 40 {
					wg.Add(1)
					go func() {
						defer wg.Done()
						out, err := node.store.Apply(ctx, req)
						if err != nil {
							t.Errorf("node %s: %v", node.name, err)
							return
						}
						if out.Allowed {
							mu.Lock()
							allowed++
							mu.Unlock()
						}
					}()
				}
			}
			wg.Wait()

			if allowed != 50 {
				t.Fatalf("allowed = %d across two nodes, want exactly the limit of 50", allowed)
			}
		})
	}
}
