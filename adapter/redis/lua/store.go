// Package lua holds a port.Store that runs each algorithm's transition as a
// Redis script. One round trip per decision, and the script reads TIME itself
// so every node agrees on the instant.
package lua

import (
	"context"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"

	rediskey "github.com/tunedev/rate_limiter/adapter/redis"
	"github.com/tunedev/rate_limiter/domain"
	"github.com/tunedev/rate_limiter/port"
)

// script is one algorithm's transition plus how its arguments are built.
type script struct {
	run      *goredis.Script
	args     func(p domain.Params, cost int64, ttl time.Duration) []any
	lifetime func(domain.Params) time.Duration
}

// Store applies transitions as Redis scripts.
type Store struct {
	client  *goredis.Client
	scripts map[domain.Algorithm]script
}

// New returns a Store issuing commands through client.
func New(client *goredis.Client) *Store {
	return &Store{
		client: client,
		scripts: map[domain.Algorithm]script{
			domain.TokenBucket: {
				run: goredis.NewScript(tokenBucketSrc),
				args: func(p domain.Params, cost int64, ttl time.Duration) []any {
					return []any{p.Limit, p.Burst, micros(p.Window), p.Rate().Nanoseconds(), cost, millis(ttl)}
				},
				lifetime: domain.TokenBucketLimiter{}.Lifetime,
			},
			domain.FixedWindow: {
				run: goredis.NewScript(fixedWindowSrc),
				args: func(p domain.Params, cost int64, ttl time.Duration) []any {
					return []any{p.Limit, micros(p.Window), cost, millis(ttl)}
				},
				lifetime: domain.FixedWindowLimiter{}.Lifetime,
			},
		},
	}
}

// Apply runs req's script, which is atomic for req's key by construction.
func (s *Store) Apply(ctx context.Context, req port.Request) (domain.Outcome, error) {
	sc, ok := s.scripts[req.Algorithm]
	if !ok {
		return domain.Outcome{}, fmt.Errorf("lua: unsupported algorithm %q", req.Algorithm)
	}

	key := rediskey.Key(req.RuleID, req.Key)
	raw, err := sc.run.Run(ctx, s.client, []string{key}, sc.args(req.Params, req.Cost, sc.lifetime(req.Params))...).Slice()
	if err != nil {
		return domain.Outcome{}, fmt.Errorf("lua: %w", err)
	}
	if len(raw) != 4 {
		return domain.Outcome{}, fmt.Errorf("lua: script returned %d values, want 4", len(raw))
	}

	fields := make([]int64, 4)
	for i, v := range raw {
		n, ok := v.(int64)
		if !ok {
			return domain.Outcome{}, fmt.Errorf("lua: return value %d is %T, want an integer", i, v)
		}
		fields[i] = n
	}

	return domain.Outcome{
		Allowed:    fields[0] == 1,
		Remaining:  fields[1],
		RetryAfter: time.Duration(fields[2]) * time.Microsecond,
		ResetAt:    time.UnixMicro(fields[3]),
	}, nil
}

// now reports the store's own time, which is the Redis server's.
func (s *Store) now(ctx context.Context) (time.Time, error) {
	return s.client.Time(ctx).Result()
}

// micros converts a duration to whole microseconds, the unit every script
// keeps timestamps in.
func micros(d time.Duration) int64 { return int64(d / time.Microsecond) }

// millis converts a lifetime to whole milliseconds, the unit PEXPIRE takes,
// with a floor of one. Redis treats PEXPIRE key 0 as a delete, so a lifetime
// under a millisecond would make every write drop the state it just stored
// and every request read full capacity. A millisecond of extra life can only
// hold state longer than the rule needs, never hand back capacity early.
func millis(d time.Duration) int64 {
	if ms := d.Milliseconds(); ms > 0 {
		return ms
	}
	return 1
}
