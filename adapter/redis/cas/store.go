// Package cas holds a port.Store that runs each algorithm's transition in Go
// and gets atomicity from Redis WATCH rather than a script. It shares its
// transitions with adapter/memory, so each algorithm has one implementation.
package cas

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	goredis "github.com/redis/go-redis/v9"

	rediskey "github.com/tunedev/rate_limiter/adapter/redis"
	"github.com/tunedev/rate_limiter/domain"
	"github.com/tunedev/rate_limiter/port"
)

// defaultMaxAttempts is how many times Apply retries a lost WATCH race before
// giving up, absent an explicit Option. Under WATCH exactly one contender's
// EXEC commits per round, so serialising N concurrent contenders on a key
// costs on the order of N attempts: the budget has to exceed the contention a
// single key sees, and a hot key is the traffic this adapter exists for. The
// default carries the 400 concurrent takes the conformance suite drives.
// Attempts are bounded in latency by the caller's context, which Apply checks
// before each one, so a deadline rather than a smaller budget is the way to
// cap how long a contended decision may take.
const defaultMaxAttempts = 2000

// Option configures a Store.
type Option func(*Store)

// WithMaxAttempts sets how many times Apply retries a lost WATCH race before
// returning an error. Zero means Apply never even tries once.
func WithMaxAttempts(n int) Option {
	return func(s *Store) { s.maxAttempts = n }
}

// Store applies transitions under WATCH: the key is watched, its state and
// the server's time are read, the transition runs in Go, and the write goes
// through TxPipelined. A concurrent writer that changes the key first makes
// go-redis report TxFailedErr, which Apply retries.
type Store struct {
	client      *goredis.Client
	algorithms  map[domain.Algorithm]algorithm
	maxAttempts int

	retries atomic.Int64
}

// New returns a Store issuing commands through client.
func New(client *goredis.Client, opts ...Option) *Store {
	s := &Store{
		client:      client,
		algorithms:  algorithms(),
		maxAttempts: defaultMaxAttempts,
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Apply runs req's transition, retrying up to maxAttempts times when the
// watched key changes before the write commits.
func (s *Store) Apply(ctx context.Context, req port.Request) (domain.Outcome, error) {
	if err := ctx.Err(); err != nil {
		return domain.Outcome{}, err
	}

	a, ok := s.algorithms[req.Algorithm]
	if !ok {
		return domain.Outcome{}, fmt.Errorf("cas: unsupported algorithm %q", req.Algorithm)
	}

	key := rediskey.Key(req.RuleID, req.Key)

	for attempt := 0; attempt < s.maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return domain.Outcome{}, err
		}

		var out domain.Outcome
		err := s.client.Watch(ctx, func(tx *goredis.Tx) error {
			now, state, err := a.load(ctx, tx, key)
			if err != nil {
				return err
			}
			next, o := a.apply(state, now, req.Params, req.Cost)
			_, err = tx.TxPipelined(ctx, func(pipe goredis.Pipeliner) error {
				pipe.HSet(ctx, key, a.encode(next)...)
				pipe.PExpire(ctx, key, a.lifetime(req.Params))
				return nil
			})
			out = o
			return err
		}, key)

		if err == nil {
			return out, nil
		}
		if errors.Is(err, goredis.TxFailedErr) {
			s.retries.Add(1)
			continue
		}
		return domain.Outcome{}, err
	}

	return domain.Outcome{}, fmt.Errorf("cas: gave up on %s after %d attempts", key, s.maxAttempts)
}

// load reads the server's time and the key's hash in one round trip: both are
// plain reads, so they run through a non-transactional pipeline on tx rather
// than needing MULTI.
func (a algorithm) load(ctx context.Context, tx *goredis.Tx, key string) (time.Time, any, error) {
	cmds, err := tx.Pipelined(ctx, func(pipe goredis.Pipeliner) error {
		pipe.Time(ctx)
		pipe.HGetAll(ctx, key)
		return nil
	})
	if err != nil {
		return time.Time{}, nil, err
	}

	now, err := cmds[0].(*goredis.TimeCmd).Result()
	if err != nil {
		return time.Time{}, nil, err
	}
	h, err := cmds[1].(*goredis.MapStringStringCmd).Result()
	if err != nil {
		return time.Time{}, nil, err
	}

	state, err := a.decode(h)
	if err != nil {
		return time.Time{}, nil, err
	}
	return now, state, nil
}

// Retries reports how many times Apply has retried a lost WATCH race, across
// every key and algorithm.
func (s *Store) Retries() int64 { return s.retries.Load() }

// now reports the store's own time, which is the Redis server's.
func (s *Store) now(ctx context.Context) (time.Time, error) {
	return s.client.Time(ctx).Result()
}
