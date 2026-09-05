// Package memory holds an in-process port.Store. It is the reference adapter:
// the transitions it runs are the same pure functions the domain exports.
package memory

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/tunedev/rate_limiter/domain"
	"github.com/tunedev/rate_limiter/port"
)

// transition erases a generic domain.Limiter to a function over any. A failed
// assertion yields the zero state, which is contract clause 4.
type transition func(prev any, now time.Time, p domain.Params, cost int64) (any, domain.Outcome)

func erase[S any](l domain.Limiter[S]) transition {
	return func(prev any, now time.Time, p domain.Params, cost int64) (any, domain.Outcome) {
		s, _ := prev.(S)
		return l.Apply(s, now, p, cost)
	}
}

type entry struct {
	mu      sync.Mutex
	state   any
	expires time.Time
}

// Option configures a Store.
type Option func(*Store)

// WithSweepInterval sets how often expired entries are reclaimed. Zero disables
// the background sweeper, leaving Sweep to be called directly.
func WithSweepInterval(d time.Duration) Option {
	return func(s *Store) { s.sweepEvery = d }
}

// Store applies transitions under one mutex per key.
type Store struct {
	clk         port.Clock
	transitions map[domain.Algorithm]transition
	sweepEvery  time.Duration

	mu      sync.RWMutex
	entries map[domain.Key]*entry

	stop chan struct{}
	done sync.WaitGroup
}

// New returns an empty Store reading time from clk.
func New(clk port.Clock, opts ...Option) *Store {
	s := &Store{
		clk: clk,
		transitions: map[domain.Algorithm]transition{
			domain.TokenBucket: erase[domain.TokenBucketState](domain.TokenBucketLimiter{}),
			domain.FixedWindow: erase[domain.FixedWindowState](domain.FixedWindowLimiter{}),
		},
		sweepEvery: time.Minute,
		entries:    make(map[domain.Key]*entry),
		stop:       make(chan struct{}),
	}
	for _, o := range opts {
		o(s)
	}
	if s.sweepEvery > 0 {
		s.done.Add(1)
		go s.sweepLoop()
	}
	return s
}

// Apply runs req's transition atomically for req.Key.
func (s *Store) Apply(ctx context.Context, req port.Request) (domain.Outcome, error) {
	if err := ctx.Err(); err != nil {
		return domain.Outcome{}, err
	}

	apply, ok := s.transitions[req.Algorithm]
	if !ok {
		return domain.Outcome{}, fmt.Errorf("memory: unsupported algorithm %q", req.Algorithm)
	}

	e := s.entryFor(req.Key)

	e.mu.Lock()
	defer e.mu.Unlock()

	now := s.clk.Now()
	if now.After(e.expires) {
		e.state = nil
	}

	state, out := apply(e.state, now, req.Params, req.Cost)
	e.state = state
	e.expires = now.Add(2 * req.Params.Window)
	return out, nil
}

func (s *Store) entryFor(k domain.Key) *entry {
	s.mu.RLock()
	e, ok := s.entries[k]
	s.mu.RUnlock()
	if ok {
		return e
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.entries[k]; ok {
		return e
	}
	e = &entry{}
	s.entries[k] = e
	return e
}

// Len reports how many keys hold state.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.entries)
}

// Sweep reclaims entries whose state has expired.
func (s *Store) Sweep() {
	now := s.clk.Now()

	s.mu.Lock()
	defer s.mu.Unlock()
	for k, e := range s.entries {
		if e.mu.TryLock() {
			if now.After(e.expires) {
				delete(s.entries, k)
			}
			e.mu.Unlock()
		}
	}
}

func (s *Store) sweepLoop() {
	defer s.done.Done()
	t := time.NewTicker(s.sweepEvery)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			s.Sweep()
		case <-s.stop:
			return
		}
	}
}

// Close stops the background sweeper.
func (s *Store) Close() error {
	close(s.stop)
	s.done.Wait()
	return nil
}
