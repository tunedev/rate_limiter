// Package domain holds the rate limiter's pure value types and state
// transitions. It performs no I/O and reads no clock.
package domain

import (
	"fmt"
	"time"
)

// Algorithm names a rate limiting strategy. Stores dispatch on it to pick a
// state representation and a transition.
type Algorithm string

const (
	TokenBucket          Algorithm = "token_bucket"
	LeakyBucket          Algorithm = "leaky_bucket"
	FixedWindow          Algorithm = "fixed_window"
	SlidingWindowLog     Algorithm = "sliding_window_log"
	SlidingWindowCounter Algorithm = "sliding_window_counter"
)

// Key identifies the subject a limit applies to, such as an IP or a tenant.
type Key string

// RuleID names a rule within the rule set.
type RuleID string

// GroupID names a group of rules evaluated together.
type GroupID string

// Params holds every algorithm's knobs. Limit permits are granted per Window.
// Burst is extra capacity above Limit and applies to bucket algorithms only.
type Params struct {
	Limit  int64
	Window time.Duration
	Burst  int64
}

// Rate returns the interval between two permits, or zero when Limit is not
// positive.
func (p Params) Rate() time.Duration {
	if p.Limit <= 0 {
		return 0
	}
	return time.Duration(int64(p.Window) / p.Limit)
}

// Validate reports whether p is usable for a. A rule source validates each rule
// at load; a request that reaches a transition with unusable params would be
// denied with nothing to retry after.
func (p Params) Validate(a Algorithm) error {
	switch a {
	case TokenBucket, LeakyBucket, FixedWindow, SlidingWindowLog, SlidingWindowCounter:
	default:
		return fmt.Errorf("domain: unknown algorithm %q", a)
	}
	switch {
	case p.Limit <= 0:
		return fmt.Errorf("domain: Limit must be positive, got %d", p.Limit)
	case p.Window <= 0:
		return fmt.Errorf("domain: Window must be positive, got %v", p.Window)
	case p.Rate() <= 0:
		return fmt.Errorf("domain: Limit %d over Window %v leaves no time between permits", p.Limit, p.Window)
	case p.Burst < 0:
		return fmt.Errorf("domain: Burst must not be negative, got %d", p.Burst)
	}
	return nil
}

// Outcome is what a store can answer without knowing which rule asked.
type Outcome struct {
	Allowed    bool
	Remaining  int64
	RetryAfter time.Duration
	ResetAt    time.Time
}

// Decision is an Outcome attributed to the rule and group that produced it.
type Decision struct {
	Allowed    bool
	Limit      int64
	Remaining  int64
	RetryAfter time.Duration
	ResetAt    time.Time
	RuleID     RuleID
	Group      GroupID
}

// Limiter is a pure state transition over an algorithm's own state type. The
// zero value of S means full capacity.
type Limiter[S any] interface {
	Apply(s S, now time.Time, p Params, cost int64) (S, Outcome)
}
