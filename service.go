// Package ratelimit is the rate limiter's public entry point. It composes a
// store's Outcome into an attributed Decision and owns instrumentation, so
// store adapters stay free of it.
package ratelimit

import (
	"context"
	"fmt"

	"github.com/tunedev/rate_limiter/adapter/observer/noop"
	"github.com/tunedev/rate_limiter/domain"
	"github.com/tunedev/rate_limiter/port"
)

// CheckRequest is one take against one key under one rule.
type CheckRequest struct {
	RuleID    domain.RuleID
	Key       domain.Key
	Algorithm domain.Algorithm
	Params    domain.Params
	Cost      int64
}

// Option configures a Service.
type Option func(*Service)

// WithObserver sends traces, metrics and logs to o. The default discards them.
func WithObserver(o port.Observer) Option {
	return func(s *Service) { s.obs = o }
}

// Service answers rate limit questions against a store.
type Service struct {
	store port.Store
	obs   port.Observer
}

// New returns a Service backed by store.
func New(store port.Store, opts ...Option) *Service {
	s := &Service{store: store, obs: noop.Observer{}}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Check validates req's params, applies it and returns the decision. On an
// invalid rule or a store error the Decision is zero-valued and the error is
// returned: whether that allows or denies is the caller's policy.
func (s *Service) Check(ctx context.Context, req CheckRequest) (domain.Decision, error) {
	ctx, endDecision := s.obs.BeginDecision(ctx, req.RuleID, req.Algorithm)

	if err := req.Params.Validate(req.Algorithm); err != nil {
		endDecision(domain.Decision{}, err)
		return domain.Decision{}, err
	}
	if req.Cost < 0 {
		err := fmt.Errorf("ratelimit: Cost must not be negative, got %d", req.Cost)
		endDecision(domain.Decision{}, err)
		return domain.Decision{}, err
	}

	storeCtx, endStore := s.obs.BeginStore(ctx, req.Algorithm)
	out, err := s.store.Apply(storeCtx, port.Request{
		RuleID:    req.RuleID,
		Key:       req.Key,
		Algorithm: req.Algorithm,
		Params:    req.Params,
		Cost:      req.Cost,
	})
	endStore(err)

	if err != nil {
		endDecision(domain.Decision{}, err)
		return domain.Decision{}, err
	}

	d := domain.Decision{
		Allowed:    out.Allowed,
		Limit:      req.Params.Limit,
		Remaining:  out.Remaining,
		RetryAfter: out.RetryAfter,
		ResetAt:    out.ResetAt,
		RuleID:     req.RuleID,
	}
	endDecision(d, nil)
	return d, nil
}
