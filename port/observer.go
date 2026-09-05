package port

import (
	"context"

	"github.com/tunedev/rate_limiter/domain"
)

// EndDecision closes a decision's span and records its result.
type EndDecision func(domain.Decision, error)

// EndStore closes a store round trip's span and records its result.
type EndStore func(error)

// Observer receives the traces, metrics and logs a decision produces. It
// returns a context so spans propagate into the store call beneath.
//
// The limit key is never passed here: it is unbounded, and a metric label
// built from it would be unbounded too.
type Observer interface {
	BeginDecision(ctx context.Context, rule domain.RuleID, algo domain.Algorithm) (context.Context, EndDecision)
	BeginStore(ctx context.Context, algo domain.Algorithm) (context.Context, EndStore)
	RulesReloaded(ctx context.Context, err error)
}
