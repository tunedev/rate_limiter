// Package noop holds the inert port.Observer used by default, so a library
// consumer acquires no exporter it did not ask for.
package noop

import (
	"context"

	"github.com/tunedev/rate_limiter/domain"
	"github.com/tunedev/rate_limiter/port"
)

// Observer discards everything it is given.
type Observer struct{}

// BeginDecision returns ctx unchanged and a function that does nothing.
func (Observer) BeginDecision(ctx context.Context, _ domain.RuleID, _ domain.Algorithm) (context.Context, port.EndDecision) {
	return ctx, func(domain.Decision, error) {}
}

// BeginStore returns ctx unchanged and a function that does nothing.
func (Observer) BeginStore(ctx context.Context, _ domain.Algorithm) (context.Context, port.EndStore) {
	return ctx, func(error) {}
}

// RulesReloaded does nothing.
func (Observer) RulesReloaded(context.Context, error) {}
