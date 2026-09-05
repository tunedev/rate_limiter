package noop

import (
	"context"
	"errors"
	"testing"

	"github.com/tunedev/rate_limiter/domain"
	"github.com/tunedev/rate_limiter/port"
)

var _ port.Observer = Observer{}

func TestObserverIsInert(t *testing.T) {
	var o Observer
	ctx := context.Background()

	got, end := o.BeginDecision(ctx, "rule", domain.TokenBucket)
	if got != ctx {
		t.Fatal("BeginDecision changed the context, want it returned unchanged")
	}
	end(domain.Decision{}, errors.New("ignored"))

	got, endStore := o.BeginStore(ctx, domain.TokenBucket)
	if got != ctx {
		t.Fatal("BeginStore changed the context, want it returned unchanged")
	}
	endStore(nil)

	o.RulesReloaded(ctx, nil)
}
