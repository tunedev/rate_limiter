package port

import (
	"context"

	"github.com/tunedev/rate_limiter/domain"
)

// Request is one take against one key.
type Request struct {
	Key       domain.Key
	Algorithm domain.Algorithm
	Params    domain.Params
	Cost      int64
}

// Store applies an algorithm's transition atomically per key.
//
// The contract, asserted by storetest.RunConformance:
//
//  1. Apply is atomic per key. Concurrent calls on one key serialize.
//  2. Apply is not atomic across keys. There are no multi-key transactions.
//  3. The store owns the clock. Request carries no instant, and the returned
//     Outcome's ResetAt and RetryAfter come from the store's own time.
//  4. Missing state is full capacity, never an error.
//  5. Errors carry no policy. A non-nil error returns a zero Outcome; whether
//     that allows or denies is the caller's rule to decide. Apply honours
//     context cancellation and returns ctx.Err() without applying anything.
//  6. Apply is not idempotent. A timed-out call may or may not have applied.
type Store interface {
	Apply(ctx context.Context, req Request) (domain.Outcome, error)
}
