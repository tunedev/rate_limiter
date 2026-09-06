package cas

import (
	"fmt"
	"strconv"
	"time"

	"github.com/tunedev/rate_limiter/domain"
)

// transition erases a generic domain.Limiter to a function over any, the same
// erasure adapter/memory's Store uses. A failed assertion yields the zero
// state, which is contract clause 4.
type transition func(prev any, now time.Time, p domain.Params, cost int64) (any, domain.Outcome)

func erase[S any](l domain.Limiter[S]) transition {
	return func(prev any, now time.Time, p domain.Params, cost int64) (any, domain.Outcome) {
		s, _ := prev.(S)
		return l.Apply(s, now, p, cost)
	}
}

// algorithm is one algorithm's Redis hash codec plus the transition it runs.
// decode reads a hash into that algorithm's state, returning the zero state
// for an empty hash and an error for a partially written one. encode is the
// inverse, producing HSET field/value pairs.
type algorithm struct {
	decode   func(map[string]string) (any, error)
	encode   func(any) []any
	apply    transition
	lifetime func(domain.Params) time.Duration
}

// algorithms builds the codec and transition for each algorithm this store
// supports. The field names and units match the Lua adapter's scripts, since
// both write the same logical state to the same key layout.
func algorithms() map[domain.Algorithm]algorithm {
	return map[domain.Algorithm]algorithm{
		domain.TokenBucket: {
			decode:   decodeTokenBucket,
			encode:   encodeTokenBucket,
			apply:    erase[domain.TokenBucketState](domain.TokenBucketLimiter{}),
			lifetime: domain.TokenBucketLimiter{}.Lifetime,
		},
		domain.FixedWindow: {
			decode:   decodeFixedWindow,
			encode:   encodeFixedWindow,
			apply:    erase[domain.FixedWindowState](domain.FixedWindowLimiter{}),
			lifetime: domain.FixedWindowLimiter{}.Lifetime,
		},
	}
}

// decodeTokenBucket reads the "tokens" and "at" fields the Lua adapter's
// script also uses, "at" being a Unix microsecond timestamp. Both fields
// absent is a fresh bucket; one without the other is a partially written hash.
func decodeTokenBucket(h map[string]string) (any, error) {
	tokens, tOk := h["tokens"]
	at, aOk := h["at"]
	if !tOk && !aOk {
		return domain.TokenBucketState{}, nil
	}
	if tOk != aOk {
		return nil, fmt.Errorf("cas: partial token bucket state")
	}

	tv, err := strconv.ParseInt(tokens, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("cas: token bucket tokens: %w", err)
	}
	av, err := strconv.ParseInt(at, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("cas: token bucket at: %w", err)
	}
	return domain.TokenBucketState{Tokens: tv, At: time.UnixMicro(av)}, nil
}

func encodeTokenBucket(s any) []any {
	st := s.(domain.TokenBucketState)
	return []any{"tokens", st.Tokens, "at", st.At.UnixMicro()}
}

// decodeFixedWindow reads the "start" and "count" fields the Lua adapter's
// script also uses, "start" being a Unix microsecond timestamp. Both fields
// absent is an expired window; one without the other is a partially written
// hash.
func decodeFixedWindow(h map[string]string) (any, error) {
	start, sOk := h["start"]
	count, cOk := h["count"]
	if !sOk && !cOk {
		return domain.FixedWindowState{}, nil
	}
	if sOk != cOk {
		return nil, fmt.Errorf("cas: partial fixed window state")
	}

	sv, err := strconv.ParseInt(start, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("cas: fixed window start: %w", err)
	}
	cv, err := strconv.ParseInt(count, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("cas: fixed window count: %w", err)
	}
	return domain.FixedWindowState{Start: time.UnixMicro(sv), Count: cv}, nil
}

func encodeFixedWindow(s any) []any {
	st := s.(domain.FixedWindowState)
	return []any{"start", st.Start.UnixMicro(), "count", st.Count}
}
