package domain

import "time"

// TokenBucketState is a token count and the instant those tokens were counted.
// The zero value is a fresh bucket, filled on first use.
type TokenBucketState struct {
	Tokens int64
	At     time.Time
}

// TokenBucketLimiter grants Params.Limit permits per Params.Window, allowing a
// burst of up to Limit plus Burst permits held in reserve.
type TokenBucketLimiter struct{}

// Lifetime reports how long a bucket's state must outlive its last use: long
// enough to refill from empty, plus a window of slack. Expiring sooner would
// hand back capacity the rule has not accrued.
func (TokenBucketLimiter) Lifetime(p Params) time.Duration {
	refill := p.Window + time.Duration(p.Burst)*p.Rate()
	return refill + p.Window
}

// Apply accrues tokens for the clamped elapsed time, then takes cost of them.
func (TokenBucketLimiter) Apply(s TokenBucketState, now time.Time, p Params, cost int64) (TokenBucketState, Outcome) {
	capacity := p.Limit + p.Burst
	rate := p.Rate()
	if rate <= 0 {
		return s, Outcome{}
	}

	if s.At.IsZero() {
		s = TokenBucketState{Tokens: capacity, At: now}
	}

	if accrued := int64(clampElapsed(s.At, now, p.Window) / rate); accrued > 0 {
		s.Tokens = min(capacity, s.Tokens+accrued)
		s.At = s.At.Add(time.Duration(accrued) * rate)
	}
	if s.Tokens >= capacity {
		s.At = now
	}

	if s.Tokens < cost {
		untilFull := time.Duration(capacity-s.Tokens) * rate
		return s, Outcome{
			Remaining:  s.Tokens,
			RetryAfter: time.Duration(cost-s.Tokens) * rate,
			ResetAt:    now.Add(untilFull),
		}
	}

	s.Tokens -= cost
	return s, Outcome{
		Allowed:   true,
		Remaining: s.Tokens,
		ResetAt:   now.Add(time.Duration(capacity-s.Tokens) * rate),
	}
}
