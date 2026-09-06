package domain

import "time"

// LeakyBucketState is a queue depth and the instant it was measured. The zero
// value is an empty bucket.
type LeakyBucketState struct {
	Depth int64
	At    time.Time
}

// LeakyBucketLimiter admits while the bucket holds room. Depth rises by cost on
// admission and drains one unit per Params.Rate(), so the bucket empties at the
// configured rate no matter how fast requests arrive.
type LeakyBucketLimiter struct{}

// Lifetime reports how long a bucket's state must outlive its last use: long
// enough to drain from full, plus a window of slack. Expiring sooner would hand
// back room the rule has not drained.
func (LeakyBucketLimiter) Lifetime(p Params) time.Duration {
	drain := p.Window + time.Duration(p.Burst)*p.Rate()
	return drain + p.Window
}

// Apply drains the bucket for the clamped elapsed time, then adds cost to it.
func (LeakyBucketLimiter) Apply(s LeakyBucketState, now time.Time, p Params, cost int64) (LeakyBucketState, Outcome) {
	capacity := p.Limit + p.Burst
	rate := p.Rate()
	if rate <= 0 {
		return s, Outcome{}
	}

	if s.At.IsZero() {
		s = LeakyBucketState{At: now}
	}

	if drained := int64(clampElapsed(s.At, now, p.Window) / rate); drained > 0 {
		s.Depth = max(0, s.Depth-drained)
		s.At = s.At.Add(time.Duration(drained) * rate)
	}
	if s.Depth <= 0 {
		s.At = now
	}

	if s.Depth+cost > capacity {
		return s, Outcome{
			Remaining:  capacity - s.Depth,
			RetryAfter: time.Duration(s.Depth+cost-capacity) * rate,
			ResetAt:    now.Add(time.Duration(s.Depth) * rate),
		}
	}

	s.Depth += cost
	return s, Outcome{
		Allowed:   true,
		Remaining: capacity - s.Depth,
		ResetAt:   now.Add(time.Duration(s.Depth) * rate),
	}
}
