package domain

import "time"

// clampElapsed bounds now-prev to [0, window]. A clock that moved backward
// yields zero; a forward jump yields at most one window of credit.
func clampElapsed(prev, now time.Time, window time.Duration) time.Duration {
	d := now.Sub(prev)
	switch {
	case d < 0:
		return 0
	case d > window:
		return window
	default:
		return d
	}
}

// truncateFromEpoch rounds t down to a whole number of windows measured from
// the Unix epoch. time.Truncate measures from the zero time instead, which for
// windows longer than a day lands on a different boundary than any system
// computing from Unix seconds.
func truncateFromEpoch(t time.Time, window time.Duration) time.Time {
	if window <= 0 {
		return t
	}
	n, w := t.UnixNano(), int64(window)
	q := n / w
	if n < 0 && q*w != n {
		q--
	}
	return time.Unix(0, q*w).UTC()
}
