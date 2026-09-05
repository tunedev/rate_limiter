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
