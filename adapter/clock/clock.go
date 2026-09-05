// Package clock provides the real and test implementations of port.Clock.
package clock

import (
	"sync"
	"time"
)

// System reads the host clock.
type System struct{}

// Now returns the host's current time.
func (System) Now() time.Time { return time.Now() }

// Fake is a clock that only moves when a test moves it. It is safe for
// concurrent use.
type Fake struct {
	mu  sync.RWMutex
	now time.Time
}

// NewFake returns a Fake reading t.
func NewFake(t time.Time) *Fake { return &Fake{now: t} }

// Now returns the current fake time.
func (f *Fake) Now() time.Time {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.now
}

// Advance moves the clock forward by d.
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}

// Set moves the clock to t, forward or backward.
func (f *Fake) Set(t time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = t
}
