// Package port declares the interfaces the domain depends on. Adapters
// implement them; nothing here performs I/O.
package port

import "time"

// Clock reports the current instant. Stores own the clock, so this is the only
// place time enters the system.
type Clock interface {
	Now() time.Time
}
