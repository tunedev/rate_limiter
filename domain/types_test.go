package domain

import (
	"testing"
	"time"
)

func TestParamsRate(t *testing.T) {
	cases := []struct {
		name string
		p    Params
		want time.Duration
	}{
		{"ten per second", Params{Limit: 10, Window: time.Second}, 100 * time.Millisecond},
		{"one per minute", Params{Limit: 1, Window: time.Minute}, time.Minute},
		{"zero limit is not a rate", Params{Limit: 0, Window: time.Second}, 0},
		{"negative limit is not a rate", Params{Limit: -1, Window: time.Second}, 0},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.p.Rate(); got != c.want {
				t.Fatalf("Rate = %v, want %v", got, c.want)
			}
		})
	}
}
