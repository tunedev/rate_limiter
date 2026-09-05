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

func TestParamsValidate(t *testing.T) {
	cases := []struct {
		name string
		algo Algorithm
		p    Params
		ok   bool
	}{
		{"ten per second", TokenBucket, Params{Limit: 10, Window: time.Second}, true},
		{"burst above the limit", TokenBucket, Params{Limit: 10, Window: time.Second, Burst: 90}, true},
		{"missing window", TokenBucket, Params{Limit: 10}, false},
		{"missing limit", FixedWindow, Params{Window: time.Second}, false},
		{"negative window", FixedWindow, Params{Limit: 1, Window: -time.Second}, false},
		{"limit finer than a nanosecond", TokenBucket, Params{Limit: 2, Window: time.Nanosecond}, false},
		{"negative burst", TokenBucket, Params{Limit: 10, Window: time.Second, Burst: -1}, false},
		{"unknown algorithm", Algorithm("nonexistent"), Params{Limit: 10, Window: time.Second}, false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := c.p.Validate(c.algo)
			if c.ok && err != nil {
				t.Fatalf("Validate = %v, want nil", err)
			}
			if !c.ok && err == nil {
				t.Fatal("Validate = nil, want an error")
			}
		})
	}
}
