package lua

import (
	"context"
	"testing"
	"time"

	"github.com/tunedev/rate_limiter/domain"
	"github.com/tunedev/rate_limiter/internal/redistest"
	"github.com/tunedev/rate_limiter/port"
	"github.com/tunedev/rate_limiter/storetest"
)

func TestStoreConformance(t *testing.T) {
	storetest.RunConformance(t, func(t *testing.T) storetest.Harness {
		s := New(redistest.Start(t))
		return storetest.Harness{
			Store:   s,
			Now:     func() time.Time { return storeNow(t, s) },
			Advance: func(d time.Duration) { time.Sleep(d) },
		}
	}, domain.TokenBucket, domain.FixedWindow)
}

func storeNow(t *testing.T, s *Store) time.Time {
	t.Helper()
	now, err := s.now(context.Background())
	if err != nil {
		t.Fatalf("reading the store's time: %v", err)
	}
	return now
}

func TestStoreRejectsUnsupportedAlgorithm(t *testing.T) {
	s := New(redistest.Start(t))

	out, err := s.Apply(context.Background(), port.Request{
		RuleID:    "rule",
		Key:       "subject",
		Algorithm: domain.SlidingWindowLog,
		Params:    domain.Params{Limit: 10, Window: time.Second},
		Cost:      1,
	})
	if err == nil {
		t.Fatal("Apply with an unsupported algorithm returned nil error, want an error")
	}
	if out != (domain.Outcome{}) {
		t.Fatalf("Outcome = %+v on error, want the zero value", out)
	}
}

func TestStoreRejectsPartiallyWrittenState(t *testing.T) {
	client := redistest.Start(t)
	s := New(client)
	ctx := context.Background()

	req := port.Request{
		RuleID:    "rule",
		Key:       "subject",
		Algorithm: domain.TokenBucket,
		Params:    domain.Params{Limit: 10, Window: time.Second},
		Cost:      1,
	}
	if _, err := s.Apply(ctx, req); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	// Half of the hash survives. Treating that as fresh state would hand back
	// a full bucket.
	if err := client.HDel(ctx, "rl:v1:{rule/subject}", "at").Err(); err != nil {
		t.Fatalf("HDel: %v", err)
	}

	out, err := s.Apply(ctx, req)
	if err == nil {
		t.Fatal("Apply over partial state returned nil error, want an error")
	}
	if out != (domain.Outcome{}) {
		t.Fatalf("Outcome = %+v on error, want the zero value", out)
	}
}

func TestStoreSetsATTLFromLifetime(t *testing.T) {
	client := redistest.Start(t)
	s := New(client)
	ctx := context.Background()

	p := domain.Params{Limit: 10, Window: time.Second, Burst: 90}
	if _, err := s.Apply(ctx, port.Request{
		RuleID: "rule", Key: "subject", Algorithm: domain.TokenBucket, Params: p, Cost: 1,
	}); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	ttl, err := client.PTTL(ctx, "rl:v1:{rule/subject}").Result()
	if err != nil {
		t.Fatalf("PTTL: %v", err)
	}

	want := domain.TokenBucketLimiter{}.Lifetime(p)
	if ttl <= 0 || ttl > want {
		t.Fatalf("PTTL = %v, want a positive value no greater than Lifetime %v", ttl, want)
	}
	if ttl < want/2 {
		t.Fatalf("PTTL = %v, want close to Lifetime %v: a TTL from Window alone would expire early", ttl, want)
	}
}

// TestArgsFloorTheTTLAtOneMillisecond guards the TTL argument every script
// hands PEXPIRE. Redis treats PEXPIRE key 0 as a delete, so a lifetime that
// rounds down to zero milliseconds would make each write drop the state it
// just stored and every request read full capacity. These params pass
// domain.Params.Validate and give both algorithms a lifetime of 800us.
func TestArgsFloorTheTTLAtOneMillisecond(t *testing.T) {
	s := New(nil)
	p := domain.Params{Limit: 10, Window: 400 * time.Microsecond}

	for algo, sc := range s.scripts {
		lifetime := sc.lifetime(p)
		if lifetime >= time.Millisecond {
			t.Fatalf("%s: lifetime = %v, want under a millisecond for this test to mean anything", algo, lifetime)
		}

		args := sc.args(p, 1, lifetime)
		ttl, ok := args[len(args)-1].(int64)
		if !ok {
			t.Fatalf("%s: ttl argument is %T, want an integer", algo, args[len(args)-1])
		}
		if ttl < 1 {
			t.Fatalf("%s: ttl argument = %d ms for a lifetime of %v, want at least 1", algo, ttl, lifetime)
		}
	}
}
