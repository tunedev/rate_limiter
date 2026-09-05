package clock

import (
	"sync"
	"testing"
	"time"
)

func TestFakeAdvancesAndSets(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	f := NewFake(base)

	if !f.Now().Equal(base) {
		t.Fatalf("Now = %v, want %v", f.Now(), base)
	}

	f.Advance(time.Second)
	if want := base.Add(time.Second); !f.Now().Equal(want) {
		t.Fatalf("Now = %v, want %v", f.Now(), want)
	}

	f.Set(base)
	if !f.Now().Equal(base) {
		t.Fatalf("Now = %v, want %v after Set", f.Now(), base)
	}
}

func TestFakeIsRaceFree(t *testing.T) {
	f := NewFake(time.Unix(1_700_000_000, 0))

	var wg sync.WaitGroup
	for range 50 {
		wg.Add(2)
		go func() { defer wg.Done(); f.Advance(time.Millisecond) }()
		go func() { defer wg.Done(); _ = f.Now() }()
	}
	wg.Wait()

	if want := time.Unix(1_700_000_000, 0).Add(50 * time.Millisecond); !f.Now().Equal(want) {
		t.Fatalf("Now = %v, want %v", f.Now(), want)
	}
}

func TestSystemAdvances(t *testing.T) {
	var c System
	first := c.Now()
	if second := c.Now(); second.Before(first) {
		t.Fatalf("Now went backward: %v then %v", first, second)
	}
}
