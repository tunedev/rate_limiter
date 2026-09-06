package redistest

import (
	"context"
	"testing"
)

func TestStartGivesAWorkingEmptyServer(t *testing.T) {
	c := Start(t)
	ctx := context.Background()

	if err := c.Ping(ctx).Err(); err != nil {
		t.Fatalf("Ping: %v", err)
	}

	n, err := c.DBSize(ctx).Result()
	if err != nil {
		t.Fatalf("DBSize: %v", err)
	}
	if n != 0 {
		t.Fatalf("DBSize = %d on a fresh server, want 0", n)
	}
}

func TestStartGivesEachCallerItsOwnServer(t *testing.T) {
	ctx := context.Background()

	a := Start(t)
	if err := a.Set(ctx, "shared", "1", 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}

	b := Start(t)
	if n, err := b.Exists(ctx, "shared").Result(); err != nil {
		t.Fatalf("Exists: %v", err)
	} else if n != 0 {
		t.Fatal("a key written to one server is visible from another, want isolation")
	}
}
