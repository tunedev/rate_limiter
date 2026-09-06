// Package redistest runs a real redis-server for tests that need one.
package redistest

import (
	"context"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// runner is the subset of *testing.T and *testing.B that start needs. Both
// satisfy it through testing's shared common struct.
type runner interface {
	Helper()
	TempDir() string
	Cleanup(func())
	Skip(args ...any)
	Fatalf(format string, args ...any)
}

// Start runs a redis-server bound to a unix socket in the test's temp
// directory and returns a client for it. The server is killed and the client
// closed when the test finishes. A test calling this is skipped when
// redis-server is not installed.
func Start(t *testing.T) *redis.Client {
	t.Helper()
	return start(t)
}

// StartB is Start for a benchmark.
func StartB(b *testing.B) *redis.Client {
	b.Helper()
	return start(b)
}

func start(t runner) *redis.Client {
	t.Helper()

	bin, err := exec.LookPath("redis-server")
	if err != nil {
		t.Skip("redis-server not on PATH")
	}

	sock := filepath.Join(t.TempDir(), "redis.sock")
	cmd := exec.Command(bin,
		"--port", "0",
		"--unixsocket", sock,
		"--save", "",
		"--appendonly", "no",
	)
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting redis-server: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	})

	client := redis.NewClient(&redis.Options{Network: "unix", Addr: sock})
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for {
		if err := client.Ping(ctx).Err(); err == nil {
			return client
		}
		select {
		case <-ctx.Done():
			t.Fatalf("redis-server did not become ready on %s", sock)
		case <-time.After(10 * time.Millisecond):
		}
	}
}
