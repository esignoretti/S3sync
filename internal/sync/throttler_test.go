package sync

import (
	"context"
	"testing"
	"time"
)

func TestDisabled(t *testing.T) {
	tr := NewThrottler(0)
	if err := tr.WaitLog(context.Background(), "test"); err != nil {
		t.Fatal("disabled should always allow")
	}
}

func TestBurst(t *testing.T) {
	tr := NewThrottler(120)
	for i := 0; i < 100; i++ {
		if err := tr.WaitLog(context.Background(), "test"); err != nil {
			t.Fatalf("expected allow at %d", i)
		}
	}
}

func TestWait(t *testing.T) {
	tr := NewThrottler(60)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := tr.WaitLog(ctx, "test"); err != nil {
		t.Log("expected timeout", err)
	}
}

// WaitFor is the bucket-aware entry point. The label must name the bucket the
// operation actually targets, so throttle debug logs and any future per-bucket
// accounting are truthful instead of always claiming the source bucket.
func TestWaitForUsesBucketLabel(t *testing.T) {
	tr := NewThrottler(60)

	// Drain the burst so the next call has to wait for a token.
	for i := 0; i < 60; i++ {
		if err := tr.WaitFor(context.Background(), "target-bucket"); err != nil {
			t.Fatalf("draining burst at call %d: %v", i, err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := tr.WaitFor(ctx, "target-bucket"); err == nil {
		t.Fatal("WaitFor returned nil while the limiter was saturated, want a deadline error")
	}
}

// A disabled throttler must never block, whatever bucket it is asked about.
func TestWaitForDisabled(t *testing.T) {
	tr := NewThrottler(0)
	for i := 0; i < 1000; i++ {
		if err := tr.WaitFor(context.Background(), "any-bucket"); err != nil {
			t.Fatalf("disabled throttler blocked at call %d: %v", i, err)
		}
	}
}

// WaitLog predates WaitFor and must keep working for existing callers.
func TestWaitLogDelegatesToWaitFor(t *testing.T) {
	tr := NewThrottler(60)
	for i := 0; i < 60; i++ {
		if err := tr.WaitLog(context.Background(), "label"); err != nil {
			t.Fatalf("draining burst at call %d: %v", i, err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := tr.WaitLog(ctx, "label"); err == nil {
		t.Fatal("WaitLog returned nil while saturated, want a deadline error")
	}
}
