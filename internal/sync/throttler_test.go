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
