package sync

import (
	"context"
	"testing"
	"time"
)

func TestDisabled(t *testing.T) {
	tr := NewThrottler(0)
	if !tr.Allow() {
		t.Fatal("disabled should always allow")
	}
}

func TestBurst(t *testing.T) {
	tr := NewThrottler(120) // 120/min, burst 120
	for i := 0; i < 100; i++ {
		if !tr.Allow() {
			t.Fatalf("expected allow at %d", i)
		}
	}
}

func TestWait(t *testing.T) {
	tr := NewThrottler(60) // 1/sec, burst 60
	for i := 0; i < 60; i++ {
		tr.Allow()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := tr.Wait(ctx); err == nil {
		t.Fatal("expected timeout")
	}
}
