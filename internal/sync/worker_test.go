package sync

import "testing"

func TestWorkerPoolCreation(t *testing.T) {
	pool := NewWorkerPool(5, nil, "src", "tgt", NewThrottler(0), "")
	if pool == nil {
		t.Fatal("expected non-nil pool")
	}
}
