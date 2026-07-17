package sync

import (
	"context"
	"errors"
	"testing"
)

func TestWorkerPoolCreation(t *testing.T) {
	pool := NewWorkerPool(5, nil, "src", "tgt", NewThrottler(0), "", nil)
	if pool == nil {
		t.Fatal("expected non-nil pool")
	}
}

func TestRetryS3_Success(t *testing.T) {
	attempts := 0
	err := retryS3(context.Background(), "test", func(ctx context.Context) error {
		attempts++
		return nil
	}, 3)
	if err != nil {
		t.Fatal(err)
	}
	if attempts != 1 {
		t.Fatalf("expected 1 attempt, got %d", attempts)
	}
}

func TestRetryS3_RetryThenSuccess(t *testing.T) {
	attempts := 0
	err := retryS3(context.Background(), "test", func(ctx context.Context) error {
		attempts++
		if attempts < 3 {
			return errors.New("transient")
		}
		return nil
	}, 3)
	if err != nil {
		t.Fatal(err)
	}
	if attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts)
	}
}

func TestRetryS3_Exhausted(t *testing.T) {
	attempts := 0
	err := retryS3(context.Background(), "test", func(ctx context.Context) error {
		attempts++
		return errors.New("persistent")
	}, 2)
	if err == nil {
		t.Fatal("expected error")
	}
	if attempts != 2 {
		t.Fatalf("expected 2 attempts, got %d", attempts)
	}
}

func TestRetryS3_ContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	attempts := 0
	err := retryS3(ctx, "test", func(ctx context.Context) error {
		attempts++
		return errors.New("fail")
	}, 5)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected Canceled, got %v", err)
	}
}
