package sync

import (
	"context"
	"log/slog"
	"time"

	"golang.org/x/time/rate"
)

type Throttler struct {
	limiter *rate.Limiter
	enabled bool
}

func NewThrottler(maxOpsPerMinute int) *Throttler {
	if maxOpsPerMinute <= 0 {
		return &Throttler{enabled: false}
	}
	limit := rate.Limit(float64(maxOpsPerMinute) / 60.0)
	return &Throttler{
		limiter: rate.NewLimiter(limit, maxOpsPerMinute),
		enabled: true,
	}
}

// WaitFor blocks until the limiter admits one operation for the given bucket.
// The label is the bucket the operation actually targets — the worker pool does
// its HeadObject/CopyObject/DeleteObject against the TARGET bucket, so passing
// the source bucket there made the throttle logs claim the wrong bucket.
func (t *Throttler) WaitFor(ctx context.Context, bucket string) error {
	if !t.enabled {
		return nil
	}
	if t.limiter.Allow() {
		return nil
	}
	slog.Debug("throttle wait", "bucket", bucket)
	start := time.Now()
	err := t.limiter.Wait(ctx)
	if err == nil {
		slog.Debug("throttle done", "bucket", bucket, "waited_ms", time.Since(start).Milliseconds())
	}
	return err
}

// WaitLog is retained for existing callers and delegates to WaitFor.
func (t *Throttler) WaitLog(ctx context.Context, label string) error {
	return t.WaitFor(ctx, label)
}
