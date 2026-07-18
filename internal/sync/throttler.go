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

func (t *Throttler) WaitLog(ctx context.Context, label string) error {
	if !t.enabled {
		return nil
	}
	if t.limiter.Allow() {
		return nil
	}
	slog.Debug("throttle wait", "label", label)
	start := time.Now()
	err := t.limiter.Wait(ctx)
	if err == nil {
		slog.Debug("throttle done", "label", label, "waited_ms", time.Since(start).Milliseconds())
	}
	return err
}
