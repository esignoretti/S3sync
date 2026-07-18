package sync

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type WorkerPool struct {
	workers      int
	client       *s3.Client
	sourceBucket string
	targetBucket string
	throttler    *Throttler
	storageClass string
	progress     *Progress
}

func NewWorkerPool(workers int, client *s3.Client, source, target string,
	throttler *Throttler, storageClass string, progress *Progress) *WorkerPool {
	return &WorkerPool{
		workers: workers, client: client,
		sourceBucket: source, targetBucket: target,
		throttler: throttler, storageClass: storageClass,
		progress: progress,
	}
}

// Run reads actions from the channel until it is closed.
func (wp *WorkerPool) Run(ctx context.Context, actions <-chan SyncAction) ([]SyncAction, int, int) {
	type result struct {
		action SyncAction
		err    error
	}

	results := make(chan result)
	var wg sync.WaitGroup

	workerCount := wp.workers
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for a := range actions {
				start := time.Now()
				var err error
				switch a.Type {
				case ActionCopy:
					err = wp.copyObject(ctx, a)
				case ActionDelete:
					err = wp.deleteObject(ctx, a)
				}
				if err != nil {
					if !errors.Is(err, context.Canceled) {
						slog.Error("worker failed", "key", a.Key, "action", a.Type, "error", err)
					}
				} else {
					slog.Info("worker done", "key", a.Key, "action", a.Type, "ms", time.Since(start).Milliseconds())
				}
			results <- result{action: a, err: err}
			if wp.progress != nil {
				wp.progress.Add(err != nil)
			}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var succeeded []SyncAction
	failed := 0
	for r := range results {
		if r.err != nil {
			failed++
		} else {
			succeeded = append(succeeded, r.action)
		}
	}

	return succeeded, len(succeeded), failed
}

func (wp *WorkerPool) copyObject(ctx context.Context, a SyncAction) error {
	reqCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	src := fmt.Sprintf("%s/%s", wp.sourceBucket, a.Key)
	needCopy := true
	err := retryS3(reqCtx, "HeadObject", func(c context.Context) error {
		if err := wp.throttler.WaitLog(c, wp.sourceBucket); err != nil {
			return err
		}
		headOut, err := wp.client.HeadObject(c, &s3.HeadObjectInput{
			Bucket: &wp.targetBucket, Key: &a.Key,
		})
		if err != nil {
			return err
		}
		if aws.ToString(headOut.ETag) == a.ETag {
			needCopy = false
		}
		return nil
	}, 3)
	if err != nil {
		return err
	}
	if !needCopy {
		slog.Debug("skip unchanged", "key", a.Key)
		return nil
	}

	return retryS3(reqCtx, "CopyObject", func(c context.Context) error {
		if err := wp.throttler.WaitLog(c, wp.sourceBucket); err != nil {
			return err
		}
		output := &s3.CopyObjectInput{
			Bucket:     &wp.targetBucket,
			CopySource: &src,
			Key:        &a.Key,
		}
		if wp.storageClass != "" {
			output.StorageClass = types.StorageClass(wp.storageClass)
		}
		_, err := wp.client.CopyObject(c, output)
		return err
	}, 3)
}

func (wp *WorkerPool) deleteObject(ctx context.Context, a SyncAction) error {
	reqCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	return retryS3(reqCtx, "DeleteObject", func(c context.Context) error {
		if err := wp.throttler.WaitLog(c, wp.sourceBucket); err != nil {
			return err
		}
		_, err := wp.client.DeleteObject(c, &s3.DeleteObjectInput{
			Bucket: &wp.targetBucket, Key: &a.Key,
		})
		return err
	}, 3)
}

func retryS3(ctx context.Context, label string, fn func(context.Context) error, maxAttempts int) error {
	var err error
	delay := time.Second
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if attempt > 1 {
			slog.Debug("retry", "label", label, "attempt", attempt, "delay_ms", delay.Milliseconds())
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			}
			delay *= 2
		}
		err = fn(ctx)
		if err == nil {
			return nil
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		if attempt < maxAttempts {
			slog.Warn("s3 call failed, retrying", "label", label, "attempt", attempt, "max", maxAttempts, "error", err)
		}
	}
	return fmt.Errorf("%s failed after %d attempts: %w", label, maxAttempts, err)
}
