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
	mu           sync.Mutex
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
					wp.mu.Lock()
					wp.progress.Completed++
					if err != nil {
						wp.progress.Failed++
					}
					wp.mu.Unlock()
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

	if err := wp.throttler.WaitLog(reqCtx, wp.sourceBucket); err != nil {
		return err
	}

	headOut, err := wp.client.HeadObject(reqCtx, &s3.HeadObjectInput{
		Bucket: &wp.targetBucket, Key: &a.Key,
	})
	if err == nil {
		if aws.ToString(headOut.ETag) == a.ETag {
			slog.Debug("skip unchanged", "key", a.Key)
			return nil
		}
	}

	if err := wp.throttler.WaitLog(reqCtx, wp.sourceBucket); err != nil {
		return err
	}

	src := fmt.Sprintf("%s/%s", wp.sourceBucket, a.Key)
	input := &s3.CopyObjectInput{
		Bucket:     &wp.targetBucket,
		CopySource: &src,
		Key:        &a.Key,
	}
	if wp.storageClass != "" {
		input.StorageClass = types.StorageClass(wp.storageClass)
	}

	_, err = wp.client.CopyObject(reqCtx, input)
	return err
}

func (wp *WorkerPool) deleteObject(ctx context.Context, a SyncAction) error {
	reqCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := wp.throttler.WaitLog(reqCtx, wp.sourceBucket); err != nil {
		return err
	}
	_, err := wp.client.DeleteObject(reqCtx, &s3.DeleteObjectInput{
		Bucket: &wp.targetBucket, Key: &a.Key,
	})
	return err
}
