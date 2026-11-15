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
	src          copyAPI
	tgt          copyAPI
	sourceBucket string
	targetBucket string
	throttler    *Throttler
	storageClass string
	progress     *Progress
}

// copyAPI is the subset of the S3 API the copy and delete paths use.
// *s3.Client satisfies it directly; tests substitute a fake.
type copyAPI interface {
	HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	CopyObject(context.Context, *s3.CopyObjectInput, ...func(*s3.Options)) (*s3.CopyObjectOutput, error)
	GetObjectTagging(context.Context, *s3.GetObjectTaggingInput, ...func(*s3.Options)) (*s3.GetObjectTaggingOutput, error)
	PutObjectTagging(context.Context, *s3.PutObjectTaggingInput, ...func(*s3.Options)) (*s3.PutObjectTaggingOutput, error)
	DeleteObject(context.Context, *s3.DeleteObjectInput, ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
}

// ErrObjectTooLarge reports a source object that CopyObject cannot move in one
// request. It is permanent: the object will not shrink on a retry, so it is
// returned unwrapped by retryS3.
var ErrObjectTooLarge = errors.New("object exceeds the 5 GiB CopyObject limit")

// errObjectVanished reports a source object that disappeared between listing
// and HEAD. Run treats it as neither success nor failure: an object that was
// never copied must not reach the cache, or StreamingDiff would consider the
// key synced and never retry it.
var errObjectVanished = errors.New("source object vanished")

const maxCopyObjectSize = 5 << 30

func NewWorkerPool(workers int, client *s3.Client, source, target string,
	throttler *Throttler, storageClass string, progress *Progress) *WorkerPool {
	return &WorkerPool{
		workers: workers, src: client, tgt: client,
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
					if !errors.Is(err, context.Canceled) && !errors.Is(err, errObjectVanished) {
						slog.Error("worker failed", "key", a.Key, "action", a.Type, "error", err)
					}
				} else {
					slog.Info("worker done", "key", a.Key, "action", a.Type, "ms", time.Since(start).Milliseconds())
				}
				results <- result{action: a, err: err}
				if wp.progress != nil && !errors.Is(err, errObjectVanished) {
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
		switch {
		case errors.Is(r.err, errObjectVanished):
			// Skipped: not copied, so neither succeeded nor failed, and never
			// cached. Counting it either way would lie about the run.
		case r.err != nil:
			failed++
		default:
			succeeded = append(succeeded, r.action)
		}
	}

	return succeeded, len(succeeded), failed
}

func (wp *WorkerPool) copyObject(ctx context.Context, a SyncAction) error {
	reqCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	copySource := fmt.Sprintf("%s/%s", wp.sourceBucket, a.Key)

	// HEAD the source, not the target: ContentLength (and, later, the attributes
	// worth replicating) lives on the source, and the differ has already decided
	// this object needs copying. Probing the target here duplicated that decision
	// and treated the 404 a brand-new key always produces as a retryable failure,
	// so CopyObject was never issued for new objects.
	var headOut *s3.HeadObjectOutput
	vanished := false
	err := retryS3(reqCtx, "HeadObject", func(c context.Context) error {
		if err := wp.throttler.WaitLog(c, wp.sourceBucket); err != nil {
			return err
		}
		out, err := wp.src.HeadObject(c, &s3.HeadObjectInput{
			Bucket: &wp.sourceBucket, Key: &a.Key,
		})
		if err != nil {
			// The differ listed this key moments ago; if it is gone now the source
			// object vanished mid-run. That is a skip, not a failure, so it must not
			// reach the retry budget or the failed count.
			if isNotFound(err, "NotFound", "NoSuchKey") {
				vanished = true
				return nil
			}
			return err
		}
		headOut = out
		return nil
	}, 3)
	if err != nil {
		return err
	}
	if vanished {
		slog.Debug("source object vanished", "key", a.Key)
		return errObjectVanished
	}

	if size := aws.ToInt64(headOut.ContentLength); size > maxCopyObjectSize {
		return fmt.Errorf("%w: key %q is %d bytes, limit %d", ErrObjectTooLarge, a.Key, size, int64(maxCopyObjectSize))
	}

	// No target-side ETag comparison: the differ already decided this object
	// differs, and the target does not evaluate x-amz-copy-source-if-match, so a
	// conditional copy would buy nothing.
	return retryS3(reqCtx, "CopyObject", func(c context.Context) error {
		if err := wp.throttler.WaitLog(c, wp.sourceBucket); err != nil {
			return err
		}
		output := &s3.CopyObjectInput{
			Bucket:     &wp.targetBucket,
			CopySource: &copySource,
			Key:        &a.Key,
		}
		if wp.storageClass != "" {
			output.StorageClass = types.StorageClass(wp.storageClass)
		}
		_, err := wp.tgt.CopyObject(c, output)
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
		_, err := wp.tgt.DeleteObject(c, &s3.DeleteObjectInput{
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
