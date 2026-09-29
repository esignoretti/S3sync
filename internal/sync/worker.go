package sync

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"
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

// maxSingleCopySize is the largest object S3's CopyObject can copy in one call.
// Larger objects need a multipart upload, which is not implemented, so they are
// rejected up front instead of failing as an opaque EntityTooLarge.
const maxSingleCopySize = 5 * 1024 * 1024 * 1024

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
	// S3 rejects CopyObject above 5 GiB with EntityTooLarge, which retryS3 would
	// retry to exhaustion for an operation that can never succeed. Fail fast and
	// name the real reason. Checked before any round trip so no request is wasted.
	if a.Size > maxSingleCopySize {
		return fmt.Errorf("object %q is %d bytes; exceeds the %d-byte single-copy limit (multipart copy not implemented)",
			a.Key, a.Size, int64(maxSingleCopySize))
	}

	reqCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	src := wp.copySource(a.Key)
	needCopy := true
	// HeadObject on the target: a 404 means the object is absent and therefore
	// MUST be copied, so it is a successful probe rather than a failure.
	// Transient errors are retried — they say nothing about whether it exists.
	headErr := retryS3(reqCtx, "HeadObject", func(c context.Context) error {
		if err := wp.throttler.WaitFor(c, wp.targetBucket); err != nil {
			return err
		}
		headOut, err := wp.client.HeadObject(c, &s3.HeadObjectInput{
			Bucket: &wp.targetBucket, Key: &a.Key,
		})
		if err != nil {
			if isNotFound(err) {
				return nil // absent in target -> needs copy, not an error
			}
			return err
		}
		if aws.ToString(headOut.ETag) == a.ETag {
			needCopy = false
		}
		return nil
	}, 3)
	if headErr != nil {
		return headErr
	}
	if !needCopy {
		slog.Debug("skip unchanged", "key", a.Key)
		return nil
	}

	return retryS3(reqCtx, "CopyObject", func(c context.Context) error {
		if err := wp.throttler.WaitFor(c, wp.targetBucket); err != nil {
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
		if err := wp.throttler.WaitFor(c, wp.targetBucket); err != nil {
			return err
		}
		_, err := wp.client.DeleteObject(c, &s3.DeleteObjectInput{
			Bucket: &wp.targetBucket, Key: &a.Key,
		})
		return err
	}, 3)
}

// copySource builds the x-amz-copy-source header value.
// S3 requires each key segment to be URL-encoded separately,
// but the bucket/key separator and slashes in the key path
// must remain literal.
func (wp *WorkerPool) copySource(key string) string {
	segments := strings.Split(key, "/")
	for i, s := range segments {
		segments[i] = url.PathEscape(s)
	}
	return fmt.Sprintf("%s/%s", wp.sourceBucket, strings.Join(segments, "/"))
}

// isNotFound reports whether err is S3's "no such key" answer. The SDK maps a
// HEAD 404 to *types.NotFound even though HEAD carries no error body, because
// the shared error deserializer is configured with UseStatusCode. The raw
// status-code check is a belt-and-braces fallback for any other error shape.
func isNotFound(err error) bool {
	var nf *types.NotFound
	if errors.As(err, &nf) {
		return true
	}
	var re *smithyhttp.ResponseError
	if errors.As(err, &re) {
		return re.HTTPStatusCode() == http.StatusNotFound
	}
	return false
}

func retryS3(ctx context.Context, label string, fn func(context.Context) error, maxAttempts int) error {
	var err error
	delay := 500 * time.Millisecond
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
			if delay > 8*time.Second {
				delay = 8 * time.Second
			}
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
