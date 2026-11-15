package sync

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestWorkerPoolCreation(t *testing.T) {
	pool := NewWorkerPool(5, nil, "src", "tgt", NewThrottler(0), "", nil)
	if pool == nil {
		t.Fatal("expected non-nil pool")
	}
}

// fakeCopyAPI stands in for both clients of a WorkerPool.
type fakeCopyAPI struct {
	copied     int
	headCalls  int
	headErr    error
	headLen    int64
	headBucket string
	copyIn     *s3.CopyObjectInput
}

// testSourceBucket/testTargetBucket are the bucket names newTestWorkerPool uses,
// so the fake can answer source and target HEADs differently the way a real
// endpoint does.
const (
	testSourceBucket = "src"
	testTargetBucket = "tgt"
)

func (f *fakeCopyAPI) HeadObject(_ context.Context, in *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	f.headCalls++
	f.headBucket = aws.ToString(in.Bucket)
	if f.headBucket == testTargetBucket {
		// A brand-new key is absent from the target. This is the answer the P0
		// misread as a retryable failure.
		return nil, &s3types.NotFound{}
	}
	if f.headErr != nil {
		return nil, f.headErr
	}
	return &s3.HeadObjectOutput{ContentLength: aws.Int64(f.headLen)}, nil
}

func (f *fakeCopyAPI) CopyObject(_ context.Context, in *s3.CopyObjectInput, _ ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
	f.copied++
	f.copyIn = in
	return &s3.CopyObjectOutput{}, nil
}

func (f *fakeCopyAPI) GetObjectTagging(context.Context, *s3.GetObjectTaggingInput, ...func(*s3.Options)) (*s3.GetObjectTaggingOutput, error) {
	return &s3.GetObjectTaggingOutput{}, nil
}

func (f *fakeCopyAPI) PutObjectTagging(context.Context, *s3.PutObjectTaggingInput, ...func(*s3.Options)) (*s3.PutObjectTaggingOutput, error) {
	return &s3.PutObjectTaggingOutput{}, nil
}

func (f *fakeCopyAPI) DeleteObject(context.Context, *s3.DeleteObjectInput, ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	return &s3.DeleteObjectOutput{}, nil
}

// The real client must satisfy copyAPI with no adapter type.
var _ copyAPI = (*s3.Client)(nil)

func newTestWorkerPool(api copyAPI) *WorkerPool {
	return &WorkerPool{
		workers: 1, src: api, tgt: api,
		sourceBucket: testSourceBucket, targetBucket: testTargetBucket,
		throttler: NewThrottler(0),
	}
}

// TestWorkerPoolRun_CopiesNewObjectAndReportsSuccess is the regression test for
// the P0 from 6d13e9d: the worker HEADed the *target*, and the 404 that a
// brand-new key always produces there was retried and returned as a failure, so
// CopyObject was never issued and a fresh sync copied nothing.
func TestWorkerPoolRun_CopiesNewObjectAndReportsSuccess(t *testing.T) {
	api := &fakeCopyAPI{headLen: 10}
	wp := newTestWorkerPool(api)
	actions := make(chan SyncAction, 1)
	actions <- SyncAction{Type: ActionCopy, Key: "new-key", ETag: `"e1"`}
	close(actions)

	succeeded, copied, failed := wp.Run(context.Background(), actions)
	if failed != 0 || copied != 1 || len(succeeded) != 1 {
		t.Fatalf("Run: copied %d, failed %d; want 1 copied and 0 failed", copied, failed)
	}
	if api.copied != 1 {
		t.Fatalf("CopyObject called %d times, want exactly 1", api.copied)
	}
}

func TestCopyObject_HeadsSourceAndCopiesFromIt(t *testing.T) {
	api := &fakeCopyAPI{headLen: 10}
	wp := newTestWorkerPool(api)
	if err := wp.copyObject(context.Background(), SyncAction{Type: ActionCopy, Key: "k", ETag: `"e1"`}); err != nil {
		t.Fatal(err)
	}
	if api.headBucket != testSourceBucket {
		t.Fatalf("HEADed bucket %q, want the source bucket", api.headBucket)
	}
	if got, want := aws.ToString(api.copyIn.CopySource), testSourceBucket+"/k"; got != want {
		t.Fatalf("CopySource = %q, want %q", got, want)
	}
	if got := aws.ToString(api.copyIn.Bucket); got != testTargetBucket {
		t.Fatalf("copy target = %q, want %q", got, testTargetBucket)
	}
}

// A source object that vanished between listing and HEAD is absent, not a
// failure, and must not burn the retry budget.
func TestCopyObject_SourceVanishedIsSkipped(t *testing.T) {
	api := &fakeCopyAPI{headErr: &s3types.NotFound{}}
	wp := newTestWorkerPool(api)
	if err := wp.copyObject(context.Background(), SyncAction{Type: ActionCopy, Key: "gone", ETag: `"e1"`}); err != nil {
		t.Fatalf("copyObject returned %v; a vanished source is skipped, not failed", err)
	}
	if api.copied != 0 {
		t.Fatalf("CopyObject called %d times, want 0", api.copied)
	}
	if api.headCalls != 1 {
		t.Fatalf("HeadObject called %d times, want 1 (a 404 must not be retried)", api.headCalls)
	}
}

// A genuine HEAD failure is retried and surfaced, and no copy is attempted.
func TestCopyObject_SourceHeadErrorIsRetriedAndFails(t *testing.T) {
	boom := errors.New("connection reset")
	api := &fakeCopyAPI{headErr: boom}
	wp := newTestWorkerPool(api)
	err := wp.copyObject(context.Background(), SyncAction{Type: ActionCopy, Key: "k", ETag: `"e1"`})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want it to wrap the HEAD failure", err)
	}
	if api.headCalls != 3 {
		t.Fatalf("HeadObject called %d times, want 3 attempts", api.headCalls)
	}
	if api.copied != 0 {
		t.Fatalf("CopyObject called %d times, want 0", api.copied)
	}
}

func TestCopyObject_TooLarge(t *testing.T) {
	api := &fakeCopyAPI{headLen: 6 << 30}
	wp := newTestWorkerPool(api)
	err := wp.copyObject(context.Background(), SyncAction{Type: ActionCopy, Key: "big", ETag: `"e"`})
	if !errors.Is(err, ErrObjectTooLarge) {
		t.Fatalf("err = %v, want ErrObjectTooLarge", err)
	}
	if api.copied != 0 {
		t.Fatalf("CopyObject called %d times, want 0", api.copied)
	}
	// The user must be able to see why their object was rejected.
	for _, want := range []string{"6442450944", "5368709120"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("err = %q, want it to name %s", err, want)
		}
	}
}

// 5 GiB is the inclusive CopyObject limit, so it must still be copied.
func TestCopyObject_ExactlyFiveGiBIsCopied(t *testing.T) {
	api := &fakeCopyAPI{headLen: 5 << 30}
	wp := newTestWorkerPool(api)
	if err := wp.copyObject(context.Background(), SyncAction{Type: ActionCopy, Key: "edge", ETag: `"e"`}); err != nil {
		t.Fatalf("copyObject returned %v; 5 GiB is allowed", err)
	}
	if api.copied != 1 {
		t.Fatalf("CopyObject called %d times, want 1", api.copied)
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
