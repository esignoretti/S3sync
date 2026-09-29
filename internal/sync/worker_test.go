package sync

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// copyOKBody is the minimal valid CopyObject response. An empty 200 body makes
// the SDK report "deserialization failed, received empty response payload",
// which masks whether the copy actually happened.
const copyOKBody = `<?xml version="1.0" encoding="UTF-8"?>
<CopyObjectResult><ETag>"abc123"</ETag><LastModified>2026-01-01T00:00:00.000Z</LastModified></CopyObjectResult>`

const notFoundBody = `<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>NotFound</Code><Message>Not Found</Message></Error>`

// newTestPool builds a WorkerPool whose S3 client talks to srv. The SDK-level
// retryer is disabled so these tests exercise retryS3's behaviour only; the
// SDK would otherwise retry 503s itself and hide a missing retry here.
func newTestPool(t *testing.T, srvURL string) *WorkerPool {
	t.Helper()
	client := s3.NewFromConfig(aws.Config{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider("k", "s", ""),
		Retryer:     func() aws.Retryer { return aws.NopRetryer{} },
	}, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(srvURL)
		o.UsePathStyle = true
	})
	return NewWorkerPool(1, client, "src-bucket", "tgt-bucket", NewThrottler(0), "", nil)
}

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

// A 404 from HeadObject means the object is absent from the target, which is
// exactly the case where a copy IS required. It must not be reported as an
// error, and it must not be retried.
func TestCopyObjectHeadNotFoundStillCopies(t *testing.T) {
	var headCalls, copyCalls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			headCalls++
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, notFoundBody)
		case http.MethodPut:
			copyCalls++
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, copyOKBody)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	pool := newTestPool(t, srv.URL)
	err := pool.copyObject(context.Background(), SyncAction{
		Type: ActionCopy, Key: "new-object.bin", ETag: `"abc123"`,
		Size: 1024, LastModified: time.Now(),
	})

	if err != nil {
		t.Fatalf("copyObject returned error for a new object: %v", err)
	}
	if copyCalls != 1 {
		t.Errorf("CopyObject calls = %d, want 1", copyCalls)
	}
	if headCalls != 1 {
		t.Errorf("HeadObject calls = %d, want 1 (404 must not be retried)", headCalls)
	}
}

// HEAD responses carry no body, so a 404 arrives with nothing to deserialise.
// The SDK must still surface it as a not-found, not as a generic API error.
func TestCopyObjectHead404WithEmptyBodyStillCopies(t *testing.T) {
	var headCalls, copyCalls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			headCalls++
			w.WriteHeader(http.StatusNotFound) // no body: HEAD responses have none
		case http.MethodPut:
			copyCalls++
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, copyOKBody)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	pool := newTestPool(t, srv.URL)
	err := pool.copyObject(context.Background(), SyncAction{
		Type: ActionCopy, Key: "new-object.bin", ETag: `"abc123"`,
		Size: 1024, LastModified: time.Now(),
	})

	t.Logf("bodyless 404 -> err = %v", err)
	if err != nil {
		t.Fatalf("copyObject returned error for a bodyless HEAD 404: %v", err)
	}
	if copyCalls != 1 {
		t.Errorf("CopyObject calls = %d, want 1", copyCalls)
	}
	if headCalls != 1 {
		t.Errorf("HeadObject calls = %d, want 1 (404 must not be retried)", headCalls)
	}
}

// A transient 5xx on HEAD must be retried: the object may genuinely need
// copying and the failure says nothing about whether it exists.
func TestCopyObjectHeadTransientErrorIsRetried(t *testing.T) {
	var headCalls, copyCalls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			headCalls++
			if headCalls < 3 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.Header().Set("ETag", `"different"`)
			w.WriteHeader(http.StatusOK)
		case http.MethodPut:
			copyCalls++
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, copyOKBody)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	pool := newTestPool(t, srv.URL)
	err := pool.copyObject(context.Background(), SyncAction{
		Type: ActionCopy, Key: "flaky.bin", ETag: `"abc123"`,
		Size: 1024, LastModified: time.Now(),
	})

	if err != nil {
		t.Fatalf("copyObject returned error after transient HEAD failures: %v", err)
	}
	if headCalls != 3 {
		t.Errorf("HeadObject calls = %d, want 3 (two 503s then success)", headCalls)
	}
	if copyCalls != 1 {
		t.Errorf("CopyObject calls = %d, want 1", copyCalls)
	}
}

// S3's CopyObject cannot copy an object larger than 5 GiB — it answers with
// EntityTooLarge, which retryS3 would burn all its attempts on even though the
// operation can never succeed. Detect the oversize case up front and say so.
func TestCopyObjectRejectsOversizeObject(t *testing.T) {
	const sixGiB = 6 * 1024 * 1024 * 1024

	var headCalls, copyCalls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			headCalls++
			w.WriteHeader(http.StatusNotFound)
		case http.MethodPut:
			copyCalls++
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, copyOKBody)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	pool := newTestPool(t, srv.URL)
	err := pool.copyObject(context.Background(), SyncAction{
		Type: ActionCopy, Key: "huge.bin", ETag: `"abc123"`,
		Size: sixGiB, LastModified: time.Now(),
	})

	if err == nil {
		t.Fatal("copyObject accepted a 6 GiB object, want an error naming the 5 GiB limit")
	}
	if !strings.Contains(err.Error(), "huge.bin") {
		t.Errorf("error %q does not name the offending key", err)
	}
	if !strings.Contains(err.Error(), "multipart") {
		t.Errorf("error %q does not mention that multipart copy is unimplemented", err)
	}
	if copyCalls != 0 {
		t.Errorf("CopyObject calls = %d, want 0 (the copy can never succeed)", copyCalls)
	}
	if headCalls != 0 {
		t.Errorf("HeadObject calls = %d, want 0 (reject before any round trip)", headCalls)
	}
}

// The limit is inclusive: exactly 5 GiB is still a legal single CopyObject.
// Pinning the boundary keeps an off-by-one from silently refusing copies that
// S3 would have accepted.
func TestCopyObjectAllowsExactlyFiveGiB(t *testing.T) {
	const fiveGiB = 5 * 1024 * 1024 * 1024

	var copyCalls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.WriteHeader(http.StatusNotFound)
		case http.MethodPut:
			copyCalls++
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, copyOKBody)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	pool := newTestPool(t, srv.URL)
	err := pool.copyObject(context.Background(), SyncAction{
		Type: ActionCopy, Key: "big-but-legal.bin", ETag: `"abc123"`,
		Size: fiveGiB, LastModified: time.Now(),
	})

	if err != nil {
		t.Fatalf("copyObject rejected an exactly-5-GiB object: %v", err)
	}
	if copyCalls != 1 {
		t.Errorf("CopyObject calls = %d, want 1", copyCalls)
	}
}
