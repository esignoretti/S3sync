package sync

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/esignoretti/S3sync/internal/cache"
	"github.com/esignoretti/S3sync/internal/config"
)

// newTestStore returns a bbolt-backed cache in a temp dir, closed on cleanup.
func newTestStore(t *testing.T) *cache.Store {
	t.Helper()
	s, err := cache.Open(t.TempDir() + "/cache.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// cachedObjects enumerates a pair's cache through the cursor API the engine
// itself uses, so tests read the cache the same way production does.
func cachedObjects(t *testing.T, store *cache.Store, pairID string) []cache.CachedObject {
	t.Helper()
	cur, closeCursor, err := store.NewCursor(pairID)
	if err != nil {
		t.Fatalf("NewCursor: %v", err)
	}
	defer closeCursor()
	var out []cache.CachedObject
	for cur.Next() {
		out = append(out, cur.Object())
	}
	return out
}

// newTestEngine wires an Engine against an httptest S3 endpoint. The SDK-level
// retryer is disabled so failures surface immediately instead of being retried
// by the SDK, which would make assertions on call counts unreliable.
func newTestEngine(t *testing.T, srvURL string, store *cache.Store, pair *config.SyncPair) *Engine {
	t.Helper()
	client := s3.NewFromConfig(aws.Config{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider("k", "s", ""),
		Retryer:     func() aws.Retryer { return aws.NopRetryer{} },
	}, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(srvURL)
		o.UsePathStyle = true
	})
	src := &config.Bucket{Name: "src", BucketName: "src-bucket", Region: "us-east-1"}
	tgt := &config.Bucket{Name: "tgt", BucketName: "tgt-bucket", Region: "us-east-1"}
	return NewEngine(pair, src, tgt, client, client, store)
}

// emptyListing is a valid, complete ListObjectsV2 response with no objects.
func emptyListing() string {
	return `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult><Name>src-bucket</Name><KeyCount>0</KeyCount><IsTruncated>false</IsTruncated></ListBucketResult>`
}

// oneObjectListing is a valid, complete ListObjectsV2 response with one object.
func oneObjectListing(key, etag string) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult><Name>src-bucket</Name><KeyCount>1</KeyCount><IsTruncated>false</IsTruncated>
<Contents><Key>%s</Key><ETag>%s</ETag><Size>10</Size><LastModified>2026-01-01T00:00:00.000Z</LastModified></Contents></ListBucketResult>`, key, etag)
}

// A key deleted from the target must be removed from the cache. Otherwise the
// next diff still sees it, emits another delete, and the pair never converges.
func TestDeleteConvergesAcrossRuns(t *testing.T) {
	var deleteCalls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		switch r.Method {
		case http.MethodDelete:
			deleteCalls++
			w.WriteHeader(http.StatusNoContent)
		case http.MethodGet:
			fmt.Fprint(w, emptyListing())
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	store := newTestStore(t)
	if err := store.Put(&cache.CachedObject{
		PairID: "p1", Key: "gone.txt", ETag: `"e1"`, SyncedAt: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}

	pair := &config.SyncPair{ID: "p1", Name: "t", WorkerCount: 1, DeletePropagation: true}
	eng := newTestEngine(t, srv.URL, store, pair)

	for run := 1; run <= 2; run++ {
		if err := eng.RunOnce(context.Background()); err != nil {
			t.Fatalf("run %d: %v", run, err)
		}
	}

	if deleteCalls != 1 {
		t.Errorf("DELETE calls = %d, want 1 (delete propagation must converge)", deleteCalls)
	}

	remaining := cachedObjects(t, store, "p1")
	for _, o := range remaining {
		if o.Key == "gone.txt" {
			t.Errorf("deleted key still present in cache after successful delete")
		}
	}
}

// objectPath reports whether a path-style S3 request targets an object rather
// than a bucket. CopyObject/HeadObject/DeleteObject are "/{bucket}/{key}";
// CreateBucket/HeadBucket are just "/{bucket}". Without this distinction a
// bucket-creation PUT is miscounted as an object copy.
func objectPath(p string) bool {
	trimmed := strings.Trim(p, "/")
	return strings.Contains(trimmed, "/")
}

// A dry run only reports what it WOULD do. Recording those planned actions in
// the cache makes the next real run treat them as already synced and skip them,
// so a dry run silently suppresses the sync it was supposed to preview.
func TestDryRunDoesNotWriteCache(t *testing.T) {
	var copyCalls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			if !objectPath(r.URL.Path) {
				w.WriteHeader(http.StatusOK) // bucket exists
				return
			}
			// Absent in target: the copy is genuinely needed.
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, notFoundBody)
		case http.MethodPut:
			if objectPath(r.URL.Path) {
				copyCalls++
			}
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, copyOKBody)
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/xml")
			fmt.Fprint(w, oneObjectListing("important.txt", `"e1"`))
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	store := newTestStore(t)
	pair := &config.SyncPair{ID: "p1", Name: "t", WorkerCount: 1, DryRun: true}
	eng := newTestEngine(t, srv.URL, store, pair)

	if err := eng.RunOnce(context.Background()); err != nil {
		t.Fatalf("dry run: %v", err)
	}

	cached := cachedObjects(t, store, "p1")
	if len(cached) != 0 {
		t.Errorf("cache entries after dry run = %d %v, want 0 (a dry run must not record planned actions)",
			len(cached), cached)
	}
	if copyCalls != 0 {
		t.Errorf("CopyObject calls during dry run = %d, want 0", copyCalls)
	}

	// The real run must still do the work the dry run only previewed.
	pair.DryRun = false
	if err := eng.RunOnce(context.Background()); err != nil {
		t.Fatalf("real run: %v", err)
	}
	if copyCalls != 1 {
		t.Errorf("CopyObject calls after real run = %d, want 1 (dry run must not suppress the copy)", copyCalls)
	}
}

// newCopyServer returns a test S3 endpoint where the source lists exactly one
// object and the target does not have it, so a run performs exactly one copy
// and completes normally.
func newCopyServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			if !objectPath(r.URL.Path) {
				w.WriteHeader(http.StatusOK) // bucket exists
				return
			}
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, notFoundBody)
		case http.MethodPut:
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, copyOKBody)
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/xml")
			fmt.Fprint(w, oneObjectListing("obj.bin", `"e1"`))
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// Status must hand back plain data. Returning Progress by value copies its
// sync.Mutex, which go vet flags and which means readers and writers can end up
// observing different locks.
func TestStatusReturnsSnapshotWithoutMutexCopy(t *testing.T) {
	srv := newCopyServer(t)
	store := newTestStore(t)
	pair := &config.SyncPair{ID: "p1", Name: "t", WorkerCount: 1}
	eng := newTestEngine(t, srv.URL, store, pair)

	running, _, _, _, snap := eng.Status()

	// Compile-time proof the fifth value is a mutex-free snapshot type.
	var _ ProgressSnapshot = snap
	if running {
		t.Error("engine reports running before RunOnce was called")
	}
}

// lastRun must be recorded as part of the same locked update as the status, so
// a concurrent Status() reader can never observe one without the other.
func TestSetResultRecordsLastRun(t *testing.T) {
	srv := newCopyServer(t)
	store := newTestStore(t)
	pair := &config.SyncPair{ID: "p1", Name: "t", WorkerCount: 1}
	eng := newTestEngine(t, srv.URL, store, pair)

	before := time.Now().Add(-time.Second)
	if err := eng.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	running, lastRun, status, _, _ := eng.Status()
	if running {
		t.Error("engine still reports running after RunOnce returned")
	}
	if lastRun.IsZero() {
		t.Fatal("lastRun is zero after a completed run")
	}
	if lastRun.Before(before) {
		t.Errorf("lastRun = %v, want at or after %v", lastRun, before)
	}
	if status != "ok" {
		t.Errorf("status = %q, want %q", status, "ok")
	}
}

// Exercises the lastRun write against concurrent Status() readers under -race.
// The defect is structural and go vet catches it deterministically; this is a
// best-effort runtime check on the same path.
func TestStatusConcurrentWithRunOnce(t *testing.T) {
	srv := newCopyServer(t)
	store := newTestStore(t)
	pair := &config.SyncPair{ID: "p1", Name: "t", WorkerCount: 1}
	eng := newTestEngine(t, srv.URL, store, pair)

	done := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
					eng.Status()
				}
			}
		}()
	}

	if err := eng.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	close(done)
	wg.Wait()
}
