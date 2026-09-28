package sync

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
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

	remaining, err := store.List("p1")
	if err != nil {
		t.Fatal(err)
	}
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

	cached, err := store.List("p1")
	if err != nil {
		t.Fatal(err)
	}
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
