package sync

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
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
