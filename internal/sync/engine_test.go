package sync

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/esignoretti/S3sync/internal/cache"
	"github.com/esignoretti/S3sync/internal/config"
	"github.com/esignoretti/S3sync/internal/s3client"
)

// NewEngine must hand the pool the source client for source HEADs. Pairs whose
// buckets carry different credentials are allowed, so a source HEAD signed with
// the target's credentials is either a 403 (every object fails) or a 404 (every
// object looks vanished, and the run reports ok with zero copies).
func TestNewEngine_PoolHeadsSourceWithSourceClient(t *testing.T) {
	srcS3, err := s3client.NewClient(&config.Bucket{
		Endpoint: "https://source.example", Region: "us-east-1",
		AccessKey: "source-ak", SecretKey: "source-sk",
	})
	if err != nil {
		t.Fatal(err)
	}
	tgtS3, err := s3client.NewClient(&config.Bucket{
		Endpoint: "https://target.example", Region: "us-east-1",
		AccessKey: "target-ak", SecretKey: "target-sk",
	})
	if err != nil {
		t.Fatal(err)
	}

	pair := &config.SyncPair{ID: "p1", Name: "pair", WorkerCount: 1}
	src := &config.Bucket{BucketName: testSourceBucket}
	tgt := &config.Bucket{BucketName: testTargetBucket}

	e := NewEngine(pair, src, tgt, srcS3, tgtS3, nil)

	if e.pool.src != srcS3 {
		t.Fatalf("pool.src is not the source client: a source HEAD would be signed with the target's credentials")
	}
	if e.pool.tgt != tgtS3 {
		t.Fatalf("pool.tgt is not the target client")
	}
}

// fakeS3Server answers the calls one RunOnce cycle makes for a single source
// object whose HEAD returns 404.
type fakeS3Server struct {
	headBuckets []string
}

func (f *fakeS3Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodHead && r.URL.Path == "/"+testTargetBucket:
		w.WriteHeader(http.StatusOK)
	case r.Method == http.MethodGet && r.URL.Path == "/"+testSourceBucket:
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>%s</Name><KeyCount>1</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>
<Contents><Key>gone.txt</Key><ETag>"e1"</ETag><Size>10</Size><LastModified>2026-01-01T00:00:00.000Z</LastModified></Contents>
</ListBucketResult>`, testSourceBucket)
	case r.Method == http.MethodHead:
		f.headBuckets = append(f.headBuckets, r.URL.Path)
		w.WriteHeader(http.StatusNotFound)
	default:
		w.WriteHeader(http.StatusOK)
	}
}

// The engine-level consequence of a skipped object: a key that vanished before
// its HEAD must not be written to the cache. Cached, it would look synced, and
// a versioned source that later restored the object with an unchanged ETag and
// LastModified would never be copied again.
func TestRunOnce_VanishedObjectIsNotCached(t *testing.T) {
	fake := &fakeS3Server{}
	srv := httptest.NewServer(fake)
	defer srv.Close()

	srcS3, err := s3client.NewClient(&config.Bucket{
		Endpoint: srv.URL, Region: "us-east-1", AccessKey: "ak", SecretKey: "sk",
	})
	if err != nil {
		t.Fatal(err)
	}
	tgtS3, err := s3client.NewClient(&config.Bucket{
		Endpoint: srv.URL, Region: "us-east-1", AccessKey: "ak", SecretKey: "sk",
	})
	if err != nil {
		t.Fatal(err)
	}

	store, err := cache.Open(filepath.Join(t.TempDir(), "cache.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	pair := &config.SyncPair{ID: "p1", Name: "pair", WorkerCount: 1}
	src := &config.Bucket{BucketName: testSourceBucket}
	tgt := &config.Bucket{BucketName: testTargetBucket}

	e := NewEngine(pair, src, tgt, srcS3, tgtS3, store)
	if err := e.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	if len(fake.headBuckets) != 1 || !strings.HasPrefix(fake.headBuckets[0], "/"+testSourceBucket+"/") {
		t.Fatalf("HEAD requests hit %v, want exactly one on the source bucket (the test would be vacuous otherwise)", fake.headBuckets)
	}

	cached, err := store.List(pair.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range cached {
		if o.Key == "gone.txt" {
			t.Fatalf("vanished key %q was cached as synced; it was never copied", o.Key)
		}
	}
	if len(cached) != 0 {
		t.Fatalf("cache holds %d objects, want 0: %+v", len(cached), cached)
	}

	succeeded, failed := e.LastResult()
	if succeeded != 0 || failed != 0 {
		t.Fatalf("LastResult = (%d, %d), want (0, 0): a skipped object is neither", succeeded, failed)
	}
}
