package sync

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

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

// engineEndpoint answers the S3 calls one RunOnce cycle makes and records every
// request, so a test can assert what the engine did — or, for the expired guard,
// that it touched the target at all.
type engineEndpoint struct {
	mu             sync.Mutex
	requests       []string
	objects        []string // <Contents> entries for the source listing
	tags           string   // <Tag> entries for a source tag read
	lockConfigured bool
}

// engineListingObject is one source object whose identity matches
// engineCachedObject below, so the differ has nothing to do and any copy a test
// observes came from somewhere else.
const engineListingObject = `<Contents><Key>k</Key><ETag>"e1"</ETag><Size>10</Size><LastModified>2026-01-01T00:00:00.000Z</LastModified></Contents>`

var engineCachedObject = cache.CachedObject{
	PairID: "p1", Key: "k", ETag: `"e1"`, Size: 10,
	LastModified: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
}

func (f *engineEndpoint) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	// RequestURI, not Path: a CopyObject and the PutObjectTagging that
	// follows it share a path, and the tests count copies.
	f.requests = append(f.requests, r.Method+" "+r.URL.RequestURI())
	f.mu.Unlock()

	switch {
	case r.Method == http.MethodHead && r.URL.Path == "/"+testTargetBucket:
		w.WriteHeader(http.StatusOK)
	case r.Method == http.MethodGet && r.URL.Query().Has("object-lock"):
		if !f.lockConfigured {
			// What an endpoint without a lock configuration answers.
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		io.WriteString(w, `<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled></ObjectLockConfiguration>`)
	case r.Method == http.MethodGet && r.URL.Path == "/"+testSourceBucket:
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>%s</Name><KeyCount>%d</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>%s</ListBucketResult>`,
			testSourceBucket, len(f.objects), strings.Join(f.objects, ""))
	case r.Method == http.MethodGet && r.URL.Query().Has("tagging"):
		w.Header().Set("Content-Type", "application/xml")
		io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?><Tagging><TagSet>`+f.tags+`</TagSet></Tagging>`)
	case r.Method == http.MethodPut && r.URL.Query().Has("tagging"):
		w.WriteHeader(http.StatusOK)
	case r.Method == http.MethodPut:
		w.Header().Set("Content-Type", "application/xml")
		io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?><CopyObjectResult><ETag>"e1"</ETag></CopyObjectResult>`)
	case r.Method == http.MethodHead:
		w.Header().Set("Content-Type", "text/csv")
		w.Header().Set("Content-Length", "10")
		w.Header().Set("ETag", `"e1"`)
		w.WriteHeader(http.StatusOK)
	default:
		w.WriteHeader(http.StatusOK)
	}
}

func (f *engineEndpoint) log() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.requests...)
}

// newTestEngine builds an engine against a fake endpoint and a real cache, so
// the run exercises the same paths production does.
func newTestEngine(t *testing.T, pair *config.SyncPair, ep *engineEndpoint) *Engine {
	t.Helper()
	srv := httptest.NewServer(ep)
	t.Cleanup(srv.Close)

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
	t.Cleanup(func() { store.Close() })

	if pair.ID == "" {
		pair.ID = "p1"
	}
	if pair.Name == "" {
		pair.Name = "pair"
	}
	if pair.WorkerCount == 0 {
		pair.WorkerCount = 1
	}
	return NewEngine(pair, &config.Bucket{BucketName: testSourceBucket},
		&config.Bucket{BucketName: testTargetBucket}, srcS3, tgtS3, store)
}

// A lifecycle Expiration.Date already in the past has the target deleting
// objects as fast as the run copies them, and the source still holds them, so
// every reset or cache loss re-copies what has already expired. The run must
// refuse, report a terminal status, and touch nothing.
func TestRunOnceRefusesExpiredPair(t *testing.T) {
	past := time.Now().UTC().Add(-time.Hour)
	ep := &engineEndpoint{}
	e := newTestEngine(t, &config.SyncPair{TargetExpireAt: &past}, ep)

	err := e.RunOnce(context.Background())
	if !errors.Is(err, ErrPairExpired) {
		t.Fatalf("err = %v, want ErrPairExpired", err)
	}
	if _, _, status, _, _ := e.Status(); status != "expired" {
		t.Fatalf("status = %q, want expired", status)
	}
	if got := ep.log(); len(got) != 0 {
		t.Fatalf("expired pair made %d S3 requests (%v), want none", len(got), got)
	}
}

func TestRunOnceAllowsPairWhoseExpiryIsInTheFuture(t *testing.T) {
	// Midnight UTC is what the lifecycle builder accepts, so this date proves
	// the run reached provisioning rather than dying in validation.
	future := time.Now().UTC().Truncate(24 * time.Hour).Add(48 * time.Hour)
	ep := &engineEndpoint{}
	e := newTestEngine(t, &config.SyncPair{TargetExpireAt: &future}, ep)

	err := e.RunOnce(context.Background())
	if errors.Is(err, ErrPairExpired) {
		t.Fatal("future expiry must not block the run")
	}
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if _, _, status, _, _ := e.Status(); status != "ok" {
		t.Fatalf("status = %q, want ok", status)
	}
	if !strings.Contains(strings.Join(ep.log(), "\n"), "PUT /"+testTargetBucket+"?lifecycle") {
		t.Fatalf("no lifecycle write: %v", ep.log())
	}
}

func TestProgressCountsPendingAttrs(t *testing.T) {
	p := &Progress{}
	p.AddPending()
	if _, completed, failed := p.Snapshot(); completed != 1 || failed != 0 {
		t.Fatalf("pending must count as completed: %d/%d", completed, failed)
	}
	if got := p.PendingCount(); got != 1 {
		t.Fatalf("PendingCount = %d, want 1", got)
	}
}

// Provisioning failures are what feature 2 exists to prevent, so a pair whose
// target cannot be provisioned must fail the run legibly instead of copying into
// a bucket that cannot hold the data and failing every object.
func TestRunOnce_SurfacesProvisioningError(t *testing.T) {
	ep := &engineEndpoint{lockConfigured: false}
	pair := &config.SyncPair{RequireObjectLock: true, OnLockMissing: "fail"}
	e := newTestEngine(t, pair, ep)

	err := e.RunOnce(context.Background())
	if !errors.Is(err, ErrLockMissing) {
		t.Fatalf("err = %v, want ErrLockMissing", err)
	}
	if _, _, status, lastError, _ := e.Status(); status != "error" || lastError == "" {
		t.Fatalf("status = %q, lastError = %q, want error with a message", status, lastError)
	}
	if pair.ProvisionError == "" {
		t.Fatal("pair.ProvisionError is empty: the failure would be invisible to the API")
	}
	if pair.ProvisionedAt != nil {
		t.Fatalf("ProvisionedAt = %v, want nil after a failed provision", pair.ProvisionedAt)
	}
	for _, req := range ep.log() {
		if strings.HasPrefix(req, "GET /"+testSourceBucket+"?list-type=2") {
			t.Fatalf("the run listed the source after provisioning failed: %v", ep.log())
		}
	}

	// setupOnce means one attempt per engine: a second run must not retry
	// provisioning, and must fail the same way rather than silently proceeding.
	before := len(ep.log())
	if err := e.RunOnce(context.Background()); !errors.Is(err, ErrLockMissing) {
		t.Fatalf("second run err = %v, want ErrLockMissing", err)
	}
	if after := len(ep.log()); after != before {
		t.Fatalf("second run issued %d more S3 requests, want 0", after-before)
	}
}

func TestRunOnce_ClearsProvisionErrorOnSuccess(t *testing.T) {
	ep := &engineEndpoint{}
	pair := &config.SyncPair{ProvisionError: "boom"}
	e := newTestEngine(t, pair, ep)

	if err := e.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if pair.ProvisionError != "" {
		t.Fatalf("ProvisionError = %q, want cleared after a successful provision", pair.ProvisionError)
	}
	if pair.ProvisionedAt == nil {
		t.Fatal("ProvisionedAt is nil after a successful provision")
	}
}

// Drift on an unchanged object is invisible to the differ, so the sweep is the
// only thing that can find it. Its actions must reach the pool through the same
// channel the diff writes to, and the sweep must record that it ran.
func TestRunOnce_SweepsDriftBeforeTheDiff(t *testing.T) {
	ep := &engineEndpoint{
		objects: []string{engineListingObject},
		tags:    `<Tag><Key>env</Key><Value>prod</Value></Tag>`,
	}
	pair := &config.SyncPair{AttrsReconcileInterval: 3600, ReplicateAttributes: true}
	e := newTestEngine(t, pair, ep)

	stale := engineCachedObject
	stale.AttrsHash = "stale"
	if err := e.store.PutMany([]*cache.CachedObject{&stale}); err != nil {
		t.Fatal(err)
	}

	if err := e.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	log := ep.log()
	if len(log) == 0 {
		t.Fatal("no S3 requests at all")
	}
	// The sweep reads the source object before the diff lists the bucket: the
	// sweep's writes must be consumed by a reader that is already running, or a
	// large sweep would block on a full channel before the diff ever starts.
	sweepHead, listing := -1, -1
	for i, req := range log {
		if req == "HEAD /"+testSourceBucket+"/k" && sweepHead < 0 {
			sweepHead = i
		}
		if strings.HasPrefix(req, "GET /"+testSourceBucket+"?list-type=2") && listing < 0 {
			listing = i
		}
	}
	if sweepHead < 0 {
		t.Fatalf("no source HEAD: the sweep never ran: %v", log)
	}
	if listing < 0 || sweepHead > listing {
		t.Fatalf("sweep HEAD at %d, listing at %d: the sweep must run before the diff", sweepHead, listing)
	}

	copies := 0
	for _, req := range log {
		if req == "PUT /"+testTargetBucket+"/k?x-id=CopyObject" {
			copies++
		}
	}
	if copies != 1 {
		t.Fatalf("CopyObject issued %d times, want 1 for the drifted key: %v", copies, log)
	}
	if pair.LastReconcileAt == nil {
		t.Fatal("LastReconcileAt is nil after a completed sweep")
	}

	cached, err := e.store.List(pair.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(cached) != 1 || cached[0].AttrsHash == "" || cached[0].AttrsHash == "stale" {
		t.Fatalf("cache after the repair = %+v, want the recomputed fingerprint", cached)
	}
	if _, _, status, _, _ := e.Status(); status != "ok" {
		t.Fatalf("status = %q, want ok", status)
	}
}

// A dry run reports what it would do and writes nothing. If it cached what it
// "copied", the next real run's differ would see matching ETags and skip those
// objects: the bytes would never arrive and nothing would ever retry them.
func TestRunOnceDryRunDoesNotWriteCache(t *testing.T) {
	ep := &engineEndpoint{objects: []string{
		`<Contents><Key>a.txt</Key><ETag>"e1"</ETag><Size>10</Size><LastModified>2026-01-01T00:00:00.000Z</LastModified></Contents>`,
		`<Contents><Key>b.txt</Key><ETag>"e2"</ETag><Size>10</Size><LastModified>2026-01-01T00:00:00.000Z</LastModified></Contents>`,
	}}
	pair := &config.SyncPair{DryRun: true}
	e := newTestEngine(t, pair, ep)

	if err := e.RunOnce(context.Background()); err != nil {
		t.Fatalf("dry run: %v", err)
	}

	// The run must have seen work, or "cached nothing" proves nothing. LastResult
	// is left at zero by design on a dry run, so read the progress total.
	if _, _, _, _, prog := e.Status(); prog.Total != 2 {
		t.Fatalf("dry run saw %d actions, want 2; the test would be vacuous", prog.Total)
	}
	cached, err := e.store.List(pair.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(cached) != 0 {
		t.Fatalf("cache holds %d objects after a dry run, want 0: %+v", len(cached), cached)
	}
}

// The sweep is interval-gated; running it on every cycle would put a HEAD and a
// tag read on every cached object each time, which is the cost the interval
// exists to avoid.
func TestRunOnce_SkipsSweepWhenNotDue(t *testing.T) {
	last := time.Now().UTC()
	ep := &engineEndpoint{objects: []string{engineListingObject}}
	pair := &config.SyncPair{AttrsReconcileInterval: 3600, LastReconcileAt: &last}
	e := newTestEngine(t, pair, ep)

	cached := engineCachedObject
	cached.AttrsHash = "stale"
	if err := e.store.PutMany([]*cache.CachedObject{&cached}); err != nil {
		t.Fatal(err)
	}

	if err := e.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	for _, req := range ep.log() {
		if req == "HEAD /"+testSourceBucket+"/k" {
			t.Fatalf("sweep ran before its interval elapsed: %v", ep.log())
		}
	}
	if !pair.LastReconcileAt.Equal(last) {
		t.Fatalf("LastReconcileAt moved to %v, want %v", pair.LastReconcileAt, last)
	}
}
