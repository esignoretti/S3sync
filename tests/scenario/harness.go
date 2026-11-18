// tests/scenario/harness.go
//go:build integration

// Package scenario proves the sync engine's behaviour across whole-bucket
// scenarios against a real S3 endpoint. It drives the real Engine, WorkerPool,
// differ and ProvisionTarget in-process rather than re-implementing them, so an
// assertion here is an assertion about the shipped code path.
package scenario

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/esignoretti/S3sync/internal/cache"
	appconfig "github.com/esignoretti/S3sync/internal/config"
	appsync "github.com/esignoretti/S3sync/internal/sync"
)

// scenarioRequestTimeout bounds one request from the scenario client. The
// production client uses 30s (internal/s3client/client.go:22), which is right
// for a sync worker's individual call but too short for a test that composes or
// range-reads multi-gigabyte objects.
const scenarioRequestTimeout = 5 * time.Minute

// newClient builds a path-style S3 client from the S3_TEST_* environment and
// skips the test when they are absent, matching tests/sync_test.go:20-31.
func newClient(t *testing.T) (*s3.Client, string) {
	t.Helper()
	endpoint := os.Getenv("S3_TEST_ENDPOINT")
	if endpoint == "" {
		t.Skip("S3_TEST_ENDPOINT not set")
	}
	accessKey := os.Getenv("S3_TEST_ACCESS_KEY")
	secretKey := os.Getenv("S3_TEST_SECRET_KEY")
	if accessKey == "" || secretKey == "" {
		t.Skip("S3_TEST_ACCESS_KEY or S3_TEST_SECRET_KEY not set")
	}

	creds := credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(creds),
		config.WithHTTPClient(&http.Client{
			Timeout: scenarioRequestTimeout,
			Transport: &http.Transport{
				ResponseHeaderTimeout: 60 * time.Second,
				ExpectContinueTimeout: 5 * time.Second,
				MaxIdleConnsPerHost:   32,
			},
		}),
	)
	if err != nil {
		t.Fatalf("load aws config: %v", err)
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
	return client, endpoint
}

// createBucket creates a uniquely named scratch bucket and registers its
// teardown. objectLock can only be enabled here — this endpoint, like S3,
// refuses to enable it on an existing bucket.
func createBucket(t *testing.T, client *s3.Client, objectLock bool) string {
	t.Helper()
	name := fmt.Sprintf("s3sync-sc-%d", time.Now().UnixNano())
	in := &s3.CreateBucketInput{Bucket: aws.String(name)}
	if objectLock {
		in.ObjectLockEnabledForBucket = aws.Bool(true)
	}
	if _, err := client.CreateBucket(context.Background(), in); err != nil {
		t.Fatalf("create scratch bucket %q: %v", name, err)
	}
	t.Cleanup(func() { destroyBucket(t, client, name) })
	return name
}

// destroyBucket removes a bucket and everything in it, then proves it is gone.
//
// This is deliberately not a ListObjectsV2-then-DeleteBucket loop. That shape
// stops at the first page, cannot remove a delete marker (whose VersionId must
// be deleted explicitly), and cannot remove a retained version. On this endpoint
// a leaked bucket bills forever, because there is no
// AbortIncompleteMultipartUpload lifecycle rule to clean up after a crash.
func destroyBucket(t *testing.T, client *s3.Client, name string) {
	t.Helper()
	ctx := context.Background()

	// A bucket that never existed (or was already destroyed) is not a failure:
	// several scenarios destroy the target deliberately.
	if _, err := client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(name)}); isNoSuchBucket(err) {
		return
	}

	var keyMarker, versionMarker *string
	for {
		out, err := client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
			Bucket:          aws.String(name),
			KeyMarker:       keyMarker,
			VersionIdMarker: versionMarker,
		})
		if isNoSuchBucket(err) {
			return
		}
		if err != nil {
			t.Fatalf("list versions of %q: %v", name, err)
		}
		// Deletions run in parallel: one request to this endpoint costs ~750ms,
		// so tearing down a 1,200-object scenario serially would take 15
		// minutes and dominate the whole suite's runtime.
		type target struct {
			key       string
			versionID *string
		}
		var batch []target
		for _, v := range out.Versions {
			batch = append(batch, target{aws.ToString(v.Key), v.VersionId})
		}
		// A delete marker holds a VersionId that must itself be deleted, or the
		// bucket never empties.
		for _, d := range out.DeleteMarkers {
			batch = append(batch, target{aws.ToString(d.Key), d.VersionId})
		}
		var wg sync.WaitGroup
		sem := make(chan struct{}, 24)
		var mu sync.Mutex
		var delErr error
		for _, tg := range batch {
			wg.Add(1)
			sem <- struct{}{}
			go func(tg target) {
				defer wg.Done()
				defer func() { <-sem }()
				if err := deleteVersionErr(client, name, tg.key, tg.versionID); err != nil {
					mu.Lock()
					if delErr == nil {
						delErr = err
					}
					mu.Unlock()
				}
			}(tg)
		}
		wg.Wait()
		if delErr != nil {
			t.Fatalf("%v", delErr)
		}
		if !aws.ToBool(out.IsTruncated) {
			break
		}
		keyMarker, versionMarker = out.NextKeyMarker, out.NextVersionIdMarker
	}

	// Orphaned parts from an aborted or crashed upload are invisible to the
	// object listing and bill indefinitely.
	var uploadKeyMarker, uploadIDMarker *string
	for {
		ups, err := client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{
			Bucket:         aws.String(name),
			KeyMarker:      uploadKeyMarker,
			UploadIdMarker: uploadIDMarker,
		})
		if err != nil {
			break // best effort; a bucket without multipart support still deletes
		}
		for _, u := range ups.Uploads {
			_, _ = client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
				Bucket: aws.String(name), Key: u.Key, UploadId: u.UploadId,
			})
		}
		if !aws.ToBool(ups.IsTruncated) {
			break
		}
		uploadKeyMarker, uploadIDMarker = ups.NextKeyMarker, ups.NextUploadIdMarker
	}

	// DeleteBucket can fail transiently while the endpoint settles after the
	// deletions, so retry briefly before treating it as a real failure.
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		_, lastErr = client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(name)})
		if lastErr == nil || isNoSuchBucket(lastErr) {
			lastErr = nil
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if lastErr != nil {
		t.Fatalf("delete bucket %q: %v", name, lastErr)
	}

	// Verify. A teardown that reports success without checking is how buckets leak.
	for attempt := 0; attempt < 5; attempt++ {
		if _, err := client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(name)}); isNoSuchBucket(err) {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("bucket %q still exists after teardown", name)
}

// deleteVersionErr removes one object version, bypassing governance retention
// if the endpoint says the version is held. A COMPLIANCE hold cannot be
// bypassed, so it fails with the retain-until date instead of hanging the suite.
func deleteVersionErr(client *s3.Client, bucket, key string, versionID *string) error {
	ctx := context.Background()
	del := &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), VersionId: versionID}
	if _, err := client.DeleteObject(ctx, del); err == nil {
		return nil
	}
	del.BypassGovernanceRetention = aws.Bool(true)
	if _, err := client.DeleteObject(ctx, del); err == nil {
		return nil
	}

	var until string
	if head, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), VersionId: versionID,
	}); err == nil && head.ObjectLockRetainUntilDate != nil {
		until = head.ObjectLockRetainUntilDate.Format(time.RFC3339)
	}
	return fmt.Errorf("cannot delete %q version %q from %q (retained until %q): a COMPLIANCE hold cannot be bypassed",
		key, aws.ToString(versionID), bucket, until)
}

// isNoSuchBucket matches the endpoint's answers for an absent bucket. The SDK
// has no typed error for HeadBucket, so a bare 404 has to be accepted too —
// the same reasoning as internal/sync/provision.go:258-269.
func isNoSuchBucket(err error) bool {
	if err == nil {
		return false
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NoSuchBucket", "NotFound", "NoSuchKey":
			return true
		}
	}
	var respErr interface{ HTTPStatusCode() int }
	if errors.As(err, &respErr) {
		return respErr.HTTPStatusCode() == http.StatusNotFound
	}
	return false
}

// putBody writes a literal body, optionally mutating the put request. Tags are
// NOT set here: this endpoint ignores x-amz-tagging on PutObject as well as on
// CopyObject, so tags always go through putTags.
func putBody(t *testing.T, client *s3.Client, bucket, key, body string, mutate ...func(*s3.PutObjectInput)) {
	t.Helper()
	in := &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   strings.NewReader(body),
	}
	for _, m := range mutate {
		m(in)
	}
	if _, err := client.PutObject(context.Background(), in); err != nil {
		t.Fatalf("put %q: %v", key, err)
	}
}

// putTags replaces an object's tag set. A separate call is mandatory: the
// endpoint stores grants from x-amz-tagging but never applies them.
func putTags(t *testing.T, client *s3.Client, bucket, key string, tags map[string]string) {
	t.Helper()
	set := make([]types.Tag, 0, len(tags))
	for k, v := range tags {
		set = append(set, types.Tag{Key: aws.String(k), Value: aws.String(v)})
	}
	_, err := client.PutObjectTagging(context.Background(), &s3.PutObjectTaggingInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
		Tagging: &types.Tagging{TagSet: set},
	})
	if err != nil {
		t.Fatalf("put tags on %q: %v", key, err)
	}
}

// deleteTags clears an object's tags with an empty tag set, which this endpoint
// accepts.
func deleteTags(t *testing.T, client *s3.Client, bucket, key string) {
	t.Helper()
	_, err := client.PutObjectTagging(context.Background(), &s3.PutObjectTaggingInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
		Tagging: &types.Tagging{TagSet: []types.Tag{}},
	})
	if err != nil {
		t.Fatalf("clear tags on %q: %v", key, err)
	}
}

// envOptions selects the matrix axes for one scenario's world.
type envOptions struct {
	ObjectLock             bool
	Versioning             bool
	ReplicateAttrs         bool
	TargetExpireIn         time.Duration
	RetentionMode          string
	Retention              time.Duration
	WorkerCount            int
	StorageClass           string
	AttrsReconcileInterval int
}

// scenarioEnv is one scenario's isolated world: its own buckets, its own cache
// file, its own pair. Nothing is shared, so scenarios can run in any order and
// one failure cannot contaminate another.
type scenarioEnv struct {
	T        *testing.T
	Client   *s3.Client
	Source   string
	Target   string
	Pair     *appconfig.SyncPair
	CacheDir string

	Store *cache.Store

	// Counter tallies S3 operations by kind so a scenario can assert on wire
	// traffic instead of on reported success.
	Counter *countingTransport

	srcBucket *appconfig.Bucket
	tgtBucket *appconfig.Bucket
	srcClient *s3.Client
	tgtClient *s3.Client

	literalDigest map[string][32]byte
	largeSeed     map[string]string

	bgDone chan struct{}
	bgErr  error
}

// newScenarioEnv creates a fresh source and target and wraps them.
func newScenarioEnv(t *testing.T, opts envOptions) *scenarioEnv {
	t.Helper()
	client, _ := newClient(t)
	src := createBucket(t, client, opts.ObjectLock)
	tgt := createBucket(t, client, opts.ObjectLock)
	e, err := newScenarioEnvWithBuckets(t, client, src, tgt, opts)
	if err != nil {
		t.Fatalf("build scenario env: %v", err)
	}
	return e
}

// newScenarioEnvWithBuckets is the testable core: it refuses a source bucket
// that already holds objects, because a green run built on a leftover bucket
// proves nothing.
func newScenarioEnvWithBuckets(t *testing.T, client *s3.Client, src, tgt string, opts ...envOptions) (*scenarioEnv, error) {
	t.Helper()
	o := envOptions{WorkerCount: 16}
	if len(opts) > 0 {
		o = opts[0]
	}
	if o.WorkerCount <= 0 {
		o.WorkerCount = 16
	}

	if keys, err := listAll(t, client, src); err != nil {
		return nil, fmt.Errorf("list source %q: %w", src, err)
	} else if len(keys) > 0 {
		return nil, fmt.Errorf("source bucket %q already holds %d objects", src, len(keys))
	}

	ctx := context.Background()
	if o.Versioning {
		for _, b := range []string{src, tgt} {
			if _, err := client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
				Bucket: aws.String(b), VersioningConfiguration: &types.VersioningConfiguration{
					Status: types.BucketVersioningStatusEnabled,
				},
			}); err != nil {
				return nil, fmt.Errorf("enable versioning on %q: %w", b, err)
			}
		}
	}

	cacheDir := filepath.Join(t.TempDir(), "cache.bolt")
	store, err := cache.Open(cacheDir)
	if err != nil {
		return nil, fmt.Errorf("open cache: %w", err)
	}
	t.Cleanup(func() { store.Close() })

	// Both clients share one counting transport, so a copy in either direction
	// is tallied. The transport wraps the scenario HTTP client's transport so
	// the counting is per-request, not per-SDK-call.
	counter := newCountingTransport(&http.Transport{
		ResponseHeaderTimeout: 60 * time.Second,
		ExpectContinueTimeout: 5 * time.Second,
		MaxIdleConnsPerHost:   32,
	})
	countedCfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			os.Getenv("S3_TEST_ACCESS_KEY"), os.Getenv("S3_TEST_SECRET_KEY"), "")),
		config.WithHTTPClient(&http.Client{Timeout: scenarioRequestTimeout, Transport: counter}),
	)
	if err != nil {
		return nil, fmt.Errorf("load counted aws config: %w", err)
	}
	newCounted := func() *s3.Client {
		return s3.NewFromConfig(countedCfg, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(os.Getenv("S3_TEST_ENDPOINT"))
			o.UsePathStyle = true
		})
	}

	e := &scenarioEnv{
		T:         t,
		Client:    client,
		Source:    src,
		Target:    tgt,
		CacheDir:  cacheDir,
		Store:     store,
		Counter:   counter,
		srcClient: newCounted(),
		tgtClient: newCounted(),
		srcBucket: &appconfig.Bucket{
			Name: "src", Endpoint: os.Getenv("S3_TEST_ENDPOINT"), Region: "us-east-1",
			AccessKey: os.Getenv("S3_TEST_ACCESS_KEY"), SecretKey: os.Getenv("S3_TEST_SECRET_KEY"),
			BucketName: src, ObjectLock: o.ObjectLock, Versioning: o.Versioning,
			RetentionMode: o.RetentionMode,
		},
		tgtBucket: &appconfig.Bucket{
			Name: "tgt", Endpoint: os.Getenv("S3_TEST_ENDPOINT"), Region: "us-east-1",
			AccessKey: os.Getenv("S3_TEST_ACCESS_KEY"), SecretKey: os.Getenv("S3_TEST_SECRET_KEY"),
			BucketName: tgt, ObjectLock: o.ObjectLock, Versioning: o.Versioning,
			RetentionMode: o.RetentionMode,
		},
		literalDigest: map[string][32]byte{},
		largeSeed:     map[string]string{},
	}

	pair := &appconfig.SyncPair{
		ID:                     fmt.Sprintf("pair-%d", time.Now().UnixNano()),
		Name:                   "scenario",
		WorkerCount:            o.WorkerCount,
		ReplicateAttributes:    o.ReplicateAttrs,
		AttrsReconcileInterval: o.AttrsReconcileInterval,
		TargetStorageClass:     o.StorageClass,
		OnLockMissing:          "fail",
		Enabled:                true,
	}
	if o.TargetExpireIn > 0 {
		// Midnight UTC, because the lifecycle rule carries a Date and this
		// endpoint rejects anything else (internal/sync/lifecycle.go).
		exp := time.Now().UTC().Add(o.TargetExpireIn).Truncate(24 * time.Hour)
		pair.TargetExpireAt = &exp
	}
	e.Pair = pair
	return e, nil
}

// listAll returns every key in a bucket, following pagination. A scenario that
// only looked at the first page would silently pass on a truncated bucket.
func listAll(t *testing.T, client *s3.Client, bucket string) ([]string, error) {
	t.Helper()
	var keys []string
	var token *string
	for {
		out, err := client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			ContinuationToken: token,
		})
		if err != nil {
			return nil, err
		}
		for _, o := range out.Contents {
			keys = append(keys, aws.ToString(o.Key))
		}
		if !aws.ToBool(out.IsTruncated) {
			return keys, nil
		}
		token = out.NextContinuationToken
	}
}

// engineTimeout is the per-run budget for scenario syncs. Large-object
// scenarios need more than the 300s production default.
const engineTimeout = 3600

// newEngine builds the production engine for this scenario's world. Nothing is
// stubbed: the differ, worker pool, multipart path and provisioning are the
// shipped ones, so a green assertion here is an assertion about shipped code.
func (e *scenarioEnv) newEngine() *appsync.Engine {
	return appsync.NewEngine(e.Pair, e.srcBucket, e.tgtBucket, e.srcClient, e.tgtClient, e.Store,
		func() appconfig.Settings { return appconfig.Settings{RunTimeoutSeconds: engineTimeout} })
}

// sync runs one full sync and fails the test if the engine returns an error.
func (e *scenarioEnv) sync() {
	e.T.Helper()
	if err := e.newEngine().RunOnce(context.Background()); err != nil {
		e.T.Fatalf("sync: %v", err)
	}
}

// syncExpectingError runs one sync and returns the error instead of failing, for
// scenarios that assert a run is refused.
func (e *scenarioEnv) syncExpectingError() error {
	e.T.Helper()
	return e.newEngine().RunOnce(context.Background())
}

// assertNoWorkOnSecondSync is the idempotence assertion and the reason this
// suite exists. Counting requests is deliberately stronger than reading
// Engine.LastResult(): a copy that happens and is then cached reports success,
// which is exactly the shape of the dry-run-poisoning and stale-fingerprint
// bugs this suite is meant to catch.
func (e *scenarioEnv) assertNoWorkOnSecondSync() {
	e.T.Helper()
	e.Counter.Reset()
	e.sync()
	copies := e.Counter.Count(kindCopyObject) + e.Counter.Count(kindUploadPartCopy) + e.Counter.Count(kindPutObject)
	if copies != 0 {
		e.T.Fatalf("second sync copied %d objects, want 0 (%s)", copies, e.Counter.Summary())
	}
}

// targetKeys lists the target with full pagination.
func (e *scenarioEnv) targetKeys() []string {
	e.T.Helper()
	keys, err := listAll(e.T, e.Client, e.Target)
	if err != nil {
		e.T.Fatalf("list target: %v", err)
	}
	return keys
}

// assertTargetCount requires an exact object count in the target.
func (e *scenarioEnv) assertTargetCount(want int) {
	e.T.Helper()
	keys := e.targetKeys()
	if len(keys) != want {
		e.T.Fatalf("target holds %d objects, want %d: %v", len(keys), want, keys)
	}
}

// assertBytesMatch proves the target object's bytes are the source's bytes. The
// expected digest is recomputed from the key and size rather than downloaded
// from the source, so the assertion cannot pass merely because both sides are
// equally wrong.
func (e *scenarioEnv) assertBytesMatch(key string) {
	e.T.Helper()
	ctx := context.Background()

	head, err := e.Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(e.Target), Key: aws.String(key),
	})
	if err != nil {
		e.T.Fatalf("head target %q: %v", key, err)
	}
	size := aws.ToInt64(head.ContentLength)

	got, err := digestOfObject(ctx, e.Client, e.Target, key)
	if err != nil {
		e.T.Fatalf("digest target %q: %v", key, err)
	}
	want, ok := e.literalDigest[key]
	if !ok {
		want = synthChecksum(key, size)
	}
	if got != want {
		e.T.Fatalf("target %q has digest %x, want %x (size %d)", key, got, want, size)
	}
}

// objectFacts is every replicated characteristic of one object.
type objectFacts struct {
	ContentType   string
	Metadata      map[string]string
	Tags          map[string]string
	RetentionMode string
	RetainUntil   *time.Time
	LegalHold     string
}

// fetchFacts reads one object's characteristics. Tags need a separate call
// because HeadObject never returns them on this endpoint.
func fetchFacts(t *testing.T, client *s3.Client, bucket, key string) objectFacts {
	t.Helper()
	ctx := context.Background()
	head, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
	})
	if err != nil {
		t.Fatalf("head %q in %q: %v", key, bucket, err)
	}
	f := objectFacts{
		ContentType:   aws.ToString(head.ContentType),
		Metadata:      head.Metadata,
		RetentionMode: string(head.ObjectLockMode),
		RetainUntil:   head.ObjectLockRetainUntilDate,
		LegalHold:     string(head.ObjectLockLegalHoldStatus),
	}
	tags, err := client.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
	})
	if err != nil {
		t.Fatalf("get tags of %q in %q: %v", key, bucket, err)
	}
	f.Tags = map[string]string{}
	for _, tag := range tags.TagSet {
		f.Tags[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
	}
	return f
}

// assertFactsMatch compares source characteristics to target characteristics
// field by field, naming the key and field in the failure.
func (e *scenarioEnv) assertFactsMatch(key string) {
	e.T.Helper()
	src := fetchFacts(e.T, e.Client, e.Source, key)
	tgt := fetchFacts(e.T, e.Client, e.Target, key)

	if src.ContentType != tgt.ContentType {
		e.T.Fatalf("key %q: content-type = %q, want %q", key, tgt.ContentType, src.ContentType)
	}
	for k, v := range src.Metadata {
		if tgt.Metadata[k] != v {
			e.T.Fatalf("key %q: metadata %q = %q, want %q", key, k, tgt.Metadata[k], v)
		}
	}
	for k := range tgt.Metadata {
		if _, ok := src.Metadata[k]; !ok {
			e.T.Fatalf("key %q: target has extra metadata %q = %q", key, k, tgt.Metadata[k])
		}
	}
	if len(src.Tags) != len(tgt.Tags) {
		e.T.Fatalf("key %q: %d tags, want %d (%v vs %v)", key, len(tgt.Tags), len(src.Tags), tgt.Tags, src.Tags)
	}
	for k, v := range src.Tags {
		if tgt.Tags[k] != v {
			e.T.Fatalf("key %q: tag %q = %q, want %q", key, k, tgt.Tags[k], v)
		}
	}
	if src.RetentionMode != tgt.RetentionMode {
		e.T.Fatalf("key %q: retention mode = %q, want %q", key, tgt.RetentionMode, src.RetentionMode)
	}
	if src.LegalHold != tgt.LegalHold {
		e.T.Fatalf("key %q: legal hold = %q, want %q", key, tgt.LegalHold, src.LegalHold)
	}
	// Retain-until is compared to the second: the endpoint truncates it.
	if src.RetainUntil != nil && tgt.RetainUntil == nil {
		e.T.Fatalf("key %q: target lost the retain-until date %s", key, src.RetainUntil)
	}
	if src.RetainUntil != nil && tgt.RetainUntil != nil {
		if d := src.RetainUntil.Sub(*tgt.RetainUntil); d > time.Second || d < -time.Second {
			e.T.Fatalf("key %q: retain-until = %s, want %s", key, tgt.RetainUntil, src.RetainUntil)
		}
	}
}

// seed writes synthetic objects derived from their keys. Uploads run in
// parallel because per-request latency, not bandwidth, dominates this
// endpoint: one small object takes ~1s, so a 5,000-object fixture is only
// feasible concurrently.
func (e *scenarioEnv) seed(keys []string) {
	e.T.Helper()
	sem := make(chan struct{}, 16)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	for _, key := range keys {
		wg.Add(1)
		sem <- struct{}{}
		go func(key string) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := e.seedOneErr(key); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
			}
		}(key)
	}
	wg.Wait()
	if firstErr != nil {
		e.T.Fatalf("seed: %v", firstErr)
	}
}

// seedOne writes one synthetic object and fails the test on error.
func (e *scenarioEnv) seedOne(key string) {
	e.T.Helper()
	if err := e.seedOneErr(key); err != nil {
		e.T.Fatalf("%v", err)
	}
}

// seedOneErr writes one synthetic object sized by its shape.
//
// ContentLength is mandatory: this endpoint answers 400 InvalidRequest for a
// PutObject whose body is non-seekable and has no declared length (it does not
// accept chunked encoding), and synthReader is deliberately non-seekable so it
// can stream a 6 GiB fixture in bounded memory.
func (e *scenarioEnv) seedOneErr(key string) error {
	size := e.sizeFor(key)
	_, err := e.Client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket:        aws.String(e.Source),
		Key:           aws.String(key),
		Body:          newSynthReader(key, size),
		ContentLength: aws.Int64(size),
		ContentType:   aws.String(contentTypeFor(key)),
	})
	if err != nil {
		return fmt.Errorf("seed %q (%d bytes): %w", key, size, err)
	}
	return nil
}

// sizeFor derives an object's size from its key's shape suffix, defaulting to
// the "small" shape.
func (e *scenarioEnv) sizeFor(key string) int64 {
	for _, sh := range shapes {
		if strings.Contains(key, "-"+sh.Name+".") {
			return synthSize(key, sh)
		}
	}
	return synthSize(key, shapes[1])
}

// contentTypeFor gives each shape a distinct content type so a dropped
// Content-Type is visible.
func contentTypeFor(key string) string {
	switch {
	case strings.Contains(key, "-tiny."), strings.Contains(key, "-small."):
		return "text/plain"
	case strings.Contains(key, "-medium."):
		return "application/json"
	default:
		return "application/octet-stream"
	}
}

// seedCacheOnly writes cache rows with the source's real ETag and LastModified,
// as if a previous run had synced the keys. Used to make the differ emit an
// action for a key that no longer exists.
func (e *scenarioEnv) seedCacheOnly(keys []string) {
	e.T.Helper()
	ctx := context.Background()
	rows := make([]*cache.CachedObject, 0, len(keys))
	for _, key := range keys {
		head, err := e.Client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(e.Source), Key: aws.String(key),
		})
		if err != nil {
			e.T.Fatalf("head source %q: %v", key, err)
		}
		rows = append(rows, &cache.CachedObject{
			PairID:       e.Pair.ID,
			Key:          key,
			ETag:         aws.ToString(head.ETag),
			Size:         aws.ToInt64(head.ContentLength),
			LastModified: aws.ToTime(head.LastModified),
			SyncedAt:     time.Now().UTC(),
		})
	}
	if err := e.Store.PutMany(rows); err != nil {
		e.T.Fatalf("seed cache: %v", err)
	}
}

// cachedKey reports whether the cache holds a row for key.
func (e *scenarioEnv) cachedKey(key string) (cache.CachedObject, bool) {
	e.T.Helper()
	rows, err := e.Store.List(e.Pair.ID)
	if err != nil {
		e.T.Fatalf("list cache: %v", err)
	}
	for _, r := range rows {
		if r.Key == key {
			return r, true
		}
	}
	return cache.CachedObject{}, false
}

// manyKeys returns n small-object keys.
func manyKeys(n int) []string {
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		keys[i] = synthKey(i, "small")
	}
	return keys
}

// deleteFromSource removes keys from the source bucket.
func deleteFromSource(t *testing.T, client *s3.Client, bucket string, keys []string) {
	t.Helper()
	for _, key := range keys {
		if _, err := client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
			Bucket: aws.String(bucket), Key: aws.String(key),
		}); err != nil {
			t.Fatalf("delete %q: %v", key, err)
		}
	}
}

// digestOfObject streams one object through SHA-256.
func digestOfObject(ctx context.Context, client *s3.Client, bucket, key string) ([32]byte, error) {
	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
	})
	if err != nil {
		return [32]byte{}, err
	}
	defer out.Body.Close()
	h := sha256.New()
	if _, err := io.Copy(h, out.Body); err != nil {
		return [32]byte{}, err
	}
	var sum [32]byte
	copy(sum[:], h.Sum(nil))
	return sum, nil
}

// digestOf is the SHA-256 of a literal body.
func digestOf(body string) [32]byte {
	return sha256.Sum256([]byte(body))
}

// contains reports membership.
func contains(ss []string, want string) bool {
	for _, s := range ss {
		if s == want {
			return true
		}
	}
	return false
}
