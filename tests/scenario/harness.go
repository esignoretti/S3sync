// tests/scenario/harness.go
//go:build integration

// Package scenario proves the sync engine's behaviour across whole-bucket
// scenarios against a real S3 endpoint. It drives the real Engine, WorkerPool,
// differ and ProvisionTarget in-process rather than re-implementing them, so an
// assertion here is an assertion about the shipped code path.
package scenario

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
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
		for _, v := range out.Versions {
			deleteVersion(t, client, name, aws.ToString(v.Key), v.VersionId)
		}
		// A delete marker holds a VersionId that must itself be deleted, or the
		// bucket never empties.
		for _, d := range out.DeleteMarkers {
			deleteVersion(t, client, name, aws.ToString(d.Key), d.VersionId)
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

// deleteVersion removes one object version, bypassing governance retention if
// the endpoint says the version is held. A COMPLIANCE hold cannot be bypassed,
// so it fails loudly with the retain-until date instead of hanging the suite.
func deleteVersion(t *testing.T, client *s3.Client, bucket, key string, versionID *string) {
	t.Helper()
	ctx := context.Background()
	del := &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), VersionId: versionID}
	if _, err := client.DeleteObject(ctx, del); err == nil {
		return
	}
	del.BypassGovernanceRetention = aws.Bool(true)
	if _, err := client.DeleteObject(ctx, del); err == nil {
		return
	}

	var until string
	if head, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), VersionId: versionID,
	}); err == nil && head.ObjectLockRetainUntilDate != nil {
		until = head.ObjectLockRetainUntilDate.Format(time.RFC3339)
	}
	t.Fatalf("cannot delete %q version %q from %q (retained until %q): a COMPLIANCE hold cannot be bypassed",
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
	o := envOptions{WorkerCount: 4}
	if len(opts) > 0 {
		o = opts[0]
	}
	if o.WorkerCount <= 0 {
		o.WorkerCount = 4
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

	e := &scenarioEnv{
		T:        t,
		Client:   client,
		Source:   src,
		Target:   tgt,
		CacheDir: cacheDir,
		Store:    store,
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
