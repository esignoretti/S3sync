// tests/scenario/large.go
//go:build integration

package scenario

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// chunkSize is the size of the single seeded chunk every large object is
// composed from. Upload bandwidth to this endpoint is ~2 MB/s, so a 6 GiB
// object cannot be uploaded at all — but server-side copy runs at ~450 MB/s,
// which is what makes composing it feasible.
const chunkSize = 64 << 20

// seedChunkKey is the key of the chunk inside the bucket being composed into.
// It is deleted after composition so it never appears in a sync.
const seedChunkKey = "_seed/chunk.bin"

// Tier names for SCENARIO_TIER.
const (
	tierFast = "fast"
	tierFull = "full"
	tierAll  = "all"
)

// tier reports which cost tier this run is in. "full" is the default: it covers
// every behaviour except the multi-gigabyte compositions, which are slow enough
// that they would otherwise dominate a routine run.
func tier() string {
	switch v := strings.ToLower(os.Getenv("SCENARIO_TIER")); v {
	case tierFast, tierFull, tierAll:
		return v
	default:
		return tierFull
	}
}

// composeLarge builds a large object server-side from one seeded chunk.
//
// It exists because the object cannot be uploaded: at the measured ~2 MB/s a
// 6 GiB fixture would take an hour. Uploading one 64 MiB chunk and copying it
// into itself with UploadPartCopy takes ~15s.
func composeLarge(t *testing.T, client *s3.Client, bucket, key string, size int64, mutate func(*s3.CreateMultipartUploadInput)) {
	t.Helper()
	ctx := context.Background()

	// One seeded chunk, uploaded once per bucket and reused by every large
	// object composed into it.
	if _, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(seedChunkKey),
	}); err != nil {
		_, err = client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        aws.String(bucket),
			Key:           aws.String(seedChunkKey),
			Body:          newSynthReader(seedChunkKey, chunkSize),
			ContentLength: aws.Int64(chunkSize),
			ContentType:   aws.String("application/octet-stream"),
		})
		if err != nil {
			t.Fatalf("upload seed chunk: %v", err)
		}
	}

	create := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	if mutate != nil {
		mutate(create)
	}
	up, err := client.CreateMultipartUpload(ctx, create)
	if err != nil {
		t.Fatalf("create multipart upload for %q: %v", key, err)
	}
	// If the composition fails halfway, the open upload must not survive: this
	// endpoint has no AbortIncompleteMultipartUpload lifecycle rule, so orphaned
	// parts bill forever.
	completed := false
	t.Cleanup(func() {
		if completed {
			return
		}
		_, _ = client.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{
			Bucket: aws.String(bucket), Key: aws.String(key), UploadId: up.UploadId,
		})
	})

	// Parts are 64 MiB so a 6 GiB object is 96 parts — far under the 10,000
	// limit — and every range is closed: this endpoint rejects "bytes=a-" with
	// InvalidRange, so an open-ended range is not an option.
	numParts := int32((size + chunkSize - 1) / chunkSize)
	parts := make([]types.CompletedPart, 0, numParts)
	for i := int32(0); i < numParts; i++ {
		start := int64(i) * chunkSize
		length := int64(chunkSize)
		if start+length > size {
			length = size - start
		}
		// Every part reads from offset 0 of the same chunk, so the composed
		// object is the chunk repeated — which is what lets assertRangeMatches
		// predict any window without downloading it.
		rng := fmt.Sprintf("bytes=%d-%d", 0, length-1)
		var out *s3.UploadPartCopyOutput
		err := retryScenario(ctx, "UploadPartCopy", func(c context.Context) error {
			var err error
			out, err = client.UploadPartCopy(c, &s3.UploadPartCopyInput{
				Bucket:          aws.String(bucket),
				Key:             aws.String(key),
				UploadId:        up.UploadId,
				PartNumber:      aws.Int32(i + 1),
				CopySource:      aws.String(copySourceValue(bucket, seedChunkKey)),
				CopySourceRange: aws.String(rng),
			})
			return err
		})
		if err != nil {
			t.Fatalf("upload part %d of %q: %v", i+1, key, err)
		}
		parts = append(parts, types.CompletedPart{
			ETag: out.CopyPartResult.ETag, PartNumber: aws.Int32(i + 1),
		})
	}

	if _, err := client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: up.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: parts,
		},
	}); err != nil {
		t.Fatalf("complete multipart upload for %q: %v", key, err)
	}
	completed = true
}

// retryScenario retries an idempotent request a few times. Composition makes
// thousands of calls, so a single transient 5xx must not fail the fixture.
func retryScenario(ctx context.Context, label string, fn func(context.Context) error) error {
	var err error
	for attempt := 0; attempt < 4; attempt++ {
		if err = fn(ctx); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt+1) * 500 * time.Millisecond):
		}
	}
	return fmt.Errorf("%s failed after 4 attempts: %w", label, err)
}

// copySourceValue escapes a CopySource the way the production worker must: the
// endpoint decodes a raw value, so a key containing % would resolve to a
// different key and answer 404. Path separators stay literal.
func copySourceValue(bucket, key string) string {
	u := &url.URL{Path: bucket + "/" + key}
	return strings.ReplaceAll(u.EscapedPath(), "%2F", "/")
}

// composeLarge is the env-aware wrapper: it records which chunk a composed key
// was built from, so assertRangeMatches can predict the object's bytes.
func (e *scenarioEnv) composeLarge(key string, size int64, mutate func(*s3.CreateMultipartUploadInput)) {
	e.T.Helper()
	composeLarge(e.T, e.Client, e.Source, key, size, mutate)
	e.largeSeed[key] = seedChunkKey
	e.composedSize[key] = size
	// The chunk must not be synced: it is scaffolding, not a fixture.
	if _, err := e.Client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String(e.Source), Key: aws.String(seedChunkKey),
	}); err != nil {
		e.T.Fatalf("delete seed chunk: %v", err)
	}
}

// assertRangeMatches proves a target object's bytes without downloading it. For
// a composed object it compares against the chunk it was built from; for an
// ordinary object against synthBytesAt of the key itself.
func (e *scenarioEnv) assertRangeMatches(key string, offsets ...int64) {
	e.T.Helper()
	ctx := context.Background()

	head, err := e.Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(e.Target), Key: aws.String(key),
	})
	if err != nil {
		e.T.Fatalf("head target %q: %v", key, err)
	}
	size := aws.ToInt64(head.ContentLength)

	// The target's length must match what was composed. Without this a truncated
	// object passes every offset assertion inside the truncation, because the
	// expected bytes are a function of the offset alone.
	if composed := e.largeSeed[key]; composed != "" {
		if want := e.composedSize[key]; size != want {
			e.T.Fatalf("target %q is %d bytes, want %d", key, size, want)
		}
	}

	seedKey, composed := e.largeSeed[key]
	if !composed {
		seedKey = key
	}

	for _, off := range offsets {
		if off < 0 || off >= size {
			e.T.Fatalf("offset %d is outside %q (size %d)", off, key, size)
		}
		n := int64(65536)
		if off+n > size {
			n = size - off
		}
		out, err := e.Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(e.Target), Key: aws.String(key),
			Range: aws.String(fmt.Sprintf("bytes=%d-%d", off, off+n-1)),
		})
		if err != nil {
			e.T.Fatalf("range read %q at %d: %v", key, off, err)
		}
		got := readAll(e.T, out.Body)
		_ = out.Body.Close()
		if int64(len(got)) != n {
			e.T.Fatalf("range read %q at %d returned %d bytes, want %d", key, off, len(got), n)
		}
		want := e.expectedBytes(seedKey, off, n, composed)
		if string(got) != string(want) {
			e.T.Fatalf("target %q differs from the source at offset %d (%d bytes)", key, off, n)
		}
	}
}

// expectedBytes predicts the bytes at an absolute offset of a composed or
// ordinary object.
func (e *scenarioEnv) expectedBytes(seedKey string, off, n int64, composed bool) []byte {
	if !composed {
		return synthBytesAt(seedKey, off, n)
	}
	// A composed object is the chunk repeated, so its bytes wrap at chunkSize.
	// A window may straddle the wrap point, so it is assembled in pieces.
	out := make([]byte, 0, n)
	for int64(len(out)) < n {
		abs := off + int64(len(out))
		inChunk := abs % chunkSize
		take := chunkSize - inChunk
		if take > n-int64(len(out)) {
			take = n - int64(len(out))
		}
		out = append(out, synthBytesAt(seedKey, inChunk, take)...)
	}
	return out
}

// startSyncInBackground runs one sync on a goroutine so a test can cancel it
// while a copy is in flight.
func (e *scenarioEnv) startSyncInBackground(ctx context.Context) {
	e.T.Helper()
	e.bgDone = make(chan struct{})
	e.bgErr = nil
	go func() {
		defer close(e.bgDone)
		e.bgErr = e.newEngine().RunOnce(ctx)
	}()
}

// waitForSync blocks until the background sync returns and yields its error. A
// cancellation is an expected error here, so the caller decides what to assert.
func (e *scenarioEnv) waitForSync() error {
	e.T.Helper()
	if e.bgDone == nil {
		e.T.Fatal("waitForSync called without startSyncInBackground")
	}
	<-e.bgDone
	return e.bgErr
}
