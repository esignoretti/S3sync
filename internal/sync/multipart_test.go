package sync

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestMultipartPartSize(t *testing.T) {
	const (
		mib = int64(1) << 20
		gib = int64(1) << 30
	)
	cases := []struct {
		size     int64
		wantMin  int64
		maxParts int64
	}{
		{gib + 1, 5 * mib, 10000},
		{100 * gib, 5 * mib, 10000},
		{5 << 40, 5 * mib, 10000},
	}
	for _, c := range cases {
		ps := multipartPartSize(c.size)
		if ps < c.wantMin {
			t.Errorf("size %d: part size %d is below the 5 MiB minimum", c.size, ps)
		}
		if ps > 5*gib {
			t.Errorf("size %d: part size %d exceeds the 5 GiB maximum", c.size, ps)
		}
		if ps%mib != 0 {
			t.Errorf("size %d: part size %d is not a whole MiB", c.size, ps)
		}
		if parts := (c.size + ps - 1) / ps; parts > c.maxParts {
			t.Errorf("size %d: %d parts exceeds the 10,000 limit", c.size, parts)
		}
	}
}

// The part count must stay inside the endpoint's 10,000-part ceiling at the hard
// object limit, which is the case the naive "one part per GiB" choice gets wrong.
func TestMultipartPartSizeAtObjectLimit(t *testing.T) {
	ps := multipartPartSize(maxMultipartObjectSize)
	parts := (maxMultipartObjectSize + ps - 1) / ps
	if parts > maxMultipartParts {
		t.Fatalf("%d parts at the %d-byte limit, ceiling is %d", parts, int64(maxMultipartObjectSize), maxMultipartParts)
	}
}

func TestCopyObjectMultipartAboveSingleCopyLimit(t *testing.T) {
	api := &fakeCopyAPI{headLen: maxCopyObjectSize + 1}
	wp := newTestWorkerPool(api)

	a := SyncAction{Type: ActionCopy, Key: "big.bin", ETag: `"e1"`}
	if _, err := wp.copyObject(context.Background(), a); err != nil {
		t.Fatalf("copyObject: %v", err)
	}
	if api.createdMPU != 1 {
		t.Fatalf("CreateMultipartUpload calls = %d, want 1", api.createdMPU)
	}
	if api.copied != 0 {
		t.Fatalf("single CopyObject was issued for a %d-byte object", api.headLen)
	}
	for i, r := range api.partRanges {
		// The endpoint rejects an open-ended range with InvalidRange, so a part
		// without an explicit end offset fails the whole copy.
		if !strings.Contains(r, "-") || strings.HasSuffix(r, "-") {
			t.Fatalf("part %d range %q has no explicit end offset", i+1, r)
		}
	}
	if got := api.partsTotal(); got != api.headLen {
		t.Fatalf("parts cover %d bytes, want %d", got, api.headLen)
	}
	if api.completedParts != len(api.partRanges) {
		t.Fatalf("completion carried %d parts, want %d", api.completedParts, len(api.partRanges))
	}
	if api.aborted != 0 {
		t.Fatalf("AbortMultipartUpload was called after a successful copy")
	}
}

// The storage class must reach the multipart path even when attribute
// replication is off, which is why it is a parameter and not an ObjectAttrs
// field: with replication off attrs is the zero value.
func TestCopyObjectMultipartAppliesStorageClassWithoutAttrs(t *testing.T) {
	api := &fakeCopyAPI{headLen: maxCopyObjectSize + 1}
	wp := newTestWorkerPoolWithoutAttrs(api)
	wp.storageClass = "STANDARD_IA"

	if _, err := wp.copyObject(context.Background(), SyncAction{Type: ActionCopy, Key: "big.bin"}); err != nil {
		t.Fatal(err)
	}
	if got := api.mpuStorageClass; got != "STANDARD_IA" {
		t.Fatalf("CreateMultipartUpload storage class = %q, want STANDARD_IA", got)
	}
}

func TestCopyObjectMultipartLockRejectionIsExplained(t *testing.T) {
	// A locked source object always carries a retain-until date, and carriesLock
	// requires a LIVE retention: without the date the request would carry no lock
	// field, so the rejection could not be about lock.
	until := time.Now().UTC().Add(24 * time.Hour)
	api := &fakeCopyAPI{
		headLen: maxCopyObjectSize + 1,
		headOut: &s3.HeadObjectOutput{
			ContentLength:             aws.Int64(maxCopyObjectSize + 1),
			ObjectLockMode:            "GOVERNANCE",
			ObjectLockRetainUntilDate: &until,
		},
		mpuErr: errors.New("Bad Request"),
	}
	wp := newTestWorkerPool(api)

	_, err := wp.copyObject(context.Background(), SyncAction{Type: ActionCopy, Key: "big.bin"})
	if err == nil {
		t.Fatal("want an error")
	}
	if !strings.Contains(err.Error(), "object lock") || !strings.Contains(err.Error(), testTargetBucket) {
		t.Fatalf("err = %v, want it to name object lock and the target bucket", err)
	}
}

func TestCopyObjectMultipartAbortsOnPartFailure(t *testing.T) {
	api := &fakeCopyAPI{headLen: maxCopyObjectSize + 1, partErrAt: 2}
	wp := newTestWorkerPool(api)

	if _, err := wp.copyObject(context.Background(), SyncAction{Type: ActionCopy, Key: "big.bin"}); err == nil {
		t.Fatal("want an error")
	}
	if api.aborted == 0 {
		t.Fatal("AbortMultipartUpload was never called")
	}
	if api.abortUploadID != api.createdUploadID {
		t.Fatalf("aborted upload %q, want %q", api.abortUploadID, api.createdUploadID)
	}
}

// This endpoint has no AbortIncompleteMultipartUpload lifecycle rule, so an
// orphaned upload bills forever. The abort must therefore not inherit the
// cancelled context: it would be cancelled before it left the process.
func TestCopyObjectMultipartAbortsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	api := &fakeCopyAPI{headLen: maxCopyObjectSize + 1, cancelAfterPart: 1, cancelFunc: cancel}
	wp := newTestWorkerPool(api)

	if _, err := wp.copyObject(ctx, SyncAction{Type: ActionCopy, Key: "big.bin"}); err == nil {
		t.Fatal("want an error")
	}
	if api.aborted == 0 {
		t.Fatal("AbortMultipartUpload was not called after cancellation")
	}
	if !api.abortContextLive {
		t.Fatal("the abort used the cancelled context and would never reach the server")
	}
}

func TestCopyObjectOverFiveTiBStillFails(t *testing.T) {
	api := &fakeCopyAPI{headLen: maxMultipartObjectSize + 1}
	wp := newTestWorkerPool(api)

	_, err := wp.copyObject(context.Background(), SyncAction{Type: ActionCopy, Key: "huge.bin"})
	if !errors.Is(err, ErrObjectTooLarge) {
		t.Fatalf("err = %v, want ErrObjectTooLarge", err)
	}
	if api.createdMPU != 0 {
		t.Fatal("a multipart upload was started for an object past the hard limit")
	}
}

// A multipart copy carries the same characteristics as a single one, so the
// fingerprint must be identical or the sweep re-emits the object every pass.
func TestCopyObjectMultipartRecordsSameFingerprint(t *testing.T) {
	ct := "text/plain"
	api := &fakeCopyAPI{
		headLen: maxCopyObjectSize + 1,
		headOut: &s3.HeadObjectOutput{
			ContentLength: aws.Int64(maxCopyObjectSize + 1),
			ContentType:   &ct,
			Metadata:      map[string]string{"origin": "prod"},
		},
	}
	wp := newTestWorkerPool(api)

	got, err := wp.copyObject(context.Background(), SyncAction{Type: ActionCopy, Key: "big.bin"})
	if err != nil {
		t.Fatal(err)
	}
	attrs := AttrsFromHead(api.headOut)
	want := attrs.WithEffectiveStorageClass(wp.storageClass).withTags(nil).Hash()
	if got.AttrsHash != want {
		t.Fatalf("AttrsHash = %q, want %q", got.AttrsHash, want)
	}
	if api.mpuContentType != "text/plain" {
		t.Fatalf("multipart content type = %q, want text/plain", api.mpuContentType)
	}
	if api.mpuMetadata["origin"] != "prod" {
		t.Fatalf("multipart metadata = %v, want origin=prod", api.mpuMetadata)
	}
}

func TestMultipartPartRangesCoverObjectExactly(t *testing.T) {
	for _, size := range []int64{maxCopyObjectSize + 1, 6 << 30, 12 << 30} {
		ps := multipartPartSize(size)
		var covered int64
		parts := 0
		for start := int64(0); start < size; start += ps {
			end := start + ps - 1
			if end >= size {
				end = size - 1
			}
			covered += end - start + 1
			parts++
		}
		if covered != size {
			t.Errorf("size %d: parts cover %d bytes", size, covered)
		}
		if parts > maxMultipartParts {
			t.Errorf("size %d: %d parts", size, parts)
		}
	}
}

// The tag write happens AFTER the bytes move. On a multi-GiB multipart copy that
// is minutes later, so a deadline taken once for the whole object would already
// be spent: the tag write would fail with DeadlineExceeded and the object would
// be marked attrs_pending, which is silent — the bytes are right, so nothing
// retries the copy. Each request needs its own bounded context.
func TestCopyObjectMultipartTagWriteHasItsOwnDeadline(t *testing.T) {
	api := &fakeCopyAPI{
		headLen: maxCopyObjectSize + 1,
		headOut: &s3.HeadObjectOutput{ContentLength: aws.Int64(maxCopyObjectSize + 1)},
		tags:    []s3types.Tag{{Key: aws.String("team"), Value: aws.String("alpha")}},
	}
	// Shrink the per-request deadline and burn more than it inside the copy, so a
	// single deadline taken once for the whole object is provably spent by the
	// time the tag write runs. With the real 30s it would take a real 30s copy to
	// show the bug, and the test would pass either way.
	restore := copyRequestTimeout
	copyRequestTimeout = 200 * time.Millisecond
	t.Cleanup(func() { copyRequestTimeout = restore })
	api.completeHook = func() { time.Sleep(400 * time.Millisecond) }

	wp := newTestWorkerPool(api)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	action, err := wp.copyObject(ctx, SyncAction{Type: ActionCopy, Key: "big", ETag: `"e"`})
	if err != nil {
		t.Fatalf("copyObject returned %v", err)
	}
	if action.PendingAttrs {
		t.Fatalf("attributes were left pending after a multipart copy: the tag write ran on a spent deadline")
	}
	if api.putTags != 1 {
		t.Fatalf("PutObjectTagging calls = %d, want 1", api.putTags)
	}
	if action.AttrsHash == "" {
		t.Fatal("no fingerprint recorded after a multipart copy, so the sweep would re-copy forever")
	}
}
