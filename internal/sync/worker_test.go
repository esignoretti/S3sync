package sync

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestWorkerPoolCreation(t *testing.T) {
	pool := NewWorkerPool(5, nil, nil, "src", "tgt", NewThrottler(0), "", nil, false)
	if pool == nil {
		t.Fatal("expected non-nil pool")
	}
}

// fakeCopyAPI stands in for both clients of a WorkerPool.
type fakeCopyAPI struct {
	copied       int
	headCalls    int
	headErr      error
	headLen      int64
	headBucket   string
	headOut      *s3.HeadObjectOutput
	copyIn       *s3.CopyObjectInput
	copyOut      *s3.CopyObjectOutput
	copyErr      error
	deleteBucket string

	tags       []s3types.Tag
	getTagsErr error
	putTagsErr error
	getTags    int
	putTags    int
	lastTags   []s3types.Tag
	putTagsIn  *s3.PutObjectTaggingInput
	getTagsIn  *s3.GetObjectTaggingInput

	// Multipart state. partRanges records the CopySourceRange of each part so a
	// test can prove every range carries an explicit end offset.
	createdMPU       int
	createdUploadID  string
	mpuErr           error
	mpuStorageClass  string
	mpuContentType   string
	mpuMetadata      map[string]string
	partRanges       []string
	partErrAt        int
	partFailuresLeft int
	cancelAfterPart  int
	cancelFunc       context.CancelFunc
	completedParts   int
	completeHook     func()
	aborted          int
	abortUploadID    string
	abortContextLive bool
}

// partsTotal sums the byte counts of the ranges the fake was asked to copy.
func (f *fakeCopyAPI) partsTotal() int64 {
	var total int64
	for _, r := range f.partRanges {
		var start, end int64
		if _, err := fmt.Sscanf(r, "bytes=%d-%d", &start, &end); err != nil {
			continue
		}
		total += end - start + 1
	}
	return total
}

func (f *fakeCopyAPI) CreateMultipartUpload(_ context.Context, in *s3.CreateMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	f.createdMPU++
	f.createdUploadID = "upload-1"
	if in.StorageClass != "" {
		f.mpuStorageClass = string(in.StorageClass)
	}
	if in.ContentType != nil {
		f.mpuContentType = *in.ContentType
	}
	f.mpuMetadata = in.Metadata
	if f.mpuErr != nil {
		return nil, f.mpuErr
	}
	return &s3.CreateMultipartUploadOutput{UploadId: aws.String(f.createdUploadID)}, nil
}

func (f *fakeCopyAPI) UploadPartCopy(ctx context.Context, in *s3.UploadPartCopyInput, _ ...func(*s3.Options)) (*s3.UploadPartCopyOutput, error) {
	if in.CopySourceRange != nil {
		f.partRanges = append(f.partRanges, *in.CopySourceRange)
	}
	n := len(f.partRanges)
	// Failures key on the PART NUMBER, not the attempt count: a retry re-sends
	// the same part, and keying on attempts would make the retry succeed by
	// accident and prove nothing.
	// partFailuresLeft: 0 = never fail, -1 = always fail, n>0 = fail n times.
	partNo := int(aws.ToInt32(in.PartNumber))
	if f.partErrAt != 0 && partNo == f.partErrAt && f.partFailuresLeft != 0 {
		if f.partFailuresLeft > 0 {
			f.partFailuresLeft--
		}
		return nil, errors.New("part copy failed")
	}
	if f.cancelAfterPart != 0 && n >= f.cancelAfterPart {
		// Simulates the run budget expiring mid-upload: the test's cancel func
		// kills the parent context, which is what copyObject hands the
		// multipart path.
		if f.cancelFunc == nil {
			panic("fakeCopyAPI.cancelAfterPart set without cancelFunc")
		}
		f.cancelFunc()
		return nil, ctx.Err()
	}
	return &s3.UploadPartCopyOutput{
		CopyPartResult: &s3types.CopyPartResult{ETag: aws.String(fmt.Sprintf(`"part%d"`, n))},
	}, nil
}

func (f *fakeCopyAPI) CompleteMultipartUpload(_ context.Context, in *s3.CompleteMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	if in.MultipartUpload != nil {
		f.completedParts = len(in.MultipartUpload.Parts)
	}
	if f.completeHook != nil {
		f.completeHook()
	}
	return &s3.CompleteMultipartUploadOutput{}, nil
}

func (f *fakeCopyAPI) AbortMultipartUpload(ctx context.Context, in *s3.AbortMultipartUploadInput, _ ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	f.aborted++
	f.abortUploadID = aws.ToString(in.UploadId)
	f.abortContextLive = ctx.Err() == nil
	return &s3.AbortMultipartUploadOutput{}, nil
}

// testSourceBucket/testTargetBucket are the bucket names newTestWorkerPool uses,
// so the fake can answer source and target HEADs differently the way a real
// endpoint does.
const (
	testSourceBucket = "src"
	testTargetBucket = "tgt"
)

func (f *fakeCopyAPI) HeadObject(_ context.Context, in *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	f.headCalls++
	f.headBucket = aws.ToString(in.Bucket)
	if f.headBucket == testTargetBucket {
		// A brand-new key is absent from the target. This is the answer the P0
		// misread as a retryable failure.
		return nil, &s3types.NotFound{}
	}
	if f.headErr != nil {
		return nil, f.headErr
	}
	if f.headOut != nil {
		return f.headOut, nil
	}
	return &s3.HeadObjectOutput{ContentLength: aws.Int64(f.headLen)}, nil
}

func (f *fakeCopyAPI) CopyObject(_ context.Context, in *s3.CopyObjectInput, _ ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
	f.copied++
	f.copyIn = in
	if f.copyErr != nil {
		return nil, f.copyErr
	}
	if f.copyOut != nil {
		return f.copyOut, nil
	}
	return &s3.CopyObjectOutput{}, nil
}

func (f *fakeCopyAPI) GetObjectTagging(ctx context.Context, in *s3.GetObjectTaggingInput, _ ...func(*s3.Options)) (*s3.GetObjectTaggingOutput, error) {
	// A real client fails immediately on a spent context; a fake that ignores it
	// cannot reproduce the bug that a shared deadline causes.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.getTags++
	f.getTagsIn = in
	if f.getTagsErr != nil {
		return nil, f.getTagsErr
	}
	return &s3.GetObjectTaggingOutput{TagSet: f.tags}, nil
}

func (f *fakeCopyAPI) PutObjectTagging(ctx context.Context, in *s3.PutObjectTaggingInput, _ ...func(*s3.Options)) (*s3.PutObjectTaggingOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.putTags++
	f.putTagsIn = in
	if in.Tagging != nil {
		f.lastTags = in.Tagging.TagSet
	}
	if f.putTagsErr != nil {
		return nil, f.putTagsErr
	}
	return &s3.PutObjectTaggingOutput{}, nil
}

func (f *fakeCopyAPI) DeleteObject(_ context.Context, in *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	f.deleteBucket = aws.ToString(in.Bucket)
	return &s3.DeleteObjectOutput{}, nil
}

// The real client must satisfy copyAPI with no adapter type.
var _ copyAPI = (*s3.Client)(nil)

// newTestWorkerPool builds a pool with attribute replication on, which is what
// the tag and fingerprint tests assert. Tests that need the legacy behaviour use
// newTestWorkerPoolWithoutAttrs.
func newTestWorkerPool(api copyAPI) *WorkerPool {
	return &WorkerPool{
		workers: 1, src: api, tgt: api,
		sourceBucket: testSourceBucket, targetBucket: testTargetBucket,
		throttler: NewThrottler(0), replicateAttrs: true,
	}
}

func newTestWorkerPoolWithoutAttrs(api copyAPI) *WorkerPool {
	return &WorkerPool{
		workers: 1, src: api, tgt: api,
		sourceBucket: testSourceBucket, targetBucket: testTargetBucket,
		throttler: NewThrottler(0),
	}
}

// TestWorkerPoolRun_CopiesNewObjectAndReportsSuccess is the regression test for
// the P0 from 6d13e9d: the worker HEADed the *target*, and the 404 that a
// brand-new key always produces there was retried and returned as a failure, so
// CopyObject was never issued and a fresh sync copied nothing.
func TestWorkerPoolRun_CopiesNewObjectAndReportsSuccess(t *testing.T) {
	api := &fakeCopyAPI{headLen: 10}
	wp := newTestWorkerPool(api)
	actions := make(chan SyncAction, 1)
	actions <- SyncAction{Type: ActionCopy, Key: "new-key", ETag: `"e1"`}
	close(actions)

	succeeded, copied, failed := wp.Run(context.Background(), actions)
	if failed != 0 || copied != 1 || len(succeeded) != 1 {
		t.Fatalf("Run: copied %d, failed %d; want 1 copied and 0 failed", copied, failed)
	}
	if api.copied != 1 {
		t.Fatalf("CopyObject called %d times, want exactly 1", api.copied)
	}
}

func TestCopyObject_HeadsSourceAndCopiesFromIt(t *testing.T) {
	api := &fakeCopyAPI{headLen: 10}
	wp := newTestWorkerPool(api)
	if _, err := wp.copyObject(context.Background(), SyncAction{Type: ActionCopy, Key: "k", ETag: `"e1"`}); err != nil {
		t.Fatal(err)
	}
	if api.headBucket != testSourceBucket {
		t.Fatalf("HEADed bucket %q, want the source bucket", api.headBucket)
	}
	if got, want := aws.ToString(api.copyIn.CopySource), testSourceBucket+"/k"; got != want {
		t.Fatalf("CopySource = %q, want %q", got, want)
	}
	if got := aws.ToString(api.copyIn.Bucket); got != testTargetBucket {
		t.Fatalf("copy target = %q, want %q", got, testTargetBucket)
	}
}

// A source object that vanished between listing and HEAD is absent, not a
// failure, and must not burn the retry budget.
func TestCopyObject_SourceVanishedIsSkipped(t *testing.T) {
	api := &fakeCopyAPI{headErr: &s3types.NotFound{}}
	wp := newTestWorkerPool(api)
	_, err := wp.copyObject(context.Background(), SyncAction{Type: ActionCopy, Key: "gone", ETag: `"e1"`})
	if !errors.Is(err, errObjectVanished) {
		t.Fatalf("copyObject returned %v; a vanished source must report errObjectVanished", err)
	}
	if api.copied != 0 {
		t.Fatalf("CopyObject called %d times, want 0", api.copied)
	}
	if api.headCalls != 1 {
		t.Fatalf("HeadObject called %d times, want 1 (a 404 must not be retried)", api.headCalls)
	}
}

// A vanished object was never copied, so it must not be reported as synced and
// must not reach the cache: caching it would make StreamingDiff skip the key
// forever if the object came back with an unchanged ETag and LastModified.
func TestWorkerPoolRun_VanishedObjectIsNeitherSucceededNorFailed(t *testing.T) {
	api := &fakeCopyAPI{headErr: &s3types.NotFound{}}
	wp := newTestWorkerPool(api)
	actions := make(chan SyncAction, 1)
	actions <- SyncAction{Type: ActionCopy, Key: "gone", ETag: `"e1"`}
	close(actions)

	succeeded, succeededCount, failed := wp.Run(context.Background(), actions)
	if succeededCount != 0 || len(succeeded) != 0 {
		t.Fatalf("succeeded = %d (%d actions), want 0: a skipped object was never copied", succeededCount, len(succeeded))
	}
	if failed != 0 {
		t.Fatalf("failed = %d, want 0: a vanished object is a skip, not a failure", failed)
	}
	if api.copied != 0 {
		t.Fatalf("CopyObject called %d times, want 0", api.copied)
	}
}

// A genuine HEAD failure is retried and surfaced, and no copy is attempted.
func TestCopyObject_SourceHeadErrorIsRetriedAndFails(t *testing.T) {
	boom := errors.New("connection reset")
	api := &fakeCopyAPI{headErr: boom}
	wp := newTestWorkerPool(api)
	_, err := wp.copyObject(context.Background(), SyncAction{Type: ActionCopy, Key: "k", ETag: `"e1"`})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want it to wrap the HEAD failure", err)
	}
	if errors.Is(err, errObjectVanished) {
		t.Fatalf("err = %v; a genuine HEAD failure must not look like a vanished object", err)
	}
	if api.headCalls != 3 {
		t.Fatalf("HeadObject called %d times, want 3 attempts", api.headCalls)
	}
	if api.copied != 0 {
		t.Fatalf("CopyObject called %d times, want 0", api.copied)
	}
}

// Pins the accounting split at the Run level: a too-large object is a failure,
// a vanished one is neither. "Too large" now means past the 5 TiB object limit,
// not past the 5 GiB single-copy limit: between them the copy goes multipart.
func TestWorkerPoolRun_TooLargeObjectIsFailed(t *testing.T) {
	api := &fakeCopyAPI{headLen: maxMultipartObjectSize + 1}
	wp := newTestWorkerPool(api)
	actions := make(chan SyncAction, 1)
	actions <- SyncAction{Type: ActionCopy, Key: "big", ETag: `"e"`}
	close(actions)

	succeeded, succeededCount, failed := wp.Run(context.Background(), actions)
	if failed != 1 {
		t.Fatalf("failed = %d, want 1", failed)
	}
	if succeededCount != 0 || len(succeeded) != 0 {
		t.Fatalf("succeeded = %d (%d actions), want 0", succeededCount, len(succeeded))
	}
}

// A genuine (non-NotFound) HEAD failure must reach the failed count.
func TestWorkerPoolRun_HeadErrorIsFailed(t *testing.T) {
	api := &fakeCopyAPI{headErr: errors.New("connection reset")}
	wp := newTestWorkerPool(api)
	actions := make(chan SyncAction, 1)
	actions <- SyncAction{Type: ActionCopy, Key: "k", ETag: `"e"`}
	close(actions)

	succeeded, succeededCount, failed := wp.Run(context.Background(), actions)
	if failed != 1 {
		t.Fatalf("failed = %d, want 1", failed)
	}
	if succeededCount != 0 || len(succeeded) != 0 {
		t.Fatalf("succeeded = %d (%d actions), want 0", succeededCount, len(succeeded))
	}
}

// Deletes always go to the target bucket, never the source.
func TestWorkerPoolRun_DeleteRoutesToTarget(t *testing.T) {
	api := &fakeCopyAPI{}
	wp := newTestWorkerPool(api)
	actions := make(chan SyncAction, 1)
	actions <- SyncAction{Type: ActionDelete, Key: "old"}
	close(actions)

	_, succeededCount, failed := wp.Run(context.Background(), actions)
	if failed != 0 || succeededCount != 1 {
		t.Fatalf("Run: succeeded %d, failed %d; want 1 succeeded and 0 failed", succeededCount, failed)
	}
	if api.deleteBucket != testTargetBucket {
		t.Fatalf("DeleteObject called on bucket %q, want %q", api.deleteBucket, testTargetBucket)
	}
}

func TestCopyObject_TooLarge(t *testing.T) {
	api := &fakeCopyAPI{headLen: maxMultipartObjectSize + 1}
	wp := newTestWorkerPool(api)
	_, err := wp.copyObject(context.Background(), SyncAction{Type: ActionCopy, Key: "big", ETag: `"e"`})
	if !errors.Is(err, ErrObjectTooLarge) {
		t.Fatalf("err = %v, want ErrObjectTooLarge", err)
	}
	if api.copied != 0 {
		t.Fatalf("CopyObject called %d times, want 0", api.copied)
	}
	// The user must be able to see why their object was rejected.
	for _, want := range []string{"5497558138881", "5497558138880"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("err = %q, want it to name %s", err, want)
		}
	}
	if api.createdMPU != 0 {
		t.Fatalf("a multipart upload was started for an object past the hard limit")
	}
}

// 6 GiB is above the single-copy limit and below the object limit, so it must
// move as parts rather than fail.
func TestCopyObject_JustAboveSingleCopyLimitGoesMultipart(t *testing.T) {
	api := &fakeCopyAPI{headLen: 6 << 30}
	wp := newTestWorkerPool(api)
	if _, err := wp.copyObject(context.Background(), SyncAction{Type: ActionCopy, Key: "big", ETag: `"e"`}); err != nil {
		t.Fatalf("copyObject returned %v; 6 GiB fits the multipart path", err)
	}
	if api.copied != 0 {
		t.Fatalf("a single CopyObject was issued for a 6 GiB object")
	}
	if api.createdMPU != 1 {
		t.Fatalf("CreateMultipartUpload calls = %d, want 1", api.createdMPU)
	}
}

// 5 GiB is the inclusive CopyObject limit, so it must still be copied.
func TestCopyObject_ExactlyFiveGiBIsCopied(t *testing.T) {
	api := &fakeCopyAPI{headLen: 5 << 30}
	wp := newTestWorkerPool(api)
	if _, err := wp.copyObject(context.Background(), SyncAction{Type: ActionCopy, Key: "edge", ETag: `"e"`}); err != nil {
		t.Fatalf("copyObject returned %v; 5 GiB is allowed", err)
	}
	if api.copied != 1 {
		t.Fatalf("CopyObject called %d times, want 1", api.copied)
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

// --- attribute pipeline -----------------------------------------------------

// The endpoint does not apply x-amz-tagging on CopyObject, so tags must travel
// through GetObjectTagging on the source and PutObjectTagging on the target, and
// the resulting fingerprint must cover them.
func TestCopyObject_AppliesTagsAndRecordsHash(t *testing.T) {
	head := &s3.HeadObjectOutput{
		ContentType: aws.String("text/csv"),
		Metadata:    map[string]string{"a": "b"},
	}
	api := &fakeCopyAPI{
		headOut: head,
		tags:    []s3types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
	}
	wp := newTestWorkerPool(api)

	got, err := wp.copyObject(context.Background(), SyncAction{Type: ActionCopy, Key: "k", ETag: `"e"`})
	if err != nil {
		t.Fatal(err)
	}
	if api.putTags != 1 {
		t.Fatalf("PutObjectTagging called %d times, want 1", api.putTags)
	}
	if len(api.lastTags) != 1 || aws.ToString(api.lastTags[0].Key) != "env" {
		t.Fatalf("tags not applied: %+v", api.lastTags)
	}
	if aws.ToString(api.getTagsIn.Bucket) != testSourceBucket {
		t.Fatalf("GetObjectTagging hit bucket %q, want the source bucket", aws.ToString(api.getTagsIn.Bucket))
	}
	if aws.ToString(api.putTagsIn.Bucket) != testTargetBucket {
		t.Fatalf("PutObjectTagging hit bucket %q, want the target bucket", aws.ToString(api.putTagsIn.Bucket))
	}
	if got.AttrsHash == "" || got.PendingAttrs {
		t.Fatalf("action = %+v, want hash set and pending false", got)
	}

	// The fingerprint must describe what was applied: source HEAD fields plus
	// the tags fetched separately. If tags were dropped the two would match.
	withTags := AttrsFromHead(head)
	withTags.Tags = api.tags
	if got.AttrsHash != withTags.Hash() {
		t.Fatalf("AttrsHash = %q, want %q", got.AttrsHash, withTags.Hash())
	}
	if got.AttrsHash == AttrsFromHead(head).Hash() {
		t.Fatal("AttrsHash ignored the tags; the tag component of the fingerprint was dropped")
	}
}

// Tags are applied to the version CopyObject created, not to whatever is
// current on the target.
func TestCopyObject_TagsTargetTheCopiedVersion(t *testing.T) {
	api := &fakeCopyAPI{
		headOut: &s3.HeadObjectOutput{},
		copyOut: &s3.CopyObjectOutput{VersionId: aws.String("v2")},
		tags:    []s3types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
	}
	wp := newTestWorkerPool(api)
	if _, err := wp.copyObject(context.Background(), SyncAction{Type: ActionCopy, Key: "k", ETag: `"e"`}); err != nil {
		t.Fatal(err)
	}
	if aws.ToString(api.putTagsIn.VersionId) != "v2" {
		t.Fatalf("VersionId = %q, want the version CopyObject returned", aws.ToString(api.putTagsIn.VersionId))
	}
}

// Bytes are correct even when the tag write fails, so the object must be
// reported as copied with attributes pending, not as a failed copy.
func TestCopyObject_TagFailureMarksPending(t *testing.T) {
	api := &fakeCopyAPI{headOut: &s3.HeadObjectOutput{}, putTagsErr: errors.New("nope")}
	wp := newTestWorkerPool(api)
	got, err := wp.copyObject(context.Background(), SyncAction{Type: ActionCopy, Key: "k", ETag: `"e"`})
	if err != nil {
		t.Fatalf("a tag failure must not fail the copy: %v", err)
	}
	if !got.PendingAttrs {
		t.Fatal("PendingAttrs must be true after a tag failure")
	}
	if api.copied != 1 {
		t.Fatalf("bytes were copied %d times, want 1", api.copied)
	}
	// The target's attribute state is partially applied, so the cache must not
	// claim a fingerprint it cannot vouch for.
	if got.AttrsHash != "" {
		t.Fatalf("AttrsHash = %q, want empty after a partial attribute application", got.AttrsHash)
	}
}

// A failed tag read is the same partial application as a failed tag write.
func TestCopyObject_TagReadFailureMarksPending(t *testing.T) {
	api := &fakeCopyAPI{headOut: &s3.HeadObjectOutput{}, getTagsErr: errors.New("nope")}
	wp := newTestWorkerPool(api)
	got, err := wp.copyObject(context.Background(), SyncAction{Type: ActionCopy, Key: "k", ETag: `"e"`})
	if err != nil {
		t.Fatalf("a tag read failure must not fail the copy: %v", err)
	}
	if !got.PendingAttrs {
		t.Fatal("PendingAttrs must be true after a tag read failure")
	}
	if api.putTags != 0 {
		t.Fatalf("PutObjectTagging called %d times, want 0: there was nothing to write", api.putTags)
	}
	// Same partial application as a failed write: no fingerprint to cache.
	if got.AttrsHash != "" {
		t.Fatalf("AttrsHash = %q, want empty after a partial attribute application", got.AttrsHash)
	}
}

// Metadata and retention travel as directives on the copy request itself.
func TestCopyObject_AppliesMetadataAndLockToCopyInput(t *testing.T) {
	until := time.Now().UTC().Add(24 * time.Hour)
	api := &fakeCopyAPI{headOut: &s3.HeadObjectOutput{
		ContentType:               aws.String("image/png"),
		ContentEncoding:           aws.String("gzip"),
		Metadata:                  map[string]string{"owner": "ops"},
		StorageClass:              s3types.StorageClassStandardIa,
		ObjectLockMode:            s3types.ObjectLockModeCompliance,
		ObjectLockRetainUntilDate: &until,
		ObjectLockLegalHoldStatus: s3types.ObjectLockLegalHoldStatusOn,
	}}
	wp := newTestWorkerPool(api)
	if _, err := wp.copyObject(context.Background(), SyncAction{Type: ActionCopy, Key: "k", ETag: `"e"`}); err != nil {
		t.Fatal(err)
	}
	in := api.copyIn
	if in.MetadataDirective != s3types.MetadataDirectiveReplace {
		t.Fatalf("MetadataDirective = %v, want REPLACE: COPY is not reliable on this endpoint", in.MetadataDirective)
	}
	if aws.ToString(in.ContentType) != "image/png" || aws.ToString(in.ContentEncoding) != "gzip" {
		t.Fatalf("content headers not passed: %v / %v", aws.ToString(in.ContentType), aws.ToString(in.ContentEncoding))
	}
	if in.Metadata["owner"] != "ops" {
		t.Fatalf("metadata = %+v", in.Metadata)
	}
	if in.StorageClass != s3types.StorageClassStandardIa {
		t.Fatalf("StorageClass = %v, want the source class", in.StorageClass)
	}
	if in.ObjectLockMode != s3types.ObjectLockModeCompliance || in.ObjectLockRetainUntilDate == nil {
		t.Fatalf("lock fields not passed: %v / %v", in.ObjectLockMode, in.ObjectLockRetainUntilDate)
	}
	if in.ObjectLockLegalHoldStatus != s3types.ObjectLockLegalHoldStatusOn {
		t.Fatalf("legal hold = %v", in.ObjectLockLegalHoldStatus)
	}
}

// A pair that overrides the storage class keeps overriding it.
func TestCopyObject_StorageClassOverrideWins(t *testing.T) {
	api := &fakeCopyAPI{headOut: &s3.HeadObjectOutput{StorageClass: s3types.StorageClassStandardIa}}
	wp := newTestWorkerPool(api)
	wp.storageClass = "GLACIER"
	if _, err := wp.copyObject(context.Background(), SyncAction{Type: ActionCopy, Key: "k", ETag: `"e"`}); err != nil {
		t.Fatal(err)
	}
	if api.copyIn.StorageClass != s3types.StorageClassGlacier {
		t.Fatalf("StorageClass = %v, want the pair override", api.copyIn.StorageClass)
	}
}

// A source whose retention deadline has already passed must still be copied, with
// no lock fields on the request (a past date makes S3 reject the whole copy) and
// with a fingerprint that describes the applied state rather than an unknown one.
func TestCopyObject_ExpiredRetentionCopiesWithoutLock(t *testing.T) {
	past := time.Now().UTC().Add(-time.Hour)
	head := &s3.HeadObjectOutput{
		ContentType:               aws.String("text/csv"),
		ObjectLockMode:            s3types.ObjectLockModeGovernance,
		ObjectLockRetainUntilDate: &past,
	}
	api := &fakeCopyAPI{headOut: head}
	wp := newTestWorkerPool(api)

	got, err := wp.copyObject(context.Background(), SyncAction{Type: ActionCopy, Key: "k", ETag: `"e"`})
	if err != nil {
		t.Fatal(err)
	}
	if api.copied != 1 {
		t.Fatalf("CopyObject called %d times, want 1: an expired retention must not block the copy", api.copied)
	}
	in := api.copyIn
	if in.ObjectLockMode != "" || in.ObjectLockRetainUntilDate != nil {
		t.Fatalf("lock fields sent for an expired retention: %v / %v", in.ObjectLockMode, in.ObjectLockRetainUntilDate)
	}
	if aws.ToString(in.ContentType) != "text/csv" {
		t.Fatalf("ContentType = %q, want the source type", aws.ToString(in.ContentType))
	}
	if got.AttrsHash == "" || got.PendingAttrs {
		t.Fatalf("action = %+v, want hash set and pending false", got)
	}
	if want := AttrsFromHead(head).Hash(); got.AttrsHash != want {
		t.Fatalf("AttrsHash = %q, want %q (the past retention is omitted on both sides)", got.AttrsHash, want)
	}
}

// The backward-compatibility contract: with attribute replication off the copy
// request is exactly the legacy one and no attribute call is made.
func TestCopyObject_WithoutAttrReplicationTouchesNoAttributes(t *testing.T) {
	api := &fakeCopyAPI{headOut: &s3.HeadObjectOutput{
		ContentType:               aws.String("text/csv"),
		Metadata:                  map[string]string{"a": "b"},
		StorageClass:              s3types.StorageClassStandardIa,
		ObjectLockMode:            s3types.ObjectLockModeCompliance,
		ObjectLockRetainUntilDate: aws.Time(time.Now().UTC().Add(time.Hour)),
	}}
	wp := newTestWorkerPoolWithoutAttrs(api)

	got, err := wp.copyObject(context.Background(), SyncAction{Type: ActionCopy, Key: "k", ETag: `"e"`})
	if err != nil {
		t.Fatal(err)
	}
	if api.getTags != 0 || api.putTags != 0 {
		t.Fatalf("tag calls: %d get, %d put; want 0 with replicateAttrs off", api.getTags, api.putTags)
	}
	in := api.copyIn
	if in.MetadataDirective != "" || in.ContentType != nil || in.Metadata != nil ||
		in.ObjectLockMode != "" || in.ObjectLockRetainUntilDate != nil || in.ObjectLockLegalHoldStatus != "" {
		t.Fatalf("copy request carries attributes with replication off: %+v", in)
	}
	if in.StorageClass != "" {
		t.Fatalf("StorageClass = %v, want unset: the source class is not copied when replication is off", in.StorageClass)
	}
	// Empty means "unknown", never "matches".
	if got.AttrsHash != "" || got.PendingAttrs {
		t.Fatalf("action = %+v, want no hash and not pending", got)
	}
}

// Run must hand the enriched action to the cache writer.
func TestWorkerPoolRun_PropagatesAttrsHash(t *testing.T) {
	api := &fakeCopyAPI{headOut: &s3.HeadObjectOutput{ContentType: aws.String("text/csv")}}
	wp := newTestWorkerPool(api)
	actions := make(chan SyncAction, 1)
	actions <- SyncAction{Type: ActionCopy, Key: "k", ETag: `"e"`}
	close(actions)

	succeeded, _, failed := wp.Run(context.Background(), actions)
	if failed != 0 || len(succeeded) != 1 {
		t.Fatalf("Run: succeeded %d, failed %d", len(succeeded), failed)
	}
	if succeeded[0].AttrsHash == "" {
		t.Fatal("Run dropped the fingerprint; the cache entry would record an unknown attribute state")
	}
	if succeeded[0].Key != "k" || succeeded[0].ETag != `"e"` {
		t.Fatalf("Run lost the action's identity: %+v", succeeded[0])
	}
}

// The fingerprint must describe the target's effective class. With a pair
// override the source class is never applied, so hashing it would make the
// reconciler report drift the copy can never resolve.
func TestCopyObject_HashRecordsEffectiveStorageClass(t *testing.T) {
	api := &fakeCopyAPI{headOut: &s3.HeadObjectOutput{StorageClass: s3types.StorageClassStandardIa}}
	wp := newTestWorkerPool(api)
	wp.storageClass = "GLACIER"

	got, err := wp.copyObject(context.Background(), SyncAction{Type: ActionCopy, Key: "k", ETag: `"e"`})
	if err != nil {
		t.Fatal(err)
	}
	want := AttrsFromHead(api.headOut).WithEffectiveStorageClass(wp.storageClass)
	if got.AttrsHash != want.Hash() {
		t.Fatalf("AttrsHash = %q, want the effective-class hash %q", got.AttrsHash, want.Hash())
	}
	if got.AttrsHash == AttrsFromHead(api.headOut).Hash() {
		t.Fatal("AttrsHash recorded the source class, which the target never receives")
	}
}

// A source whose tags were all removed must clear the target's, so the empty set
// is written rather than skipped: the target's tags must equal the source's, not
// merely be a superset of whatever an earlier write left behind.
func TestCopyObject_TaglessSourceClearsTargetTags(t *testing.T) {
	api := &fakeCopyAPI{headOut: &s3.HeadObjectOutput{}}
	wp := newTestWorkerPool(api)

	got, err := wp.copyObject(context.Background(), SyncAction{Type: ActionCopy, Key: "k", ETag: `"e"`})
	if err != nil {
		t.Fatal(err)
	}
	if api.getTags != 1 {
		t.Fatalf("GetObjectTagging called %d times, want 1", api.getTags)
	}
	if api.putTags != 1 {
		t.Fatalf("PutObjectTagging called %d times, want 1: an empty set must clear the target's tags", api.putTags)
	}
	if len(api.lastTags) != 0 {
		t.Fatalf("TagSet = %+v, want empty", api.lastTags)
	}
	if got.PendingAttrs {
		t.Fatal("a tagless source is fully applied, not pending")
	}
	if got.AttrsHash == "" {
		t.Fatal("a tagless source still has a fingerprint")
	}
}

// --- attribute-only action ---------------------------------------------------

// The attribute-only action must never move bytes: the whole point of a separate
// action is that a retry of the characteristics costs no copy.
func TestApplyAttrs_NeverCopiesBytes(t *testing.T) {
	api := &fakeCopyAPI{
		headOut: &s3.HeadObjectOutput{ContentType: aws.String("text/csv")},
		tags:    []s3types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
	}
	wp := newTestWorkerPool(api)

	got, err := wp.applyAttrs(context.Background(), SyncAction{Type: ActionCopyAttributes, Key: "k", ETag: `"e"`})
	if err != nil {
		t.Fatal(err)
	}
	if api.copied != 0 {
		t.Fatalf("CopyObject called %d times, want 0", api.copied)
	}
	if api.headBucket != testSourceBucket {
		t.Fatalf("HEADed bucket %q, want the source bucket", api.headBucket)
	}
	if api.getTags != 1 || api.putTags != 1 {
		t.Fatalf("tag calls: %d get, %d put; want 1 and 1", api.getTags, api.putTags)
	}
	if got.AttrsHash == "" || got.PendingAttrs {
		t.Fatalf("action = %+v, want a fingerprint and pending false", got)
	}
	if got.Key != "k" || got.ETag != `"e"` {
		t.Fatalf("action lost its identity: %+v", got)
	}
}

// The retried tags belong to the target's current version, so no VersionId is
// sent: naming one would tag a version that is no longer current.
func TestApplyAttrs_TagsCurrentTargetVersion(t *testing.T) {
	api := &fakeCopyAPI{headOut: &s3.HeadObjectOutput{}}
	wp := newTestWorkerPool(api)

	if _, err := wp.applyAttrs(context.Background(), SyncAction{Type: ActionCopyAttributes, Key: "k"}); err != nil {
		t.Fatal(err)
	}
	if api.putTagsIn.VersionId != nil {
		t.Fatalf("VersionId = %q, want none: the current target version is the one to tag",
			aws.ToString(api.putTagsIn.VersionId))
	}
}

// The two paths must fingerprint the same source state identically, or a retried
// object never matches what the writer recorded and the sweep loops forever.
func TestApplyAttrs_HashMatchesCopyObject(t *testing.T) {
	until := time.Now().UTC().Add(time.Hour)
	newAPI := func() *fakeCopyAPI {
		return &fakeCopyAPI{
			headOut: &s3.HeadObjectOutput{
				ContentType:               aws.String("image/png"),
				Metadata:                  map[string]string{"owner": "ops"},
				StorageClass:              s3types.StorageClassStandardIa,
				ObjectLockMode:            s3types.ObjectLockModeGovernance,
				ObjectLockRetainUntilDate: &until,
			},
			tags: []s3types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
		}
	}

	copyPool := newTestWorkerPool(newAPI())
	copyPool.storageClass = "GLACIER"
	copied, err := copyPool.copyObject(context.Background(), SyncAction{Type: ActionCopy, Key: "k"})
	if err != nil {
		t.Fatal(err)
	}

	attrsPool := newTestWorkerPool(newAPI())
	attrsPool.storageClass = "GLACIER"
	retried, err := attrsPool.applyAttrs(context.Background(), SyncAction{Type: ActionCopyAttributes, Key: "k"})
	if err != nil {
		t.Fatal(err)
	}

	if retried.AttrsHash != copied.AttrsHash {
		t.Fatalf("applyAttrs hash = %q, copyObject hash = %q; the retry can never converge",
			retried.AttrsHash, copied.AttrsHash)
	}
}

// A tag failure leaves the characteristics partially applied, exactly as it does
// after a copy: the bytes are untouched and still correct.
func TestApplyAttrs_TagFailureMarksPending(t *testing.T) {
	api := &fakeCopyAPI{headOut: &s3.HeadObjectOutput{}, putTagsErr: errors.New("nope")}
	wp := newTestWorkerPool(api)

	got, err := wp.applyAttrs(context.Background(), SyncAction{Type: ActionCopyAttributes, Key: "k"})
	if err != nil {
		t.Fatalf("a tag failure must not fail the action: %v", err)
	}
	if !got.PendingAttrs {
		t.Fatal("PendingAttrs must be true after a tag failure")
	}
	if got.AttrsHash != "" {
		t.Fatalf("AttrsHash = %q, want empty after a partial application", got.AttrsHash)
	}
	if api.copied != 0 {
		t.Fatalf("CopyObject called %d times, want 0", api.copied)
	}
}

// The source can vanish between the sweep and the action; that is the same skip
// as a vanished copy source.
func TestApplyAttrs_VanishedSourceIsSkipped(t *testing.T) {
	api := &fakeCopyAPI{headErr: &s3types.NotFound{}}
	wp := newTestWorkerPool(api)

	_, err := wp.applyAttrs(context.Background(), SyncAction{Type: ActionCopyAttributes, Key: "gone"})
	if !errors.Is(err, errObjectVanished) {
		t.Fatalf("applyAttrs returned %v; a vanished source must report errObjectVanished", err)
	}
	if api.putTags != 0 {
		t.Fatalf("PutObjectTagging called %d times, want 0", api.putTags)
	}
}

// With replication off there is nothing to apply, and the empty fingerprint
// records "unknown".
func TestApplyAttrs_WithoutAttrReplicationDoesNothing(t *testing.T) {
	api := &fakeCopyAPI{headOut: &s3.HeadObjectOutput{}}
	wp := newTestWorkerPoolWithoutAttrs(api)

	got, err := wp.applyAttrs(context.Background(), SyncAction{Type: ActionCopyAttributes, Key: "k"})
	if err != nil {
		t.Fatal(err)
	}
	if api.headCalls != 0 || api.getTags != 0 || api.putTags != 0 {
		t.Fatalf("calls: %d head, %d get, %d put; want 0", api.headCalls, api.getTags, api.putTags)
	}
	if got.AttrsHash != "" || got.PendingAttrs {
		t.Fatalf("action = %+v, want no hash and not pending", got)
	}
}

// A pending action that arrives while replication is off must come back with
// PendingAttrs cleared. StreamingDiff keys on that flag alone and engine.go
// writes it straight back to the cache, so leaving it set re-emits
// ActionCopyAttributes for the key on every cycle, forever.
func TestApplyAttrs_WithoutAttrReplicationClearsPending(t *testing.T) {
	api := &fakeCopyAPI{headOut: &s3.HeadObjectOutput{}}
	wp := newTestWorkerPoolWithoutAttrs(api)

	got, err := wp.applyAttrs(context.Background(), SyncAction{
		Type: ActionCopyAttributes, Key: "k", PendingAttrs: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.PendingAttrs {
		t.Fatal("PendingAttrs is still set; the differ would re-emit this action every cycle")
	}
	if got.AttrsHash != "" {
		t.Fatalf("AttrsHash = %q, want empty: nothing was applied", got.AttrsHash)
	}
	if api.headCalls != 0 || api.getTags != 0 || api.putTags != 0 {
		t.Fatalf("calls: %d head, %d get, %d put; want 0", api.headCalls, api.getTags, api.putTags)
	}
}

// Run must route the new action type to applyAttrs, not to the copy path.
func TestWorkerPoolRun_DispatchesCopyAttributes(t *testing.T) {
	api := &fakeCopyAPI{headOut: &s3.HeadObjectOutput{ContentType: aws.String("text/csv")}}
	wp := newTestWorkerPool(api)
	actions := make(chan SyncAction, 1)
	actions <- SyncAction{Type: ActionCopyAttributes, Key: "k", ETag: `"e"`}
	close(actions)

	succeeded, _, failed := wp.Run(context.Background(), actions)
	if failed != 0 || len(succeeded) != 1 {
		t.Fatalf("Run: succeeded %d, failed %d", len(succeeded), failed)
	}
	if api.copied != 0 {
		t.Fatalf("CopyObject called %d times, want 0", api.copied)
	}
	if api.putTags != 1 {
		t.Fatalf("PutObjectTagging called %d times, want 1", api.putTags)
	}
	if succeeded[0].AttrsHash == "" {
		t.Fatal("Run dropped the fingerprint")
	}
}

// A locked source copied into a bucket created without object lock fails every
// object with a bare S3 rejection that names neither the cause nor the fix.
// Object lock can only be enabled at bucket creation, so the error must say so.
func TestCopyObject_LockRejectionNamesObjectLockAndTargetBucket(t *testing.T) {
	until := time.Now().UTC().Add(24 * time.Hour)
	api := &fakeCopyAPI{
		headOut: &s3.HeadObjectOutput{
			ObjectLockMode:            s3types.ObjectLockModeGovernance,
			ObjectLockRetainUntilDate: &until,
		},
		copyErr: errors.New("InvalidRequest: object lock is not enabled for this bucket"),
	}
	wp := newTestWorkerPool(api)
	_, err := wp.copyObject(context.Background(), SyncAction{Type: ActionCopy, Key: "locked", ETag: `"e"`})
	if err == nil {
		t.Fatal("expected the CopyObject rejection to surface")
	}
	msg := err.Error()
	if !strings.Contains(msg, "object lock") {
		t.Fatalf("error does not name object lock: %v", msg)
	}
	if !strings.Contains(msg, testTargetBucket) {
		t.Fatalf("error does not name the target bucket: %v", msg)
	}
	if !strings.Contains(msg, "locked") {
		t.Fatalf("error does not name the object: %v", msg)
	}
	if !strings.Contains(msg, "created with object lock") {
		t.Fatalf("error does not say lock must be set at bucket creation: %v", msg)
	}
	if !errors.Is(err, api.copyErr) {
		t.Fatalf("error does not wrap the original: %v", err)
	}
}

// A legal hold is lock state without a retention date, so it can be rejected the
// same way and must be explained the same way.
func TestCopyObject_LegalHoldRejectionIsExplained(t *testing.T) {
	api := &fakeCopyAPI{
		headOut: &s3.HeadObjectOutput{ObjectLockLegalHoldStatus: s3types.ObjectLockLegalHoldStatusOn},
		copyErr: errors.New("InvalidRequest"),
	}
	wp := newTestWorkerPool(api)
	_, err := wp.copyObject(context.Background(), SyncAction{Type: ActionCopy, Key: "k", ETag: `"e"`})
	if err == nil || !strings.Contains(err.Error(), "object lock") {
		t.Fatalf("legal-hold rejection not explained: %v", err)
	}
}

// An unlocked source has nothing to do with object lock, so its rejection must
// arrive unwrapped: adding the lock hint would send the user to the wrong cause.
func TestCopyObject_UnlockedRejectionIsNotWrapped(t *testing.T) {
	orig := errors.New("AccessDenied")
	api := &fakeCopyAPI{
		headOut: &s3.HeadObjectOutput{ContentType: aws.String("text/csv")},
		copyErr: orig,
	}
	wp := newTestWorkerPool(api)
	_, err := wp.copyObject(context.Background(), SyncAction{Type: ActionCopy, Key: "k", ETag: `"e"`})
	if !errors.Is(err, orig) {
		t.Fatalf("error does not wrap the original: %v", err)
	}
	if strings.Contains(err.Error(), "object lock") {
		t.Fatalf("unlocked source got the object-lock hint: %v", err)
	}
}

// An expired retention is dropped from the request, so it cannot be the cause of
// a rejection and must not attract the lock hint either.
func TestCopyObject_ExpiredRetentionRejectionIsNotWrapped(t *testing.T) {
	past := time.Now().UTC().Add(-time.Hour)
	orig := errors.New("AccessDenied")
	api := &fakeCopyAPI{
		headOut: &s3.HeadObjectOutput{
			ObjectLockMode:            s3types.ObjectLockModeGovernance,
			ObjectLockRetainUntilDate: &past,
		},
		copyErr: orig,
	}
	wp := newTestWorkerPool(api)
	_, err := wp.copyObject(context.Background(), SyncAction{Type: ActionCopy, Key: "k", ETag: `"e"`})
	if strings.Contains(err.Error(), "object lock") {
		t.Fatalf("expired retention got the lock hint for fields that were not sent: %v", err)
	}
}

// A pending action copied the bytes but not all of the characteristics. It is
// completed work, and the separate count is what makes the gap visible instead
// of hiding it inside "succeeded".
func TestWorkerPoolRun_CountsPendingAttrs(t *testing.T) {
	api := &fakeCopyAPI{
		headOut:    &s3.HeadObjectOutput{ContentType: aws.String("text/csv")},
		putTagsErr: errors.New("tag write refused"),
	}
	progress := &Progress{}
	wp := newTestWorkerPool(api)
	wp.progress = progress
	actions := make(chan SyncAction, 1)
	actions <- SyncAction{Type: ActionCopy, Key: "k", ETag: `"e"`}
	close(actions)

	succeeded, count, failed := wp.Run(context.Background(), actions)
	if failed != 0 || count != 1 || len(succeeded) != 1 {
		t.Fatalf("Run: succeeded %d, failed %d; a pending object still copied its bytes", count, failed)
	}
	if !succeeded[0].PendingAttrs {
		t.Fatal("the action lost its pending mark; the cache would record a complete attribute state")
	}
	if got := progress.PendingCount(); got != 1 {
		t.Fatalf("PendingCount = %d, want 1", got)
	}
	total, completed, failedCount := progress.Snapshot()
	if completed != 1 {
		t.Fatalf("Completed = %d, want 1: a pending action did complete", completed)
	}
	if failedCount != 0 {
		t.Fatalf("Failed = %d, want 0", failedCount)
	}
	if total != 0 {
		t.Fatalf("Total = %d, want 0: the engine sets it after the run", total)
	}
}

// A clean copy must not be counted as pending.
func TestWorkerPoolRun_CleanCopyIsNotPending(t *testing.T) {
	api := &fakeCopyAPI{headOut: &s3.HeadObjectOutput{ContentType: aws.String("text/csv")}}
	progress := &Progress{}
	wp := newTestWorkerPool(api)
	wp.progress = progress
	actions := make(chan SyncAction, 1)
	actions <- SyncAction{Type: ActionCopy, Key: "k", ETag: `"e"`}
	close(actions)

	if _, _, failed := wp.Run(context.Background(), actions); failed != 0 {
		t.Fatalf("failed = %d", failed)
	}
	if got := progress.PendingCount(); got != 0 {
		t.Fatalf("PendingCount = %d, want 0", got)
	}
	if _, completed, _ := progress.Snapshot(); completed != 1 {
		t.Fatalf("Completed = %d, want 1", completed)
	}
}

// TestCopyObjectEscapesCopySource covers a key containing a percent sign. The
// SDK does not escape CopySource, so building it with fmt.Sprintf("%s/%s")
// sends "percent%20.txt" verbatim, the endpoint decodes %20 as a space, and the
// copy answers 404 NoSuchKey — the object can never be copied and fails every
// cycle. Verified against live Cubbit DS3: raw → 404, escaped → 200.
func TestCopyObjectEscapesCopySource(t *testing.T) {
	api := &fakeCopyAPI{headLen: 4}
	wp := newTestWorkerPoolWithoutAttrs(api)
	wp.sourceBucket = "src"
	wp.targetBucket = "tgt"

	if _, err := wp.copyObject(context.Background(), SyncAction{Type: ActionCopy, Key: "percent%20.txt"}); err != nil {
		t.Fatalf("copyObject: %v", err)
	}
	if api.copyIn == nil {
		t.Fatal("CopyObject was never issued")
	}
	if got, want := aws.ToString(api.copyIn.CopySource), "src/percent%2520.txt"; got != want {
		t.Fatalf("CopySource = %q, want %q", got, want)
	}
	// The object key itself is sent as a query parameter and must NOT be
	// escaped twice — the SDK handles that.
	if got, want := aws.ToString(api.copyIn.Key), "percent%20.txt"; got != want {
		t.Fatalf("Key = %q, want %q", got, want)
	}
}

// TestCopySourceEscapesAwkwardKeys pins the helper's behaviour across every
// shape the scenario suite seeds, including the slashes that must stay literal.
func TestCopySourceEscapesAwkwardKeys(t *testing.T) {
	cases := []struct{ bucket, key, want string }{
		{"b", "plain.txt", "b/plain.txt"},
		{"b", "a/b", "b/a/b"},
		{"b", "with space.txt", "b/with%20space.txt"},
		{"b", "plus+sign.txt", "b/plus+sign.txt"},
		{"b", "percent%20.txt", "b/percent%2520.txt"},
		{"b", "unicode-é.txt", "b/unicode-%C3%A9.txt"},
		{"b", "hash#tag.txt", "b/hash%23tag.txt"},
		{"b", "quest?ion.txt", "b/quest%3Fion.txt"},
		{"b", "deep/a/b/c/d.txt", "b/deep/a/b/c/d.txt"},
	}
	for _, c := range cases {
		if got := copySource(c.bucket, c.key); got != c.want {
			t.Fatalf("copySource(%q, %q) = %q, want %q", c.bucket, c.key, got, c.want)
		}
	}
}
