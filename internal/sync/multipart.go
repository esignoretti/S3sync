package sync

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const mib = int64(1) << 20

// multipartPartSize picks a part size that keeps the part count inside the
// 10,000 limit without making parts absurdly large: proportional to the object,
// rounded up to a whole MiB, clamped to the endpoint's 5 MiB..5 GiB range.
func multipartPartSize(size int64) int64 {
	want := (size + maxMultipartParts - 1) / maxMultipartParts
	// Round up to a whole MiB.
	want = ((want + mib - 1) / mib) * mib
	if want < minPartSize {
		want = minPartSize
	}
	if want > maxPartSize {
		want = maxPartSize
	}
	return want
}

// copyMultipart moves an object too large for a single CopyObject, by creating a
// multipart upload on the target and copying the source into it part by part.
//
// Parts are copied sequentially and server-side: the source bytes never travel
// through this process, so the only cost is one UploadPartCopy per part. Part
// ranges always carry an explicit end offset because this endpoint rejects an
// open-ended range with InvalidRange.
//
// The characteristics ride on CreateMultipartUpload, which this endpoint honours,
// so there is no second retention call. Tags still go through PutObjectTagging,
// applied by the caller's attribute step.
func (wp *WorkerPool) copyMultipart(ctx context.Context, a SyncAction, attrs ObjectAttrs, size int64) (SyncAction, error) {
	createOut, err := wp.tgt.CreateMultipartUpload(ctx, attrs.createMPUInput(wp.targetBucket, a.Key, wp.storageClass))
	if err != nil {
		return a, wp.explainCopyError(a.Key, attrs, err)
	}
	uploadID := aws.ToString(createOut.UploadId)

	completed := false
	defer func() {
		if completed {
			return
		}
		// Detach from ctx: on cancellation the request context is already done,
		// so an abort built from it would be cancelled before it left the
		// process. This endpoint has no AbortIncompleteMultipartUpload lifecycle
		// rule, so orphaned parts bill forever — the abort must actually land.
		abortCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		if _, err := wp.tgt.AbortMultipartUpload(abortCtx, &s3.AbortMultipartUploadInput{
			Bucket:   &wp.targetBucket,
			Key:      &a.Key,
			UploadId: &uploadID,
		}); err != nil {
			slog.Warn("multipart: abort failed, parts may be orphaned",
				"key", a.Key, "upload", uploadID, "error", err)
		}
	}()

	partSize := multipartPartSize(size)
	var parts []s3types.CompletedPart
	for partNumber, start := 1, int64(0); start < size; partNumber, start = partNumber+1, start+partSize {
		end := start + partSize - 1
		if end >= size {
			end = size - 1
		}
		rng := fmt.Sprintf("bytes=%d-%d", start, end)
		out, err := wp.tgt.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
			Bucket:          &wp.targetBucket,
			Key:             &a.Key,
			UploadId:        &uploadID,
			PartNumber:      ptr(int32(partNumber)),
			CopySource:      ptr(fmt.Sprintf("%s/%s", wp.sourceBucket, a.Key)),
			CopySourceRange: &rng,
		})
		if err != nil {
			return a, wp.explainCopyError(a.Key, attrs, err)
		}
		var etag *string
		if out.CopyPartResult != nil {
			etag = out.CopyPartResult.ETag
		}
		// The completion request needs QUOTED ETags, which is what the endpoint
		// returns; passing them through unchanged is what keeps it valid.
		parts = append(parts, s3types.CompletedPart{
			ETag: etag, PartNumber: ptr(int32(partNumber)),
		})
	}

	if _, err := wp.tgt.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          &wp.targetBucket,
		Key:             &a.Key,
		UploadId:        &uploadID,
		MultipartUpload: &s3types.CompletedMultipartUpload{Parts: parts},
	}); err != nil {
		return a, wp.explainCopyError(a.Key, attrs, err)
	}
	completed = true
	return a, nil
}
