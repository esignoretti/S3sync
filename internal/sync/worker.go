package sync

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type WorkerPool struct {
	workers        int
	src            copyAPI
	tgt            copyAPI
	sourceBucket   string
	targetBucket   string
	throttler      *Throttler
	storageClass   string
	progress       *Progress
	replicateAttrs bool
}

// copyAPI is the subset of the S3 API the copy and delete paths use.
// *s3.Client satisfies it directly; tests substitute a fake.
type copyAPI interface {
	HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	CopyObject(context.Context, *s3.CopyObjectInput, ...func(*s3.Options)) (*s3.CopyObjectOutput, error)
	GetObjectTagging(context.Context, *s3.GetObjectTaggingInput, ...func(*s3.Options)) (*s3.GetObjectTaggingOutput, error)
	PutObjectTagging(context.Context, *s3.PutObjectTaggingInput, ...func(*s3.Options)) (*s3.PutObjectTaggingOutput, error)
	DeleteObject(context.Context, *s3.DeleteObjectInput, ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
}

// ErrObjectTooLarge reports a source object that CopyObject cannot move in one
// request. It is permanent: the object will not shrink on a retry, so it is
// returned unwrapped by retryS3.
var ErrObjectTooLarge = errors.New("object exceeds the 5 GiB CopyObject limit")

// errObjectVanished reports a source object that disappeared between listing
// and HEAD. Run treats it as neither success nor failure: an object that was
// never copied must not reach the cache, or StreamingDiff would consider the
// key synced and never retry it.
var errObjectVanished = errors.New("source object vanished")

const maxCopyObjectSize = 5 << 30

func NewWorkerPool(workers int, srcClient, tgtClient *s3.Client, source, target string,
	throttler *Throttler, storageClass string, progress *Progress, replicateAttrs bool) *WorkerPool {
	return &WorkerPool{
		workers: workers, src: srcClient, tgt: tgtClient,
		sourceBucket: source, targetBucket: target,
		throttler: throttler, storageClass: storageClass,
		progress: progress, replicateAttrs: replicateAttrs,
	}
}

// Run reads actions from the channel until it is closed.
func (wp *WorkerPool) Run(ctx context.Context, actions <-chan SyncAction) ([]SyncAction, int, int) {
	type result struct {
		action SyncAction
		err    error
	}

	results := make(chan result)
	var wg sync.WaitGroup

	workerCount := wp.workers
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for a := range actions {
				start := time.Now()
				out := a
				var err error
				switch a.Type {
				case ActionCopy:
					// copyObject returns the action enriched with the attribute
					// outcome, which the cache writer needs.
					out, err = wp.copyObject(ctx, a)
				case ActionDelete:
					err = wp.deleteObject(ctx, a)
				case ActionCopyAttributes:
					out, err = wp.applyAttrs(ctx, a)
				}
				if err != nil {
					if !errors.Is(err, context.Canceled) && !errors.Is(err, errObjectVanished) {
						slog.Error("worker failed", "key", a.Key, "action", a.Type, "error", err)
					}
				} else {
					slog.Info("worker done", "key", a.Key, "action", a.Type, "ms", time.Since(start).Milliseconds())
				}
				results <- result{action: out, err: err}
				if wp.progress != nil && !errors.Is(err, errObjectVanished) {
					wp.progress.Add(err != nil)
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var succeeded []SyncAction
	failed := 0
	for r := range results {
		switch {
		case errors.Is(r.err, errObjectVanished):
			// Skipped: not copied, so neither succeeded nor failed, and never
			// cached. Counting it either way would lie about the run.
		case r.err != nil:
			failed++
		default:
			succeeded = append(succeeded, r.action)
		}
	}

	return succeeded, len(succeeded), failed
}

// copyObject moves one object and, when attribute replication is on, applies its
// characteristics. It returns the action carrying the fingerprint to cache: an
// empty AttrsHash means the attribute state is unknown (replication off, or a
// partial application), never that it matches.
func (wp *WorkerPool) copyObject(ctx context.Context, a SyncAction) (SyncAction, error) {
	reqCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// HEAD the source, not the target: ContentLength (and the attributes worth
	// replicating) lives on the source, and the differ has already decided this
	// object needs copying. Probing the target here duplicated that decision and
	// treated the 404 a brand-new key always produces as a retryable failure, so
	// CopyObject was never issued for new objects.
	headOut, err := wp.headSource(reqCtx, a.Key)
	if err != nil {
		return a, err
	}
	if headOut == nil {
		slog.Debug("source object vanished", "key", a.Key)
		return a, errObjectVanished
	}

	if size := aws.ToInt64(headOut.ContentLength); size > maxCopyObjectSize {
		return a, fmt.Errorf("%w: key %q is %d bytes, limit %d", ErrObjectTooLarge, a.Key, size, int64(maxCopyObjectSize))
	}

	var attrs ObjectAttrs
	if wp.replicateAttrs {
		attrs = wp.effectiveAttrs(a.Key, headOut)
	}

	// No target-side ETag comparison: the differ already decided this object
	// differs, and the target does not evaluate x-amz-copy-source-if-match, so a
	// conditional copy would buy nothing.
	copyOut, err := wp.copyBytes(reqCtx, a.Key, attrs)
	if err != nil {
		return a, err
	}

	if !wp.replicateAttrs {
		return a, nil
	}

	var versionID *string
	if copyOut != nil {
		versionID = copyOut.VersionId
	}
	attrs, ok := wp.writeAttrs(reqCtx, a.Key, attrs, versionID)
	if !ok {
		// The bytes are correct; only the characteristics are incomplete. Retrying
		// the copy would re-copy bytes for no reason, so this is a pending state.
		a.PendingAttrs = true
		return a, nil
	}

	a.AttrsHash = attrs.Hash()
	return a, nil
}

// applyAttrs applies the characteristics of an object whose bytes are already on
// the target. It runs the same attribute phase as copyObject and nothing else:
// no CopyObject, no size check, and no VersionId, so the target's current
// version is the one tagged.
//
// This is what a pending_attrs entry or a drifted fingerprint turns into, so the
// two paths must fingerprint identically or the sweep re-emits the action every
// cycle.
func (wp *WorkerPool) applyAttrs(ctx context.Context, a SyncAction) (SyncAction, error) {
	if !wp.replicateAttrs {
		// Attribute replication is off, so nothing here is wanted on the target.
		// The empty hash records "unknown", which stops the differ and the sweep
		// from asking again.
		return a, nil
	}

	reqCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	headOut, err := wp.headSource(reqCtx, a.Key)
	if err != nil {
		return a, err
	}
	if headOut == nil {
		slog.Debug("source object vanished", "key", a.Key)
		return a, errObjectVanished
	}

	attrs, ok := wp.writeAttrs(reqCtx, a.Key, wp.effectiveAttrs(a.Key, headOut), nil)
	if !ok {
		a.PendingAttrs = true
		return a, nil
	}

	a.AttrsHash = attrs.Hash()
	return a, nil
}

// effectiveAttrs reads the characteristics a source HEAD can see and normalizes
// the storage class to the one the target will actually carry. Every fingerprint
// goes through here, on the writer and on the sweep alike: with a pair override
// the source class is never applied, so hashing it would report drift no copy
// can fix.
func (wp *WorkerPool) effectiveAttrs(key string, headOut *s3.HeadObjectOutput) ObjectAttrs {
	attrs := AttrsFromHead(headOut)
	if attrs.RetainUntil != nil && !attrs.liveRetention(time.Now().UTC()) {
		// Sending a past RetainUntilDate makes S3 reject the whole copy, and it
		// protects nothing, so the lock fields are dropped for this object.
		slog.Debug("source retention already expired, not replicating lock",
			"key", key, "retain_until", attrs.RetainUntil.UTC().Format(time.RFC3339))
	}
	return attrs.WithEffectiveStorageClass(wp.storageClass)
}

// writeAttrs reads the source's tags and writes them onto the target, returning
// the attributes with Tags filled in. ok is false when the characteristics are
// only partially applied, which the caller records as pending.
//
// Tags cannot ride on CopyObject on this endpoint (x-amz-tagging is not applied),
// so they always travel through GetObjectTagging and PutObjectTagging. HEAD could
// not see them, which is why AttrsFromHead leaves Tags nil and they are attached
// here before hashing.
func (wp *WorkerPool) writeAttrs(ctx context.Context, key string, attrs ObjectAttrs, versionID *string) (ObjectAttrs, bool) {
	tags, err := wp.sourceTags(ctx, key)
	if err != nil {
		slog.Warn("read source tags", "key", key, "error", err)
		return attrs, false
	}
	attrs.Tags = tags
	if err := wp.applyTags(ctx, key, tags, versionID); err != nil {
		slog.Warn("apply target tags", "key", key, "error", err)
		return attrs, false
	}
	return attrs, true
}

// headSource HEADs the source object. It returns (nil, nil) when the object is
// gone: the differ listed this key moments ago, so a 404 now means the object
// vanished mid-run. That is a skip, not a failure, and must not burn the retry
// budget.
func (wp *WorkerPool) headSource(ctx context.Context, key string) (*s3.HeadObjectOutput, error) {
	var headOut *s3.HeadObjectOutput
	vanished := false
	err := retryS3(ctx, "HeadObject", func(c context.Context) error {
		if err := wp.throttler.WaitLog(c, wp.sourceBucket); err != nil {
			return err
		}
		out, err := wp.src.HeadObject(c, &s3.HeadObjectInput{
			Bucket: &wp.sourceBucket, Key: &key,
		})
		if err != nil {
			if isNotFound(err, "NotFound", "NoSuchKey") {
				vanished = true
				return nil
			}
			return err
		}
		headOut = out
		return nil
	}, 3)
	if err != nil {
		return nil, err
	}
	if vanished {
		return nil, nil
	}
	return headOut, nil
}

// copyBytes issues the CopyObject that moves the bytes. With attribute
// replication off it builds exactly the legacy request.
func (wp *WorkerPool) copyBytes(ctx context.Context, key string, attrs ObjectAttrs) (*s3.CopyObjectOutput, error) {
	copySource := fmt.Sprintf("%s/%s", wp.sourceBucket, key)
	var copyOut *s3.CopyObjectOutput
	err := retryS3(ctx, "CopyObject", func(c context.Context) error {
		if err := wp.throttler.WaitLog(c, wp.sourceBucket); err != nil {
			return err
		}
		out, err := wp.tgt.CopyObject(c, wp.copyRequest(key, copySource, attrs))
		copyOut = out
		return err
	}, 3)
	if err != nil {
		return nil, err
	}
	return copyOut, nil
}

func (wp *WorkerPool) copyRequest(key, copySource string, attrs ObjectAttrs) *s3.CopyObjectInput {
	if wp.replicateAttrs {
		return attrs.copyInput(wp.targetBucket, key, copySource, wp.storageClass)
	}
	in := &s3.CopyObjectInput{
		Bucket:     &wp.targetBucket,
		CopySource: &copySource,
		Key:        &key,
	}
	if wp.storageClass != "" {
		in.StorageClass = types.StorageClass(wp.storageClass)
	}
	return in
}

// sourceTags reads the source object's tags. The call is a source read, so it is
// charged to the get-op budget like the HEAD and the copy.
func (wp *WorkerPool) sourceTags(ctx context.Context, key string) ([]types.Tag, error) {
	var tags []types.Tag
	err := retryS3(ctx, "GetObjectTagging", func(c context.Context) error {
		if err := wp.throttler.WaitLog(c, wp.sourceBucket); err != nil {
			return err
		}
		out, err := wp.src.GetObjectTagging(c, &s3.GetObjectTaggingInput{
			Bucket: &wp.sourceBucket, Key: &key,
		})
		if err != nil {
			return err
		}
		tags = out.TagSet
		return nil
	}, 3)
	return tags, err
}

// applyTags writes the source's tags onto the version the copy just created.
// An empty set is written too, so a source whose tags were all removed clears
// the target's: the target's tags must equal the source's, not merely be a
// superset of whatever an earlier write left behind.
//
// This assumes the endpoint accepts an empty <TagSet/>: some S3 implementations
// reject it as MalformedXML, and then a tagless object would sit in
// pending_attrs forever. Needs an integration check in Task 9.
func (wp *WorkerPool) applyTags(ctx context.Context, key string, tags []types.Tag, versionID *string) error {
	return retryS3(ctx, "PutObjectTagging", func(c context.Context) error {
		in := &s3.PutObjectTaggingInput{
			Bucket:  &wp.targetBucket,
			Key:     &key,
			Tagging: &types.Tagging{TagSet: tags},
		}
		if versionID != nil {
			in.VersionId = versionID
		}
		_, err := wp.tgt.PutObjectTagging(c, in)
		return err
	}, 3)
}

func (wp *WorkerPool) deleteObject(ctx context.Context, a SyncAction) error {
	reqCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	return retryS3(reqCtx, "DeleteObject", func(c context.Context) error {
		if err := wp.throttler.WaitLog(c, wp.sourceBucket); err != nil {
			return err
		}
		_, err := wp.tgt.DeleteObject(c, &s3.DeleteObjectInput{
			Bucket: &wp.targetBucket, Key: &a.Key,
		})
		return err
	}, 3)
}

func retryS3(ctx context.Context, label string, fn func(context.Context) error, maxAttempts int) error {
	var err error
	delay := time.Second
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if attempt > 1 {
			slog.Debug("retry", "label", label, "attempt", attempt, "delay_ms", delay.Milliseconds())
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			}
			delay *= 2
		}
		err = fn(ctx)
		if err == nil {
			return nil
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		if attempt < maxAttempts {
			slog.Warn("s3 call failed, retrying", "label", label, "attempt", attempt, "max", maxAttempts, "error", err)
		}
	}
	return fmt.Errorf("%s failed after %d attempts: %w", label, maxAttempts, err)
}
