package sync

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/esignoretti/S3sync/internal/cache"
)

func listPageFrom(page []s3types.Object, token *string) func(context.Context, *string) (*s3.ListObjectsV2Output, error) {
	done := false
	return func(ctx context.Context, tok *string) (*s3.ListObjectsV2Output, error) {
		if done {
			return &s3.ListObjectsV2Output{}, nil
		}
		done = true
		return &s3.ListObjectsV2Output{Contents: page}, nil
	}
}

func collectActions(t *testing.T, actions <-chan SyncAction) []SyncAction {
	var out []SyncAction
	for a := range actions {
		out = append(out, a)
	}
	return out
}

func TestStreamingDiffNew(t *testing.T) {
	listing := []s3types.Object{{Key: aws.String("a.jpg"), ETag: aws.String(`"1"`)}}
	actions := make(chan SyncAction)
	cacheCur := &cache.CacheCursor{}
	go func() {
		StreamingDiff(context.Background(), listPageFrom(listing, nil), cacheCur, false, actions)
	}()
	result := collectActions(t, actions)
	if len(result) != 1 {
		t.Fatalf("expected 1 action, got %d", len(result))
	}
	if result[0].Type != ActionCopy {
		t.Fatal("expected copy")
	}
}

func TestStreamingDiffUnchanged(t *testing.T) {
	tm := time.Now()
	listing := []s3types.Object{{Key: aws.String("a.jpg"), ETag: aws.String(`"1"`), LastModified: aws.Time(tm)}}
	actions := make(chan SyncAction)
	// No cache bucket → empty cursor
	go func() {
		StreamingDiff(context.Background(), listPageFrom(listing, nil), &cache.CacheCursor{}, false, actions)
	}()
	result := collectActions(t, actions)
	// No cache → all are new copies
	if len(result) != 1 {
		t.Fatalf("expected 1 copy, got %d", len(result))
	}
}

func TestStreamingDiffDelete(t *testing.T) {
	listing := []s3types.Object{{Key: aws.String("a.jpg")}}
	actions := make(chan SyncAction)
	go func() {
		StreamingDiff(context.Background(), listPageFrom(listing, nil), &cache.CacheCursor{}, true, actions)
	}()
	result := collectActions(t, actions)
	if len(result) != 1 {
		t.Fatalf("expected 1 copy (a.jpg), got %d", len(result))
	}
}

// newDiffCursor builds a cache cursor holding objs, so the differ can be tested
// against entries that already exist.
func newDiffCursor(t *testing.T, pairID string, objs []cache.CachedObject) (*cache.CacheCursor, func()) {
	t.Helper()
	store, err := cache.Open(filepath.Join(t.TempDir(), "cache.db"))
	if err != nil {
		t.Fatal(err)
	}
	ptrs := make([]*cache.CachedObject, 0, len(objs))
	for i := range objs {
		ptrs = append(ptrs, &objs[i])
	}
	if err := store.PutMany(ptrs); err != nil {
		t.Fatal(err)
	}
	cur, closeCur, err := store.NewCursor(pairID)
	if err != nil {
		t.Fatal(err)
	}
	return cur, func() {
		closeCur()
		store.Close()
	}
}

// The bytes matching says nothing about the characteristics: a tag or metadata
// change bumps neither the ETag nor LastModified, so a pending entry must be
// retried on its attributes alone.
func TestDifferEmitsCopyAttributesForPending(t *testing.T) {
	tm := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	listing := []s3types.Object{{
		Key: aws.String("a.jpg"), ETag: aws.String(`"1"`),
		Size: aws.Int64(7), LastModified: aws.Time(tm),
	}}
	cur, done := newDiffCursor(t, "p1", []cache.CachedObject{{
		PairID: "p1", Key: "a.jpg", ETag: `"1"`, Size: 7,
		LastModified: tm, PendingAttrs: true,
	}})
	defer done()

	actions := make(chan SyncAction)
	go func() {
		StreamingDiff(context.Background(), listPageFrom(listing, nil), cur, false, actions)
	}()
	result := collectActions(t, actions)

	if len(result) != 1 {
		t.Fatalf("expected exactly 1 action, got %d: %+v", len(result), result)
	}
	got := result[0]
	if got.Type != ActionCopyAttributes {
		t.Fatalf("action type = %v, want ActionCopyAttributes: the bytes already match", got.Type)
	}
	if got.Key != "a.jpg" {
		t.Fatalf("key = %q, want a.jpg", got.Key)
	}
	if got.ETag != `"1"` || got.Size != 7 || !got.LastModified.Equal(tm) {
		t.Fatalf("action identity = %+v, want the listing's ETag/Size/LastModified", got)
	}
}

// An entry written before the feature existed has an unknown attribute state,
// not a drifted one, and must not cost a source read.
func TestDifferIgnoresUnknownAttrsHash(t *testing.T) {
	tm := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	listing := []s3types.Object{{
		Key: aws.String("a.jpg"), ETag: aws.String(`"1"`),
		Size: aws.Int64(7), LastModified: aws.Time(tm),
	}}
	cur, done := newDiffCursor(t, "p1", []cache.CachedObject{{
		PairID: "p1", Key: "a.jpg", ETag: `"1"`, Size: 7, LastModified: tm,
	}})
	defer done()

	actions := make(chan SyncAction)
	go func() {
		StreamingDiff(context.Background(), listPageFrom(listing, nil), cur, false, actions)
	}()
	if result := collectActions(t, actions); len(result) != 0 {
		t.Fatalf("expected no actions for an unknown AttrsHash, got %d: %+v", len(result), result)
	}
}

// When the bytes changed there is nothing to retry: the copy carries the
// attributes anyway, so a pending entry must still be copied, not re-tagged.
func TestDifferPrefersCopyOverCopyAttributesWhenBytesChanged(t *testing.T) {
	tm := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	listing := []s3types.Object{{
		Key: aws.String("a.jpg"), ETag: aws.String(`"2"`),
		Size: aws.Int64(7), LastModified: aws.Time(tm),
	}}
	cur, done := newDiffCursor(t, "p1", []cache.CachedObject{{
		PairID: "p1", Key: "a.jpg", ETag: `"1"`, Size: 7,
		LastModified: tm, PendingAttrs: true,
	}})
	defer done()

	actions := make(chan SyncAction)
	go func() {
		StreamingDiff(context.Background(), listPageFrom(listing, nil), cur, false, actions)
	}()
	result := collectActions(t, actions)
	if len(result) != 1 || result[0].Type != ActionCopy {
		t.Fatalf("expected exactly one ActionCopy, got %+v", result)
	}
}

// A listing that fails part-way through is not the end of the bucket. If the
// error is swallowed as end-of-list, the merge join believes every remaining
// cached object is absent from the source and — with delete propagation on —
// emits ActionDelete for objects that are still there. The run then deletes
// live target data because a page fetch timed out.
func TestStreamingDiffMidStreamListErrorIsReturned(t *testing.T) {
	// A full page: the differ treats any page with fewer than maxS3Keys entries
	// as the last one, so a short page would never fetch page 2 and the error
	// would never be reached.
	page1 := make([]s3types.Object, 0, maxS3Keys)
	for i := 0; i < maxS3Keys; i++ {
		page1 = append(page1, s3types.Object{
			Key: aws.String(fmt.Sprintf("a%04d.txt", i)), ETag: aws.String(`"1"`),
			Size: aws.Int64(1), LastModified: aws.Time(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)),
		})
	}
	calls := 0
	listPage := func(ctx context.Context, tok *string) (*s3.ListObjectsV2Output, error) {
		calls++
		if calls == 1 {
			return &s3.ListObjectsV2Output{
				Contents:              page1,
				IsTruncated:           aws.Bool(true),
				NextContinuationToken: aws.String("t1"),
			}, nil
		}
		return nil, errors.New("connection reset")
	}

	store, err := cache.Open(filepath.Join(t.TempDir(), "cache.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	// The cached object sorts after the page-1 key, so the merge join reaches it
	// only after the failing fetch.
	if err := store.PutMany([]*cache.CachedObject{{
		PairID: "p1", Key: "z.txt", ETag: `"9"`, Size: 1,
		LastModified: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}}); err != nil {
		t.Fatal(err)
	}
	cur, closeCur, err := store.NewCursor("p1")
	if err != nil {
		t.Fatal(err)
	}
	defer closeCur()

	actions := make(chan SyncAction, 10)
	var diffErr error
	done := make(chan struct{})
	go func() {
		defer close(done)
		diffErr = StreamingDiff(context.Background(), listPage, cur, true, actions)
	}()

	var deletes []string
	for a := range actions {
		if a.Type == ActionDelete {
			deletes = append(deletes, a.Key)
		}
	}
	<-done

	if diffErr == nil {
		t.Fatalf("StreamingDiff returned nil after a mid-stream list error; it emitted deletes %v", deletes)
	}
	if len(deletes) != 0 {
		t.Fatalf("emitted %d ActionDelete (%v) for objects that may still exist", len(deletes), deletes)
	}
}
