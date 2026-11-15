package sync

import (
	"context"
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
