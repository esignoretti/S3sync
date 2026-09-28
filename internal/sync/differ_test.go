package sync

import (
	"context"
	"fmt"
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

// fakeLister serves a fixed sequence of pages, then an empty page — mirroring
// how ListObjectsV2 ends. It counts calls so tests can prove pagination did or
// did not continue.
type fakeLister struct {
	pages []*s3.ListObjectsV2Output
	calls int
}

func (f *fakeLister) list(_ context.Context, _ *string) (*s3.ListObjectsV2Output, error) {
	if f.calls >= len(f.pages) {
		return &s3.ListObjectsV2Output{}, nil
	}
	p := f.pages[f.calls]
	f.calls++
	return p, nil
}

// makePage builds a page of n objects with lexicographically increasing keys
// starting at start, so the differ's monotonicity guard stays satisfied.
func makePage(start, n int, truncated bool, token string) *s3.ListObjectsV2Output {
	objs := make([]s3types.Object, 0, n)
	for i := 0; i < n; i++ {
		objs = append(objs, s3types.Object{
			Key:  aws.String(fmt.Sprintf("k-%06d", start+i)),
			ETag: aws.String(`"e"`),
			Size: aws.Int64(1),
		})
	}
	out := &s3.ListObjectsV2Output{Contents: objs, IsTruncated: aws.Bool(truncated)}
	if token != "" {
		out.NextContinuationToken = aws.String(token)
	}
	return out
}

// runDiff drives StreamingDiff to completion and returns the actions and the
// terminal error.
func runDiff(t *testing.T, lister func(context.Context, *string) (*s3.ListObjectsV2Output, error), deleteProp bool) ([]SyncAction, error) {
	t.Helper()
	actions := make(chan SyncAction)
	errCh := make(chan error, 1)
	go func() {
		errCh <- StreamingDiff(context.Background(), lister, &cache.CacheCursor{}, deleteProp, actions)
	}()
	out := collectActions(t, actions)
	return out, <-errCh
}

// A page shorter than MaxKeys is NOT a pagination terminator. S3-compatible
// providers may return a short page with IsTruncated=true and a continuation
// token; treating length as a stop signal silently drops every later page.
func TestStreamingDiffFollowsAllPagesWhenPageIsShort(t *testing.T) {
	lister := &fakeLister{pages: []*s3.ListObjectsV2Output{
		makePage(0, 500, true, "t1"),
		makePage(500, 500, true, "t2"),
		makePage(1000, 500, true, "t3"),
		makePage(1500, 500, true, "t4"),
	}}
	result, err := runDiff(t, lister.list, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 2000 {
		t.Errorf("actions = %d, want 2000 (all four short pages must be followed)", len(result))
	}
	if lister.calls != 4 {
		t.Errorf("list calls = %d, want 4", lister.calls)
	}
}

// IsTruncated=false is the authoritative stop signal: no further page may be
// requested even if the page was short.
func TestStreamingDiffStopsWhenNotTruncated(t *testing.T) {
	lister := &fakeLister{pages: []*s3.ListObjectsV2Output{
		makePage(0, 500, false, ""),
	}}
	result, err := runDiff(t, lister.list, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 500 {
		t.Errorf("actions = %d, want 500", len(result))
	}
	if lister.calls != 1 {
		t.Errorf("list calls = %d, want 1 (not truncated means stop)", lister.calls)
	}
}

// A provider that returns the same continuation token forever must not spin.
func TestStreamingDiffRepeatedTokenTerminates(t *testing.T) {
	lister := &fakeLister{pages: []*s3.ListObjectsV2Output{
		makePage(0, 1000, true, "same"),
		makePage(1000, 1000, true, "same"),
	}}
	result, err := runDiff(t, lister.list, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if lister.calls != 2 {
		t.Errorf("list calls = %d, want 2 (repeat token must terminate)", lister.calls)
	}
	if len(result) != 2000 {
		t.Errorf("actions = %d, want 2000", len(result))
	}
}
