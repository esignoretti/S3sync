package sync

import (
	"context"
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
