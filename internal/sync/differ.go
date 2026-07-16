package sync

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/esignoretti/S3sync/internal/cache"
)

type ActionType int

const (
	ActionCopy ActionType = iota
	ActionDelete
)

type SyncAction struct {
	Type         ActionType
	Key          string
	ETag         string
	Size         int64
	LastModified time.Time
}

// StreamingDiff performs a merge-join between S3 listing pages and the cache cursor.
// It emits SyncActions to the actions channel as it goes.
// S3 listing comes page-by-page via listPage func.
// Cache cursor iterates BoltDB in key order.
// Both are sorted by key, so we advance both pointers like a merge-join.
func StreamingDiff(
	ctx context.Context,
	listPage func(context.Context, *string) (*s3.ListObjectsV2Output, error),
	cacheCur *cache.CacheCursor,
	deletePropagation bool,
	actions chan<- SyncAction,
) error {
	defer close(actions)

	var token *string
	var pageObjs []s3types.Object
	var pageIdx int

	nextListObj := func() (*s3types.Object, bool) {
		for pageIdx >= len(pageObjs) {
			out, err := listPage(ctx, token)
			if err != nil {
				return nil, false
			}
			pageObjs = out.Contents
			pageIdx = 0
			token = out.NextContinuationToken
			if len(pageObjs) == 0 && token == nil {
				return nil, false
			}
		}
		obj := pageObjs[pageIdx]
		pageIdx++
		return &obj, true
	}

	cacheHasMore := cacheCur.Next()
	listObj, listHasMore := nextListObj()

	for listHasMore || cacheHasMore {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if listHasMore && (!cacheHasMore || keyLt(aws.ToString(listObj.Key), cacheCur.Key())) {
			actions <- SyncAction{
				Type: ActionCopy, Key: aws.ToString(listObj.Key),
				ETag: aws.ToString(listObj.ETag), Size: aws.ToInt64(listObj.Size),
				LastModified: aws.ToTime(listObj.LastModified),
			}
			listObj, listHasMore = nextListObj()
		} else if cacheHasMore && (!listHasMore || keyLt(cacheCur.Key(), aws.ToString(listObj.Key))) {
			if deletePropagation {
				actions <- SyncAction{Type: ActionDelete, Key: cacheCur.Key()}
			}
			cacheHasMore = cacheCur.Next()
		} else {
			co := cacheCur.Object()
			if co.ETag != aws.ToString(listObj.ETag) || !co.LastModified.Equal(aws.ToTime(listObj.LastModified)) {
				actions <- SyncAction{
					Type: ActionCopy, Key: aws.ToString(listObj.Key),
					ETag: aws.ToString(listObj.ETag), Size: aws.ToInt64(listObj.Size),
					LastModified: aws.ToTime(listObj.LastModified),
				}
			}
			listObj, listHasMore = nextListObj()
			cacheHasMore = cacheCur.Next()
		}
	}

	return nil
}

func keyLt(a, b string) bool {
	return a < b
}
