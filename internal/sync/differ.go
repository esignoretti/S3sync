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

const maxS3Keys = 1000

func StreamingDiff(
	ctx context.Context,
	listPage func(context.Context, *string) (*s3.ListObjectsV2Output, error),
	cacheCur *cache.CacheCursor,
	deletePropagation bool,
	actions chan<- SyncAction,
) error {
	defer close(actions)

	type fetchedPage struct {
		objs []s3types.Object
		last bool
	}

	pages := make(chan fetchedPage, 2)
	errCh := make(chan error, 1)

	// Background page fetcher
	go func() {
		defer close(pages)
		var token *string
		var prevToken string
		var seenMaxKey string
		for {
			out, err := listPage(ctx, token)
			if err != nil {
				errCh <- err
				return
			}
			if len(out.Contents) == 0 {
				return
			}
			isTruncated := out.IsTruncated != nil && *out.IsTruncated
			nextToken := ""
			if out.NextContinuationToken != nil {
				nextToken = *out.NextContinuationToken
			}
			last := !isTruncated || nextToken == "" || len(out.Contents) < maxS3Keys
			if nextToken != "" && nextToken == prevToken {
				last = true
			}
			firstKey := aws.ToString(out.Contents[0].Key)
			if seenMaxKey != "" && firstKey <= seenMaxKey {
				last = true
			}
			if lastKey := aws.ToString(out.Contents[len(out.Contents)-1].Key); lastKey > seenMaxKey {
				seenMaxKey = lastKey
			}
			prevToken = nextToken
			select {
			case pages <- fetchedPage{objs: out.Contents, last: last}:
			case <-ctx.Done():
				return
			}
			token = out.NextContinuationToken
			if last {
				return
			}
		}
	}()

	// Check for initial fetch error
	select {
	case err := <-errCh:
		return err
	default:
	}

	// nextListObj reads from the prefetch channel
	var currentPage []s3types.Object
	var pageIdx int
	var pageLast bool
	var listDone bool

	nextListObj := func() (*s3types.Object, bool) {
		if listDone {
			return nil, false
		}
		for pageIdx >= len(currentPage) {
			select {
			case p, ok := <-pages:
				if !ok {
					listDone = true
					return nil, false
				}
				currentPage = p.objs
				pageIdx = 0
				pageLast = p.last
			case <-errCh:
				return nil, false
			case <-ctx.Done():
				return nil, false
			}
		}
		obj := currentPage[pageIdx]
		pageIdx++
		if pageIdx == len(currentPage) && pageLast {
			listDone = true
		}
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
