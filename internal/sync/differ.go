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
	// ActionCopyAttributes retries only the characteristics of an object whose
	// bytes already match. Appended last so the existing values never shift.
	ActionCopyAttributes
)

type SyncAction struct {
	Type         ActionType
	Key          string
	ETag         string
	Size         int64
	LastModified time.Time
	// AttrsHash is the fingerprint of the characteristics applied to the copy.
	// Empty means unknown (attribute replication off, or a partial application),
	// never that the target matches the source.
	AttrsHash string
	// PendingAttrs reports that the bytes were copied but the characteristics
	// were not fully applied, so the next cycle retries them without re-copying.
	PendingAttrs bool
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
	// A failed page fetch is NOT the end of the bucket. Recording it here and
	// stopping the merge join is what keeps the differ from concluding that
	// every remaining cached object is absent from the source: with delete
	// propagation on, that conclusion deletes live target data because one
	// listing call timed out.
	var listErr error

	nextListObj := func() (*s3types.Object, bool) {
		if listDone || listErr != nil {
			return nil, false
		}
		for pageIdx >= len(currentPage) {
			select {
			case p, ok := <-pages:
				if !ok {
					// The fetcher closes `pages` on its way out, error or not,
					// so a closed channel alone does not mean the listing
					// finished. Check for a recorded error before concluding
					// the bucket ended.
					select {
					case err := <-errCh:
						listErr = err
					default:
					}
					listDone = true
					return nil, false
				}
				currentPage = p.objs
				pageIdx = 0
				pageLast = p.last
			case err := <-errCh:
				listErr = err
				listDone = true
				return nil, false
			case <-ctx.Done():
				listErr = ctx.Err()
				listDone = true
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
		if listErr != nil {
			return listErr
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
			etag := aws.ToString(listObj.ETag)
			lastModified := aws.ToTime(listObj.LastModified)
			switch {
			case co.ETag != etag || !co.LastModified.Equal(lastModified):
				actions <- SyncAction{
					Type: ActionCopy, Key: aws.ToString(listObj.Key),
					ETag: etag, Size: aws.ToInt64(listObj.Size),
					LastModified: lastModified,
				}
			case co.PendingAttrs:
				// The bytes match, so no copy will run and nothing would carry
				// the characteristics: retry them on their own. This is the only
				// way a tag or metadata change gets applied, because neither
				// bumps the ETag or LastModified the differ compares.
				actions <- SyncAction{
					Type: ActionCopyAttributes, Key: aws.ToString(listObj.Key),
					ETag: etag, Size: aws.ToInt64(listObj.Size),
					LastModified: lastModified,
				}
			}
			listObj, listHasMore = nextListObj()
			cacheHasMore = cacheCur.Next()
		}
	}

	return listErr
}

func keyLt(a, b string) bool {
	return a < b
}
