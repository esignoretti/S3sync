package sync

import (
	"context"
	"fmt"
	"log/slog"
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
		pageNum := 0
		var seenMaxKey string
		const maxPages = 100000
		for {
			if pageNum >= maxPages {
				errCh <- fmt.Errorf("list: exceeded %d pages", maxPages)
				return
			}
			out, err := listPage(ctx, token)
			if err != nil {
				errCh <- err
				return
			}
			if len(out.Contents) == 0 {
				slog.Debug("list: empty page, done", "page", pageNum)
				return
			}
			pageNum++
			isTruncated := out.IsTruncated != nil && *out.IsTruncated
			nextToken := ""
			if out.NextContinuationToken != nil {
				nextToken = *out.NextContinuationToken
			}
			firstKey := aws.ToString(out.Contents[0].Key)
			lastKey := aws.ToString(out.Contents[len(out.Contents)-1].Key)

			// Pagination stops on the API's own signals only. Page length is NOT a
			// terminator: S3-compatible providers may return fewer than MaxKeys
			// objects with IsTruncated=true and a continuation token, and treating
			// a short page as the end silently drops every later page.
			last := !isTruncated || nextToken == ""
			if nextToken != "" && nextToken == prevToken {
				slog.Warn("list: same token twice, terminating", "page", pageNum, "token", prevToken)
				last = true
			}
			// Detect Cubbit DS3 regression: unique continuation token but
			// same page content (first key hasn't advanced past previous max).
			if seenMaxKey != "" && firstKey <= seenMaxKey {
				slog.Warn("list: key regression, terminating", "page", pageNum, "firstKey", firstKey, "seenMaxKey", seenMaxKey)
				last = true
			}
			if lastKey > seenMaxKey {
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
				slog.Debug("list: done", "totalPages", pageNum, "firstKey", firstKey, "lastKey", lastKey)
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
	var listErr error

	nextListObj := func() (*s3types.Object, bool) {
		if listDone {
			return nil, false
		}
		for pageIdx >= len(currentPage) {
			select {
			case p, ok := <-pages:
				if !ok {
					listDone = true
					select {
					case err := <-errCh:
						listErr = err
					default:
					}
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

	var listCount, cacheCount, copyCount, deleteCount int
	for listHasMore || cacheHasMore {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		select {
		case err := <-errCh:
			return err
		default:
		}

		if listHasMore && (!cacheHasMore || keyLt(aws.ToString(listObj.Key), cacheCur.Key())) {
			copyCount++
			actions <- SyncAction{
				Type: ActionCopy, Key: aws.ToString(listObj.Key),
				ETag: aws.ToString(listObj.ETag), Size: aws.ToInt64(listObj.Size),
				LastModified: aws.ToTime(listObj.LastModified),
			}
			listObj, listHasMore = nextListObj()
			listCount++
		} else if cacheHasMore && (!listHasMore || keyLt(cacheCur.Key(), aws.ToString(listObj.Key))) {
			if deletePropagation {
				deleteCount++
				actions <- SyncAction{Type: ActionDelete, Key: cacheCur.Key()}
			}
			cacheHasMore = cacheCur.Next()
			cacheCount++
		} else {
			co := cacheCur.Object()
			if co.ETag != aws.ToString(listObj.ETag) || !co.LastModified.Equal(aws.ToTime(listObj.LastModified)) {
				copyCount++
				actions <- SyncAction{
					Type: ActionCopy, Key: aws.ToString(listObj.Key),
					ETag: aws.ToString(listObj.ETag), Size: aws.ToInt64(listObj.Size),
					LastModified: aws.ToTime(listObj.LastModified),
				}
			}
			listObj, listHasMore = nextListObj()
			listCount++
			cacheHasMore = cacheCur.Next()
			cacheCount++
		}
	}

	slog.Info("diff summary",
		"listObjects", listCount,
		"cacheObjects", cacheCount,
		"copies", copyCount,
		"deletes", deleteCount)

	select {
	case err := <-errCh:
		return err
	default:
	}
	return listErr
}

func keyLt(a, b string) bool {
	return a < b
}
