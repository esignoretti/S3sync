package sync

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type ListedObject struct {
	Key          string
	ETag         string
	Size         int64
	LastModified time.Time
}

type Lister struct {
	client    *s3.Client
	bucket    string
	throttler *Throttler
}

func NewLister(client *s3.Client, bucket string, throttler *Throttler) *Lister {
	return &Lister{client: client, bucket: bucket, throttler: throttler}
}

func (l *Lister) List(ctx context.Context) ([]ListedObject, error) {
	var objects []ListedObject
	var token *string

	for {
		if err := l.throttler.WaitLog(ctx, l.bucket); err != nil {
			return nil, fmt.Errorf("throttle: %w", err)
		}

		out, err := l.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            &l.bucket,
			MaxKeys:           aws.Int32(1000),
			ContinuationToken: token,
		})
		if err != nil {
			return nil, fmt.Errorf("list: %w", err)
		}

		for _, obj := range out.Contents {
			objects = append(objects, ListedObject{
				Key:          aws.ToString(obj.Key),
				ETag:         aws.ToString(obj.ETag),
				Size:         aws.ToInt64(obj.Size),
				LastModified: aws.ToTime(obj.LastModified),
			})
		}

		slog.Debug("listed page", "bucket", l.bucket,
			"page_size", len(out.Contents), "total", len(objects))

		if !aws.ToBool(out.IsTruncated) {
			break
		}
		token = out.NextContinuationToken
	}

	return objects, nil
}
