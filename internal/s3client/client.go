package s3client

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	bucketc "github.com/esignoretti/S3sync/internal/config"
)

func NewClient(b *bucketc.Bucket) (*s3.Client, error) {
	creds := credentials.NewStaticCredentialsProvider(b.AccessKey, b.SecretKey, "")
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(b.Region),
		config.WithCredentialsProvider(creds),
		config.WithHTTPClient(&http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				ResponseHeaderTimeout: 10 * time.Second,
				ExpectContinueTimeout: 5 * time.Second,
			},
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}

	var opts []func(*s3.Options)
	if b.Endpoint != "" {
		opts = append(opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(b.Endpoint)
			o.UsePathStyle = true
		})
	}

	return s3.NewFromConfig(cfg, opts...), nil
}
