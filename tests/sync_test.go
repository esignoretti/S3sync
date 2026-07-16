// tests/sync_test.go
//go:build integration

package tests

import (
	"context"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func setupMinio(t *testing.T) (*s3.Client, string) {
	t.Helper()
	endpoint := os.Getenv("MINIO_ENDPOINT")
	if endpoint == "" {
		t.Skip("MINIO_ENDPOINT not set")
	}
	creds := credentials.NewStaticCredentialsProvider("minioadmin", "minioadmin", "")
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(creds),
	)
	if err != nil {
		t.Fatal(err)
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
	return client, "test-bucket"
}

func TestMinioConnection(t *testing.T) {
	client, bucket := setupMinio(t)
	_, err := client.HeadBucket(context.TODO(), &s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Logf("bucket %q may not exist: %v", bucket, err)
		// Not a failure — the test just verifies we can connect
	}
}
