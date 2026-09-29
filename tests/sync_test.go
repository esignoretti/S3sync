// tests/sync_test.go
//go:build integration

package tests

import (
	"context"
	"errors"
	"net/http"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

func setupS3Client(t *testing.T) (*s3.Client, string) {
	t.Helper()
	endpoint := os.Getenv("S3_TEST_ENDPOINT")
	if endpoint == "" {
		t.Skip("S3_TEST_ENDPOINT not set")
	}
	accessKey := os.Getenv("S3_TEST_ACCESS_KEY")
	secretKey := os.Getenv("S3_TEST_SECRET_KEY")
	if accessKey == "" || secretKey == "" {
		t.Skip("S3_TEST_ACCESS_KEY or S3_TEST_SECRET_KEY not set")
	}

	creds := credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")
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
	return client, "test-s3sync-integration"
}

// TestS3Connection proves the configured endpoint is actually reachable. The
// previous version only logged the error, so it passed no matter what went
// wrong — a wrong endpoint, bad credentials, or no network at all.
//
// A 404 counts as success: it means the S3 service answered and simply does not
// have the bucket yet, which is the expected state on a first run and is still
// positive proof of connectivity. Anything else is a real failure.
func TestS3Connection(t *testing.T) {
	client, bucket := setupS3Client(t)
	_, err := client.HeadBucket(context.TODO(), &s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	})
	if err == nil {
		return
	}

	var notFound *s3types.NotFound
	var respErr *smithyhttp.ResponseError
	if errors.As(err, &notFound) ||
		(errors.As(err, &respErr) && respErr.HTTPStatusCode() == http.StatusNotFound) {
		t.Logf("endpoint reachable; bucket %q does not exist yet: %v", bucket, err)
		return
	}

	t.Fatalf("cannot reach the S3 endpoint: %v", err)
}
