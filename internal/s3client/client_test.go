package s3client

import (
	"testing"

	"github.com/esignoretti/S3sync/internal/config"
)

func TestNewClient(t *testing.T) {
	b := &config.Bucket{
		Endpoint: "https://s3.amazonaws.com", Region: "us-east-1",
		AccessKey: "test", SecretKey: "test",
	}
	c, err := NewClient(b)
	if err != nil {
		t.Fatal(err)
	}
	if c == nil {
		t.Fatal("expected non-nil client")
	}
}
