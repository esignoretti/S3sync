// tests/scenario/harness_test.go
//go:build integration

package scenario

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestDestroyBucketRemovesVersionsAndDeleteMarkers(t *testing.T) {
	client, _ := newClient(t)
	name := createBucket(t, client, true)
	ctx := context.Background()

	// Three versions of one key, one delete marker, one locked object.
	for i := 0; i < 3; i++ {
		putBody(t, client, name, "multi.txt", fmt.Sprintf("v%d", i))
	}
	_, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(name), Key: aws.String("multi.txt"),
	})
	if err != nil {
		t.Fatalf("delete marker: %v", err)
	}
	putBody(t, client, name, "locked.txt", "held", func(in *s3.PutObjectInput) {
		in.ObjectLockMode = types.ObjectLockModeGovernance
		in.ObjectLockRetainUntilDate = aws.Time(time.Now().Add(24 * time.Hour))
	})

	// The scratch bucket was registered with t.Cleanup, so destroy it by hand
	// here and then assert on the result rather than waiting for cleanup.
	destroyBucket(t, client, name)

	_, err = client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(name)})
	if !isNoSuchBucket(err) {
		t.Fatalf("bucket %q still exists after destroyBucket: %v", name, err)
	}
}

func TestNewScenarioEnvRefusesExistingBucket(t *testing.T) {
	client, _ := newClient(t)
	name := createBucket(t, client, false)
	putBody(t, client, name, "stale.txt", "left over")

	_, err := newScenarioEnvWithBuckets(t, client, name, "")
	if err == nil {
		t.Fatal("newScenarioEnv accepted a source bucket that already holds objects")
	}
	if !strings.Contains(err.Error(), name) {
		t.Fatalf("error does not name the offending bucket %q: %v", name, err)
	}
}

// TestKeepBucket pins the SCENARIO_KEEP contract the README promises: a failing
// scenario's buckets survive for inspection, a passing scenario's never do.
// Keeping a passing scenario's bucket would turn a debug flag into a leak, and
// on this endpoint a leaked bucket bills forever.
func TestKeepBucket(t *testing.T) {
	cases := []struct {
		keep   string
		failed bool
		want   bool
	}{
		{"", false, false},
		{"", true, false},
		{"1", false, false},
		{"1", true, true},
	}
	for _, c := range cases {
		if got := keepBucket(c.keep, c.failed); got != c.want {
			t.Fatalf("keepBucket(%q, failed=%v) = %v, want %v", c.keep, c.failed, got, c.want)
		}
	}
}
