// tests/scenario/scenario_large_test.go
//go:build integration

package scenario

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestScenarioZeroByteNeverMultipart(t *testing.T) {
	// Review Focus 3: a zero-byte object must take the single-copy path. An
	// object with no parts has nothing to compose, so a multipart attempt would
	// fail at CompleteMultipartUpload.
	e := newScenarioEnv(t, envOptions{})
	e.seed([]string{"obj/zero.bin"})
	e.sync()
	e.assertBytesMatch("obj/zero.bin")
	if got := e.Counter.Count(kindCreateMPU); got != 0 {
		t.Fatalf("a zero-byte object opened %d multipart uploads", got)
	}
}

func TestScenarioFiveGiBMinusOne(t *testing.T) {
	if tier() == tierFast {
		t.Skip("large object")
	}
	e := newScenarioEnv(t, envOptions{})
	e.composeLarge("big/below.bin", (5<<30)-1, nil)
	e.sync()
	e.assertTargetCount(1)
	e.assertRangeMatches("big/below.bin", 0, 1<<30, (5<<30)-65536)
	if got := e.Counter.Count(kindUploadPartCopy); got != 0 {
		t.Fatalf("an object under the single-copy limit used %d multipart part copies", got)
	}
}

func TestScenarioFiveGiBPlusOne(t *testing.T) {
	if tier() != tierAll {
		t.Skip("5 GiB+1 costs ~1000 sequential part copies; tier=all only")
	}
	e := newScenarioEnv(t, envOptions{})
	e.composeLarge("big/above.bin", (5<<30)+1, nil)
	e.sync()
	e.assertRangeMatches("big/above.bin", 0, 5<<30, (5<<30)+1-65536)
	if got := e.Counter.Count(kindUploadPartCopy); got == 0 {
		t.Fatal("an object over the single-copy limit did not use multipart")
	}
}

func TestScenarioSixGiB(t *testing.T) {
	if tier() != tierAll {
		t.Skip("6 GiB; tier=all only")
	}
	e := newScenarioEnv(t, envOptions{})
	e.composeLarge("big/six.bin", 6<<30, nil)
	e.sync()
	e.assertTargetCount(1)
	// Verified by range: the object cannot be downloaded on a 5 GB disk.
	e.assertRangeMatches("big/six.bin", 0, 3<<30, (6<<30)-65536)
	if e.Counter.Count(kindUploadPartCopy) == 0 {
		t.Fatal("a 6 GiB object did not use multipart")
	}
}

// TestScenarioLargeWithAttrs pins that a >5 GiB object's characteristics travel
// on CreateMultipartUpload rather than on any copy call.
func TestScenarioLargeWithAttrs(t *testing.T) {
	if tier() != tierAll {
		t.Skip("6 GiB; tier=all only")
	}
	e := newScenarioEnv(t, envOptions{ReplicateAttrs: true})
	e.composeLarge("big/meta.bin", 6<<30, func(in *s3.CreateMultipartUploadInput) {
		in.ContentType = aws.String("application/x-large")
		in.Metadata = map[string]string{"origin": "composed"}
	})
	putTags(t, e.Client, e.Source, "big/meta.bin", map[string]string{"size": "huge"})
	e.sync()
	f := fetchFacts(t, e.Client, e.Target, "big/meta.bin")
	if f.ContentType != "application/x-large" || f.Metadata["origin"] != "composed" || f.Tags["size"] != "huge" {
		t.Fatalf("target facts = %+v, want content-type, metadata and tag all replicated", f)
	}
}

// TestScenarioLargeSecondSyncIsNoop is the fingerprint round-trip at scale: a
// 6 GiB multipart copy must cache a hash the sweep and differ both accept, or
// every cycle re-copies it.
func TestScenarioLargeSecondSyncIsNoop(t *testing.T) {
	if tier() != tierAll {
		t.Skip("6 GiB; tier=all only")
	}
	e := newScenarioEnv(t, envOptions{ReplicateAttrs: true})
	e.composeLarge("big/attrs.bin", 6<<30, nil)
	e.sync()
	e.assertNoWorkOnSecondSync()
}

func TestScenarioLargeCancelMidCopy(t *testing.T) {
	if tier() != tierAll {
		t.Skip("cancellation case; tier=all only")
	}
	e := newScenarioEnv(t, envOptions{})
	e.composeLarge("big/cancel.bin", 6<<30, nil)

	ctx, cancel := context.WithCancel(context.Background())
	e.startSyncInBackground(ctx)
	time.Sleep(3 * time.Second) // let the multipart upload open
	cancel()
	_ = e.waitForSync() // cancellation is the expected error

	// A cancelled copy must abort its upload, or the orphaned parts bill forever
	// (this endpoint supports no AbortIncompleteMultipartUpload lifecycle rule).
	ups, err := e.Client.ListMultipartUploads(context.Background(), &s3.ListMultipartUploadsInput{
		Bucket: aws.String(e.Target),
	})
	if err != nil {
		t.Fatalf("list target uploads: %v", err)
	}
	if len(ups.Uploads) != 0 {
		t.Fatalf("%d incomplete uploads left on the target after cancellation", len(ups.Uploads))
	}
}
