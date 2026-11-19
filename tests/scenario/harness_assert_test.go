// tests/scenario/harness_assert_test.go
//go:build integration

package scenario

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// TestAssertBytesMatchSeesTruncation is the guard on the suite's primary byte
// assertion. Before it compared the target's length against the target's own
// length, so a copy that wrote a PREFIX of the source produced exactly the
// expected digest and passed — the failure mode is silent data loss.
func TestAssertBytesMatchSeesTruncation(t *testing.T) {
	e := newScenarioEnv(t, envOptions{})
	key := "obj/0001-small.bin"
	e.seed([]string{key})
	srcSize := e.seedSize[key]
	if srcSize < 100 {
		t.Fatalf("fixture is only %d bytes; the truncation case needs a larger object", srcSize)
	}

	// Put a 100-byte prefix of the source at the target, as a truncated copy would.
	src, err := e.Client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(e.Source), Key: aws.String(key), Range: aws.String("bytes=0-99"),
	})
	if err != nil {
		t.Fatalf("read prefix: %v", err)
	}
	body := readAll(t, src.Body)
	_ = src.Body.Close()
	putBody(t, e.Client, e.Target, key, string(body))

	head, err := e.Client.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(e.Target), Key: aws.String(key),
	})
	if err != nil {
		t.Fatalf("head target: %v", err)
	}
	gotSize := aws.ToInt64(head.ContentLength)
	if gotSize == srcSize {
		t.Fatalf("the truncated fixture is %d bytes, same as the source; the case is not set up", gotSize)
	}
	// The digest the assertion demands is the digest of the WHOLE source. The
	// truncated target's own digest must not satisfy it — that is the property
	// the pre-fix code violated, because it derived the expectation from the
	// target's length.
	want := e.expectedDigest(key, srcSize)
	truncated, err := digestOfObject(context.Background(), e.Client, e.Target, key)
	if err != nil {
		t.Fatalf("digest the truncated target: %v", err)
	}
	if truncated == want {
		t.Fatal("a truncated target satisfies the expected digest, so the assertion cannot detect truncation")
	}
	// And the digest derived from the TARGET's length — the pre-fix behaviour —
	// is exactly the truncated object's digest, which is why it passed.
	if wrongWay := e.expectedDigest(key, gotSize); wrongWay != truncated {
		t.Logf("note: deriving from the target length no longer coincides with the truncated digest")
	}
	t.Logf("target %d bytes vs source %d bytes: the expected digest rejects the truncated copy", gotSize, srcSize)
}
