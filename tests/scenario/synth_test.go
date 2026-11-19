// tests/scenario/synth_test.go — NO build tag; runs in the default suite.
package scenario

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestSynthBytesAreDeterministicAndRangeConsistent(t *testing.T) {
	a := synthBytesAt("obj/0001-small.bin", 0, 64)
	b := synthBytesAt("obj/0001-small.bin", 0, 64)
	if !bytes.Equal(a, b) {
		t.Fatal("the same key and offset produced different bytes")
	}
	// A window read must equal the same window of a larger read — this is the
	// property that lets a 6 GiB object be verified from a few hundred KB.
	full := synthBytesAt("obj/0001-small.bin", 0, 4096)
	if !bytes.Equal(synthBytesAt("obj/0001-small.bin", 1000, 64), full[1000:1064]) {
		t.Fatal("a range read disagrees with the same window of a full read")
	}

	// Sweep offsets and lengths rather than trusting one sample. synthBytesAt
	// assembles a window from the tail of one 8-byte word, then whole words,
	// then a partial word — three code paths, and an offset that is not a
	// multiple of 8 exercises the first. A single 1000/64 sample happens to be
	// word-aligned and would not catch a broken head-word path, which would
	// silently corrupt every range assertion in the large-object scenarios.
	for off := int64(0); off < 64; off++ {
		for n := int64(1); n <= 40; n++ {
			got := synthBytesAt("obj/0001-small.bin", off, n)
			if !bytes.Equal(got, full[off:off+n]) {
				t.Fatalf("synthBytesAt(off=%d, n=%d) disagrees with the full read", off, n)
			}
		}
	}
}

func TestSynthBytesDifferPerKey(t *testing.T) {
	if bytes.Equal(synthBytesAt("a", 0, 64), synthBytesAt("b", 0, 64)) {
		t.Fatal("two different keys produced identical bytes")
	}
}

func TestSynthChecksumMatchesReader(t *testing.T) {
	const size = 1 << 20
	got := synthChecksum("obj/0002-medium.bin", size)
	want := sha256.Sum256(readAll(t, newSynthReader("obj/0002-medium.bin", size)))
	if got != want {
		t.Fatal("streamed checksum disagrees with the reader")
	}
}

func TestCountingTransportClassifiesCopy(t *testing.T) {
	c := newCountingTransport(roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(""))}, nil
	}))
	req, _ := http.NewRequest("PUT", "https://s3.example/t/b/k?partNumber=3&uploadId=x", nil)
	req.Header.Set("X-Amz-Copy-Source", "t/b/src")
	_, _ = c.RoundTrip(req)
	if c.Count(kindUploadPartCopy) != 1 {
		t.Fatalf("upload part copy count = %d, want 1", c.Count(kindUploadPartCopy))
	}
	if c.Count(kindCopyObject) != 0 {
		t.Fatalf("plain copy count = %d, want 0", c.Count(kindCopyObject))
	}
}

// readAll drains a reader or fails the test.
func readAll(t *testing.T, r io.Reader) []byte {
	t.Helper()
	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read all: %v", err)
	}
	return b
}

// roundTripFunc adapts a function to http.RoundTripper.
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestSynthKeyIsStableAndSortable(t *testing.T) {
	if got, want := synthKey(42, "small"), "obj/0042-small.bin"; got != want {
		t.Fatalf("synthKey = %q, want %q", got, want)
	}
	// Zero-padded so a lexicographic listing matches numeric order — the
	// differ's merge join depends on key order agreeing with the listing.
	if synthKey(9, "small") >= synthKey(10, "small") {
		t.Fatal("synthKey is not zero-padded, so listing order would disagree with numeric order")
	}
}

func TestSynthSizeIsDeterministicWithinShape(t *testing.T) {
	small := shapes[1]
	a := synthSize("obj/0001-small.bin", small)
	b := synthSize("obj/0001-small.bin", small)
	if a != b {
		t.Fatalf("synthSize is not deterministic: %d then %d", a, b)
	}
	if a < small.Min || a > small.Max {
		t.Fatalf("synthSize = %d, outside [%d,%d]", a, small.Min, small.Max)
	}
	// A fixed shape must always produce its exact size.
	huge := shapes[4]
	if got := synthSize("obj/0009-huge.bin", huge); got != huge.Min {
		t.Fatalf("huge size = %d, want %d", got, huge.Min)
	}
	// The huge shape must cross the 5 GiB CopyObject limit or the multipart
	// path is never exercised.
	if huge.Min <= 5<<30 {
		t.Fatalf("huge shape is %d bytes, which does not exceed the 5 GiB copy limit", huge.Min)
	}
}

func TestSynthReaderYieldsExactSizeInAnyChunking(t *testing.T) {
	const size = 5000
	whole := readAll(t, newSynthReader("obj/0003-small.bin", size))
	if int64(len(whole)) != size {
		t.Fatalf("reader yielded %d bytes, want %d", len(whole), size)
	}
	// The SDK picks the chunk size, so chunked reads must agree with one read.
	var chunked []byte
	r := newSynthReader("obj/0003-small.bin", size)
	buf := make([]byte, 7)
	for {
		n, err := r.Read(buf)
		chunked = append(chunked, buf[:n]...)
		if err != nil {
			break
		}
	}
	if !bytes.Equal(whole, chunked) {
		t.Fatal("7-byte chunked reads disagree with a single read")
	}
	if c := synthChecksum("obj/0003-small.bin", size); c != sha256.Sum256(whole) {
		t.Fatal("checksum disagrees with the reader output")
	}
}

// TestCountingTransportUsesHasNotGet is the regression test for a classifier
// that silently counted nothing: the SDK sends valueless sub-resources
// ("?tagging", "?uploads"), and url.Values.Get returns "" for those, so a
// Get-based check misclassified every tag write as a plain PutObject.
func TestCountingTransportUsesHasNotGet(t *testing.T) {
	cases := []struct {
		method, url, want string
	}{
		{"PUT", "https://h/b/k?tagging", kindPutTagging},
		{"GET", "https://h/b/k?tagging", kindGetTagging},
		{"POST", "https://h/b?uploads", kindCreateMPU},
		{"POST", "https://h/b/k?uploadId=x", kindCompleteMPU},
		{"PUT", "https://h/b/k", kindPutObject},
	}
	for _, c := range cases {
		req, err := http.NewRequest(c.method, c.url, nil)
		if err != nil {
			t.Fatalf("build request: %v", err)
		}
		if got := classify(req); got != c.want {
			t.Fatalf("%s %s classified as %q, want %q", c.method, c.url, got, c.want)
		}
	}
}

// TestCountingTransportIgnoresBucketOperations is the regression test for a
// counter that made an idempotent sync look like a copy: provisioning re-issues
// PUT ?lifecycle and PUT ?versioning on every engine start, and a bucket-level
// PUT has no key in its path.
func TestCountingTransportIgnoresBucketOperations(t *testing.T) {
	for _, url := range []string{
		"https://h/bucket?lifecycle=",
		"https://h/bucket?versioning=",
		"https://h/bucket?object-lock=",
	} {
		req, err := http.NewRequest(http.MethodPut, url, nil)
		if err != nil {
			t.Fatalf("build request: %v", err)
		}
		if got := classify(req); got == kindPutObject {
			t.Fatalf("%s was counted as an object write", url)
		}
	}
	// An object PUT must still be counted.
	req, _ := http.NewRequest(http.MethodPut, "https://h/bucket/obj/key.bin", nil)
	if got := classify(req); got != kindPutObject {
		t.Fatalf("object PUT classified as %q, want %q", got, kindPutObject)
	}
}

// TestCountingTransportIgnoresFailedRequests pins that a request which never
// reached the endpoint is not counted as work: counting it turned a transport
// failure into "second sync copied 1 objects", hiding the real cause.
func TestCountingTransportIgnoresFailedRequests(t *testing.T) {
	failing := roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, fmt.Errorf("connection reset")
	})
	c := newCountingTransport(failing)
	req, err := http.NewRequest(http.MethodPut, "https://h/b/k", nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	if _, err := c.RoundTrip(req); err == nil {
		t.Fatal("the fake transport was supposed to fail")
	}
	if got := c.Total(); got != 0 {
		t.Fatalf("a failed request counted as %d operations, want 0", got)
	}
}

// TestCountingTransportCountsDeletesAndUploadParts pins the two kinds that were
// previously invisible: a plain UploadPart (no copy header) and a DELETE.
func TestCountingTransportCountsDeletesAndUploadParts(t *testing.T) {
	cases := []struct{ method, url, want string }{
		{"PUT", "https://h/b/k?partNumber=1&uploadId=x", kindPutObject},
		{"DELETE", "https://h/b/k", kindDeleteObject},
		{"DELETE", "https://h/b/k?versionId=abc", kindDeleteObject},
	}
	for _, tc := range cases {
		req, err := http.NewRequest(tc.method, tc.url, nil)
		if err != nil {
			t.Fatalf("build request: %v", err)
		}
		if got := classify(req); got != tc.want {
			t.Fatalf("%s %s classified as %q, want %q", tc.method, tc.url, got, tc.want)
		}
	}
}
