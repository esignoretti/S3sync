// tests/scenario/counting.go — NO build tag; the classifier is unit-testable
// without an endpoint.

package scenario

import (
	"net/http"
	"strings"
	"sync"
)

// Request kinds the scenario assertions distinguish. A second sync that does
// no work must show zero of all of them; counting requests is stronger than
// reading Engine.LastResult(), because a copy that succeeds but is then
// mis-cached still reports success while re-copying next time.
const (
	kindCopyObject     = "copy_object"
	kindUploadPartCopy = "upload_part_copy"
	kindPutObject      = "put_object"
	kindPutTagging     = "put_tagging"
	kindGetTagging     = "get_tagging"
	kindCreateMPU      = "create_multipart_upload"
	kindCompleteMPU    = "complete_multipart_upload"
	kindDeleteObject   = "delete_object"
	kindOther          = "other"
)

// countingTransport wraps a RoundTripper and tallies S3 operations by kind.
// Scenarios install it as the client's transport so they can assert on the
// actual wire traffic rather than on log lines.
type countingTransport struct {
	base http.RoundTripper

	mu     sync.Mutex
	counts map[string]int
}

// newCountingTransport wraps base, defaulting to http.DefaultTransport so a
// caller can pass nil.
func newCountingTransport(base http.RoundTripper) *countingTransport {
	if base == nil {
		base = http.DefaultTransport
	}
	return &countingTransport{base: base, counts: map[string]int{}}
}

// RoundTrip classifies the request, delegates it, and records the kind only
// after the base transport returns — so a request that never reached the
// endpoint (a client-side rejection) is not counted as work performed.
func (c *countingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := c.base.RoundTrip(r)
	if err != nil {
		// A request that never reached the endpoint performed no work. Counting
		// it turned a transport failure into "second sync copied 1 objects",
		// which hides the real cause behind a misleading message.
		return resp, err
	}
	kind := classify(r)
	c.mu.Lock()
	c.counts[kind]++
	c.mu.Unlock()
	return resp, err
}

// classify maps a request to a kind.
//
// Three distinctions the counting assertions depend on:
//   - a bucket-level request has no key in its path (PUT /bucket?lifecycle) and
//     is never an object write, because provisioning re-issues it on every
//     engine start and counting it made an idempotent sync look like a copy;
//   - the SDK sends VALUELESS sub-resources ("?tagging", "?uploads"), for which
//     url.Values.Get returns "", so presence must be tested with Has — a Get
//     test silently classified every tag write as a plain PutObject;
//   - an UploadPartCopy is a PUT with X-Amz-Copy-Source AND a partNumber, while
//     CopyObject carries the header without a partNumber.
func classify(r *http.Request) string {
	q := r.URL.Query()
	isCopy := r.Header.Get("X-Amz-Copy-Source") != ""
	hasKey := strings.Count(strings.Trim(r.URL.Path, "/"), "/") >= 1
	isPut := r.Method == http.MethodPut

	switch {
	case isPut && isCopy && q.Has("partNumber"):
		return kindUploadPartCopy
	case isPut && isCopy:
		return kindCopyObject
	case isPut && q.Has("tagging"):
		return kindPutTagging
	case r.Method == http.MethodGet && q.Has("tagging"):
		return kindGetTagging
	case r.Method == http.MethodPost && q.Has("uploads"):
		return kindCreateMPU
	case r.Method == http.MethodPost && q.Has("uploadId"):
		return kindCompleteMPU
	case isPut && hasKey:
		// A plain UploadPart (partNumber + uploadId, no copy header) is a data
		// write too: production only issues UploadPartCopy today, but if an
		// upload path is ever added an uncounted one would keep the idempotence
		// assertions green.
		return kindPutObject
	case r.Method == http.MethodDelete && hasKey:
		return kindDeleteObject
	case r.Method == http.MethodPost && q.Has("delete"):
		return kindDeleteObject
	default:
		return kindOther
	}
}

// Count returns how many requests of one kind have been seen.
func (c *countingTransport) Count(kind string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.counts[kind]
}

// Reset zeroes every tally, so a scenario can measure one sync in isolation.
func (c *countingTransport) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counts = map[string]int{}
}

// Total is the number of requests of every kind, for assertions that only care
// that the second sync was silent.
func (c *countingTransport) Total() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, v := range c.counts {
		n += v
	}
	return n
}

// Summary renders the tallies for a failure message, so a broken assertion says
// what traffic actually happened.
func (c *countingTransport) Summary() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var b strings.Builder
	for _, k := range []string{
		kindCopyObject, kindUploadPartCopy, kindPutObject, kindPutTagging,
		kindGetTagging, kindCreateMPU, kindCompleteMPU, kindOther,
	} {
		if v := c.counts[k]; v > 0 {
			b.WriteString(k)
			b.WriteString("=")
			b.WriteString(itoa(v))
			b.WriteString(" ")
		}
	}
	if b.Len() == 0 {
		return "(no requests)"
	}
	return strings.TrimSpace(b.String())
}

// itoa avoids pulling strconv in for three call sites.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
