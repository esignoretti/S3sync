package s3client

import (
	"net/http"
	"testing"

	"github.com/esignoretti/S3sync/internal/config"
)

// clientTransport returns the *http.Transport the S3 client actually uses, so
// the pool settings can be asserted on rather than assumed.
func clientTransport(t *testing.T) *http.Transport {
	t.Helper()
	b := &config.Bucket{
		Endpoint: "https://s3.amazonaws.com", Region: "us-east-1",
		AccessKey: "test", SecretKey: "test",
	}
	c, err := NewClient(b)
	if err != nil {
		t.Fatal(err)
	}
	hc, ok := c.Options().HTTPClient.(*http.Client)
	if !ok {
		t.Fatalf("HTTPClient type = %T, want *http.Client", c.Options().HTTPClient)
	}
	tr, ok := hc.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("Transport type = %T, want *http.Transport", hc.Transport)
	}
	return tr
}

// Go's default is 2 idle connections per host. With worker_count defaulting to
// 10 and the README suggesting 20, every operation beyond the second must open a
// fresh TCP+TLS connection, which adds latency to each copy and feeds the retry
// path. The pool must be sized for the worker counts the tool advertises.
func TestTransportHasConnectionPoolLimits(t *testing.T) {
	tr := clientTransport(t)

	if tr.MaxIdleConnsPerHost < 100 {
		t.Errorf("MaxIdleConnsPerHost = %d, want >= 100 (Go default is 2)", tr.MaxIdleConnsPerHost)
	}
	if tr.MaxIdleConns < 100 {
		t.Errorf("MaxIdleConns = %d, want >= 100", tr.MaxIdleConns)
	}
	if tr.IdleConnTimeout == 0 {
		t.Error("IdleConnTimeout = 0, want a bounded idle timeout so dead connections are reaped")
	}
	if tr.TLSHandshakeTimeout == 0 {
		t.Error("TLSHandshakeTimeout = 0, want a bounded handshake timeout")
	}
}
