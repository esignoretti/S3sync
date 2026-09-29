package sync

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/esignoretti/S3sync/internal/config"
)

// countingListener counts accepted connections, which is how a test can tell
// whether the HTTP client reused its keep-alive connection.
type countingListener struct {
	net.Listener
	mu    sync.Mutex
	conns int
}

func (l *countingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err == nil {
		l.mu.Lock()
		l.conns++
		l.mu.Unlock()
	}
	return c, err
}

func (l *countingListener) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.conns
}

// Closing a response body without reading it discards the payload AND the
// keep-alive connection: net/http cannot return a connection with unread data
// to the idle pool. Webhooks fire on every sync, so an undrained body means one
// fresh TCP connection per delivery and a server-side write nobody consumes.
func TestSendWebhookDrainsResponseBody(t *testing.T) {
	// Comfortably above net/http's 256 KiB drain-on-close threshold, so an
	// implementation that does not drain cannot reuse the connection anyway.
	payload := strings.Repeat("x", 1<<20)

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, payload)
	}))
	listener := &countingListener{Listener: srv.Listener}
	srv.Listener = listener
	srv.Start()
	defer srv.Close()

	for i := 0; i < 2; i++ {
		SendWebhook(srv.URL, "all", WebhookPayload{
			Event: "sync_completed", PairID: "p1", Status: "ok",
		})
	}

	if got := listener.count(); got != 1 {
		t.Errorf("connections accepted = %d, want 1 (the response body must be drained so the connection is reused)", got)
	}
}

// The webhook must report the error count as of the run it describes. The
// engine's in-memory pair is never updated by a run — RunOneShot writes the new
// count to the stored pair afterwards — so reading e.pair.ConsecutiveErrors
// reports the count from BEFORE this failure and always lags by one.
func TestWebhookReportsPostRunErrorCount(t *testing.T) {
	received := make(chan WebhookPayload, 1)
	hook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var p WebhookPayload
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
			t.Errorf("decode webhook payload: %v", err)
		}
		select {
		case received <- p:
		default:
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer hook.Close()

	// Every copy fails, so the run ends in "error".
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		switch r.Method {
		case http.MethodHead:
			if !objectPath(r.URL.Path) {
				w.WriteHeader(http.StatusOK) // bucket exists
				return
			}
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, notFoundBody)
		case http.MethodGet:
			fmt.Fprint(w, oneObjectListing("fails.txt", `"e1"`))
		default:
			if !objectPath(r.URL.Path) {
				w.WriteHeader(http.StatusOK) // CreateBucket
				return
			}
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprint(w, `<Error><Code>InternalError</Code></Error>`)
		}
	}))
	defer srv.Close()

	store := newTestStore(t)
	pair := &config.SyncPair{
		ID: "p1", Name: "t", WorkerCount: 1,
		WebhookURL: hook.URL, WebhookEvents: "all",
		ConsecutiveErrors: 2, // two earlier failures already on record
	}
	eng := newTestEngine(t, srv.URL, store, pair)

	if err := eng.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	select {
	case p := <-received:
		if p.Status != "error" {
			t.Fatalf("webhook status = %q, want %q", p.Status, "error")
		}
		if p.ConsecutiveErrors != 3 {
			t.Errorf("webhook ConsecutiveErrors = %d, want 3 (2 already recorded plus this failed run)", p.ConsecutiveErrors)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("webhook was not delivered")
	}
}
