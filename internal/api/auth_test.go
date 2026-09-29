package api

import (
	"net/http"
	"testing"
)

// withAuth turns on the API-key gate for the duration of a test and restores
// the previous package state, since authEnabled/apiKey are process globals set
// from the environment in init().
func withAuth(t *testing.T, key string) {
	t.Helper()
	prevEnabled, prevKey := authEnabled, apiKey
	authEnabled = true
	apiKey = key
	t.Cleanup(func() { authEnabled, apiKey = prevEnabled, prevKey })
}

// The container healthcheck runs without credentials. If /api/health is gated
// when S3SYNC_API_KEY is set, the probe gets 401, the container is marked
// unhealthy forever, and restart:unless-stopped turns that into a restart loop.
func TestHealthEndpointSkipsAuth(t *testing.T) {
	withAuth(t, "secret-key")
	r, _ := newTestRouter(t)

	w := doJSON(t, r, http.MethodGet, "/api/health", "")
	if w.Code == http.StatusUnauthorized {
		t.Fatalf("GET /api/health returned 401 while auth is enabled: %s", w.Body.String())
	}
	if w.Code != http.StatusOK {
		t.Fatalf("GET /api/health status = %d, want 200", w.Code)
	}
}

// Guards the test above against passing for the wrong reason: the middleware
// must still reject unauthenticated requests everywhere else.
func TestProtectedEndpointStillRequiresAuth(t *testing.T) {
	withAuth(t, "secret-key")
	r, _ := newTestRouter(t)

	w := doJSON(t, r, http.MethodGet, "/api/buckets", "")
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("GET /api/buckets status = %d, want 401 (auth must still be enforced)", w.Code)
	}
}
