package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"

	"github.com/esignoretti/S3sync/internal/config"
)

// newTestServer builds a Server over an in-memory SQLite repository and hands
// back both the server and the repository, so tests can drive the server's own
// methods (not just its routes) and assert on what was persisted.
func newTestServer(t *testing.T) (*Server, *config.Repository) {
	t.Helper()
	db, err := config.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	repo := config.NewRepository(db)
	srv, err := NewServer(repo, filepath.Join(t.TempDir(), "cache.db"))
	if err != nil {
		t.Fatal(err)
	}
	return srv, repo
}

// makeBucket creates a bucket through the repository and returns its ID.
// Sync pairs now carry real foreign keys, so pairs in tests need real buckets.
func makeBucket(t *testing.T, repo *config.Repository, name string) string {
	t.Helper()
	b := &config.Bucket{
		Name:       name,
		Endpoint:   "https://s3.example.com",
		Region:     "eu-west-1",
		BucketName: "data",
	}
	if err := repo.CreateBucket(b); err != nil {
		t.Fatal(err)
	}
	return b.ID
}

// A pair found with no status but a nonzero error count is one that was running
// when the process died. Recording it as "ok" claims a run that never finished
// and hides the crash from the dashboard; the honest value is "interrupted".
func TestRecoverCrashedPairsMarksInterrupted(t *testing.T) {
	srv, repo := newTestServer(t)
	src := makeBucket(t, repo, "src")
	tgt := makeBucket(t, repo, "tgt")

	cases := []struct {
		name   string
		status string
		errors int
		want   string
		why    string
	}{
		{
			name: "crashed mid-run", status: "", errors: 3, want: "interrupted",
			why: "no status plus accumulated errors means the run never completed",
		},
		{
			name: "healthy", status: "ok", errors: 0, want: "ok",
			why: "a pair with a recorded status must be left alone",
		},
		{
			name: "failed but recorded", status: "error", errors: 5, want: "error",
			why: "a real recorded status is not a crash and must be preserved",
		},
	}

	ids := make(map[string]string, len(cases))
	for i, tc := range cases {
		p := &config.SyncPair{
			Name:              tc.name,
			SourceBucketID:    src,
			TargetBucketID:    tgt,
			Enabled:           true,
			LastSyncStatus:    tc.status,
			ConsecutiveErrors: tc.errors,
			WorkerCount:       1,
		}
		if err := repo.CreateSyncPair(p); err != nil {
			t.Fatalf("case %d (%s): %v", i, tc.name, err)
		}
		ids[tc.name] = p.ID
	}

	// Router() runs crash recovery as part of wiring the routes.
	_ = srv.Router()

	for _, tc := range cases {
		got, err := repo.GetSyncPair(ids[tc.name])
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if got.LastSyncStatus != tc.want {
			t.Errorf("%s: LastSyncStatus = %q, want %q (%s)", tc.name, got.LastSyncStatus, tc.want, tc.why)
		}
	}
}

// Session cookies must carry SameSite. Without it a browser will attach the
// session cookie to cross-site requests, which is exactly the CSRF exposure
// the HttpOnly flag does not cover.
func TestAuthCookiesSetSameSite(t *testing.T) {
	withAuth(t, "secret-key")
	r, _ := newTestRouter(t)

	form := url.Values{"password": {"secret-key"}}
	req := httptest.NewRequest(http.MethodPost, "/api/auth/login", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusFound {
		t.Fatalf("login status = %d, want 302: %s", w.Code, w.Body.String())
	}

	var found bool
	for _, c := range w.Result().Cookies() {
		if c.Name != "s3sync_token" {
			continue
		}
		found = true
		if c.Value == "" {
			t.Fatalf("login set an empty session cookie")
		}
		if c.SameSite != http.SameSiteLaxMode {
			t.Errorf("session cookie SameSite = %v, want Lax", c.SameSite)
		}
		if !c.HttpOnly {
			t.Error("session cookie is not HttpOnly")
		}
	}
	if !found {
		t.Fatal("login did not set a session cookie")
	}
}

// The reported version must come from the build, not from a literal in the
// handler. A hardcoded string silently misreports every release after the one
// it was written for, which is worse than reporting nothing.
func TestVersionEndpointReportsBuildVersion(t *testing.T) {
	srv, _ := newTestServer(t)
	srv.SetVersion("1.2.3")

	w := doJSON(t, srv.Router(), http.MethodGet, "/api/version", "")
	if w.Code != http.StatusOK {
		t.Fatalf("GET /api/version status = %d, want 200", w.Code)
	}
	var resp struct {
		Data struct {
			Version string `json:"version"`
		} `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode %s: %v", w.Body.String(), err)
	}
	if resp.Data.Version != "1.2.3" {
		t.Errorf("/api/version reported %q, want %q — the handler must report the build version, not a literal",
			resp.Data.Version, "1.2.3")
	}
}

// With no -ldflags injection the server must still report something honest.
func TestVersionDefaultsToDev(t *testing.T) {
	srv, _ := newTestServer(t)
	if got := srv.Version(); got != "dev" {
		t.Errorf("Server.Version() = %q, want %q when no build version is injected", got, "dev")
	}
}
