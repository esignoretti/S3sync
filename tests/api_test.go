// tests/api_test.go
package tests

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/esignoretti/S3sync/internal/api"
	"github.com/esignoretti/S3sync/internal/config"
)

func jsonBody(s string) *strings.Reader {
	return strings.NewReader(s)
}

func TestAPIHealth(t *testing.T) {
	db, err := config.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	repo := config.NewRepository(db)
	srv, err := api.NewServer(repo, filepath.Join(t.TempDir(), "cache.db"))
	if err != nil {
		t.Fatal(err)
	}
	router := srv.Router()

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/health", nil)
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp struct {
		Data struct {
			Status string `json:"status"`
		} `json:"data"`
	}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.Data.Status != "ok" {
		t.Fatalf("expected ok, got %s", resp.Data.Status)
	}
}

// TestAPIVersion pins the reported version so a release bump is verified rather
// than assumed. It asserts the value equals the api package constant, so the
// two can never drift; bumping the constant is the only edit a release needs.
func TestAPIVersion(t *testing.T) {
	db, err := config.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	srv, err := api.NewServer(config.NewRepository(db), filepath.Join(t.TempDir(), "cache.db"))
	if err != nil {
		t.Fatal(err)
	}
	router := srv.Router()

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/version", nil)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp struct {
		Data struct {
			Version string `json:"version"`
		} `json:"data"`
	}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.Data.Version != api.Version {
		t.Fatalf("reported version %q, want %q", resp.Data.Version, api.Version)
	}
	if resp.Data.Version == "" {
		t.Fatal("the version endpoint reported an empty version")
	}
}

func TestAPIBucketCRUD(t *testing.T) {
	db, err := config.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	repo := config.NewRepository(db)
	srv, err := api.NewServer(repo, filepath.Join(t.TempDir(), "cache.db"))
	if err != nil {
		t.Fatal(err)
	}
	router := srv.Router()

	// Create bucket
	body := `{"name":"test","endpoint":"https://s3.amazonaws.com","region":"us-east-1","access_key":"ak","secret_key":"sk","bucket_name":"b"}`
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/buckets", jsonBody(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	if w.Code != 201 {
		t.Fatalf("expected 201, got %d: %s", w.Code, w.Body.String())
	}

	var createResp struct {
		Data config.Bucket `json:"data"`
	}
	json.NewDecoder(w.Body).Decode(&createResp)
	bucketID := createResp.Data.ID
	if bucketID == "" {
		t.Fatal("expected bucket ID")
	}

	// List buckets
	w = httptest.NewRecorder()
	req, _ = http.NewRequest("GET", "/api/buckets", nil)
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var listResp struct {
		Data []config.Bucket `json:"data"`
	}
	json.NewDecoder(w.Body).Decode(&listResp)
	if len(listResp.Data) != 1 {
		t.Fatalf("expected 1 bucket, got %d", len(listResp.Data))
	}

	// Get bucket
	w = httptest.NewRecorder()
	req, _ = http.NewRequest("GET", "/api/buckets/"+bucketID, nil)
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	// Delete bucket
	w = httptest.NewRecorder()
	req, _ = http.NewRequest("DELETE", "/api/buckets/"+bucketID, nil)
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

// fakeTargetS3 stands in for the target endpoint so the pair preflight can run
// without a real bucket: every request succeeds, and the bucket already exists.
func fakeTargetS3() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
}

func TestAPISyncPairCRUD(t *testing.T) {
	db, err := config.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	endpoint := fakeTargetS3()
	defer endpoint.Close()

	repo := config.NewRepository(db)
	srv, err := api.NewServer(repo, filepath.Join(t.TempDir(), "cache.db"))
	if err != nil {
		t.Fatal(err)
	}
	router := srv.Router()

	// The buckets are inserted through the repository rather than the API: the
	// preflight now signs real requests against the target, and credentials are
	// deliberately not accepted over the wire (json:"-" on the model).
	for _, name := range []string{"src", "tgt"} {
		b := &config.Bucket{
			Name: name, Endpoint: endpoint.URL, Region: "us-east-1",
			AccessKey: "ak", SecretKey: "sk", BucketName: "b",
		}
		if err := repo.CreateBucket(b); err != nil {
			t.Fatalf("create bucket %s: %v", name, err)
		}
	}

	// Create sync pair
	var listResp struct {
		Data []config.Bucket `json:"data"`
	}
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest("GET", "/api/buckets", nil))
	json.NewDecoder(w.Body).Decode(&listResp)

	if len(listResp.Data) < 2 {
		t.Fatal("need 2 buckets")
	}

	pairBody := `{"name":"test-pair","source_bucket_id":"` + listResp.Data[0].ID + `","target_bucket_id":"` + listResp.Data[1].ID + `","sync_interval":300,"worker_count":5}`
	w = httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/sync-pairs", jsonBody(pairBody))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	if w.Code != 201 {
		t.Fatalf("expected 201, got %d: %s", w.Code, w.Body.String())
	}
}

// A pair whose target expiry is already past is refused before it is stored: the
// engine would refuse to run it anyway, and a stored pair that can never run is
// worse than a rejected request.
func TestCreatePairRejectsPastExpiry(t *testing.T) {
	db, err := config.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	endpoint := fakeTargetS3()
	defer endpoint.Close()

	repo := config.NewRepository(db)
	srv, err := api.NewServer(repo, filepath.Join(t.TempDir(), "cache.db"))
	if err != nil {
		t.Fatal(err)
	}
	router := srv.Router()

	for _, name := range []string{"src", "tgt"} {
		b := &config.Bucket{
			Name: name, Endpoint: endpoint.URL, Region: "us-east-1",
			AccessKey: "ak", SecretKey: "sk", BucketName: "b",
		}
		if err := repo.CreateBucket(b); err != nil {
			t.Fatalf("create bucket %s: %v", name, err)
		}
	}

	var listResp struct {
		Data []config.Bucket `json:"data"`
	}
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest("GET", "/api/buckets", nil))
	json.NewDecoder(w.Body).Decode(&listResp)
	if len(listResp.Data) < 2 {
		t.Fatal("need 2 buckets")
	}

	pairBody := `{"name":"expired-pair","source_bucket_id":"` + listResp.Data[0].ID +
		`","target_bucket_id":"` + listResp.Data[1].ID +
		`","sync_interval":300,"worker_count":5,"target_expire_at":"2020-01-01T00:00:00Z"}`
	w = httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/sync-pairs", jsonBody(pairBody))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422: %s", w.Code, w.Body.String())
	}
	var resp struct {
		Error string `json:"error"`
		Code  string `json:"code"`
	}
	json.NewDecoder(w.Body).Decode(&resp)
	if resp.Code != "preflight_failed" {
		t.Fatalf("code = %q, want preflight_failed", resp.Code)
	}

	// The pair must not be stored.
	var pairs struct {
		Data []config.SyncPair `json:"data"`
	}
	w = httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest("GET", "/api/sync-pairs", nil))
	json.NewDecoder(w.Body).Decode(&pairs)
	for _, p := range pairs.Data {
		if p.Name == "expired-pair" {
			t.Fatalf("rejected pair was persisted: %+v", p)
		}
	}
}

// An engine is built from a snapshot of the pair and reads target_expire_at,
// replicate_attributes and attrs_reconcile_interval from that snapshot on every
// run. A successful update therefore has to replace the running engine, or the
// documented recovery from an expired pair (moving the date into the future)
// reports success while the engine keeps refusing every run.
func TestUpdatePairRestartsRunningEngine(t *testing.T) {
	// A file DB, not :memory:: the engine loop runs on its own goroutine and
	// needs to see the same schema as the request handler.
	db, err := config.Open(filepath.Join(t.TempDir(), "config.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	endpoint := fakeTargetS3()
	defer endpoint.Close()

	repo := config.NewRepository(db)
	srv, err := api.NewServer(repo, filepath.Join(t.TempDir(), "cache.db"))
	if err != nil {
		t.Fatal(err)
	}
	router := srv.Router()

	for _, name := range []string{"src", "tgt"} {
		b := &config.Bucket{
			Name: name, Endpoint: endpoint.URL, Region: "us-east-1",
			AccessKey: "ak", SecretKey: "sk", BucketName: "b",
		}
		if err := repo.CreateBucket(b); err != nil {
			t.Fatalf("create bucket %s: %v", name, err)
		}
	}
	past := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	pair := &config.SyncPair{
		Name: "rearm", SourceBucketID: "", TargetBucketID: "",
		SyncInterval: 3600, WorkerCount: 1, Enabled: true, TargetExpireAt: &past,
	}
	var listResp struct {
		Data []config.Bucket `json:"data"`
	}
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest("GET", "/api/buckets", nil))
	json.NewDecoder(w.Body).Decode(&listResp)
	if len(listResp.Data) < 2 {
		t.Fatal("need 2 buckets")
	}
	pair.SourceBucketID = listResp.Data[0].ID
	pair.TargetBucketID = listResp.Data[1].ID
	if err := repo.CreateSyncPair(pair); err != nil {
		t.Fatal(err)
	}

	// Start an engine for the pair, as `serve` does at boot.
	if err := srv.StartEngineLoop(context.Background(), *pair); err != nil {
		t.Fatalf("start engine: %v", err)
	}
	before, ok := srv.GetEngine(pair.ID)
	if !ok {
		t.Fatal("no engine registered after StartEngineLoop")
	}

	// Move the expiry into the future through the API.
	body := `{"target_expire_at":"2027-06-01T00:00:00Z"}`
	w = httptest.NewRecorder()
	req := httptest.NewRequest("PUT", "/api/sync-pairs/"+pair.ID, jsonBody(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("update status = %d, want 200: %s", w.Code, w.Body.String())
	}

	after, ok := srv.GetEngine(pair.ID)
	if !ok {
		t.Fatal("update left no engine registered")
	}
	if after == before {
		t.Error("the engine was not replaced: it still holds the pre-update pair, " +
			"so the new expiry will not take effect until the process restarts")
	}
	stored, err := repo.GetSyncPair(pair.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.TargetExpireAt == nil || !stored.TargetExpireAt.After(time.Now()) {
		t.Fatalf("stored expiry = %v, want a future date", stored.TargetExpireAt)
	}
}

// The run timeout is a global setting read at the start of every run, so the API
// has to be able to change it without a restart. The range check is what keeps a
// typo from silently making every run time out immediately.
func TestSettingsRoundTrip(t *testing.T) {
	db, err := config.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	repo := config.NewRepository(db)
	srv, err := api.NewServer(repo, filepath.Join(t.TempDir(), "cache.db"))
	if err != nil {
		t.Fatal(err)
	}
	router := srv.Router()

	call := func(method, path, body string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		var r *http.Request
		if body == "" {
			r, _ = http.NewRequest(method, path, nil)
		} else {
			r, _ = http.NewRequest(method, path, strings.NewReader(body))
			r.Header.Set("Content-Type", "application/json")
		}
		router.ServeHTTP(w, r)
		return w
	}

	if w := call("GET", "/api/settings", ""); w.Code != 200 || !strings.Contains(w.Body.String(), "300") {
		t.Fatalf("default GET /api/settings = %d %s, want 200 with 300", w.Code, w.Body.String())
	}
	if w := call("PUT", "/api/settings", `{"run_timeout_seconds": 1800}`); w.Code != 200 {
		t.Fatalf("PUT /api/settings = %d, body %s", w.Code, w.Body.String())
	}
	if w := call("GET", "/api/settings", ""); !strings.Contains(w.Body.String(), "1800") {
		t.Fatalf("GET /api/settings = %s, want run_timeout_seconds 1800", w.Body.String())
	}
	if w := call("PUT", "/api/settings", `{"run_timeout_seconds": 5}`); w.Code != 422 ||
		!strings.Contains(w.Body.String(), "invalid_setting") {
		t.Fatalf("out-of-range PUT = %d %s, want 422 invalid_setting", w.Code, w.Body.String())
	}
	if w := call("PUT", "/api/settings", `{}`); w.Code != 400 {
		t.Fatalf("empty PUT = %d, want 400", w.Code)
	}
}
