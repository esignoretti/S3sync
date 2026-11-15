// tests/api_test.go
package tests

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

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
