// tests/api_test.go
package tests

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
	srv := api.NewServer(repo)
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
	srv := api.NewServer(repo)
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

func TestAPISyncPairCRUD(t *testing.T) {
	db, err := config.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	repo := config.NewRepository(db)
	srv := api.NewServer(repo)
	router := srv.Router()

	// Create two buckets first
	for _, name := range []string{"src", "tgt"} {
		body := `{"name":"` + name + `","endpoint":"e","region":"r","access_key":"a","secret_key":"s","bucket_name":"b"}`
		w := httptest.NewRecorder()
		req, _ := http.NewRequest("POST", "/api/buckets", jsonBody(body))
		req.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(w, req)
		if w.Code != 201 {
			t.Fatalf("create bucket %s: %d", name, w.Code)
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
