package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/esignoretti/S3sync/internal/config"
	"github.com/gin-gonic/gin"
)

func init() {
	gin.SetMode(gin.TestMode)
}

// newTestRouter builds a Server over an in-memory SQLite repository and returns
// the router plus the repository, so tests can assert on what was PERSISTED
// rather than only on what the handler echoed back.
func newTestRouter(t *testing.T) (http.Handler, *config.Repository) {
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
	return srv.Router(), repo
}

func doJSON(t *testing.T, r http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	var req *http.Request
	if body == "" {
		req = httptest.NewRequest(method, path, nil)
	} else {
		req = httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func createBucketViaAPI(t *testing.T, r http.Handler, body string) string {
	t.Helper()
	w := doJSON(t, r, http.MethodPost, "/api/buckets", body)
	if w.Code != http.StatusCreated {
		t.Fatalf("create bucket: status %d: %s", w.Code, w.Body.String())
	}
	var resp struct {
		Data config.Bucket `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Data.ID == "" {
		t.Fatal("create bucket returned no ID")
	}
	return resp.Data.ID
}

// A bucket created through the REST API must have its credentials stored. The
// storage struct tags them json:"-" so they are excluded from BOTH marshal and
// unmarshal, which means binding straight into config.Bucket silently drops them.
func TestCreateBucketPersistsCredentials(t *testing.T) {
	r, repo := newTestRouter(t)

	id := createBucketViaAPI(t, r,
		`{"name":"b1","endpoint":"https://s3.example.com","region":"eu-west-1","bucket_name":"data","access_key":"AKIAEXAMPLE","secret_key":"supersecret"}`)

	stored, err := repo.GetBucket(id)
	if err != nil {
		t.Fatal(err)
	}
	if stored.AccessKey != "AKIAEXAMPLE" {
		t.Errorf("stored AccessKey = %q, want %q", stored.AccessKey, "AKIAEXAMPLE")
	}
	if stored.SecretKey != "supersecret" {
		t.Errorf("stored SecretKey = %q, want %q", stored.SecretKey, "supersecret")
	}
}

// Omitting the secret on update means "leave it alone". A blind assignment
// re-encrypts an empty string and silently destroys working credentials,
// breaking every sync pair that references the bucket.
func TestUpdateBucketKeepsCredentialsWhenOmitted(t *testing.T) {
	r, repo := newTestRouter(t)

	id := createBucketViaAPI(t, r,
		`{"name":"b1","endpoint":"https://s3.example.com","region":"eu-west-1","bucket_name":"data","access_key":"AKIAEXAMPLE","secret_key":"supersecret"}`)

	// Rename only; no credentials in the payload.
	w := doJSON(t, r, http.MethodPut, "/api/buckets/"+id,
		`{"name":"b1-renamed","endpoint":"https://s3.example.com","region":"eu-west-1","bucket_name":"data"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("update bucket: status %d: %s", w.Code, w.Body.String())
	}

	stored, err := repo.GetBucket(id)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Name != "b1-renamed" {
		t.Errorf("stored Name = %q, want %q (update must still apply)", stored.Name, "b1-renamed")
	}
	if stored.AccessKey != "AKIAEXAMPLE" {
		t.Errorf("stored AccessKey after update = %q, want it unchanged (%q)", stored.AccessKey, "AKIAEXAMPLE")
	}
	if stored.SecretKey != "supersecret" {
		t.Errorf("stored SecretKey after update = %q, want it unchanged (%q)", stored.SecretKey, "supersecret")
	}
}

// Supplying a new secret must replace the stored one.
func TestUpdateBucketReplacesCredentialsWhenSupplied(t *testing.T) {
	r, repo := newTestRouter(t)

	id := createBucketViaAPI(t, r,
		`{"name":"b1","endpoint":"https://s3.example.com","region":"eu-west-1","bucket_name":"data","access_key":"AKIAEXAMPLE","secret_key":"supersecret"}`)

	w := doJSON(t, r, http.MethodPut, "/api/buckets/"+id,
		`{"name":"b1","endpoint":"https://s3.example.com","region":"eu-west-1","bucket_name":"data","access_key":"AKIANEW","secret_key":"brandnew"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("update bucket: status %d: %s", w.Code, w.Body.String())
	}

	stored, err := repo.GetBucket(id)
	if err != nil {
		t.Fatal(err)
	}
	if stored.AccessKey != "AKIANEW" || stored.SecretKey != "brandnew" {
		t.Errorf("stored creds = %q/%q, want AKIANEW/brandnew", stored.AccessKey, stored.SecretKey)
	}
}

// Credentials must never leave the server, on any bucket endpoint.
func TestBucketResponseOmitsCredentials(t *testing.T) {
	r, _ := newTestRouter(t)

	id := createBucketViaAPI(t, r,
		`{"name":"b1","endpoint":"https://s3.example.com","region":"eu-west-1","bucket_name":"data","access_key":"AKIAEXAMPLE","secret_key":"supersecret"}`)

	for _, tc := range []struct{ name, path string }{
		{"get", "/api/buckets/" + id},
		{"list", "/api/buckets"},
	} {
		w := doJSON(t, r, http.MethodGet, tc.path, "")
		if w.Code != http.StatusOK {
			t.Fatalf("%s: status %d", tc.name, w.Code)
		}
		body := w.Body.String()
		for _, leak := range []string{"AKIAEXAMPLE", "supersecret", "access_key", "secret_key"} {
			if strings.Contains(body, leak) {
				t.Errorf("%s response leaked %q: %s", tc.name, leak, body)
			}
		}
	}
}
