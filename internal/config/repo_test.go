// internal/config/repo_test.go
package config

import (
	"strings"
	"testing"
)

func TestBucketCRUD(t *testing.T) {
	db := setupDB(t)
	r := NewRepository(db)

	b := &Bucket{
		Name: "test", Endpoint: "https://s3.amazonaws.com",
		Region: "us-east-1", AccessKey: "AKID", SecretKey: "secret",
		BucketName: "my-bucket",
	}
	if err := r.CreateBucket(b); err != nil {
		t.Fatal(err)
	}
	if b.ID == "" {
		t.Fatal("expected ID")
	}

	got, _ := r.GetBucket(b.ID)
	if got.Name != "test" {
		t.Fatalf("got %s", got.Name)
	}

	list, _ := r.ListBuckets()
	if len(list) != 1 {
		t.Fatalf("got %d", len(list))
	}

	b.Name = "updated"
	r.UpdateBucket(b)
	got, _ = r.GetBucket(b.ID)
	if got.Name != "updated" {
		t.Fatalf("got %s", got.Name)
	}

	r.DeleteBucket(b.ID)
	list, _ = r.ListBuckets()
	if len(list) != 0 {
		t.Fatalf("got %d", len(list))
	}
}

func TestSyncPairCRUD(t *testing.T) {
	db := setupDB(t)
	r := NewRepository(db)
	src := &Bucket{Name: "src", Endpoint: "e", Region: "r", AccessKey: "a", SecretKey: "s", BucketName: "b"}
	tgt := &Bucket{Name: "tgt", Endpoint: "e", Region: "r", AccessKey: "a", SecretKey: "s", BucketName: "b"}
	r.CreateBucket(src)
	r.CreateBucket(tgt)

	p := &SyncPair{
		Name: "pair", SourceBucketID: src.ID, TargetBucketID: tgt.ID,
		SyncInterval: 300, WorkerCount: 5, Enabled: true,
	}
	if err := r.CreateSyncPair(p); err != nil {
		t.Fatal(err)
	}

	got, _ := r.GetSyncPair(p.ID)
	if got.Name != "pair" {
		t.Fatalf("got %s", got.Name)
	}

	list, _ := r.ListSyncPairs()
	if len(list) != 1 {
		t.Fatalf("got %d", len(list))
	}
}

// A stored credential that cannot be decrypted means the master key changed
// since it was saved. Warning and carrying on handed back the hex ciphertext as
// if it were an access key, so every later S3 call failed with an auth error
// far from the real cause.
func TestDecryptCredsFailsOnWrongKey(t *testing.T) {
	db := setupDB(t)

	t.Setenv(MasterKeyEnv, "master-key-A")
	repoA := NewRepository(db)
	b := &Bucket{
		Name: "wrongkey-bucket", Endpoint: "https://s3.amazonaws.com",
		Region: "us-east-1", AccessKey: "AKIDEXAMPLE", SecretKey: "shh",
		BucketName: "my-bucket",
	}
	if err := repoA.CreateBucket(b); err != nil {
		t.Fatal(err)
	}

	// Guard against a vacuous test: the credential must really be encrypted.
	var storedAK string
	if err := db.QueryRow(`SELECT access_key FROM buckets WHERE id = ?`, b.ID).Scan(&storedAK); err != nil {
		t.Fatal(err)
	}
	if storedAK == "AKIDEXAMPLE" {
		t.Fatal("access key was stored in plaintext; the test would not exercise decryption")
	}

	// Same database, different master key: decryption must now fail loudly.
	t.Setenv(MasterKeyEnv, "master-key-B")
	repoB := NewRepository(db)

	got, err := repoB.GetBucket(b.ID)
	if err == nil {
		t.Fatalf("GetBucket with the wrong master key returned %+v, want an error", got)
	}
	if !strings.Contains(err.Error(), "wrongkey-bucket") {
		t.Errorf("error %q does not name the bucket", err)
	}
	if !strings.Contains(err.Error(), MasterKeyEnv) {
		t.Errorf("error %q does not name %s, the thing the operator must check", err, MasterKeyEnv)
	}
	if got != nil && got.AccessKey == storedAK {
		t.Errorf("AccessKey still holds the ciphertext %q", storedAK)
	}
}

// ListBuckets must surface the same failure rather than returning a slice whose
// credentials are unusable.
func TestListBucketsPropagatesDecryptFailure(t *testing.T) {
	db := setupDB(t)

	t.Setenv(MasterKeyEnv, "master-key-A")
	repoA := NewRepository(db)
	if err := repoA.CreateBucket(&Bucket{
		Name: "listbucket-bucket", Endpoint: "e", Region: "r",
		AccessKey: "AKID", SecretKey: "s", BucketName: "b",
	}); err != nil {
		t.Fatal(err)
	}

	t.Setenv(MasterKeyEnv, "master-key-B")
	repoB := NewRepository(db)

	buckets, err := repoB.ListBuckets()
	if err == nil {
		t.Fatalf("ListBuckets with the wrong master key returned %d buckets, want an error", len(buckets))
	}
	if !strings.Contains(err.Error(), "listbucket-bucket") {
		t.Errorf("error %q does not name the bucket", err)
	}
}

// With no master key configured, credentials are stored as plaintext by design
// and must keep coming back untouched.
func TestDecryptCredsPlaintextPathUnaffected(t *testing.T) {
	db := setupDB(t)
	t.Setenv(MasterKeyEnv, "")
	repo := NewRepository(db)

	b := &Bucket{Name: "plain", Endpoint: "e", Region: "r", AccessKey: "AKID", SecretKey: "s", BucketName: "b"}
	if err := repo.CreateBucket(b); err != nil {
		t.Fatal(err)
	}

	got, err := repo.GetBucket(b.ID)
	if err != nil {
		t.Fatalf("GetBucket without a master key: %v", err)
	}
	if got.AccessKey != "AKID" || got.SecretKey != "s" {
		t.Errorf("credentials = %q/%q, want AKID/s", got.AccessKey, got.SecretKey)
	}
}
