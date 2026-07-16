// internal/config/repo_test.go
package config

import "testing"

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
