// internal/config/repo_test.go
package config

import (
	"testing"
	"time"
)

func testRepo(t *testing.T) (*Repository, func()) {
	t.Helper()
	db := setupDB(t)
	r := NewRepository(db)
	return r, func() {}
}

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

func TestSyncPairRoundTripsNewFields(t *testing.T) {
	repo, cleanup := testRepo(t)
	defer cleanup()

	exp := time.Date(2027, 3, 1, 0, 0, 0, 0, time.UTC)
	last := time.Date(2026, 11, 15, 4, 5, 6, 0, time.UTC)
	p := &SyncPair{
		Name: "p1", SourceBucketID: "s", TargetBucketID: "t",
		SyncInterval: 300, WorkerCount: 4, Enabled: true,
		ReplicateAttributes: true, TargetExpireAt: &exp,
		NoncurrentExpireDays: 7, NewerNoncurrentVersions: 2,
		RequireObjectLock: true, OnLockMissing: "recreate",
		AttrsReconcileInterval: 3600, ProvisionedAt: &last,
		ProvisionError: "boom", LastReconcileAt: &last,
	}
	if err := repo.CreateSyncPair(p); err != nil {
		t.Fatal(err)
	}
	got, err := repo.GetSyncPair(p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !got.ReplicateAttributes || !got.RequireObjectLock {
		t.Fatalf("bool fields lost: %+v", got)
	}
	if got.OnLockMissing != "recreate" || got.NoncurrentExpireDays != 7 ||
		got.NewerNoncurrentVersions != 2 || got.AttrsReconcileInterval != 3600 {
		t.Fatalf("scalar fields lost: %+v", got)
	}
	if got.TargetExpireAt == nil || !got.TargetExpireAt.Equal(exp) {
		t.Fatalf("TargetExpireAt = %v, want %v", got.TargetExpireAt, exp)
	}
	if got.ProvisionError != "boom" {
		t.Fatalf("ProvisionError = %q", got.ProvisionError)
	}

	// Update path must persist them too.
	got.ProvisionError = "fixed"
	got.OnLockMissing = "fail"
	if err := repo.UpdateSyncPair(got); err != nil {
		t.Fatal(err)
	}
	again, err := repo.GetSyncPair(p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if again.ProvisionError != "fixed" || again.OnLockMissing != "fail" {
		t.Fatalf("update lost fields: %+v", again)
	}
}

func TestSyncPairDefaultsForLegacyRows(t *testing.T) {
	repo, cleanup := testRepo(t)
	defer cleanup()

	p := &SyncPair{Name: "old", SourceBucketID: "s", TargetBucketID: "t",
		SyncInterval: 300, WorkerCount: 4, Enabled: true}
	if err := repo.CreateSyncPair(p); err != nil {
		t.Fatal(err)
	}
	got, err := repo.GetSyncPair(p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ReplicateAttributes || got.RequireObjectLock || got.AttrsReconcileInterval != 0 {
		t.Fatalf("new fields must default off: %+v", got)
	}
	if got.OnLockMissing != "fail" {
		t.Fatalf("OnLockMissing default = %q, want fail", got.OnLockMissing)
	}
	if got.TargetExpireAt != nil || got.ProvisionedAt != nil || got.LastReconcileAt != nil {
		t.Fatalf("nullable fields must default nil: %+v", got)
	}
}
