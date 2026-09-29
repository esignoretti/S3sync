package config

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func setupDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestMigrate(t *testing.T) {
	db := setupDB(t)
	rows, err := db.Query("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var tables []string
	for rows.Next() {
		var n string
		rows.Scan(&n)
		tables = append(tables, n)
	}
	if len(tables) < 2 {
		t.Fatalf("expected >=2 tables, got %d", len(tables))
	}
}

// The schema declares REFERENCES on sync_pairs, but SQLite only enforces those
// constraints when the foreign_keys pragma is on — and it is off by default.
// Without it a pair can be written against a bucket id that does not exist, and
// every later run fails on a bucket lookup with nothing pointing at the cause.
func TestForeignKeysEnforced(t *testing.T) {
	db := setupDB(t)
	r := NewRepository(db)

	src := &Bucket{Name: "fk-src", Endpoint: "e", Region: "r", AccessKey: "a", SecretKey: "s", BucketName: "b"}
	if err := r.CreateBucket(src); err != nil {
		t.Fatal(err)
	}

	p := &SyncPair{
		Name: "orphan", SourceBucketID: src.ID,
		TargetBucketID: "no-such-bucket-id",
		SyncInterval:   300, WorkerCount: 1, Enabled: true,
	}
	if err := r.CreateSyncPair(p); err == nil {
		t.Fatal("CreateSyncPair accepted a nonexistent target bucket; foreign keys are not enforced")
	}
}

// _pragma in the DSN is applied per connection, and database/sql pools
// connections. With a file-backed database several connections really can be
// open at once, so assert the pragma is on for each of them — a pool handing out
// an unenforced connection would silently allow orphan rows.
func TestForeignKeysEnforcedAcrossPooledConnections(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "fk.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	db.SetMaxOpenConns(4)

	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var on int
			if err := db.QueryRow(`PRAGMA foreign_keys`).Scan(&on); err != nil {
				errs <- err
				return
			}
			if on != 1 {
				errs <- fmt.Errorf("PRAGMA foreign_keys = %d on a pooled connection, want 1", on)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// Deleting a bucket that a sync pair still points at must not be allowed: the
// pair would be left dangling and every later run would fail on a bucket lookup,
// with nothing pointing back at the delete that caused it.
func TestDeleteBucketRejectedWhenReferenced(t *testing.T) {
	db := setupDB(t)
	r := NewRepository(db)

	src := &Bucket{Name: "del-src", Endpoint: "e", Region: "r", AccessKey: "a", SecretKey: "s", BucketName: "b"}
	tgt := &Bucket{Name: "del-tgt", Endpoint: "e", Region: "r", AccessKey: "a", SecretKey: "s", BucketName: "b"}
	if err := r.CreateBucket(src); err != nil {
		t.Fatal(err)
	}
	if err := r.CreateBucket(tgt); err != nil {
		t.Fatal(err)
	}
	p := &SyncPair{
		Name: "referencing-pair", SourceBucketID: src.ID, TargetBucketID: tgt.ID,
		SyncInterval: 300, WorkerCount: 1, Enabled: true,
	}
	if err := r.CreateSyncPair(p); err != nil {
		t.Fatal(err)
	}

	err := r.DeleteBucket(src.ID)
	if err == nil {
		t.Fatal("DeleteBucket removed a bucket still referenced by a sync pair")
	}
	if !strings.Contains(err.Error(), "del-src") {
		t.Errorf("error %q does not name the bucket", err)
	}

	// The bucket must survive the refused delete.
	if _, err := r.GetBucket(src.ID); err != nil {
		t.Errorf("bucket vanished despite the refused delete: %v", err)
	}

	// Once the pair is gone, the same delete must succeed.
	if err := r.DeleteSyncPair(p.ID); err != nil {
		t.Fatalf("DeleteSyncPair: %v", err)
	}
	if err := r.DeleteBucket(src.ID); err != nil {
		t.Errorf("DeleteBucket after removing the pair: %v", err)
	}
}

// sync_logs.pair_id references sync_pairs(id), so once foreign keys are enforced
// a pair with logs could no longer be deleted unless its logs go with it. The
// logs of a deleted pair are meaningless, so deleting the pair must remove them.
func TestDeleteSyncPairRemovesItsLogs(t *testing.T) {
	db := setupDB(t)
	r := NewRepository(db)

	src := &Bucket{Name: "log-src", Endpoint: "e", Region: "r", AccessKey: "a", SecretKey: "s", BucketName: "b"}
	tgt := &Bucket{Name: "log-tgt", Endpoint: "e", Region: "r", AccessKey: "a", SecretKey: "s", BucketName: "b"}
	if err := r.CreateBucket(src); err != nil {
		t.Fatal(err)
	}
	if err := r.CreateBucket(tgt); err != nil {
		t.Fatal(err)
	}
	p := &SyncPair{
		Name: "logged-pair", SourceBucketID: src.ID, TargetBucketID: tgt.ID,
		SyncInterval: 300, WorkerCount: 1, Enabled: true,
	}
	if err := r.CreateSyncPair(p); err != nil {
		t.Fatal(err)
	}
	if err := r.CreateSyncLog(&SyncLogEntry{
		PairID: p.ID, Status: "ok", Succeeded: 1,
		StartedAt: time.Now().UTC(), CompletedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}

	if err := r.DeleteSyncPair(p.ID); err != nil {
		t.Fatalf("DeleteSyncPair with existing logs: %v", err)
	}
	logs, err := r.ListSyncLogs(p.ID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) != 0 {
		t.Errorf("%d logs survived the pair delete, want 0", len(logs))
	}
}
