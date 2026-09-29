package cache

import (
	"os"
	"testing"

	"go.etcd.io/bbolt"
)

// listObjects enumerates a pair's cache through the same cursor API the engine
// uses. Store.List was production-dead code kept alive only by tests; reading
// through NewCursor exercises the real path instead.
func listObjects(t *testing.T, s *Store, pairID string) []CachedObject {
	t.Helper()
	cur, closeCursor, err := s.NewCursor(pairID)
	if err != nil {
		t.Fatalf("NewCursor: %v", err)
	}
	defer closeCursor()
	var out []CachedObject
	for cur.Next() {
		out = append(out, cur.Object())
	}
	return out
}

func TestCRUD(t *testing.T) {
	f, _ := os.CreateTemp("", "bolt-test-*.db")
	defer os.Remove(f.Name())
	s, err := Open(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	obj := &CachedObject{PairID: "p1", Key: "a.jpg", ETag: `"abc"`, Size: 4096}
	if err := s.Put(obj); err != nil {
		t.Fatal(err)
	}

	list := listObjects(t, s, "p1")
	if len(list) != 1 || list[0].ETag != `"abc"` {
		t.Fatalf("expected 1 object with etag abc, got %+v", list)
	}

	if err := s.Delete("p1", "a.jpg"); err != nil {
		t.Fatal(err)
	}
	if list = listObjects(t, s, "p1"); len(list) != 0 {
		t.Fatal("expected empty after delete")
	}

	if err := s.Put(&CachedObject{PairID: "p1", Key: "x.jpg"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(&CachedObject{PairID: "p1", Key: "y.jpg"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Clear("p1"); err != nil {
		t.Fatal(err)
	}
	if list = listObjects(t, s, "p1"); len(list) != 0 {
		t.Fatalf("expected 0 after clear, got %d", len(list))
	}
}

func TestPutMany(t *testing.T) {
	db, err := bbolt.Open(t.TempDir()+"/test.db", 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := &Store{db: db}
	objs := []*CachedObject{
		{PairID: "p1", Key: "a", ETag: "e1"},
		{PairID: "p1", Key: "b", ETag: "e2"},
	}
	if err := s.PutMany(objs); err != nil {
		t.Fatal(err)
	}
	list := listObjects(t, s, "p1")
	if len(list) != 2 {
		t.Fatalf("expected 2, got %d", len(list))
	}
}

func TestPutMany_Empty(t *testing.T) {
	s := &Store{}
	if err := s.PutMany(nil); err != nil {
		t.Fatal(err)
	}
	if err := s.PutMany([]*CachedObject{}); err != nil {
		t.Fatal(err)
	}
}

func TestDeleteManyRemovesAllKeys(t *testing.T) {
	db, err := bbolt.Open(t.TempDir()+"/test.db", 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := &Store{db: db}
	if err := s.PutMany([]*CachedObject{
		{PairID: "p1", Key: "a", ETag: "e1"},
		{PairID: "p1", Key: "b", ETag: "e2"},
		{PairID: "p1", Key: "c", ETag: "e3"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteMany("p1", []string{"a", "c"}); err != nil {
		t.Fatal(err)
	}
	list := listObjects(t, s, "p1")
	if len(list) != 1 || list[0].Key != "b" {
		t.Fatalf("expected only b to remain, got %+v", list)
	}
}

func TestDeleteManyEmptyIsNoOp(t *testing.T) {
	db, err := bbolt.Open(t.TempDir()+"/test.db", 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := &Store{db: db}
	if err := s.DeleteMany("absent", nil); err != nil {
		t.Fatalf("nil keys: %v", err)
	}
	if err := s.DeleteMany("absent", []string{}); err != nil {
		t.Fatalf("empty keys: %v", err)
	}
	if err := s.DeleteMany("absent", []string{"x"}); err != nil {
		t.Fatalf("absent bucket: %v", err)
	}
}

// bucketExists reports whether a pair's cache bucket is present in the store.
func bucketExists(t *testing.T, s *Store, pairID string) bool {
	t.Helper()
	var found bool
	if err := s.db.View(func(tx *bbolt.Tx) error {
		found = tx.Bucket(s.bucket(pairID)) != nil
		return nil
	}); err != nil {
		t.Fatalf("View: %v", err)
	}
	return found
}

// Clear must leave the pair with a valid, empty cache bucket, including when
// nothing was ever cached. The original implementation returned early on
// ErrBucketNotFound and so never reached CreateBucket, leaving a pair that had
// not synced yet with no cache bucket at all.
func TestClearLeavesEmptyBucketOnAbsentPair(t *testing.T) {
	s, err := Open(t.TempDir() + "/clear-absent.db")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if err := s.Clear("never-synced"); err != nil {
		t.Fatalf("Clear on a pair with no cached objects: %v", err)
	}
	if !bucketExists(t, s, "never-synced") {
		t.Error("Clear left no cache bucket for the pair; want an empty bucket")
	}
	if got := listObjects(t, s, "never-synced"); len(got) != 0 {
		t.Errorf("cache after Clear = %+v, want empty", got)
	}

	// Clearing again must stay a no-op rather than failing or dropping the bucket.
	if err := s.Clear("never-synced"); err != nil {
		t.Fatalf("second Clear: %v", err)
	}
	if !bucketExists(t, s, "never-synced") {
		t.Error("second Clear removed the bucket")
	}
}

// A store that cannot run a transaction at all must not report a successful
// reset. This pins the real-world broken-store path; the error here comes from
// db.Update itself, before the transaction closure runs.
func TestClearOnClosedStoreReturnsError(t *testing.T) {
	s, err := Open(t.TempDir() + "/clear.db")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	if err := s.Clear("p1"); err == nil {
		t.Fatal("Clear on a closed store returned nil, want an error")
	}
}
