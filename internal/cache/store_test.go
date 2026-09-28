package cache

import (
	"os"
	"testing"

	"go.etcd.io/bbolt"
)

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

	list, _ := s.List("p1")
	if len(list) != 1 || list[0].ETag != `"abc"` {
		t.Fatalf("expected 1 object with etag abc, got %+v", list)
	}

	s.Delete("p1", "a.jpg")
	list, _ = s.List("p1")
	if len(list) != 0 {
		t.Fatal("expected empty after delete")
	}

	s.Put(&CachedObject{PairID: "p1", Key: "x.jpg"})
	s.Put(&CachedObject{PairID: "p1", Key: "y.jpg"})
	s.Clear("p1")
	list, _ = s.List("p1")
	if len(list) != 0 {
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
	list, err := s.List("p1")
	if err != nil {
		t.Fatal(err)
	}
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
	list, err := s.List("p1")
	if err != nil {
		t.Fatal(err)
	}
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
