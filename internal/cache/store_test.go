package cache

import (
	"os"
	"testing"
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
