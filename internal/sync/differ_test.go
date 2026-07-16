package sync

import (
	"testing"
	"time"
)

func TestDiffNew(t *testing.T) {
	listing := []ListedObject{{Key: "a.jpg", ETag: `"1"`}}
	result := Diff(listing, nil, false)
	if len(result.NewOrChanged) != 1 {
		t.Fatalf("expected 1 new, got %d", len(result.NewOrChanged))
	}
}

func TestDiffUnchanged(t *testing.T) {
	tm := time.Now()
	listing := []ListedObject{{Key: "a.jpg", ETag: `"1"`, LastModified: tm}}
	cached := []cachedEntry{{Key: "a.jpg", ETag: `"1"`, LastModified: tm}}
	result := Diff(listing, cached, false)
	if result.Skipped != 1 {
		t.Fatalf("expected 1 skipped, got %d", result.Skipped)
	}
}

func TestDiffChanged(t *testing.T) {
	listing := []ListedObject{{Key: "a.jpg", ETag: `"2"`}}
	cached := []cachedEntry{{Key: "a.jpg", ETag: `"1"`}}
	result := Diff(listing, cached, false)
	if len(result.NewOrChanged) != 1 {
		t.Fatalf("expected 1 changed, got %d", len(result.NewOrChanged))
	}
}

func TestDiffDelete(t *testing.T) {
	listing := []ListedObject{{Key: "a.jpg"}}
	cached := []cachedEntry{{Key: "a.jpg"}, {Key: "b.jpg"}}
	result := Diff(listing, cached, true)
	if len(result.ToDelete) != 1 || result.ToDelete[0].Key != "b.jpg" {
		t.Fatal("expected b.jpg to delete")
	}
}

func TestDiffNoDeleteWhenDisabled(t *testing.T) {
	listing := []ListedObject{{Key: "a.jpg"}}
	cached := []cachedEntry{{Key: "a.jpg"}, {Key: "b.jpg"}}
	result := Diff(listing, cached, false)
	if len(result.ToDelete) != 0 {
		t.Fatal("expected no deletes")
	}
}
