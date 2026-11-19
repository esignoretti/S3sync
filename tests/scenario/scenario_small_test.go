// tests/scenario/scenario_small_test.go
//go:build integration

package scenario

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestScenarioEmptySource(t *testing.T) {
	e := newScenarioEnv(t, envOptions{})
	e.sync()
	e.assertTargetCount(0)
}

// TestScenarioTiny covers the two sizes a differ gets wrong. The keys must carry
// the "tiny" shape: sizeFor matches on "-<shape>." and falls back to "small"
// (1-64 KiB), so keys named "obj/zero.bin" would silently seed kilobyte objects
// and prove nothing about the 0- and 1-byte cases.
func TestScenarioTiny(t *testing.T) {
	e := newScenarioEnv(t, envOptions{})
	// The tiny shape is deterministic at 0 bytes, so the empty case is always the
	// empty case; the 1-byte neighbour is seeded explicitly.
	zero, one := synthKey(0, "tiny"), "obj/one-byte.bin"
	if got := e.sizeFor(zero); got != 0 {
		t.Fatalf("the zero-byte fixture is %d bytes, want 0", got)
	}
	e.seed([]string{zero})
	e.seedLiteral(one, "x")
	e.sync()
	e.assertTargetCount(2)
	e.assertBytesMatch(zero)
	e.assertBytesMatch(one)
}

func TestScenarioSingleObject(t *testing.T) {
	e := newScenarioEnv(t, envOptions{})
	e.seed([]string{"obj/0001-small.bin"})
	e.sync()
	e.assertBytesMatch("obj/0001-small.bin")
	e.assertNoWorkOnSecondSync()
}

func TestScenarioManySmall(t *testing.T) {
	if tier() == tierFast {
		t.Skip("1,200 objects; set SCENARIO_TIER=full")
	}
	if testing.Short() {
		t.Skip("1,200 objects")
	}
	// 1,200 crosses maxS3Keys (1000, internal/sync/differ.go) so the paginated
	// listing path runs, without the ~10 minutes 5,000 objects would cost at
	// the measured 117ms/object.
	e := newScenarioEnv(t, envOptions{WorkerCount: 32})
	keys := manyKeys(1200)
	e.seed(keys)
	e.sync()
	e.assertTargetCount(1200)
	for _, k := range []string{keys[0], keys[617], keys[1199]} {
		e.assertBytesMatch(k)
	}
	e.assertNoWorkOnSecondSync()
}

func TestScenarioManySmallWithDeletes(t *testing.T) {
	if tier() == tierFast {
		t.Skip("120 objects; set SCENARIO_TIER=full")
	}
	e := newScenarioEnv(t, envOptions{})
	e.Pair.DeletePropagation = true
	keys := manyKeys(120)
	e.seed(keys)
	e.sync()
	deleteFromSource(t, e.Client, e.Source, keys[10:20])
	e.sync()
	e.assertTargetCount(110)
	e.assertNoWorkOnSecondSync()
}

func TestScenarioSourceChanges(t *testing.T) {
	e := newScenarioEnv(t, envOptions{})
	keys := manyKeys(60)
	e.seed(keys)
	e.sync()
	for _, k := range keys[:10] {
		putBody(t, e.Client, e.Source, k, "changed")
		e.setLiteral(k, "changed")
	}
	e.Counter.Reset()
	e.sync()
	// A lower bound, not equality: the transport counts ATTEMPTS, and the SDK
	// retries a copy whose response was lost, so a correct run can count 11.
	if got := e.Counter.Count(kindCopyObject); got < 10 {
		t.Fatalf("copied %d objects after changing 10, want 10 (%s)", got, e.Counter.Summary())
	}
}

func TestScenarioSourceDeletedWithPropagationOff(t *testing.T) {
	// Deleting from the source without delete propagation must leave the target
	// untouched and issue no copy. The vanished-mid-run race itself (a key
	// listed, then gone by the time it is HEADed) is covered by
	// TestCopyObject_SourceVanishedIsSkipped in internal/sync, because it cannot
	// be triggered deterministically from outside the engine: the differ only
	// emits actions for keys the listing returned.
	e := newScenarioEnv(t, envOptions{})
	e.seed([]string{"obj/keep.bin", "obj/gone.bin"})
	e.sync()
	e.assertTargetCount(2)

	deleteFromSource(t, e.Client, e.Source, []string{"obj/gone.bin"})
	e.Counter.Reset()
	e.sync()
	if copies := e.Counter.Count(kindCopyObject); copies != 0 {
		t.Fatalf("deleting from the source issued %d copies, want 0 (%s)", copies, e.Counter.Summary())
	}
	// The target keeps the object: propagation is off, so nothing is removed.
	e.assertTargetCount(2)
	if _, ok := e.cachedKey("obj/gone.bin"); !ok {
		t.Fatal("the cache dropped a key whose source vanished, so a later restore would not be detected")
	}
}

func TestScenarioSourceDeletesThenRestores(t *testing.T) {
	// A versioned source: delete the key (marker), then re-upload identical
	// bytes. ETag and LastModified both come back unchanged, so StreamingDiff
	// never re-emits a copy — the vanished-object path must not leave the key
	// permanently absent from the target.
	e := newScenarioEnv(t, envOptions{Versioning: true})
	e.Pair.DeletePropagation = true
	putBody(t, e.Client, e.Source, "flip.txt", "stable-body")
	e.setLiteral("flip.txt", "stable-body")
	e.sync()
	if keys := e.targetKeys(); len(keys) != 1 {
		t.Fatalf("target after first sync = %v, want [flip.txt]", keys)
	}
	// Delete every current version on the source, leaving only a marker, then
	// restore the identical body.
	versions, err := e.Client.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(e.Source), Prefix: aws.String("flip.txt"),
	})
	if err != nil {
		t.Fatalf("list source versions: %v", err)
	}
	for _, v := range versions.Versions {
		if _, err := e.Client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
			Bucket: aws.String(e.Source), Key: aws.String("flip.txt"), VersionId: v.VersionId,
		}); err != nil {
			t.Fatalf("delete source version: %v", err)
		}
	}
	putBody(t, e.Client, e.Source, "flip.txt", "stable-body")
	e.sync()
	// Whatever the intermediate state, the key must exist in the target at the end.
	if keys := e.targetKeys(); len(keys) != 1 || keys[0] != "flip.txt" {
		t.Fatalf("target after delete-then-restore = %v, want [flip.txt]", keys)
	}
}

func TestScenarioAwkwardKeys(t *testing.T) {
	// Review Focus 2: "a/b" and "a/bc" are distinct keys that a prefix-based
	// listing can confuse, and the rest break naive URL escaping.
	e := newScenarioEnv(t, envOptions{})
	keys := []string{
		"a/b", "a/bc", "with space.txt", "plus+sign.txt", "percent%20.txt",
		"unicode-é.txt", "deep/a/b/c/d.txt", "trailing-dot.",
	}
	e.seed(keys)
	e.sync()
	e.assertTargetCount(len(keys))
	for _, k := range keys {
		e.assertBytesMatch(k)
	}
	e.assertNoWorkOnSecondSync()
}
