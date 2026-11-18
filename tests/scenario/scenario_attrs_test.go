// tests/scenario/scenario_attrs_test.go
//go:build integration

package scenario

import (
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/esignoretti/S3sync/internal/sync"
)

func TestScenarioMetadata(t *testing.T) {
	e := newScenarioEnv(t, envOptions{ReplicateAttrs: true})
	putBody(t, e.Client, e.Source, "doc.txt", "body", func(in *s3.PutObjectInput) {
		in.ContentType = aws.String("text/csv")
		in.ContentDisposition = aws.String(`attachment; filename="doc.csv"`)
		in.CacheControl = aws.String("max-age=3600")
		in.Metadata = map[string]string{"origin": "prod", "tier": "hot"}
	})
	e.sync()
	f := fetchFacts(t, e.Client, e.Target, "doc.txt")
	if f.ContentType != "text/csv" {
		t.Fatalf("content-type = %q, want text/csv", f.ContentType)
	}
	if f.Metadata["origin"] != "prod" || f.Metadata["tier"] != "hot" {
		t.Fatalf("metadata = %v, want origin=prod tier=hot", f.Metadata)
	}
}

func TestScenarioTags(t *testing.T) {
	e := newScenarioEnv(t, envOptions{ReplicateAttrs: true})
	putBody(t, e.Client, e.Source, "doc.txt", "body")
	putTags(t, e.Client, e.Source, "doc.txt", map[string]string{"team": "alpha", "env": "prod"})
	e.sync()
	if f := fetchFacts(t, e.Client, e.Target, "doc.txt"); f.Tags["team"] != "alpha" || f.Tags["env"] != "prod" {
		t.Fatalf("target tags = %v, want team=alpha env=prod", f.Tags)
	}
}

// TestScenarioEmptyTagsClearsTarget pins the endpoint behaviour the writer
// depends on: PutObjectTagging with an empty TagSet is accepted and clears the
// target's tags. If it were rejected, a source whose tags were removed could
// never converge.
func TestScenarioEmptyTagsClearsTarget(t *testing.T) {
	e := newScenarioEnv(t, envOptions{ReplicateAttrs: true, AttrsReconcileInterval: 60})
	putBody(t, e.Client, e.Source, "doc.txt", "body")
	putTags(t, e.Client, e.Source, "doc.txt", map[string]string{"team": "alpha"})
	e.sync()
	deleteTags(t, e.Client, e.Source, "doc.txt")
	e.reconcile()
	if f := fetchFacts(t, e.Client, e.Target, "doc.txt"); len(f.Tags) != 0 {
		t.Fatalf("target tags = %v after the source's tags were removed, want none", f.Tags)
	}
}

func TestScenarioTagDriftRepaired(t *testing.T) {
	e := newScenarioEnv(t, envOptions{ReplicateAttrs: true, AttrsReconcileInterval: 60})
	putBody(t, e.Client, e.Source, "doc.txt", "body")
	putTags(t, e.Client, e.Source, "doc.txt", map[string]string{"team": "alpha"})
	e.sync()
	putTags(t, e.Client, e.Source, "doc.txt", map[string]string{"team": "beta"})
	e.reconcile()
	if f := fetchFacts(t, e.Client, e.Target, "doc.txt"); f.Tags["team"] != "beta" {
		t.Fatalf("target tag team = %q after drift, want beta", f.Tags["team"])
	}
}

// TestScenarioMetadataDriftRepaired is the sweep's reason to exist: a metadata
// change bumps neither the ETag nor LastModified, so the differ cannot see it.
func TestScenarioMetadataDriftRepaired(t *testing.T) {
	e := newScenarioEnv(t, envOptions{ReplicateAttrs: true, AttrsReconcileInterval: 60})
	putBody(t, e.Client, e.Source, "doc.txt", "body", func(in *s3.PutObjectInput) {
		in.Metadata = map[string]string{"rev": "1"}
	})
	e.sync()
	copySelfWithMetadata(t, e.Client, e.Source, "doc.txt", map[string]string{"rev": "2"})
	e.reconcile()
	if f := fetchFacts(t, e.Client, e.Target, "doc.txt"); f.Metadata["rev"] != "2" {
		t.Fatalf("target metadata rev = %q after drift, want 2", f.Metadata["rev"])
	}
}

// TestScenarioStorageClassOverride is the scenario that would have caught the
// writer/reader fingerprint mismatch fixed earlier in this project: the writer
// hashed the effective TARGET class while the sweep hashed the raw source
// class, so every pair with TargetStorageClass set re-copied every object every
// sweep. A quiet second sync after the copy AND after a sweep is the assertion.
func TestScenarioStorageClassOverride(t *testing.T) {
	e := newScenarioEnv(t, envOptions{ReplicateAttrs: true, AttrsReconcileInterval: 60})
	e.Pair.TargetStorageClass = "STANDARD"
	putBody(t, e.Client, e.Source, "doc.txt", "body")
	e.sync()
	e.assertNoWorkOnSecondSync()
	e.reconcile()
	e.assertNoWorkOnSecondSync()
}

// TestScenarioUnreplicableFields pins that Content-Language and Expires are
// deliberately excluded from the fingerprint. This endpoint does not store
// them, so including them would make source and target differ on every sweep
// forever.
func TestScenarioUnreplicableFields(t *testing.T) {
	e := newScenarioEnv(t, envOptions{ReplicateAttrs: true, AttrsReconcileInterval: 60})
	putBody(t, e.Client, e.Source, "doc.txt", "body", func(in *s3.PutObjectInput) {
		in.ContentLanguage = aws.String("en-GB")
		in.Expires = aws.Time(time.Now().Add(24 * time.Hour))
		in.ContentType = aws.String("text/plain")
	})
	e.sync()
	e.assertNoWorkOnSecondSync()
	e.reconcile()
	e.assertNoWorkOnSecondSync() // perpetual drift would show up here
}

// TestScenarioPendingAttrsRecovery covers the attribute-only retry: when a tag
// write fails the object is marked pending, and the next run must rewrite the
// characteristics WITHOUT copying the bytes again.
func TestScenarioPendingAttrsRecovery(t *testing.T) {
	e := newScenarioEnv(t, envOptions{ReplicateAttrs: true})
	putBody(t, e.Client, e.Source, "doc.txt", "body")
	putTags(t, e.Client, e.Source, "doc.txt", map[string]string{"team": "alpha"})
	e.sync()

	// Simulate the failed tag write the engine would have recorded.
	e.markPending("doc.txt")
	e.Counter.Reset()
	e.sync()
	if got := e.Counter.Count(kindPutObject) + e.Counter.Count(kindCopyObject); got != 0 {
		t.Fatalf("a pending-attributes object was re-copied (%d byte writes), want attributes only", got)
	}
	if got := e.Counter.Count(kindPutTagging); got == 0 {
		t.Fatal("a pending-attributes object did not get its tags written")
	}
	if f := fetchFacts(t, e.Client, e.Target, "doc.txt"); f.Tags["team"] != "alpha" {
		t.Fatalf("target tag team = %q after recovery, want alpha", f.Tags["team"])
	}
}

// TestScenarioAttrsOff is the control: with replication off the worker must not
// spend a request reading or writing tags, because every extra request costs
// ~750ms on this endpoint.
//
// It deliberately does NOT assert "no tags on the target". This endpoint's
// CopyObject carries the source's tags by default (TaggingDirective defaults to
// COPY) — only an inline x-amz-tagging header is ignored — so tags ride along
// with the bytes whether or not replication is on. What replication-off buys is
// the absence of the extra GetObjectTagging/PutObjectTagging round-trip.
func TestScenarioAttrsOff(t *testing.T) {
	e := newScenarioEnv(t, envOptions{ReplicateAttrs: false})
	putBody(t, e.Client, e.Source, "doc.txt", "body")
	putTags(t, e.Client, e.Source, "doc.txt", map[string]string{"team": "alpha"})
	e.sync()
	if got := e.Counter.Count(kindGetTagging); got != 0 {
		t.Fatalf("replication is off but %d GetObjectTagging calls were made", got)
	}
	if got := e.Counter.Count(kindPutTagging); got != 0 {
		t.Fatalf("replication is off but %d PutObjectTagging calls were made", got)
	}
	// The copy itself must still have happened.
	e.assertTargetCount(1)
}

// TestScenarioInvalidStorageClass records the endpoint's real constraint: only
// STANDARD is accepted, so any other value must fail loudly rather than
// silently produce an object in the wrong class.
func TestScenarioInvalidStorageClass(t *testing.T) {
	e := newScenarioEnv(t, envOptions{})
	e.Pair.TargetStorageClass = "GLACIER"
	putBody(t, e.Client, e.Source, "doc.txt", "body")
	out := e.syncOutcome()
	if out.Failed == 0 {
		t.Fatalf("an invalid storage class was accepted (status %q)", out.Status)
	}
	if !strings.Contains(strings.ToLower(out.Log), "storage class") {
		t.Fatalf("the failure does not mention the storage class: %q", out.Log)
	}
}

// TestScenarioReconcileOffByDefault pins that no sweep runs when the interval is
// unset, so the opt-in stays opt-in.
func TestScenarioReconcileOffByDefault(t *testing.T) {
	e := newScenarioEnv(t, envOptions{ReplicateAttrs: true})
	if sync.ShouldReconcile(e.Pair, time.Now().UTC()) {
		t.Fatal("ShouldReconcile is true with no interval configured")
	}
	e.Pair.LastReconcileAt = nil
	if sync.ShouldReconcile(e.Pair, time.Now().UTC()) {
		t.Fatal("ShouldReconcile is true with a nil LastReconcileAt and no interval")
	}
}
