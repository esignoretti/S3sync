// tests/scenario/scenario_matrix_test.go
//go:build integration

package scenario

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// TestScenarioDryRunThenReal is the direct regression test for the cache
// poisoning bug fixed in this project: a dry run recorded every action as
// synced, so the next real run's differ saw matching ETags and skipped them —
// the bytes never arrived and nothing ever retried.
func TestScenarioDryRunThenReal(t *testing.T) {
	e := newScenarioEnv(t, envOptions{})
	e.Pair.DryRun = true
	e.seed([]string{"obj/a.bin", "obj/b.bin"})
	e.sync()
	if keys := e.targetKeys(); len(keys) != 0 {
		t.Fatalf("a dry run copied %v", keys)
	}
	e.Pair.DryRun = false
	e.sync()
	e.assertTargetCount(2) // the dry run must not have cached them
}

// TestScenarioDryRunDoesNotProvision is the regression test for the other
// dry-run bug: setupOnce provisioned the target, creating a bucket and
// rewriting its lifecycle for a run that was only supposed to report.
func TestScenarioDryRunDoesNotProvision(t *testing.T) {
	e := newScenarioEnv(t, envOptions{TargetExpireIn: 30 * 24 * time.Hour})
	e.Pair.DryRun = true
	e.sync()
	_, err := e.Client.GetBucketLifecycleConfiguration(context.Background(), &s3.GetBucketLifecycleConfigurationInput{
		Bucket: aws.String(e.Target),
	})
	if err == nil {
		t.Fatal("a dry run wrote a lifecycle configuration")
	}
}

// TestScenarioLockedVersionedAttrs is the all-features seam: a 6 GiB object
// whose content-type, tag, storage class and retention must all survive a
// multipart copy into a locked, versioned target.
func TestScenarioLockedVersionedAttrs(t *testing.T) {
	if tier() != tierAll {
		t.Skip("6 GiB composition; set SCENARIO_TIER=all")
	}
	e := newScenarioEnv(t, envOptions{
		ObjectLock: true, Versioning: true, ReplicateAttrs: true, WorkerCount: 4,
	})
	e.composeLarge("big/all.bin", 6<<30, func(in *s3.CreateMultipartUploadInput) {
		in.ContentType = aws.String("application/x-custom")
		in.ObjectLockMode = types.ObjectLockModeGovernance
		in.ObjectLockRetainUntilDate = aws.Time(time.Now().Add(24 * time.Hour))
	})
	putTags(t, e.Client, e.Source, "big/all.bin", map[string]string{"kind": "matrix"})
	e.sync()
	f := fetchFacts(t, e.Client, e.Target, "big/all.bin")
	if f.ContentType != "application/x-custom" || f.RetentionMode != "GOVERNANCE" || f.Tags["kind"] != "matrix" {
		t.Fatalf("target facts = %+v, want content-type, GOVERNANCE and tag all replicated", f)
	}
}

// TestScenarioExpiryWithLockAndVersioning is the combination where the two
// lifecycle rules and the lock interact: expiry cannot delete a version still
// under retention, which is why the noncurrent rule exists at all.
func TestScenarioExpiryWithLockAndVersioning(t *testing.T) {
	e := newScenarioEnv(t, envOptions{
		ObjectLock: true, Versioning: true, TargetExpireIn: 60 * 24 * time.Hour,
	})
	e.Pair.NoncurrentExpireDays = 7
	e.sync()
	cfg, err := e.Client.GetBucketLifecycleConfiguration(context.Background(), &s3.GetBucketLifecycleConfigurationInput{
		Bucket: aws.String(e.Target),
	})
	if err != nil {
		t.Fatalf("get lifecycle: %v", err)
	}
	ids := ruleIDs(cfg.Rules)
	if len(ids) != 2 || !contains(ids, "s3sync-expire") || !contains(ids, "s3sync-noncurrent") {
		t.Fatalf("rules = %v, want both s3sync-expire and s3sync-noncurrent", ids)
	}
}

// TestScenarioManySmallLocked crosses the paginated-listing boundary with every
// object locked and attributes on, which is the shape a real backup pair takes.
func TestScenarioManySmallLocked(t *testing.T) {
	if tier() != tierAll {
		t.Skip("500 locked objects; set SCENARIO_TIER=all")
	}
	const n = 500
	e := newScenarioEnv(t, envOptions{ObjectLock: true, ReplicateAttrs: true, WorkerCount: 16})
	for i := 0; i < n; i++ {
		k := synthKey(i, "small")
		putBody(t, e.Client, e.Source, k, "body", func(in *s3.PutObjectInput) {
			in.ObjectLockMode = types.ObjectLockModeGovernance
			in.ObjectLockRetainUntilDate = aws.Time(time.Now().Add(24 * time.Hour))
			in.Metadata = map[string]string{"n": fmt.Sprint(i)}
		})
		putTags(t, e.Client, e.Source, k, map[string]string{"batch": "matrix"})
	}
	e.sync()
	e.assertTargetCount(n)
	e.assertNoWorkOnSecondSync()
	for _, i := range []int{0, n / 2, n - 1} {
		k := synthKey(i, "small")
		f := fetchFacts(t, e.Client, e.Target, k)
		if f.RetentionMode != "GOVERNANCE" || f.Tags["batch"] != "matrix" || f.Metadata["n"] != fmt.Sprint(i) {
			t.Fatalf("%s facts = %+v, want GOVERNANCE, batch=matrix and metadata n=%d", k, f, i)
		}
	}
}

// TestScenarioWorkerCountIndependence asserts that correctness does not depend
// on the worker pool's size — the concurrency bug class (a shared counter, a
// lost result) shows up as a count mismatch at one width but not another.
//
// The key count is 12, not the 50 the plan sketched: at workers=1 this endpoint
// takes ~1.3s per object for the copy plus ~1.5s for the tag round-trip, so 50
// keys cost ~190s per sync and the scenario has three syncs plus 50 fact
// comparisons — it blew the 680s test timeout. Twelve keys still exercises both
// the serial and the parallel path; the paginated-listing boundary is covered by
// TestScenarioManySmall.
func TestScenarioWorkerCountIndependence(t *testing.T) {
	for _, workers := range []int{1, 16} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			e := newScenarioEnv(t, envOptions{WorkerCount: workers, ReplicateAttrs: true})
			keys := manyKeys(12)
			e.seed(keys)
			e.sync()
			e.assertTargetCount(12)
			for _, k := range keys {
				e.assertFactsMatch(k)
			}
			e.assertNoWorkOnSecondSync()
		})
	}
}

// TestScenarioAttrsAndExpiryTogether is the pair an operator reaches for when
// both features are wanted: characteristics replicated and a target that expires.
func TestScenarioAttrsAndExpiryTogether(t *testing.T) {
	e := newScenarioEnv(t, envOptions{ReplicateAttrs: true, TargetExpireIn: 60 * 24 * time.Hour})
	e.seed([]string{"obj/0001-small.bin"})
	putTags(t, e.Client, e.Source, "obj/0001-small.bin", map[string]string{"team": "alpha"})
	e.sync()
	if f := fetchFacts(t, e.Client, e.Target, "obj/0001-small.bin"); f.Tags["team"] != "alpha" {
		t.Fatalf("target tags = %v, want team=alpha", f.Tags)
	}
	cfg, err := e.Client.GetBucketLifecycleConfiguration(context.Background(), &s3.GetBucketLifecycleConfigurationInput{
		Bucket: aws.String(e.Target),
	})
	if err != nil {
		t.Fatalf("get lifecycle: %v", err)
	}
	if !contains(ruleIDs(cfg.Rules), "s3sync-expire") {
		t.Fatalf("rules = %v, want the s3sync-expire rule", ruleIDs(cfg.Rules))
	}
	e.assertNoWorkOnSecondSync()
}

// TestScenarioDeletePropagationWithAttrs pins that propagation deletes only what
// the source deleted, and that the attributes machinery does not resurrect it.
func TestScenarioDeletePropagationWithAttrs(t *testing.T) {
	e := newScenarioEnv(t, envOptions{ReplicateAttrs: true})
	e.Pair.DeletePropagation = true
	e.seed([]string{"obj/a.bin", "obj/b.bin"})
	e.sync()
	e.assertTargetCount(2)

	if _, err := e.Client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String(e.Source), Key: aws.String("obj/a.bin"),
	}); err != nil {
		t.Fatalf("delete source: %v", err)
	}
	e.sync()
	keys := e.targetKeys()
	if contains(keys, "obj/a.bin") {
		t.Fatalf("target still holds obj/a.bin after propagation: %v", keys)
	}
	if !contains(keys, "obj/b.bin") {
		t.Fatalf("target lost obj/b.bin: %v", keys)
	}
}
