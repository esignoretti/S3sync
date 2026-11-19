// tests/scenario/scenario_lock_test.go
//go:build integration

package scenario

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/esignoretti/S3sync/internal/sync"
)

// provision runs the target preflight against the env's real target bucket.
// sync.ProvisionTarget takes an unexported interface, which *s3.Client
// satisfies implicitly, so no production change is needed to drive it here.
func (e *scenarioEnv) provision(opts sync.ProvisionOptions) error {
	e.T.Helper()
	return sync.ProvisionTarget(context.Background(), e.tgtClient, "us-east-1", e.tgtBucket, e.Pair, opts)
}

// TestScenarioGovernanceOneDay uses the full day the user asked for: GOVERNANCE
// can be bypassed, so teardown can always remove it.
func TestScenarioGovernanceOneDay(t *testing.T) {
	e := newScenarioEnv(t, envOptions{ObjectLock: true, ReplicateAttrs: true})
	putBody(t, e.Client, e.Source, "held.txt", "body", func(in *s3.PutObjectInput) {
		in.ObjectLockMode = types.ObjectLockModeGovernance
		in.ObjectLockRetainUntilDate = aws.Time(time.Now().Add(24 * time.Hour))
	})
	e.sync()
	f := fetchFacts(t, e.Client, e.Target, "held.txt")
	if f.RetentionMode != string(types.ObjectLockModeGovernance) {
		t.Fatalf("target retention mode = %q, want GOVERNANCE", f.RetentionMode)
	}
	// Object lock forces versioning on, so a plain DeleteObject would only
	// write a delete marker and return 204. The hold is proven by deleting the
	// VERSION, which the endpoint must refuse.
	if _, err := e.Client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket:    aws.String(e.Target),
		Key:       aws.String("held.txt"),
		VersionId: aws.String(currentVersionID(t, e.Client, e.Target, "held.txt")),
	}); err == nil {
		t.Fatal("the target allowed deletion of a version under GOVERNANCE retention")
	}
}

// TestScenarioComplianceTwoMinutes keeps the hold to minutes, not a day:
// COMPLIANCE cannot be bypassed by anyone, so a 24-hour hold would make the
// bucket impossible to tear down and leak it for a day.
func TestScenarioComplianceTwoMinutes(t *testing.T) {
	e := newScenarioEnv(t, envOptions{ObjectLock: true, ReplicateAttrs: true})
	putBody(t, e.Client, e.Source, "strict.txt", "body", func(in *s3.PutObjectInput) {
		in.ObjectLockMode = types.ObjectLockModeCompliance
		in.ObjectLockRetainUntilDate = aws.Time(time.Now().Add(2 * time.Minute))
	})
	e.sync()
	f := fetchFacts(t, e.Client, e.Target, "strict.txt")
	if f.RetentionMode != string(types.ObjectLockModeCompliance) {
		t.Fatalf("target retention mode = %q, want COMPLIANCE", f.RetentionMode)
	}
	if _, err := e.Client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket:                    aws.String(e.Target),
		Key:                       aws.String("strict.txt"),
		VersionId:                 aws.String(currentVersionID(t, e.Client, e.Target, "strict.txt")),
		BypassGovernanceRetention: aws.Bool(true),
	}); err == nil {
		t.Fatal("a COMPLIANCE hold was bypassed")
	}
}

func TestScenarioLegalHold(t *testing.T) {
	e := newScenarioEnv(t, envOptions{ObjectLock: true, ReplicateAttrs: true})
	putBody(t, e.Client, e.Source, "hold.txt", "body", func(in *s3.PutObjectInput) {
		in.ObjectLockLegalHoldStatus = types.ObjectLockLegalHoldStatusOn
	})
	e.sync()
	if f := fetchFacts(t, e.Client, e.Target, "hold.txt"); f.LegalHold != "ON" {
		t.Fatalf("target legal hold = %q, want ON", f.LegalHold)
	}

	// The point of a legal hold is that it blocks deletion independently of any
	// retention period — there is no retain-until here at all. Deleting the
	// VERSION (not just the key, which only writes a delete marker) must fail
	// even with the governance bypass.
	version := currentVersionID(t, e.Client, e.Target, "hold.txt")
	_, err := e.Client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket:                    aws.String(e.Target),
		Key:                       aws.String("hold.txt"),
		VersionId:                 aws.String(version),
		BypassGovernanceRetention: aws.Bool(true),
	})
	if err == nil {
		t.Fatal("a legal-held version was deleted despite the hold")
	}
}

// TestScenarioLockWithoutTargetLock is the failure mode Review Focus 5 names:
// the endpoint's raw error says nothing about object lock, so the worker must
// wrap it or the operator is sent after the wrong cause.
func TestScenarioLockWithoutTargetLock(t *testing.T) {
	// The source must have lock enabled or the locked PUT itself is rejected;
	// the TARGET is the one that must lack it.
	e := newScenarioEnv(t, envOptions{ObjectLock: true, TargetObjectLock: aws.Bool(false), ReplicateAttrs: true})
	putBody(t, e.Client, e.Source, "held.txt", "body", func(in *s3.PutObjectInput) {
		in.ObjectLockMode = types.ObjectLockModeGovernance
		in.ObjectLockRetainUntilDate = aws.Time(time.Now().Add(time.Hour))
	})
	// A refused object is a per-object failure, not a run error: the run itself
	// succeeds and reports status "error" with one failed worker.
	out := e.syncOutcome()
	if out.Err != nil {
		t.Fatalf("run error = %v, want a per-object failure", out.Err)
	}
	if out.Failed != 1 {
		t.Fatalf("failed = %d, want 1", out.Failed)
	}
	if !strings.Contains(out.Log, "object lock") {
		t.Fatalf("the failure does not name object lock, so the operator is sent after the wrong cause: %q", out.Log)
	}
	e.assertTargetCount(0)
}

// TestScenarioRetentionJustExpired covers the clock-skew case: a retain-until
// that is already past must be omitted, not sent. Sending it is rejected by the
// endpoint, so the object would fail forever.
func TestScenarioRetentionJustExpired(t *testing.T) {
	e := newScenarioEnv(t, envOptions{ObjectLock: true, ReplicateAttrs: true})
	putBody(t, e.Client, e.Source, "stale.txt", "body", func(in *s3.PutObjectInput) {
		in.ObjectLockMode = types.ObjectLockModeGovernance
		in.ObjectLockRetainUntilDate = aws.Time(time.Now().Add(2 * time.Second))
	})
	time.Sleep(3 * time.Second)
	e.sync()
	if f := fetchFacts(t, e.Client, e.Target, "stale.txt"); f.RetentionMode != "" {
		t.Fatalf("an expired retention was replicated: mode = %q", f.RetentionMode)
	}
}

// TestScenarioLockAtCreation proves the create path: a missing target must be
// created WITH lock enabled, because this endpoint cannot convert afterwards.
func TestScenarioLockAtCreation(t *testing.T) {
	e := newScenarioEnv(t, envOptions{ObjectLock: false})
	e.Pair.RequireObjectLock = true
	e.Pair.OnLockMissing = "fail"
	// Drop the target bucket so provisioning has to create it.
	destroyBucket(t, e.Client, e.Target)

	if err := e.provision(sync.ProvisionOptions{Now: time.Now().UTC()}); err != nil {
		t.Fatalf("provisioning a missing target with require-object-lock failed: %v", err)
	}
	cfg, err := e.Client.GetObjectLockConfiguration(context.Background(), &s3.GetObjectLockConfigurationInput{
		Bucket: aws.String(e.Target),
	})
	if err != nil {
		t.Fatalf("get lock config: %v", err)
	}
	if cfg.ObjectLockConfiguration == nil || cfg.ObjectLockConfiguration.ObjectLockEnabled != types.ObjectLockEnabledEnabled {
		t.Fatal("the created target does not have object lock enabled")
	}
}

func TestScenarioLockMissingFail(t *testing.T) {
	e := newScenarioEnv(t, envOptions{ObjectLock: false})
	e.Pair.RequireObjectLock = true
	e.Pair.OnLockMissing = "fail"
	err := e.provision(sync.ProvisionOptions{Now: time.Now().UTC()})
	if !errors.Is(err, sync.ErrLockMissing) {
		t.Fatalf("provision error = %v, want ErrLockMissing", err)
	}
}

func TestScenarioLockMissingRecreateEmpty(t *testing.T) {
	e := newScenarioEnv(t, envOptions{ObjectLock: false})
	e.Pair.RequireObjectLock = true
	e.Pair.OnLockMissing = "recreate"
	if err := e.provision(sync.ProvisionOptions{ForceRecreate: true, Now: time.Now().UTC()}); err != nil {
		t.Fatalf("recreate of an empty unlocked target failed: %v", err)
	}
	cfg, err := e.Client.GetObjectLockConfiguration(context.Background(), &s3.GetObjectLockConfigurationInput{
		Bucket: aws.String(e.Target),
	})
	if err != nil {
		t.Fatalf("get lock config: %v", err)
	}
	if cfg.ObjectLockConfiguration == nil || cfg.ObjectLockConfiguration.ObjectLockEnabled != types.ObjectLockEnabledEnabled {
		t.Fatal("the recreated target does not have object lock enabled")
	}
}

// TestScenarioLockMissingRecreateNonEmpty is the destructive-path guard: the
// target holds data, so recreation must be refused BEFORE anything is deleted.
func TestScenarioLockMissingRecreateNonEmpty(t *testing.T) {
	e := newScenarioEnv(t, envOptions{ObjectLock: false})
	putBody(t, e.Client, e.Target, "precious.txt", "do not delete")
	e.Pair.RequireObjectLock = true
	e.Pair.OnLockMissing = "recreate"
	err := e.provision(sync.ProvisionOptions{ForceRecreate: true, Now: time.Now().UTC()})
	if !errors.Is(err, sync.ErrTargetNotEmpty) {
		t.Fatalf("provision error = %v, want ErrTargetNotEmpty", err)
	}
	keys, err := listAll(t, e.Client, e.Target)
	if err != nil {
		t.Fatalf("list target: %v", err)
	}
	if len(keys) != 1 || keys[0] != "precious.txt" {
		t.Fatalf("target contents after a refused recreate = %v, want [precious.txt]", keys)
	}
}
