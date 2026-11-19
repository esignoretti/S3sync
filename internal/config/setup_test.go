// internal/config/setup_test.go
package config

import (
	"errors"
	"testing"
)

// newPairStepState returns a SetupState positioned at the sync-pair step with
// both buckets already created, as the wizard would have it.
func newPairStepState(t *testing.T, repo *Repository) *SetupState {
	t.Helper()
	src := &Bucket{
		Name: "src", Endpoint: "https://s3.example.com", Region: "eu-west-1",
		AccessKey: "ak", SecretKey: "sk", BucketName: "src-b",
	}
	if err := repo.CreateBucket(src); err != nil {
		t.Fatal(err)
	}
	tgt := &Bucket{
		Name: "tgt", Endpoint: "https://s3.example.com", Region: "eu-west-1",
		AccessKey: "ak", SecretKey: "sk", BucketName: "tgt-b",
	}
	if err := repo.CreateBucket(tgt); err != nil {
		t.Fatal(err)
	}
	return &SetupState{
		SourceBucket: src,
		TargetBucket: tgt,
		Step:         StepSyncPair,
	}
}

// A field the wizard form posts but SetupInput does not declare is discarded by
// ShouldBindJSON, so the pair is silently created with the zero value. This test
// pins every option the setup.html form submits.
func TestSetupInputMapsPairOptions(t *testing.T) {
	in := &SetupInput{
		PairName:                "p",
		ReplicateAttributes:     true,
		TargetExpireAt:          "2027-03-01",
		NoncurrentExpireDays:    30,
		NewerNoncurrentVersions: 2,
		RequireObjectLock:       true,
		OnLockMissing:           "recreate",
		AttrsReconcileInterval:  3600,
	}

	p := in.ToSyncPair("src", "tgt")

	if !p.ReplicateAttributes {
		t.Error("ReplicateAttributes was dropped")
	}
	if p.TargetExpireAt == nil {
		t.Fatal("TargetExpireAt was dropped")
	}
	if got, want := p.TargetExpireAt.Format("2006-01-02"), "2027-03-01"; got != want {
		t.Errorf("TargetExpireAt = %s, want %s", got, want)
	}
	if p.NoncurrentExpireDays != 30 {
		t.Errorf("NoncurrentExpireDays = %d, want 30", p.NoncurrentExpireDays)
	}
	if p.NewerNoncurrentVersions != 2 {
		t.Errorf("NewerNoncurrentVersions = %d, want 2", p.NewerNoncurrentVersions)
	}
	if !p.RequireObjectLock {
		t.Error("RequireObjectLock was dropped")
	}
	if p.OnLockMissing != "recreate" {
		t.Errorf("OnLockMissing = %q, want recreate", p.OnLockMissing)
	}
	if p.AttrsReconcileInterval != 3600 {
		t.Errorf("AttrsReconcileInterval = %d, want 3600", p.AttrsReconcileInterval)
	}
}

func TestSetupInputDefaultsOnLockMissing(t *testing.T) {
	p := (&SetupInput{PairName: "p"}).ToSyncPair("src", "tgt")
	if p.OnLockMissing != "fail" {
		t.Errorf("OnLockMissing = %q, want fail", p.OnLockMissing)
	}
}

func TestSetupInputIgnoresMalformedExpireAt(t *testing.T) {
	p := (&SetupInput{PairName: "p", TargetExpireAt: "01/03/2027"}).ToSyncPair("src", "tgt")
	if p.TargetExpireAt != nil {
		t.Errorf("TargetExpireAt = %v, want nil so ValidatePairOptions reports it", p.TargetExpireAt)
	}
}

// A provisioning failure must abort the step with nothing written: a pair whose
// target cannot be prepared would otherwise be stored and fail every cycle.
func TestSetupApplyProvisionErrorWritesNothing(t *testing.T) {
	repo, _ := testRepo(t)
	st := newPairStepState(t, repo)

	want := errors.New("target bucket lacks object lock")
	st.Provision = func(*SyncPair) error { return want }

	err := st.Apply(repo, &SetupInput{PairName: "p"})
	if !errors.Is(err, want) {
		t.Fatalf("err = %v, want %v", err, want)
	}
	if st.Step != StepSyncPair {
		t.Errorf("step = %v, want to stay at StepSyncPair", st.Step)
	}
	if st.SyncPair != nil {
		t.Error("SyncPair was set despite the provisioning failure")
	}
	pairs, err := repo.ListSyncPairs()
	if err != nil {
		t.Fatal(err)
	}
	if len(pairs) != 0 {
		t.Fatalf("got %d persisted pairs, want 0", len(pairs))
	}
}

func TestSetupApplyRunsProvisionBeforePersisting(t *testing.T) {
	repo, _ := testRepo(t)
	st := newPairStepState(t, repo)

	var sawPersisted bool
	st.Provision = func(*SyncPair) error {
		pairs, err := repo.ListSyncPairs()
		if err != nil {
			t.Fatal(err)
		}
		sawPersisted = len(pairs) > 0
		return nil
	}

	if err := st.Apply(repo, &SetupInput{PairName: "p"}); err != nil {
		t.Fatal(err)
	}
	if sawPersisted {
		t.Error("pair was already persisted when the preflight ran")
	}
	if st.Step != StepDone {
		t.Errorf("step = %v, want StepDone", st.Step)
	}
	pairs, err := repo.ListSyncPairs()
	if err != nil {
		t.Fatal(err)
	}
	if len(pairs) != 1 {
		t.Fatalf("got %d persisted pairs, want 1", len(pairs))
	}
}

// The wizard's recreate option is only reachable with a typed bucket name; the
// name is forwarded on the state so the injected preflight can check it.
func TestSetupApplyForwardsRecreateConfirm(t *testing.T) {
	repo, _ := testRepo(t)
	st := newPairStepState(t, repo)

	var got string
	st.Provision = func(*SyncPair) error {
		got = st.RecreateConfirm
		return nil
	}

	if err := st.Apply(repo, &SetupInput{PairName: "p", RecreateConfirm: "tgt-b"}); err != nil {
		t.Fatal(err)
	}
	if got != "tgt-b" {
		t.Errorf("RecreateConfirm = %q, want tgt-b", got)
	}
}
