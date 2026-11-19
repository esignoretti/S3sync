package sync

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/esignoretti/S3sync/internal/config"
)

// runTestRepo opens a real on-disk config DB, because RunOneShot persists
// through the same repository a caller uses.
func runTestRepo(t *testing.T) *config.Repository {
	t.Helper()
	db, err := config.Open(filepath.Join(t.TempDir(), "config.db"))
	if err != nil {
		t.Fatalf("open config: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return config.NewRepository(db)
}

// runTestPair creates a pair with both buckets and returns it. The endpoint is
// unreachable by design; tests that need a run to reach S3 use their own fake.
func runTestPair(t *testing.T, repo *config.Repository, pair *config.SyncPair) *config.SyncPair {
	t.Helper()
	src := &config.Bucket{
		Name: "s", BucketName: "s", Endpoint: "https://example.invalid",
		Region: "us-east-1", AccessKey: "a", SecretKey: "b",
	}
	tgt := &config.Bucket{
		Name: "t", BucketName: "t", Endpoint: "https://example.invalid",
		Region: "us-east-1", AccessKey: "a", SecretKey: "b",
	}
	if err := repo.CreateBucket(src); err != nil {
		t.Fatal(err)
	}
	if err := repo.CreateBucket(tgt); err != nil {
		t.Fatal(err)
	}
	pair.SourceBucketID = src.ID
	pair.TargetBucketID = tgt.ID
	pair.SyncInterval = 3600
	pair.WorkerCount = 1
	pair.Enabled = true
	if err := repo.CreateSyncPair(pair); err != nil {
		t.Fatal(err)
	}
	return pair
}

// A pair whose expiry has passed is dead: the lifecycle rule has already deleted
// its target objects, so every later run is refused with ErrPairExpired and the
// pair never succeeds again. The stored status is what `pair list`, `status` and
// the dashboard read, so a refused run that returns without writing leaves a
// dead pair displayed as healthy indefinitely.
func TestRunOneShot_PersistsExpiredStatus(t *testing.T) {
	repo := runTestRepo(t)
	past := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	pair := runTestPair(t, repo, &config.SyncPair{
		Name:           "dead",
		TargetExpireAt: &past,
		LastSyncStatus: "ok", // a previous healthy run, which must be overwritten
	})

	err := RunOneShot(context.Background(), repo, pair.ID, filepath.Join(t.TempDir(), "cache.db"))
	if err == nil {
		t.Fatal("RunOneShot succeeded for a pair whose expiry has passed")
	}

	got, err := repo.GetSyncPair(pair.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.LastSyncStatus != "expired" {
		t.Errorf("stored status = %q, want \"expired\" — the run was refused but the "+
			"previous \"ok\" is still what the dashboard shows", got.LastSyncStatus)
	}
}

// The engine's terminal status must also reach the database when a run fails
// partway, not only when it succeeds.
func TestRunOneShot_PersistsErrorStatus(t *testing.T) {
	repo := runTestRepo(t)
	pair := runTestPair(t, repo, &config.SyncPair{
		Name:           "broken",
		LastSyncStatus: "ok",
	})

	err := RunOneShot(context.Background(), repo, pair.ID, filepath.Join(t.TempDir(), "cache.db"))
	if err == nil {
		t.Fatal("RunOneShot succeeded against an unreachable endpoint")
	}

	got, err := repo.GetSyncPair(pair.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.LastSyncStatus != "error" {
		t.Errorf("stored status = %q, want \"error\"", got.LastSyncStatus)
	}
}

// Provisioning and the sweep record their outcomes on the engine's private pair,
// so persisting a re-read pair would silently drop them. provision_error and
// last_reconcile_at are advertised in the pair payload, which makes an
// always-empty column a lie rather than a cosmetic gap.
func TestRunOneShot_PersistsProvisionState(t *testing.T) {
	repo := runTestRepo(t)
	pair := runTestPair(t, repo, &config.SyncPair{Name: "prov"})

	// A run against an unreachable endpoint fails during provisioning, which is
	// exactly the case where provision_error must be recorded.
	_ = RunOneShot(context.Background(), repo, pair.ID, filepath.Join(t.TempDir(), "cache.db"))

	got, err := repo.GetSyncPair(pair.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ProvisionError == "" {
		t.Error("provision_error is empty after a failed provision: the column can " +
			"never report anything, so an operator sees no reason for the failure")
	}
}

// A one-shot sync from the CLI must leave a history row. Only the API path
// wrote sync_logs, so a pair driven by cron had an empty history in the
// dashboard: the pair showed a last_sync_at and nothing else, which makes the
// history view useless for exactly the pairs that run unattended.
func TestRunOneShot_WritesSyncLog(t *testing.T) {
	repo := runTestRepo(t)
	pair := runTestPair(t, repo, &config.SyncPair{Name: "logged"})

	_ = RunOneShot(context.Background(), repo, pair.ID, filepath.Join(t.TempDir(), "cache.db"))

	logs, err := repo.ListSyncLogs(pair.ID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) != 1 {
		t.Fatalf("sync_logs rows = %d, want 1: a CLI sync leaves no history", len(logs))
	}
	if logs[0].Status == "" {
		t.Error("the logged row has no status")
	}
}

// A refused run must be logged too, not silently skipped: "the pair never ran
// because its expiry passed" is the single most important history entry there
// is, and an empty log is indistinguishable from a healthy pair.
func TestRunOneShot_LogsRefusedRun(t *testing.T) {
	repo := runTestRepo(t)
	past := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	pair := runTestPair(t, repo, &config.SyncPair{Name: "dead", TargetExpireAt: &past})

	_ = RunOneShot(context.Background(), repo, pair.ID, filepath.Join(t.TempDir(), "cache.db"))

	logs, err := repo.ListSyncLogs(pair.ID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) != 1 {
		t.Fatalf("sync_logs rows = %d, want 1", len(logs))
	}
	if logs[0].Status != "expired" {
		t.Errorf("logged status = %q, want \"expired\"", logs[0].Status)
	}
	if logs[0].ErrorMsg == "" {
		t.Error("the refused run was logged with no reason")
	}
}
