package sync

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/esignoretti/S3sync/internal/cache"
	"github.com/esignoretti/S3sync/internal/config"
	"github.com/esignoretti/S3sync/internal/s3client"
)

// RunOneShot performs a single sync cycle for the given pair ID.
// It loads config from the repository, creates S3 clients, and runs the engine.
func RunOneShot(ctx context.Context, repo *config.Repository, pairID, cacheDir string) error {
	pair, err := repo.GetSyncPair(pairID)
	if err != nil {
		return fmt.Errorf("get pair: %w", err)
	}

	src, err := repo.GetBucket(pair.SourceBucketID)
	if err != nil {
		return fmt.Errorf("get source bucket: %w", err)
	}
	tgt, err := repo.GetBucket(pair.TargetBucketID)
	if err != nil {
		return fmt.Errorf("get target bucket: %w", err)
	}

	srcS3, err := s3client.NewClient(src)
	if err != nil {
		return fmt.Errorf("s3 source client: %w", err)
	}
	tgtS3, err := s3client.NewClient(tgt)
	if err != nil {
		return fmt.Errorf("s3 target client: %w", err)
	}

	cacheStore, err := cache.Open(cacheDir)
	if err != nil {
		return fmt.Errorf("open cache: %w", err)
	}
	defer cacheStore.Close()

	engine := NewEngine(pair, src, tgt, srcS3, tgtS3, cacheStore, repo.GetSettingsOrDefaults)
	start := time.Now()

	// Progress display goroutine
	done := make(chan struct{})
	progDone := make(chan struct{})
	go func() {
		defer close(progDone)
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				_, _, _, _, prog := engine.Status()
				fmt.Fprintf(os.Stderr, "\r  objects: %d copied, %d failed  ", prog.Completed, prog.Failed)
			case <-done:
				return
			}
		}
	}()

	if err := engine.RunOnce(ctx); err != nil {
		close(done)
		<-progDone
		// The engine has already decided a terminal status (expired, or error from
		// a failed preflight). Persisting it here matters more on the error path
		// than the success path: the pair list and the dashboard read the stored
		// status, so returning without writing leaves a dead pair looking healthy
		// until the next successful run — which for an expired pair never comes.
		persistStatus(repo, pair, engine)
		fmt.Fprintf(os.Stderr, "\nSync FAILED: %v\n", err)
		return fmt.Errorf("sync: %w", err)
	}

	close(done)
	<-progDone // wait for progress goroutine to finish

	_, _, status, _, prog := engine.Status()
	fmt.Fprintf(os.Stderr, "\r  objects: %d copied, %d failed  \n", prog.Completed, prog.Failed)
	persistStatus(repo, pair, engine)

	fmt.Printf("Sync complete for %q. Status: %s (duration: %s)\n", pair.Name, status, time.Since(start).Round(time.Second))
	return nil
}

// persistStatus writes the engine's current status onto the pair. Failures are
// logged rather than returned: the caller is already reporting the run's own
// outcome, and losing the status write must not mask it.
func persistStatus(repo *config.Repository, pair *config.SyncPair, engine *Engine) {
	_, _, status, _, _ := engine.Status()
	now := time.Now().UTC()
	pair.LastSyncAt = &now
	pair.LastSyncStatus = status
	if status == "error" {
		pair.ConsecutiveErrors++
	} else {
		pair.ConsecutiveErrors = 0
	}
	// Provisioning and the drift sweep both write to the engine's own copy of the
	// pair, so their outcomes only reach the database through here. Without this
	// the columns exist and never fill in: last_reconcile_at would be nil on every
	// start (re-sweeping the whole cache each time) and provision_error could
	// never hold a value at all.
	if err := engine.SyncProvisionState(pair); err != nil {
		slog.Warn("read engine provision state", "pair", pair.Name, "error", err)
	}
	if err := repo.UpdateSyncPair(pair); err != nil {
		slog.Warn("failed to persist pair status", "pair", pair.Name, "error", err)
	}
}

// ProvisionPair runs the target-bucket preflight for a pair that is about to be
// created or changed, outside any sync run.
//
// Pair creation calls it before writing the pair: a pair whose target cannot be
// provisioned would otherwise be persisted and then fail every object on every
// cycle, with the real cause buried in per-object errors.
func ProvisionPair(ctx context.Context, repo *config.Repository, pair *config.SyncPair, opts ProvisionOptions) error {
	tgt, err := repo.GetBucket(pair.TargetBucketID)
	if err != nil {
		return fmt.Errorf("get target bucket: %w", err)
	}
	src, err := repo.GetBucket(pair.SourceBucketID)
	if err != nil {
		return fmt.Errorf("get source bucket: %w", err)
	}

	tgtS3, err := s3client.NewClient(tgt)
	if err != nil {
		return fmt.Errorf("s3 target client: %w", err)
	}

	if opts.Now.IsZero() {
		opts.Now = time.Now().UTC()
	}
	if err := ProvisionTarget(ctx, tgtS3, src.Region, tgt, pair, opts); err != nil {
		pair.ProvisionError = err.Error()
		return err
	}
	now := opts.Now.UTC()
	pair.ProvisionedAt = &now
	pair.ProvisionError = ""
	return nil
}
