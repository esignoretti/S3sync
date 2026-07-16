package sync

import (
	"context"
	"fmt"
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

	engine := NewEngine(pair, src, tgt, srcS3, tgtS3, cacheStore)
	start := time.Now()

	if err := engine.RunOnce(ctx); err != nil {
		return fmt.Errorf("sync: %w", err)
	}

	_, _, status, _ := engine.Status()
	now := time.Now().UTC()
	pair.LastSyncAt = &now
	pair.LastSyncStatus = status
	if status == "error" {
		pair.ConsecutiveErrors++
	} else {
		pair.ConsecutiveErrors = 0
	}
	if err := repo.UpdateSyncPair(pair); err != nil {
		return fmt.Errorf("update pair status: %w", err)
	}

	fmt.Printf("Sync complete for %q. Status: %s (duration: %s)\n", pair.Name, status, time.Since(start).Round(time.Second))
	return nil
}
