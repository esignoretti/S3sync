package sync

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/esignoretti/S3sync/internal/cache"
	"github.com/esignoretti/S3sync/internal/config"
)

type Engine struct {
	pair      *config.SyncPair
	src       *config.Bucket
	tgt       *config.Bucket
	srcS3     *s3.Client
	tgtS3     *s3.Client
	cache     *cache.Store
	throttler *Throttler
	lister    *Lister
	pool      *WorkerPool

	mu         sync.Mutex
	running    bool
	lastRun    time.Time
	lastStatus string

	setupOnce sync.Once
}

func NewEngine(pair *config.SyncPair, src, tgt *config.Bucket,
	srcS3, tgtS3 *s3.Client, cacheStore *cache.Store) *Engine {

	thr := NewThrottler(pair.MaxGetOpsPerMinute)
	return &Engine{
		pair:      pair,
		src:       src,
		tgt:       tgt,
		srcS3:     srcS3,
		tgtS3:     tgtS3,
		cache:     cacheStore,
		throttler: thr,
		lister:    NewLister(srcS3, src.BucketName, thr),
		pool: NewWorkerPool(pair.WorkerCount, tgtS3,
			src.BucketName, tgt.BucketName, thr, pair.TargetStorageClass),
	}
}

func (e *Engine) RunOnce(ctx context.Context) error {
	e.mu.Lock()
	if e.running {
		e.mu.Unlock()
		return fmt.Errorf("sync already running")
	}
	e.running = true
	e.mu.Unlock()

	defer func() {
		e.mu.Lock()
		e.running = false
		e.mu.Unlock()
	}()

	slog.Info("sync start", "pair", e.pair.Name)

	e.setupOnce.Do(func() {
		if err := SetupTargetBucket(ctx, e.tgtS3, e.src.Region, TargetConfig{
			BucketName:    e.tgt.BucketName,
			Versioning:    e.tgt.Versioning,
			ObjectLock:    e.tgt.ObjectLock,
			RetentionMode: e.tgt.RetentionMode,
			RetentionDays: e.tgt.RetentionDays,
		}); err != nil {
			slog.Warn("target bucket setup", "pair", e.pair.Name, "error", err)
		}
	})

	listing, err := e.lister.List(ctx)
	if err != nil {
		e.setStatus("error")
		return fmt.Errorf("list: %w", err)
	}

	cached, err := e.cache.List(e.pair.ID)
	if err != nil {
		e.setStatus("error")
		return fmt.Errorf("cache: %w", err)
	}

	entries := make([]cachedEntry, len(cached))
	for i, c := range cached {
		entries[i] = cachedEntry{
			Key: c.Key, ETag: c.ETag, Size: c.Size,
			LastModified: c.LastModified,
			ErrorCount:   c.ErrorCount, LastError: c.LastError,
		}
	}

	diff := Diff(listing, entries, e.pair.DeletePropagation)
	slog.Info("diff complete", "pair", e.pair.Name,
		"new_changed", len(diff.NewOrChanged),
		"delete", len(diff.ToDelete),
		"skipped", diff.Skipped)

	succeeded, failed := e.pool.Run(ctx, diff.NewOrChanged)

	delSucceeded, delFailed := e.pool.Run(ctx, diff.ToDelete)

	now := time.Now().UTC()
	for _, a := range diff.NewOrChanged {
		e.cache.Put(&cache.CachedObject{
			PairID: e.pair.ID, Key: a.Key,
			ETag: a.ETag, Size: a.Size,
			LastModified: a.LastModified, SyncedAt: now,
		})
	}
	for _, a := range diff.ToDelete {
		e.cache.Delete(e.pair.ID, a.Key)
	}

	totalSucceeded := succeeded + delSucceeded
	totalFailed := failed + delFailed

	slog.Info("sync complete", "pair", e.pair.Name,
		"succeeded", totalSucceeded, "failed", totalFailed)

	if totalFailed > 0 {
		e.setStatus("error")
	} else {
		e.setStatus("ok")
	}
	e.lastRun = time.Now()

	return nil
}

func (e *Engine) setStatus(status string) {
	e.mu.Lock()
	e.lastStatus = status
	e.mu.Unlock()
}

func (e *Engine) Status() (running bool, lastRun time.Time, status string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.running, e.lastRun, e.lastStatus
}
