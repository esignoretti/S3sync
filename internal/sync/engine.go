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

type Progress struct {
	Total     int `json:"total"`
	Completed int `json:"completed"`
	Failed    int `json:"failed"`
}

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
	lastError  string
	progress   *Progress
	cancel     context.CancelFunc

	setupOnce sync.Once
}

func NewEngine(pair *config.SyncPair, src, tgt *config.Bucket,
	srcS3, tgtS3 *s3.Client, cacheStore *cache.Store) *Engine {

	thr := NewThrottler(pair.MaxGetOpsPerMinute)
	progress := &Progress{}
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
			src.BucketName, tgt.BucketName, thr, pair.TargetStorageClass, progress),
		progress: progress,
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

	// Reset progress before any work
	e.mu.Lock()
	e.progress.Total = 0
	e.progress.Completed = 0
	e.progress.Failed = 0
	e.lastError = ""
	e.mu.Unlock()

	ctx, cancel := context.WithCancel(ctx)
	e.mu.Lock()
	if e.cancel != nil {
		e.cancel()
	}
	e.cancel = cancel
	e.mu.Unlock()

	defer func() {
		e.mu.Lock()
		e.running = false
		e.cancel = nil
		e.mu.Unlock()
		cancel()
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
		e.setResult("error", fmt.Sprintf("list: %v", err))
		return fmt.Errorf("list: %w", err)
	}

	cached, err := e.cache.List(e.pair.ID)
	if err != nil {
		e.setResult("error", fmt.Sprintf("cache: %v", err))
		return fmt.Errorf("cache: %w", err)
	}

	diff := Diff(listing, cached, e.pair.DeletePropagation)
	slog.Info("diff complete", "pair", e.pair.Name,
		"new_changed", len(diff.NewOrChanged),
		"delete", len(diff.ToDelete),
		"skipped", diff.Skipped)

	e.mu.Lock()
	e.progress.Total = len(diff.NewOrChanged) + len(diff.ToDelete)
	e.progress.Completed = 0
	e.progress.Failed = 0
	e.mu.Unlock()

	succeeded, succeededCount, failed := e.pool.Run(ctx, diff.NewOrChanged)

	delSucceeded, delSucceededCount, delFailed := e.pool.Run(ctx, diff.ToDelete)

	now := time.Now().UTC()
	for _, a := range succeeded {
		e.cache.Put(&cache.CachedObject{
			PairID: e.pair.ID, Key: a.Key,
			ETag: a.ETag, Size: a.Size,
			LastModified: a.LastModified, SyncedAt: now,
		})
	}
	// Remove from cache items that were successfully deleted
	for _, a := range delSucceeded {
		e.cache.Delete(e.pair.ID, a.Key)
	}

	totalSucceeded := succeededCount + delSucceededCount
	totalFailed := failed + delFailed

	slog.Info("sync complete", "pair", e.pair.Name,
		"succeeded", totalSucceeded, "failed", totalFailed)

	if totalFailed > 0 {
		e.setResult("error", fmt.Sprintf("%d worker(s) failed", totalFailed))
	} else {
		e.setResult("ok", "")
	}
	e.lastRun = time.Now()

	return nil
}

func (e *Engine) setResult(status, errMsg string) {
	e.mu.Lock()
	e.lastStatus = status
	e.lastError = errMsg
	e.mu.Unlock()
}

func (e *Engine) Stop() {
	e.mu.Lock()
	if e.cancel != nil {
		e.cancel()
		e.cancel = nil
	}
	e.running = false
	e.lastStatus = "stopped"
	e.mu.Unlock()
}

func (e *Engine) Status() (running bool, lastRun time.Time, status string, lastError string, progress Progress) {
	e.mu.Lock()
	defer e.mu.Unlock()
	p := Progress{}
	if e.progress != nil {
		p = *e.progress
	}
	return e.running, e.lastRun, e.lastStatus, e.lastError, p
}
