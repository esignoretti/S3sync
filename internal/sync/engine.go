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
	mu        sync.Mutex
	Total     int `json:"total"`
	Completed int `json:"completed"`
	Failed    int `json:"failed"`
}

// ProgressSnapshot is a plain-data copy of a Progress. Progress embeds a
// sync.Mutex, so returning it by value copies the lock — go vet flags that, and
// a copied lock means readers and writers can observe different mutexes. Handing
// back a snapshot keeps the mutex private to the engine.
type ProgressSnapshot struct {
	Total     int `json:"total"`
	Completed int `json:"completed"`
	Failed    int `json:"failed"`
}

func (p *Progress) Add(err bool) {
	p.mu.Lock()
	p.Completed++
	if err {
		p.Failed++
	}
	p.mu.Unlock()
}

func (p *Progress) Snapshot() (total, completed, failed int) {
	p.mu.Lock()
	total = p.Total
	completed = p.Completed
	failed = p.Failed
	p.mu.Unlock()
	return
}

type Engine struct {
	pair      *config.SyncPair
	src       *config.Bucket
	tgt       *config.Bucket
	srcS3     *s3.Client
	tgtS3     *s3.Client
	store     *cache.Store
	throttler *Throttler
	pool      *WorkerPool

	mu            sync.Mutex
	running       bool
	lastRun       time.Time
	lastStatus    string
	lastError     string
	progress      *Progress
	lastResult    *ProgressSnapshot
	lastSucceeded int
	lastFailed    int
	lastCopied    int64
	lastDeleted   int64
	lastBytes     int64
	cancel        context.CancelFunc

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
		store:     cacheStore,
		throttler: thr,
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

	e.mu.Lock()
	if e.progress.Total > 0 || e.progress.Completed > 0 {
		e.lastResult = &ProgressSnapshot{Total: e.progress.Total, Completed: e.progress.Completed, Failed: e.progress.Failed}
	}
	e.progress.Total = 0
	e.progress.Completed = 0
	e.progress.Failed = 0
	e.lastError = ""
	e.mu.Unlock()

	// No global timeout — per-request 30s timeouts handle hang protection.
	// Large buckets need unbounded listing + copy time. The caller's context
	// (e.g. s.rootCtx from serve mode) handles shutdown.
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
	startTime := time.Now()

	e.setupOnce.Do(func() {
		setupCtx, setupCancel := context.WithTimeout(ctx, 30*time.Second)
		defer setupCancel()
		if err := SetupTargetBucket(setupCtx, e.tgtS3, e.src.Region, e.tgt); err != nil {
			slog.Warn("target bucket setup", "pair", e.pair.Name, "error", err)
		}
	})

	cacheCur, closeCur, err := e.store.NewCursor(e.pair.ID)
	if err != nil {
		e.setResult("error", fmt.Sprintf("cache cursor: %v", err), time.Now())
		return fmt.Errorf("cache cursor: %w", err)
	}

	actions := make(chan SyncAction, 10000)
	diffErrCh := make(chan error, 1)

	listPage := func(ctx context.Context, token *string) (*s3.ListObjectsV2Output, error) {
		reqCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		if err := e.throttler.WaitFor(reqCtx, e.src.BucketName); err != nil {
			return nil, err
		}
		return e.srcS3.ListObjectsV2(reqCtx, &s3.ListObjectsV2Input{
			Bucket:            &e.src.BucketName,
			MaxKeys:           ptr[int32](1000),
			ContinuationToken: token,
		})
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				slog.Error("diff panic", "pair", e.pair.Name, "recover", r)
				diffErrCh <- fmt.Errorf("diff panic: %v", r)
			}
		}()
		err := StreamingDiff(ctx, listPage, cacheCur, e.pair.DeletePropagation, actions)
		diffErrCh <- err
	}()

	var succeeded []SyncAction
	var succeededCount, failed int

	if e.pair.DryRun {
		for a := range actions {
			slog.Info("dry-run", "action", a.Type, "key", a.Key)
			succeeded = append(succeeded, a)
			succeededCount++
		}
	} else {
		type poolResult struct {
			succeeded []SyncAction
			count     int
			failed    int
		}
		poolCh := make(chan poolResult, 1)
		go func() {
			s, c, f := e.pool.Run(ctx, actions)
			poolCh <- poolResult{s, c, f}
		}()
		r := <-poolCh
		succeeded, succeededCount, failed = r.succeeded, r.count, r.failed

		e.mu.Lock()
		e.lastSucceeded = succeededCount
		e.lastFailed = failed
		e.mu.Unlock()

		slog.Info("pool completed", "pair", e.pair.Name, "succeeded", succeededCount, "failed", failed)
	}

	slog.Info("waiting for diff", "pair", e.pair.Name)
	diffErr := <-diffErrCh
	slog.Info("diff completed", "pair", e.pair.Name, "err", diffErr)
	closeCur()

	// Cache succeeded actions regardless of diff error.
	// Without this, a partial listing error discards all pool work and
	// the next run starts from scratch — infinite cycle on large buckets.
	//
	// Copies are recorded; deletes are REMOVED. A deleted key left in the cache
	// is re-diffed as a deletion on every later run, so the pair never converges
	// and issues one DELETE per run forever. Only successful actions reach
	// `succeeded`, so a failed delete stays cached and is retried next run.
	//
	// Dry runs are excluded entirely: their actions were never performed, so
	// recording them would make the next real run treat the objects as already
	// synced and silently skip the work the dry run was meant to preview.
	if len(succeeded) > 0 && !e.pair.DryRun {
		now := time.Now().UTC()
		cached := make([]*cache.CachedObject, 0, len(succeeded))
		deletedKeys := make([]string, 0, len(succeeded))
		for _, a := range succeeded {
			if a.Type == ActionDelete {
				deletedKeys = append(deletedKeys, a.Key)
				continue
			}
			cached = append(cached, &cache.CachedObject{
				PairID: e.pair.ID, Key: a.Key,
				ETag: a.ETag, Size: a.Size,
				LastModified: a.LastModified, SyncedAt: now,
			})
		}
		if len(cached) > 0 {
			if err := e.store.PutMany(cached); err != nil {
				slog.Warn("cache put failed", "pair", e.pair.Name, "error", err)
			}
		}
		if len(deletedKeys) > 0 {
			if err := e.store.DeleteMany(e.pair.ID, deletedKeys); err != nil {
				slog.Warn("cache delete failed", "pair", e.pair.Name, "error", err)
			}
		}
	}

	if diffErr != nil {
		e.setResult("error", fmt.Sprintf("diff: %v", diffErr), time.Now())
		return fmt.Errorf("diff: %w", diffErr)
	}

	var copiedCount, deletedCount int64
	var totalBytes int64
	for _, a := range succeeded {
		if a.Type == ActionCopy {
			copiedCount++
		} else if a.Type == ActionDelete {
			deletedCount++
		}
		totalBytes += a.Size
	}
	e.mu.Lock()
	e.lastCopied = copiedCount
	e.lastDeleted = deletedCount
	e.lastBytes = totalBytes
	e.mu.Unlock()

	if e.pair.CacheMaxSizeMB > 0 || e.pair.CacheTTLDays > 0 {
		if _, pruneErr := e.store.PruneIfNeeded(e.pair.ID, e.pair.CacheMaxSizeMB, e.pair.CacheTTLDays); pruneErr != nil {
			slog.Warn("cache prune failed", "pair", e.pair.Name, "error", pruneErr)
		}
	}

	totalActions := succeededCount + failed
	e.mu.Lock()
	e.progress.Total = totalActions
	e.mu.Unlock()

	slog.Info("sync complete", "pair", e.pair.Name,
		"succeeded", succeededCount, "failed", failed)

	status := "ok"
	lastErr := ""
	if failed > 0 {
		status = "error"
		lastErr = fmt.Sprintf("%d worker(s) failed", failed)
	}
	e.setResult(status, lastErr, time.Now())

	// The stored pair's counter is advanced by the caller AFTER RunOnce returns
	// (RunOneShot/StartEngineLoop), so e.pair.ConsecutiveErrors still holds the
	// pre-run value here. Compute the post-run count so the webhook describes the
	// run it is reporting instead of lagging one failure behind.
	consecutiveErrors := 0
	if status == "error" {
		consecutiveErrors = e.pair.ConsecutiveErrors + 1
	}

	if e.pair.WebhookURL != "" {
		completedAt := time.Now().UTC().Format(time.RFC3339)
		go func() {
			p := WebhookPayload{
				Event:             "sync_completed",
				PairID:            e.pair.ID,
				PairName:          e.pair.Name,
				Status:            status,
				ConsecutiveErrors: consecutiveErrors,
				LastError:         lastErr,
				Succeeded:         succeededCount,
				Failed:            failed,
				StartedAt:         startTime.UTC().Format(time.RFC3339),
				CompletedAt:       completedAt,
				Source:            e.src.BucketName,
				Target:            e.tgt.BucketName,
			}
			SendWebhook(e.pair.WebhookURL, e.pair.WebhookEvents, p)
		}()
	}

	return nil
}

// setResult records the terminal status, error and completion time in a single
// locked update, so a concurrent Status() reader can never observe a status
// without its matching lastRun.
func (e *Engine) setResult(status, errMsg string, lastRun time.Time) {
	e.mu.Lock()
	e.lastStatus = status
	e.lastError = errMsg
	e.lastRun = lastRun
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

func (e *Engine) Status() (running bool, lastRun time.Time, status string, lastError string, progress ProgressSnapshot) {
	e.mu.Lock()
	defer e.mu.Unlock()
	total, completed, failed := e.progress.Snapshot()
	progress.Total = total
	progress.Completed = completed
	progress.Failed = failed
	if !e.running && progress.Total == 0 && progress.Completed == 0 && e.lastResult != nil {
		progress.Total = e.lastResult.Total
		progress.Completed = e.lastResult.Completed
		progress.Failed = e.lastResult.Failed
	}
	return e.running, e.lastRun, e.lastStatus, e.lastError, progress
}

func (e *Engine) LastResult() (succeeded, failed int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.lastSucceeded, e.lastFailed
}

func (e *Engine) LastTransfer() (copied, deleted, bytes int64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.lastCopied, e.lastDeleted, e.lastBytes
}

func ptr[T any](v T) *T {
	return &v
}
