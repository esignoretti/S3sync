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
	store     *cache.Store
	throttler *Throttler
	pool      *WorkerPool

	mu         sync.Mutex
	running    bool
	lastRun    time.Time
	lastStatus string
	lastError  string
	progress   *Progress
	lastResult    *Progress
	lastSucceeded int
	lastFailed    int
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
		e.lastResult = &Progress{Total: e.progress.Total, Completed: e.progress.Completed, Failed: e.progress.Failed}
	}
	e.progress.Total = 0
	e.progress.Completed = 0
	e.progress.Failed = 0
	e.lastError = ""
	e.mu.Unlock()

	timeout := 5 * time.Minute
	ctx, cancel := context.WithTimeout(ctx, timeout)
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
		e.setResult("error", fmt.Sprintf("cache cursor: %v", err))
		return fmt.Errorf("cache cursor: %w", err)
	}

	actions := make(chan SyncAction, 10000)
	diffErrCh := make(chan error, 1)

	listPage := func(ctx context.Context, token *string) (*s3.ListObjectsV2Output, error) {
		reqCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		if err := e.throttler.WaitLog(reqCtx, e.src.BucketName); err != nil {
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
	if diffErr != nil {
		e.setResult("error", fmt.Sprintf("diff: %v", diffErr))
		return fmt.Errorf("diff: %w", diffErr)
	}

	now := time.Now().UTC()
	cached := make([]*cache.CachedObject, 0, len(succeeded))
	for _, a := range succeeded {
		cached = append(cached, &cache.CachedObject{
			PairID: e.pair.ID, Key: a.Key,
			ETag: a.ETag, Size: a.Size,
			LastModified: a.LastModified, SyncedAt: now,
		})
	}
	if err := e.store.PutMany(cached); err != nil {
		slog.Warn("cache put failed", "pair", e.pair.Name, "error", err)
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
	e.setResult(status, lastErr)
	e.lastRun = time.Now()

	if e.pair.WebhookURL != "" {
		go func() {
			p := WebhookPayload{
				Event:             "sync_completed",
				PairID:            e.pair.ID,
				PairName:          e.pair.Name,
				Status:            status,
				ConsecutiveErrors: e.pair.ConsecutiveErrors,
				LastError:         lastErr,
				Succeeded:         succeededCount,
				Failed:            failed,
				StartedAt:         startTime.UTC().Format(time.RFC3339),
				CompletedAt:       now.Format(time.RFC3339),
				Source:            e.src.BucketName,
				Target:            e.tgt.BucketName,
			}
			SendWebhook(e.pair.WebhookURL, e.pair.WebhookEvents, p)
		}()
	}

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
	if !e.running && p.Total == 0 && p.Completed == 0 && e.lastResult != nil {
		p = *e.lastResult
	}
	return e.running, e.lastRun, e.lastStatus, e.lastError, p
}

func (e *Engine) LastResult() (succeeded, failed int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.lastSucceeded, e.lastFailed
}

func ptr[T any](v T) *T {
	return &v
}
