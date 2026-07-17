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
	startTime := time.Now()

	e.setupOnce.Do(func() {
		if err := SetupTargetBucket(ctx, e.tgtS3, e.src.Region, e.tgt); err != nil {
			slog.Warn("target bucket setup", "pair", e.pair.Name, "error", err)
		}
	})

	cacheCur, closeCur, err := e.store.NewCursor(e.pair.ID)
	if err != nil {
		e.setResult("error", fmt.Sprintf("cache cursor: %v", err))
		return fmt.Errorf("cache cursor: %w", err)
	}
	defer closeCur()

	actions := make(chan SyncAction, 10000)
	diffErrCh := make(chan error, 1)

	listPage := func(ctx context.Context, token *string) (*s3.ListObjectsV2Output, error) {
		if err := e.throttler.WaitLog(ctx, e.src.BucketName); err != nil {
			return nil, err
		}
		return e.srcS3.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            &e.src.BucketName,
			MaxKeys:           ptr[int32](1000),
			ContinuationToken: token,
		})
	}

	go func() {
		err := StreamingDiff(ctx, listPage, cacheCur, e.pair.DeletePropagation, actions)
		diffErrCh <- err
	}()

	// Count actions live for progress bar by forwarding through a counter
	countedActions := make(chan SyncAction, 10000)
	go func() {
		count := 0
		for a := range actions {
			count++
			e.mu.Lock()
			e.progress.Total = count
			e.mu.Unlock()
			countedActions <- a
		}
		close(countedActions)
	}()

	var succeeded []SyncAction
	var succeededCount, failed int

	if e.pair.DryRun {
		for a := range countedActions {
			slog.Info("dry-run", "action", a.Type, "key", a.Key)
			succeeded = append(succeeded, a)
			succeededCount++
			e.mu.Lock()
			e.progress.Completed++
			e.mu.Unlock()
		}
	} else {
		type poolResult struct {
			succeeded []SyncAction
			count     int
			failed    int
		}
		poolCh := make(chan poolResult, 1)
		go func() {
			s, c, f := e.pool.Run(ctx, countedActions)
			poolCh <- poolResult{s, c, f}
		}()
		r := <-poolCh
		succeeded, succeededCount, failed = r.succeeded, r.count, r.failed
	}

	diffErr := <-diffErrCh
	if diffErr != nil {
		slog.Error("diff failed", "pair", e.pair.Name, "error", diffErr)
		e.setResult("error", fmt.Sprintf("diff: %v", diffErr))
		return diffErr
	}

	totalActions := succeededCount + failed
	e.mu.Lock()
	e.progress.Total = totalActions
	e.mu.Unlock()

	now := time.Now().UTC()
	for _, a := range succeeded {
		if err := e.store.Put(&cache.CachedObject{
			PairID: e.pair.ID, Key: a.Key,
			ETag: a.ETag, Size: a.Size,
			LastModified: a.LastModified, SyncedAt: now,
		}); err != nil {
			slog.Warn("cache put failed", "pair", e.pair.Name, "key", a.Key, "error", err)
		}
	}

	totalSucceeded := succeededCount
	totalFailed := failed

	slog.Info("sync complete", "pair", e.pair.Name,
		"succeeded", totalSucceeded, "failed", totalFailed)

	e.mu.Lock()
	e.progress.Total = totalSucceeded + totalFailed
	e.mu.Unlock()

	status := "ok"
	lastErr := ""
	if totalFailed > 0 {
		status = "error"
		lastErr = fmt.Sprintf("%d worker(s) failed", totalFailed)
	}
	e.setResult(status, lastErr)
	e.lastRun = time.Now()

	// Fire webhook (async)
	if e.pair.WebhookURL != "" {
		go func() {
			p := WebhookPayload{
				Event:            "sync_completed",
				PairID:           e.pair.ID,
				PairName:         e.pair.Name,
				Status:           status,
				ConsecutiveErrors: e.pair.ConsecutiveErrors,
				LastError:        lastErr,
				Succeeded:        totalSucceeded,
				Failed:           totalFailed,
				StartedAt:        startTime.UTC().Format(time.RFC3339),
				CompletedAt:      now.Format(time.RFC3339),
				Source:           e.src.BucketName,
				Target:           e.tgt.BucketName,
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
	return e.running, e.lastRun, e.lastStatus, e.lastError, p
}

func ptr[T any](v T) *T {
	return &v
}
