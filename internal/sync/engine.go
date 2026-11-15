package sync

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/esignoretti/S3sync/internal/cache"
	"github.com/esignoretti/S3sync/internal/config"
)

// ErrPairExpired reports a pair whose target lifecycle expiry date has passed.
// The run refuses instead of copying objects the target will delete again: per
// the design, the pair is finished rather than broken, and the recovery is
// moving target_expire_at into the future.
var ErrPairExpired = errors.New("sync pair target expiry has passed")

type Progress struct {
	mu        sync.Mutex
	Total     int `json:"total"`
	Completed int `json:"completed"`
	Failed    int `json:"failed"`
	// Pending counts objects whose bytes were copied but whose characteristics
	// were only partially applied. The copy did happen, so they are completed
	// work too; the separate count is what makes the incomplete part visible.
	Pending int `json:"pending"`
}

func (p *Progress) Add(err bool) {
	p.mu.Lock()
	p.Completed++
	if err {
		p.Failed++
	}
	p.mu.Unlock()
}

// AddPending records one object that completed without fully applying its
// characteristics.
func (p *Progress) AddPending() {
	p.mu.Lock()
	p.Pending++
	p.Completed++
	p.mu.Unlock()
}

func (p *Progress) PendingCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.Pending
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
	lastResult    *Progress
	lastSucceeded int
	lastFailed    int
	cancel        context.CancelFunc

	setupOnce sync.Once
	setupErr  error
}

func NewEngine(pair *config.SyncPair, src, tgt *config.Bucket,
	srcS3, tgtS3 *s3.Client, cacheStore *cache.Store) *Engine {

	thr := NewThrottler(pair.MaxGetOpsPerMinute)
	progress := &Progress{}
	pool := NewWorkerPool(pair.WorkerCount, srcS3, tgtS3,
		src.BucketName, tgt.BucketName, thr, pair.TargetStorageClass, progress, pair.ReplicateAttributes)
	return &Engine{
		pair:      pair,
		src:       src,
		tgt:       tgt,
		srcS3:     srcS3,
		tgtS3:     tgtS3,
		store:     cacheStore,
		throttler: thr,
		pool:      pool,
		progress:  progress,
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

	e.mu.Lock()
	if e.progress.Total > 0 || e.progress.Completed > 0 {
		e.lastResult = &Progress{
			Total: e.progress.Total, Completed: e.progress.Completed,
			Failed: e.progress.Failed, Pending: e.progress.Pending,
		}
	}
	e.progress.Total = 0
	e.progress.Completed = 0
	e.progress.Failed = 0
	e.progress.Pending = 0
	e.lastError = ""
	e.mu.Unlock()

	// A lifecycle Expiration.Date already in the past has the target deleting
	// objects as fast as this run copies them while the source still holds them,
	// so every reset or cache loss re-copies what has already expired, forever.
	// Refuse before touching anything: the pair has reached end of life.
	if e.pair.TargetExpireAt != nil && time.Now().UTC().After(*e.pair.TargetExpireAt) {
		e.setResult("expired", fmt.Sprintf("target expiry passed at %s",
			e.pair.TargetExpireAt.UTC().Format(time.RFC3339)))
		return ErrPairExpired
	}

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
		e.cancel = nil
		e.mu.Unlock()
		cancel()
	}()

	slog.Info("sync start", "pair", e.pair.Name)
	startTime := time.Now()

	// Provisioning failures are exactly what this preflight exists to prevent, so
	// they fail the run instead of being logged and swallowed: carrying on would
	// copy every object into a target that rejects it and report a run full of
	// per-object failures rather than the one real cause. setupOnce still means
	// one attempt per engine; the recorded error is what every later run returns.
	e.setupOnce.Do(func() {
		setupCtx, setupCancel := context.WithTimeout(ctx, 30*time.Second)
		defer setupCancel()
		err := ProvisionTarget(setupCtx, e.tgtS3, e.src.Region, e.tgt, e.pair,
			ProvisionOptions{Now: time.Now().UTC()})

		e.mu.Lock()
		defer e.mu.Unlock()
		if err != nil {
			e.setupErr = err
			e.pair.ProvisionError = err.Error()
			e.pair.ProvisionedAt = nil
			return
		}
		provisionedAt := time.Now().UTC()
		e.setupErr = nil
		e.pair.ProvisionError = ""
		e.pair.ProvisionedAt = &provisionedAt
	})

	e.mu.Lock()
	setupErr := e.setupErr
	e.mu.Unlock()
	if setupErr != nil {
		e.setResult("error", fmt.Sprintf("provision target: %v", setupErr))
		return fmt.Errorf("provision target: %w", setupErr)
	}

	cacheCur, closeCur, err := e.store.NewCursor(e.pair.ID)
	if err != nil {
		e.setResult("error", fmt.Sprintf("cache cursor: %v", err))
		return fmt.Errorf("cache cursor: %w", err)
	}

	actions := make(chan SyncAction, 10000)
	diffErrCh := make(chan error, 1)

	// The drift sweep is the only detector for characteristics that changed
	// without bumping the ETag or LastModified. It writes into the same channel
	// the diff does, so it runs in its own goroutine and the diff's goroutine
	// waits for it: the sweep must not overlap the diff (only the diff closes the
	// channel), and it must not run inline either, because it can emit more
	// actions than the buffer holds while nothing is reading yet. The consumer
	// below therefore starts before either producer.
	sweepDue := ShouldReconcile(e.pair, time.Now().UTC())
	sweepErrCh := make(chan error, 1)
	if sweepDue {
		go func() { sweepErrCh <- e.reconcileSweep(ctx, actions) }()
	}

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
		// The sweep writes into the same channel the diff does, and the diff is
		// what closes it, so the diff must not start until the sweep is done.
		// Waiting here (rather than running the sweep inline) is what keeps the
		// two producers from overlapping on a channel only one of them closes.
		if sweepDue {
			if err := <-sweepErrCh; err != nil {
				slog.Warn("reconcile sweep", "pair", e.pair.Name, "error", err)
			} else {
				sweptAt := time.Now().UTC()
				e.mu.Lock()
				e.pair.LastReconcileAt = &sweptAt
				e.mu.Unlock()
			}
		}
		err := StreamingDiff(ctx, listPage, cacheCur, e.pair.DeletePropagation, actions)
		diffErrCh <- err
	}()

	type poolResult struct {
		succeeded []SyncAction
		count     int
		failed    int
	}
	poolCh := make(chan poolResult, 1)

	// The consumer starts before any producer: the sweep can emit more actions
	// than the channel buffer holds, and nothing else is reading yet, so without
	// a live reader the sweep would block until the run's deadline.
	if e.pair.DryRun {
		go func() {
			var done []SyncAction
			for a := range actions {
				slog.Info("dry-run", "action", a.Type, "key", a.Key)
				done = append(done, a)
			}
			poolCh <- poolResult{succeeded: done, count: len(done)}
		}()
	} else {
		go func() {
			s, c, f := e.pool.Run(ctx, actions)
			poolCh <- poolResult{succeeded: s, count: c, failed: f}
		}()
	}

	r := <-poolCh
	succeeded, succeededCount, failed := r.succeeded, r.count, r.failed

	if !e.pair.DryRun {
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
			AttrsHash: a.AttrsHash, PendingAttrs: a.PendingAttrs,
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
		total, completed, failed := e.progress.Snapshot()
		p.Total = total
		p.Completed = completed
		p.Failed = failed
		p.Pending = e.progress.PendingCount()
	}
	if !e.running && p.Total == 0 && p.Completed == 0 && e.lastResult != nil {
		p.Total = e.lastResult.Total
		p.Completed = e.lastResult.Completed
		p.Failed = e.lastResult.Failed
		p.Pending = e.lastResult.Pending
	}
	return e.running, e.lastRun, e.lastStatus, e.lastError, p
}

// PendingAttrs reports how many objects in the last run completed with their
// characteristics only partially applied. It is what the API exposes as
// attrs_pending.
func (e *Engine) PendingAttrs() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.progress == nil {
		return 0
	}
	return e.progress.PendingCount()
}

func (e *Engine) LastResult() (succeeded, failed int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.lastSucceeded, e.lastFailed
}

func ptr[T any](v T) *T {
	return &v
}
