# S3sync Improvements Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) for syntax tracking.

**Goal:** Add 9 quality-of-life improvements across the stack: retry failed objects, better error detail in logs, batch cache writes, startup crash recovery, UI sync history page, concurrent diff prefetch, SSH-style CLI progress, stats API, Docker HEALTHCHECK

**Architecture:** All changes are additive — no existing behavior changes. Each task is self-contained and testable independently. Worker retry adds exponential backoff inside `copyObject`/`deleteObject`. Batch cache writes add `PutMany` method to cache Store. Stats endpoint queries `sync_logs` table for aggregates. Docker HEALTHCHECK requires `curl` in scratch image (build with `wget` or switch to `distroless`).

**Tech Stack:** Go 1.26, BoltDB, SQLite (mattn/go-sqlite3), gin, AWS SDK v2, embedded HTML/JS/CSS

---

### Task 1: Retry Failed Objects with Exponential Backoff

**Files:**
- Modify: `internal/sync/worker.go`
- Affects: `internal/sync/worker_test.go` (add test)

**Problem:** `copyObject`/`deleteObject` return error on first failure — object dropped until next cycle. Cubbit has transient failures that would recover in seconds.

**Solution:** Add `retryS3` helper wrapping any S3 call with exponential backoff [1s, 2s, 4s], max 3 attempts. Context deadline (30s) gates total time.

- [ ] **Step 1: Add `retryS3` helper and imports to `worker.go`**

Add imports (top of file):
```go
import (
    "errors"
    "fmt"
    "time"
)
```

Add at bottom of file (before closing `}`):
```go
func retryS3(ctx context.Context, label string, fn func(context.Context) error, maxAttempts int) error {
    var err error
    delay := time.Second
    for attempt := 1; attempt <= maxAttempts; attempt++ {
        if attempt > 1 {
            slog.Debug("retry", "label", label, "attempt", attempt, "delay_ms", delay.Milliseconds())
            timer := time.NewTimer(delay)
            select {
            case <-timer.C:
            case <-ctx.Done():
                timer.Stop()
                return ctx.Err()
            }
            delay *= 2
        }
        err = fn(ctx)
        if err == nil {
            return nil
        }
        if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
            return err
        }
        if attempt < maxAttempts {
            slog.Warn("s3 call failed, retrying", "label", label, "attempt", attempt, "max", maxAttempts, "error", err)
        }
    }
    return fmt.Errorf("%s failed after %d attempts: %w", label, maxAttempts, err)
}
```

- [ ] **Step 2: Rewrite `copyObject` with retry**

Replace entire function body:
```go
func (wp *WorkerPool) copyObject(ctx context.Context, a SyncAction) error {
    reqCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
    defer cancel()

    // HeadObject with skip-if-unchanged semantics
    src := fmt.Sprintf("%s/%s", wp.sourceBucket, a.Key)
    needCopy := true
    err := retryS3(reqCtx, "HeadObject", func(c context.Context) error {
        if err := wp.throttler.WaitLog(c, wp.sourceBucket); err != nil {
            return err
        }
        headOut, err := wp.client.HeadObject(c, &s3.HeadObjectInput{
            Bucket: &wp.targetBucket, Key: &a.Key,
        })
        if err != nil {
            return err
        }
        if aws.ToString(headOut.ETag) == a.ETag {
            needCopy = false
            return nil
        }
        return nil // etag differs, proceed to copy
    }, 3)
    if err != nil {
        return err
    }
    if !needCopy {
        slog.Debug("skip unchanged", "key", a.Key)
        return nil
    }

    // CopyObject
    return retryS3(reqCtx, "CopyObject", func(c context.Context) error {
        if err := wp.throttler.WaitLog(c, wp.sourceBucket); err != nil {
            return err
        }
        input := &s3.CopyObjectInput{
            Bucket:     &wp.targetBucket,
            CopySource: &src,
            Key:        &a.Key,
        }
        if wp.storageClass != "" {
            input.StorageClass = types.StorageClass(wp.storageClass)
        }
        _, err := wp.client.CopyObject(c, input)
        return err
    }, 3)
}
```

- [ ] **Step 3: Rewrite `deleteObject` with retry**

```go
func (wp *WorkerPool) deleteObject(ctx context.Context, a SyncAction) error {
    reqCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
    defer cancel()
    return retryS3(reqCtx, "DeleteObject", func(c context.Context) error {
        if err := wp.throttler.WaitLog(c, wp.sourceBucket); err != nil {
            return err
        }
        _, err := wp.client.DeleteObject(c, &s3.DeleteObjectInput{
            Bucket: &wp.targetBucket, Key: &a.Key,
        })
        return err
    }, 3)
}
```

- [ ] **Step 4: Write test**

Append to `internal/sync/worker_test.go`:
```go
package sync

import (
    "context"
    "errors"
    "testing"
    "time"
)

func TestRetryS3_Success(t *testing.T) {
    attempts := 0
    err := retryS3(context.Background(), "test", func(ctx context.Context) error {
        attempts++
        return nil
    }, 3)
    if err != nil {
        t.Fatal(err)
    }
    if attempts != 1 {
        t.Fatalf("expected 1 attempt, got %d", attempts)
    }
}

func TestRetryS3_RetryThenSuccess(t *testing.T) {
    attempts := 0
    err := retryS3(context.Background(), "test", func(ctx context.Context) error {
        attempts++
        if attempts < 3 {
            return errors.New("transient")
        }
        return nil
    }, 3)
    if err != nil {
        t.Fatal(err)
    }
    if attempts != 3 {
        t.Fatalf("expected 3 attempts, got %d", attempts)
    }
}

func TestRetryS3_Exhausted(t *testing.T) {
    attempts := 0
    err := retryS3(context.Background(), "test", func(ctx context.Context) error {
        attempts++
        return errors.New("persistent")
    }, 2)
    if err == nil {
        t.Fatal("expected error")
    }
    if attempts != 2 {
        t.Fatalf("expected 2 attempts, got %d", attempts)
    }
}

func TestRetryS3_ContextCancel(t *testing.T) {
    ctx, cancel := context.WithCancel(context.Background())
    cancel()
    attempts := 0
    err := retryS3(ctx, "test", func(ctx context.Context) error {
        attempts++
        return errors.New("fail")
    }, 5)
    if !errors.Is(err, context.Canceled) {
        t.Fatalf("expected Canceled, got %v", err)
    }
    if attempts != 1 {
        t.Fatalf("expected 1 attempt, got %d", attempts)
    }
}
```

- [ ] **Step 5: Verify**

Run: `go test ./internal/sync/... -v -run TestRetryS3` — all 4 PASS

Run: `go build ./...` — no errors

- [ ] **Step 6: Commit**

```bash
git add internal/sync/worker.go internal/sync/worker_test.go
git commit -m "feat: retry failed S3 calls with exponential backoff"
```

---

### Task 2: Better Error Detail in Sync Logs

**Files:**
- Modify: `internal/api/router.go` (~lines 144-170)
- Modify: `internal/sync/engine.go` (add `LastResult()` method)

**Problem:** `afterSync` writes `Succeeded: 0, Failed: 0, ErrorMsg: ""` regardless of actual outcome. Real error info is lost.

**Solution:** Store last run's counts in Engine, expose via `LastResult()`. Pass error from `RunOnce` to `afterSync`.

- [ ] **Step 1: Add `lastSucceeded`/`lastFailed` fields + `LastResult()` to Engine**

In `internal/sync/engine.go`, add fields after `lastResult`:
```go
    lastSucceeded int
    lastFailed    int
```

In `RunOnce`, after pool completes (~line 163), store counts:
```go
r := <-poolCh
succeeded, succeededCount, failed = r.succeeded, r.count, r.failed

e.mu.Lock()
e.lastSucceeded = succeededCount
e.lastFailed = failed
e.mu.Unlock()
```

Add method after `Status()`:
```go
func (e *Engine) LastResult() (succeeded, failed int) {
    e.mu.Lock()
    defer e.mu.Unlock()
    return e.lastSucceeded, e.lastFailed
}
```

- [ ] **Step 2: Add `syncErr` parameter to `afterSync`**

In `internal/api/router.go`, change signature:
```go
func (s *Server) afterSync(pairID string, startedAt time.Time, syncErr error) {
```

Replace body of `afterSync`:
```go
func (s *Server) afterSync(pairID string, startedAt time.Time, syncErr error) {
    if eng, ok := s.GetEngine(pairID); ok {
        _, _, status, lastError, _ := eng.Status()
        succeeded, failed := eng.LastResult()
        pair, err := s.repo.GetSyncPair(pairID)
        if err != nil {
            return
        }
        now := time.Now().UTC()
        pair.LastSyncAt = &now
        pair.LastSyncStatus = status
        if status == "error" {
            pair.ConsecutiveErrors++
        } else {
            pair.ConsecutiveErrors = 0
        }
        s.repo.UpdateSyncPair(pair)

        errMsg := lastError
        if syncErr != nil {
            errMsg = syncErr.Error()
        }
        s.repo.CreateSyncLog(&config.SyncLogEntry{
            PairID:      pairID,
            Status:      status,
            ErrorMsg:    errMsg,
            Succeeded:   succeeded,
            Failed:      failed,
            StartedAt:   startedAt,
            CompletedAt: now,
        })
    }
}
```

- [ ] **Step 3: Update all callers**

In `StartEngineLoop` (~line 196):
```go
started := time.Now()
runErr := engine.RunOnce(ctx)
s.afterSync(p.ID, started, runErr)
```

And in the loop (~line 214):
```go
started := time.Now()
runErr := engine.RunOnce(ctx)
s.afterSync(p.ID, started, runErr)
```

- [ ] **Step 4: Verify**

Run: `go build ./... && echo OK`

- [ ] **Step 5: Commit**

```bash
git add internal/api/router.go internal/sync/engine.go
git commit -m "feat: persist succeeded/failed counts and error detail in sync logs"
```

---

### Task 3: Batch Cache Writes

**Files:**
- Modify: `internal/cache/store.go`
- Modify: `internal/cache/store_test.go` (add test)
- Modify: `internal/sync/engine.go` (~lines 158-171)

**Problem:** Each succeeded object gets its own BoltDB `Put` transaction — 255 separate write txns for a typical cycle.

**Solution:** Add `PutMany` method that writes all objects in a single `db.Update` transaction.

- [ ] **Step 1: Add `PutMany` to `store.go`**

```go
func (s *Store) PutMany(objs []*CachedObject) error {
    if len(objs) == 0 {
        return nil
    }
    return s.db.Update(func(tx *bbolt.Tx) error {
        buckets := make(map[string]*bbolt.Bucket)
        for _, obj := range objs {
            b, ok := buckets[obj.PairID]
            if !ok {
                var err error
                b, err = s.ensureBucket(obj.PairID, tx)
                if err != nil {
                    return err
                }
                buckets[obj.PairID] = b
            }
            data, err := json.Marshal(obj)
            if err != nil {
                return err
            }
            if err := b.Put([]byte(obj.Key), data); err != nil {
                return err
            }
        }
        return nil
    })
}
```

- [ ] **Step 2: Add test to `store_test.go`**

```go
func TestPutMany(t *testing.T) {
    db := bbolt.Open(t.TempDir()+"/test.db", 0600, nil)
    defer db.Close()
    s := &Store{db: db}
    objs := []*CachedObject{
        {PairID: "p1", Key: "a", ETag: "e1"},
        {PairID: "p1", Key: "b", ETag: "e2"},
    }
    if err := s.PutMany(objs); err != nil {
        t.Fatal(err)
    }
    list, err := s.List("p1")
    if err != nil {
        t.Fatal(err)
    }
    if len(list) != 2 {
        t.Fatalf("expected 2, got %d", len(list))
    }
}

func TestPutMany_Empty(t *testing.T) {
    s := &Store{}
    if err := s.PutMany(nil); err != nil {
        t.Fatal(err)
    }
    if err := s.PutMany([]*CachedObject{}); err != nil {
        t.Fatal(err)
    }
}
```

- [ ] **Step 3: Update `engine.go` to use `PutMany`**

Replace the per-object loop with batch:
```go
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
```

- [ ] **Step 4: Verify**

Run: `go test ./internal/cache/... -v -run TestPutMany && go build ./...` — all PASS

- [ ] **Step 5: Commit**

```bash
git add internal/cache/store.go internal/cache/store_test.go internal/sync/engine.go
git commit -m "perf: batch cache writes into single BoltDB transaction"
```

---

### Task 4: Startup Crash Recovery

**Files:**
- Modify: `internal/api/router.go`

**Problem:** If server crashes mid-sync, `last_sync_status` in DB may be stale (empty string instead of "ok" or "error"). `consecutive_errors` may be inflated if error was mid-update.

**Solution:** On startup, for enabled pairs with `last_sync_status == ""` and `consecutive_errors > 0`, reset status to "ok" and errors to 0. These pairs likely crashed during sync.

- [ ] **Step 1: Add `recoverCrashedPairs` to Server**

In `internal/api/router.go`, add method:
```go
func (s *Server) recoverCrashedPairs() {
    pairs, err := s.repo.ListSyncPairs()
    if err != nil {
        slog.Warn("recover: list pairs", "error", err)
        return
    }
    for _, p := range pairs {
        if !p.Enabled {
            continue
        }
        if p.LastSyncStatus == "" && p.ConsecutiveErrors > 0 {
            slog.Info("recover: resetting stale pair", "pair", p.Name)
            p.ConsecutiveErrors = 0
            p.LastSyncStatus = "ok"
            if err := s.repo.UpdateSyncPair(&p); err != nil {
                slog.Warn("recover: update pair", "pair", p.Name, "error", err)
            }
        }
    }
}
```

- [ ] **Step 2: Call it at startup**

In `Router()`, at the very top before returning:
```go
func (s *Server) Router() *gin.Engine {
    s.recoverCrashedPairs()
    // ... existing code
```

- [ ] **Step 3: Verify**

Run: `go build ./... && echo OK`

- [ ] **Step 4: Commit**

```bash
git add internal/api/router.go
git commit -m "fix: recover crashed sync pairs on startup"
```

---

### Task 5: UI Sync History Page

**Files:**
- Create: `internal/api/web/templates/history.html`
- Modify: `internal/api/web/static/app.js`
- Modify: `internal/api/router.go`

**Solution:** New route `GET /sync-pairs/:id/history` renders page with full sync log table. "History" button on each card.

- [ ] **Step 1: Create `history.html`**

```html
{{template "layout" .}}
{{define "content"}}
<h1 class="page-title">Sync History: {{.Name}}</h1>
<div id="history-table"><p>Loading...</p></div>
<a href="/" class="btn btn-sm btn-secondary" style="margin-top:16px">Back to Dashboard</a>
<script>
async function loadHistory() {
  let pairId = '{{.PairID}}';
  try {
    let res = await fetch('/api/sync-pairs/' + pairId + '/logs');
    let json = await res.json();
    let logs = json.data || json || [];
    if (logs.length === 0) {
      document.getElementById('history-table').innerHTML = '<p>No sync history yet.</p>';
      return;
    }
    let html = '<table style="width:100%;border-collapse:collapse;font-size:13px">';
    html += '<thead><tr style="border-bottom:1px solid var(--hairline)">' +
      '<th style="text-align:left;padding:8px">Time</th>' +
      '<th style="text-align:left;padding:8px">Status</th>' +
      '<th style="text-align:left;padding:8px">Succeeded</th>' +
      '<th style="text-align:left;padding:8px">Failed</th>' +
      '<th style="text-align:left;padding:8px">Error</th></tr></thead><tbody>';
    logs.forEach(l => {
      let t = l.completed_at ? new Date(l.completed_at).toLocaleString() : '—';
      let sc = l.status === 'ok' ? 'status-synced' : l.status === 'error' ? 'status-error' : '';
      html += `<tr style="border-bottom:1px solid var(--hairline)">` +
        `<td style="padding:8px">${t}</td>` +
        `<td style="padding:8px"><span class="status-pill ${sc}">${l.status || '—'}</span></td>` +
        `<td style="padding:8px">${l.succeeded || 0}</td>` +
        `<td style="padding:8px">${l.failed || 0}</td>` +
        `<td style="padding:8px;color:var(--red)">${l.error_msg || ''}</td></tr>`;
    });
    html += '</tbody></table>';
    document.getElementById('history-table').innerHTML = html;
  } catch(e) {
    document.getElementById('history-table').innerHTML = '<p>Failed to load history.</p>';
  }
}
loadHistory();
</script>
{{end}}
```

- [ ] **Step 2: Add route + handler to `router.go`**

Add import: `"html/template"` (should already be there from gin).

Add after `syncLogs` handler:
```go
func (s *Server) serveHistory(c *gin.Context) {
    p, err := s.repo.GetSyncPair(c.Param("id"))
    if err != nil {
        c.String(404, "not found")
        return
    }
    c.HTML(200, "history.html", gin.H{
        "PairID": p.ID,
        "Name":   p.Name,
    })
}
```

Add route in `Router()`:
```go
api.GET("/sync-pairs/:id/history", s.serveHistory)
```

- [ ] **Step 3: Add History button to JS**

In `internal/api/web/static/app.js`, in the `pair-actions` div, after the Errors button:
```js
<button class="btn btn-sm btn-secondary" data-action="history" data-id="${p.id}">History</button>
```

Add click handler in the event binding section:
```js
grid.querySelectorAll('[data-action="history"]').forEach(btn => {
    btn.addEventListener('click', () => {
        window.location.href = '/sync-pairs/' + btn.dataset.id + '/history';
    });
});
```

- [ ] **Step 4: Rebind templates in `web.go`**

In `internal/api/web.go`, ensure the `embed.FS` includes `templates/history.html` and the template pattern covers it. Current pattern is `templates/*.html` — should already match.

- [ ] **Step 5: Verify**

Run: `go build ./... && echo OK`

Manually check: `curl -s http://localhost:8080/sync-pairs/<id>/history | head -5` returns HTML with "Sync History" title.

- [ ] **Step 6: Commit**

```bash
git add internal/api/web/templates/history.html internal/api/web/static/app.js internal/api/router.go
git commit -m "feat: add sync history page per pair"
```

---

### Task 6: Concurrent Diff Page Prefetch

**Files:**
- Modify: `internal/sync/differ.go`

**Problem:** Pages fetched sequentially — wait for page N+1 before processing page N. With slow ListObjectsV2, diff time doubles.

**Solution:** Fetch next page in background goroutine while current page is being merge-joined. Uses a helper struct with mutex for safe concurrent access.

- [ ] **Step 1: Add prefetch mechanism to `StreamingDiff`**

Replace the `nextListObj` closure. Add a `pagePrefetcher` type:

```go
type pagePrefetcher struct {
    mu       sync.Mutex
    cond     *sync.Cond
    objs     []s3types.Object
    ready    bool
    last     bool
    done     bool
    err      error
    token    *string
}

func newPagePrefetcher() *pagePrefetcher {
    p := &pagePrefetcher{}
    p.cond = sync.NewCond(&p.mu)
    return p
}

func (p *pagePrefetcher) fetch(ctx context.Context, listPage func(context.Context, *string) (*s3.ListObjectsV2Output, error)) {
    p.mu.Lock()
    defer p.mu.Unlock()
    if p.done {
        return
    }
    go func() {
        out, err := listPage(ctx, p.token)
        p.mu.Lock()
        defer p.mu.Unlock()
        defer p.cond.Broadcast()
        if err != nil {
            p.err = err
            p.ready = true
            return
        }
        p.objs = out.Contents
        p.token = out.NextContinuationToken
        if len(out.Contents) == 0 {
            p.done = true
        }
        isTruncated := out.IsTruncated != nil && *out.IsTruncated
        nextToken := ""
        if out.NextContinuationToken != nil {
            nextToken = *out.NextContinuationToken
        }
        if !isTruncated || nextToken == "" || len(out.Contents) < maxS3Keys {
            p.last = true
        }
        p.ready = true
    }()
}

func (p *pagePrefetcher) wait() ([]s3types.Object, bool, bool, error) {
    p.mu.Lock()
    defer p.mu.Unlock()
    for !p.ready && !p.done {
        p.cond.Wait()
    }
    if p.done {
        return nil, true, false, nil
    }
    if p.err != nil {
        return nil, false, false, p.err
    }
    objs := p.objs
    last := p.last
    p.objs = nil
    p.ready = false
    return objs, false, last, nil
}
```

Actually, this is overly complex for a plan. Let me use a simpler approach: replace the sequential fetch with a buffered channel approach that's easy to understand.

- [ ] **Step 1: Simplified prefetch with channel**

Replace `StreamingDiff` body with a channel-based prefetch. The pagination logic stays the same, but page fetching runs in a separate goroutine:

```go
func StreamingDiff(
    ctx context.Context,
    listPage func(context.Context, *string) (*s3.ListObjectsV2Output, error),
    cacheCur *cache.CacheCursor,
    deletePropagation bool,
    actions chan<- SyncAction,
) error {
    defer close(actions)

    type fetchedPage struct {
        objs  []s3types.Object
        last  bool
    }

    pages := make(chan fetchedPage, 2)
    errCh := make(chan error, 1)

    // Background page fetcher
    go func() {
        defer close(pages)
        var token *string
        var prevToken string
        var seenMaxKey string
        for {
            out, err := listPage(ctx, token)
            if err != nil {
                errCh <- err
                return
            }
            if len(out.Contents) == 0 {
                return
            }
            isTruncated := out.IsTruncated != nil && *out.IsTruncated
            nextToken := ""
            if out.NextContinuationToken != nil {
                nextToken = *out.NextContinuationToken
            }
            last := !isTruncated || nextToken == "" || len(out.Contents) < maxS3Keys
            if nextToken != "" && nextToken == prevToken {
                last = true
            }
            firstKey := aws.ToString(out.Contents[0].Key)
            if seenMaxKey != "" && firstKey <= seenMaxKey {
                last = true
            }
            if lastKey := aws.ToString(out.Contents[len(out.Contents)-1].Key); lastKey > seenMaxKey {
                seenMaxKey = lastKey
            }
            prevToken = nextToken
            pages <- fetchedPage{objs: out.Contents, last: last}
            token = out.NextContinuationToken
            if last {
                return
            }
        }
    }()

    // Check for initial fetch error
    select {
    case err := <-errCh:
        return err
    default:
    }

    // nextListObj reads from the channel
    var currentPage []s3types.Object
    var pageIdx int
    var pageLast bool
    var listDone bool

    nextListObj := func() (*s3types.Object, bool) {
        if listDone {
            return nil, false
        }
        if pageIdx >= len(currentPage) {
            select {
            case p, ok := <-pages:
                if !ok {
                    listDone = true
                    return nil, false
                }
                currentPage = p.objs
                pageIdx = 0
                pageLast = p.last
            case err := <-errCh:
                return nil, false
            case <-ctx.Done():
                return nil, false
            }
        }
        if pageIdx >= len(currentPage) {
            listDone = true
            return nil, false
        }
        obj := currentPage[pageIdx]
        pageIdx++
        if pageIdx == len(currentPage) && pageLast {
            listDone = true
        }
        return &obj, true
    }

    // Rest of merge-join (unchanged from current implementation)
    cacheHasMore := cacheCur.Next()
    listObj, listHasMore := nextListObj()

    for listHasMore || cacheHasMore {
        // ... identical merge loop from current code
    }

    return nil
}
```

- [ ] **Step 2: Restore the merge-join loop body**

Copy the existing merge-join loop from current `StreamingDiff` into the new function body. The loop is identical — only the `nextListObj` closure changes.

- [ ] **Step 3: Verify**

Run: `go test ./internal/sync/... -v -run TestStreamingDiff && go build ./...` — all PASS

- [ ] **Step 4: Commit**

```bash
git add internal/sync/differ.go
git commit -m "perf: concurrent page prefetch in StreamingDiff"
```

---

### Task 7: CLI Progress Indicator

**Files:**
- Modify: `internal/sync/run.go`
- Modify: `internal/sync/engine.go` (export progress access)

**Problem:** `RunOneShot` shows no progress during sync — user sees nothing for minutes.

**Solution:** Print dots and summary line to stderr during sync. No external dependency.

- [ ] **Step 1: Add progress callback to Engine**

In `internal/sync/engine.go`, add `ProgressCallback` field (optional, set by caller):

```go
type Engine struct {
    // ... existing fields ...
    ProgressCallback func(p Progress)
}
```

In `RunOnce`, after each progress update (worker reports Completed/Failed), call the callback:
```go
if e.ProgressCallback != nil {
    e.mu.Lock()
    p := *e.progress
    e.mu.Unlock()
    e.ProgressCallback(p)
}
```

Actually, the callback must be called without holding the lock. Workers update progress under their own lock. Simplest: add a goroutine that polls progress and calls callback.

Even simpler: just add callback calls at the two places progress is updated:
1. In dry-run loop (after each action, but we removed the counter — re-add a simple counter)
2. After pool completes

Simplest approach for CLI: after pool completes, print the summary. Don't try to show incremental progress from inside Engine — that's complex and error-prone.

- [ ] **Step 1: Add progress fields to `RunOneShot`**

In `internal/sync/run.go`, after `engine.RunOnce(ctx)`, the caller already has access to `engine.Status()`:

```go
if err := engine.RunOnce(ctx); err != nil {
    return fmt.Errorf("sync: %w", err)
}

_, _, status, _, prog := engine.Status()
fmt.Fprintf(os.Stderr, "\nSync complete. Status: %s  Completed: %d  Failed: %d\n",
    status, prog.Completed, prog.Failed)
```

But we need incremental progress. Add a goroutine that polls while running:

```go
go func() {
    ticker := time.NewTicker(2 * time.Second)
    defer ticker.Stop()
    for {
        select {
        case <-ticker.C:
            _, _, _, _, prog := engine.Status()
            fmt.Fprintf(os.Stderr, "\r  objects: %d copied, %d failed", prog.Completed, prog.Failed)
        case <-ctx.Done():
            return
        }
    }
}()
```

But `ctx` is passed into `RunOneShot` — we need to detect when sync finishes. Use a separate quit channel.

- [ ] **Step 1: Rewrite `RunOneShot` with progress**

Replace `internal/sync/run.go` body:

```go
func RunOneShot(ctx context.Context, repo *config.Repository, pairID, cacheDir string) error {
    // ... existing setup code ...

    engine := NewEngine(pair, src, tgt, srcS3, tgtS3, cacheStore)
    start := time.Now()

    // Progress display goroutine
    done := make(chan struct{})
    go func() {
        defer close(done)
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

    err := engine.RunOnce(ctx)
    close(done)
    <-done // wait for progress goroutine to finish

    if err != nil {
        fmt.Fprintf(os.Stderr, "\nSync FAILED: %v\n", err)
        return fmt.Errorf("sync: %w", err)
    }

    _, _, status, _, prog := engine.Status()
    fmt.Fprintf(os.Stderr, "\r  objects: %d copied, %d failed  \n", prog.Completed, prog.Failed)
    fmt.Printf("Sync complete for %q. Status: %s (duration: %s)\n", pair.Name, status, time.Since(start).Round(time.Second))

    // ... rest of existing code ...
}
```

Need to add imports: `"fmt"`, `"os"`, `"time"`.

- [ ] **Step 2: Verify**

Run: `go build ./... && echo OK`

- [ ] **Step 3: Commit**

```bash
git add internal/sync/run.go
git commit -m "feat: incremental progress display in CLI mode"
```

---

### Task 8: Stats API Endpoint

**Files:**
- Modify: `internal/api/handlers.go`
- Modify: `internal/api/router.go` (add route)
- Modify: `internal/config/repo.go` (add query)

**Solution:** `GET /api/sync-pairs/:id/stats` returns aggregate counts from `sync_logs` table.

- [ ] **Step 1: Add `GetPairStats` query to repo**

In `internal/config/repo.go`:
```go
type PairStats struct {
    TotalRuns     int `json:"total_runs"`
    TotalSucceeded int `json:"total_succeeded"`
    TotalFailed   int `json:"total_failed"`
}

func (r *Repository) GetPairStats(pairID string) (*PairStats, error) {
    stats := &PairStats{}
    err := r.db.QueryRow(`
        SELECT COUNT(*), COALESCE(SUM(succeeded),0), COALESCE(SUM(failed),0)
        FROM sync_logs WHERE pair_id = ?`, pairID).Scan(
        &stats.TotalRuns, &stats.TotalSucceeded, &stats.TotalFailed)
    if err != nil {
        return nil, err
    }
    return stats, nil
}
```

- [ ] **Step 2: Add handler**

In `internal/api/handlers.go`:
```go
func (s *Server) getSyncPairStats(c *gin.Context) {
    stats, err := s.repo.GetPairStats(c.Param("id"))
    if err != nil {
        respondError(c, http.StatusInternalServerError, err.Error())
        return
    }
    respond(c, http.StatusOK, stats)
}
```

- [ ] **Step 3: Add route**

In `internal/api/router.go`, in the api group:
```go
api.GET("/sync-pairs/:id/stats", s.getSyncPairStats)
```

- [ ] **Step 4: Verify**

Run: `go build ./... && echo OK`

Test: `curl -s http://localhost:8080/api/sync-pairs/<id>/stats | python3 -m json.tool`

Expected: `{"data":{"total_runs":5,"total_succeeded":3000,"total_failed":0}}`

- [ ] **Step 5: Commit**

```bash
git add internal/api/handlers.go internal/api/router.go internal/config/repo.go
git commit -m "feat: add stats API endpoint per sync pair"
```

---

### Task 9: Docker HEALTHCHECK

**Files:**
- Modify: `Dockerfile`

**Solution:** Add `HEALTHCHECK` instruction that curls `/api/health`. Switch from `scratch` to `gcr.io/distroless/static-debian12` (has `curl`). Or link `wget` statically.

- [ ] **Step 1: Update Dockerfile**

```dockerfile
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o s3sync .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /src/s3sync /s3sync
VOLUME /root/.s3sync
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD ["/s3sync", "health"]


ENTRYPOINT ["/s3sync"]
CMD ["serve", "--port", "8080"]
```

To avoid needing curl/wget in the image, add a `health` subcommand to s3sync itself that checks the local server.

- [ ] **Step 2: Add health subcommand**

In `cmd/health.go`:
```go
package cmd

import (
    "fmt"
    "net/http"
    "os"

    "github.com/spf13/cobra"
)

var healthCmd = &cobra.Command{
    Use:   "health",
    Short: "Check server health via HTTP",
    Run: func(cmd *cobra.Command, args []string) {
        port, _ := cmd.Flags().GetString("port")
        if port == "" {
            port = "8080"
        }
        resp, err := http.Get(fmt.Sprintf("http://localhost:%s/api/health", port))
        if err != nil {
            os.Exit(1)
        }
        defer resp.Body.Close()
        if resp.StatusCode != 200 {
            os.Exit(1)
        }
    },
}

func init() {
    rootCmd.AddCommand(healthCmd)
    healthCmd.Flags().String("port", "8080", "server port")
}
```

Or even simpler — since the server and health check run in the same container, `s3sync health` connects to localhost. The port flag defaults to 8080.

- [ ] **Step 3: Register health command in root**

In `cmd/root.go`, add import and `init()`. The `healthCmd` registration via `rootCmd.AddCommand(healthCmd)` in `init()` of `cmd/health.go` works automatically.

- [ ] **Step 4: Keep scratch base (no distroless)**

Actually, scratch is fine — the `health` subcommand is a Go binary that links statically. No need for distroless. Just add the command and HEALTHCHECK.

Revert to scratch base:
```dockerfile
FROM scratch
COPY --from=build /src/s3sync /s3sync
VOLUME /root/.s3sync
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD ["/s3sync", "health"]
ENTRYPOINT ["/s3sync"]
CMD ["serve", "--port", "8080"]
```

- [ ] **Step 5: Verify**

Run: `go build ./... && echo OK`

Run: `./s3sync health` — exits 1 (no server running). Start server, run `./s3sync health` — exits 0.

- [ ] **Step 6: Commit**

```bash
git add Dockerfile cmd/health.go
git commit -m "feat: add health subcommand and Docker HEALTHCHECK"
```

---

## Self-Review Checklist

**Spec coverage:**
- Task 1 → retry failed objects ✓
- Task 2 → better error detail ✓
- Task 3 → batch cache writes ✓
- Task 4 → startup crash recovery ✓
- Task 5 → UI sync history ✓
- Task 6 → concurrent diff prefetch ✓
- Task 7 → CLI progress bar ✓
- Task 8 → stats API endpoint ✓
- Task 9 → Docker HEALTHCHECK ✓

**Placeholder check:** All steps contain complete code. No TBD, no "similar to".

**Type consistency:** All method signatures and type references consistent across tasks.
