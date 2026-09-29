# S3sync Review-Findings Remediation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix every confirmed finding from the S3sync code review — correctness of sync, data-loss vectors, credential handling, deployment breakage, and the hygiene items — with a failing test first for each.

**Architecture:** Targeted repairs to existing packages, no restructuring. The sync engine keeps its streaming diff + worker-pool design; we correct its pagination stop-signal, its not-found handling on `HeadObject`, its cache write-back for deletes, and its dry-run branch. Supporting fixes land in `internal/cache`, `internal/config`, `internal/api`, `internal/s3client`, plus Docker/CI/docs.

**Tech Stack:** Go 1.26, AWS SDK v2, bbolt, SQLite (modernc), gin, cobra, Prometheus client.

**Spec:** `/Users/esignoretti/S3sync-review/CODE_REVIEW.md`

## Global Constraints

- Go 1.26.4 (as declared in `go.mod`); do not bump the language version.
- Module path is `github.com/esignoretti/S3sync`.
- No new third-party dependencies. Everything needed is already in `go.mod` (`errors`, `net/http`, `net/url`, `database/sql` are stdlib).
- Public JSON API field names are a compatibility surface: `access_key`, `secret_key`, `bucket_name`, `object_lock`, `retention_mode`, `retention_days`, `sync_interval`, `worker_count`, `delete_propagation`, `dry_run`, `webhook_url`, `cache_max_size_mb`, `cache_ttl_days` must keep working unchanged.
- Credentials must never be serialised back out of the API (`Bucket.AccessKey`/`SecretKey` stay `json:"-"` for marshalling).
- Every task ends green: `go test ./... && go vet ./...` with zero findings.
- Test files use the existing `package sync` / `package config` / `package cache` internal-test convention already in the repo.

## Review Focus

The input classes and failure modes the review found that no existing test exercises. Each line's test is added in the task named beside it.

1. An S3-compatible provider returning `IsTruncated=true` with a short page — a reasonable person expects every object to sync; the code silently stops listing. (Task 3)
2. A key containing `#`, a space, or `+` — a reasonable person expects a byte-identical copy; a mis-encoded `CopySource` copies the wrong object or nothing. (Task 1)
3. A transient `5xx` during listing with delete propagation on — a reasonable person expects a retry, not deletion of valid target data. (Task 2, Task 3)
4. A `HeadObject` that fails transiently (503) on an object that genuinely needs copying — a reasonable person expects a retry; the current code fails the object permanently. (Task 1)
5. Posting `access_key`/`secret_key` to the REST API — a reasonable person expects them stored; `PUT` must not wipe existing credentials when the field is omitted. (Task 5)

---

### Task 1: Correct `HeadObject` not-found handling and restore HEAD retry

**Files:**
- Modify: `internal/sync/worker.go:93-142` (`copyObject`), add `isNotFound` helper near `retryS3` (`internal/sync/worker.go:171`)
- Test: `internal/sync/worker_test.go`

**Interfaces:**
- Consumes: `retryS3(ctx context.Context, label string, fn func(context.Context) error, maxAttempts int) error` (existing, `worker.go:171`); `WorkerPool.copySource(key string) string` (existing, `worker.go:163`).
- Produces: `func isNotFound(err error) bool` — package-level in `internal/sync`.

- [ ] **Step 1: Write the failing test**

Three tests in `internal/sync/worker_test.go`. The mock must return a **valid** `CopyObject` XML body (`<?xml version="1.0" encoding="UTF-8"?><CopyObjectResult><ETag>"abc"</ETag><LastModified>2026-01-01T00:00:00.000Z</LastModified></CopyObjectResult>`) — an empty 200 body deserialises to `deserialization failed, received empty response payload`, which is what made the original repro inconclusive.

```go
func TestCopyObjectHeadNotFoundStillCopies(t *testing.T)     // HEAD→404, COPY→200 XML; assert 1 CopyObject issued, err==nil
func TestCopyObjectHead404WithEmptyBodyStillCopies(t *testing.T) // HEAD→404 with NO error XML body; assert 1 CopyObject issued
func TestCopyObjectHeadTransientErrorIsRetried(t *testing.T)  // HEAD→503,503,200; assert 3 HEAD calls and 1 CopyObject
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/sync/ -run 'TestCopyObjectHead' -v`
Expected: `TestCopyObjectHead404WithEmptyBodyStillCopies` FAILS (the bare `*types.NotFound` assertion does not match a 404 carrying no deserialisable error body); `TestCopyObjectHeadTransientErrorIsRetried` FAILS with 1 HEAD call, not 3.

- [ ] **Step 3: Implement `isNotFound` and rework the HEAD block in `copyObject`**

`isNotFound` must accept both the modelled error and the raw HTTP status:

```go
func isNotFound(err error) bool {
	var nf *types.NotFound
	if errors.As(err, &nf) {
		return true
	}
	var re *smithyhttp.ResponseError
	if errors.As(err, &re) {
		return re.HTTPStatusCode() == http.StatusNotFound
	}
	return false
}
```

Wrap the HEAD in `retryS3(reqCtx, "HeadObject", …, 3)`, and inside the callback return `nil` (not the error) when `isNotFound(err)` so the retry helper sees success and `needCopy` stays `true`. Add `net/http` and `github.com/aws/smithy-go/transport/http` to the import block.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/sync/ -run 'TestCopyObjectHead' -v`
Expected: all three PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/sync/worker.go internal/sync/worker_test.go
git commit -m "fix(sync): treat HEAD 404 as needs-copy and retry transient HEAD errors"
```

---

### Task 2: Make delete propagation converge

**Files:**
- Modify: `internal/cache/store.go` (add `DeleteMany` after `PutMany`, `store.go:76`), `internal/sync/engine.go:204-221` (cache write-back)
- Test: `internal/cache/store_test.go`, `internal/sync/engine_test.go` (new file)

**Interfaces:**
- Consumes: `Store.PutMany(objs []*CachedObject) error` (existing); `Store.Delete(pairID, key string) error` (existing, `store.go:78`).
- Produces: `func (s *Store) DeleteMany(pairID string, keys []string) error` — one bbolt transaction, no-op on empty slice or absent bucket.

- [ ] **Step 1: Write the failing tests**

```go
// internal/cache/store_test.go
func TestDeleteManyRemovesAllKeys(t *testing.T)   // Put 3, DeleteMany 2, List → 1 remains
func TestDeleteManyEmptyIsNoOp(t *testing.T)      // DeleteMany(nil) on absent bucket → nil error

// internal/sync/engine_test.go
func TestDeleteConvergesAcrossRuns(t *testing.T)  // 2 runs with delete propagation: total DELETE calls == 1, cache empty of the deleted key
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/cache/ -run TestDeleteMany -v && go test ./internal/sync/ -run TestDeleteConverges -v`
Expected: `DeleteMany` undefined (build failure); convergence test reports the key deleted twice.

- [ ] **Step 3: Implement `DeleteMany` and partition the cache write-back**

`DeleteMany` mirrors `PutMany`'s transaction shape. In `engine.go`, partition `succeeded` before writing: copies go to `PutMany`, deletes collect into a `deletedKeys []string` that goes to `DeleteMany(e.pair.ID, deletedKeys)`. Both calls stay guarded by `len(...) > 0`. Log a `slog.Warn` on either error, as `PutMany` already does.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/cache/ ./internal/sync/ -v`
Expected: PASS, and no pre-existing test regresses.

- [ ] **Step 5: Commit**

```bash
git add internal/cache/store.go internal/cache/store_test.go internal/sync/engine.go internal/sync/engine_test.go
git commit -m "fix(sync): remove deleted keys from cache so delete propagation converges"
```

---

### Task 3: Trust only the API's pagination signals

**Files:**
- Modify: `internal/sync/differ.go:80` (`last :=` expression), remove the now-unused `maxS3Keys` const at `differ.go:30` if nothing else references it
- Test: `internal/sync/differ_test.go`

**Interfaces:**
- Consumes: `StreamingDiff(ctx, listPage, cacheCur, deletePropagation, actions) error` (existing signature, unchanged).
- Produces: no new exported symbols; behaviour change only.

- [ ] **Step 1: Write the failing tests**

```go
func TestStreamingDiffFollowsAllPagesWhenPageIsShort(t *testing.T) // 4 pages × 500 objs, all IsTruncated=true + token → 2000 actions
func TestStreamingDiffStopsWhenNotTruncated(t *testing.T)          // IsTruncated=false → 1 page only
func TestStreamingDiffRepeatedTokenTerminates(t *testing.T)        // same token twice → no infinite loop, no error
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/sync/ -run TestStreamingDiff -v`
Expected: `TestStreamingDiffFollowsAllPagesWhenPageIsShort` FAILS with 500 actions, not 2000.

- [ ] **Step 3: Implement the corrected stop signal**

Replace the `last` expression with the API signals alone:

```go
last := !isTruncated || nextToken == ""
```

Keep the `nextToken == prevToken` repeat guard and the `seenMaxKey` monotonicity guard immediately below it — both are defensive and the tests in Step 1 pin their behaviour.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/sync/ -run TestStreamingDiff -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/sync/differ.go internal/sync/differ_test.go
git commit -m "fix(sync): paginate on IsTruncated/NextContinuationToken only"
```

---

### Task 4: Keep dry runs out of the cache

**Files:**
- Modify: `internal/sync/engine.go:168-221` (dry-run branch and cache write-back)
- Test: `internal/sync/engine_test.go`

**Interfaces:**
- Consumes: `Engine.pair.DryRun` (existing field), `Store.PutMany`, `Store.DeleteMany` (Task 2).
- Produces: no new symbols.

- [ ] **Step 1: Write the failing test**

```go
func TestDryRunDoesNotWriteCache(t *testing.T) // DryRun pair, 1 source object → after RunOnce the pair's cache is empty and a second non-dry run still copies it
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/sync/ -run TestDryRun -v`
Expected: FAIL — 1 cache entry present after the dry run.

- [ ] **Step 3: Guard the cache write-back on dry run**

Both the `PutMany` and the Task 2 `DeleteMany` calls must be inside a `if !e.pair.DryRun { … }` block. Leave the dry-run branch appending to `succeeded` for logging, but the persisted cache stays untouched. Update the existing comment above the block so it states the dry-run exclusion.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/sync/ -run TestDryRun -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/sync/engine.go internal/sync/engine_test.go
git commit -m "fix(sync): dry run no longer writes the sync cache"
```

---

### Task 5: Bind bucket credentials in the REST API

**Files:**
- Modify: `internal/api/handlers.go:29-40` (`bucketInput` already exists and is correct — verify), `handlers.go:82-110` (`updateBucket`)
- Test: `internal/api/handlers_test.go` (new file)

**Interfaces:**
- Consumes: `bucketInput` struct (`handlers.go:29`), `config.Bucket`, `Server.repo`.
- Produces: `func newBucketFromInput(in bucketInput) *config.Bucket` and `func applyBucketInput(b *config.Bucket, in bucketInput)` — unexported helpers in `package api`, so both create and update share one mapping.

- [ ] **Step 1: Write the failing tests**

```go
func TestCreateBucketPersistsCredentials(t *testing.T)   // POST JSON with access_key/secret_key → repo bucket has both
func TestUpdateBucketKeepsCredentialsWhenOmitted(t *testing.T) // PUT without secret_key → stored secret unchanged
func TestBucketResponseOmitsCredentials(t *testing.T)     // GET response body contains neither access_key nor secret_key
```

Drive the handlers through `gin.New()` with the routes registered as the router does, or call the handlers with a `httptest.NewRecorder` and a crafted `*gin.Context` — match whichever pattern `tests/api_test.go` already uses.

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/api/ -run 'TestCreateBucket|TestUpdateBucket|TestBucketResponse' -v`
Expected: `TestUpdateBucketKeepsCredentialsWhenOmitted` FAILS — the stored secret is wiped.

- [ ] **Step 3: Route create and update through the shared mappers**

`createBucket` already builds a `config.Bucket` from `bucketInput` (correct — that is why it passes). Replace the hand-written field assignments in `updateBucket` with `applyBucketInput(existing, in)`, and make `applyBucketInput` copy `AccessKey`/`SecretKey` only when the incoming value is non-empty. Keep `Bucket.AccessKey`/`SecretKey` tagged `json:"-"` so they never marshal out.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/api/ -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/api/handlers.go internal/api/handlers_test.go
git commit -m "fix(api): persist bucket credentials on create and update without wiping on omission"
```

---

### Task 6: Repair the Docker deployment path

**Files:**
- Modify: `Dockerfile:8-15`, `docker-compose.yml:9`, `internal/api/auth.go:79-85` (allowlist), `cmd/health.go`
- Test: `internal/api/auth_test.go` (new file)

**Interfaces:**
- Consumes: `authRequired(c *gin.Context)` (existing middleware).
- Produces: no new symbols; `/api/health` becomes reachable without credentials when `S3SYNC_API_KEY` is set.

- [ ] **Step 1: Write the failing test**

```go
func TestHealthEndpointSkipsAuth(t *testing.T) // authEnabled=true, GET /api/health through authRequired → not 401
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/api/ -run TestHealthEndpointSkipsAuth -v`
Expected: FAIL with 401.

- [ ] **Step 3: Apply the deployment fixes**

Add `path == "/api/health"` to the allowlist condition in `authRequired`. Switch the runtime stage of the `Dockerfile` to `gcr.io/distroless/static-debian12:nonroot` (it ships CA certificates and `tzdata`, and the `HEALTHCHECK` exec form still works because `/s3sync health` is a static binary). In `cmd/health.go`, give the client a timeout: `&http.Client{Timeout: 3 * time.Second}` instead of `http.Get`. In `docker-compose.yml`, make the master key required: `BUCKETSYNC_MASTER_KEY=${BUCKETSYNC_MASTER_KEY:?BUCKETSYNC_MASTER_KEY is required}`.

- [ ] **Step 4: Run test to verify it passes, and verify the image builds**

Run: `go test ./internal/api/ -run TestHealthEndpointSkipsAuth -v`
Expected: PASS.
Run: `docker build -t s3sync:test .`
Expected: build succeeds. (If the Docker daemon is unavailable in this environment, record that in the ledger and rely on the test plus a `docker build --help` check; do not claim the image was verified.)

- [ ] **Step 5: Commit**

```bash
git add Dockerfile docker-compose.yml internal/api/auth.go internal/api/auth_test.go cmd/health.go
git commit -m "fix(deploy): distroless runtime with CA certs, health endpoint exempt from auth, required master key"
```

---

### Task 7: Locking hygiene — `lastRun` and `ProgressSnapshot`

**Files:**
- Modify: `internal/sync/engine.go:15-20` (add `ProgressSnapshot`), `engine.go:264`, `engine.go:290-295` (`setResult`), `engine.go:308-325` (`Status`), `internal/sync/run.go:59,77`, `internal/api/handlers.go:178,371`
- Test: `internal/sync/engine_test.go`

**Interfaces:**
- Consumes: `Progress.Snapshot() (total, completed, failed int)` (existing, `engine.go:27`).
- Produces: `type ProgressSnapshot struct { Total int; Completed int; Failed int }` with JSON tags `total`, `completed`, `failed`; `func (e *Engine) setResult(status, errMsg string, lastRun time.Time)`; `Status()` returns `ProgressSnapshot` as its fifth value.

- [ ] **Step 1: Write the failing tests**

```go
func TestStatusReturnsSnapshotWithoutMutexCopy(t *testing.T) // Status() while RunOnce is in flight, under -race
func TestSetResultRecordsLastRun(t *testing.T)               // after RunOnce, Status().lastRun is non-zero
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test -race ./internal/sync/ -run 'TestStatus|TestSetResult' -v`
Expected: `TestSetResultRecordsLastRun` FAILS (lastRun still zero because nothing set it under the lock), and `go vet ./...` still reports `return copies lock value` at `engine.go:321`.

- [ ] **Step 3: Fold `lastRun` into `setResult` and return a snapshot**

Change `setResult` to take `lastRun time.Time` and assign it inside the existing lock; delete the standalone `e.lastRun = time.Now()` at `engine.go:264` and pass `time.Now()` to `setResult` instead. Change `Status()` to build and return a `ProgressSnapshot` value (no mutex, no `Progress` copy). Update the two `run.go` call sites and the two `handlers.go` call sites to the new type — they only read `.Total`/`.Completed`/`.Failed`, so the change is mechanical.

- [ ] **Step 4: Run tests and vet**

Run: `go test -race ./... && go vet ./...`
Expected: tests PASS, `go vet` reports **zero** findings.

- [ ] **Step 5: Commit**

```bash
git add internal/sync/engine.go internal/sync/run.go internal/api/handlers.go internal/sync/engine_test.go
git commit -m "fix(sync): guard lastRun with the engine mutex and return a lock-free progress snapshot"
```

---

### Task 8: Connection-pool tuning and honest throttle accounting

**Files:**
- Modify: `internal/s3client/client.go:18-27`, `internal/sync/throttler.go` (add a bucket-aware entry point), `internal/sync/worker.go:100,123,143`, `internal/sync/engine.go` (listing call site)
- Test: `internal/s3client/client_test.go`, `internal/sync/throttler_test.go`

**Interfaces:**
- Consumes: `Throttler.WaitLog(ctx context.Context, label string) error` (existing).
- Produces: `func (t *Throttler) WaitFor(ctx context.Context, bucket string) error` — `WaitLog` delegates to it so existing call sites keep compiling; the label becomes the bucket name the operation actually targets.

- [ ] **Step 1: Write the failing tests**

```go
// internal/s3client/client_test.go
func TestTransportHasConnectionPoolLimits(t *testing.T) // NewClient's transport has MaxIdleConnsPerHost >= 100 and TLSHandshakeTimeout set
// internal/sync/throttler_test.go
func TestWaitForUsesBucketLabel(t *testing.T) // WaitFor(ctx, "target") blocks under a saturated limiter just as WaitLog does
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/s3client/ ./internal/sync/ -run 'TestTransport|TestWaitFor' -v`
Expected: both FAIL — `MaxIdleConnsPerHost` is 0 (Go default 2) and `WaitFor` is undefined.

- [ ] **Step 3: Implement**

Set `MaxIdleConns: 100`, `MaxIdleConnsPerHost: 100`, `IdleConnTimeout: 90 * time.Second`, `TLSHandshakeTimeout: 10 * time.Second` alongside the existing timeouts. Add `WaitFor` and pass the **target** bucket name at the three `worker.go` call sites (`HeadObject`, `CopyObject`, `DeleteObject`) and the **source** bucket name at the listing call site in `engine.go`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/s3client/ ./internal/sync/ -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/s3client/client.go internal/s3client/client_test.go internal/sync/throttler.go internal/sync/throttler_test.go internal/sync/worker.go internal/sync/engine.go
git commit -m "perf(s3): tune the HTTP connection pool; throttle on the bucket each op targets"
```

---

### Task 9: Fail loudly on objects above the single-copy limit

**Files:**
- Modify: `internal/sync/worker.go` (`copyObject`, before the `CopyObject` call), add `const maxSingleCopySize = 5 * 1024 * 1024 * 1024`
- Test: `internal/sync/worker_test.go`

**Interfaces:**
- Consumes: `SyncAction.Size int64` (existing field, set by `differ.go`).
- Produces: no new symbols; a >5 GiB action returns an error naming the key and the limit instead of an opaque `EntityTooLarge`.

- [ ] **Step 1: Write the failing test**

```go
func TestCopyObjectRejectsOversizeObject(t *testing.T) // Size = 6 GiB → error mentions the key and "multipart", zero CopyObject calls
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/sync/ -run TestCopyObjectRejectsOversize -v`
Expected: FAIL — the mock receives a `CopyObject` and no clear error is returned.

- [ ] **Step 3: Implement the guard**

Return before issuing the copy when `a.Size > maxSingleCopySize`, with an error of the form `object %q is %d bytes; exceeds the %d-byte single-copy limit (multipart copy not implemented)`.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/sync/ -run TestCopyObjectRejectsOversize -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/sync/worker.go internal/sync/worker_test.go
git commit -m "fix(sync): reject objects over the 5 GiB single-copy limit with a clear error"
```

---

### Task 10: Make credential decryption failures visible

**Files:**
- Modify: `internal/config/repo.go:402-420` (`decryptCreds` and its three call sites at `repo.go:72,83,93`)
- Test: `internal/config/repo_test.go`

**Interfaces:**
- Consumes: `Decrypt(ciphertext string, key []byte) ([]byte, error)` (existing, `internal/config/encrypt.go`).
- Produces: `func decryptCreds(key []byte, b *Bucket) error` — returns a wrapped error naming the bucket when a stored credential cannot be decrypted; call sites propagate it.

- [ ] **Step 1: Write the failing test**

```go
func TestDecryptCredsFailsOnWrongKey(t *testing.T) // bucket encrypted with key A, read with key B → error mentions the bucket name, credential is NOT the ciphertext
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/config/ -run TestDecryptCredsFailsOnWrongKey -v`
Expected: FAIL — no error is returned and `AccessKey` still holds the hex ciphertext.

- [ ] **Step 3: Implement**

Return `fmt.Errorf("decrypt access key for bucket %q: %w (is BUCKETSYNC_MASTER_KEY the same one used to store it?)", b.Name, err)` (and the secret-key equivalent) instead of only warning, and have `GetBucket`/`ListBuckets`/`GetBucketByName` propagate it. Keep the `key == nil` early return — that is the legitimate plaintext-storage path.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/config/ -v`
Expected: PASS, existing repo tests still green.

- [ ] **Step 5: Commit**

```bash
git add internal/config/repo.go internal/config/repo_test.go
git commit -m "fix(config): surface master-key mismatch instead of using ciphertext as a credential"
```

---

### Task 11: Enforce referential integrity in SQLite

**Files:**
- Modify: `internal/config/db.go:19` (DSN pragmas), `internal/config/repo.go` (`DeleteBucket`), `internal/api/handlers.go` (delete-bucket handler)
- Test: `internal/config/db_test.go`

**Interfaces:**
- Consumes: `Open(path string) (*sql.DB, error)` (existing), `DeleteBucket(id string) error` (existing).
- Produces: `DeleteBucket` returns a non-nil error when sync pairs still reference the bucket.

- [ ] **Step 1: Write the failing tests**

```go
func TestForeignKeysEnforced(t *testing.T)      // inserting a sync_pair with a nonexistent source_bucket_id → error
func TestDeleteBucketRejectedWhenReferenced(t *testing.T) // bucket referenced by a pair → DeleteBucket errors
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/config/ -run 'TestForeignKeys|TestDeleteBucketRejected' -v`
Expected: both FAIL — the orphan insert succeeds and the delete is allowed.

- [ ] **Step 3: Implement**

Append `&_pragma=foreign_keys(1)` to the DSN in `Open`. In `DeleteBucket`, run a `SELECT COUNT(*) FROM sync_pairs WHERE source_bucket_id = ? OR target_bucket_id = ?` first and return a descriptive error when non-zero. Note the pragma is per-connection: because `database/sql` pools connections, also verify the test passes with more than one concurrent query; if it does not, set `db.SetMaxOpenConns(1)` or register the pragma via a `ConnectHook` on the driver.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/config/ -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/config/db.go internal/config/repo.go internal/config/db_test.go internal/api/handlers.go
git commit -m "fix(config): enforce foreign keys and refuse to delete referenced buckets"
```

---

### Task 12: Cache hygiene — dead code, swallowed errors, honest locking docs

**Files:**
- Modify: `internal/cache/store.go` (`Clear` error handling, remove `List`), `internal/cache/store_test.go`, `README.md` (cache-lock note)
- Test: `internal/cache/store_test.go`

**Interfaces:**
- Consumes: `Store.Clear(pairID string) error` (existing), `Store.List(pairID string)` (existing, production-dead).
- Produces: `Store.List` removed; `Clear` returns the underlying bbolt error.

- [ ] **Step 1: Write the failing tests**

```go
func TestClearPropagatesBucketError(t *testing.T) // Clear on a closed store → non-nil error
```

Also delete the `List` assertions from `store_test.go` only after confirming no production caller exists: run `grep -rn 'store\.List(\|\.List(pairID' --include='*.go' .` and confirm the only hits are tests.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/cache/ -run TestClearPropagates -v`
Expected: FAIL — `Clear` returns nil.

- [ ] **Step 3: Implement**

In `Clear`, return the `DeleteBucket` error unless it is `bbolt.ErrBucketNotFound` (the legitimate "nothing to clear" case) — do not swallow it unconditionally. Remove `Store.List`. Add one line to the README's Configuration section: the BoltDB cache takes a 5-second lock, so `s3sync pair sync` fails while `s3sync serve` is running against the same config directory.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/cache/ -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/cache/store.go internal/cache/store_test.go README.md
git commit -m "chore(cache): propagate Clear errors, drop unused List, document the cache lock"
```

---

### Task 13: Server and API hygiene

**Files:**
- Modify: `cmd/serve.go:59-63`, `internal/api/router.go:83-87` (`SetSetupState` removal), `internal/api/router.go:248-256` (`recoverCrashedPairs`), `internal/api/handlers.go:715` (version), `main.go`, `internal/api/auth.go:65` (cookie)
- Test: `internal/api/router_test.go` (new file)

**Interfaces:**
- Consumes: `Server` methods; `var version = "dev"` to be declared in `main.go`.
- Produces: `main.version` injected via `-ldflags "-X main.version=..."`; `Server.Version() string` exposed to the API handler.

- [ ] **Step 1: Write the failing tests**

```go
func TestRecoverCrashedPairsMarksInterrupted(t *testing.T) // pair with LastSyncStatus=="" && ConsecutiveErrors>0 → status "interrupted", not "ok"
func TestVersionEndpointReportsBuildVersion(t *testing.T)  // Server.Version() == "1.2.3" when set → /api/version returns it
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/api/ -run 'TestRecoverCrashed|TestVersionEndpoint' -v`
Expected: both FAIL.

- [ ] **Step 3: Implement**

Set `p.LastSyncStatus = "interrupted"` in `recoverCrashedPairs`. Add `var version = "dev"` to `main.go`, a `Server.Version()` accessor defaulting to `"dev"`, and have the `/api/version` handler return it instead of the hardcoded `"0.2.0"`. Delete `SetSetupState` (no callers). In `cmd/serve.go`, replace `IdleTimeout: 5 * time.Second` with `IdleTimeout: 60 * time.Second` and add `ReadHeaderTimeout: 10 * time.Second`, `ReadTimeout: 60 * time.Second`, `WriteTimeout: 120 * time.Second`. Add `SameSite: http.SameSiteLaxMode` to the three `SetCookie` calls that currently set neither.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/api/ -v && go build ./...`
Expected: PASS and a clean build.

- [ ] **Step 5: Commit**

```bash
git add cmd/serve.go internal/api/router.go internal/api/handlers.go internal/api/auth.go internal/api/router_test.go main.go
git commit -m "chore(api): honest crash-recovery status, build-injected version, safer server timeouts and cookies"
```

---

### Task 14: Logging, webhook, and dependency hygiene

**Files:**
- Modify: `internal/log/logger.go:18,53`, `internal/sync/notify.go:47`, `internal/sync/engine.go:274`, `go.mod`/`go.sum`
- Test: `internal/log/logger_test.go`, `internal/sync/notify_test.go` (new file)

**Interfaces:**
- Consumes: `log.Init(cfg Config) func()` (existing).
- Produces: `Init` returns a closer that closes **its own** file handle, so a second `Init` does not leak the first.

- [ ] **Step 1: Write the failing tests**

```go
// internal/log/logger_test.go
func TestSecondInitClosesFirstWriter(t *testing.T) // Init twice with distinct files → both closers close their own file; no leak
// internal/sync/notify_test.go
func TestSendWebhookDrainsResponseBody(t *testing.T) // server returns a large body → it is fully read before Close
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/log/ ./internal/sync/ -run 'TestSecondInit|TestSendWebhookDrains' -v`
Expected: `TestSecondInitClosesFirstWriter` FAILS (package-level `closeOnce` prevents the second close); the drain test FAILS (body is closed unread).

- [ ] **Step 3: Implement**

Remove the package-level `closeOnce`/`fileWriter`; capture the opened file in a closure-local variable and return a closer that closes it exactly once for that `Init` call (`var once sync.Once` local to the returned function). In `SendWebhook`, `io.Copy(io.Discard, resp.Body)` before `resp.Body.Close()`. Fix the webhook payload to report the post-run consecutive-error count by computing it before building `WebhookPayload` rather than reading `e.pair.ConsecutiveErrors`. Run `go mod tidy` to drop the unused `go.mongodb.org/mongo-driver/v2` requirement.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/log/ ./internal/sync/ -v && go mod tidy && git diff --stat go.mod`
Expected: PASS; `go.mod` loses the mongo-driver line.

- [ ] **Step 5: Commit**

```bash
git add internal/log/logger.go internal/log/logger_test.go internal/sync/notify.go internal/sync/notify_test.go internal/sync/engine.go go.mod go.sum
git commit -m "chore: per-init log closers, drain webhook bodies, report live error counts, tidy deps"
```

---

### Task 15: Documentation and CI

**Files:**
- Modify: `README.md` (Configuration + Serve Mode sections), create `.github/workflows/ci.yml`, `docker-compose.yml` (already touched in Task 6)
- Test: none (configuration and documentation) — verification is the CI run itself.

**Interfaces:**
- Consumes: nothing.
- Produces: a CI workflow that fails on `go vet` findings, which is what would have caught Tasks 7's original defects.

- [ ] **Step 1: Add the CI workflow**

`.github/workflows/ci.yml` with a single `build-test` job on `ubuntu-latest`: `actions/checkout@v4`, `actions/setup-go@v5` with `go-version-file: go.mod`, then `go build ./...`, `go vet ./...`, `go test -race ./...`.

- [ ] **Step 2: Verify the workflow file is valid**

Run: `python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/ci.yml')); print('valid yaml')"`
Expected: `valid yaml`.

- [ ] **Step 3: Document the gaps the review found**

Add to the README's Configuration section: `S3SYNC_API_KEY` (unset by default, meaning the server is unauthenticated), the fact that `serve` binds `0.0.0.0:8080`, the 5 GiB single-copy limit, and the master-key requirement for the Docker path. Add to Serve Mode: the auth behaviour and the login URL.

- [ ] **Step 4: Verify the documented claims against the code**

Run: `grep -n 'S3SYNC_API_KEY' README.md internal/api/auth.go && grep -n '5 GiB\|5 GiB\|5368709120\|maxSingleCopySize' README.md internal/sync/worker.go`
Expected: each README claim has a matching code reference.

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/ci.yml README.md
git commit -m "docs+ci: document auth/limits and gate builds on vet and race tests"
```

---

### Task 16: Make the misleading tests honest

**Files:**
- Modify: `internal/sync/differ_test.go:49-74`, `tests/sync_test.go`
- Test: the files themselves.

**Interfaces:**
- Consumes: `StreamingDiff` (existing).
- Produces: `TestStreamingDiffUnchanged` actually exercises unchanged-detection; `TestStreamingDiffDelete` actually asserts delete propagation.

- [ ] **Step 1: Write the corrected tests**

`TestStreamingDiffUnchanged`: seed the cache with an object whose ETag and LastModified match the listing, then assert **zero** actions. `TestStreamingDiffDelete`: seed the cache with a key absent from the listing and `deletePropagation=true`, then assert exactly one `ActionDelete` for that key; add the `deletePropagation=false` counterpart asserting zero actions. In `tests/sync_test.go`, make the integration test assert (`if err != nil { t.Fatalf(...) }`) rather than only logging.

- [ ] **Step 2: Run tests to verify the corrected ones pass and the old behaviour is gone**

Run: `go test ./internal/sync/ -run 'TestStreamingDiffUnchanged|TestStreamingDiffDelete' -v`
Expected: PASS with the new assertions. Confirm each new test fails if the production behaviour is reverted — check by temporarily restoring the old `last :=` expression (Task 3) and re-running; then restore the fix.

- [ ] **Step 3: Commit**

```bash
git add internal/sync/differ_test.go tests/sync_test.go
git commit -m "test: make unchanged-detection and delete-propagation tests assert their named behaviour"
```

---

### Task 17: Final hygiene sweep

**Files:**
- Modify: `internal/sync/differ.go:222-224` (remove `keyLt`), `internal/sync/differ.go` call sites, `README.md` (Development section)
- Test: existing suite is the verification.

**Interfaces:**
- Consumes: nothing.
- Produces: `keyLt` removed; call sites use direct `string` comparison.

- [ ] **Step 1: Replace the `keyLt` wrapper**

`keyLt(a, b string) bool { return a < b }` is a one-line wrapper over the built-in operator. Replace its three call sites with the direct comparison and delete the function. No behaviour change; the existing `differ_test.go` cases are the regression guard.

- [ ] **Step 2: Run the suite**

Run: `go test ./internal/sync/ -v`
Expected: PASS, unchanged from before the edit.

- [ ] **Step 3: Note the intentional test-only helpers**

`Store.Put` and `Store.Delete` are now used only by tests (`PutMany`/`DeleteMany` are the production paths). Add a one-line comment above each stating that, so the next reviewer does not re-report them as dead code. Do not delete them — `store_test.go` depends on them.

- [ ] **Step 4: Document the local dev loop**

Extend the README Development section with the exact commands CI runs (`go build ./... && go vet ./... && go test -race ./...`) so a contributor reproduces CI locally.

- [ ] **Step 5: Commit**

```bash
git add internal/sync/differ.go internal/cache/store.go README.md
git commit -m "chore: inline keyLt, document test-only cache helpers and the local CI loop"
```

---

## Notes for the executor

- `docs/superpowers/plans/2026-07-17-improvements.md` is checked in with every box unticked while the code was in fact implemented. Do not treat its checkboxes as ground truth; the code is.
- The review's P0-4 (swallowed list error), P1-2 (`CopySource` encoding), P2-1 (5-minute timeout), P3-7 (session pruning), and P3-8 (`rand.Read` error) are **already fixed** at `0a95201`. Do not re-fix them; Task 16 and the Task 3 tests are what keep them honest.
- The review's suspected `bucketExists` bug was empirically disproved — both guard tests pass. Do not "fix" it.
