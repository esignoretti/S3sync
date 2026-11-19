# Multipart Copy, Configurable Run Timeout, and Two Fixes — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the dry-run cache corruption and the dead `success` webhook event, make the per-run timeout a global setting, give the drift sweep its own budget with a rotating cursor, and add multipart copy for objects above 5 GiB.

**Architecture:** A new `settings` key/value table backs one global `run_timeout_seconds`; the engine reads it at the start of every run so a change takes effect without a restart. The sweep gets a quarter of the run budget plus an in-memory rotating cursor so a truncated sweep resumes instead of restarting. `copyObject` branches to a multipart path above 5 GiB that reuses the existing attribute pipeline.

**Tech Stack:** Go 1.25, `aws-sdk-go-v2/service/s3`, `modernc.org/sqlite`, bbolt, cobra, gin.

**Spec:** `docs/superpowers/specs/2026-11-17-multipart-timeout-and-fixes-design.md`

## Global Constraints

- Endpoint is Cubbit DS3. `UploadPartCopy` requires an explicit `CopySourceRange` end offset; open-ended ranges return `InvalidRange`.
- `AbortIncompleteMultipartUpload` is NOT supported by the endpoint, so no lifecycle rule reaps orphaned parts. Abort must be explicit.
- `ErrObjectTooLarge` stays permanent and unwrapped by `retryS3`.
- `maxCopyObjectSize = 5 << 30` (single copy), `maxMultipartObjectSize = 5 << 40` (hard limit).
- `DefaultRunTimeoutSeconds = 300`, `MinRunTimeoutSeconds = 30`, `MaxRunTimeoutSeconds = 86400`.
- Multipart part size: `max(5 MiB, ceil(size/10000))` rounded up to a whole MiB, capped at 5 GiB; part count must never exceed 10,000.
- Parts are sequential. No parallel part uploads.
- No change to the cache schema, the pair schema, or the attribute fingerprint.
- `go test ./...` and `go vet ./...` must pass. `go vet` has two pre-existing copylocks warnings (`internal/sync/engine.go:419`, `internal/api/handlers.go:455`); do not add new ones.

## Review Focus

- **A dry run followed by a real run.** A dry run must copy nothing AND cache nothing, so the real run still copies everything. If the dry-run path writes the cache, the bytes never arrive and nothing ever retries them.
- **A cache too large to sweep in one budget.** The sweep must advance across runs, not restart at the same key forever. Otherwise drift past the cut-off is permanently invisible while every run pays the full budget.
- **A multipart copy that fails or is cancelled mid-parts.** Every failure path must abort the upload, with a context that is not already cancelled, or parts bill forever with no reaper.
- **An object above 5 GiB on a `--storage-class` pair with replication off.** It must land in the same tier as every smaller object; the storage class must not ride on the attribute path.
- **An object above 5 GiB copied to a target without object lock, when the source object is locked.** The error must name object lock, exactly as the single-copy path does.

---

### Task 1: Dry-run must not write the cache

**Files:**
- Modify: `internal/sync/engine.go:336-347`
- Modify: `cmd/pair.go` (flags at `:312-327`, `:329-342`; `applyPairFlags`)
- Test: `internal/sync/engine_test.go`, `cmd/pair_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: nothing new; `DryRun` behaviour changes.

- [ ] **Step 1: Write the failing test**

In `internal/sync/engine_test.go`:

```go
func TestRunOnceDryRunDoesNotWriteCache(t *testing.T) {
	ep := &engineEndpoint{objects: []string{"a.txt", "b.txt"}}
	e := newTestEngine(t, &config.SyncPair{DryRun: true}, ep)

	if err := e.RunOnce(context.Background()); err != nil {
		t.Fatalf("dry run: %v", err)
	}
	if got := e.store.Len("p1"); got != 0 {
		t.Fatalf("cache holds %d objects after a dry run, want 0", got)
	}
}
```

If `cache.Store` has no `Len` (it does not — see `internal/cache/store.go`, which has `Put`, `PutMany`, `Delete`, `List`, `Clear`, `NewCursor`), use `e.store.List("p1")` and assert `len(entries) == 0`.

- [ ] **Step 2: Run it to verify it fails**

Run: `go test ./internal/sync/ -run TestRunOnceDryRunDoesNotWriteCache -v`
Expected: FAIL with `cache holds 2 objects after a dry run, want 0`.

- [ ] **Step 3: Guard the cache write**

In `internal/sync/engine.go`, wrap the write at `:345`:

```go
if !e.pair.DryRun {
    if err := e.store.PutMany(cached); err != nil {
        slog.Warn("cache put failed", "pair", e.pair.Name, "error", err)
    }
}
```

- [ ] **Step 4: Run it to verify it passes**

Run: `go test ./internal/sync/ -run TestRunOnceDryRunDoesNotWriteCache -v`
Expected: PASS.

- [ ] **Step 5: Add the `--dry-run` CLI flag**

In `cmd/pair.go`, register on BOTH commands in `init()`:

```go
pairAddCmd.Flags().Bool("dry-run", false, "Log actions without copying or caching anything")
pairUpdateCmd.Flags().Bool("dry-run", false, "Log actions without copying or caching anything")
```

In `applyPairFlags`, handle it outside the `create` gate (mutable on both paths):

```go
if cmd.Flags().Changed("dry-run") {
    v, _ := cmd.Flags().GetBool("dry-run")
    p.DryRun = v
}
```

- [ ] **Step 6: Write the flag test**

In `cmd/pair_test.go`:

```go
func TestApplyPairFlagsDryRun(t *testing.T) {
	c := pairUpdateCmd
	c.Flags().Set("dry-run", "true")
	p := &config.SyncPair{}
	if err := applyPairFlags(c, p, false); err != nil {
		t.Fatal(err)
	}
	if !p.DryRun {
		t.Fatal("dry-run flag was not applied")
	}
}
```

Reset the flag with `c.Flags().Set("dry-run", "false")` in `t.Cleanup` so other tests are unaffected.

- [ ] **Step 7: Run the suite and commit**

Run: `go test ./cmd/ ./internal/sync/`
Expected: PASS.

```bash
git add internal/sync/engine.go internal/sync/engine_test.go cmd/pair.go cmd/pair_test.go
git commit -m "fix(sync): a dry run must not write the sync cache"
```

---

### Task 2: `success` and `ok` mean the same thing

**Files:**
- Modify: `internal/sync/notify.go:61-77`
- Test: `internal/sync/notify_test.go` (create)

**Interfaces:**
- Consumes: nothing.
- Produces: nothing new; `shouldWebhook(events, status string) bool` keeps its signature.

- [ ] **Step 1: Write the failing test**

Create `internal/sync/notify_test.go`:

```go
package sync

import "testing"

func TestShouldWebhookAcceptsSuccessAndOk(t *testing.T) {
	cases := []struct {
		events string
		status string
		want   bool
	}{
		{"success", "ok", true},
		{"ok", "ok", true},
		{"success", "error", false},
		{"error", "error", true},
		{"ok,error", "error", true},
		{"success,error", "ok", true},
		{"all", "expired", true},
		{"none", "ok", false},
		{"", "ok", false},
		{"success", "expired", false},
	}
	for _, c := range cases {
		if got := shouldWebhook(c.events, c.status); got != c.want {
			t.Errorf("shouldWebhook(%q, %q) = %v, want %v", c.events, c.status, got, c.want)
		}
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `go test ./internal/sync/ -run TestShouldWebhookAcceptsSuccessAndOk -v`
Expected: FAIL on the first case — `shouldWebhook("success", "ok") = false, want true`.

- [ ] **Step 3: Map the spelling**

In `internal/sync/notify.go`, inside the split loop:

```go
for _, e := range strings.Split(events, ",") {
    e = strings.TrimSpace(e)
    // "success" is what the UI label, the original spec and the README call a
    // clean run; the status column spells it "ok". Accept both.
    if e == "success" {
        e = "ok"
    }
    if e == status || e == "all" {
        return true
    }
}
```

- [ ] **Step 4: Run it to verify it passes**

Run: `go test ./internal/sync/ -run TestShouldWebhookAcceptsSuccessAndOk -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/sync/notify.go internal/sync/notify_test.go
git commit -m "fix(sync): accept the 'success' webhook event, which never matched"
```

---

### Task 3: Settings store

**Files:**
- Modify: `internal/config/db.go` (inside `migrate()`, after the `sync_logs` table)
- Create: `internal/config/settings.go`
- Test: `internal/config/settings_test.go`

**Interfaces:**
- Consumes: `Repository` from `internal/config/repo.go`.
- Produces:
  - `type Settings struct { RunTimeoutSeconds int }`
  - `func (r *Repository) GetSettings() (Settings, error)`
  - `func (r *Repository) SetSetting(key, value string) error`
  - Constants `SettingRunTimeoutSeconds`, `DefaultRunTimeoutSeconds`, `MinRunTimeoutSeconds`, `MaxRunTimeoutSeconds`.

- [ ] **Step 1: Write the failing tests**

Create `internal/config/settings_test.go`:

```go
package config

import (
	"path/filepath"
	"testing"
)

func settingsTestRepo(t *testing.T) *Repository {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "config.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return NewRepository(db)
}

func TestGetSettingsDefaultsWhenAbsent(t *testing.T) {
	s, err := settingsTestRepo(t).GetSettings()
	if err != nil {
		t.Fatal(err)
	}
	if s.RunTimeoutSeconds != DefaultRunTimeoutSeconds {
		t.Fatalf("RunTimeoutSeconds = %d, want %d", s.RunTimeoutSeconds, DefaultRunTimeoutSeconds)
	}
}

func TestSetSettingRejectsUnknownKey(t *testing.T) {
	if err := settingsTestRepo(t).SetSetting("nope", "1"); err == nil {
		t.Fatal("unknown key was accepted")
	}
}

func TestSetSettingRejectsOutOfRange(t *testing.T) {
	r := settingsTestRepo(t)
	for _, v := range []string{"0", "29", "86401", "abc"} {
		if err := r.SetSetting(SettingRunTimeoutSeconds, v); err == nil {
			t.Fatalf("value %q was accepted", v)
		}
	}
}

func TestSetSettingRoundTrips(t *testing.T) {
	r := settingsTestRepo(t)
	if err := r.SetSetting(SettingRunTimeoutSeconds, "1800"); err != nil {
		t.Fatal(err)
	}
	s, err := r.GetSettings()
	if err != nil {
		t.Fatal(err)
	}
	if s.RunTimeoutSeconds != 1800 {
		t.Fatalf("RunTimeoutSeconds = %d, want 1800", s.RunTimeoutSeconds)
	}
}

func TestGetSettingsUnparseableValueFallsBack(t *testing.T) {
	r := settingsTestRepo(t)
	if err := r.SetSetting(SettingRunTimeoutSeconds, "garbage"); err == nil {
		t.Skip("SetSetting rejects garbage; the fallback is unreachable through it")
	}
	// Write past validation to prove a corrupt row cannot stop a run.
	if _, err := r.db.Exec(`INSERT INTO settings (key, value) VALUES (?, ?)`,
		SettingRunTimeoutSeconds, "garbage"); err != nil {
		t.Fatal(err)
	}
	s, err := r.GetSettings()
	if err != nil {
		t.Fatalf("GetSettings returned an error for a corrupt row: %v", err)
	}
	if s.RunTimeoutSeconds != DefaultRunTimeoutSeconds {
		t.Fatalf("RunTimeoutSeconds = %d, want the default", s.RunTimeoutSeconds)
	}
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `go test ./internal/config/ -run 'TestGetSettings|TestSetSetting' -v`
Expected: FAIL to build — `undefined: DefaultRunTimeoutSeconds`, `r.GetSettings undefined`.

- [ ] **Step 3: Add the table**

In `internal/config/db.go`, inside `migrate()` after the `sync_logs` statement:

```go
if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
)`); err != nil {
    return err
}
```

- [ ] **Step 4: Implement `internal/config/settings.go`**

```go
package config

import (
	"fmt"
	"log/slog"
	"strconv"
)

const (
	SettingRunTimeoutSeconds = "run_timeout_seconds"
	DefaultRunTimeoutSeconds = 300
	MinRunTimeoutSeconds     = 30
	MaxRunTimeoutSeconds     = 86400
)

type Settings struct {
	RunTimeoutSeconds int
}

// GetSettings returns the stored settings, falling back to defaults for absent
// or unparseable rows. A corrupt settings row must never stop a sync, so an
// unreadable value is logged and replaced rather than returned as an error.
func (r *Repository) GetSettings() (Settings, error) {
	s := Settings{RunTimeoutSeconds: DefaultRunTimeoutSeconds}

	rows, err := r.db.Query(`SELECT key, value FROM settings`)
	if err != nil {
		return s, fmt.Errorf("query settings: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return s, fmt.Errorf("scan settings: %w", err)
		}
		if k != SettingRunTimeoutSeconds {
			continue
		}
		n, err := strconv.Atoi(v)
		if err != nil || n < MinRunTimeoutSeconds || n > MaxRunTimeoutSeconds {
			slog.Warn("settings: ignoring unusable value",
				"key", k, "value", v, "fallback", DefaultRunTimeoutSeconds)
			continue
		}
		s.RunTimeoutSeconds = n
	}
	return s, rows.Err()
}

// SetSetting validates and stores one setting. Unknown keys and out-of-range
// values are rejected so a typo cannot silently become a no-op.
func (r *Repository) SetSetting(key, value string) error {
	switch key {
	case SettingRunTimeoutSeconds:
		n, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("%s: %q is not a number", key, value)
		}
		if n < MinRunTimeoutSeconds || n > MaxRunTimeoutSeconds {
			return fmt.Errorf("%s: %d is outside %d..%d", key, n, MinRunTimeoutSeconds, MaxRunTimeoutSeconds)
		}
	default:
		return fmt.Errorf("unknown setting %q", key)
	}
	_, err := r.db.Exec(
		`INSERT INTO settings (key, value) VALUES (?, ?)
		 ON CONFLICT(key) DO UPDATE SET value = excluded.value`, key, value)
	if err != nil {
		return fmt.Errorf("set setting: %w", err)
	}
	return nil
}
```

- [ ] **Step 5: Run them to verify they pass**

Run: `go test ./internal/config/ -run 'TestGetSettings|TestSetSetting' -v`
Expected: PASS (5 tests).

- [ ] **Step 6: Commit**

```bash
git add internal/config/db.go internal/config/settings.go internal/config/settings_test.go
git commit -m "feat(config): add a settings table with a global run timeout"
```

---

### Task 4: The engine reads the timeout at the start of every run

**Files:**
- Modify: `internal/sync/engine.go:91-137` (Engine struct, `NewEngine`), `:176`
- Modify: `internal/api/router.go:124`
- Modify: `internal/sync/run.go:47`
- Test: `internal/sync/engine_test.go`, `internal/sync/engine_test.go:45,112,243` (existing call sites)

**Interfaces:**
- Consumes: `config.Settings`, `config.DefaultRunTimeoutSeconds` (Task 3).
- Produces:
  - `NewEngine(pair *config.SyncPair, src, tgt *config.Bucket, srcS3, tgtS3 *s3.Client, cacheStore *cache.Store, settingsFn func() config.Settings) *Engine`
  - `func (e *Engine) settings() config.Settings` — returns defaults when `settingsFn` is nil.

- [ ] **Step 1: Write the failing test**

In `internal/sync/engine_test.go`:

```go
func TestRunOnceUsesConfiguredTimeout(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	ep := &engineEndpoint{listBlock: release}
	e := newTestEngineWithSettings(t, &config.SyncPair{},
		ep, func() config.Settings { return config.Settings{RunTimeoutSeconds: 1} })

	start := time.Now()
	err := e.RunOnce(context.Background())
	if err == nil {
		t.Fatal("want a timeout error, got nil")
	}
	if !strings.Contains(err.Error(), "diff") {
		t.Fatalf("err = %v, want a diff/deadline error", err)
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Fatalf("run took %s; the configured 1s timeout was not applied", elapsed)
	}
}
```

Add `newTestEngineWithSettings` alongside `newTestEngine` at `:211`, identical but passing `settingsFn` as the last argument to `NewEngine`.

`engineEndpoint` (`internal/sync/engine_test.go:143-149`) gains a `listBlock chan struct{}` field; its list handler does `<-f.listBlock` when non-nil. Its current fields are `mu`, `requests`, `objects`, `tags`, `lockConfigured`.

- [ ] **Step 2: Run it to verify it fails**

Run: `go test ./internal/sync/ -run TestRunOnceUsesConfiguredTimeout -v`
Expected: FAIL to build — `too many arguments in call to NewEngine`.

- [ ] **Step 3: Add the settings function**

In `internal/sync/engine.go`, add the field to `Engine` (after `pool`):

```go
settingsFn func() config.Settings
```

Change the constructor signature and body:

```go
func NewEngine(pair *config.SyncPair, src, tgt *config.Bucket,
	srcS3, tgtS3 *s3.Client, cacheStore *cache.Store,
	settingsFn func() config.Settings) *Engine {
```

and add `settingsFn: settingsFn,` to the literal. Then:

```go
// settings returns the current global settings. It is called at the start of
// every run rather than at construction: the pair-update bug this project
// already fixed once was an engine holding a stale copy of its configuration,
// and reading per run makes that staleness structurally impossible here.
func (e *Engine) settings() config.Settings {
	if e.settingsFn == nil {
		return config.Settings{RunTimeoutSeconds: config.DefaultRunTimeoutSeconds}
	}
	return e.settingsFn()
}
```

- [ ] **Step 4: Use it for the run context**

Replace `timeout := 5 * time.Minute` at `:176`:

```go
s := e.settings()
runTimeout := time.Duration(s.RunTimeoutSeconds) * time.Second
slog.Info("run starting", "pair", e.pair.Name, "timeout_s", s.RunTimeoutSeconds)
ctx, cancel := context.WithTimeout(ctx, runTimeout)
```

- [ ] **Step 5: Update the call sites**

`internal/sync/run.go:47` — `RunOneShot` already holds `repo`:

```go
engine := NewEngine(pair, src, tgt, srcS3, tgtS3, cacheStore, repo.GetSettingsOrDefaults)
```

Add to `internal/config/repo.go` (not `internal/sync` — `Repository` lives in
`config`, and `internal/sync` already imports it):

```go
// GetSettingsOrDefaults adapts GetSettings to the engine's settings function,
// degrading to defaults rather than failing a run.
func (r *Repository) GetSettingsOrDefaults() Settings {
	s, err := r.GetSettings()
	if err != nil {
		slog.Warn("settings: read failed, using defaults", "error", err)
		return Settings{RunTimeoutSeconds: DefaultRunTimeoutSeconds}
	}
	return s
}
```

`internal/api/router.go:124` — `s.repo` is in scope:

```go
engine := sync.NewEngine(pair, src, tgt, srcS3, tgtS3, s.cache, s.repo.GetSettingsOrDefaults)
```

Existing test call sites at `engine_test.go:45,112,243` pass `nil`.

- [ ] **Step 6: Run the package tests**

Run: `go build ./... && go test ./internal/sync/ -run TestRunOnce -v`
Expected: PASS, including the new timeout test.

- [ ] **Step 7: Commit**

```bash
git add internal/sync/engine.go internal/sync/run.go internal/sync/engine_test.go internal/config/repo.go internal/api/router.go
git commit -m "feat(sync): read the run timeout from settings on every run"
```

---

### Task 5: Sweep budget and rotating cursor

**Files:**
- Modify: `internal/sync/engine.go` (`Engine` struct, sweep dispatch at `:246-284`)
- Modify: `internal/sync/reconcile.go:46-105`
- Test: `internal/sync/reconcile_test.go`, `internal/sync/engine_test.go`

**Interfaces:**
- Consumes: `runTimeout` from Task 4.
- Produces:
  - `Engine.sweepCursor string` (guarded by `e.mu`)
  - `func (e *Engine) reconcileSweep(ctx context.Context, actions chan<- SyncAction) error` — unchanged signature, now cursor-aware.

- [ ] **Step 1: Write the failing tests**

In `internal/sync/reconcile_test.go`:

```go
func TestReconcileSweepResumesFromCursor(t *testing.T) {
	// Three cached entries; the budget allows exactly one HEAD per pass.
	api, e := sweepFixture(t, []string{"a", "b", "c"})

	for pass, want := range []string{"a", "b", "c"} {
		if err := e.sweepOnePass(context.Background()); err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
		if got := api.lastHeadKey(); got != want {
			t.Fatalf("pass %d examined %q, want %q", pass, got, want)
		}
	}
}

func TestReconcileSweepCursorResetsOnFullPass(t *testing.T) {
	api, e := sweepFixture(t, []string{"a", "b"})
	if err := e.reconcileSweep(context.Background(), make(chan SyncAction, 10)); err != nil {
		t.Fatal(err)
	}
	_ = api
	if got := e.SweepCursor(); got != "" {
		t.Fatalf("cursor = %q after a full pass, want empty", got)
	}
}
```

`sweepFixture` and `sweepOnePass` are new test helpers this task adds to `internal/sync/reconcile_test.go`. `sweepFixture(t, keys []string) (*fakeCopyAPI, *Engine)` builds an engine whose fake records the last HEAD key and counts HEADs. `sweepOnePass(ctx)` runs `reconcileSweep` with a context that expires after the first HEAD (the fake cancels it via a hook). Add `Engine.SweepCursor() string` as a locked getter in Step 3.

- [ ] **Step 2: Run them to verify they fail**

Run: `go test ./internal/sync/ -run TestReconcileSweepResumes -v`
Expected: FAIL — pass 1 examines `"a"` again, `pass 1 examined "a", want "b"`.

- [ ] **Step 3: Add the cursor to the Engine**

In `internal/sync/engine.go`, in the mutex-guarded block of `Engine`:

```go
sweepCursor string // last key fully examined by the sweep; "" = start at the front
```

and the getter:

```go
// SweepCursor reports the sweep's resume point, for tests and diagnostics.
func (e *Engine) SweepCursor() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.sweepCursor
}
```

- [ ] **Step 4: Make the sweep cursor-aware**

In `internal/sync/reconcile.go`, at the top of the entry loop, skip anything at or before the cursor, and advance it after each fully examined entry:

```go
e.mu.Lock()
cursor := e.sweepCursor
e.mu.Unlock()

for _, entry := range entries {
	if err := ctx.Err(); err != nil {
		return err
	}
	if cursor != "" && entry.Key <= cursor {
		continue
	}
	if entry.AttrsHash == "" {
		e.advanceCursor(entry.Key)
		continue
	}
	// ... existing HEAD / tag / compare work ...
	e.advanceCursor(entry.Key)
}

e.mu.Lock()
e.sweepCursor = ""
e.mu.Unlock()
```

with:

```go
// advanceCursor records how far the sweep got, so a sweep stopped by its budget
// resumes at the next key instead of re-walking from the front. Without this a
// cache larger than one budget's worth of work never converges: every run pays
// the full budget and examines the same prefix.
func (e *Engine) advanceCursor(key string) {
	e.mu.Lock()
	e.sweepCursor = key
	e.mu.Unlock()
}
```

Reset the cursor to `""` only on a full pass reaching the end. A sweep that returns early on `ctx.Err()` leaves the cursor where it is.

- [ ] **Step 5: Give the sweep its own budget**

In `internal/sync/engine.go`, replace the sweep dispatch at `:246-248`:

```go
sweepDue := ShouldReconcile(e.pair, time.Now().UTC())
sweepErrCh := make(chan error, 1)
if sweepDue {
	sweepCtx, sweepCancel := context.WithTimeout(ctx, runTimeout/4)
	go func() {
		defer sweepCancel()
		sweepErrCh <- e.reconcileSweep(sweepCtx, actions)
	}()
}
```

The existing handler at `:277-279` already logs a sweep error and continues to the diff, so a budget expiry degrades to "sweep did less work this run" without further change.

- [ ] **Step 6: Write the starvation test**

In `internal/sync/engine_test.go`:

```go
func TestRunOnceSweepOverrunStillRunsDiff(t *testing.T) {
	ep := &engineEndpoint{objects: []string{"a.txt"}}
	e := newTestEngine(t, &config.SyncPair{AttrsReconcileInterval: 1}, ep)
	e.seedStaleCacheEntry(t, "stale.txt")

	if err := e.RunOnce(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if e.pair.LastReconcileAt != nil {
		t.Fatal("LastReconcileAt was set by an incomplete sweep")
	}
	if !ep.sawList() {
		t.Fatal("the diff never ran after the sweep overran")
	}
}
```

`seedStaleCacheEntry` is a new helper that writes one `CachedObject` with a non-empty `AttrsHash` straight to `e.store`. `sawList()` reads `engineEndpoint.requests` for a `list-type=2` request. Make the fake's HEAD block past the sweep budget when the key is `stale.txt`.

- [ ] **Step 7: Run the package tests**

Run: `go test ./internal/sync/ -run 'TestReconcile|TestRunOnce' -v`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add internal/sync/engine.go internal/sync/reconcile.go internal/sync/reconcile_test.go internal/sync/engine_test.go
git commit -m "feat(sync): give the drift sweep its own budget and a rotating cursor"
```

---

### Task 6: Multipart copy above 5 GiB

**Files:**
- Modify: `internal/sync/worker.go:29-49` (`copyAPI`, limits), `:139-215` (`copyObject`)
- Modify: `internal/sync/attrs.go` (add `createMPUInput`)
- Create: `internal/sync/multipart.go`
- Test: `internal/sync/worker_test.go`, `internal/sync/multipart_test.go` (create)

**Interfaces:**
- Consumes: `ObjectAttrs`, `copyAPI`, `explainCopyError`, `wp.storageClass`.
- Produces:
  - `copyAPI` gains `CreateMultipartUpload`, `UploadPartCopy`, `CompleteMultipartUpload`, `AbortMultipartUpload`.
  - `func (a ObjectAttrs) createMPUInput(bucket, key, storageClass string) *s3.CreateMultipartUploadInput`
  - `func (wp *WorkerPool) copyMultipart(ctx context.Context, a SyncAction, attrs ObjectAttrs, size int64) (SyncAction, error)`
  - `func multipartPartSize(size int64) int64`
  - `const maxMultipartObjectSize = 5 << 40`

- [ ] **Step 1: Write the failing part-size test**

Create `internal/sync/multipart_test.go`:

```go
package sync

import "testing"

func TestMultipartPartSize(t *testing.T) {
	const (
		mib = int64(1) << 20
		gib = int64(1) << 30
	)
	cases := []struct {
		size     int64
		wantMin  int64
		maxParts int64
	}{
		{gib + 1, 5 * mib, 10000},
		{100 * gib, 5 * mib, 10000},
		{5 << 40, 5 * mib, 10000},
	}
	for _, c := range cases {
		ps := multipartPartSize(c.size)
		if ps < c.wantMin {
			t.Errorf("size %d: part size %d is below the 5 MiB minimum", c.size, ps)
		}
		if ps > 5*gib {
			t.Errorf("size %d: part size %d exceeds the 5 GiB maximum", c.size, ps)
		}
		if ps%mib != 0 {
			t.Errorf("size %d: part size %d is not a whole MiB", c.size, ps)
		}
		if parts := (c.size + ps - 1) / ps; parts > c.maxParts {
			t.Errorf("size %d: %d parts exceeds the 10,000 limit", c.size, parts)
		}
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `go test ./internal/sync/ -run TestMultipartPartSize -v`
Expected: FAIL to build — `undefined: multipartPartSize`.

- [ ] **Step 3: Implement the limits and part size**

In `internal/sync/worker.go`, add next to `maxCopyObjectSize`:

```go
const (
	maxCopyObjectSize      = 5 << 30 // single CopyObject limit
	maxMultipartObjectSize = 5 << 40 // hard object limit on this endpoint
	maxMultipartParts      = 10000
	minPartSize            = 5 << 20
	maxPartSize            = 5 << 30
)
```

In `internal/sync/multipart.go`:

```go
package sync

const mib = int64(1) << 20

// multipartPartSize picks a part size that keeps the part count inside the
// 10,000 limit without making parts absurdly large: proportional to the object,
// rounded up to a whole MiB, clamped to the endpoint's 5 MiB..5 GiB range.
func multipartPartSize(size int64) int64 {
	want := (size + maxMultipartParts - 1) / maxMultipartParts
	// Round up to a whole MiB.
	want = ((want + mib - 1) / mib) * mib
	if want < minPartSize {
		want = minPartSize
	}
	if want > maxPartSize {
		want = maxPartSize
	}
	return want
}
```

Note: `internal/sync/` has no `mib` constant yet (checked), so `mib` is free. `ptr[T any](v T) *T` already exists at `internal/sync/engine.go:457` — use it, do not redeclare.

- [ ] **Step 4: Run it to verify it passes**

Run: `go test ./internal/sync/ -run TestMultipartPartSize -v`
Expected: PASS.

- [ ] **Step 5: Write the failing multipart tests**

In `internal/sync/multipart_test.go`:

```go
func TestCopyObjectMultipartAboveSingleCopyLimit(t *testing.T) {
	api := &fakeCopyAPI{headLen: maxCopyObjectSize + 1}
	wp := newTestWorkerPool(api)

	a := SyncAction{Type: ActionCopy, Key: "big.bin", ETag: `"e1"`}
	if _, err := wp.copyObject(context.Background(), a); err != nil {
		t.Fatalf("copyObject: %v", err)
	}
	if api.createdMPU != 1 {
		t.Fatalf("CreateMultipartUpload calls = %d, want 1", api.createdMPU)
	}
	if api.copied != 0 {
		t.Fatalf("single CopyObject was issued for a %d-byte object", api.headLen)
	}
	for i, r := range api.partRanges {
		if !strings.Contains(r, "-") {
			t.Fatalf("part %d range %q has no explicit end offset", i+1, r)
		}
	}
	if got := api.partsTotal(); got != api.headLen {
		t.Fatalf("parts cover %d bytes, want %d", got, api.headLen)
	}
	if api.completedParts != len(api.partRanges) {
		t.Fatalf("completion carried %d parts, want %d", api.completedParts, len(api.partRanges))
	}
}

func TestCopyObjectMultipartAppliesStorageClassWithoutAttrs(t *testing.T) {
	api := &fakeCopyAPI{headLen: maxCopyObjectSize + 1}
	wp := newTestWorkerPoolWithoutAttrs(api)
	wp.storageClass = "STANDARD_IA"

	if _, err := wp.copyObject(context.Background(), SyncAction{Type: ActionCopy, Key: "big.bin"}); err != nil {
		t.Fatal(err)
	}
	if got := api.mpuStorageClass; got != "STANDARD_IA" {
		t.Fatalf("CreateMultipartUpload storage class = %q, want STANDARD_IA", got)
	}
}

func TestCopyObjectMultipartLockRejectionIsExplained(t *testing.T) {
	api := &fakeCopyAPI{
		headLen:  maxCopyObjectSize + 1,
		headOut:  &s3.HeadObjectOutput{ContentLength: aws.Int64(maxCopyObjectSize + 1), ObjectLockMode: "GOVERNANCE"},
		mpuErr:   errors.New("Bad Request"),
	}
	wp := newTestWorkerPool(api)

	_, err := wp.copyObject(context.Background(), SyncAction{Type: ActionCopy, Key: "big.bin"})
	if err == nil {
		t.Fatal("want an error")
	}
	if !strings.Contains(err.Error(), "object lock") || !strings.Contains(err.Error(), testTargetBucket) {
		t.Fatalf("err = %v, want it to name object lock and the target bucket", err)
	}
}

func TestCopyObjectMultipartAbortsOnPartFailure(t *testing.T) {
	api := &fakeCopyAPI{headLen: maxCopyObjectSize + 1, partErrAt: 2}
	wp := newTestWorkerPool(api)

	if _, err := wp.copyObject(context.Background(), SyncAction{Type: ActionCopy, Key: "big.bin"}); err == nil {
		t.Fatal("want an error")
	}
	if api.aborted == 0 {
		t.Fatal("AbortMultipartUpload was never called")
	}
	if api.abortUploadID != api.createdUploadID {
		t.Fatalf("aborted upload %q, want %q", api.abortUploadID, api.createdUploadID)
	}
}

func TestCopyObjectMultipartAbortsOnCancel(t *testing.T) {
	api := &fakeCopyAPI{headLen: maxCopyObjectSize + 1, cancelAfterPart: 1}
	wp := newTestWorkerPool(api)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if _, err := wp.copyObject(ctx, SyncAction{Type: ActionCopy, Key: "big.bin"}); err == nil {
		t.Fatal("want an error")
	}
	if api.aborted == 0 {
		t.Fatal("AbortMultipartUpload was not called after cancellation")
	}
	if !api.abortContextLive {
		t.Fatal("the abort used the cancelled context and would never reach the server")
	}
}

func TestCopyObjectOverFiveTiBStillFails(t *testing.T) {
	api := &fakeCopyAPI{headLen: maxMultipartObjectSize + 1}
	wp := newTestWorkerPool(api)

	_, err := wp.copyObject(context.Background(), SyncAction{Type: ActionCopy, Key: "huge.bin"})
	if !errors.Is(err, ErrObjectTooLarge) {
		t.Fatalf("err = %v, want ErrObjectTooLarge", err)
	}
	if api.createdMPU != 0 {
		t.Fatal("a multipart upload was started for an object past the hard limit")
	}
}
```

Add to `fakeCopyAPI` in `worker_test.go`: `createdMPU, createdUploadID, partRanges []string, partErrAt int, cancelAfterPart int, mpuErr error, mpuStorageClass string, completedParts, aborted int, abortUploadID string, abortContextLive bool`, plus `partsTotal()`.

- [ ] **Step 6: Run them to verify they fail**

Run: `go test ./internal/sync/ -run 'TestCopyObjectMultipart|TestCopyObjectOverFiveTiB' -v`
Expected: FAIL — `wp.copyObject` issues a single `CopyObject`, `createdMPU calls = 0, want 1`.

- [ ] **Step 7: Extend `copyAPI`**

In `internal/sync/worker.go`:

```go
type copyAPI interface {
	HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	CopyObject(context.Context, *s3.CopyObjectInput, ...func(*s3.Options)) (*s3.CopyObjectOutput, error)
	GetObjectTagging(context.Context, *s3.GetObjectTaggingInput, ...func(*s3.Options)) (*s3.GetObjectTaggingOutput, error)
	PutObjectTagging(context.Context, *s3.PutObjectTaggingInput, ...func(*s3.Options)) (*s3.PutObjectTaggingOutput, error)
	DeleteObject(context.Context, *s3.DeleteObjectInput, ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	CreateMultipartUpload(context.Context, *s3.CreateMultipartUploadInput, ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
	UploadPartCopy(context.Context, *s3.UploadPartCopyInput, ...func(*s3.Options)) (*s3.UploadPartCopyOutput, error)
	CompleteMultipartUpload(context.Context, *s3.CompleteMultipartUploadInput, ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
	AbortMultipartUpload(context.Context, *s3.AbortMultipartUploadInput, ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)
}
```

`var _ copyAPI = (*s3.Client)(nil)` already guards this if present; keep it.

- [ ] **Step 8: Add `createMPUInput`**

In `internal/sync/attrs.go`, next to `copyInput` at `:137`:

```go
// createMPUInput builds the multipart-create request carrying the same
// characteristics copyInput carries, so the two copy paths cannot drift. The
// storage class is a parameter rather than an ObjectAttrs field because it must
// apply even when attribute replication is off.
func (a ObjectAttrs) createMPUInput(bucket, key, storageClass string) *s3.CreateMultipartUploadInput {
	in := &s3.CreateMultipartUploadInput{Bucket: &bucket, Key: &key}
	if a.ContentType != "" {
		in.ContentType = &a.ContentType
	}
	// ... mirror every field copyInput sets that CreateMultipartUpload accepts:
	// ContentDisposition, ContentEncoding, CacheControl, Metadata, ObjectLockMode,
	// ObjectLockRetainUntilDate (only when liveRetention), ObjectLockLegalHoldStatus.
	if storageClass != "" {
		in.StorageClass = types.StorageClass(storageClass)
	}
	return in
}
```

Never set `Tagging` — the endpoint ignores it, and tags go through `PutObjectTagging` in the existing `writeAttrs` step.

- [ ] **Step 9: Implement `copyMultipart`**

In `internal/sync/multipart.go`:

```go
func (wp *WorkerPool) copyMultipart(ctx context.Context, a SyncAction, attrs ObjectAttrs, size int64) (SyncAction, error) {
	in := attrs.createMPUInput(wp.targetBucket, a.Key, wp.storageClass)
	createOut, err := wp.tgt.CreateMultipartUpload(ctx, in)
	if err != nil {
		return a, wp.explainCopyError(a.Key, attrs, err)
	}
	uploadID := aws.ToString(createOut.UploadId)
	completed := false
	defer func() {
		if completed {
			return
		}
		// Detach from ctx: on cancellation the request context is already done
		// and the abort would never reach the server, leaving parts that this
		// endpoint has no lifecycle rule to reap.
		abortCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		if _, err := wp.tgt.AbortMultipartUpload(abortCtx, &s3.AbortMultipartUploadInput{
			Bucket: &wp.targetBucket, Key: &a.Key, UploadId: &uploadID,
		}); err != nil {
			slog.Warn("multipart: abort failed, parts may be orphaned",
				"key", a.Key, "upload", uploadID, "error", err)
		}
	}()

	partSize := multipartPartSize(size)
	var parts []types.CompletedPart
	for partNumber, start := 1, int64(0); start < size; partNumber, start = partNumber+1, start+partSize {
		end := start + partSize - 1
		if end >= size {
			end = size - 1
		}
		rng := fmt.Sprintf("bytes=%d-%d", start, end) // never open-ended
		out, err := wp.tgt.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
			Bucket: &wp.targetBucket, Key: &a.Key, UploadId: &uploadID,
			PartNumber: ptr[int32](int32(partNumber)),
			CopySource: ptr(fmt.Sprintf("%s/%s", wp.sourceBucket, a.Key)),
			CopySourceRange: &rng,
		})
		if err != nil {
			return a, wp.explainCopyError(a.Key, attrs, err)
		}
		parts = append(parts, types.CompletedPart{
			ETag: out.CopyPartResult.ETag, PartNumber: ptr[int32](int32(partNumber)),
		})
	}

	if _, err := wp.tgt.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket: &wp.targetBucket, Key: &a.Key, UploadId: &uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	}); err != nil {
		return a, wp.explainCopyError(a.Key, attrs, err)
	}
	completed = true
	return a, nil
}
```

- [ ] **Step 10: Branch in `copyObject`**

Replace the size guard at `internal/sync/worker.go:166-168`:

```go
size := aws.ToInt64(headOut.ContentLength)
if size > maxMultipartObjectSize {
	return a, fmt.Errorf("%w: key %q is %d bytes, limit %d",
		ErrObjectTooLarge, a.Key, size, int64(maxMultipartObjectSize))
}
```

and after `attrs` is populated, before `copyBytes`:

```go
if size > maxCopyObjectSize {
	// No 30s cap on the copy itself: a multi-GiB object legitimately takes
	// minutes, and the run budget is the only sensible bound.
	if _, err := wp.copyMultipart(ctx, a, attrs, size); err != nil {
		return a, err
	}
} else {
	copyOut, err := wp.copyBytes(reqCtx, a.Key, attrs)
	if err != nil {
		return a, err
	}
	// ... existing versionID handling ...
}
```

Restructure so the attribute step (`writeAttrs`) runs for both branches. Update `ErrObjectTooLarge`'s message to name the 5 TiB limit.

- [ ] **Step 11: Run the tests**

Run: `go test ./internal/sync/ -run 'TestCopyObject|TestWorkerPool|TestMultipart' -v`
Expected: PASS.

- [ ] **Step 12: Commit**

```bash
git add internal/sync/worker.go internal/sync/attrs.go internal/sync/multipart.go internal/sync/worker_test.go internal/sync/multipart_test.go
git commit -m "feat(sync): multipart copy for objects above the 5 GiB single-copy limit"
```

---

### Task 7: CLI, API, and README surface

**Files:**
- Modify: `cmd/config.go` (add `get`/`set` subcommands)
- Modify: `internal/api/router.go:289-312`, `internal/api/handlers.go`
- Modify: `README.md`
- Test: `internal/config/settings_test.go` (covered), `tests/api_test.go`

**Interfaces:**
- Consumes: `GetSettings`, `SetSetting`, `Min/MaxRunTimeoutSeconds` (Task 3).
- Produces: `GET /api/settings`, `PUT /api/settings`, `s3sync config get|set`.

- [ ] **Step 1: Write the failing API test**

In `tests/api_test.go`. This file has no `newTestServer`/`doJSON` helpers today — each test builds the server inline (`config.Open(":memory:")`, `config.NewRepository`, `api.NewServer(repo, filepath.Join(t.TempDir(), "cache.db"))`, `srv.Router()`; see `:22-33`). Write this test the same inline way rather than inventing helpers:

```go
func TestSettingsRoundTrip(t *testing.T) {
	db, err := config.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	repo := config.NewRepository(db)
	srv, err := api.NewServer(repo, filepath.Join(t.TempDir(), "cache.db"))
	if err != nil {
		t.Fatal(err)
	}
	router := srv.Router()

	call := func(method, path, body string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		var r *http.Request
		if body == "" {
			r, _ = http.NewRequest(method, path, nil)
		} else {
			r, _ = http.NewRequest(method, path, strings.NewReader(body))
			r.Header.Set("Content-Type", "application/json")
		}
		router.ServeHTTP(w, r)
		return w
	}

	if w := call("PUT", "/api/settings", `{"run_timeout_seconds": 1800}`); w.Code != 200 {
		t.Fatalf("PUT /api/settings = %d, body %s", w.Code, w.Body.String())
	}
	if w := call("GET", "/api/settings", ""); !strings.Contains(w.Body.String(), "1800") {
		t.Fatalf("GET /api/settings = %s, want run_timeout_seconds 1800", w.Body.String())
	}
	if w := call("PUT", "/api/settings", `{"run_timeout_seconds": 5}`); w.Code != 422 ||
		!strings.Contains(w.Body.String(), "invalid_setting") {
		t.Fatalf("out-of-range PUT = %d %s, want 422 invalid_setting", w.Code, w.Body.String())
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `go test ./tests/ -run TestSettingsRoundTrip -v`
Expected: FAIL — 404 on `PUT /api/settings`.

- [ ] **Step 3: Add the endpoints**

In `internal/api/handlers.go`:

```go
func (s *Server) getSettings(c *gin.Context) {
	st, err := s.repo.GetSettings()
	if err != nil {
		respondError(c, http.StatusInternalServerError, err.Error())
		return
	}
	respond(c, http.StatusOK, gin.H{"run_timeout_seconds": st.RunTimeoutSeconds})
}

func (s *Server) updateSettings(c *gin.Context) {
	var in struct {
		RunTimeoutSeconds *int `json:"run_timeout_seconds"`
	}
	if err := c.ShouldBindJSON(&in); err != nil {
		respondError(c, http.StatusBadRequest, err.Error())
		return
	}
	if in.RunTimeoutSeconds == nil {
		respondError(c, http.StatusBadRequest, "run_timeout_seconds is required")
		return
	}
	if err := s.repo.SetSetting(config.SettingRunTimeoutSeconds,
		strconv.Itoa(*in.RunTimeoutSeconds)); err != nil {
		respondErrorCode(c, http.StatusUnprocessableEntity, "invalid_setting", err.Error())
		return
	}
	s.getSettings(c)
}
```

Register in `internal/api/router.go` next to `/health`:

```go
api.GET("/settings", s.getSettings)
api.PUT("/settings", s.updateSettings)
```

- [ ] **Step 4: Add the CLI subcommands**

In `cmd/config.go`, add `configGetCmd` and `configSetCmd` and register them:

```go
var configGetCmd = &cobra.Command{
	Use:   "get <key>",
	Short: "Read a global setting",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		repo, closeFn, err := openConfig()
		if err != nil {
			return err
		}
		defer closeFn()
		st, err := repo.GetSettings()
		if err != nil {
			return err
		}
		switch args[0] {
		case config.SettingRunTimeoutSeconds:
			fmt.Println(st.RunTimeoutSeconds)
		default:
			return fmt.Errorf("unknown setting %q", args[0])
		}
		return nil
	},
}

var configSetCmd = &cobra.Command{
	Use:   "set <key> <value>",
	Short: "Write a global setting",
	Args:  cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		repo, closeFn, err := openConfig()
		if err != nil {
			return err
		}
		defer closeFn()
		if err := repo.SetSetting(args[0], args[1]); err != nil {
			return err
		}
		fmt.Printf("%s = %s\n", args[0], args[1])
		return nil
	},
}
```

- [ ] **Step 5: Document it**

In `README.md`, add to the CLI section:

```
s3sync config get run_timeout_seconds
s3sync config set run_timeout_seconds 1800   # 30..86400
```

and replace the "multipart copy is not implemented" sentence in the Architecture block with the real ceiling: objects above 5 GiB are copied with sequential `UploadPartCopy`; how large an object can be depends on the run timeout, roughly 100–200 GiB at the 300 s default, because a 5 TiB object is 10,000 sequential part copies. State that a copy which does not finish inside the run is aborted and restarts from part 1 next cycle.

- [ ] **Step 6: Add the README's timeout arithmetic**

`README.md` must also state that the timeout is a global setting read at the start
of each run, so a change applies to the next run of every pair without a restart.

- [ ] **Step 7: Run everything**

Run: `go build ./... && go vet ./... && go test ./...`
Expected: PASS; `go vet` reports only the two pre-existing copylocks warnings.

- [ ] **Step 8: Commit**

```bash
git add cmd/config.go internal/api/handlers.go internal/api/router.go tests/api_test.go README.md
git commit -m "feat(cli,api): expose the global run timeout"
```

---

## Self-Review

**Spec coverage:** §7.1 → Task 3. §7.2 → Task 4. §7.3 → Tasks 4, 5. §7.4 → Task 6 Step 10. §7.5 → Task 6. §7.6 → Tasks 1, 7. §7.7 → no code (documented in spec §7.7). Bugs §6 → Tasks 1, 2. §5.1 ceiling → Task 7 Step 5 (README) — note the `pair add` timeout warning from spec §5.1 and §9 is NOT a task; it is left to the builder as spec §9 states, and the README statement is the required part.

**Gap found and accepted:** spec §5.1 says `pair add`/`pair update` warn when the timeout cannot finish a source object. That needs a source listing at pair-add time, which is a cost the user did not ask for. It is listed in spec §9 as a builder decision; the plan delivers the README statement and leaves the warning out. If the reviewer disagrees, add it as Task 8.

**Type consistency:** `settingsFn func() config.Settings` (Task 4) matches `Repository.GetSettingsOrDefaults() Settings` (Task 4 Step 5). `createMPUInput(bucket, key, storageClass string)` (Task 6 Step 8) matches its call site in `copyMultipart` (Task 6 Step 9). `multipartPartSize(int64) int64` consistent across Task 6 Steps 3, 9, 10. `Engine.SweepCursor()` used by the Task 5 test and defined in Task 5 Step 3.

**Review Focus coverage:** dry-run → Task 1. Sweep progress → Task 5 Steps 1, 6. Abort on failure/cancel → Task 6 Steps 5, 9. Storage class without attrs → Task 6 Step 5. Lock rejection message → Task 6 Step 5.
