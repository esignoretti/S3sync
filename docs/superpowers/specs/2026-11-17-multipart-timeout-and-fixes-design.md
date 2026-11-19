# Multipart copy, configurable run timeout, and two bug fixes — design

Date: 2026-11-17
Branch: `feat/multipart-timeout` (off `main` after PR #6 merges)

## 1. Intent

Three independent pieces of work, chosen from a longer list of candidate
enhancements. Two are bugs that are actively wrong today; two are features that
make the tool usable on real buckets.

1. **Fix:** dry-run writes to the sync cache, so a dry run marks objects synced
   that were never copied.
2. **Fix:** a webhook configured with the event `success` never fires, because
   the status it is compared against is spelled `ok`. The event name is
   reachable only through `PUT /api/sync-pairs/:id` and the dashboard; there are
   no `--webhook-*` flags on `pair add`/`pair update`.
3. **Feature C:** the per-run timeout is a hardcoded 5 minutes and one budget
   covers both the drift sweep and the diff, so a large sweep starves the diff.
   Make the timeout a global setting and give the sweep its own share of it.
4. **Feature B:** objects over 5 GiB fail permanently with `ErrObjectTooLarge`.
   Implement multipart copy.

Not chosen (recorded so the reasoning is not lost): prefix filtering (needs a
cache-key scoping decision, larger than this work), verify mode, a metrics
endpoint, webhook retry.

## 2. Goals

- A dry run leaves the cache exactly as it found it.
- `success` and `ok` both mean "the run finished without failures", everywhere.
- An operator can raise the run timeout without rebuilding, and the value takes
  effect on the next run of every pair without restarting the server.
- A sweep that overruns cannot consume the budget the diff needs.
- Objects above 5 GiB sync instead of failing every cycle, up to whatever the
  configured run timeout can actually complete (see §5.1 for the arithmetic).
- Objects that cannot finish inside the run timeout fail with a clear,
  per-object error naming the limit that stopped them, rather than an opaque
  abort.
- The drift sweep makes progress across runs even when a full pass does not fit
  in its budget.

## 3. Non-goals

- Prefix filtering.
- Parallel part uploads.
- Resumable multipart copy across runs. A copy that does not finish inside the
  run is aborted and restarted from part 1 next cycle. Delivering the full 5 TiB
  ceiling would require persisting upload IDs and completed parts, which is a
  cache-schema change and a state machine — explicitly deferred.
- Reaping orphaned multipart uploads left by a process kill. There is no
  lifecycle rule on this endpoint to do it, and a reaper is a separate feature.
- Any change to the attribute fingerprint, the cache schema, or the pair schema.

## 4. Anti-goals

- A timeout that differs between `s3sync serve`, `pair sync` and a cron-driven
  one-shot. One setting, one meaning, every entry point.
- A multipart path that leaves orphaned parts behind. Cubbit does not support
  the `AbortIncompleteMultipartUpload` lifecycle rule, so nothing reaps them and
  they bill forever.
- A setting that only applies after a restart. That is the exact bug fixed for
  pairs in PR #6 (`Server.restartEngineLoop`); repeating it for a global setting
  would be worse, because the operator has no way to notice.

## 5. Constraints

Verified against Cubbit DS3 live on 2026-11-17 (12 MiB object, 3 parts):

| Constraint | Evidence |
|---|---|
| `UploadPartCopy` is supported | 3 parts copied, `CompleteMultipartUpload` succeeded, bytes byte-identical |
| Open-ended ranges are rejected | `bytes=5242880-` → `InvalidRange`; `bytes=5242880-6291455` → OK |
| Lock headers are accepted on `CreateMultipartUpload` | `GOVERNANCE` + retain-until applied and read back on the completed object |
| Metadata and content-type survive on `CreateMultipartUpload` | `text/x-probe` and `Origin: probe` read back on the completed object |
| Completed multipart ETag differs from the source ETag | `...-3` vs source; the cache stores the **source** ETag from the differ, so `StreamingDiff` is unaffected |
| Max object size 5 TiB, max 10,000 parts, min part 5 MiB | Cubbit multipart limits table |
| `AbortIncompleteMultipartUpload` lifecycle rule unsupported | Cubbit lifecycle docs |
| No settings store exists | `internal/config/db.go` creates only `buckets`, `sync_pairs`, `sync_logs`; no config file; no env var except the master key |
| `reconcileSweep` has no cursor | `internal/sync/reconcile.go:46-55` calls `e.store.List(e.pair.ID)` and iterates from index 0 every run; `internal/cache/store.go:87-100` returns the whole bucket. A sweep stopped by its budget restarts at the same place next run and never converges. |

### 5.1 What the run timeout buys for a large object

A 5 TiB object is 10,000 sequential `UploadPartCopy` calls at the part size
below. At a plausible 3–5 s per part that is 8–14 hours. Against the default
300 s run timeout, only objects up to roughly 100–200 GiB complete. This is
deliberate and is the honest ceiling: the multipart feature removes the 5 GiB
wall and makes the ceiling a setting the operator controls, rather than
deferring a resumable-copy state machine. The README must state the arithmetic,
and `pair add`/`pair update` warn when the configured timeout cannot plausibly
finish an object the source bucket already holds.

A Nextcloud bug report claims `UploadPartCopy` is unsupported by Cubbit. That is
stale: it was verified working end to end on the live endpoint during this
design. Recorded here so the claim is not re-raised.

## 6. Approaches

### Bug fixes

One way to fix each; no alternatives worth naming.

- Dry-run: guard the cache write with `!e.pair.DryRun` (`internal/sync/engine.go:347`).
  Also add `--dry-run` to `pair add`/`pair update`: the field exists only on the
  API (`internal/api/handlers.go:235`), so today the API is the only way to
  spring a trap that corrupts the cache.
- Webhook: map `success` → `ok` inside `shouldWebhook`
  (`internal/sync/notify.go:61`), keeping `ok` working. Both spellings accepted
  because the UI label, the original spec and the README all say `success`,
  while the status column says `ok`.

### Feature C — run timeout

| Approach | Trade-off |
|---|---|
| **A. `settings` table + `config set/get` + `/api/settings`** (chosen) | One setting, one meaning, works for `serve`, `pair sync` and cron. Costs a table, a repo accessor, a CLI subcommand and two endpoints. |
| B. `--run-timeout` flag on `serve` only | Cheapest, but a cron-driven `pair sync` would keep the old hardcoded value. A timeout that depends on how the run was started is a bug generator. |
| C. Env var only | Same split-brain as B, plus invisible in `config show`. |

Chosen: **A**.

### Feature C — sweep budget

| Approach | Trade-off |
|---|---|
| **A. Sweep gets a share of the run budget; overrun cancels the sweep and the diff proceeds** (chosen) | Fixes starvation with no second setting. Requires a rotating in-memory cursor (§7.3), because without one a truncated sweep restarts at the same key every run and never converges. |
| B. Second setting `sweep_timeout_seconds` | More control, but two knobs that must be kept consistent, and the user asked for one global timeout. |
| C. Run the sweep after the diff | The sweep would emit actions no consumer is left to read. Structural rewrite for no gain. |

Chosen: **A**. Sweep budget = `min(25% of run timeout, time left in the run)`.

### Feature C — how a truncated sweep makes progress

| Approach | Trade-off |
|---|---|
| **A. Rotating in-memory cursor on the Engine** (chosen) | No schema change; converges within a process lifetime; a restart begins again at the first key, which is harmless because the sweep is idempotent and eventually re-covers everything. |
| B. Persisted cursor in the cache store | Survives restarts, but changes the cache schema and needs a migration for a value that only optimises ordering. |
| C. Best-effort prefix, documented | Cheapest, but drift past the cut-off is permanently invisible — the exact failure the sweep exists to prevent. |

Chosen: **A**.

### Feature B — multipart copy

| Approach | Trade-off |
|---|---|
| **A. Inline multipart inside `copyObject`** (chosen) | Three more methods on `copyAPI`, every existing invariant preserved (differ decides, attributes applied after, cache written by the engine). |
| B. Stream through GetObject/PutObject | Doubles bandwidth, needs local buffering, loses server-side copy entirely. |
| C. `feature/s3/manager` | New dependency, and its `Copy` path still fails above 5 GiB. |

Chosen: **A**.

**Part size:** `max(5 MiB, ceil(size/10000) rounded up to a MiB)`, capped at
5 GiB. Proportional, so a 5 TiB object uses ~10,000 parts of ~550 MiB rather
than 1,000 parts of 5 GiB.

**Sequential parts, not parallel.** The worker pool already parallelises across
objects; parallel parts would multiply get-op pressure against
`MaxGetOpsPerMinute` and make the throttler the bottleneck. Slower per large
object, simpler and safer.

**Threshold is not configurable.** 5 GiB is the endpoint's limit, not a
preference. Automatic above the limit, no flag: these objects fail today and
should simply start working.

## 7. Design

### 7.1 Settings store

New table, created in `migrate()` (`internal/config/db.go`):

```sql
CREATE TABLE IF NOT EXISTS settings (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
)
```

`internal/config/settings.go`:

```go
type Settings struct {
    RunTimeoutSeconds int // default 300
}

const (
    SettingRunTimeoutSeconds = "run_timeout_seconds"
    DefaultRunTimeoutSeconds = 300
    MinRunTimeoutSeconds     = 30
    MaxRunTimeoutSeconds     = 86400
)

func (r *Repository) GetSettings() (Settings, error)
func (r *Repository) SetSetting(key, value string) error  // validates known keys
```

`GetSettings` returns defaults for absent rows and for unparseable values (log a
warning rather than fail the run — a corrupt settings row must not stop syncing).
`SetSetting` rejects an unknown key and an out-of-range timeout.

### 7.2 Making the setting take effect without a restart

The engine reads settings **at the start of every run**, not at construction:

```go
// Engine
settingsFn func() config.Settings
```

`NewEngine` takes the function; `RunOnce` calls it and derives the run context.
Default when nil: `config.Settings{RunTimeoutSeconds: 300}`.

Rationale: the pair-update bug in PR #6 was an engine holding a stale copy of
its configuration. Reading per run makes staleness structurally impossible for
this value, and avoids bolting a settings-restart hook onto the same mechanism.

### 7.3 Run and sweep budgets

In `RunOnce` (`internal/sync/engine.go:176`):

```go
s := e.settings()
runTimeout := time.Duration(s.RunTimeoutSeconds) * time.Second
ctx, cancel := context.WithTimeout(ctx, runTimeout)
```

Sweep, when due, gets its own child context:

```go
sweepBudget := runTimeout / 4
sweepCtx, sweepCancel := context.WithTimeout(ctx, sweepBudget)
go func() { sweepErrCh <- e.reconcileSweep(sweepCtx, actions) }()
```

A sweep cancelled by its budget returns `context.DeadlineExceeded`, which the
existing handler already logs and continues past (`engine.go:277-279`) — the
diff then runs with the remaining budget. `LastReconcileAt` is not updated, so
the sweep is due again next cycle. That is the intended semantics: a sweep that
did not finish has not converged.

**Progress across runs.** Because `reconcileSweep` walks the cache from index 0
every time (`internal/sync/reconcile.go:46-55`), a truncated sweep would restart
at the same key and never get past the cut-off. The Engine therefore keeps a
rotating cursor:

```go
// Engine, guarded by e.mu
sweepCursor string // last key fully examined; "" means start at the front
```

`reconcileSweep` skips entries whose key sorts at or before `sweepCursor`,
advances the cursor after each fully-examined entry, and resets it to `""` when
the walk reaches the end. A sweep that finishes normally still resets to `""`,
so a converged sweep always begins from the front next time. A restart loses the
cursor and begins at the first key — harmless, because the sweep is idempotent
and a full pass re-covers everything.

### 7.4 Per-object timeout for copies

Found while designing this: `copyObject` wraps HEAD, copy and attribute writes in
a single 30-second `reqCtx` (`internal/sync/worker.go:150-151`). That is already
too short for a multi-GiB single copy on a slow link, and it is fatal for
multipart, where one object legitimately takes minutes.

The copy path stops applying its own 30-second cap: `copyObject` derives the
copy context from the run context with no additional timeout, so an object's
copy is bounded only by the run budget (§5.1) — which is exactly the intended
semantics, since a copy cannot usefully outlive the run that would cache it. The
30-second context stays for HEAD and tag reads, which are small and should fail
fast.

### 7.5 Multipart copy

`copyAPI` (`internal/sync/worker.go:29-35`) gains four methods:
`CreateMultipartUpload`, `UploadPartCopy`, `CompleteMultipartUpload`,
`AbortMultipartUpload`.

`copyObject` branches after the size check:

```go
if size > maxCopyObjectSize {        // 5 GiB: single-copy limit
    if size > maxMultipartObjectSize { // 5 TiB: hard limit
        return a, fmt.Errorf("%w: key %q is %d bytes, limit %d", ErrObjectTooLarge, ...)
    }
    return wp.copyMultipart(reqCtx, a, attrs, size)
}
```

`copyMultipart`:

1. `CreateMultipartUpload` with the attribute input (content-type, metadata,
   cache-control, content-disposition, content-encoding, storage class, and the
   lock fields when the source is locked).
2. `defer` an abort that fires unless `complete` succeeded — including on context
   cancellation and on panic.
3. For each part: `UploadPartCopy` with an explicit
   `CopySourceRange: bytes=<start>-<end>`. Never open-ended.
4. `CompleteMultipartUpload` with the collected `{ETag, PartNumber}` pairs.
5. Return the same `SyncAction` shape as the single-copy path so the existing
   attribute step (`writeAttrs`, which still handles tags via
   `PutObjectTagging`) and the cache write are unchanged.

`ObjectAttrs` gains
`createMPUInput(bucket, key, storageClass string) *s3.CreateMultipartUploadInput`,
sharing field assembly with the existing `copyInput` so the two cannot drift.
The storage class is a parameter, not a field of `ObjectAttrs`, because it must
apply even when attribute replication is off — otherwise a `--storage-class`
pair would put a >5 GiB object in the wrong tier while every smaller object
landed in the right one.

`copyMultipart` routes its `CreateMultipartUpload` and
`CompleteMultipartUpload` errors through the existing `explainCopyError`, so a
target bucket that rejects the lock fields produces the same message naming
object lock that the single-copy path produces.

The abort must use a context detached from the cancelled request context
(`context.WithoutCancel` or a fresh short-lived context), or the abort request
is itself cancelled and never reaches the server. A `defer` cannot run after
`SIGKILL`, OOM or node loss; that residue is accepted (§3) and documented.

`ErrObjectTooLarge` keeps its name and its "permanent, not retried" meaning; its
message changes to name the 5 TiB limit. `maxCopyObjectSize` stays 5 GiB and a
new `maxMultipartObjectSize = 5 << 40` is added.

### 7.6 CLI and API surface

- `s3sync config get run_timeout_seconds`
- `s3sync config set run_timeout_seconds <n>` — validates 30..86400
- `s3sync pair add|update --dry-run`
- `GET /api/settings` → `{data: {run_timeout_seconds: n}}`
- `PUT /api/settings` → validates, 422 `invalid_setting` on a bad value
- Dashboard: a small settings block on the pairs page (one number input)

### 7.7 Objects already failing

Objects that failed with `ErrObjectTooLarge` are in no cache and are re-listed
every cycle, so they are picked up on the first run after upgrade with no
migration. Confirmed as the intended behaviour: no special handling.

## 8. Testing

Unit, no network:

- `TestRunOnce_DryRunDoesNotWriteCache` — dry run over a fake source, then assert
  the cache is empty and a second run re-emits the same actions.
- `TestShouldWebhook_AcceptsSuccessAndOk` — table over
  `success|ok|error|all|none|"ok,error"`.
- `TestGetSettings_DefaultsWhenAbsent`, `TestSetSetting_RejectsUnknownKey`,
  `TestSetSetting_RejectsOutOfRange`, `TestGetSettings_UnparseableValueFallsBack`.
- `TestRunOnce_UsesConfiguredTimeout` — settings fn returns a tiny timeout; a
  blocked list call must produce a deadline error naming the run, not a hang.
- `TestRunOnce_SweepOverrunStillRunsDiff` — sweep blocks past its budget; assert
  the diff still emitted actions and `LastReconcileAt` was not updated.
- `TestReconcileSweep_ResumesFromCursor` — a cache of three entries with a
  budget that allows exactly one; assert the first run examined key `a` and
  stopped, the second run started at `b`, and the third wrapped back to `a`.
- `TestReconcileSweep_CursorResetsOnFullPass` — a budget large enough for all
  entries; assert the cursor is `""` afterwards.
- `TestCopyObject_MultipartAboveSingleCopyLimit` — fake API records
  `CreateMultipartUpload` → N × `UploadPartCopy` → `Complete`; assert every range
  is explicit, parts sum to the object size, and completion carried every part.
- `TestCopyObject_MultipartAppliesStorageClassWithoutAttrs` — replication off,
  `storageClass` set; assert `CreateMultipartUpload` carried it.
- `TestCopyObject_MultipartLockRejectionIsExplained` — `CreateMultipartUpload`
  fails with a bare 400 and lock fields were carried; assert the error names
  object lock and the target bucket.
- `TestCopyObject_MultipartAbortsOnPartFailure` — part 2 fails; assert
  `AbortMultipartUpload` was called with the same upload ID.
- `TestCopyObject_MultipartAbortsOnCancel` — cancel mid-parts; assert the abort
  request still reached the fake (i.e. it did not inherit the cancelled context).
- `TestCopyObject_OverFiveTiBStillFails` — `ErrObjectTooLarge`, no MPU started.
- `TestCopyObject_PartSizeFormula` — table over 5 GiB+1, 100 GiB, 5 TiB: assert
  part size ≥ 5 MiB, ≤ 5 GiB, part count ≤ 10,000.

Integration (`tests/ds3_claims_test.go`, `//go:build integration`):

- `TestDS3Claim_MultipartCopyCarriesAttributes` — a real >5 GiB object is
  expensive; instead run the multipart path with a forced small part size against
  a 12 MiB object and assert bytes, metadata, content-type and retention.

Live check before merge: one real object over 5 GiB, end to end, on Cubbit DS3.
This is not optional — it is the only check that the part-size formula and the
run-timeout interaction hold against the real endpoint.

## 9. Left to the builder

- Whether `--dry-run` belongs on `pair update` as well as `pair add`. Leaning yes
  for symmetry with the other mutable fields.
- The exact shape of the dashboard settings block.
- Whether the `pair add` timeout warning (§5.1) should also fire on
  `pair update` when the timeout is lowered. Leaning yes: lowering the timeout
  is what turns a working large object into a failing one.
- The exact wording of the timeout warning and whether it needs the source's
  largest object size, which requires an extra list or a cached maximum.
- Whether `GetSettings` should cache within a process. Leaning no: one small
  query per run is cheaper than an invalidation problem.
- Part-size rounding granularity (MiB vs 8 MiB). MiB is the proposal.
- Whether to log the effective run timeout once per run at info level. Leaning
  yes: it makes a timeout failure self-explaining.
