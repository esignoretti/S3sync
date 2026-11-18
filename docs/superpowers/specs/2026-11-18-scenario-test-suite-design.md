# Scenario test suite — design

Date: 2026-11-18
Status: approved approach (A), design pending approval

## 1. Intent

Prove the two shipped features — attribute replication and object-locked,
expiring targets — behave correctly across the full matrix of realistic
workloads, against a real S3 endpoint, on a machine that is not the developer's
laptop. The existing suite proves *behaviours in isolation* (does a tag survive a
copy? does the differ emit a copy?). This suite proves *outcomes for whole
sync scenarios*: a bucket of 5,000 small objects, a bucket of a few large ones, a
versioned source with delete markers, a locked target, all of them combined.

The distinction matters because the bugs this project has actually shipped were
not in any single call — they were in the seams: a dry run poisoning the cache, a
sweep hashing different inputs than the writer, a fingerprint recorded from a
different storage class than the sweep computes. Each unit behaved correctly.
Only a whole-scenario run reveals that the second sync copies everything again.

## 2. Goals

- One command runs the whole matrix against a real endpoint and reports per-
  scenario pass/fail.
- Every scenario asserts on **outcome**, not just exit status: object count,
  bytes, content-type, metadata, tags, retention, legal hold, versioning state,
  lifecycle configuration.
- Concurrency/idempotence are first-class: each scenario runs its sync **twice**
  and asserts the second run copies zero objects. That single assertion catches
  the whole class of fingerprint/cache/dry-run bugs above.
- Data is synthetic, deterministic, and cheap to verify at any offset — so a
  6 GiB object can be validated without downloading it and a 5,000-object bucket
  without keeping 5,000 files.
- Cleanup is reliable even with object lock, versions, and delete markers
  present, and is proven to leave no buckets behind.
- Reproducible from any machine: cross-compile, ship, run. No toolchain needed on
  the target host.

## 3. Non-goals

- Not a performance benchmark. Timings are logged, never asserted (endpoint
  throughput varies by an order of magnitude between runs).
- Not a replacement for the existing unit and integration tests. It is additive
  and complementary; the unit tests stay the fast feedback loop.
- Not testing third-party S3 implementations. One endpoint, the one shipped.
- No CI wiring. The target host is provisioned by hand; wiring it into CI is a
  separate decision with its own credential-management problem.

## 4. Anti-goals

- **A suite that passes while the feature is broken.** Every assertion must be
  capable of failing. A scenario that cannot distinguish a working sync from a
  no-op sync is worse than no scenario.
- **A suite that leaks buckets.** Each scenario that leaves a bucket, a locked
  version, or an incomplete upload behind costs money forever on this endpoint
  (no `AbortIncompleteMultipartUpload` lifecycle rule exists).
- **A suite that takes hours.** If it cannot run on a whim it will not be run.

## 5. Constraints discovered (verified live, 2026-11-18)

These are measured facts, not documentation claims. Each one shapes the design.

| Constraint | Evidence | Consequence |
|---|---|---|
| Only `STANDARD` storage class is accepted | `STANDARD_IA`, `REDUCED_REDUNDANCY`, `GLACIER`, `DEEP_ARCHIVE`, `ONEZONE_IA`, `INTELLIGENT_TIERING`, `GLACIER_IR`, `EXPRESS_ONEZONE`, `OUTPOSTS`, `SNOW` all return 400 `InvalidStorageClass` | The storage-class scenario asserts the override is *requested and honoured as STANDARD* and that an invalid class is rejected — there is no second real class to assert |
| Retention accepts sub-day granularity | A 2-minute `COMPLIANCE` hold was honoured and blocked deletion | Lock tests need not wait a day; COMPLIANCE is capped at 2 minutes |
| `COMPLIANCE` cannot be bypassed | `x-amz-bypass-governance-retention: true` still returned 403 | COMPLIANCE fixtures must use a 2-minute retain-until or cleanup blocks for a day |
| `GOVERNANCE` can be bypassed | Same request with the bypass header returned 204 | GOVERNANCE fixtures use the full 1-day retention the brief asks for, and are torn down with the bypass header |
| Upload from the target host is ~2 MB/s | 64 MiB `PutObject` took 37.7s | A 6 GiB object cannot be uploaded directly (~50 min) |
| Server-side copy is ~450 MB/s | 16 `UploadPartCopy` of a 64 MiB seed = 1 GiB in 2.3s | Large objects are **composed** from one small seed upload |
| Target host has 5.1 GB free disk | `df -h /` | A 6 GiB object can never be downloaded, so verification must be range-based |
| Cross-compiled test binaries run unmodified | `GOOS=linux go test -c`, scp, run: existing `TestDS3Claim_*` pass | No Go toolchain needed on the host |

## 6. Approaches considered

### A. Go integration tests in `tests/`, tagged `integration` (chosen)

Reuses the real `Engine`, `WorkerPool`, `differ`, and `ProvisionTarget`
in-process, so assertions read real structs and typed errors. Cross-compiled for
the host.

- **For:** precise assertions; reuses existing harness (`setupS3Client`,
  `ds3Bucket`); failures point at a Go line; the same code path production uses.
- **Against:** the CLI surface is not exercised (no flag parsing, no cobra).
- **Mitigation:** one binary-level smoke test drives the installed `s3sync` CLI
  through a full pair add → sync → verify → delete cycle, so the flag surface is
  covered at least once.

### B. Python/shell harness driving the installed binary

- **For:** closest to operator reality; tests the CLI surface thoroughly.
- **Against:** every assertion becomes string-matching on log output, which is
  brittle against exactly the failure modes that matter — a silently dropped tag
  or a stale fingerprint produces *no* log line to match.
- **Rejected:** poor fault attribution for the class of bug this suite exists to
  catch.

### C. Both

- **For:** maximum coverage.
- **Against:** two harnesses, two cleanup implementations, two data generators.
- **Rejected:** the one thing B adds over A is CLI coverage, which A gets from a
  single smoke test.

## 7. Design

### 7.1 Layout

```
tests/
  scenario/                    # package scenario, build tag: integration
    harness.go                 # bucket factory, cleanup, sync runner, assertions
    synth.go                   # deterministic synthetic data
    large.go                   # server-side object composition
    scenario_small_test.go     # many small objects
    scenario_large_test.go     # few large objects
    scenario_versioning_test.go
    scenario_lock_test.go
    scenario_attrs_test.go
    scenario_expiry_test.go
    scenario_matrix_test.go    # combinations
    scenario_cli_test.go       # the one binary-level smoke test
  run-scenarios.sh             # cross-compile, ship, run, report
```

A separate package keeps the existing `tests/*.go` files untouched and lets the
scenario suite be built and run independently.

### 7.2 Synthetic data (`synth.go`)

The core primitive: **a byte at (seed, offset) is a pure function**, so any range
of any object can be regenerated without storage or transfer.

```go
// synthReader yields deterministic bytes derived from a key-specific seed.
// The same (seed, offset) always produces the same byte, so a verifier can
// regenerate any window of an object — including a 6 GiB one — in memory.
type synthReader struct {
    seed   uint64
    offset int64   // next offset to produce
    limit  int64   // total length
}

func newSynthReader(key string, size int64) *synthReader
func synthBytesAt(key string, off, n int64) []byte   // for verification
func synthChecksum(key string, size int64) [32]byte  // full-object digest, streamed
```

Byte generation uses `splitmix64(seed ^ offset)` — cheap, branch-free, and
reproducible across platforms and Go versions (no `math/rand` global state, no
`hash/maphash` seeding).

Object *shape* variants, so "small objects" is not 5,000 identical files:

| Shape | Size range | Purpose |
|---|---|---|
| tiny | 0 B, 1 B | empty and single-byte objects — the boundary cases a differ gets wrong |
| small | 1 KiB – 64 KiB | the bulk |
| medium | 1 MiB – 8 MiB | crosses the 5 MiB multipart-part boundary |
| large | 100 MiB | several parts |
| huge | > 5 GiB | the single-copy limit, composed not uploaded |

Deterministic key names too: `obj/%04d-%s.bin` so ordering and prefix behaviour
are stable and a failing key is trivially reproducible.

### 7.3 Large objects (`large.go`)

A 6 GiB object is built **on the endpoint**, from one 64 MiB seed:

```go
// composeLarge creates an object of `size` bytes at `key` by copying one seeded
// chunk into parts. It uploads exactly one 64 MiB chunk regardless of size.
func composeLarge(t *testing.T, client *s3.Client, bucket, key string, size int64, mutate func(*s3.CreateMultipartUploadInput))
```

- Seed chunk uploaded once per scenario bucket (`_seed/chunk.bin`, 64 MiB).
- `CreateMultipartUpload` carries the attributes (storage class, lock headers) so
  the composed object is indistinguishable from a directly-uploaded one.
- Parts are `UploadPartCopy` with **explicit closed ranges** (`bytes=a-b`), never
  open-ended — the endpoint rejects `bytes=a-` with `InvalidRange`.
- Part size 64 MiB, so 6 GiB = 96 parts (well under the 10,000 limit).
- `CompleteMultipartUpload` with the quoted ETags the endpoint returns.
- Verification is **range-based**: fetch a few windows (head, middle at a part
  boundary, tail) and compare against `synthBytesAt` for the seed chunk. A 6 GiB
  object is verified in a few hundred KB of transfer.

### 7.4 Harness (`harness.go`)

```go
// scenarioEnv is one isolated scenario's world: its own buckets, its own cache
// directory, its own pair. Nothing is shared, so scenarios can run in any order.
type scenarioEnv struct {
    t        *testing.T
    client   *s3.Client
    source   string
    target   string
    pair     *config.SyncPair
    cacheDir string
}

func newScenarioEnv(t *testing.T, opts envOptions) *scenarioEnv
func (e *scenarioEnv) sync() *sync.Engine         // one RunOnce, fatal on error
func (e *scenarioEnv) syncExpectingError() error  // one RunOnce, returns the error
func (e *scenarioEnv) assertTargetMatches(keys []string)
func (e *scenarioEnv) assertNoWorkOnSecondSync()  // the idempotence assertion
func (e *scenarioEnv) objectCount() int
```

`envOptions` covers the matrix axes: `ObjectLock bool`, `Versioning bool`,
`ReplicateAttrs bool`, `TargetExpireIn time.Duration`, `RetentionMode string`,
`Retention time.Duration`, `WorkerCount int`, `StorageClass string`.

**Cleanup** is the part that must be bulletproof, and the existing
`cleanupBucket` is not sufficient — it lists only the first 1,000 keys, ignores
delete markers, and cannot delete a COMPLIANCE-locked version. The new
`destroyBucket` must:

1. List **all** object versions, paginating on `KeyMarker`/`VersionIdMarker`,
   including `DeleteMarkers` (a delete marker's `VersionId` must be deleted too,
   or the bucket will not empty).
2. Delete each version. If deletion fails with `AccessDenied`, the version is
   under retention: retry with `x-amz-bypass-governance-retention: true`. If it
   still fails, it is COMPLIANCE — record it and fail the test loudly rather than
   hanging, naming the retain-until date.
3. Abort every incomplete multipart upload (`ListMultipartUploads`), because
   orphaned parts bill forever here.
4. Delete the bucket; retry a few times with backoff for eventual consistency.
5. **Verify** with `HeadBucket` returning 404. A cleanup that reports success
   without verifying is how buckets leak.

### 7.5 The scenario matrix

Each scenario is a `t.Run` subtest, so one failure does not hide the others.

**Small objects**
| Scenario | What it proves |
|---|---|
| `Empty` | An empty source bucket syncs cleanly and copies nothing |
| `Tiny` | 0-byte and 1-byte objects copy; a differ that compares only size gets these wrong |
| `ManySmall` (5,000 × 1–64 KiB) | Listing pagination (>1,000 keys, multiple pages), worker pool throughput, cache correctness at scale |
| `ManySmallWithDeletes` | Delete propagation over a paginated listing |
| `SingleObject` | The minimum non-trivial case — the P0 regression's shape |
| `SecondSyncIsNoop` | **Idempotence**: second run copies 0 |
| `SourceChanges` | Modify 10% of keys → exactly those 10% copy |
| `SourceDeletesThenRestores` | Delete marker then re-upload with identical content: the vanished-object path must not leave the key permanently missing |

**Large objects**
| Scenario | What it proves |
|---|---|
| `SixGiB` | Multipart path above the 5 GiB single-copy limit; range-verified |
| `FiveGiBMinusOne` | Just below the limit → single `CopyObject` |
| `FiveGiBPlusOne` | Just above → multipart; pins the boundary in both directions |
| `LargeWithAttrs` | Metadata/tags/lock applied by `CreateMultipartUpload`, not the copy call |
| `LargeSecondSyncIsNoop` | The fingerprint recorded after a multipart copy matches what the sweep computes — the bug this would have caught |
| `LargeCancelMidCopy` | A cancelled large copy aborts its upload; `ListMultipartUploads` is empty afterwards |

**Versioning**
| Scenario | What it proves |
|---|---|
| `VersionedSource` | Current versions only are copied (the documented unit of sync) |
| `VersionedTarget` | Target versioning enabled; an overwrite creates a new version, not a replacement |
| `DeleteMarkerOnSource` | A source delete marker propagates as a target delete, not as a copy of nothing |
| `NoncurrentLifecycle` | `noncurrent-expire-days` produces a `NoncurrentVersionExpiration` rule and nothing else |

**Object lock**
| Scenario | What it proves |
|---|---|
| `GovernanceOneDay` | Retention replicated; target refuses early deletion; teardown via bypass |
| `ComplianceTwoMinutes` | COMPLIANCE replicated; deletion refused even with bypass; expires in 2 min |
| `LegalHold` | Legal hold replicated and blocks deletion independently of retention |
| `LockWithoutTargetLock` | Copy onto an unlocked target fails with the message naming object lock, and does not retry forever |
| `LockExpiredOnSource` | An expired source retention is not replicated (the `liveRetention` predicate) |
| `LockAtCreation` | `require-object-lock` on a new target creates it locked |
| `LockMissingFail` | `on-lock-missing=fail` on an existing unlocked target fails provisioning with `ErrLockMissing` and writes no pair |
| `LockMissingRecreate` | `on-lock-missing=recreate --force-recreate` on an **empty** target recreates it locked |
| `LockMissingRecreateNonEmpty` | The same on a **non-empty** target refuses (`ErrTargetNotEmpty`) — the guard that protects data |

**Attribute replication**
| Scenario | What it proves |
|---|---|
| `Metadata` | Content-type, content-disposition, cache-control, `x-amz-meta-*` all land |
| `Tags` | Tags land via the two-call path (never `x-amz-tagging`) |
| `EmptyTags` | Source with no tags → target's tags are cleared, not left stale |
| `TagDrift` | Retag the source; the sweep detects it and repairs — with `ActionCopy`, so metadata drift is repaired too |
| `MetadataDrift` | Change metadata only (no ETag/LastModified change); sweep repairs |
| `StorageClassOverride` | `--storage-class STANDARD` is requested and the fingerprint records the effective class, so the sweep does not re-copy forever |
| `InvalidStorageClass` | A class the endpoint rejects surfaces a clear error |
| `UnreplicableFields` | `Content-Language`/`Expires` are deliberately not replicated and do not cause perpetual drift |
| `AttrsOff` | With replication off, no `GetObjectTagging` is issued and no fingerprint is recorded |
| `PendingAttrsRecovery` | A failed tag write marks pending; the next run retries only attributes, not bytes |

**Expiry**
| Scenario | What it proves |
|---|---|
| `ExpireDateRule` | `target-expire-at` produces exactly one `s3sync-expire` rule with `Expiration.Date`, no `Filter`/`Prefix` |
| `ExpiredPairRefuses` | A pair past its expiry refuses to run, status `expired`, no S3 call |
| `ExpiryRecovery` | Moving the date forward re-arms the pair and it syncs again |
| `ExpiryCleared` | Clearing the date warns that the existing rule is not removed |
| `NonMidnightRejected` | A non-midnight date is rejected quoting the value |
| `PastDateRejected` | A past date is rejected |

**Matrix (combinations)** — the seams where single-feature tests cannot reach
| Scenario | What it proves |
|---|---|
| `LockedVersionedAttrs` | Lock + versioning + attributes together: every copy carries all three |
| `ExpiryWithLockAndVersioning` | Expiry rule coexists with lock and noncurrent rules in one lifecycle config |
| `ManySmallLocked` | 500 locked small objects: lock applied per object, second sync is a no-op |
| `DryRunThenReal` | A dry run caches nothing; the real run actually copies (the poisoning bug) |
| `DryRunDoesNotProvision` | A dry run creates no bucket and rewrites no lifecycle |
| `WorkerCountOneAndEight` | The same outcome at 1 and 8 workers — no ordering dependence |

That is 47 scenarios. Realistically not all run in every invocation.

### 7.6 Tiering

Running everything takes a while (the 6 GiB scenarios dominate). Three tiers,
selected by `-test.run` or an env var:

| Tier | Contents | Target time |
|---|---|---|
| `fast` | small objects, attrs, expiry, dry-run, matrix — no >5 GiB | ~2 min |
| `full` | fast + large objects + lock + versioning | ~10 min |
| `all` | everything including the 6 GiB multipart and cancel cases | ~25 min |

Default when the tag is present: `full`. `SCENARIO_TIER=fast` narrows it.
Every scenario skips cleanly (not fails) when the endpoint is absent.

### 7.7 Runner (`run-scenarios.sh`)

```
./tests/run-scenarios.sh [tier] [host]
```

1. `GOOS=linux GOARCH=amd64 go test -c -tags=integration -o /tmp/scenario-linux ./tests/scenario/`
2. `scp` to the host.
3. Run with credentials from the local environment, streamed over SSH (never
   written to disk on the host, never in a file in the repo).
4. Tee output locally; exit non-zero on any failure.
5. Print a summary: scenarios run / passed / failed / skipped, and the wall time.

Credentials come from `S3_TEST_ENDPOINT`/`S3_TEST_ACCESS_KEY`/`S3_TEST_SECRET_KEY`
in the caller's environment, passed via `ssh -o SendEnv` or an inline `env` prefix
— never committed, never echoed.

### 7.8 Failure ergonomics

A scenario failure must be actionable from the output alone:

- Every assertion names the key and the field: `key "obj/0042-small.bin": tag
  "team" = "" want "alpha"`.
- A bucket is **preserved** on failure when `SCENARIO_KEEP=1`, with its name
  printed, so it can be inspected; otherwise it is destroyed.
- The sync log for a failing scenario is captured and printed (it is already
  `slog` structured; the harness sets a test-scoped handler).

## 8. What is left to the builder

- Whether to reuse `tests/setupS3Client` or duplicate it (it is unexported in
  package `tests`; a new package cannot import it — duplication or a small shared
  package, builder's call).
- Exact 5,000-object runtime on this endpoint; adjust the count if it exceeds the
  tier budget.
- Whether `assertNoWorkOnSecondSync` should assert on `Engine.LastResult()` or on
  the count of `CopyObject` requests seen by a wrapper client. The latter is
  stronger (it catches a copy that happens and is then cached); builder's call,
  recommendation is the wrapper.
- Cleanup backoff parameters.
- Whether the CLI smoke test belongs in this package or stays in `tests/`.
