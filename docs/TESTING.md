# Test Suite and Results — v0.3.0

What is tested, how it is run, and what the runs actually produced. Every number
here comes from a recorded run against Cubbit DS3 (`https://s3.cubbit.eu`), not
from an estimate. Where a figure is a single measurement that varies, the
variance is stated rather than averaged away.

- Version: **0.3.0** (commit `23f5dfc`, tag `v0.3.0`)
- Endpoint: Cubbit DS3, `https://s3.cubbit.eu`, region `us-east-1`
- Go: `go test` on the host plus cross-compiled `linux/amd64` binaries on
  `51.159.133.103` (Ubuntu 26.04, 2 cores, 3 GB RAM, 5.1 GB free disk)

## 1. Inventory

| Package | Default suite | With `-tags=integration` | What it covers |
| --- | --- | --- | --- |
| `cmd` | 3 | 3 | CLI flag wiring, `ValidatePairOptions` |
| `internal/cache` | 3 | 3 | Bolt store, cursors |
| `internal/config` | 19 | 19 | Schema migrations, repository CRUD, encryption, settings, setup wizard |
| `internal/log` | 1 | 1 | Logger construction |
| `internal/s3client` | 1 | 1 | Client construction |
| `internal/sync` | 124 | 124 | Worker, differ, engine, multipart, attrs, reconcile, provision, lifecycle, validate, notify |
| `tests` | 7 | 16 | API handlers; 8 endpoint-claim tests + 1 live sync test behind the tag |
| `tests/scenario` | 14 | 75 | Behavioural scenarios + harness/synth unit layer |
| **Total** | **172** | **242** | |

`go test ./...` runs 172 tests with no endpoint and no credentials. The other 70
need a live endpoint.

Two files are worth calling out because their placement is deliberate:

- `tests/scenario/pure_test.go` is **not** behind the integration tag, even
  though it lives in an integration package. It only exercises
  `ShouldReconcile` and `ValidatePairOptions` on a plain struct — pure functions
  with no endpoint involved. Gating it would have meant the cheapest regression
  checks never ran in the default suite.
- `tests/scenario/synth.go` and `counting.go` are untagged production-shaped
  helpers, so their unit tests (`synth_test.go`) run by default. The synthetic
  data generator is what lets a 6 GiB object be verified from a few hundred
  kilobytes, so it is tested on its own.

## 2. How to run

```bash
# Unit suite: no endpoint, no credentials, ~56s (dominated by internal/sync's
# retry-path tests, which sleep through real backoff)
go test ./...

# Endpoint claims: 8 behaviours the tool's correctness rests on
export S3_TEST_ENDPOINT=https://s3.cubbit.eu
export S3_TEST_ACCESS_KEY=... S3_TEST_SECRET_KEY=...
go test -tags=integration ./tests/ -run TestDS3 -v

# Behavioural scenarios, on a host with ~10 GB free disk
./tests/run-scenarios.sh fast root@your-host
```

Scenario tiers, selected with `SCENARIO_TIER`:

| Tier | Tests run | Measured wall clock | Adds |
| --- | --- | --- | --- |
| `fast` | 65 pass / 10 skip | **239 s** | Everything needing no large object and no bulk seeding |
| `full` | 68 pass / 7 skip | **889 s** | Adds the 5 GiB object just below the single-copy limit, the 1,200-object listing scenario, and 120 objects with deletes |
| `all` | 75 pass | hours | Adds the 5 GiB plus-one boundary, a 6 GiB object, 500 locked objects |

The same `fast` tier measured **700 s** on an earlier run and **239 s** on the
run recorded here — a 3x spread on identical code. This endpoint's per-request
latency varies that much. Treat every duration below as an order of magnitude,
not a budget.

`SCENARIO_TIMEOUT` overrides `go test`'s own timeout (45m / 90m / 4h by tier).
That matters: a `go test` timeout panics without running `t.Cleanup`, so every
bucket alive at that moment leaks and bills forever on this endpoint.

## 3. Results: unit suite

`go test -count=1 ./...` — **all green**, 172 tests:

```
ok  github.com/esignoretti/S3sync/cmd                 0.594s
ok  github.com/esignoretti/S3sync/internal/cache      0.432s
ok  github.com/esignoretti/S3sync/internal/config     0.510s
ok  github.com/esignoretti/S3sync/internal/log        0.362s
ok  github.com/esignoretti/S3sync/internal/s3client   0.435s
ok  github.com/esignoretti/S3sync/internal/sync      54.738s
ok  github.com/esignoretti/S3sync/tests               0.623s
ok  github.com/esignoretti/S3sync/tests/scenario      0.419s
```

`go vet ./...` reports two warnings, both **pre-existing at the base commit**
`bedb1f4`, neither introduced here:

```
internal/sync/engine.go:500:58: return copies lock value: ...Progress contains sync.Mutex
internal/api/handlers.go:529:22: assignment copies lock value to resp["progress"]: ...Progress contains sync.Mutex
```

`gofmt -l` lists `internal/api/router.go` and `internal/cache/store.go`, also
pre-existing.

`go test -race` is clean on the concurrency-sensitive packages: reconcile,
multipart, `RunOnce`, `WorkerPool`, `StreamingDiff`.

## 4. Results: endpoint claims (8 tests, all pass)

`tests/ds3_claims_test.go` verifies the eight endpoint behaviours the tool
depends on. Each, if wrong, produces silently incorrect data rather than an
error — which is why they are tested rather than assumed.

| Claim | Result |
| --- | --- |
| `CopyObject` ignores an inline `x-amz-tagging` header | Confirmed |
| `CopyObject` carries the source's own tags by default | Confirmed (see note) |
| An empty `<TagSet/>` via `PutObjectTagging` clears tags | **Accepted** |
| Metadata needs `MetadataDirective: REPLACE` | Confirmed |
| `Content-Language` and `Expires` are not stored | Both read back `nil` |
| Lifecycle `Expiration.Date` works with no `Filter` | Accepted, read back `2027-11-15T00:00:00Z` |
| Lock headers apply inline on copy | `mode=GOVERNANCE until=2027-11-15T21:10:44Z`, legal hold ON |
| Lock cannot be enabled after bucket creation | `PutObjectLockConfiguration` → 400 `InvalidRequest`; `GetObjectLockConfiguration` → `ObjectLockConfigurationNotFoundError` |
| `x-amz-copy-source-if-*` is not evaluated | Confirmed: copy proceeds unconditionally |

**Note on tag propagation.** The last probe corrected our own documentation.
`CopyObject` *does* carry the source's tags by default, because `TaggingDirective`
defaults to `COPY`; only an *inline* `x-amz-tagging` header is ignored. Measured
on a source tagged `team=alpha`:

| Request | Target tags |
| --- | --- |
| no directive | `[team=alpha]` |
| `MetadataDirective=REPLACE` only | `[team=alpha]` |
| `TaggingDirective=COPY` | `[team=alpha]` |
| `TaggingDirective=REPLACE other=value` | `[team=alpha]` |

Consequence: with `--replicate-attributes` **off**, the target still inherits
tags. The separate `PutObjectTagging` call is redundant-but-harmless (it is what
makes clearing explicit), at the cost of one extra request per object.

`TestDS3Claim_CopyIgnoresInlineTagging` was initially confounded: it seeded tags
with an inline `Tagging` field on `PutObject`, which this endpoint also ignores,
so the source had no tags and the copy proved nothing. It now seeds via
`PutObjectTagging` and requests a *different* inline tag.

## 5. Results: scenarios (fast tier, 65 pass / 10 skip)

Wall clock **239 s**. Skips are tier gates, not failures.

### Small objects and source changes

| Scenario | Time | Asserts |
| --- | --- | --- |
| `EmptySource` | 1.06 s | Run succeeds, target untouched, no actions |
| `Tiny` | 2.03 s | 0-byte and 1-byte objects copy |
| `SingleObject` | 1.95 s | Bytes, content-type, metadata |
| `SourceChanges` | 5.37 s | Only changed keys re-copy (≥10 copies) |
| `SourceDeletedWithPropagationOff` | 1.94 s | Deleted source key is kept on target |
| `SourceDeletesThenRestores` | 2.59 s | Restored object re-copies despite identical ETag |
| `AwkwardKeys` | 3.39 s | 11 keys with spaces, `%`, `+`, unicode, `/`, trailing dot |
| `ManySmall` | *skip (full)* | 1,200 objects crossing the 1000-key page boundary |
| `ManySmallWithDeletes` | *skip (full)* | 120 objects with delete propagation |

### Large objects

| Scenario | Time | Asserts |
| --- | --- | --- |
| `ZeroByteNeverMultipart` | 1.57 s | Empty object takes the single-copy path, 0 `UploadPartCopy` |
| `FiveGiBMinusOne` | *skip (full)* — 313 s when run | 5 GiB − 1 byte copies as ONE `CopyObject`, 0 part copies |
| `FiveGiBPlusOne` | *skip (all)* | The boundary is crossed: multipart is used |
| `SixGiB` | *skip (all)* | 6 GiB via multipart, verified by range |
| `LargeWithAttrs` | *skip (all)* | Characteristics survive a multipart copy |
| `LargeSecondSyncIsNoop` | *skip (all)* | Fingerprint round-trips through the sweep |
| `LargeCancelMidCopy` | *skip (all)* | Cancellation leaves 0 incomplete uploads |

### Versioning

| Scenario | Time | Asserts |
| --- | --- | --- |
| `VersionedSource` | 2.08 s | Current version only is synced |
| `VersionedTarget` | 2.27 s | Exactly 2 versions after two syncs — also catches a double copy |
| `DeleteMarkerOnSource` | 2.22 s | A source delete marker is treated as an absent key |
| `NoncurrentLifecycle` | 1.43 s | `s3sync-noncurrent` rule shape |

### Object lock and provisioning

| Scenario | Time | Asserts |
| --- | --- | --- |
| `GovernanceOneDay` | 2.86 s | Mode + retain-until replicated (1 day, the maximum allowed) |
| `ComplianceTwoMinutes` | 122.56 s | COMPLIANCE hold applied; most of the time is waiting it out in teardown |
| `LegalHold` | 2.61 s | Hold ON **and** a version delete refused despite the governance bypass |
| `LockWithoutTargetLock` | 4.73 s | Failure names object lock; cause only appears in the log line |
| `RetentionJustExpired` | 3.96 s | An expired retention is not replicated |
| `LockAtCreation` | 1.55 s | `require_object_lock` provisions a locked bucket |
| `LockMissingFail` | 1.06 s | `--on-lock-missing fail` refuses, writes no pair |
| `LockMissingRecreateEmpty` | 1.45 s | `recreate --force-recreate` works on an empty bucket |
| `LockMissingRecreateNonEmpty` | 1.36 s | Recreate refuses a non-empty bucket |

### Attribute replication

| Scenario | Time | Asserts |
| --- | --- | --- |
| `Metadata` | 2.00 s | Content-type + `x-amz-meta-*` on the target |
| `Tags` | 2.38 s | Two tags on the target |
| `EmptyTagsClearsTarget` | 4.35 s | Source tag removal clears the target |
| `TagDriftRepaired` | 2.49 s | A tag-only edit (no ETag change) is re-copied by the sweep |
| `MetadataDriftRepaired` | 2.65 s | A metadata-only edit is re-copied |
| `StorageClassOverride` | 2.28 s | Fingerprint round-trip with an override configured |
| `UnreplicableFields` | 2.62 s | Neither side retains `Content-Language`/`Expires` |
| `PendingAttrsRecovery` | 2.41 s | A pending-attribute object reaches a *changed* tag |
| `AttrsOff` | 1.76 s | No tag requests at all when replication is off |
| `InvalidStorageClass` | 4.60 s | A rejected class fails loudly |

### Expiry

| Scenario | Time | Asserts |
| --- | --- | --- |
| `ExpireDateRule` | 1.91 s | `s3sync-expire` carries a `Date`, no `Filter` |
| `ExpiredPairRefuses` | 1.29 s | Past expiry ⇒ `ErrPairExpired`, zero target objects |
| `ExpiryRecovery` | 1.91 s | Moving the date forward re-arms the pair |
| `ExpiryCleared` | 1.42 s | Clearing the date warns that the rule survives |
| `NonMidnightRejected` | 0.89 s | A non-midnight date is rejected |
| `PastDateRejected` | 0.94 s | Past date at run time is `ErrPairExpired` |
| `NoncurrentAndExpireRulesTogether` | 1.45 s | Both rules coexist |

### Cross-feature combinations

| Scenario | Time | Asserts |
| --- | --- | --- |
| `DryRunThenReal` | 1.73 s | A dry run then a real run copies everything |
| `DryRunDoesNotProvision` | 1.50 s | No lifecycle written **and** no bucket created |
| `ExpiryWithLockAndVersioning` | 1.60 s | Expiry + lock + versioning together |
| `WorkerCountIndependence` | 12.28 s | 1 / 4 / 16 workers produce identical results |
| `AttrsAndExpiryTogether` | 2.23 s | Attributes + expiry |
| `DeletePropagationWithAttrs` | 2.07 s | Deletes + attributes |
| `LockedVersionedAttrs` | *skip (all)* | 6 GiB composition |
| `ManySmallLocked` | *skip (all)* | 500 locked objects |
| `CLIEndToEnd` | 4.02 s | The shipped binary: `bucket add` → `pair add` → `pair sync` ×2 |

## 6. Bugs the suite found

The suite earned its keep during construction. Each of these was found by a
failing test, not by reading code.

**Production bugs (fixed in this release):**

1. **`CopySource` was not escaped** (`internal/sync/worker.go`,
   `internal/sync/multipart.go`). `fmt.Sprintf("%s/%s", bucket, key)` is not
   escaped by the SDK, so a key containing `%` was decoded by the endpoint into a
   *different* key → `404 NoSuchKey`. Such an object could never be copied. Fixed
   with `copySource()` using `url.URL.EscapedPath()`. Proven live: raw → 404 for
   `percent%20.txt` only; escaped → all 11 awkward keys copy.
2. **`StreamingDiff` deleted live data on a listing error** (found while adding
   the run-timeout test). A mid-stream `ListObjectsV2` failure was treated as
   end-of-list; with delete propagation on, the merge join then emitted
   `ActionDelete` for every cached object after the failed page. Fixed to return
   the error.

**Test-suite bugs found by review (fixed):**

3. **The byte assertion could not detect a truncated copy.** `assertBytesMatch`
   took the expected length from the *target*, and the synthetic stream is
   defined by `(key, size)` — so a target holding a prefix was exactly what the
   expectation described. A copy that wrote nothing gave `sha256("")` against an
   expected `sha256("")` and passed.
4. **The zero-byte fixtures were never zero-byte.** `sizeFor` matched on
   `-<shape>.` and fell through to the 1–64 KiB shape for `obj/zero.bin`.
5. **Attribute replication was largely untested.** With `ReplicateAttributes`
   forced to `false`, four of the attribute scenarios still passed — because
   `CopyObject` copies metadata and tags by default on this endpoint.
6. **The counting transport miscounted.** `url.Values.Get("tagging")` returns
   `""` for the SDK's valueless `?tagging` sub-resource, so every tag write was
   counted as a `PutObject`; and requests that never reached the endpoint were
   counted as work performed.
7. **The idempotence assertion checked 3 of 8 request kinds**, so a second sync
   that re-issued tag reads/writes passed it.
8. **`SCENARIO_KEEP` was untested at the wiring level** — only the predicate was
   pinned, so a regression that read `t.Failed()` at the wrong time would have
   turned the debug flag into a silent no-op or a leak.

## 7. Known limits

Stated plainly, because a test document that only lists successes is not useful.

- **`TestScenarioStorageClassOverride` cannot discriminate on this endpoint.**
  Only `STANDARD` is accepted (all 10 other classes → 400 `InvalidStorageClass`)
  and a plain `PutObject` already reports `STANDARD`. What the test actually
  pins is the fingerprint round-trip with an override configured — which caught a
  real bug (writer hashing the target class, sweep hashing the source class, so
  every pair with `TargetStorageClass` set re-copied forever). The class *value*
  becomes a meaningful assertion only on an endpoint honouring other classes.
- **The `all` tier is not routinely run.** The 5 GiB plus-one boundary and the
  6 GiB multipart object are ~20 minutes each. The multipart path is covered at
  the unit level (`internal/sync/multipart_test.go`, 13 tests) and was verified
  once by hand live: a 12 MiB object as 3 parts, byte-identical
  (`sha=9c2b38b1f4e85077`), 0 incomplete uploads afterwards.
- **Timing is not asserted.** No scenario asserts a duration or a throughput
  figure; those vary 3x between runs on this endpoint.
- **COMPLIANCE fixtures use a 2-minute hold**, not the 1 day used for
  GOVERNANCE, because COMPLIANCE cannot be bypassed by anyone and teardown would
  otherwise hang. The 2 minutes are real wall clock in the `fast` tier (122 s of
  the 239 s total).
- **No coverage percentage is claimed.** There is no threshold and no gate;
  coverage was never measured for this document.
- **`ListBuckets` on this endpoint is eventually consistent** and keeps listing
  deleted buckets, so the leak detector confirms each candidate with a `HeadBucket`
  before reporting.

## 8. Leak hygiene

Scratch buckets are named `s3sync-sc-<unixnano>` and destroyed by a teardown that
paginates `ListObjectVersions` (including delete markers), aborts incomplete
multipart uploads, lifts legal holds, waits out COMPLIANCE retention, retries
`DeleteBucket` with the governance bypass, and verifies the result with a
`HeadBucket` 404.

`TestLeakedBucketsIsQuietAfterACleanRun` reports — rather than fails on — buckets
left by an earlier killed run, because they are not the current run's fault but
they bill forever. It found **27 leaked buckets** from runs I interrupted during
development; all were cleaned. The current `fast` and `full` tiers finish with
**zero** leaks.
