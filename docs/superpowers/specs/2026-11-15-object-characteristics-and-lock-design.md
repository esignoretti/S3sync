# Object Characteristic Replication & Locked/Expiring Target — Design

**Date:** 2026-11-15
**Status:** Draft — awaiting approval
**Repo:** `esignoretti/S3sync` @ `bedb1f4`

---

## 1. Intent

Two features, both selected **at sync-pair creation time**:

1. **Replicate object characteristics**, not just bytes: content-type/metadata, user tags, object-lock retention + legal hold, storage class — for the *current* version of each object.
2. **Create the target as a locked, expiring replica**: force object lock (fail if the existing bucket cannot be locked), and set a fixed expiry date after which the target's objects are deleted by lifecycle.

The why: a replica is only useful as a compliance/WORM archive if the target reproduces the *retention semantics* of the source and has a bounded lifetime. Bytes-only replication produces a copy that is neither tamper-evident nor self-expiring.

## 2. Goals / Non-Goals

**Goals**
- Per-pair opt-in flags, chosen at creation, persisted, visible in CLI/API/UI.
- Characteristics applied atomically-enough that a partial failure is retried without re-copying bytes.
- Target provisioning (bucket, lock, versioning, default retention, lifecycle) done as an idempotent preflight at pair creation, failing fast and loudly.
- No regression to existing pairs: all new behaviour off by default.

**Non-goals (deliberate)**
- **Object ACL replication** — dropped. AWS disables ACLs on new buckets (April 2023); Cubbit DS3 stores object grants but never evaluates them. Replicating writes data nothing reads.
- **Bucket-policy / IAM-policy replication** — dropped. Bucket policies are bucket-level, not per-object, and Cubbit DS3 does not support the S3 `PutBucketPolicy` API. IAM policies belong to the principal's account and are not readable via S3. (A separate "target access template" feature may be considered later.)
- **Version-history replication** (all versions, delete markers) — out of scope; `ListObjectVersions`-based diffing is a different architecture.
- **Server-side encryption / KMS / checksums** — Cubbit does not apply or verify them; nothing to replicate.
- **Lifecycle transitions / tiering, `AbortIncompleteMultipartUpload`** — unsupported by Cubbit DS3.
- **Multipart copy for objects > 5 GB** — still unsupported; see §9 for the required explicit error.

**Anti-goals (would mean this failed even if it works)**
- A pair that reports `ok` while the target is unlocked or the objects carry no retention.
- Silently overwriting lifecycle rules on a target the operator curated. (User has stated the target is always new — accepted risk, but see §7.3.)
- Deleting target data because a lock requirement was "forced" without an explicit flag.

## 3. Constraints (verified against Cubbit DS3 docs, 2025)

These drive most of the design; each was checked, not assumed.

| Constraint | Consequence |
|---|---|
| `CopyObject` ignores `x-amz-tagging`; `x-amz-tagging-directive` is "not applied" | Tags must be copied with `GetObjectTagging` → `PutObjectTagging`. Not a directive. |
| Metadata is stored on `CopyObject` **only** with `x-amz-metadata-directive: REPLACE` | Must `HeadObject` the source and pass the full metadata map explicitly. `COPY` is not reliable. |
| `Content-Language` and `Expires` are not stored at all | Unreplicable. Document as a known loss. |
| `x-amz-object-lock-mode` / `-retain-until-date` / `-legal-hold` on `CopyObject` are **enforced** | Retention + legal hold can be applied inline in the copy — no second call needed. |
| `x-amz-copy-source-if-*` conditionals are not evaluated | No server-side preconditions on copy; dedup must remain client-side (see the P0 in §9). |
| Object Lock can **only** be enabled at `CreateBucket`; a bucket without it cannot be converted | "Forced if necessary" can only mean *create a different bucket*. |
| Lock forces versioning on, and versioning can never be suspended afterwards | Every overwrite leaves a noncurrent version; noncurrent expiration is mandatory. |
| `Expiration` supports `Date` (midnight UTC) and `Days`; `Filter` and `Prefix` are **unsupported** | An expiry date expires **every object in the bucket**. Acceptable only because the target is dedicated to one pair. |
| `NoncurrentVersionExpiration` (`NewerNoncurrentVersions`, `NoncurrentDays`) supported | Used to bound growth from lock + versioning. |
| Lifecycle deletion **silently skips** versions still under retention or legal hold | Effective deletion = `max(expire_at, retain_until)`. Must be surfaced, not silently accepted. |
| `PutBucketLifecycleConfiguration` **replaces** the whole config | Accepted (target is new); see §7.3 for the guard. |
| COMPLIANCE retention cannot be shortened or removed by anyone | Irreversible. Requires an explicit confirmation flag. |

## 4. Approaches Considered

### Feature 1 — characteristic replication

| | Approach | Trade-off | Verdict |
|---|---|---|---|
| **A** | Inline pipeline in `copyObject`: `HeadObject` source → `CopyObject` with `MetadataDirective=REPLACE` + metadata + lock fields → `GetObjectTagging`→`PutObjectTagging`; store an attribute fingerprint in the cache | Fewest calls on the happy path; attributes coupled to the copy; needs a retry path that does not re-copy bytes | **Chosen** |
| B | Separate attributes pass after the copy, independent worker stage | Clean retry semantics, one extra listing pass | Rejected: duplicates the diff machinery for no gain once A carries a pending-attributes flag |
| C | Declarative reconciler converging desired state | Handles drift and pre-existing targets | Rejected: far more state and code than the ask |

**Chosen: A**, with the cache carrying an attributes fingerprint so drift and partial failures are detectable.

### Feature 2 — locked, expiring target

| | Approach | Trade-off | Verdict |
|---|---|---|---|
| **A** | Preflight at pair creation: create/validate bucket, force lock, apply retention + lifecycle; fail with a clear message if impossible | Predictable, no data risk, fails fast | **Chosen** |
| B | Best-effort enable, warn and continue | Convenient, but silently yields an unlocked target — defeats WORM | Rejected as default; available as opt-in |
| C | Recreate the target bucket whenever lock is missing | Guarantees the guarantee, destroys data on a typo | Rejected as default; available behind two explicit flags |

**Chosen: A**, with `--on-lock-missing fail|recreate` (default `fail`) and `recreate` requiring both an explicit `--force-recreate` and an empty target bucket.

### Lifecycle conflict policy

| | Approach | Verdict |
|---|---|---|
| Overwrite | Replace any existing lifecycle config | **Chosen** — user confirmed the target is always new |
| Merge | Read, append, write back | Rejected: more failure modes, and unnecessary here |
| Refuse | Fail if any config exists | Rejected: same reason |

Guard retained: the preflight logs the previous config at `warn` before replacing it, so an accident is at least visible in the log.

## 5. Data Model Changes

### `sync_pairs` (SQLite migration, additive)

| Column | Type | Default | Meaning |
|---|---|---|---|
| `replicate_attributes` | INTEGER | 0 | Feature 1 on/off |
| `target_expire_at` | TEXT NULL | NULL | RFC3339 midnight-UTC date for lifecycle `Expiration.Date` |
| `noncurrent_expire_days` | INTEGER NULL | NULL | `NoncurrentVersionExpiration.NoncurrentDays`; requires `newer_noncurrent_versions` |
| `newer_noncurrent_versions` | INTEGER NULL | NULL | `NewerNoncurrentVersions` |
| `require_object_lock` | INTEGER | 0 | Force the target bucket to be lock-enabled |
| `on_lock_missing` | TEXT | `fail` | `fail` \| `recreate` |
| `provisioned_at` | TEXT NULL | NULL | Set when preflight succeeded |
| `provision_error` | TEXT | `` | Last preflight failure, surfaced in UI |

`config.SyncPair` gains the matching fields. Migrations follow the existing pattern in `internal/config/db.go:60-70` (ignore "duplicate column" errors).

### Cache `CachedObject` (BoltDB, JSON — schema-tolerant)

| Field | Type | Meaning |
|---|---|---|
| `attrs_hash` | string | Hex SHA-256 fingerprint of the replicated characteristics (see §6.3) |
| `pending_attrs` | bool | Bytes copied, characteristics not yet applied — retried next cycle without re-copying |

Older entries decode with empty values; empty `attrs_hash` + `pending_attrs=false` is treated as "unknown, do not reconcile" (see §8.2).

## 6. Feature 1 — Design

### 6.1 Per-object pipeline

`internal/sync/worker.go:copyObject` is rewritten as:

1. **`HeadObject` on the source** (throttled) — obtains `ContentType`, `ContentDisposition`, `ContentEncoding`, `CacheControl`, `Metadata`, `StorageClass`, `ObjectLockMode`, `ObjectLockRetainUntilDate`, `ObjectLockLegalHoldStatus`.
2. **`CopyObject` to the target** with:
   - `MetadataDirective = REPLACE`, all metadata fields above passed explicitly.
   - `StorageClass`: pair `TargetStorageClass` if set, else the source's storage class.
   - `ObjectLockMode` / `ObjectLockRetainUntilDate` / `ObjectLockLegalHoldStatus`: **only when the source retention is still in the future**; a past `RetainUntilDate` would be rejected and is semantically meaningless. Legal hold is passed independently of the date.
   - Size guard: if `ContentLength > 5 GiB`, return a typed `ErrObjectTooLarge` with a clear message (see §9).
3. **Tags**: `GetObjectTagging` on the source → `PutObjectTagging` on the target with the returned `VersionId`.
4. Compute `attrs_hash` over the applied state and return it with the action result, so the cache entry is written with `pending_attrs=false`.

If step 3 fails, the object is reported as **succeeded with pending attributes**: cache entry written with `pending_attrs=true`, the copy is not repeated, and the pair's status shows a distinct `attrs_pending` counter rather than a plain failure.

### 6.2 New action type

`differ.go` gains `ActionCopyAttributes`. The merge-join emits it in exactly one case:

> cache entry exists, `ETag`/`LastModified` match the listing, and `pending_attrs == true`

This gives automatic, throttled, per-cycle retry of failed attribute application with no extra HEAD and no extra listing pass. The worker handles it by running only steps 1, 3, 4 (no byte copy).

### 6.3 Fingerprint

`attrs_hash = SHA-256(canonical serialisation of)`:
- tags, sorted by key, `k=v` joined
- metadata keys lowercased, sorted, `k=v` joined
- `content-type`, `content-disposition`, `content-encoding`, `cache-control`
- lock mode, `retain_until` (RFC3339, omitted when in the past), legal-hold status
- effective target storage class

Purpose: detect drift on re-copy and prove (in tests) that a copy carried what we think it did. It is **not** used for a per-cycle sweep by default — see §8.2.

### 6.4 What is explicitly not replicated

`Content-Language`, `Expires`, ACLs, SSE settings, checksums, `LastModified`, version history. Each is either unsupported by Cubbit DS3 or not per-object data. `LastModified` is not writable by S3 at all, which matters because the differ uses it as a change signal — that stays as-is.

## 7. Feature 2 — Design

### 7.1 Preflight (idempotent, at pair creation and re-run on first sync)

`internal/sync/setup.go` is extended into `ProvisionTarget(ctx, srcRegion, tgt *config.Bucket, pair *config.SyncPair) error`, decision table:

| Target bucket | `require_object_lock` | Action |
|---|---|---|
| absent | any | `CreateBucket` with `ObjectLockEnabledForBucket` when required; region from source; then versioning; then `PutObjectLockConfiguration` with default retention when the bucket config has a mode |
| present, lock **enabled** | any | proceed; `warnIfMismatch` as today |
| present, lock **absent** | false | proceed (existing behaviour) |
| present, lock **absent** | true, `on_lock_missing=fail` | **error**: "target bucket exists without object lock and cannot be converted; use --on-lock-missing recreate with an empty bucket" |
| present, lock **absent** | true, `on_lock_missing=recreate` | requires `--force-recreate` **and** `ListObjectsV2(MaxKeys=1)` empty → `DeleteBucket` + create as absent. Non-empty → error. |

Ordering is forced by the API: `CreateBucket(ObjectLockEnabledForBucket=true)` must precede `PutObjectLockConfiguration`, and versioning is enabled implicitly by lock but is still set explicitly when `tgt.Versioning` is true.

COMPLIANCE mode additionally requires an explicit `--accept-compliance` flag; without it, provisioning with `RetentionMode=COMPLIANCE` is rejected with an explanation of irreversibility.

### 7.2 Lifecycle

When `target_expire_at` is set, build and `PutBucketLifecycleConfiguration`:

```
Rule 1 (if expire_at):      ID=s3sync-expire, Status=Enabled,
                            Expiration{Date: <midnight UTC>}
Rule 2 (if noncurrent_days): ID=s3sync-noncurrent, Status=Enabled,
                            NoncurrentVersionExpiration{NoncurrentDays, NewerNoncurrentVersions}
```

Validation before writing:
- `Date` must be **midnight UTC** and **in the future** — both rejected otherwise, with the offending value in the message.
- `Expiration.Date` + `ExpiredObjectDeleteMarker` in the same rule is forbidden by the API; we emit them as separate rules.
- `NoncurrentVersionExpiration` is **recommended** (warned, not enforced) whenever lock is required: lock + versioning means every overwrite leaves a version behind, and nothing else reclaims it.
- If the effective default retention extends past `target_expire_at`, **warn loudly**: locked versions will be skipped until retention expires, so deletion will happen later than the configured date.

### 7.3 Replacement guard

`PutBucketLifecycleConfiguration` replaces the whole config. Before writing, `GetBucketLifecycleConfiguration` is called and any existing rules are logged at `warn` (IDs, actions). Then we overwrite. Accepted per user's confirmation that the target is always new.

### 7.4 The resurrection problem (must be handled)

Once lifecycle expires objects at the target, the source still has them. The cache still has matching entries, so the differ sees "unchanged" and does nothing — but a `reset` or a cache loss would re-copy everything, which then expires again, forever.

Design: the engine refuses to run when `now > target_expire_at`, sets pair status `expired`, and the UI/CLI show it as terminal rather than an error. Recovery is `pair update --target-expire-at <future date>`: moving the date forward (or back into the future) re-arms the pair, and the next cycle re-copies anything lifecycle already deleted because the target `HeadObject` now returns 404. This makes the expiry a real end-of-life for the pair, which is the only coherent reading of "the target expires on date D".

## 8. Detection and Reconciliation

### 8.1 What triggers an attribute write
1. Object copied (new or changed bytes) → attributes applied inline.
2. `pending_attrs` set from a previous partial failure → attribute-only action next cycle.

### 8.2 Drift on unchanged objects
Tags, retention, and ACL changes on the source do **not** bump `ETag` or `LastModified`, so the existing differ cannot see them. Options: sweep every object with a `HeadObject` each cycle (expensive and throttled), or opt in.

Design: pair field `attrs_reconcile_interval` (seconds, default `0` = off). When non-zero, a full `HeadObject` sweep on the source runs once the interval has elapsed since the last sweep, comparing `attrs_hash` against the cache and emitting `ActionCopyAttributes` on mismatch. Documented cost: one throttled HEAD per object per sweep. Default off keeps existing pairs' behaviour and cost unchanged, and the interval is settable at `pair add` and changeable at `pair update` (`--attrs-reconcile-interval`).

## 9. Bug Fixed In Passing (P0)

`internal/sync/worker.go:93-136` HEADs the **target** and treats a 404 as a retryable error, so after 3 attempts `copyObject` returns before ever issuing `CopyObject` — **new objects are never copied** (documented and reproduced in `S3sync-review/CODE_REVIEW.md`, regression from `6d13e9d`). We are rewriting this function, so it is fixed here: `errors.As(err, &types.NotFound{})` on `HeadObject` means "needs copy", not "retry". A regression test asserts `CopyObject` is called exactly once for a missing target object.

Also added: `ErrObjectTooLarge` for source objects > 5 GiB, surfaced as a clear per-object error instead of an opaque S3 failure (multipart copy itself remains out of scope).

## 10. Interfaces

### CLI (`cmd/pair.go`)

```
pair add NAME --source-bucket ID --target-bucket ID \
  [--replicate-attributes] \
  [--target-expire-at YYYY-MM-DD] \
  [--noncurrent-expire-days N] [--newer-noncurrent-versions N] \
  [--require-object-lock] [--on-lock-missing fail|recreate] \
  [--force-recreate] [--accept-compliance] \
  [--attrs-reconcile-interval SECONDS]
```

`pair add` now performs the preflight synchronously and exits non-zero on failure, with the pair **not** created. `pair update` allows changing `replicate_attributes`, `attrs_reconcile_interval`, and `target_expire_at` (any date, later or earlier; an **earlier** date is destructive because lifecycle will delete already-synced objects, so it requires `--confirm-expire-earlier` and is rejected without it); lock/creation fields are immutable after creation, and `pair update` rejects attempts to change them.

### API (`internal/api/handlers.go`)

- `POST /api/sync-pairs` accepts the new fields, runs the preflight, returns `422` with `{"error": "...", "code": "preflight_failed"}` on failure (pair not persisted).
- `GET /api/sync-pairs` response gains `replicate_attributes`, `target_expire_at`, `require_object_lock`, `provisioned_at`, `provision_error`, `attrs_pending` (live counter from the engine).
- `PUT /api/sync-pairs/:id` accepts only the mutable subset, same rules as CLI.

### Web UI

- Setup wizard and pair edit modal gain: "Replicate object characteristics" checkbox; "Expire target on" date input; "Require object lock" checkbox with the `fail|recreate` select; noncurrent-days input.
- Dashboard card gains badges: `lock`, `attrs`, `expires YYYY-MM-DD`, and a distinct pill for `attrs_pending`.
- Destructive confirmations: recreate requires typing the bucket name; COMPLIANCE requires an explicit checkbox; moving `target_expire_at` earlier requires a second confirmation naming the current date and the new one.

## 11. Testing

- **Unit** — lifecycle builder (date normalisation, midnight-UTC rejection, past-date rejection, rule splitting); preflight decision table as a table-driven test over a fake S3 client (all six rows of §7.1); fingerprint canonicalisation (ordering, lowercasing, past-retention omission); differ emitting `ActionCopyAttributes` for `pending_attrs`.
- **Regression** — the P0 test from §9.
- **Integration** (`-tags=integration`, existing env-var harness in `tests/`) — full cycle against MinIO: copy with tags + metadata + retention, verify with `GetObjectTagging`/`GetObjectRetention`; lifecycle round-trip via `GetBucketLifecycleConfiguration`.
- **Migration** — open a DB created by the current schema, assert new columns exist with correct defaults and existing rows decode.

## 12. Left to the Builder

- Exact wiring of the `attrs_pending` counter into `Progress`/`Status()` and the dashboard poll payload.
- Whether `ProvisionTarget` runs inside `Engine.setupOnce` (current) or in `cmd/pair.go` only, given `serve` mode must also provision pairs created via API before any sync.
- Whether `pair update` of `target_expire_at` should re-run preflight immediately or lazily on next sync.
- Throttling accounting for the new `HeadObject` on the **source** (currently only listing and copy are counted) — it must be counted.
- Whether the `expired` pair status blocks `pair sync` explicitly or is merely advisory.
