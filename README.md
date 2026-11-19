# S3sync

**Version 0.3.0**

One-way S3-compatible bucket sync. CLI, REST API, and embedded web UI. Configurable parallelism, token-bucket throttling, object lock support, optional replication of object characteristics, and target expiry dates.

Optimized for **Cubbit DS3** but works with any S3-compatible endpoint (AWS, MinIO, etc.).

```
s3sync config init
s3sync bucket add prod-source   --endpoint https://s3.cubbit.eu --region eu-west-1 --bucket-name my-data --access-key AKID --secret-key secret
s3sync bucket add prod-target   --endpoint https://s3.cubbit.eu --region eu-west-1 --bucket-name my-data-replica --access-key AKID --secret-key secret --versioning --object-lock --retention-mode GOVERNANCE --retention-days 365
s3sync pair add prod-sync       --source-bucket <src-id> --target-bucket <tgt-id> --interval 300 --workers 20 --max-ops 600 --replicate-attributes --target-expire-at 2027-06-01
s3sync pair sync <pair-id>
```

## Features

- **One-way sync** — source → target, ETag-based diff, delete propagation
- **Parallel transfers** — configurable goroutine worker pool per sync pair
- **Throttling** — token bucket limits GET ops per minute on source bucket
- **Target auto-config** — creates bucket, enables versioning, object lock with retention
- **Attribute replication** — optionally copies metadata, tags, storage class, retention and legal hold, not just bytes (opt-in per pair)
- **Target expiry** — a bucket lifecycle rule expires the whole target at a chosen date, optionally with noncurrent-version cleanup
- **Drift sweep** — optional periodic re-check of replicated attributes, so tag or metadata edits that bump neither ETag nor LastModified still converge
- **CLI + API + Web UI** — single binary, `serve` mode starts all three
- **Dual DB** — SQLite for config, BoltDB for sync cache
- **Encrypted credentials** — AES-256-GCM with master key env var
- **S3-compatible** — works with Cubbit DS3, AWS, any S3-compatible endpoint
- **Live dashboard** — progress bars, status pills, Pause/Resume/Reset per pair

## Install

```bash
go install github.com/esignoretti/S3sync@latest

# Or build from source
git clone https://github.com/esignoretti/S3sync.git
cd S3sync
go build -o s3sync .
```

## Quick Start

```bash
# 1. Initialize config
s3sync config init

# 2. Add buckets
s3sync bucket add source \
  --endpoint https://s3.cubbit.eu \
  --region eu-west-1 \
  --bucket-name my-source-bucket \
  --access-key AKIDEXAMPLE \
  --secret-key wJalrXUt

s3sync bucket add target \
  --endpoint https://s3.cubbit.eu \
  --region eu-west-1 \
  --bucket-name my-target-bucket \
  --access-key AKIDEXAMPLE \
  --secret-key wJalrXUt \
  --versioning \
  --object-lock \
  --retention-mode GOVERNANCE \
  --retention-days 365

# 3. Create sync pair
s3sync pair add my-sync \
  --source-bucket <source-id-from-list> \
  --target-bucket <target-id-from-list> \
  --interval 300 \
  --workers 10 \
  --max-ops 600

# 3b. Optionally: replicate object characteristics and expire the target
s3sync pair add my-sync \
  --source-bucket <source-id-from-list> \
  --target-bucket <target-id-from-list> \
  --interval 300 \
  --replicate-attributes \
  --target-expire-at 2027-06-01 \
  --noncurrent-expire-days 30 \
  --newer-noncurrent-versions 3 \
  --require-object-lock \
  --on-lock-missing fail \
  --attrs-reconcile-interval 3600

# 4. Trigger one-shot sync
s3sync pair sync <pair-id>

# 5. Or run continuous sync server (visit http://localhost:8080)
s3sync serve --port 8080
```

## CLI Reference

```
Usage: s3sync [command]

Commands:
  config            Manage configuration
    config init     Initialize config database
    config show     Show config path
    config get      Read a global setting
    config set      Write a global setting

  bucket            Manage bucket configurations
    bucket add      Add a bucket (--endpoint, --region, --bucket-name,
                    --access-key, --secret-key, --versioning, --object-lock,
                    --retention-mode, --retention-days)
    bucket list     List all buckets
    bucket get      Get bucket details (JSON)
    bucket update   Update a bucket
    bucket delete   Delete a bucket

  pair              Manage sync pairs
    pair add        Create a sync pair (--source-bucket, --target-bucket,
                    --interval, --workers, --max-ops, --delete-propagation,
                    --storage-class, --replicate-attributes,
                    --target-expire-at, --noncurrent-expire-days,
                    --newer-noncurrent-versions, --require-object-lock,
                    --on-lock-missing, --force-recreate, --accept-compliance,
                    --attrs-reconcile-interval)
    pair list       List sync pairs
    pair get        Get pair details (JSON)
    pair update     Update a pair (--interval, --workers, --max-ops,
                    --replicate-attributes, --attrs-reconcile-interval,
                    --target-expire-at, --confirm-expire-earlier,
                    --noncurrent-expire-days, --newer-noncurrent-versions)
    pair delete     Delete a pair
    pair sync       Trigger one-shot sync

  serve             Start API server + sync engine + web UI
  status            Show sync status (table)
  setup             Interactive wizard (stdin)

Global Flags:
  --config-dir    Config directory (default: ~/.s3sync/)
  --log-level     debug|info|warn|error (default: info)
  --log-format    text|json (default: text)
  --log-file      Log file path (optional)
```

## Serve Mode

`s3sync serve` starts an HTTP server with:

- **REST API** — full CRUD for buckets and sync pairs via `/api/*`
- **Background sync** — runs sync cycle per enabled pair on its configured interval
- **Web UI** — dark-themed dashboard with live polling (Cubbit-inspired design)

### Web Dashboard

Cards show per-pair:
- Status pill (synced, running, error, idle, expired)
- Badges for object lock, attribute replication, expiry date, and pending attribute repairs
- Source and target with endpoint URLs
- Sync interval, worker count
- Last sync time and error count
- Live progress bar during active sync
- Error detail when sync fails

| Button | Action |
|--------|--------|
| Sync Now | One-shot sync cycle |
| Pause / Resume | Toggle periodic sync loop on/off |
| Edit | Change interval, workers, rate limit, attribute replication, reconcile interval, expiry date, noncurrent rules |
| Reset | Clear cache + status, restart from scratch |
| Delete | Remove pair config |

Editing a running pair replaces its engine, so changes take effect on the next cycle without a restart. Two settings are deliberately not editable: `require_object_lock` and `on_lock_missing`, because they describe how the target bucket was created.

## API

All responses wrapped in `{"data": ...}` envelope. Errors return `{"error": "..."}` and, for preflight rejections, `{"code": "preflight_failed"}`. Pair creation and update additionally return `"warnings"` — a list of `{"code", "message"}` advisories that did not block the write.

### Buckets

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/buckets` | Create bucket config |
| GET | `/api/buckets` | List all |
| GET | `/api/buckets/:id` | Get one |
| PUT | `/api/buckets/:id` | Update |
| DELETE | `/api/buckets/:id` | Delete |

### Sync Pairs

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/sync-pairs` | Create sync pair |
| GET | `/api/sync-pairs` | List all (enriched with live engine status) |
| GET | `/api/sync-pairs/:id` | Get one |
| PUT | `/api/sync-pairs/:id` | Partial update (sync_interval, worker_count, max_get_ops_per_minute, replicate_attributes, attrs_reconcile_interval, target_expire_at, noncurrent_expire_days, newer_noncurrent_versions; requires accept_compliance / confirm_expire_earlier for those cases) |
| DELETE | `/api/sync-pairs/:id` | Delete + stop engine |
| POST | `/api/sync-pairs/:id/sync` | Trigger one-shot sync |
| POST | `/api/sync-pairs/:id/disable` | Toggle enabled (Pause/Resume) |
| POST | `/api/sync-pairs/:id/reset` | Clear cache, reset status, restart loop |
| GET | `/api/sync-pairs/:id/status` | Live status + progress from engine |

### System

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/health` | Health check |
| GET | `/api/version` | Version info |
| GET | `/api/settings` | Read global settings |
| PUT | `/api/settings` | Write a global setting (422 `invalid_setting`) |

### Setup Wizard

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/setup` | Submit setup step (with X-Setup-Session header) |
| GET | `/api/setup?session=...` | Get setup state |

## Architecture

```
s3sync (single binary)
├── CLI mode — config management, one-shot sync, interactive setup
├── Serve mode — REST API + sync engine + embedded web UI
├── Config DB — SQLite (~/.s3sync/config.db)
└── Cache DB — BoltDB (~/.s3sync/cache.db)

Sync engine per pair (goroutine):
  Ticker → RunOnce:
    expiry guard (refuse if past target_expire_at) → provision target (once)
    → optional drift sweep → ListObjectsV2 → Cache compare (ETag) → Diff
    → Worker pool (COPY / DELETE / COPY-ATTRIBUTES) → Cache update (succeeded only)

Worker pool:
  N goroutines pull from buffered action channel
  Each action: throttle.Wait → HEAD source (size guard, attributes)
             → CopyObject (≤ 5 GiB) or multipart UploadPartCopy (> 5 GiB)
             → DeleteObject
  Token-bucket rate limiter per pair

Only successfully transferred objects are cached.
Failed objects retry on next cycle.
Source objects over 5 TiB fail with ErrObjectTooLarge — that is the endpoint's
hard object limit, so no copy path can move them.
```

### Sync Cycle Details

Each cycle:
0. **Guard** — a pair whose target expiry has passed refuses to run at all (`ErrPairExpired`, status `expired`). Provisioning runs once per engine, before anything else, and a failure fails the whole run rather than logging and continuing.
1. **Sweep** (optional) — if `attrs_reconcile_interval` has elapsed, re-read each cached object's attributes and queue a COPY for any whose fingerprint drifted.
2. **List** all source objects via paginated `ListObjectsV2` (1000/page)
3. **Load cache** from BoltDB for this pair (objects from previous successful syncs)
4. **Diff** — compare ETag + LastModified against cache:
   - Not in cache → COPY
   - ETag or timestamp changed → COPY
   - Match but `pending_attrs` set → COPY-ATTRIBUTES (retry the tag write only)
   - Match → skip
   - In cache but not in listing (if delete propagation enabled) → DELETE
5. **Execute** — worker pool copies new/changed, deletes removed
6. **Cache only succeeded** — failed items not cached, retried next cycle. Objects that vanished from the source between listing and copy are neither counted nor cached, so the next diff re-emits them.

### Restart Behavior

On server restart, cache persists. First sync cycle compares all source ETags against cached ETags. Already-synced objects match and skip. Only new or changed objects are copied.

If the cache is missing or corrupted, everything is re-copied. The diff cache is the sole authority on what has already been transferred: `CopyObject` is issued unconditionally for every action the differ emits. A target-side HEAD is deliberately *not* used as a skip test — Cubbit DS3 does not evaluate `x-amz-copy-source-if-*` conditions, so a probe could never prevent a redundant copy, it could only add a request.

## Replicating Object Characteristics

By default a pair copies bytes and the storage class. With `--replicate-attributes` it also carries the source object's characteristics to the target:

| Characteristic | How |
|----------------|-----|
| Metadata (`x-amz-meta-*`) | `MetadataDirective: REPLACE` on the copy |
| Content-Type, Content-Disposition, Content-Encoding, Cache-Control | Same, set explicitly |
| Tags | Separate `GetObjectTagging` → `PutObjectTagging` (the copy call ignores `x-amz-tagging` on Cubbit DS3) |
| Storage class | On the copy; `--storage-class` overrides the source's |
| Retention mode + retain-until | `x-amz-object-lock-*` headers on the copy |
| Legal hold | Same |

`Content-Language` and `Expires` are **not** replicated: Cubbit DS3 does not store them.

**Fingerprint.** Tag, metadata and retention edits bump neither ETag nor LastModified, so the normal diff cannot see them. Each cached object therefore stores a hash of its replicated attributes. If any attribute cannot be applied (a tag read or write fails), the object is marked `pending_attrs` and the next cycle retries just its attributes — visible as an `attrs pending` badge in the dashboard and as `attrs_pending` on the API.

**Drift sweep.** `--attrs-reconcile-interval <seconds>` (default off) periodically re-reads each source object's attributes and re-copies any whose fingerprint no longer matches the cache. This is what catches tag-only edits on objects whose bytes never changed. It costs one HEAD plus one tag read per cached object per sweep, throttled against `max_get_ops_per_minute`.

**Enabling it later does not backfill.** Objects already cached before `--replicate-attributes` was turned on have no fingerprint, and the sweep deliberately treats a missing fingerprint as "unknown" rather than "drifted" — otherwise every pre-existing object would be re-copied on the first sweep. Their characteristics therefore only reach the target when their bytes next change. Run `s3sync pair reset <id>` to clear the cache and force a full re-copy if you need them backfilled; `pair update` prints a warning saying so.

## Object Lock and Target Expiry

**Object lock** can only be enabled when a bucket is created — never afterwards. Because a pair creates its target on first sync, lock is a pair-creation decision:

```bash
s3sync pair add ... --require-object-lock --on-lock-missing fail
```

`--on-lock-missing fail` (default) refuses to start when the target exists without lock. `--on-lock-missing recreate` deletes and recreates the target — it requires `--force-recreate` and an **empty** bucket, and it destroys data. On the web UI the same option requires typing the target bucket name to confirm. Both settings are immutable after the pair exists; `pair update` rejects them.

**Expiry** sets a bucket lifecycle rule that expires every object in the target at a midnight-UTC date:

```bash
s3sync pair add ... --target-expire-at 2027-06-01
```

Cubbit DS3 supports neither `Filter` nor `Prefix` in lifecycle rules, so an expiry date applies to the **whole bucket**. Add `--noncurrent-expire-days` to also purge old versions, and `--newer-noncurrent-versions N` to keep the N most recent.

Moving an expiry date **earlier** needs `--confirm-expire-earlier` on the CLI or a confirmation in the UI. Clearing it with `--target-expire-at ""` leaves the old lifecycle rule in place — the tool warns (`expiry-cleared`) and skips writing an empty rule set, which the endpoint would reject; clear it manually if you mean it.

**After expiry, a pair refuses to run.** Once the date has passed the lifecycle rule has already deleted the target objects, so re-running would re-copy them into a bucket that will expire them again. The pair reports status `expired` and issues no S3 calls. To recover, move the date into the future — via the dashboard, the API, or `s3sync pair update` followed by a restart of `serve` (a CLI update writes the database directly; a running server only picks up changes made through its own API).

### Advisories

Creation and update return non-blocking warnings rather than failing:

| Code | Meaning |
|------|---------|
| `noncurrent-recommended` | Object lock is on but `--noncurrent-expire-days` is unset, so old versions accumulate forever |
| `retention-past-expiry` | The bucket's default retention outlasts `--target-expire-at`; lifecycle deletion skips still-locked versions |
| `expiry-cleared` | The pair no longer has an expiry but the old lifecycle rule is still on the bucket |
| `expire-rule-dropped` | The pair writes only the noncurrent rule, so any `s3sync-expire` rule from an earlier date is removed |
| `attrs-not-backfilled` | Replication was just enabled; already-synced objects keep their old characteristics until their bytes change |

`--accept-compliance` is required to use a `COMPLIANCE` retention mode, which cannot be undone even by the account root.

## Configuration

**Credentials:** S3 access/secret keys encrypted at rest with AES-256-GCM. Set `BUCKETSYNC_MASTER_KEY` env var for encryption.

**Target bucket:** On first sync, engine checks if target exists. If not, creates it with source region, enables versioning and object lock if configured. If it exists, the preflight verifies lock state and rewrites the lifecycle configuration; mismatches are reported as errors rather than silently ignored. Provisioning runs once per engine instance, so a failed provision fails that run instead of being logged and skipped. `pair update` never recreates a target — only `pair add` with `--on-lock-missing recreate --force-recreate` can, and only an empty bucket.

**Credentials in the API:** `POST /api/buckets` binds `config.Bucket`, whose `AccessKey`/`SecretKey` are `json:"-"`, so credentials cannot be supplied on that endpoint — a bucket created there has none and will fail with `static credentials are empty`. Add credentialed buckets with `bucket add` on the CLI or through the setup wizard (`POST /api/setup`), whose `SetupInput` does accept `access_key`/`secret_key`. Credentials are never returned by any endpoint.

**Run timeout:** A global setting, read at the start of every run, so a change applies to the next run of every pair without a restart:

```bash
s3sync config get run_timeout_seconds
s3sync config set run_timeout_seconds 1800   # 30..86400, default 300
```

It bounds one whole sync cycle: the listing, the diff, the copies, and the drift sweep. The sweep gets at most a quarter of it, so a large cache cannot consume the budget the copies need; a sweep that runs out of time resumes from where it stopped on the next run instead of re-walking from the front.

**Large objects:** objects above 5 GiB are copied with sequential `UploadPartCopy` calls, part size chosen to keep the count under the endpoint's 10,000-part ceiling. How large an object can actually be moved depends on the run timeout, because the parts are copied one after another: a 5 TiB object is 10,000 part copies, roughly 8–14 hours at 3–5 s per part, so only about 100–200 GiB completes inside the 300 s default. A copy that does not finish inside the run is aborted — orphaned parts have no lifecycle rule to reap them, so the abort is deliberate — and restarts from part 1 on the next cycle. Raise `run_timeout_seconds` for multi-hundred-GiB objects.

**Throttling:** Each sync pair sets `max_get_ops_per_minute`. Token bucket limits GET/HEAD ops on source. 0 = unlimited.

**Defaults:** Endpoint `https://s3.cubbit.eu`, region `eu-west-1` (Cubbit DS3).

## Development

```bash
go test ./...
go vet ./...
go build -o s3sync .
```

Integration tests require a compatible S3 endpoint:

```bash
export S3_TEST_ENDPOINT=http://localhost:9000
export S3_TEST_ACCESS_KEY=test
export S3_TEST_SECRET_KEY=test
go test -tags=integration ./tests/ -v
```

`tests/ds3_claims_test.go` is the important one: it verifies the eight endpoint behaviours this tool's correctness rests on (tag handling on copy, `MetadataDirective: REPLACE`, unstored `Content-Language`/`Expires`, lifecycle `Date` without `Filter`, lock headers on copy, lock-at-creation-only, ignored `x-amz-copy-source-if-*`). Each of those, if wrong, produces silently incorrect data rather than an error. Run it against any new endpoint before trusting attribute replication:

```bash
export S3_TEST_ENDPOINT=https://s3.cubbit.eu
export S3_TEST_ACCESS_KEY=... S3_TEST_SECRET_KEY=...
go test -tags=integration ./tests/ -run TestDS3 -v
```

Each test creates and deletes its own scratch bucket and skips when the endpoint is absent.

### Scenario tests

`tests/scenario/` is the behavioural suite. Where the unit tests pin individual
functions, these drive the real `Engine`, `WorkerPool`, differ, sweep and
provisioning against a live endpoint, and assert on what actually landed in the
target bucket — bytes, metadata, tags, retention, version count, lifecycle
rules. Every scenario syncs twice and requires the second sync to copy nothing,
which is the assertion that catches the whole class of bugs where work is
reported as done without being done (a poisoned cache, a stale attribute
fingerprint, a dry run that writes).

They run on a remote host because the large fixtures need ~10 GB of free disk
and are built server-side (a 6 GiB object is composed from one 64 MiB seed with
`UploadPartCopy`, never uploaded):

```bash
export S3_TEST_ENDPOINT=https://s3.cubbit.eu
export S3_TEST_ACCESS_KEY=... S3_TEST_SECRET_KEY=...
./tests/run-scenarios.sh full root@your-host
```

Three tiers, selected with `SCENARIO_TIER`:

| Tier | Adds | Time |
| --- | --- | --- |
| `fast` | Every behaviour that needs no large object and no bulk seeding | 239 s measured (700 s on an earlier run) |
| `full` | 1,200-object listing scenario, 120 objects with deletes, the 5 GiB object just BELOW the single-copy limit | 889 s measured |
| `all` | The 5 GiB plus-one boundary, a 6 GiB object, 500 locked objects | hours (estimate) |

`full` does not cover the 5 GiB *plus one* boundary or the 6 GiB multipart
object — those are `all`-only, because composing and copying 6 GiB takes ~20
minutes by itself.

The times are real, not estimates — every S3 request here is a round-trip to a
distributed endpoint (~750 ms to `HEAD` an object, ~1.3 s to copy one) and there
are ~65 scenarios, each building its own buckets, seeding, syncing three times
and tearing down. The same `fast` tier took 239 s on one run and 700 s on
another, so treat these as the order of magnitude. Running the suite is a
deliberate act, not something to do on every save.

`SCENARIO_TIMEOUT` overrides `go test`'s own timeout (45m / 90m / 4h by tier).
It matters: a `go test` timeout panics without running cleanups, so every bucket
live at that moment leaks and bills forever.

Every test, what it asserts and the measured result per scenario is recorded in
[`docs/TESTING.md`](docs/TESTING.md).

`SCENARIO_CLI_BIN` additionally enables the one end-to-end test that drives the
shipped binary rather than the library, so a flag wired into the engine but never
registered on the command is caught. `SCENARIO_KEEP=1` preserves the buckets of a
failing scenario for inspection instead of tearing them down.

The suite is not wired into CI: it needs live credentials and a host with real
disk. Buckets are named `s3sync-sc-<unixnano>` and torn down by a routine that
paginates every version including delete markers, lifts legal holds, aborts
incomplete uploads and waits out any retention still in force — it verifies the
bucket is gone rather than assuming it.

## License

MIT
