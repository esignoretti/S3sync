# S3sync

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
| Edit | Change interval, workers, rate limit |
| Reset | Clear cache + status, restart from scratch |
| Delete | Remove pair config |

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
    ListObjectsV2 → Cache compare (ETag) → Diff → Worker pool (COPY/DELETE) → Cache update (succeeded only)

Worker pool:
  N goroutines pull from buffered action channel
  Each action: throttle.Wait → HEAD source (size guard, attributes) → CopyObject / DeleteObject
  Token-bucket rate limiter per pair

Only successfully transferred objects are cached.
Failed objects retry on next cycle.
```

### Sync Cycle Details

Each cycle:
1. **List** all source objects via paginated `ListObjectsV2` (1000/page)
2. **Load cache** from BoltDB for this pair (objects from previous successful syncs)
3. **Diff** — compare ETag + LastModified against cache:
   - Not in cache → COPY
   - ETag or timestamp changed → COPY
   - Match → skip
   - In cache but not in listing (if delete propagation enabled) → DELETE
4. **Execute** — worker pool copies new/changed, deletes removed
5. **Cache only succeeded** — failed items not cached, retried next cycle

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

**After expiry, a pair refuses to run.** Once the date has passed the lifecycle rule has already deleted the target objects, so re-running would re-copy them into a bucket that will expire them again. The pair reports status `expired` and issues no S3 calls. To recover, move the date into the future.

### Advisories

Creation and update return non-blocking warnings rather than failing:

| Code | Meaning |
|------|---------|
| `noncurrent-recommended` | Object lock is on but `--noncurrent-expire-days` is unset, so old versions accumulate forever |
| `retention-past-expiry` | The bucket's default retention outlasts `--target-expire-at`; lifecycle deletion skips still-locked versions |
| `expiry-cleared` | The pair no longer has an expiry but the old lifecycle rule is still on the bucket |

`--accept-compliance` is required to use a `COMPLIANCE` retention mode, which cannot be undone even by the account root.

## Configuration

**Credentials:** S3 access/secret keys encrypted at rest with AES-256-GCM. Set `BUCKETSYNC_MASTER_KEY` env var for encryption.

**Target bucket:** On first sync, engine checks if target exists. If not, creates it with source region, enables versioning and object lock if configured. If it exists, the preflight verifies lock state and rewrites the lifecycle configuration; mismatches are reported as errors rather than silently ignored.

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

## License

MIT
