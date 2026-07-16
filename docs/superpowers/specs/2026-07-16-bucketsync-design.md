# BucketSync — Design Specification

**Date:** 2026-07-16
**Status:** Draft

---

## 1. Overview

BucketSync is a Go application for keeping S3-compatible bucket pairs in sync (one-way, source → target). It provides a CLI for configuration and one-shot syncs, plus a serve mode with REST API and embedded web UI for continuous sync management.

**Key principles:**
- Single binary, two modes (CLI + server)
- SQLite config (PostgreSQL adapter path ready)
- Separate cache DB for sync state (BoltDB)
- Token-bucket throttling to respect source bucket rate limits
- Configurable parallelism via goroutine worker pool

---

## 2. Architecture

```
bucketsync (single binary)
├── CLI mode (cobra)
│   ├── config management (buckets, sync-pairs CRUD)
│   ├── one-shot sync trigger
│   └── status queries (reads SQLite directly)
├── Serve mode
│   ├── REST API (Gin)
│   ├── Sync Engine (per-pair goroutine with worker pool)
│   ├── Cache Manager
│   └── embedded Web UI (htmx / Go templates + minimal JS)
├── Config DB (SQLite → PostgreSQL adapter in future)
└── Cache DB (BoltDB or separate SQLite)
```

### Sync Engine per pair

```
Scheduler (ticker at sync_interval)
  └── Lister (ListObjectsV2 paginated)
        └── CacheManager (compare listing vs cache → diff)
              └── DiffEngine (produce action list: COPY | DELETE)
                    └── WorkerPool (N goroutines, each with token bucket check)
                          ├── COPY: HEAD target → PUT copy with options
                          └── DELETE: if source object no longer exists
```

---

## 3. Data Model

### Config DB — SQLite (migration-ready for PostgreSQL)

**buckets**

| Column | Type | Notes |
|---|---|---|
| id | TEXT (UUID) | PK |
| name | TEXT UNIQUE | Human label |
| endpoint | TEXT | S3 endpoint URL |
| region | TEXT | |
| access_key | TEXT | AES-256-GCM encrypted |
| secret_key | TEXT | AES-256-GCM encrypted |
| bucket_name | TEXT | |
| object_lock | BOOLEAN | DEFAULT FALSE |
| versioning | BOOLEAN | DEFAULT FALSE |
| retention_mode | TEXT | NULL / GOVERNANCE / COMPLIANCE |
| retention_days | INTEGER | NULL |
| created_at | DATETIME | |
| updated_at | DATETIME | |

**sync_pairs**

| Column | Type | Notes |
|---|---|---|
| id | TEXT (UUID) | PK |
| name | TEXT UNIQUE | |
| source_bucket_id | TEXT | FK → buckets(id) |
| target_bucket_id | TEXT | FK → buckets(id) |
| sync_interval | INTEGER | DEFAULT 300 (seconds) |
| worker_count | INTEGER | DEFAULT 10 |
| max_get_ops_per_minute | INTEGER | DEFAULT 0 (unlimited) |
| enabled | BOOLEAN | DEFAULT TRUE |
| last_sync_at | DATETIME | NULL |
| last_sync_status | TEXT | ok / error / running |
| consecutive_errors | INTEGER | DEFAULT 0 |
| created_at | DATETIME | |
| updated_at | DATETIME | |

### Cache DB — BoltDB (local key-value store, zero dependencies)

**cache_objects** (one bucket per pair_id, or pair_id-partitioned table)

| Column | Type | Notes |
|---|---|---|
| pair_id | TEXT | Partition key |
| key | TEXT | Object key |
| etag | TEXT | |
| size | INTEGER | Bytes |
| last_modified | DATETIME | |
| synced_at | DATETIME | |
| error_count | INTEGER | DEFAULT 0 |
| last_error | TEXT | NULL |

Cache can be rebuilt from full listing without affecting config.

---

## 4. Sync Engine

### Listing
- S3 ListObjectsV2 with pagination (`MaxKeys=1000`)
- Optional: parallel prefix listing for large buckets (configurable prefix split depth)
- Each page counts as 1 GET-equivalent for throttling

### Diff computation
- Compare full listing vs cache on `(key, etag, last_modified)`
- **New:** in listing, not in cache → COPY
- **Changed:** etag or last_modified differs → COPY
- **Missing:** in cache, not in listing → DELETE (if source delete propagation enabled)
- **Unchanged:** same key + etag + last_modified → skip

### Cache update
- Cache written AFTER successful sync of each object (not before)
- Failed objects retain previous cache entry + increment `error_count`
- On startup: if cache stale (>2 sync intervals), full re-list

### Parallelism
- Worker pool size per sync pair (`worker_count`)
- Workers pull actions from a buffered channel
- Listing completes first, then actions distributed
- Each worker checks token bucket before executing GET/HEAD on source

### Throttling
- Token bucket per sync pair (`golang.org/x/time/rate.Limiter`)
- Rate: `max_get_ops_per_minute / 60` tokens/sec
- Burst: `max_get_ops_per_minute`
- 0 = unlimited (no limiter configured)
- Each ListObjectsV2 page = 1 token
- Each HEAD/GET on source = 1 token
- PUT/COPY to target is NOT counted (target is write path)

---

## 5. Target Bucket Auto-Configuration

On sync pair creation / first sync:
1. Check if target bucket exists (HEAD bucket)
2. If not found → create with source bucket's region
3. Configure versioning if `versioning = TRUE`
4. Configure object lock if `object_lock = TRUE` (requires versioning)
5. If target exists but lacks matching characteristics → log WARNING, continue sync

Warnings are non-fatal — sync proceeds with best-effort characteristics.

---

## 6. API (Serve Mode)

| Method | Path | Description |
|---|---|---|
| POST | /api/buckets | Create bucket config |
| GET | /api/buckets | List all |
| GET | /api/buckets/:id | Get one |
| PUT | /api/buckets/:id | Update |
| DELETE | /api/buckets/:id | Delete |
| POST | /api/sync-pairs | Create sync pair |
| GET | /api/sync-pairs | List all |
| GET | /api/sync-pairs/:id | Get one |
| PUT | /api/sync-pairs/:id | Update |
| DELETE | /api/sync-pairs/:id | Delete |
| POST | /api/sync-pairs/:id/sync | Trigger immediate sync |
| GET | /api/sync-pairs/:id/status | Live status |
| GET | /api/sync-pairs/:id/stats | Sync statistics |
| GET | /api/health | Health check |
| GET | /api/version | Version info |

Standard JSON envelope:
```json
{"data": {...}}  // success
{"error": "message", "code": "ERROR_CODE"}  // error
```

---

## 7. CLI

```
Usage:
  bucketsync [command]

Commands:
  config          Manage configuration
    config init          Initialize config DB
    config show          Show current config

  bucket          Manage bucket configurations
    bucket add           Add a bucket
    bucket list          List buckets
    bucket get           Get bucket details
    bucket update        Update a bucket
    bucket delete        Delete a bucket

  pair            Manage sync pairs
    pair add             Create a sync pair
    pair list            List sync pairs
    pair get             Get pair details
    pair update          Update a sync pair
    pair delete          Delete a pair
    pair sync            Trigger one-shot sync for a pair

  serve           Start API server + sync engine + web UI

  status          Show sync status (reads DB directly)

Global Flags:
  --config-dir     Config directory (default: ~/.bucketsync/)
  --log-level      Log level (debug|info|warn|error) (default: info)
  --log-format     Log format (text|json) (default: text)
  --log-file       Log file path (optional)
```

---

## 8. Web UI

**Stack:** Embedded via `embed.FS`. Go `html/template` + minimal JS (no framework, no build step) (no build step). Cubbit-inspired dark theme.

**Pages:**
- **Dashboard** — sync pair cards with live status (idle/running/error), last sync time, object counts
- **Pair Detail** — real-time sync stream, progress, object-level diff, manual trigger
- **Bucket Config** — CRUD form for bucket credentials, target options
- **Settings** — global defaults, master key setup

**UI Design (Cubbit + awesome-design-md patterns):**
- Dark canvas `#040404` → surface-1 `#0f1011` → surface-2 `#141516` ladder
- Blue `#0065FF` primary CTA
- Green `#27B681` for sync success
- Red `#f6465d` for errors
- Sync status pills: Cursor-style palette (idle=grey, running=blue, error=red, synced=green)
- No drop shadows — surface lift + `#23252a` hairline borders
- Mono for technical data (keys, etags, timestamps)
- Cubbit-style blue radial gradient overlay on welcome page
- Floating cube CSS decorative motif

---

## 9. Logging

**Dual-output:**

| Mode | Output | Format | Default Level |
|---|---|---|---|
| CLI | stdout/stderr | Colored text | info |
| serve | file + stdout | JSON structured | info |

**Levels:** debug | info | warn | error | fatal

**Implementation:** Go 1.21+ `log/slog` stdlib — zero external dependencies.

**CLI flags:**
- `--log-level` — minimum level
- `--log-format` — text (CLI default) | json (serve default)
- `--log-file` — optional file path

**Structured JSON format:**
```json
{"ts":"2026-07-16T12:00:00Z","level":"info","component":"SYNC","msg":"object synced","pair":"my-pair","key":"img.jpg","size":4294967,"duration_ms":1200,"action":"COPY"}
```

---

## 10. Error Handling & Resilience

- **Per-object isolation:** one failed COPY does not block remaining objects
- **Dead letter:** failed objects logged to cache with `error_count` + `last_error`, retried next cycle
- **Pair health:** after `max_consecutive_errors` (default: 10), pair auto-disabled
- **Rate limit backoff:** 503/429 → exponential backoff with jitter per pair
- **Graceful shutdown:** OS signal handler drains worker pool, completes in-flight syncs (with timeout)
- **Credential encryption:** AES-256-GCM, master key from `BUCKETSYNC_MASTER_KEY` env var (or auto-generated, stored in config dir)

---

## 11. Testing Strategy

- **Unit:** diff engine, cache manager, token bucket throttler, config validation
- **Integration:** S3-compatible test endpoint (env vars) for source + target, full sync cycle
- **CLI:** golden file tests for command output
- **API:** `net/http/httptest` + in-memory SQLite
- **Cache:** round-trip serialize/deserialize, rebuild from listing

---

## 12. Future Considerations

- **PostgreSQL adapter** for config DB (multi-instance coordination)
- **Event-driven sync** via SQS/SNS notification integration
- **Bi-directional sync** with conflict resolution
- **Webhook notifications** on sync completion / failure
- **Prometheus metrics** endpoint for monitoring
- **Prefix/pattern filtering** to sync only a subset of objects
- **S3-compatible lock support** (DynamoDB-based lease for multi-instance)

---

## 13. Configuration Defaults

- **DELETE propagation:** On by default per pair. When source object is deleted, target object is deleted. Can be disabled per pair.
- **Storage class:** Source storage class preserved on target by default. Overridable per sync pair via `target_storage_class` field.
