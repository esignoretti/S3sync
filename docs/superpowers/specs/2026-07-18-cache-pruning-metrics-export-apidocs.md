# Cache Pruning, Prometheus Metrics, Config Export/Import, API Docs

## Cache Pruning

### Problem
BoltDB cache DB grows unbounded. Old cached object entries accumulate forever, wasting disk and slowing cursor iteration.

### Solution
Per-pair configurable pruning. After each sync cycle, check the BoltDB bucket size for the pair. If it exceeds `CacheMaxSizeMB`, delete entries with `SyncedAt` older than `CacheTTLDays`.

#### Config
Extend `SyncPair` model with two new fields:

```
cache_max_size_mb  INTEGER DEFAULT 50   -- 0 = unlimited
cache_ttl_days     INTEGER DEFAULT 30   -- 0 = never prune
```

Both visible in edit modal as optional fields.

**Why 50 MB / 30 days default:** BoltDB performance degrades past ~100 MB for unindexed iteration. 50 MB gives headroom. 30-day TTL matches typical object churn — if an object hasn't been re-synced in 30 days it's likely stable and will be re-cached on next sync anyway.

#### Algorithm (in `internal/cache/store.go`)

```go
func (s *Store) PruneIfNeeded(pairID string, maxSizeMB, ttlDays int) (removed int64, err error)
```

1. Open BoltDB bucket for `pairID`.
2. Get bucket size via `tx.Bucket(bucket).Stats().BranchAlloc + LeafAlloc`.
3. If size < `maxSizeMB * 1MB` OR `ttlDays == 0`, return 0 (skip).
4. Iterate all keys. For each entry, unmarshal `CachedObject`. If `SyncedAt` < `now - ttlDays`, delete key.
5. Return count of removed entries.

Called from `engine.go:afterSync()` after successful diff+copy cycle.

#### Rust/Edge Cases
- `cache_max_size_mb = 0` and `cache_ttl_days = 0`: never prune (explicit opt-out).
- If both are set, check size first — if under threshold, skip TTL scan entirely (zero cost).
- BoltDB `Stats()` is cheap (reads internal counters, no scan).
- Prune does NOT run if sync cycle had errors (avoid cascading work).
- Prune runs in a separate read-write transaction; does not block concurrent sync writes (BoltDB locking is per-transaction, not per-operation).

---

## Prometheus Metrics

### Problem
No operational observability. Users can't monitor sync health, throughput, or trends over time.

### Solution
Standard Prometheus `/metrics` endpoint exposing counters and histograms per sync pair.

#### Implementation

**Library:** `github.com/prometheus/client_golang/prometheus` and `promhttp`.

**Endpoint:** `GET /metrics` registered on the main Gin router (no separate port — simpler deployment, no additional surface area since auth is planned).

**Metrics registered (all with labels `pair_id` and `pair_name`):**

| Metric | Type | Description |
|--------|------|-------------|
| `s3sync_sync_runs_total{status="success"|"fail"}` | Counter | Total sync run completions |
| `s3sync_objects_copied_total` | Counter | Objects copied from source to target |
| `s3sync_objects_deleted_total` | Counter | Objects deleted from target |
| `s3sync_bytes_transferred_total` | Counter | Bytes copied (from source object sizes) |
| `s3sync_sync_duration_seconds` | Histogram | Sync run duration (buckets: 1, 5, 15, 30, 60, 120, 300, 600, 1800) |

**Registration:** `internal/api/metrics.go` — `RegisterSyncPairMetrics(pairID, pairName string)`, `UnregisterSyncPairMetrics(pairID string)`.

**Recording:** `internal/sync/engine.go` — on each `RunOnce` completion, call `RecordMetrics(succeeded bool, copied, deleted, bytes int64, duration time.Duration)`.

**Lifecycle:** Metrics are registry-reuse safe. On pair delete, unregister and let GC reclaim. No static init — metrics created dynamically when pair is loaded.

**Pair rename handling:** On pair update (PUT `/api/sync-pairs/:id`), if name changed, unregister old metrics and re-register with new `pair_name` label. Unchanged name = no-op. This means one scrape interval may show both old and new label values, which is acceptable — `rate()` smooths over the transition.

**Edge cases:**
- Counter values survive engine restarts (already in sync logs). On startup, counters reset to zero (Prometheus handles this via rate() — transient reset is irrelevant).
- No metrics for non-running pairs (metrics registered on engine start).
- Histogram buckets chosen to cover 1s fast syncs up to 30min long syncs.

---

## Config Export/Import

### Problem
No way to backup, migrate, or bulk-manage sync configurations. Manual re-creation of pairs is error-prone.

### Solution
JSON export of all buckets + sync pairs (including credentials). Import with conflict resolution.

#### Export

**Endpoint:** `GET /api/export`

**Response:** `Content-Type: application/json`

```json
{
  "version": 1,
  "exported_at": "2026-07-18T12:00:00Z",
  "buckets": [
    {
      "id": "uuid",
      "name": "my-source",
      "endpoint": "https://s3.cubbit.eu",
      "region": "eu-west-1",
      "access_key": "AKID...",
      "secret_key": "secret..."
    }
  ],
  "sync_pairs": [
    {
      "id": "uuid",
      "name": "my-pair",
      "source_bucket_id": "uuid",
      "target_bucket_id": "uuid",
      "sync_interval": 300,
      "worker_count": 4,
      "max_get_ops_per_minute": 0,
      "delete_propagation": false,
      "target_storage_class": "",
      "enabled": true,
      "dry_run": false,
      "webhook_url": "",
      "webhook_events": "",
      "cache_max_size_mb": 50,
      "cache_ttl_days": 30
    }
  ]
}
```

Credentials included by decrypting from DB. User can redact before sharing file.

#### Import

**Endpoint:** `POST /api/import?on_conflict=skip|replace|new`

- `skip` (default): skip items that match existing ones; return count of skipped.
- `replace`: overwrite existing bucket/pair with imported values.
- `new`: always create new IDs (duplicate configs allowed).

**Matching logic:**
- Buckets matched by `name + endpoint`.
- Sync pairs matched by `name`.

**Validation (all checked before any write):**
1. All JSON fields parse correctly (invalid JSON → 400).
2. Buckets: endpoint is valid URL, region non-empty, access/secret keys non-empty.
3. Sync pairs: source_bucket_id and target_bucket_id reference valid bucket UUIDs (either existing or being imported in the same payload).

**ID Remapping:** Import processes buckets first, building a mapping from old UUID → new UUID (for `replace` the UUID stays; for `new` a fresh UUID is generated). Then sync pairs are processed with their bucket IDs rewritten through this mapping. This ensures references are consistent regardless of conflict mode.

**Response:**

```json
{
  "buckets_created": 2,
  "buckets_skipped": 1,
  "buckets_replaced": 0,
  "pairs_created": 1,
  "pairs_skipped": 0,
  "pairs_replaced": 1,
  "errors": []
}
```

If any validation error, entire import is rejected (atomic). No partial import. Validation runs first (including remapping logic), then apply happens only if all checks pass.

**Implementation:** `internal/api/handlers.go` — `exportConfig`, `importConfig` handlers. Uses `Repository` methods for CRUD. Credentials are decrypted on export, encrypted on import.

---

## API Docs

### Problem
No API documentation. Users must read source code or ask questions to use the API.

### Solution
Hand-written OpenAPI 3.0 spec served at `GET /api/openapi.json` + Swagger UI at `GET /docs`.

#### Files

**`internal/api/web/openapi.json`** — OpenAPI 3.0 spec documenting all 25 endpoints. Includes:
- Paths with parameters
- Request bodies (JSON schemas for create/update)
- Response schemas (envelope `{"data": ...}` or `{"error": "..."}`)
- Auth scheme (Bearer token, cookie)
- Tags for grouping (Buckets, Sync Pairs, System, Auth, Setup)

**`internal/api/web/templates/swagger.html`** — Minimal Swagger UI page:

```html
<!DOCTYPE html>
<html>
<head><title>S3sync API Docs</title>
<link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css">
</head>
<body>
<div id="swagger-ui"></div>
<script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
<script>SwaggerUIBundle({ url: '/api/openapi.json', dom_id: '#swagger-ui' })</script>
</body>
</html>
```

Served at `GET /docs`.

#### Implementation

Both files embedded via `//go:embed` and registered in `router.go`:

```go
r.GET("/api/openapi.json", func(c *gin.Context) {
    c.Data(200, "application/json", openapiJSON)
})
r.GET("/docs", func(c *gin.Context) {
    c.Data(200, "text/html; charset=utf-8", swaggerHTML)
})
```

#### Maintenance
OpenAPI file is hand-written and versioned. Updated whenever routes change.

---

## Files Changed

| File | Change |
|------|--------|
| `internal/config/models.go` | Add `CacheMaxSizeMB`, `CacheTTLDays` to `SyncPair` |
| `internal/config/db.go` | Add migration for new columns |
| `internal/cache/store.go` | Add `PruneIfNeeded` method |
| `internal/sync/engine.go` | Call `PruneIfNeeded` in `afterSync`; call `RecordMetrics` |
| `internal/api/metrics.go` | NEW — Prometheus metric registration + recording |
| `internal/api/handlers.go` | Add `exportConfig`, `importConfig` handlers |
| `internal/api/router.go` | Register `/api/export`, `/api/import`, `/metrics`, `/api/openapi.json`, `/docs` |
| `internal/api/web/openapi.json` | NEW — OpenAPI 3.0 spec |
| `internal/api/web/templates/swagger.html` | NEW — Swagger UI page |
| `go.mod` | Add `prometheus/client_golang` |
| `internal/api/web.go` | Embed `openapi.json`, `swagger.html` |
