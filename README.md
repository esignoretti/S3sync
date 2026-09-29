# S3sync

One-way S3-compatible bucket sync. CLI, REST API, and embedded web UI. Configurable parallelism, token-bucket throttling, object lock support.

Optimized for **Cubbit DS3** but works with any S3-compatible endpoint (AWS, MinIO, etc.).

```
s3sync config init
s3sync bucket add prod-source   --endpoint https://s3.cubbit.eu --region eu-west-1 --bucket-name my-data --access-key AKID --secret-key secret
s3sync bucket add prod-target   --endpoint https://s3.cubbit.eu --region eu-west-1 --bucket-name my-data-replica --access-key AKID --secret-key secret --versioning --object-lock --retention-mode GOVERNANCE --retention-days 365
s3sync pair add prod-sync       --source-bucket <src-id> --target-bucket <tgt-id> --interval 300 --workers 20 --max-ops 600
s3sync pair sync <pair-id>
```

## Features

- **One-way sync** — source → target, ETag-based diff, delete propagation
- **Parallel transfers** — configurable goroutine worker pool per sync pair
- **Throttling** — token bucket limits GET ops per minute on source bucket
- **Target auto-config** — creates bucket, enables versioning, object lock with retention
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
                    --storage-class)
    pair list       List sync pairs
    pair get        Get pair details (JSON)
    pair update     Update a pair
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

The server listens on **all interfaces** (`0.0.0.0`) on port `8080` by default — change it with `--port`.

### Authentication

Authentication is controlled entirely by `S3SYNC_API_KEY`:

- **Unset (default):** the server is **open**. Every endpoint is reachable by anyone who can reach the port, including `POST /api/buckets` (which stores S3 credentials) and `GET /api/export`. On a shared or public network, set the key or bind to a trusted interface.
- **Set:** requests need either a session cookie or an `Authorization: Bearer <key>` header. Browser access goes through the login page at `/login`; the session cookie is `HttpOnly` and `SameSite=Lax`. `/api/health` stays reachable without credentials so container healthchecks keep working.

```bash
export S3SYNC_API_KEY="$(openssl rand -hex 32)"
s3sync serve
```

### Web Dashboard

Cards show per-pair:
- Status pill (synced, running, error, idle)
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

All responses wrapped in `{"data": ...}` envelope. Errors return `{"error": "..."}`.

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
| PUT | `/api/sync-pairs/:id` | Partial update (sync_interval, worker_count, max_get_ops_per_minute) |
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
  Each action: throttle.Wait → HEAD target (skip if same ETag) → CopyObject / DeleteObject
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

If cache is missing or corrupted, `copyObject` does a HEAD on the target — if target ETag matches source, the copy is skipped. No redundant transfers in either case.

## Configuration

**Credentials:** S3 access/secret keys encrypted at rest with AES-256-GCM. Set `BUCKETSYNC_MASTER_KEY` env var for encryption.

**Target bucket:** On first sync, engine checks if target exists. If not, creates it with source region, enables versioning and object lock if configured.

**Throttling:** Each sync pair sets `max_get_ops_per_minute`. Token bucket limits GET/HEAD ops on source. 0 = unlimited.

**Defaults:** Endpoint `https://s3.cubbit.eu`, region `eu-west-1` (Cubbit DS3).

**Object size limit:** S3's `CopyObject` handles at most 5 GiB in a single call. Larger objects fail fast with an error naming the limit; multipart copy is not implemented. Such an object is reported as a failure for that run and is retried on the next one, so the pair does not converge until it is removed or multipart copy lands.

**Cache lock:** The BoltDB sync cache takes an exclusive 5-second lock, so `s3sync pair sync` fails while `s3sync serve` is running against the same config directory — the server already owns the cache.

### Environment variables

| Variable | Default | Effect |
| --- | --- | --- |
| `BUCKETSYNC_MASTER_KEY` | unset | Encrypts stored S3 credentials with AES-256-GCM. **Unset means credentials are written to the config database in plaintext**, and the server logs a warning at startup. Use the same key for the lifetime of the database: changing it makes existing credentials undecryptable, and reads now fail loudly instead of returning ciphertext as a credential. Required by `docker-compose.yml`. |
| `S3SYNC_API_KEY` | unset | Enables authentication for the API, the web UI and `/metrics`. **Unset means the server is fully unauthenticated** — see Serve Mode below. |
| `S3_TEST_ENDPOINT`, `S3_TEST_ACCESS_KEY`, `S3_TEST_SECRET_KEY` | unset | Used only by the integration tests. |

## Development

Reproduce exactly what CI runs (`.github/workflows/ci.yml`) with:

```bash
go build ./... && go vet ./... && gofmt -l internal/ cmd/ tests/ main.go && go test -race ./...
```

The race detector is part of the gate, not optional: the sync engine runs a
worker pool, a progress reporter and the diff concurrently, and `-race` is what
catches a regression there. `gofmt -l` prints nothing when the tree is clean.

Integration tests require a compatible S3 endpoint:

```bash
export S3_TEST_ENDPOINT=http://localhost:9000
export S3_TEST_ACCESS_KEY=test
export S3_TEST_SECRET_KEY=test
go test -tags=integration ./tests/ -v
```

## License

MIT
