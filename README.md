# S3sync

One-way S3 bucket sync. CLI, REST API, and embedded web UI. Configurable parallelism, token-bucket throttling, object lock support.

```
bucketsync config init
bucketsync bucket add prod-source   --endpoint https://s3.eu-west-1.amazonaws.com --region eu-west-1 --bucket-name my-data --access-key AKID --secret-key secret
bucketsync bucket add prod-target   --endpoint https://s3.eu-west-1.amazonaws.com --region eu-west-1 --bucket-name my-data-replica --access-key AKID --secret-key secret --versioning --object-lock --retention-mode GOVERNANCE --retention-days 365
bucketsync pair add prod-sync       --source-bucket <src-id> --target-bucket <tgt-id> --interval 300 --workers 20 --max-ops 600
bucketsync pair sync <pair-id>
```

## Features

- **One-way sync** — source → target, etag-based diff, delete propagation
- **Parallel transfers** — configurable goroutine worker pool per sync pair
- **Throttling** — token bucket limits GET ops per minute on source bucket
- **Target auto-config** — creates bucket, enables versioning, object lock with retention
- **CLI + API + Web UI** — single binary, `serve` mode starts all three
- **Dual DB** — SQLite for config (PgSQL path ready), BoltDB for sync cache
- **Encrypted credentials** — AES-256-GCM with master key env var
- **S3-compatible** — works with AWS, Cubbit DS3, any S3-compatible endpoint

## Install

```bash
# Download or build
go install github.com/esignoretti/S3sync@latest

# Or build from source
git clone https://github.com/esignoretti/S3sync.git
cd S3sync
go build -o bucketsync .
```

## Quick Start

```bash
# 1. Initialize config
bucketsync config init

# 2. Add buckets
bucketsync bucket add source \
  --endpoint https://s3.amazonaws.com \
  --region us-east-1 \
  --bucket-name my-source-bucket \
  --access-key AKIDEXAMPLE \
  --secret-key wJalrXUt

bucketsync bucket add target \
  --endpoint https://s3.amazonaws.com \
  --region us-east-1 \
  --bucket-name my-target-bucket \
  --access-key AKIDEXAMPLE \
  --secret-key wJalrXUt \
  --versioning \
  --object-lock \
  --retention-mode GOVERNANCE \
  --retention-days 365

# 3. Create sync pair
bucketsync pair add my-sync \
  --source-bucket <source-id-from-list> \
  --target-bucket <target-id-from-list> \
  --interval 300 \
  --workers 10 \
  --max-ops 600

# 4. Trigger one-shot sync
bucketsync pair sync <pair-id>

# 5. Or run continuous sync server
bucketsync serve --port 8080
```

## CLI Reference

```
Usage: bucketsync [command]

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

Global Flags:
  --config-dir    Config directory (default: ~/.bucketsync/)
  --log-level     debug|info|warn|error (default: info)
  --log-format    text|json (default: text)
  --log-file      Log file path (optional)
```

## Serve Mode

`bucketsync serve` starts an HTTP server on `:8080` with:

- **REST API** — full CRUD for buckets and sync pairs via `/api/*`
- **Background sync** — runs sync cycle per enabled pair on its configured interval
- **Web UI** — dark-themed dashboard showing pair status, polling live (Cubbit-inspired design)

## Architecture

```
bucketsync (single binary)
├── CLI mode — config management, one-shot sync
├── Serve mode — REST API + sync engine + embedded web UI
├── Config DB — SQLite (PostgreSQL adapter path ready)
└── Cache DB — BoltDB (per-pair sync state)

Sync engine per pair:
  Scheduler (ticker) → Lister (S3 ListObjectsV2) → Cache compare → Diff → Worker pool (COPY/DELETE)
```

## Configuration

**Credentials:** S3 access/secret keys are encrypted at rest with AES-256-GCM. Set `BUCKETSYNC_MASTER_KEY` environment variable for the encryption key.

**Target bucket:** On first sync, the engine checks if the target bucket exists. If not, it creates it with the source region, enables versioning and object lock if configured. Warnings are logged if an existing bucket lacks requested features.

**Throttling:** Each sync pair can set `max_get_ops_per_minute`. A token bucket limits GET/HEAD operations on the source bucket. Set to 0 for unlimited (default).

**Cache:** Object metadata (etag, size, last-modified) is cached in BoltDB. Each sync cycle diffs the fresh listing against the cache to find new, changed, or deleted objects. The cache can be rebuilt from a full listing without affecting config.

## Development

```bash
go test ./...
go vet ./...
go build -o bucketsync .
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
