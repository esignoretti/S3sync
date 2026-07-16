# Webhook Notifications

## Problem

No alerting when sync fails. Users discover failures only by checking the dashboard.

## Solution

Per-pair webhook URL. On sync completion (success or failure), `s3sync` POSTs a JSON payload to the configured URL.

### API

Extend `SyncPair` model:

```
webhook_url        TEXT        -- optional URL to notify
webhook_events     TEXT        -- comma-separated: "error","success","all" (default "error")
```

Display in edit modal as optional text field.

### Payload

```
POST /configured-url
Content-Type: application/json

{
  "event": "sync_completed",
  "pair_id": "uuid",
  "pair_name": "my-sync",
  "status": "error",
  "consecutive_errors": 3,
  "last_error": "3 worker(s) failed",
  "started_at": "2026-07-16T10:00:00Z",
  "completed_at": "2026-07-16T10:05:00Z",
  "source": "src-bucket",
  "target": "tgt-bucket"
}
```

### Delivery

- Non-blocking goroutine per notification
- 10s timeout per webhook
- Failed deliveries logged at WARN level
- No retry (fire-and-forget; history provides audit trail)

### Changed Files

| File | Change |
|------|--------|
| `internal/config/models.go` | Add `WebhookURL`, `WebhookEvents` fields + DB columns |
| `internal/sync/engine.go` | After `RunOnce` completion, fire notification goroutine |
| `internal/api/web/static/app.js` | Add webhook fields to edit modal |
| `internal/api/web/templates/layout.html` | Add webhook inputs to edit modal |
