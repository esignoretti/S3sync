# Streaming Merge-Join Diff + Atomic Progress

## Problem

Current sync engine:
- Loads **all** source objects into `[]ListedObject` via paginated `ListObjectsV2`
- Loads **all** cached objects into `[]CachedObject` from BoltDB
- Builds map, diffs in memory → O(n) memory for both sides
- Progress counters use `sync.Mutex`, bottleneck under high concurrency

With a 1M-object bucket, this is ~200MB+ RAM per sync cycle.

## Solution

Replace the two-phase list-then-diff with a **merge-join streaming diff** that processes one page at a time.

### Algorithm

```
pageN = ListObjectsV2(pageN-1 token)
cacheIter = BoltDB cursor for pairID (key-ordered, read-only tx)

for each page {
  for each obj in page.Contents (sorted by Key) {
    advance cacheIter
    if cacheIter.Key == obj.Key:
      if cacheIter.ETag == obj.ETag && cacheIter.LastModified == obj.LastModified:
        emit skip
      else:
        emit copy(obj)
      delete from "seen" set (logical, just advance cacheIter)
    elif cacheIter.Key < obj.Key:
      // cache has items source doesn't → delete
      if deletePropagation:
        emit delete(cacheIter.Key)
      advance cacheIter, re-check same obj
    else:
      // source has items cache doesn't → new
      emit copy(obj)
  }
}
// Remaining cache items → deletes
for each remaining cacheIter {
  if deletePropagation:
    emit delete(cacheIter.Key)
}
```

### Key Design

1. **BoltDB cursor** — `Bucket.Cursor()` with `Seek("")` → iterate in key order. Same read-only transaction for entire cycle.
2. **S3 listing** — already returns keys sorted (UTF-8 bytes). Page token gives us continuation.
3. **No full materialization** — actions emitted per-page, sent immediately to worker pool (bounded channel).
4. **Worker pool** — reads from a single `chan SyncAction` fed by the streaming diff goroutine. Bounded buffer (e.g. 10k). Backpressure: if pool is full, diff blocks on S3 page fetch.

### Atomic Progress

```go
type Progress struct {
    Total     atomic.Int64 `json:"total"`
    Completed atomic.Int64 `json:"completed"`
    Failed    atomic.Int64 `json:"failed"`
}
```

No mutex for progress. Workers do `Completed.Add(1)`, `Failed.Add(1)` directly.

### Merge-Join BoltDB Cursor

```go
type CacheCursor struct {
    tx  *bbolt.Tx
    b   *bbolt.Bucket
    c   *bbolt.Cursor
    cur struct {
        key []byte
        obj *cache.CachedObject
    }
}

func NewCacheCursor(db *bbolt.DB, pairID string) (*CacheCursor, func(), error) {
    tx, err := db.Begin(false) // read-only
    if err != nil { return nil, nil, err }
    b := tx.Bucket([]byte("cache_" + pairID))
    c := b.Cursor()
    k, v := c.First()
    // close func releases tx
}
```

### Changed Files

| File | Change |
|------|--------|
| `internal/sync/differ.go` | New `StreamingDiff(ctx, listingPage, cacheCursor, deletePropagation)` → emits actions to channel |
| `internal/sync/engine.go` | `RunOnce` opens cache cursor, streams listing, feeds actions to pool |
| `internal/sync/worker.go` | `Run` accepts `<-chan SyncAction`, uses `atomic.Int64` for progress |
| `internal/cache/store.go` | Add `NewCursor(pairID)` returning `*CacheCursor` + close func |
| `internal/sync/lister.go` | `ListObjects` stays paginated, called page-by-page from engine loop |

### Worker Pool Channel Interface

```go
func NewWorkerPool(...) *WorkerPool
func (wp *WorkerPool) Run(ctx context.Context, actions <-chan SyncAction) (int, int, error)
```

Pool reads from channel until closed. Returns count of succeeded/failed.

### Edge Cases

- **Empty source bucket** → cache cursor iterates remaining → deletes if deletePropagation
- **Empty cache** → all source objects are copies
- **Cancel during streaming** → ctx cancels workers + stops page fetches
- **Concurrent cache writes** → not possible (single engine per pair, bolt read tx doesn't block)

### Performance

- Memory: O(page size) = ~1000 objects = ~200KB instead of O(total objects)
- Worker start latency: first page processed immediately, no wait for full listing
- Atomic counters: zero contention

### Migration

No DB migration. Existing BoltDB cache format unchanged. Old `Diff` function removed, replaced by streaming path.
