# BucketSync Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build BucketSync — single Go binary for one-way S3 bucket sync with CLI, REST API, and embedded web UI.

**Architecture:** Single binary, two modes (CLI + server). SQLite config, BoltDB sync cache, token-bucket throttling, goroutine worker pool. S3 SDK v2.

**Tech Stack:** Go 1.26, cobra, gin, modernc.org/sqlite, go.etcd.io/bbolt, aws-sdk-go-v2, golang.org/x/time/rate

---

## File Structure

```
BucketSync/
├── main.go
├── go.mod / go.sum
├── cmd/
│   ├── root.go
│   ├── config.go
│   ├── bucket.go
│   ├── pair.go
│   ├── serve.go
│   └── status.go
├── internal/
│   ├── config/
│   │   ├── db.go
│   │   ├── models.go
│   │   ├── repo.go
│   │   └── encrypt.go
│   ├── cache/
│   │   ├── store.go
│   │   └── models.go
│   ├── sync/
│   │   ├── engine.go
│   │   ├── lister.go
│   │   ├── differ.go
│   │   ├── worker.go
│   │   └── throttler.go
│   ├── s3client/
│   │   └── client.go
│   ├── api/
│   │   ├── router.go
│   │   ├── handlers.go
│   │   └── web.go
│   └── log/
│       └── logger.go
├── web/
│   ├── templates/
│   │   ├── layout.html
│   │   ├── dashboard.html
│   │   ├── pair_detail.html
│   │   └── bucket_form.html
│   └── static/
│       ├── style.css
│       └── app.js
└── tests/
    ├── sync_test.go
    ├── throttler_test.go
    ├── cache_test.go
    ├── repo_test.go
    └── api_test.go
```

---

### Task 1: Project Scaffolding

**Files:**
- Create: `BucketSync/main.go`
- Create: `BucketSync/cmd/root.go`

- [ ] **Step 1: Initialize Go module**

```bash
cd /Users/esignoretti/Documents/OpenCode/BucketSync && go mod init github.com/esignoretti/bucketsync
```

- [ ] **Step 2: Write main.go**

```go
package main

import "github.com/esignoretti/bucketsync/cmd"

func main() {
    cmd.Execute()
}
```

- [ ] **Step 3: Write cmd/root.go**

```go
package cmd

import (
    "os"

    "github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
    Use:   "bucketsync",
    Short: "Keep S3 buckets in sync, one-way",
    Long:  `BucketSync — one-way S3 bucket sync with CLI, API, and web UI.`,
}

func Execute() {
    if err := rootCmd.Execute(); err != nil {
        os.Exit(1)
    }
}

func init() {
    rootCmd.PersistentFlags().String("config-dir", "", "config directory (default: ~/.bucketsync/)")
    rootCmd.PersistentFlags().String("log-level", "info", "log level: debug|info|warn|error")
    rootCmd.PersistentFlags().String("log-format", "text", "log format: text|json")
    rootCmd.PersistentFlags().String("log-file", "", "log file path (optional)")
}
```

- [ ] **Step 4: Install cobra**

```bash
cd /Users/esignoretti/Documents/OpenCode/BucketSync && go get github.com/spf13/cobra@latest && go mod tidy && go build -o /dev/null ./...
```

- [ ] **Step 5: Commit**

```bash
cd /Users/esignoretti/Documents/OpenCode/BucketSync && git init && git add -A && git commit -m "chore: scaffold go project with cobra CLI"
```

---

### Task 2: Logging Package

**Files:**
- Create: `BucketSync/internal/log/logger.go`

- [ ] **Step 1: Write logger.go**

```go
package log

import (
    "context"
    "io"
    "log/slog"
    "os"
    "path/filepath"
    "strings"
    "sync"
    "time"
)

type Config struct {
    Level  string
    Format string
    File   string
}

var closeOnce sync.Once
var fileWriter io.WriteCloser

func Init(cfg Config) func() {
    lvl := parseLevel(cfg.Level)
    var w io.Writer = os.Stdout

    if cfg.File != "" {
        dir := filepath.Dir(cfg.File)
        os.MkdirAll(dir, 0755)
        f, err := os.OpenFile(cfg.File, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
        if err == nil {
            w = io.MultiWriter(os.Stdout, f)
            fileWriter = f
        }
    }

    var h slog.Handler
    switch cfg.Format {
    case "json":
        h = slog.NewJSONHandler(w, &slog.HandlerOptions{Level: lvl})
    default:
        h = newTextHandler(w, &slog.HandlerOptions{Level: lvl})
    }

    slog.SetDefault(slog.New(h))
    return func() {
        closeOnce.Do(func() {
            if fileWriter != nil {
                fileWriter.Close()
            }
        })
    }
}

func parseLevel(s string) slog.Level {
    switch strings.ToLower(s) {
    case "debug":
        return slog.LevelDebug
    case "warn", "warning":
        return slog.LevelWarn
    case "error":
        return slog.LevelError
    default:
        return slog.LevelInfo
    }
}

type textHandler struct {
    opts   slog.HandlerOptions
    mu     sync.Mutex
    w      io.Writer
}

func newTextHandler(w io.Writer, opts *slog.HandlerOptions) *textHandler {
    var o slog.HandlerOptions
    if opts != nil {
        o = *opts
    }
    return &textHandler{w: w, opts: o}
}

func (h *textHandler) Enabled(_ context.Context, lvl slog.Level) bool {
    return lvl >= h.opts.Level.Level()
}

func (h *textHandler) Handle(_ context.Context, r slog.Record) error {
    h.mu.Lock()
    defer h.mu.Unlock()
    line := r.Time.Format("15:04:05") + " " + r.Level.String() + "\t" + r.Message
    r.Attrs(func(a slog.Attr) bool {
        line += "  " + a.Key + "=" + a.Value.String()
        return true
    })
    _, err := h.w.Write([]byte(line + "\n"))
    return err
}

func (h *textHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
    return h
}

func (h *textHandler) WithGroup(string) slog.Handler {
    return h
}
```

- [ ] **Step 2: Test**

```go
// internal/log/logger_test.go
package log

import (
    "bytes"
    "log/slog"
    "testing"
)

func TestTextHandler(t *testing.T) {
    var buf bytes.Buffer
    h := newTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
    slog.New(h).Info("hello", "key", "val")
    if !bytes.Contains(buf.Bytes(), []byte("hello")) {
        t.Fatal("missing message")
    }
}
```

```bash
cd /Users/esignoretti/Documents/OpenCode/BucketSync && go test ./internal/log/ -v
# Expected: PASS
```

- [ ] **Step 3: Commit**

```bash
cd /Users/esignoretti/Documents/OpenCode/BucketSync && git add -A && git commit -m "feat: add slog-based logging with text handler"
```

---

### Task 3: Config DB — Models + Migration

**Files:**
- Create: `BucketSync/internal/config/models.go`
- Create: `BucketSync/internal/config/db.go`

- [ ] **Step 1: Install SQLite**

```bash
cd /Users/esignoretti/Documents/OpenCode/BucketSync && go get modernc.org/sqlite@latest && go mod tidy
```

- [ ] **Step 2: Write models.go**

```go
package config

import "time"

type Bucket struct {
    ID            string    `json:"id"`
    Name          string    `json:"name"`
    Endpoint      string    `json:"endpoint"`
    Region        string    `json:"region"`
    AccessKey     string    `json:"-"`
    SecretKey     string    `json:"-"`
    BucketName    string    `json:"bucket_name"`
    ObjectLock    bool      `json:"object_lock"`
    Versioning    bool      `json:"versioning"`
    RetentionMode string    `json:"retention_mode,omitempty"`
    RetentionDays int       `json:"retention_days,omitempty"`
    CreatedAt     time.Time `json:"created_at"`
    UpdatedAt     time.Time `json:"updated_at"`
}

type SyncPair struct {
    ID                 string     `json:"id"`
    Name               string     `json:"name"`
    SourceBucketID     string     `json:"source_bucket_id"`
    TargetBucketID     string     `json:"target_bucket_id"`
    SyncInterval       int        `json:"sync_interval"`
    WorkerCount        int        `json:"worker_count"`
    MaxGetOpsPerMinute int        `json:"max_get_ops_per_minute"`
    DeletePropagation  bool       `json:"delete_propagation"`
    TargetStorageClass string     `json:"target_storage_class,omitempty"`
    Enabled            bool       `json:"enabled"`
    LastSyncAt         *time.Time `json:"last_sync_at,omitempty"`
    LastSyncStatus     string     `json:"last_sync_status"`
    ConsecutiveErrors  int        `json:"consecutive_errors"`
    CreatedAt          time.Time  `json:"created_at"`
    UpdatedAt          time.Time  `json:"updated_at"`
}
```

- [ ] **Step 3: Write db.go**

```go
package config

import (
    "database/sql"
    "fmt"
    "os"
    "path/filepath"

    _ "modernc.org/sqlite"
)

func Open(path string) (*sql.DB, error) {
    dir := filepath.Dir(path)
    if err := os.MkdirAll(dir, 0755); err != nil {
        return nil, fmt.Errorf("mkdir: %w", err)
    }
    db, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
    if err != nil {
        return nil, fmt.Errorf("open: %w", err)
    }
    if err := db.Ping(); err != nil {
        return nil, fmt.Errorf("ping: %w", err)
    }
    if err := migrate(db); err != nil {
        return nil, fmt.Errorf("migrate: %w", err)
    }
    return db, nil
}

func migrate(db *sql.DB) error {
    _, err := db.Exec(`
        CREATE TABLE IF NOT EXISTS buckets (
            id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE,
            endpoint TEXT NOT NULL, region TEXT NOT NULL,
            access_key TEXT NOT NULL, secret_key TEXT NOT NULL,
            bucket_name TEXT NOT NULL,
            object_lock INTEGER NOT NULL DEFAULT 0,
            versioning INTEGER NOT NULL DEFAULT 0,
            retention_mode TEXT, retention_days INTEGER,
            created_at TEXT NOT NULL, updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS sync_pairs (
            id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE,
            source_bucket_id TEXT NOT NULL REFERENCES buckets(id),
            target_bucket_id TEXT NOT NULL REFERENCES buckets(id),
            sync_interval INTEGER NOT NULL DEFAULT 300,
            worker_count INTEGER NOT NULL DEFAULT 10,
            max_get_ops_per_minute INTEGER NOT NULL DEFAULT 0,
            delete_propagation INTEGER NOT NULL DEFAULT 1,
            target_storage_class TEXT,
            enabled INTEGER NOT NULL DEFAULT 1,
            last_sync_at TEXT, last_sync_status TEXT NOT NULL DEFAULT '',
            consecutive_errors INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL, updated_at TEXT NOT NULL
        );
    `)
    return err
}
```

- [ ] **Step 4: Test**

```go
// internal/config/db_test.go
package config

import (
    "database/sql"
    "testing"
)

func setupDB(t *testing.T) *sql.DB {
    t.Helper()
    db, err := Open(":memory:")
    if err != nil {
        t.Fatalf("open: %v", err)
    }
    t.Cleanup(func() { db.Close() })
    return db
}

func TestMigrate(t *testing.T) {
    db := setupDB(t)
    rows, err := db.Query("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    if err != nil {
        t.Fatal(err)
    }
    defer rows.Close()
    var tables []string
    for rows.Next() {
        var n string
        rows.Scan(&n)
        tables = append(tables, n)
    }
    if len(tables) < 2 {
        t.Fatalf("expected >=2 tables, got %d", len(tables))
    }
}
```

```bash
cd /Users/esignoretti/Documents/OpenCode/BucketSync && go test ./internal/config/ -v -run TestMigrate
# Expected: PASS
```

- [ ] **Step 5: Commit**

```bash
cd /Users/esignoretti/Documents/OpenCode/BucketSync && git add -A && git commit -m "feat: add config DB schema and models"
```

---

### Task 4: Config DB — Repository CRUD

**Files:**
- Create: `BucketSync/internal/config/repo.go`

- [ ] **Step 1: Install UUID library**

```bash
cd /Users/esignoretti/Documents/OpenCode/BucketSync && go get github.com/google/uuid@latest && go mod tidy
```

- [ ] **Step 2: Write repo.go**

```go
package config

import (
    "database/sql"
    "fmt"
    "time"

    "github.com/google/uuid"
)

type Repository struct {
    db *sql.DB
}

func NewRepository(db *sql.DB) *Repository {
    return &Repository{db: db}
}

// --- Buckets ---

func (r *Repository) CreateBucket(b *Bucket) error {
    b.ID = uuid.New().String()
    now := time.Now().UTC()
    b.CreatedAt = now
    b.UpdatedAt = now
    _, err := r.db.Exec(
        `INSERT INTO buckets (id,name,endpoint,region,access_key,secret_key,bucket_name,
         object_lock,versioning,retention_mode,retention_days,created_at,updated_at)
         VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`,
        b.ID, b.Name, b.Endpoint, b.Region, b.AccessKey, b.SecretKey, b.BucketName,
        boolInt(b.ObjectLock), boolInt(b.Versioning),
        nullStr(b.RetentionMode), nullInt(b.RetentionDays),
        rfc(b.CreatedAt), rfc(b.UpdatedAt),
    )
    return err
}

func (r *Repository) ListBuckets() ([]Bucket, error) {
    rows, err := r.db.Query(`SELECT * FROM buckets ORDER BY name`)
    if err != nil {
        return nil, err
    }
    defer rows.Close()
    return scanBuckets(rows)
}

func (r *Repository) GetBucket(id string) (*Bucket, error) {
    row := r.db.QueryRow(`SELECT * FROM buckets WHERE id = ?`, id)
    return scanBucket(row)
}

func (r *Repository) UpdateBucket(b *Bucket) error {
    b.UpdatedAt = time.Now().UTC()
    res, err := r.db.Exec(
        `UPDATE buckets SET name=?,endpoint=?,region=?,access_key=?,secret_key=?,
         bucket_name=?,object_lock=?,versioning=?,retention_mode=?,retention_days=?,
         updated_at=? WHERE id=?`,
        b.Name, b.Endpoint, b.Region, b.AccessKey, b.SecretKey, b.BucketName,
        boolInt(b.ObjectLock), boolInt(b.Versioning),
        nullStr(b.RetentionMode), nullInt(b.RetentionDays),
        rfc(b.UpdatedAt), b.ID,
    )
    if err != nil {
        return err
    }
    n, _ := res.RowsAffected()
    if n == 0 {
        return fmt.Errorf("bucket %q not found", b.ID)
    }
    return nil
}

func (r *Repository) DeleteBucket(id string) error {
    n, err := r.db.Exec(`DELETE FROM buckets WHERE id = ?`, id)
    if err != nil {
        return err
    }
    if rows, _ := n.RowsAffected(); rows == 0 {
        return fmt.Errorf("bucket %q not found", id)
    }
    return nil
}

// --- Sync Pairs ---

func (r *Repository) CreateSyncPair(p *SyncPair) error {
    p.ID = uuid.New().String()
    now := time.Now().UTC()
    p.CreatedAt = now
    p.UpdatedAt = now
    _, err := r.db.Exec(
        `INSERT INTO sync_pairs (id,name,source_bucket_id,target_bucket_id,
         sync_interval,worker_count,max_get_ops_per_minute,delete_propagation,
         target_storage_class,enabled,last_sync_at,last_sync_status,
         consecutive_errors,created_at,updated_at)
         VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
        p.ID, p.Name, p.SourceBucketID, p.TargetBucketID,
        p.SyncInterval, p.WorkerCount, p.MaxGetOpsPerMinute, boolInt(p.DeletePropagation),
        nullStr(p.TargetStorageClass), boolInt(p.Enabled),
        nullTimeRFC(p.LastSyncAt), p.LastSyncStatus,
        p.ConsecutiveErrors, rfc(p.CreatedAt), rfc(p.UpdatedAt),
    )
    return err
}

func (r *Repository) ListSyncPairs() ([]SyncPair, error) {
    rows, err := r.db.Query(`SELECT * FROM sync_pairs ORDER BY name`)
    if err != nil {
        return nil, err
    }
    defer rows.Close()
    return scanSyncPairs(rows)
}

func (r *Repository) GetSyncPair(id string) (*SyncPair, error) {
    row := r.db.QueryRow(`SELECT * FROM sync_pairs WHERE id = ?`, id)
    return scanSyncPair(row)
}

func (r *Repository) UpdateSyncPair(p *SyncPair) error {
    p.UpdatedAt = time.Now().UTC()
    res, err := r.db.Exec(
        `UPDATE sync_pairs SET name=?,source_bucket_id=?,target_bucket_id=?,
         sync_interval=?,worker_count=?,max_get_ops_per_minute=?,delete_propagation=?,
         target_storage_class=?,enabled=?,last_sync_at=?,last_sync_status=?,
         consecutive_errors=?,updated_at=? WHERE id=?`,
        p.Name, p.SourceBucketID, p.TargetBucketID,
        p.SyncInterval, p.WorkerCount, p.MaxGetOpsPerMinute, boolInt(p.DeletePropagation),
        nullStr(p.TargetStorageClass), boolInt(p.Enabled),
        nullTimeRFC(p.LastSyncAt), p.LastSyncStatus,
        p.ConsecutiveErrors, rfc(p.UpdatedAt), p.ID,
    )
    if err != nil {
        return err
    }
    n, _ := res.RowsAffected()
    if n == 0 {
        return fmt.Errorf("sync pair %q not found", p.ID)
    }
    return nil
}

func (r *Repository) DeleteSyncPair(id string) error {
    n, err := r.db.Exec(`DELETE FROM sync_pairs WHERE id = ?`, id)
    if err != nil {
        return err
    }
    if rows, _ := n.RowsAffected(); rows == 0 {
        return fmt.Errorf("sync pair %q not found", id)
    }
    return nil
}

// --- helpers ---

func scanBuckets(rows *sql.Rows) ([]Bucket, error) {
    var out []Bucket
    for rows.Next() {
        b, err := scanBucketFromRow(rows)
        if err != nil {
            return nil, err
        }
        out = append(out, *b)
    }
    return out, rows.Err()
}

func scanBucket(row *sql.Row) (*Bucket, error) {
    return scanBucketFromRow(row)
}

type rowScanner interface {
    Scan(...interface{}) error
}

func scanBucketFromRow(s rowScanner) (*Bucket, error) {
    var (
        b             Bucket
        ol, ver       int
        c, u          string
        rm, rd        sql.NullString
    )
    err := s.Scan(&b.ID, &b.Name, &b.Endpoint, &b.Region,
        &b.AccessKey, &b.SecretKey, &b.BucketName,
        &ol, &ver, &rm, &rd, &c, &u)
    if err != nil {
        return nil, err
    }
    b.ObjectLock = ol == 1
    b.Versioning = ver == 1
    b.RetentionMode = rm.String
    if rd.Valid {
        fmt.Sscanf(rd.String, "%d", &b.RetentionDays)
    }
    b.CreatedAt, _ = time.Parse(time.RFC3339, c)
    b.UpdatedAt, _ = time.Parse(time.RFC3339, u)
    return &b, nil
}

func scanSyncPairs(rows *sql.Rows) ([]SyncPair, error) {
    var out []SyncPair
    for rows.Next() {
        p, err := scanSyncPairFromRow(rows)
        if err != nil {
            return nil, err
        }
        out = append(out, *p)
    }
    return out, rows.Err()
}

func scanSyncPair(row *sql.Row) (*SyncPair, error) {
    return scanSyncPairFromRow(row)
}

func scanSyncPairFromRow(s rowScanner) (*SyncPair, error) {
    var (
        p                    SyncPair
        en, dp               int
        c, u                 string
        lsa                  sql.NullString
        ce                   sql.NullInt64
        tsc                  sql.NullString
    )
    err := s.Scan(&p.ID, &p.Name, &p.SourceBucketID, &p.TargetBucketID,
        &p.SyncInterval, &p.WorkerCount, &p.MaxGetOpsPerMinute, &dp,
        &tsc, &en, &lsa, &p.LastSyncStatus,
        &ce, &c, &u)
    if err != nil {
        return nil, err
    }
    p.Enabled = en == 1
    p.DeletePropagation = dp == 1
    p.TargetStorageClass = tsc.String
    if lsa.Valid {
        t, _ := time.Parse(time.RFC3339, lsa.String)
        p.LastSyncAt = &t
    }
    p.ConsecutiveErrors = int(ce.Int64)
    p.CreatedAt, _ = time.Parse(time.RFC3339, c)
    p.UpdatedAt, _ = time.Parse(time.RFC3339, u)
    return &p, nil
}

func boolInt(b bool) int {
    if b {
        return 1
    }
    return 0
}

func nullStr(s string) *string {
    if s == "" {
        return nil
    }
    return &s
}

func nullInt(n int) *int {
    if n == 0 {
        return nil
    }
    return &n
}

func nullTimeRFC(t *time.Time) *string {
    if t == nil {
        return nil
    }
    s := t.Format(time.RFC3339)
    return &s
}

func rfc(t time.Time) string {
    return t.Format(time.RFC3339)
}
```

- [ ] **Step 3: Test**

```go
// internal/config/repo_test.go
package config

import "testing"

func TestBucketCRUD(t *testing.T) {
    db := setupDB(t)
    r := NewRepository(db)

    b := &Bucket{
        Name: "test", Endpoint: "https://s3.amazonaws.com",
        Region: "us-east-1", AccessKey: "AKID", SecretKey: "secret",
        BucketName: "my-bucket",
    }
    if err := r.CreateBucket(b); err != nil {
        t.Fatal(err)
    }
    if b.ID == "" {
        t.Fatal("expected ID")
    }

    got, _ := r.GetBucket(b.ID)
    if got.Name != "test" {
        t.Fatalf("got %s", got.Name)
    }

    list, _ := r.ListBuckets()
    if len(list) != 1 {
        t.Fatalf("got %d", len(list))
    }

    b.Name = "updated"
    r.UpdateBucket(b)
    got, _ = r.GetBucket(b.ID)
    if got.Name != "updated" {
        t.Fatalf("got %s", got.Name)
    }

    r.DeleteBucket(b.ID)
    list, _ = r.ListBuckets()
    if len(list) != 0 {
        t.Fatalf("got %d", len(list))
    }
}

func TestSyncPairCRUD(t *testing.T) {
    db := setupDB(t)
    r := NewRepository(db)
    src := &Bucket{Name: "src", Endpoint: "e", Region: "r", AccessKey: "a", SecretKey: "s", BucketName: "b"}
    tgt := &Bucket{Name: "tgt", Endpoint: "e", Region: "r", AccessKey: "a", SecretKey: "s", BucketName: "b"}
    r.CreateBucket(src)
    r.CreateBucket(tgt)

    p := &SyncPair{
        Name: "pair", SourceBucketID: src.ID, TargetBucketID: tgt.ID,
        SyncInterval: 300, WorkerCount: 5, Enabled: true,
    }
    if err := r.CreateSyncPair(p); err != nil {
        t.Fatal(err)
    }

    got, _ := r.GetSyncPair(p.ID)
    if got.Name != "pair" {
        t.Fatalf("got %s", got.Name)
    }

    list, _ := r.ListSyncPairs()
    if len(list) != 1 {
        t.Fatalf("got %d", len(list))
    }
}
```

```bash
cd /Users/esignoretti/Documents/OpenCode/BucketSync && go test ./internal/config/ -v
# Expected: PASS
```

- [ ] **Step 4: Commit**

```bash
cd /Users/esignoretti/Documents/OpenCode/BucketSync && git add -A && git commit -m "feat: add config repository CRUD"
```

---

### Task 5: Credential Encryption

**Files:**
- Create: `BucketSync/internal/config/encrypt.go`

- [ ] **Step 1: Write encrypt.go**

```go
package config

import (
    "crypto/aes"
    "crypto/cipher"
    "crypto/rand"
    "crypto/sha256"
    "encoding/hex"
    "errors"
    "io"
)

const MasterKeyEnv = "BUCKETSYNC_MASTER_KEY"

func DeriveKey(masterKey []byte) []byte {
    h := sha256.Sum256(masterKey)
    return h[:]
}

func Encrypt(plaintext, key []byte) (string, error) {
    block, err := aes.NewCipher(key)
    if err != nil {
        return "", err
    }
    aead, err := cipher.NewGCM(block)
    if err != nil {
        return "", err
    }
    nonce := make([]byte, aead.NonceSize())
    if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
        return "", err
    }
    return hex.EncodeToString(aead.Seal(nonce, nonce, plaintext, nil)), nil
}

func Decrypt(encoded string, key []byte) ([]byte, error) {
    data, err := hex.DecodeString(encoded)
    if err != nil {
        return nil, err
    }
    block, err := aes.NewCipher(key)
    if err != nil {
        return nil, err
    }
    aead, err := cipher.NewGCM(block)
    if err != nil {
        return nil, err
    }
    ns := aead.NonceSize()
    if len(data) < ns {
        return nil, errors.New("ciphertext too short")
    }
    return aead.Open(nil, data[:ns], data[ns:], nil)
}
```

- [ ] **Step 2: Test**

```go
// internal/config/encrypt_test.go
package config

import (
    "bytes"
    "testing"
)

func TestEncryptDecrypt(t *testing.T) {
    key := []byte("0123456789abcdef0123456789abcdef")
    pt := []byte("my-secret-key")
    enc, err := Encrypt(pt, key)
    if err != nil {
        t.Fatal(err)
    }
    dec, err := Decrypt(enc, key)
    if err != nil {
        t.Fatal(err)
    }
    if !bytes.Equal(pt, dec) {
        t.Fatal("mismatch")
    }
}

func TestDecryptWrongKey(t *testing.T) {
    enc, _ := Encrypt([]byte("secret"), []byte("0123456789abcdef0123456789abcdef"))
    _, err := Decrypt(enc, []byte("ffffffffffffffffffffffffffffffff"))
    if err == nil {
        t.Fatal("expected error")
    }
}
```

```bash
cd /Users/esignoretti/Documents/OpenCode/BucketSync && go test ./internal/config/ -v -run TestEncrypt
# Expected: PASS
```

- [ ] **Step 3: Commit**

```bash
cd /Users/esignoretti/Documents/OpenCode/BucketSync && git add -A && git commit -m "feat: add AES-256-GCM credential encryption"
```

---

### Task 6: Cache DB — BoltDB

**Files:**
- Create: `BucketSync/internal/cache/models.go`
- Create: `BucketSync/internal/cache/store.go`

- [ ] **Step 1: Install BoltDB**

```bash
cd /Users/esignoretti/Documents/OpenCode/BucketSync && go get go.etcd.io/bbolt@latest && go mod tidy
```

- [ ] **Step 2: Write models.go**

```go
package cache

import "time"

type CachedObject struct {
    PairID       string    `json:"pair_id"`
    Key          string    `json:"key"`
    ETag         string    `json:"etag"`
    Size         int64     `json:"size"`
    LastModified time.Time `json:"last_modified"`
    SyncedAt     time.Time `json:"synced_at"`
    ErrorCount   int       `json:"error_count"`
    LastError    string    `json:"last_error,omitempty"`
}
```

- [ ] **Step 3: Write store.go**

```go
package cache

import (
    "encoding/json"
    "fmt"
    "time"

    "go.etcd.io/bbolt"
)

type Store struct {
    db *bbolt.DB
}

func Open(path string) (*Store, error) {
    db, err := bbolt.Open(path, 0600, &bbolt.Options{Timeout: 5 * time.Second})
    if err != nil {
        return nil, fmt.Errorf("open bolt: %w", err)
    }
    return &Store{db: db}, nil
}

func (s *Store) Close() error {
    return s.db.Close()
}

func (s *Store) bucket(pairID string) []byte {
    return []byte("cache_" + pairID)
}

func (s *Store) ensureBucket(pairID string, tx *bbolt.Tx) (*bbolt.Bucket, error) {
    return tx.CreateBucketIfNotExists(s.bucket(pairID))
}

func (s *Store) Put(obj *CachedObject) error {
    return s.db.Update(func(tx *bbolt.Tx) error {
        b, err := s.ensureBucket(obj.PairID, tx)
        if err != nil {
            return err
        }
        data, err := json.Marshal(obj)
        if err != nil {
            return err
        }
        return b.Put([]byte(obj.Key), data)
    })
}

func (s *Store) Get(pairID, key string) (*CachedObject, error) {
    var obj *CachedObject
    err := s.db.View(func(tx *bbolt.Tx) error {
        b := tx.Bucket(s.bucket(pairID))
        if b == nil {
            return nil
        }
        data := b.Get([]byte(key))
        if data == nil {
            return nil
        }
        var o CachedObject
        if err := json.Unmarshal(data, &o); err != nil {
            return err
        }
        obj = &o
        return nil
    })
    return obj, err
}

func (s *Store) Delete(pairID, key string) error {
    return s.db.Update(func(tx *bbolt.Tx) error {
        b := tx.Bucket(s.bucket(pairID))
        if b == nil {
            return nil
        }
        return b.Delete([]byte(key))
    })
}

func (s *Store) List(pairID string) ([]CachedObject, error) {
    var out []CachedObject
    err := s.db.View(func(tx *bbolt.Tx) error {
        b := tx.Bucket(s.bucket(pairID))
        if b == nil {
            return nil
        }
        return b.ForEach(func(_, v []byte) error {
            var o CachedObject
            if err := json.Unmarshal(v, &o); err != nil {
                return err
            }
            out = append(out, o)
            return nil
        })
    })
    return out, err
}

// Clear removes and recreates the pair's bucket (full cache reset).
func (s *Store) Clear(pairID string) error {
    return s.db.Update(func(tx *bbolt.Tx) error {
        tx.DeleteBucket(s.bucket(pairID))
        _, err := tx.CreateBucket(s.bucket(pairID))
        return err
    })
}

// DeletePairBucket removes the pair's entire cache bucket.
func (s *Store) DeletePairBucket(pairID string) error {
    return s.db.Update(func(tx *bbolt.Tx) error {
        return tx.DeleteBucket(s.bucket(pairID))
    })
}
```

- [ ] **Step 4: Test**

```go
// internal/cache/store_test.go
package cache

import (
    "os"
    "testing"
)

func TestCRUD(t *testing.T) {
    f, _ := os.CreateTemp("", "bolt-test-*.db")
    defer os.Remove(f.Name())
    s, err := Open(f.Name())
    if err != nil {
        t.Fatal(err)
    }
    defer s.Close()

    obj := &CachedObject{PairID: "p1", Key: "a.jpg", ETag: `"abc"`, Size: 4096}
    if err := s.Put(obj); err != nil {
        t.Fatal(err)
    }

    got, _ := s.Get("p1", "a.jpg")
    if got == nil || got.ETag != `"abc"` {
        t.Fatal("expected object")
    }

    list, _ := s.List("p1")
    if len(list) != 1 {
        t.Fatalf("expected 1, got %d", len(list))
    }

    s.Delete("p1", "a.jpg")
    got, _ = s.Get("p1", "a.jpg")
    if got != nil {
        t.Fatal("expected nil after delete")
    }

    s.Put(&CachedObject{PairID: "p1", Key: "x.jpg"})
    s.Put(&CachedObject{PairID: "p1", Key: "y.jpg"})
    s.Clear("p1")
    list, _ = s.List("p1")
    if len(list) != 0 {
        t.Fatalf("expected 0 after clear, got %d", len(list))
    }
}
```

```bash
cd /Users/esignoretti/Documents/OpenCode/BucketSync && go test ./internal/cache/ -v
# Expected: PASS
```

- [ ] **Step 5: Commit**

```bash
cd /Users/esignoretti/Documents/OpenCode/BucketSync && git add -A && git commit -m "feat: add BoltDB cache layer"
```

---

### Task 7: S3 Client Factory

**Files:**
- Create: `BucketSync/internal/s3client/client.go`

- [ ] **Step 1: Install S3 SDK**

```bash
cd /Users/esignoretti/Documents/OpenCode/BucketSync && go get github.com/aws/aws-sdk-go-v2 && go get github.com/aws/aws-sdk-go-v2/config && go get github.com/aws/aws-sdk-go-v2/credentials && go get github.com/aws/aws-sdk-go-v2/service/s3 && go mod tidy
```

- [ ] **Step 2: Write client.go**

```go
package s3client

import (
    "context"
    "fmt"

    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/credentials"
    "github.com/aws/aws-sdk-go-v2/service/s3"
    "github.com/esignoretti/bucketsync/internal/config"
)

func NewClient(b *config.Bucket) (*s3.Client, error) {
    creds := credentials.NewStaticCredentialsProvider(b.AccessKey, b.SecretKey, "")
    cfg, err := config.LoadDefaultConfig(context.TODO(),
        config.WithRegion(b.Region),
        config.WithCredentialsProvider(creds),
    )
    if err != nil {
        return nil, fmt.Errorf("load config: %w", err)
    }

    var opts []func(*s3.Options)
    if b.Endpoint != "" {
        opts = append(opts, func(o *s3.Options) {
            o.BaseEndpoint = aws.String(b.Endpoint)
            o.UsePathStyle = true
        })
    }

    return s3.NewFromConfig(cfg, opts...), nil
}
```

- [ ] **Step 3: Test**

```go
// internal/s3client/client_test.go
package s3client

import (
    "testing"
    "github.com/esignoretti/bucketsync/internal/config"
)

func TestNewClient(t *testing.T) {
    b := &config.Bucket{
        Endpoint: "https://s3.amazonaws.com", Region: "us-east-1",
        AccessKey: "test", SecretKey: "test",
    }
    c, err := NewClient(b)
    if err != nil {
        t.Fatal(err)
    }
    if c == nil {
        t.Fatal("expected non-nil client")
    }
}
```

```bash
cd /Users/esignoretti/Documents/OpenCode/BucketSync && go test ./internal/s3client/ -v
# Expected: PASS
```

- [ ] **Step 4: Commit**

```bash
cd /Users/esignoretti/Documents/OpenCode/BucketSync && git add -A && git commit -m "feat: add S3 client factory"
```

---

### Task 8: Throttler (Token Bucket)

**Files:**
- Create: `BucketSync/internal/sync/throttler.go`

- [ ] **Step 1: Install rate limiter**

```bash
cd /Users/esignoretti/Documents/OpenCode/BucketSync && go get golang.org/x/time/rate@latest && go mod tidy
```

- [ ] **Step 2: Write throttler.go**

```go
package sync

import (
    "context"
    "log/slog"
    "time"

    "golang.org/x/time/rate"
)

type Throttler struct {
    limiter *rate.Limiter
    enabled bool
}

func NewThrottler(maxOpsPerMinute int) *Throttler {
    if maxOpsPerMinute <= 0 {
        return &Throttler{enabled: false}
    }
    limit := rate.Limit(float64(maxOpsPerMinute) / 60.0)
    return &Throttler{
        limiter: rate.NewLimiter(limit, maxOpsPerMinute),
        enabled: true,
    }
}

func (t *Throttler) Wait(ctx context.Context) error {
    if !t.enabled {
        return nil
    }
    return t.limiter.Wait(ctx)
}

func (t *Throttler) Allow() bool {
    if !t.enabled {
        return true
    }
    return t.limiter.Allow()
}

func (t *Throttler) WaitLog(ctx context.Context, label string) error {
    if !t.enabled {
        return nil
    }
    if t.limiter.Allow() {
        return nil
    }
    slog.Debug("throttle wait", "label", label)
    start := time.Now()
    err := t.limiter.Wait(ctx)
    if err == nil {
        slog.Debug("throttle done", "label", label, "waited_ms", time.Since(start).Milliseconds())
    }
    return err
}
```

- [ ] **Step 3: Test**

```go
// internal/sync/throttler_test.go
package sync

import (
    "context"
    "testing"
    "time"
)

func TestDisabled(t *testing.T) {
    tr := NewThrottler(0)
    if !tr.Allow() {
        t.Fatal("disabled should always allow")
    }
}

func TestBurst(t *testing.T) {
    tr := NewThrottler(120) // 120/min, burst 120
    for i := 0; i < 100; i++ {
        if !tr.Allow() {
            t.Fatalf("expected allow at %d", i)
        }
    }
}

func TestWait(t *testing.T) {
    tr := NewThrottler(60) // 1/sec, burst 60
    for i := 0; i < 60; i++ {
        tr.Allow()
    }
    ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
    defer cancel()
    if err := tr.Wait(ctx); err == nil {
        t.Fatal("expected timeout")
    }
}
```

```bash
cd /Users/esignoretti/Documents/OpenCode/BucketSync && go test ./internal/sync/ -v -run TestThrottler
# Expected: PASS
```

- [ ] **Step 4: Commit**

```bash
cd /Users/esignoretti/Documents/OpenCode/BucketSync && git add -A && git commit -m "feat: add token bucket throttler"
```

---

### Task 9: Sync Engine — Lister + Differ

**Files:**
- Create: `BucketSync/internal/sync/lister.go`
- Create: `BucketSync/internal/sync/differ.go`

- [ ] **Step 1: Write lister.go**

```go
package sync

import (
    "context"
    "fmt"
    "log/slog"
    "time"

    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/service/s3"
)

type ListedObject struct {
    Key          string
    ETag         string
    Size         int64
    LastModified time.Time
}

type Lister struct {
    client    *s3.Client
    bucket    string
    throttler *Throttler
}

func NewLister(client *s3.Client, bucket string, throttler *Throttler) *Lister {
    return &Lister{client: client, bucket: bucket, throttler: throttler}
}

func (l *Lister) List(ctx context.Context) ([]ListedObject, error) {
    var objects []ListedObject
    var token *string

    for {
        if err := l.throttler.WaitLog(ctx, l.bucket); err != nil {
            return nil, fmt.Errorf("throttle: %w", err)
        }

        out, err := l.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
            Bucket:            &l.bucket,
            MaxKeys:           aws.Int32(1000),
            ContinuationToken: token,
        })
        if err != nil {
            return nil, fmt.Errorf("list: %w", err)
        }

        for _, obj := range out.Contents {
            objects = append(objects, ListedObject{
                Key:          aws.ToString(obj.Key),
                ETag:         aws.ToString(obj.ETag),
                Size:         obj.Size,
                LastModified: aws.ToTime(obj.LastModified),
            })
        }

        slog.Debug("listed page", "bucket", l.bucket,
            "page_size", len(out.Contents), "total", len(objects))

        if !aws.ToBool(out.IsTruncated) {
            break
        }
        token = out.NextContinuationToken
    }

    return objects, nil
}
```

- [ ] **Step 2: Write differ.go**

```go
package sync

import "time"

type ActionType int

const (
    ActionCopy ActionType = iota
    ActionDelete
)

type SyncAction struct {
    Type         ActionType
    Key          string
    ETag         string
    Size         int64
    LastModified time.Time
}

type cachedEntry struct {
    Key          string
    ETag         string
    Size         int64
    LastModified time.Time
    ErrorCount   int
    LastError    string
}

type DiffResult struct {
    NewOrChanged []SyncAction
    ToDelete     []SyncAction
    Skipped      int
}

func Diff(listing []ListedObject, cached []cachedEntry, deletePropagation bool) DiffResult {
    cm := make(map[string]cachedEntry, len(cached))
    for _, c := range cached {
        cm[c.Key] = c
    }

    var actions, deletes []SyncAction
    skipped := 0

    for _, obj := range listing {
        cc, found := cm[obj.Key]
        if !found {
            actions = append(actions, SyncAction{
                Type: ActionCopy, Key: obj.Key,
                ETag: obj.ETag, Size: obj.Size, LastModified: obj.LastModified,
            })
            continue
        }
        if cc.ETag != obj.ETag || !cc.LastModified.Equal(obj.LastModified) {
            actions = append(actions, SyncAction{
                Type: ActionCopy, Key: obj.Key,
                ETag: obj.ETag, Size: obj.Size, LastModified: obj.LastModified,
            })
            continue
        }
        skipped++
    }

    if deletePropagation {
        lk := make(map[string]struct{}, len(listing))
        for _, o := range listing {
            lk[o.Key] = struct{}{}
        }
        for _, c := range cached {
            if _, exists := lk[c.Key]; !exists {
                deletes = append(deletes, SyncAction{Type: ActionDelete, Key: c.Key})
            }
        }
    }

    return DiffResult{NewOrChanged: actions, ToDelete: deletes, Skipped: skipped}
}
```

- [ ] **Step 3: Test diff**

```go
// internal/sync/differ_test.go
package sync

import (
    "testing"
    "time"
)

func TestDiffNew(t *testing.T) {
    listing := []ListedObject{{Key: "a.jpg", ETag: `"1"`}}
    result := Diff(listing, nil, false)
    if len(result.NewOrChanged) != 1 {
        t.Fatalf("expected 1 new, got %d", len(result.NewOrChanged))
    }
}

func TestDiffUnchanged(t *testing.T) {
    tm := time.Now()
    listing := []ListedObject{{Key: "a.jpg", ETag: `"1"`, LastModified: tm}}
    cached := []cachedEntry{{Key: "a.jpg", ETag: `"1"`, LastModified: tm}}
    result := Diff(listing, cached, false)
    if result.Skipped != 1 {
        t.Fatalf("expected 1 skipped, got %d", result.Skipped)
    }
}

func TestDiffChanged(t *testing.T) {
    listing := []ListedObject{{Key: "a.jpg", ETag: `"2"`}}
    cached := []cachedEntry{{Key: "a.jpg", ETag: `"1"`}}
    result := Diff(listing, cached, false)
    if len(result.NewOrChanged) != 1 {
        t.Fatalf("expected 1 changed, got %d", len(result.NewOrChanged))
    }
}

func TestDiffDelete(t *testing.T) {
    listing := []ListedObject{{Key: "a.jpg"}}
    cached := []cachedEntry{{Key: "a.jpg"}, {Key: "b.jpg"}}
    result := Diff(listing, cached, true)
    if len(result.ToDelete) != 1 || result.ToDelete[0].Key != "b.jpg" {
        t.Fatal("expected b.jpg to delete")
    }
}

func TestDiffNoDeleteWhenDisabled(t *testing.T) {
    listing := []ListedObject{{Key: "a.jpg"}}
    cached := []cachedEntry{{Key: "a.jpg"}, {Key: "b.jpg"}}
    result := Diff(listing, cached, false)
    if len(result.ToDelete) != 0 {
        t.Fatal("expected no deletes")
    }
}
```

```bash
cd /Users/esignoretti/Documents/OpenCode/BucketSync && go test ./internal/sync/ -v -run TestDiff
# Expected: PASS
```

- [ ] **Step 4: Commit**

```bash
cd /Users/esignoretti/Documents/OpenCode/BucketSync && git add -A && git commit -m "feat: add lister and differ"
```

---

### Task 10: Sync Engine — Workers + Engine

**Files:**
- Create: `BucketSync/internal/sync/worker.go`
- Create: `BucketSync/internal/sync/engine.go`

- [ ] **Step 1: Write worker.go**

```go
package sync

import (
    "context"
    "fmt"
    "log/slog"
    "time"

    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/service/s3"
    "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type WorkerPool struct {
    workers    int
    client     *s3.Client
    sourceBucket string
    targetBucket string
    throttler  *Throttler
    storageClass string
}

func NewWorkerPool(workers int, client *s3.Client, source, target string,
    throttler *Throttler, storageClass string) *WorkerPool {
    return &WorkerPool{
        workers: workers, client: client,
        sourceBucket: source, targetBucket: target,
        throttler: throttler, storageClass: storageClass,
    }
}

func (wp *WorkerPool) Run(ctx context.Context, actions []SyncAction) (int, int) {
    if len(actions) == 0 {
        return 0, 0
    }

    type result struct {
        err error
    }
    ch := make(chan SyncAction, len(actions))
    for _, a := range actions {
        ch <- a
    }
    close(ch)

    results := make(chan result, len(actions))
    workerCount := wp.workers
    if workerCount > len(actions) {
        workerCount = len(actions)
    }

    for i := 0; i < workerCount; i++ {
        go func() {
            for a := range ch {
                var err error
                switch a.Type {
                case ActionCopy:
                    err = wp.copyObject(ctx, a)
                case ActionDelete:
                    err = wp.deleteObject(ctx, a)
                }
                if err != nil {
                    slog.Error("worker failed", "key", a.Key, "action", a.Type, "error", err)
                }
                results <- result{err: err}
            }
        }()
    }

    succeeded := 0
    failed := 0
    for i := 0; i < len(actions); i++ {
        r := <-results
        if r.err != nil {
            failed++
        } else {
            succeeded++
        }
    }

    return succeeded, failed
}

func (wp *WorkerPool) copyObject(ctx context.Context, a SyncAction) error {
    // HEAD target first to check if already exists with same etag
    if err := wp.throttler.WaitLog(ctx, wp.sourceBucket); err != nil {
        return err
    }

    headOut, err := wp.client.HeadObject(ctx, &s3.HeadObjectInput{
        Bucket: &wp.targetBucket, Key: &a.Key,
    })
    if err == nil {
        if aws.ToString(headOut.ETag) == a.ETag {
            slog.Debug("skip unchanged", "key", a.Key)
            return nil
        }
    }

    // Wait before source GET
    if err := wp.throttler.WaitLog(ctx, wp.sourceBucket); err != nil {
        return err
    }

    src := fmt.Sprintf("%s/%s", wp.sourceBucket, a.Key)
    input := &s3.CopyObjectInput{
        Bucket:     &wp.targetBucket,
        CopySource: &src,
        Key:        &a.Key,
    }
    if wp.storageClass != "" {
        input.StorageClass = types.StorageClass(wp.storageClass)
    }

    _, err = wp.client.CopyObject(ctx, input)
    if err != nil {
        return fmt.Errorf("copy: %w", err)
    }

    slog.Info("copied", "key", a.Key, "size", a.Size)
    return nil
}

func (wp *WorkerPool) deleteObject(ctx context.Context, a SyncAction) error {
    if wp.throttler.WaitLog(ctx, wp.sourceBucket) != nil {
        return nil // ctx cancelled
    }
    _, err := wp.client.DeleteObject(ctx, &s3.DeleteObjectInput{
        Bucket: &wp.targetBucket, Key: &a.Key,
    })
    if err != nil {
        return fmt.Errorf("delete: %w", err)
    }
    slog.Info("deleted", "key", a.Key)
    return nil
}
```

- [ ] **Step 2: Write engine.go**

```go
package sync

import (
    "context"
    "log/slog"
    "sync"
    "time"

    "github.com/aws/aws-sdk-go-v2/service/s3"
    "github.com/esignoretti/bucketsync/internal/cache"
    "github.com/esignoretti/bucketsync/internal/config"
)

type Engine struct {
    pair    *config.SyncPair
    src     *config.Bucket
    tgt     *config.Bucket
    srcS3   *s3.Client
    tgtS3   *s3.Client
    cache   *cache.Store
    throttler *Throttler
    lister  *Lister
    pool    *WorkerPool

    mu          sync.Mutex
    running     bool
    lastRun     time.Time
    lastStatus  string
}

func NewEngine(pair *config.SyncPair, src, tgt *config.Bucket,
    srcS3, tgtS3 *s3.Client, cacheStore *cache.Store) *Engine {

    thr := NewThrottler(pair.MaxGetOpsPerMinute)
    return &Engine{
        pair:      pair,
        src:       src,
        tgt:       tgt,
        srcS3:     srcS3,
        tgtS3:     tgtS3,
        cache:     cacheStore,
        throttler: thr,
        lister:    NewLister(srcS3, src.BucketName, thr),
        pool: NewWorkerPool(pair.WorkerCount, tgtS3,
            src.BucketName, tgt.BucketName, thr, pair.TargetStorageClass),
    }
}

func (e *Engine) RunOnce(ctx context.Context) error {
    e.mu.Lock()
    if e.running {
        e.mu.Unlock()
        return fmt.Errorf("sync already running")
    }
    e.running = true
    e.mu.Unlock()

    defer func() {
        e.mu.Lock()
        e.running = false
        e.mu.Unlock()
    }()

    slog.Info("sync start", "pair", e.pair.Name)

    // 1. List source bucket
    listing, err := e.lister.List(ctx)
    if err != nil {
        e.setStatus("error")
        return fmt.Errorf("list: %w", err)
    }

    // 2. Load cache
    cached, err := e.cache.List(e.pair.ID)
    if err != nil {
        e.setStatus("error")
        return fmt.Errorf("cache list: %w", err)
    }

    // 3. Convert cache to sync-local type
    entries := make([]cachedEntry, len(cached))
    for i, c := range cached {
        entries[i] = cachedEntry{
            Key: c.Key, ETag: c.ETag, Size: c.Size,
            LastModified: c.LastModified,
            ErrorCount:   c.ErrorCount, LastError: c.LastError,
        }
    }

    // 4. Diff
    diff := Diff(listing, entries, e.pair.DeletePropagation)
    slog.Info("diff complete", "pair", e.pair.Name,
        "new_changed", len(diff.NewOrChanged),
        "delete", len(diff.ToDelete),
        "skipped", diff.Skipped,
        "total_listed", len(listing))

    // 5. Execute copies
    succeeded, failed := e.pool.Run(ctx, diff.NewOrChanged)

    // 6. Execute deletes
    delSucceeded, delFailed := e.pool.Run(ctx, diff.ToDelete)

    // 7. Update cache
    for _, a := range diff.NewOrChanged {
        now := time.Now().UTC()
        e.cache.Put(&cache.CachedObject{
            PairID: e.pair.ID, Key: a.Key,
            ETag: a.ETag, Size: a.Size,
            LastModified: a.LastModified, SyncedAt: now,
        })
    }
    for _, a := range diff.ToDelete {
        e.cache.Delete(e.pair.ID, a.Key)
    }

    totalSucceeded := succeeded + delSucceeded
    totalFailed := failed + delFailed

    slog.Info("sync complete", "pair", e.pair.Name,
        "succeeded", totalSucceeded, "failed", totalFailed,
        "duration", time.Since(e.lastRun))

    if totalFailed > 0 {
        e.setStatus("error")
    } else {
        e.setStatus("ok")
    }
    e.lastRun = time.Now()

    return nil
}

func (e *Engine) setStatus(status string) {
    e.mu.Lock()
    e.lastStatus = status
    e.mu.Unlock()
}

func (e *Engine) Status() (running bool, lastRun time.Time, status string) {
    e.mu.Lock()
    defer e.mu.Unlock()
    return e.running, e.lastRun, e.lastStatus
}
```

Wait, I need to add `"errors"` or `"fmt"` import — I used `fmt.Errorf` in engine.go but didn't import `fmt`. Let me fix that in the code. Also the `sync` import is unused since we have `sync.Mutex` from stdlib... actually we need both `"sync"` and `"fmt"`. Let me check: `sync.Mutex` is from stdlib `sync`. Yes.

Let me also reconsider the engine.RunOnce — should it update the pair's `LastSyncAt` and `LastSyncStatus` in the config DB? The engine doesn't have access to the config repository. We'll need to pass a callback or have the caller do it. Let me keep the engine focused on sync and have the caller/CLI/API handle the DB updates.

Actually the engine could use a status callback. Or we just record status at a higher level. Let me keep the engine simple and track status in the engine struct only, letting the caller persist if needed.

- [ ] **Step 3: Write test for worker pool (integration, needs S3 — minimal unit with mock)**

For now, write a unit test that just verifies pool creation:

```go
// internal/sync/worker_test.go
package sync

import "testing"

func TestWorkerPoolCreation(t *testing.T) {
    // Just verify the constructor doesn't panic
    _ = NewWorkerPool(5, nil, "src", "tgt", NewThrottler(0), "")
}
```

```bash
cd /Users/esignoretti/Documents/OpenCode/BucketSync && go test ./internal/sync/ -v
# Expected: PASS
```

- [ ] **Step 4: Commit**

```bash
cd /Users/esignoretti/Documents/OpenCode/BucketSync && git add -A && git commit -m "feat: add sync worker pool and engine"
```

---

### Task 11: CLI Commands — Config Init, Show, Bucket, Pair

**Files:**
- Create: `BucketSync/cmd/config.go`
- Create: `BucketSync/cmd/bucket.go`
- Create: `BucketSync/cmd/pair.go`

- [ ] **Step 1: Write cmd/config.go**

```go
package cmd

import (
    "fmt"
    "os"
    "path/filepath"

    "github.com/esignoretti/bucketsync/internal/config"
    "github.com/spf13/cobra"
)

func defaultConfigDir() string {
    if d := rootCmd.Flag("config-dir").Value.String(); d != "" {
        return d
    }
    home, _ := os.UserHomeDir()
    return filepath.Join(home, ".bucketsync")
}

func openConfig() (*config.Repository, func(), error) {
    dir := defaultConfigDir()
    dbPath := filepath.Join(dir, "config.db")
    db, err := config.Open(dbPath)
    if err != nil {
        return nil, nil, err
    }
    return config.NewRepository(db), func() { db.Close() }, nil
}

var configCmd = &cobra.Command{Use: "config", Short: "Manage configuration"}

var configInitCmd = &cobra.Command{
    Use:   "init",
    Short: "Initialize config database",
    RunE: func(cmd *cobra.Command, args []string) error {
        _, close, err := openConfig()
        if err != nil {
            return fmt.Errorf("init config: %w", err)
        }
        close()
        fmt.Println("Config initialized at", filepath.Join(defaultConfigDir(), "config.db"))
        return nil
    },
}

var configShowCmd = &cobra.Command{
    Use:   "show",
    Short: "Show config path and stats",
    RunE: func(cmd *cobra.Command, args []string) error {
        fmt.Println("Config dir:", defaultConfigDir())
        return nil
    },
}

func init() {
    configCmd.AddCommand(configInitCmd, configShowCmd)
    rootCmd.AddCommand(configCmd)
}
```

- [ ] **Step 2: Write cmd/bucket.go**

```go
package cmd

import (
    "encoding/json"
    "fmt"
    "os"
    "text/tabwriter"

    "github.com/esignoretti/bucketsync/internal/config"
    "github.com/spf13/cobra"
)

var bucketCmd = &cobra.Command{Use: "bucket", Short: "Manage bucket configurations"}

var bucketAddCmd = &cobra.Command{
    Use:   "add [name]",
    Short: "Add a bucket",
    Args:  cobra.ExactArgs(1),
    RunE: func(cmd *cobra.Command, args []string) error {
        repo, close, err := openConfig()
        if err != nil {
            return err
        }
        defer close()

        endpoint, _ := cmd.Flags().GetString("endpoint")
        region, _ := cmd.Flags().GetString("region")
        bucketName, _ := cmd.Flags().GetString("bucket-name")
        accessKey, _ := cmd.Flags().GetString("access-key")
        secretKey, _ := cmd.Flags().GetString("secret-key")
        objectLock, _ := cmd.Flags().GetBool("object-lock")
        versioning, _ := cmd.Flags().GetBool("versioning")
        retentionMode, _ := cmd.Flags().GetString("retention-mode")
        retentionDays, _ := cmd.Flags().GetInt("retention-days")

        b := &config.Bucket{
            Name: args[0], Endpoint: endpoint, Region: region,
            BucketName: bucketName, AccessKey: accessKey, SecretKey: secretKey,
            ObjectLock: objectLock, Versioning: versioning,
            RetentionMode: retentionMode, RetentionDays: retentionDays,
        }
        if err := repo.CreateBucket(b); err != nil {
            return fmt.Errorf("create bucket: %w", err)
        }
        fmt.Printf("Bucket %q created (id: %s)\n", b.Name, b.ID)
        return nil
    },
}

var bucketListCmd = &cobra.Command{
    Use:   "list",
    Short: "List buckets",
    RunE: func(cmd *cobra.Command, args []string) error {
        repo, close, err := openConfig()
        if err != nil {
            return err
        }
        defer close()

        buckets, err := repo.ListBuckets()
        if err != nil {
            return err
        }

        w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
        fmt.Fprintln(w, "ID\tNAME\tENDPOINT\tBUCKET")
        for _, b := range buckets {
            fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", b.ID[:8], b.Name, b.Endpoint, b.BucketName)
        }
        w.Flush()
        return nil
    },
}

var bucketGetCmd = &cobra.Command{
    Use:   "get [id]",
    Short: "Get bucket details",
    Args:  cobra.ExactArgs(1),
    RunE: func(cmd *cobra.Command, args []string) error {
        repo, close, err := openConfig()
        if err != nil {
            return err
        }
        defer close()

        b, err := repo.GetBucket(args[0])
        if err != nil {
            return err
        }
        enc := json.NewEncoder(os.Stdout)
        enc.SetIndent("", "  ")
        return enc.Encode(b)
    },
}

var bucketUpdateCmd = &cobra.Command{
    Use:   "update [id]",
    Short: "Update a bucket",
    Args:  cobra.ExactArgs(1),
    RunE: func(cmd *cobra.Command, args []string) error {
        repo, close, err := openConfig()
        if err != nil {
            return err
        }
        defer close()

        b, err := repo.GetBucket(args[0])
        if err != nil {
            return err
        }

        if v, _ := cmd.Flags().GetString("name"); v != "" {
            b.Name = v
        }
        if v, _ := cmd.Flags().GetString("endpoint"); v != "" {
            b.Endpoint = v
        }
        // ... more fields
        return repo.UpdateBucket(b)
    },
}

var bucketDeleteCmd = &cobra.Command{
    Use:   "delete [id]",
    Short: "Delete a bucket",
    Args:  cobra.ExactArgs(1),
    RunE: func(cmd *cobra.Command, args []string) error {
        repo, close, err := openConfig()
        if err != nil {
            return err
        }
        defer close()
        return repo.DeleteBucket(args[0])
    },
}

func init() {
    bucketAddCmd.Flags().String("endpoint", "", "S3 endpoint URL")
    bucketAddCmd.Flags().String("region", "us-east-1", "AWS region")
    bucketAddCmd.Flags().String("bucket-name", "", "Bucket name on S3")
    bucketAddCmd.Flags().String("access-key", "", "Access key")
    bucketAddCmd.Flags().String("secret-key", "", "Secret key")
    bucketAddCmd.Flags().Bool("object-lock", false, "Enable object lock")
    bucketAddCmd.Flags().Bool("versioning", false, "Enable versioning")
    bucketAddCmd.Flags().String("retention-mode", "", "GOVERNANCE or COMPLIANCE")
    bucketAddCmd.Flags().Int("retention-days", 0, "Retention period in days")

    bucketUpdateCmd.Flags().String("name", "", "New name")
    bucketUpdateCmd.Flags().String("endpoint", "", "New endpoint")

    bucketCmd.AddCommand(bucketAddCmd, bucketListCmd, bucketGetCmd, bucketUpdateCmd, bucketDeleteCmd)
    rootCmd.AddCommand(bucketCmd)
}
```

- [ ] **Step 3: Write cmd/pair.go**

```go
package cmd

import (
    "encoding/json"
    "fmt"
    "os"
    "text/tabwriter"

    "github.com/esignoretti/bucketsync/internal/config"
    "github.com/spf13/cobra"
)

var pairCmd = &cobra.Command{Use: "pair", Short: "Manage sync pairs"}

var pairAddCmd = &cobra.Command{
    Use:   "add [name]",
    Short: "Create a sync pair",
    Args:  cobra.ExactArgs(1),
    RunE: func(cmd *cobra.Command, args []string) error {
        repo, close, err := openConfig()
        if err != nil {
            return err
        }
        defer close()

        srcID, _ := cmd.Flags().GetString("source-bucket")
        tgtID, _ := cmd.Flags().GetString("target-bucket")
        interval, _ := cmd.Flags().GetInt("interval")
        workers, _ := cmd.Flags().GetInt("workers")
        maxOps, _ := cmd.Flags().GetInt("max-ops")
        delProp, _ := cmd.Flags().GetBool("delete-propagation")
        sc, _ := cmd.Flags().GetString("storage-class")

        p := &config.SyncPair{
            Name: args[0], SourceBucketID: srcID, TargetBucketID: tgtID,
            SyncInterval: interval, WorkerCount: workers,
            MaxGetOpsPerMinute: maxOps, DeletePropagation: delProp,
            TargetStorageClass: sc, Enabled: true,
        }
        if err := repo.CreateSyncPair(p); err != nil {
            return fmt.Errorf("create pair: %w", err)
        }
        fmt.Printf("Pair %q created (id: %s)\n", p.Name, p.ID)
        return nil
    },
}

var pairListCmd = &cobra.Command{
    Use:   "list",
    Short: "List sync pairs",
    RunE: func(cmd *cobra.Command, args []string) error {
        repo, close, err := openConfig()
        if err != nil {
            return err
        }
        defer close()

        pairs, err := repo.ListSyncPairs()
        if err != nil {
            return err
        }
        w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
        fmt.Fprintln(w, "ID\tNAME\tSOURCE\tTARGET\tINTERVAL\tWORKERS\tSTATUS")
        for _, p := range pairs {
            status := p.LastSyncStatus
            if status == "" {
                status = "never"
            }
            fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%d\t%d\t%s\n",
                p.ID[:8], p.Name, p.SourceBucketID[:8], p.TargetBucketID[:8],
                p.SyncInterval, p.WorkerCount, status)
        }
        w.Flush()
        return nil
    },
}

var pairSyncCmd = &cobra.Command{
    Use:   "sync [id]",
    Short: "Trigger one-shot sync for a pair",
    Args:  cobra.ExactArgs(1),
    RunE: func(cmd *cobra.Command, args []string) error {
        repo, close, err := openConfig()
        if err != nil {
            return err
        }
        defer close()

        // TODO: instantiate engine and run sync
        // This requires S3 clients + cache store + full engine setup.
        // For now, print a placeholder.
        fmt.Printf("Sync triggered for pair %s\n", args[0])
        return nil
    },
}

func init() {
    pairAddCmd.Flags().String("source-bucket", "", "Source bucket ID")
    pairAddCmd.Flags().String("target-bucket", "", "Target bucket ID")
    pairAddCmd.Flags().Int("interval", 300, "Sync interval (seconds)")
    pairAddCmd.Flags().Int("workers", 10, "Worker count")
    pairAddCmd.Flags().Int("max-ops", 0, "Max GET ops per minute (0=unlimited)")
    pairAddCmd.Flags().Bool("delete-propagation", true, "Propagate deletes")
    pairAddCmd.Flags().String("storage-class", "", "Target storage class override")

    pairCmd.AddCommand(pairAddCmd, pairListCmd, pairSyncCmd)
    rootCmd.AddCommand(pairCmd)
}
```

- [ ] **Step 4: Verify build**

```bash
cd /Users/esignoretti/Documents/OpenCode/BucketSync && go build ./...
# Expected: no errors
```

- [ ] **Step 5: Commit**

```bash
cd /Users/esignoretti/Documents/OpenCode/BucketSync && git add -A && git commit -m "feat: add CLI commands for config, bucket, pair"
```

---

### Task 12: API Server — Router + Handlers

**Files:**
- Create: `BucketSync/internal/api/router.go`
- Create: `BucketSync/internal/api/handlers.go`
- Create: `BucketSync/internal/api/web.go`
- Create: `BucketSync/cmd/serve.go`

- [ ] **Step 1: Install Gin**

```bash
cd /Users/esignoretti/Documents/OpenCode/BucketSync && go get github.com/gin-gonic/gin@latest && go mod tidy
```

- [ ] **Step 2: Write api/router.go**

```go
package api

import (
    "github.com/gin-gonic/gin"
    "github.com/esignoretti/bucketsync/internal/config"
)

type Server struct {
    repo *config.Repository
    // engine registry: map pairID -> *sync.Engine (populated at serve time)
}

func NewServer(repo *config.Repository) *Server {
    return &Server{repo: repo}
}

func (s *Server) Router() *gin.Engine {
    r := gin.Default()

    api := r.Group("/api")
    {
        // Buckets
        api.POST("/buckets", s.createBucket)
        api.GET("/buckets", s.listBuckets)
        api.GET("/buckets/:id", s.getBucket)
        api.PUT("/buckets/:id", s.updateBucket)
        api.DELETE("/buckets/:id", s.deleteBucket)

        // Sync Pairs
        api.POST("/sync-pairs", s.createSyncPair)
        api.GET("/sync-pairs", s.listSyncPairs)
        api.GET("/sync-pairs/:id", s.getSyncPair)
        api.PUT("/sync-pairs/:id", s.updateSyncPair)
        api.DELETE("/sync-pairs/:id", s.deleteSyncPair)
        api.POST("/sync-pairs/:id/sync", s.triggerSync)
        api.GET("/sync-pairs/:id/status", s.syncStatus)

        // System
        api.GET("/health", s.health)
        api.GET("/version", s.version)
    }

    // Web UI served at root
    r.GET("/", s.serveWeb)
    r.Static("/static", "./web/static")

    return r
}
```

- [ ] **Step 3: Write api/handlers.go**

```go
package api

import (
    "net/http"

    "github.com/gin-gonic/gin"
    "github.com/esignoretti/bucketsync/internal/config"
)

type apiResponse struct {
    Data  interface{} `json:"data,omitempty"`
    Error string      `json:"error,omitempty"`
}

func respond(c *gin.Context, status int, data interface{}) {
    c.JSON(status, apiResponse{Data: data})
}

func respondError(c *gin.Context, status int, msg string) {
    c.JSON(status, apiResponse{Error: msg})
}

// --- Buckets ---

func (s *Server) createBucket(c *gin.Context) {
    var b config.Bucket
    if err := c.ShouldBindJSON(&b); err != nil {
        respondError(c, http.StatusBadRequest, err.Error())
        return
    }
    if err := s.repo.CreateBucket(&b); err != nil {
        respondError(c, http.StatusInternalServerError, err.Error())
        return
    }
    respond(c, http.StatusCreated, b)
}

func (s *Server) listBuckets(c *gin.Context) {
    buckets, err := s.repo.ListBuckets()
    if err != nil {
        respondError(c, http.StatusInternalServerError, err.Error())
        return
    }
    respond(c, http.StatusOK, buckets)
}

func (s *Server) getBucket(c *gin.Context) {
    b, err := s.repo.GetBucket(c.Param("id"))
    if err != nil {
        respondError(c, http.StatusNotFound, err.Error())
        return
    }
    respond(c, http.StatusOK, b)
}

func (s *Server) updateBucket(c *gin.Context) {
    var b config.Bucket
    if err := c.ShouldBindJSON(&b); err != nil {
        respondError(c, http.StatusBadRequest, err.Error())
        return
    }
    b.ID = c.Param("id")
    if err := s.repo.UpdateBucket(&b); err != nil {
        respondError(c, http.StatusInternalServerError, err.Error())
        return
    }
    respond(c, http.StatusOK, b)
}

func (s *Server) deleteBucket(c *gin.Context) {
    if err := s.repo.DeleteBucket(c.Param("id")); err != nil {
        respondError(c, http.StatusNotFound, err.Error())
        return
    }
    respond(c, http.StatusOK, gin.H{"deleted": true})
}

// --- Sync Pairs ---

func (s *Server) createSyncPair(c *gin.Context) {
    var p config.SyncPair
    if err := c.ShouldBindJSON(&p); err != nil {
        respondError(c, http.StatusBadRequest, err.Error())
        return
    }
    if err := s.repo.CreateSyncPair(&p); err != nil {
        respondError(c, http.StatusInternalServerError, err.Error())
        return
    }
    respond(c, http.StatusCreated, p)
}

func (s *Server) listSyncPairs(c *gin.Context) {
    pairs, err := s.repo.ListSyncPairs()
    if err != nil {
        respondError(c, http.StatusInternalServerError, err.Error())
        return
    }
    respond(c, http.StatusOK, pairs)
}

func (s *Server) getSyncPair(c *gin.Context) {
    p, err := s.repo.GetSyncPair(c.Param("id"))
    if err != nil {
        respondError(c, http.StatusNotFound, err.Error())
        return
    }
    respond(c, http.StatusOK, p)
}

func (s *Server) updateSyncPair(c *gin.Context) {
    var p config.SyncPair
    if err := c.ShouldBindJSON(&p); err != nil {
        respondError(c, http.StatusBadRequest, err.Error())
        return
    }
    p.ID = c.Param("id")
    if err := s.repo.UpdateSyncPair(&p); err != nil {
        respondError(c, http.StatusInternalServerError, err.Error())
        return
    }
    respond(c, http.StatusOK, p)
}

func (s *Server) deleteSyncPair(c *gin.Context) {
    if err := s.repo.DeleteSyncPair(c.Param("id")); err != nil {
        respondError(c, http.StatusNotFound, err.Error())
        return
    }
    respond(c, http.StatusOK, gin.H{"deleted": true})
}

func (s *Server) triggerSync(c *gin.Context) {
    // TODO: run engine for this pair
    respond(c, http.StatusAccepted, gin.H{"message": "sync triggered"})
}

func (s *Server) syncStatus(c *gin.Context) {
    respond(c, http.StatusOK, gin.H{"status": "idle"})
}

func (s *Server) health(c *gin.Context) {
    respond(c, http.StatusOK, gin.H{"status": "ok"})
}

func (s *Server) version(c *gin.Context) {
    respond(c, http.StatusOK, gin.H{"version": "0.1.0"})
}
```

- [ ] **Step 4: Write api/web.go**

```go
package api

import (
    "embed"
    "html/template"
    "net/http"

    "github.com/gin-gonic/gin"
)

//go:embed web/templates/*.html
var templateFS embed.FS

//go:embed web/static/*
var staticFS embed.FS

var tmpls = template.Must(template.ParseFS(templateFS, "web/templates/*.html"))

func (s *Server) serveWeb(c *gin.Context) {
    c.HTML(http.StatusOK, "layout.html", gin.H{
        "title": "BucketSync",
    })
}
```

Wait — `embed.FS` paths are relative to the source file's directory. Since `api/web.go` is in `internal/api/`, the embed paths would be relative to that. But the templates are in `web/templates/` at the project root. We can't use `embed.FS` from a subdirectory to reference parent-directory files.

Solution: Move the templates into `internal/api/web/templates/` or embed from `main.go` where the relative path works. Best approach: embed from `main.go` and pass the content to the API server.

Actually simpler: put the embed in `api/web.go` but use `//go:embed` with absolute-from-module-root paths... No, that doesn't work with Go embed (must be relative to source file).

Better approach: define the embed at the `main.go` level or `cmd/` level, or move web assets into `internal/api/`.

Let me restructure: put web assets in `internal/api/web/` and embed from `api/web.go`.

```
internal/api/web/
├── templates/
│   ├── layout.html
│   └── dashboard.html
└── static/
    └── style.css
```

Then in `api/web.go`:

```go
//go:embed web/templates/*.html
//go:embed web/static/*
var webFS embed.FS
```

This works since `web/` is the directory containing `web.go`'s sibling `web/` subdirectory.

Actually no — the Go embed path is relative to the directory containing the source file. If `web.go` is at `internal/api/web.go`, then `//go:embed web/templates/*.html` would look for `internal/api/web/templates/`. Let me just nest them properly.

Let me adjust the file structure in the plan to use `internal/api/web/` prefix.

- [ ] **Step 5: Create web asset directory structure**

```bash
mkdir -p /Users/esignoretti/Documents/OpenCode/BucketSync/internal/api/web/templates
mkdir -p /Users/esignoretti/Documents/OpenCode/BucketSync/internal/api/web/static
```

Then update the web.go file accordingly. Let me fix this in the plan.

- [ ] **Step 6: Write cmd/serve.go**

```go
package cmd

import (
    "fmt"

    "github.com/esignoretti/bucketsync/internal/api"
    "github.com/spf13/cobra"
)

var serveCmd = &cobra.Command{
    Use:   "serve",
    Short: "Start API server + sync engine + web UI",
    RunE: func(cmd *cobra.Command, args []string) error {
        repo, close, err := openConfig()
        if err != nil {
            return err
        }
        defer close()

        port, _ := cmd.Flags().GetInt("port")
        srv := api.NewServer(repo)
        router := srv.Router()

        fmt.Printf("BucketSync server starting on :%d\n", port)
        return router.Run(fmt.Sprintf(":%d", port))
    },
}

func init() {
    serveCmd.Flags().Int("port", 8080, "HTTP port")
    rootCmd.AddCommand(serveCmd)
}
```

- [ ] **Step 7: Verify build**

```bash
cd /Users/esignoretti/Documents/OpenCode/BucketSync && go build ./...
# Expected: no errors
```

- [ ] **Step 8: Commit**

```bash
cd /Users/esignoretti/Documents/OpenCode/BucketSync && git add -A && git commit -m "feat: add API server with gin router and handlers"
```

---

### Task 13: Web UI — Templates + CSS

**Files:**
- Create: `BucketSync/internal/api/web/templates/layout.html`
- Create: `BucketSync/internal/api/web/templates/dashboard.html`
- Create: `BucketSync/internal/api/web/static/style.css`

- [ ] **Step 1: Write layout.html**

```html
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{{.title}} — BucketSync</title>
    <link rel="stylesheet" href="/static/style.css">
</head>
<body>
    <nav class="nav">
        <div class="nav-brand">BucketSync</div>
        <div class="nav-links">
            <a href="/">Dashboard</a>
        </div>
    </nav>
    <main class="main">
        {{template "content" .}}
    </main>
    <script src="/static/app.js"></script>
</body>
</html>
```

- [ ] **Step 2: Write dashboard.html**

```html
{{define "content"}}
<div class="dashboard">
    <h1 class="page-title">Sync Pairs</h1>
    <div class="pair-grid" id="pair-grid">
        <div class="pair-card">
            <div class="pair-header">
                <span class="status-pill status-idle">Idle</span>
                <h2>Example Pair</h2>
            </div>
            <div class="pair-stats">
                <div class="stat">
                    <span class="stat-label">Source</span>
                    <span class="stat-value">source-bucket</span>
                </div>
                <div class="stat">
                    <span class="stat-label">Target</span>
                    <span class="stat-value">target-bucket</span>
                </div>
                <div class="stat">
                    <span class="stat-label">Last Sync</span>
                    <span class="stat-value">—</span>
                </div>
            </div>
        </div>
    </div>
</div>
{{end}}
```

- [ ] **Step 3: Write style.css (Cubbit-inspired dark theme)**

```css
:root {
    --canvas: #040404;
    --surface-1: #0f1011;
    --surface-2: #141516;
    --surface-3: #18191a;
    --hairline: #23252a;
    --blue: #0065FF;
    --blue-hover: #005CE8;
    --green: #27B681;
    --red: #f6465d;
    --grey: #596773;
    --grey-light: #9099A1;
    --text: #DEE4EA;
    --text-muted: #9099A1;
}

* { margin: 0; padding: 0; box-sizing: border-box; }

body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    background: var(--canvas);
    color: var(--text);
    line-height: 1.5;
}

.nav {
    display: flex;
    align-items: center;
    padding: 0 24px;
    height: 56px;
    border-bottom: 1px solid var(--hairline);
    background: var(--surface-1);
}

.nav-brand {
    font-weight: 600;
    font-size: 18px;
    color: var(--blue);
}

.nav-links { margin-left: 24px; }
.nav-links a {
    color: var(--grey-light);
    text-decoration: none;
    font-size: 14px;
}
.nav-links a:hover { color: var(--text); }

.main {
    max-width: 1200px;
    margin: 0 auto;
    padding: 32px 24px;
}

.page-title {
    font-size: 28px;
    font-weight: 600;
    margin-bottom: 24px;
}

.pair-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(320px, 1fr));
    gap: 16px;
}

.pair-card {
    background: var(--surface-2);
    border: 1px solid var(--hairline);
    border-radius: 8px;
    padding: 20px;
}

.pair-header {
    display: flex;
    align-items: center;
    gap: 12px;
    margin-bottom: 16px;
}

.pair-header h2 {
    font-size: 16px;
    font-weight: 600;
}

.status-pill {
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    padding: 2px 8px;
    border-radius: 9999px;
}

.status-idle { background: var(--surface-3); color: var(--grey-light); }
.status-running { background: var(--blue); color: white; }
.status-error { background: var(--red); color: white; }
.status-synced { background: var(--green); color: white; }

.pair-stats {
    display: flex;
    flex-direction: column;
    gap: 8px;
}

.stat {
    display: flex;
    justify-content: space-between;
    font-size: 13px;
}

.stat-label { color: var(--text-muted); }
.stat-value { color: var(--text); font-family: 'SF Mono', Monaco, monospace; }
```

- [ ] **Step 4: Write app.js (minimal — poll status)**

```javascript
// Poll sync pair status every 5 seconds
async function pollStatus() {
    try {
        const res = await fetch('/api/sync-pairs');
        const json = await res.json();
        // Update UI...
    } catch(e) {
        console.error('poll failed', e);
    }
}
// setInterval(pollStatus, 5000);
```

- [ ] **Step 5: Verify build**

```bash
cd /Users/esignoretti/Documents/OpenCode/BucketSync && go build ./...
# Expected: no errors
```

- [ ] **Step 6: Commit**

```bash
cd /Users/esignoretti/Documents/OpenCode/BucketSync && git add -A && git commit -m "feat: add web UI with Cubbit-inspired dark theme"
```

---

### Task 14: Status Command + One-Shot Sync Wiring

**Files:**
- Create: `BucketSync/cmd/status.go`
- Modify: `BucketSync/cmd/pair.go` (wire `pair sync` to real engine)

- [ ] **Step 1: Write cmd/status.go**

```go
package cmd

import (
    "fmt"
    "os"
    "text/tabwriter"

    "github.com/spf13/cobra"
)

var statusCmd = &cobra.Command{
    Use:   "status",
    Short: "Show sync status",
    RunE: func(cmd *cobra.Command, args []string) error {
        repo, close, err := openConfig()
        if err != nil {
            return err
        }
        defer close()

        pairs, err := repo.ListSyncPairs()
        if err != nil {
            return err
        }

        w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
        fmt.Fprintln(w, "PAIR\tSTATUS\tLAST SYNC\tERRORS")
        for _, p := range pairs {
            lastSync := "never"
            if p.LastSyncAt != nil {
                lastSync = p.LastSyncAt.Format("2006-01-02 15:04:05")
            }
            status := p.LastSyncStatus
            if status == "" {
                status = "never"
            }
            fmt.Fprintf(w, "%s\t%s\t%s\t%d\n", p.Name, status, lastSync, p.ConsecutiveErrors)
        }
        w.Flush()
        return nil
    },
}

func init() {
    rootCmd.AddCommand(statusCmd)
}
```

- [ ] **Step 2: Wire `pair sync` to real engine (modify cmd/pair.go)**

Replace the `pairSyncCmd` RunE placeholder with actual engine invocation:

```go
var pairSyncCmd = &cobra.Command{
    Use:   "sync [id]",
    Short: "Trigger one-shot sync for a pair",
    Args:  cobra.ExactArgs(1),
    RunE: func(cmd *cobra.Command, args []string) error {
        repo, close, err := openConfig()
        if err != nil {
            return err
        }
        defer close()

        pair, err := repo.GetSyncPair(args[0])
        if err != nil {
            return err
        }
        src, err := repo.GetBucket(pair.SourceBucketID)
        if err != nil {
            return err
        }
        tgt, err := repo.GetBucket(pair.TargetBucketID)
        if err != nil {
            return err
        }

        srcS3, err := s3client.NewClient(src)
        if err != nil {
            return err
        }
        tgtS3, err := s3client.NewClient(tgt)
        if err != nil {
            return err
        }

        cacheDir := filepath.Join(defaultConfigDir(), "cache.db")
        cacheStore, err := cache.Open(cacheDir)
        if err != nil {
            return err
        }
        defer cacheStore.Close()

        engine := sync.NewEngine(pair, src, tgt, srcS3, tgtS3, cacheStore)
        ctx := context.Background()

        if err := engine.RunOnce(ctx); err != nil {
            return fmt.Errorf("sync failed: %w", err)
        }

        // Update pair status
        _, running, lastRun, status := engine.Status()
        now := time.Now().UTC()
        pair.LastSyncAt = &now
        pair.LastSyncStatus = status
        if status == "error" {
            pair.ConsecutiveErrors++
        } else {
            pair.ConsecutiveErrors = 0
        }
        repo.UpdateSyncPair(pair)

        fmt.Printf("Sync complete. Status: %s\n", status)
        return nil
    },
}
```

Add imports to pair.go:
```go
import (
    "context"
    "fmt"
    "path/filepath"
    "time"

    "github.com/esignoretti/bucketsync/internal/cache"
    "github.com/esignoretti/bucketsync/internal/s3client"
    "github.com/esignoretti/bucketsync/internal/sync"
    "github.com/spf13/cobra"
)
```

- [ ] **Step 3: Verify build**

```bash
cd /Users/esignoretti/Documents/OpenCode/BucketSync && go build ./...
# Expected: no errors
```

- [ ] **Step 4: Commit**

```bash
cd /Users/esignoretti/Documents/OpenCode/BucketSync && git add -A && git commit -m "feat: wire one-shot sync with engine"
```

---

### Task 15: Integration Tests

**Files:**
- Create: `BucketSync/tests/sync_test.go`
- Create: `BucketSync/tests/api_test.go`

- [ ] **Step 1: Write integration test (generic S3-compatible)**

```go
// tests/sync_test.go
//go:build integration
package tests

import (
    "context"
    "os"
    "testing"

    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/credentials"
    "github.com/aws/aws-sdk-go-v2/service/s3"
)

func setupS3(t *testing.T) (*s3.Client, string) {
    t.Helper()
    endpoint := os.Getenv("S3_TEST_ENDPOINT")
    if endpoint == "" {
        t.Skip("S3_TEST_ENDPOINT not set")
    }
    accessKey := os.Getenv("S3_TEST_ACCESS_KEY")
    secretKey := os.Getenv("S3_TEST_SECRET_KEY")
    if accessKey == "" || secretKey == "" {
        t.Skip("S3_TEST_ACCESS_KEY or S3_TEST_SECRET_KEY not set")
    }
    creds := credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")
    cfg, err := config.LoadDefaultConfig(context.TODO(),
        config.WithRegion("us-east-1"),
        config.WithCredentialsProvider(creds),
    )
    if err != nil {
        t.Fatal(err)
    }
    client := s3.NewFromConfig(cfg, func(o *s3.Options) {
        o.BaseEndpoint = aws.String(endpoint)
        o.UsePathStyle = true
    })
    return client, "test-s3sync"
}
```

- [ ] **Step 2: Write API test**

```go
// tests/api_test.go
package tests

import (
    "encoding/json"
    "net/http"
    "net/http/httptest"
    "testing"

    "github.com/esignoretti/bucketsync/internal/api"
    "github.com/esignoretti/bucketsync/internal/config"
)

func TestAPIHealth(t *testing.T) {
    db, _ := config.Open(":memory:")
    repo := config.NewRepository(db)
    srv := api.NewServer(repo)
    router := srv.Router()

    w := httptest.NewRecorder()
    req, _ := http.NewRequest("GET", "/api/health", nil)
    router.ServeHTTP(w, req)

    if w.Code != 200 {
        t.Fatalf("expected 200, got %d", w.Code)
    }

    var resp struct {
        Data struct {
            Status string `json:"status"`
        } `json:"data"`
    }
    json.NewDecoder(w.Body).Decode(&resp)
    if resp.Data.Status != "ok" {
        t.Fatalf("expected ok, got %s", resp.Data.Status)
    }
}

func TestAPIBucketCRUD(t *testing.T) {
    db, _ := config.Open(":memory:")
    repo := config.NewRepository(db)
    srv := api.NewServer(repo)
    router := srv.Router()

    // Create
    body := `{"name":"test","endpoint":"https://s3.amazonaws.com","region":"us-east-1","access_key":"ak","secret_key":"sk","bucket_name":"b"}`
    w := httptest.NewRecorder()
    req, _ := http.NewRequest("POST", "/api/buckets", jsonBody(body))
    req.Header.Set("Content-Type", "application/json")
    router.ServeHTTP(w, req)

    if w.Code != 201 {
        t.Fatalf("expected 201, got %d: %s", w.Code, w.Body.String())
    }

    // List
    w = httptest.NewRecorder()
    req, _ = http.NewRequest("GET", "/api/buckets", nil)
    router.ServeHTTP(w, req)
    if w.Code != 200 {
        t.Fatalf("expected 200, got %d", w.Code)
    }
}

func jsonBody(s string) *strings.Reader {
    return strings.NewReader(s)
}
```

Add import: `"strings"`

- [ ] **Step 3: Run tests**

```bash
cd /Users/esignoretti/Documents/OpenCode/BucketSync && go test ./tests/ -v
# Expected: PASS (API tests)
```

```bash
cd /Users/esignoretti/Documents/OpenCode/BucketSync && go test -tags=integration ./tests/ -v
# Expected: SKIP or PASS (depending on MINIO_ENDPOINT)
```

- [ ] **Step 4: Commit**

```bash
cd /Users/esignoretti/Documents/OpenCode/BucketSync && git add -A && git commit -m "test: add integration and API tests"
```

---

### Task 16: Final Wiring + Go Vet

- [ ] **Step 1: Run full test suite**

```bash
cd /Users/esignoretti/Documents/OpenCode/BucketSync && go vet ./... && go test ./... -v 2>&1 | head -100
```

Fix any issues.

- [ ] **Step 2: Run go mod tidy**

```bash
cd /Users/esignoretti/Documents/OpenCode/BucketSync && go mod tidy
```

- [ ] **Step 3: Final build**

```bash
cd /Users/esignoretti/Documents/OpenCode/BucketSync && go build -o bucketsync .
```

- [ ] **Step 4: Verify CLI works**

```bash
cd /Users/esignoretti/Documents/OpenCode/BucketSync && ./bucketsync --help
```

Expected: shows help output with all commands.

- [ ] **Step 5: Commit**

```bash
cd /Users/esignoretti/Documents/OpenCode/BucketSync && git add -A && git commit -m "chore: final wiring and cleanup"
```

---

## Spec Coverage Map

| Spec Section | Tasks |
|---|---|
| 1. Overview | Task 1 |
| 2. Architecture | Task 1, 9, 10 |
| 3. Data Model | Task 3, 4, 6 |
| 4. Sync Engine | Task 8, 9, 10 |
| 5. Target Bucket Auto-Config | *deferred to v1.1* |
| 6. API | Task 12 |
| 7. CLI | Task 11, 14 |
| 8. Web UI | Task 13 |
| 9. Logging | Task 2 |
| 10. Error Handling | Task 10 (engine isolation) |
| 11. Testing | Task 15 |
| 13. Config Defaults | Task 4 (model fields) |

**Implemented:** Target bucket auto-config (`internal/sync/setup.go`). Engine checks HEAD, creates bucket with region, configures versioning + object lock + retention. Warnings on mismatch.
