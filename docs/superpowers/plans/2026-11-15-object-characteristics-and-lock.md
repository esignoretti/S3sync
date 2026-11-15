# Object Characteristic Replication & Locked/Expiring Target — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replicate per-object characteristics (metadata, tags, retention, legal hold, storage class) on copy, and provision the target bucket as a lock-enabled, lifecycle-expiring replica at pair creation.

**Architecture:** The worker's copy path becomes a three-call pipeline (source `HeadObject` → `CopyObject` with `MetadataDirective=REPLACE` + lock fields → `GetObjectTagging`/`PutObjectTagging`), with an `attrs_hash` fingerprint in the BoltDB cache so partial failures retry as a new `ActionCopyAttributes` without re-copying bytes. Target provisioning becomes a decision-table preflight in `setup.go` that creates the bucket with lock, sets default retention, and writes a lifecycle config with an `Expiration.Date` plus `NoncurrentVersionExpiration`.

**Tech Stack:** Go 1.24, aws-sdk-go-v2 `service/s3` v1.105.1, SQLite (modernc.org/sqlite), bbolt cache, cobra CLI, gin API, `html/template` + vanilla JS UI.

**Spec:** `docs/superpowers/specs/2026-11-15-object-characteristics-and-lock-design.md`

## Global Constraints

- All new pair behaviour defaults **off**: `replicate_attributes=0`, `require_object_lock=0`, `attrs_reconcile_interval=0`, `on_lock_missing='fail'`.
- Tags must **not** rely on `x-amz-tagging` or `TaggingDirective` on `CopyObject` — Cubbit DS3 ignores both. Use `GetObjectTagging` → `PutObjectTagging`.
- Metadata must be sent with `MetadataDirective=REPLACE` and an explicit map; `COPY` is not reliable on the target.
- Object lock fields on `CopyObject` (`ObjectLockMode`, `ObjectLockRetainUntilDate`, `ObjectLockLegalHoldStatus`) are applied **only** when `RetainUntilDate` is in the future; legal hold is passed independently.
- Object Lock can only be enabled at `CreateBucket`. Never attempt `PutObjectLockConfiguration` to "convert" a bucket that was created without lock.
- `Expiration.Date` must be **midnight UTC** and **in the future**; both are validated and rejected with the offending value in the message.
- `Expiration` and `ExpiredObjectDeleteMarker` must never appear in the same rule.
- `PutBucketLifecycleConfiguration` replaces the whole config; log the previous config at `warn` before writing.
- COMPLIANCE retention requires an explicit `--accept-compliance`; enforcement lives in CLI/API validation, not in `ProvisionTarget`.
- Objects > 5 GiB return `ErrObjectTooLarge`; no multipart copy.
- Existing migrations are additive `ALTER TABLE ... ADD COLUMN` in `internal/config/db.go:64-70`, errors ignored. Follow that pattern.
- Tests run with `go test ./...`; integration tests are `-tags=integration` and skip without `S3_TEST_ENDPOINT`.

## Review Focus

1. **Source object vanishes between listing and `HeadObject`** — a `NotFound` from the source HEAD must be skipped (logged, not counted as a failure), never retried 3× as `retryS3` would.
2. **Past `RetainUntilDate` on the source** — must be omitted from `CopyObject` entirely; sending it makes the whole copy fail.
3. **Cache entries written by the previous version** (empty `attrs_hash`, `pending_attrs=false`) — must be treated as "unknown", never as drift, or every upgrade triggers a full re-attribute sweep.
4. **Retention copied onto a target bucket without object lock** — `replicate_attributes` without `require_object_lock` on a bucket lacking lock must produce a clear per-object error naming object lock, not an opaque S3 rejection.
5. **`target_expire_at` in the past or with a non-midnight-UTC time** — must be rejected at pair creation and at `pair update` with the offending value quoted.

---

## File Structure

**Create**
- `internal/sync/lifecycle.go` — lifecycle rule construction and expiry validation. No I/O.
- `internal/sync/lifecycle_test.go`
- `internal/sync/attrs.go` — `ObjectAttrs`, canonical fingerprint, `HeadObject` decoding, copy-input construction. No I/O.
- `internal/sync/attrs_test.go`
- `internal/sync/provision.go` — `ProvisionTarget` decision table, `targetAPI` interface, `ProvisionOptions`. All S3 I/O for bucket setup lives here.
- `internal/sync/provision_test.go` — table-driven over a fake `targetAPI`.
- `internal/sync/reconcile.go` — `shouldReconcile`, drift sweep over source HEADs.
- `internal/sync/reconcile_test.go`

**Modify**
- `internal/config/models.go` — new `SyncPair` fields.
- `internal/config/db.go:64-70` — ten new migration lines.
- `internal/config/repo.go` — insert/update/scan for the new columns (5 sites).
- `internal/config/repo_test.go`, `internal/config/db_test.go` — coverage.
- `internal/cache/models.go` — `AttrsHash`, `PendingAttrs`.
- `internal/sync/worker.go` — P0 fix, `ErrObjectTooLarge`, attrs pipeline, `applyAttrs`.
- `internal/sync/worker_test.go` — regression tests.
- `internal/sync/differ.go` — `ActionCopyAttributes`.
- `internal/sync/differ_test.go`
- `internal/sync/engine.go` — expired guard, pending counter, provisioning wiring, reconcile hook, cache fields.
- `internal/sync/setup.go` — becomes a thin compatibility wrapper delegating to `ProvisionTarget`.
- `internal/sync/run.go` — `ProvisionPair` helper.
- `cmd/pair.go` — flags for add/update, preflight on add.
- `internal/api/handlers.go` — create/update/response fields.
- `internal/api/web/static/app.js`, `internal/api/web/templates/` — badges, form fields, confirmations.

---

### Task 1: Config and cache schema

**Files:**
- Modify: `internal/config/models.go:19-39`, `internal/config/db.go:64-70`, `internal/config/repo.go:146-204`, `internal/config/repo.go:319-370`
- Modify: `internal/cache/models.go:5-11`
- Test: `internal/config/repo_test.go`, `internal/config/db_test.go`

**Interfaces:**
- Produces: `config.SyncPair` fields `ReplicateAttributes bool`, `TargetExpireAt *time.Time`, `NoncurrentExpireDays int`, `NewerNoncurrentVersions int`, `RequireObjectLock bool`, `OnLockMissing string`, `AttrsReconcileInterval int`, `ProvisionedAt *time.Time`, `ProvisionError string`, `LastReconcileAt *time.Time`; `cache.CachedObject` fields `AttrsHash string`, `PendingAttrs bool`.

- [ ] **Step 1: Write the failing test**

In `internal/config/repo_test.go`, add:

```go
func TestSyncPairRoundTripsNewFields(t *testing.T) {
	repo, cleanup := testRepo(t)
	defer cleanup()

	exp := time.Date(2027, 3, 1, 0, 0, 0, 0, time.UTC)
	last := time.Date(2026, 11, 15, 4, 5, 6, 0, time.UTC)
	p := &config.SyncPair{
		Name: "p1", SourceBucketID: "s", TargetBucketID: "t",
		SyncInterval: 300, WorkerCount: 4, Enabled: true,
		ReplicateAttributes: true, TargetExpireAt: &exp,
		NoncurrentExpireDays: 7, NewerNoncurrentVersions: 2,
		RequireObjectLock: true, OnLockMissing: "recreate",
		AttrsReconcileInterval: 3600, ProvisionedAt: &last,
		ProvisionError: "boom", LastReconcileAt: &last,
	}
	if err := repo.CreateSyncPair(p); err != nil {
		t.Fatal(err)
	}
	got, err := repo.GetSyncPair(p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !got.ReplicateAttributes || !got.RequireObjectLock {
		t.Fatalf("bool fields lost: %+v", got)
	}
	if got.OnLockMissing != "recreate" || got.NoncurrentExpireDays != 7 ||
		got.NewerNoncurrentVersions != 2 || got.AttrsReconcileInterval != 3600 {
		t.Fatalf("scalar fields lost: %+v", got)
	}
	if got.TargetExpireAt == nil || !got.TargetExpireAt.Equal(exp) {
		t.Fatalf("TargetExpireAt = %v, want %v", got.TargetExpireAt, exp)
	}
	if got.ProvisionError != "boom" {
		t.Fatalf("ProvisionError = %q", got.ProvisionError)
	}

	// Update path must persist them too.
	got.ProvisionError = "fixed"
	got.OnLockMissing = "fail"
	if err := repo.UpdateSyncPair(got); err != nil {
		t.Fatal(err)
	}
	again, err := repo.GetSyncPair(p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if again.ProvisionError != "fixed" || again.OnLockMissing != "fail" {
		t.Fatalf("update lost fields: %+v", again)
	}
}

func TestSyncPairDefaultsForLegacyRows(t *testing.T) {
	repo, cleanup := testRepo(t)
	defer cleanup()

	p := &config.SyncPair{Name: "old", SourceBucketID: "s", TargetBucketID: "t",
		SyncInterval: 300, WorkerCount: 4, Enabled: true}
	if err := repo.CreateSyncPair(p); err != nil {
		t.Fatal(err)
	}
	got, err := repo.GetSyncPair(p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ReplicateAttributes || got.RequireObjectLock || got.AttrsReconcileInterval != 0 {
		t.Fatalf("new fields must default off: %+v", got)
	}
	if got.OnLockMissing != "fail" {
		t.Fatalf("OnLockMissing default = %q, want fail", got.OnLockMissing)
	}
	if got.TargetExpireAt != nil || got.ProvisionedAt != nil || got.LastReconcileAt != nil {
		t.Fatalf("nullable fields must default nil: %+v", got)
	}
}
```

If `testRepo` does not exist in the package, add it following the existing test helpers in `internal/config/repo_test.go`.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/config/ -run 'TestSyncPairRoundTripsNewFields|TestSyncPairDefaultsForLegacyRows' -v`
Expected: FAIL — compile error, `unknown field ReplicateAttributes`.

- [ ] **Step 3: Add the fields and plumbing**

In `internal/config/models.go`, extend `SyncPair` with the ten fields listed in Interfaces (json tags: `replicate_attributes`, `target_expire_at,omitempty`, `noncurrent_expire_days,omitempty`, `newer_noncurrent_versions,omitempty`, `require_object_lock`, `on_lock_missing`, `attrs_reconcile_interval`, `provisioned_at,omitempty`, `provision_error,omitempty`, `last_reconcile_at,omitempty`).

In `internal/config/db.go`, append to the `migrations` slice:

```go
`ALTER TABLE sync_pairs ADD COLUMN replicate_attributes INTEGER NOT NULL DEFAULT 0`,
`ALTER TABLE sync_pairs ADD COLUMN target_expire_at TEXT`,
`ALTER TABLE sync_pairs ADD COLUMN noncurrent_expire_days INTEGER NOT NULL DEFAULT 0`,
`ALTER TABLE sync_pairs ADD COLUMN newer_noncurrent_versions INTEGER NOT NULL DEFAULT 0`,
`ALTER TABLE sync_pairs ADD COLUMN require_object_lock INTEGER NOT NULL DEFAULT 0`,
`ALTER TABLE sync_pairs ADD COLUMN on_lock_missing TEXT NOT NULL DEFAULT 'fail'`,
`ALTER TABLE sync_pairs ADD COLUMN attrs_reconcile_interval INTEGER NOT NULL DEFAULT 0`,
`ALTER TABLE sync_pairs ADD COLUMN provisioned_at TEXT`,
`ALTER TABLE sync_pairs ADD COLUMN provision_error TEXT NOT NULL DEFAULT ''`,
`ALTER TABLE sync_pairs ADD COLUMN last_reconcile_at TEXT`,
```

In `internal/config/repo.go`, extend the column list in `CreateSyncPair`, `UpdateSyncPair`, `ListSyncPairs` and `GetSyncPair` (all four share the same SELECT list — keep them byte-identical), plus `scanSyncPairFromRow`. Use the existing helpers `boolInt`, `nullTimeRFC`, `nullStr`; read nullable times with `sql.NullString` + `time.Parse(time.RFC3339, ...)` as `scanBucketFromRow` already does.

In `internal/cache/models.go`, add:

```go
AttrsHash    string `json:"attrs_hash,omitempty"`
PendingAttrs bool   `json:"pending_attrs,omitempty"`
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/config/ ./internal/cache/ -v`
Expected: PASS, including the pre-existing tests.

- [ ] **Step 5: Add the legacy-schema migration test**

In `internal/config/db_test.go`, add a test that creates a DB file with the pre-change `sync_pairs` table only (raw `CREATE TABLE` with the old columns), inserts one row, then calls `Open` on that path and asserts `GetSyncPair` returns the row with `OnLockMissing == "fail"` and all new fields at their defaults.

Run: `go test ./internal/config/ -run TestMigration -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/config internal/cache
git commit -m "feat(config): add attribute-replication and lock-provisioning pair fields"
```

---

### Task 2: Lifecycle rule builder

**Files:**
- Create: `internal/sync/lifecycle.go`, `internal/sync/lifecycle_test.go`
- Test: `internal/sync/lifecycle_test.go`

**Interfaces:**
- Consumes: `config.SyncPair` from Task 1.
- Produces: `func BuildLifecycleRules(p *config.SyncPair, now time.Time) ([]s3types.LifecycleRule, error)`; `func ValidateExpireAt(t time.Time, now time.Time) (time.Time, error)`; sentinel `var ErrExpireNotMidnight`, `ErrExpireInPast`.

- [ ] **Step 1: Write the failing test**

```go
func TestValidateExpireAt(t *testing.T) {
	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name    string
		in      time.Time
		wantErr error
	}{
		{"midnight future", time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC), nil},
		{"midnight future other tz", time.Date(2027, 1, 1, 0, 0, 0, 0, time.FixedZone("x", 3600)), nil},
		{"not midnight", time.Date(2027, 1, 1, 9, 30, 0, 0, time.UTC), ErrExpireNotMidnight},
		{"past", time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), ErrExpireInPast},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ValidateExpireAt(tt.in, now)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestBuildLifecycleRules(t *testing.T) {
	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	exp := time.Date(2027, 3, 1, 0, 0, 0, 0, time.UTC)

	t.Run("both rules", func(t *testing.T) {
		rules, err := BuildLifecycleRules(&config.SyncPair{
			TargetExpireAt: &exp, NoncurrentExpireDays: 7, NewerNoncurrentVersions: 2,
		}, now)
		if err != nil {
			t.Fatal(err)
		}
		if len(rules) != 2 {
			t.Fatalf("got %d rules, want 2", len(rules))
		}
		if rules[0].ID == nil || *rules[0].ID != "s3sync-expire" {
			t.Fatalf("rule 0 id = %v", rules[0].ID)
		}
		if rules[0].Expiration == nil || rules[0].Expiration.Date == nil || !rules[0].Expiration.Date.Equal(exp) {
			t.Fatalf("rule 0 expiration = %+v", rules[0].Expiration)
		}
		if rules[0].Expiration.Days != nil || rules[0].Expiration.ExpiredObjectDeleteMarker != nil {
			t.Fatal("expiration must carry only Date")
		}
		if rules[1].NoncurrentVersionExpiration == nil ||
			*rules[1].NoncurrentVersionExpiration.NoncurrentDays != 7 ||
			*rules[1].NoncurrentVersionExpiration.NewerNoncurrentVersions != 2 {
			t.Fatalf("rule 1 noncurrent = %+v", rules[1].NoncurrentVersionExpiration)
		}
		for _, r := range rules {
			if r.Status != s3types.ExpirationStatusEnabled {
				t.Fatalf("rule %v status = %v", *r.ID, r.Status)
			}
			if r.Filter != nil || r.Prefix != nil {
				t.Fatal("filters and prefixes are unsupported by the target")
			}
		}
	})

	t.Run("noncurrent only", func(t *testing.T) {
		rules, err := BuildLifecycleRules(&config.SyncPair{NoncurrentExpireDays: 3}, now)
		if err != nil {
			t.Fatal(err)
		}
		if len(rules) != 1 || rules[0].NoncurrentVersionExpiration == nil {
			t.Fatalf("rules = %+v", rules)
		}
		if rules[0].NoncurrentVersionExpiration.NewerNoncurrentVersions != nil {
			t.Fatal("NewerNoncurrentVersions must be omitted when zero")
		}
	})

	t.Run("neither set", func(t *testing.T) {
		rules, err := BuildLifecycleRules(&config.SyncPair{}, now)
		if err != nil {
			t.Fatal(err)
		}
		if len(rules) != 0 {
			t.Fatalf("got %d rules, want 0", len(rules))
		}
	})

	t.Run("invalid date propagates", func(t *testing.T) {
		bad := time.Date(2027, 3, 1, 5, 0, 0, 0, time.UTC)
		_, err := BuildLifecycleRules(&config.SyncPair{TargetExpireAt: &bad}, now)
		if !errors.Is(err, ErrExpireNotMidnight) {
			t.Fatalf("err = %v", err)
		}
	})
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/sync/ -run 'TestValidateExpireAt|TestBuildLifecycleRules' -v`
Expected: FAIL — `undefined: ValidateExpireAt`.

- [ ] **Step 3: Implement `internal/sync/lifecycle.go`**

`ValidateExpireAt` converts to UTC and returns `ErrExpireNotMidnight` if `Hour/Minute/Second/Nanosecond` are not all zero, then `ErrExpireInPast` if `!t.After(now)`. Both sentinels wrap a message that quotes the value: `fmt.Errorf("%w: %s (must be midnight UTC)", ErrExpireNotMidnight, t.UTC().Format(time.RFC3339))`.

`BuildLifecycleRules` calls `ValidateExpireAt` when `TargetExpireAt != nil`, emits rule `s3sync-expire` with only `Expiration{Date: &exp}`, and when `NoncurrentExpireDays > 0` emits rule `s3sync-noncurrent` with `NoncurrentVersionExpiration{NoncurrentDays: aws.Int32(int32(p.NoncurrentExpireDays))}` plus `NewerNoncurrentVersions` only when `p.NewerNoncurrentVersions > 0`. Never set `Filter`, `Prefix`, or `ExpiredObjectDeleteMarker`.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/sync/ -run 'TestValidateExpireAt|TestBuildLifecycleRules' -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/sync/lifecycle.go internal/sync/lifecycle_test.go
git commit -m "feat(sync): lifecycle rule builder with midnight-UTC and future validation"
```

---

### Task 3: ProvisionTarget preflight

**Files:**
- Create: `internal/sync/provision.go`, `internal/sync/provision_test.go`
- Modify: `internal/sync/setup.go` (becomes a delegating wrapper)
- Test: `internal/sync/provision_test.go`

**Interfaces:**
- Consumes: `BuildLifecycleRules` (Task 2), `config.SyncPair` fields (Task 1).
- Produces:

```go
type targetAPI interface {
	HeadBucket(context.Context, *s3.HeadBucketInput, ...func(*s3.Options)) (*s3.HeadBucketOutput, error)
	CreateBucket(context.Context, *s3.CreateBucketInput, ...func(*s3.Options)) (*s3.CreateBucketOutput, error)
	DeleteBucket(context.Context, *s3.DeleteBucketInput, ...func(*s3.Options)) (*s3.DeleteBucketOutput, error)
	PutBucketVersioning(context.Context, *s3.PutBucketVersioningInput, ...func(*s3.Options)) (*s3.PutBucketVersioningOutput, error)
	GetBucketVersioning(context.Context, *s3.GetBucketVersioningInput, ...func(*s3.Options)) (*s3.GetBucketVersioningOutput, error)
	PutObjectLockConfiguration(context.Context, *s3.PutObjectLockConfigurationInput, ...func(*s3.Options)) (*s3.PutObjectLockConfigurationOutput, error)
	GetObjectLockConfiguration(context.Context, *s3.GetObjectLockConfigurationInput, ...func(*s3.Options)) (*s3.GetObjectLockConfigurationOutput, error)
	PutBucketLifecycleConfiguration(context.Context, *s3.PutBucketLifecycleConfigurationInput, ...func(*s3.Options)) (*s3.PutBucketLifecycleConfigurationOutput, error)
	GetBucketLifecycleConfiguration(context.Context, *s3.GetBucketLifecycleConfigurationInput, ...func(*s3.Options)) (*s3.GetBucketLifecycleConfigurationOutput, error)
	ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

type ProvisionOptions struct {
	ForceRecreate bool
	Now           time.Time
}

func ProvisionTarget(ctx context.Context, api targetAPI, srcRegion string, tgt *config.Bucket, pair *config.SyncPair, opts ProvisionOptions) error
var ErrLockMissing = errors.New("target bucket exists without object lock and cannot be converted")
var ErrTargetNotEmpty = errors.New("refusing to recreate a non-empty target bucket")
```

- [ ] **Step 1: Write the failing test**

```go
type fakeTarget struct {
	headErr        error
	objects        int32
	lockEnabled    bool
	lockConfigSet  bool
	lifecycleSet   []s3types.LifecycleRule
	calls          []string
}

func (f *fakeTarget) HeadBucket(ctx context.Context, in *s3.HeadBucketInput, _ ...func(*s3.Options)) (*s3.HeadBucketOutput, error) {
	f.calls = append(f.calls, "HeadBucket")
	if f.headErr != nil { return nil, f.headErr }
	return &s3.HeadBucketOutput{}, nil
}
// ... remaining methods append their name to f.calls and record inputs.

func TestProvisionTargetDecisionTable(t *testing.T) {
	exp := time.Date(2027, 3, 1, 0, 0, 0, 0, time.UTC)
	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	notFound := &s3types.NotFound{}

	tests := []struct {
		name     string
		target   *fakeTarget
		pair     *config.SyncPair
		opts     ProvisionOptions
		wantErr  error
		wantCall []string
	}{
		{
			name:   "absent bucket created with lock then retention then lifecycle",
			target: &fakeTarget{headErr: notFound},
			pair:   &config.SyncPair{RequireObjectLock: true, OnLockMissing: "fail", TargetExpireAt: &exp},
			wantCall: []string{"HeadBucket", "CreateBucket", "PutBucketVersioning",
				"PutObjectLockConfiguration", "GetBucketLifecycleConfiguration",
				"PutBucketLifecycleConfiguration"},
		},
		{
			name:   "present locked bucket accepted",
			target: &fakeTarget{lockEnabled: true},
			pair:   &config.SyncPair{RequireObjectLock: true, OnLockMissing: "fail"},
			wantCall: []string{"HeadBucket", "GetObjectLockConfiguration"},
		},
		{
			name:    "present unlocked bucket with fail policy errors",
			target:  &fakeTarget{},
			pair:    &config.SyncPair{RequireObjectLock: true, OnLockMissing: "fail"},
			wantErr: ErrLockMissing,
			wantCall: []string{"HeadBucket", "GetObjectLockConfiguration"},
		},
		{
			name:    "recreate without force flag errors",
			target:  &fakeTarget{},
			pair:    &config.SyncPair{RequireObjectLock: true, OnLockMissing: "recreate"},
			wantErr: ErrLockMissing,
		},
		{
			name:    "recreate with force but non-empty errors",
			target:  &fakeTarget{objects: 1},
			pair:    &config.SyncPair{RequireObjectLock: true, OnLockMissing: "recreate"},
			opts:    ProvisionOptions{ForceRecreate: true},
			wantErr: ErrTargetNotEmpty,
			wantCall: []string{"HeadBucket", "GetObjectLockConfiguration", "ListObjectsV2"},
		},
		{
			name:   "recreate with force and empty succeeds",
			target: &fakeTarget{},
			pair:   &config.SyncPair{RequireObjectLock: true, OnLockMissing: "recreate"},
			opts:   ProvisionOptions{ForceRecreate: true},
			wantCall: []string{"HeadBucket", "GetObjectLockConfiguration", "ListObjectsV2",
				"DeleteBucket", "CreateBucket", "PutBucketVersioning",
				"PutObjectLockConfiguration", "GetBucketLifecycleConfiguration",
				"PutBucketLifecycleConfiguration"},
		},
		{
			name:   "lock not required leaves an unlocked bucket alone",
			target: &fakeTarget{},
			pair:   &config.SyncPair{},
			wantCall: []string{"HeadBucket"},
		},
	}
	for _, tt := range tests { /* assert tt.wantErr via errors.Is, tt.wantCall via reflect.DeepEqual */ }
}

func TestProvisionTargetRejectsInvalidExpiry(t *testing.T) {
	bad := time.Date(2027, 3, 1, 5, 0, 0, 0, time.UTC)
	err := ProvisionTarget(context.Background(), &fakeTarget{headErr: &s3types.NotFound{}},
		"us-east-1", &config.Bucket{BucketName: "b"}, &config.SyncPair{TargetExpireAt: &bad},
		ProvisionOptions{Now: time.Date(2026, 11, 15, 0, 0, 0, 0, time.UTC)})
	if !errors.Is(err, ErrExpireNotMidnight) {
		t.Fatalf("err = %v", err)
	}
}
```

Note: `CreateBucket` must be called **before** `PutObjectLockConfiguration` in the asserted order — that ordering is the contract.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/sync/ -run TestProvisionTarget -v`
Expected: FAIL — `undefined: ProvisionTarget`.

- [ ] **Step 3: Implement `internal/sync/provision.go`**

Move the bodies of `bucketExists`, `createAndConfigureBucket`, `retentionRule`, and `warnIfMismatch` from `internal/sync/setup.go` into `provision.go`, retyping them against `targetAPI`. `ProvisionTarget` implements the decision table:

1. `HeadBucket` → if `errors.As(err, &s3types.NotFound{})` treat as absent; any other error is returned wrapped.
2. If present and `pair.RequireObjectLock`: `GetObjectLockConfiguration`; if `ObjectLockConfiguration == nil` or `ObjectLockEnabled != ObjectLockEnabledEnabled` → lock is missing.
3. Lock missing and `pair.OnLockMissing != "recreate"` → return `ErrLockMissing` (message names `--on-lock-missing recreate` and `--force-recreate`).
4. Lock missing and `recreate`: if `!opts.ForceRecreate` → `ErrLockMissing`; `ListObjectsV2{MaxKeys: aws.Int32(1)}`; if any object → `ErrTargetNotEmpty`; else `DeleteBucket` and continue as absent.
5. Create path: `CreateBucket` with `ObjectLockEnabledForBucket` when required, `LocationConstraint` when `srcRegion != "us-east-1"`; then `PutBucketVersioning` when `tgt.Versioning` or `pair.RequireObjectLock`; then `PutObjectLockConfiguration` with `retentionRule(tgt)` when lock required.
6. Lifecycle: `BuildLifecycleRules(pair, opts.Now)`; if non-empty, `GetBucketLifecycleConfiguration` and log any existing rules at `warn` (ID + expiration + noncurrent), then `PutBucketLifecycleConfiguration`. Treat a `NoSuchLifecycleConfiguration` error as "no previous config" and continue.
7. Present-and-locked path still applies step 6 (lifecycle must be re-asserted) and then `warnIfMismatch`.

Reduce `internal/sync/setup.go` to:

```go
func SetupTargetBucket(ctx context.Context, client *s3.Client, srcRegion string, tgt *config.Bucket) error {
	return ProvisionTarget(ctx, client, srcRegion, tgt, &config.SyncPair{}, ProvisionOptions{Now: time.Now().UTC()})
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/sync/ -run TestProvisionTarget -v && go build ./...`
Expected: PASS and clean build.

- [ ] **Step 5: Commit**

```bash
git add internal/sync/provision.go internal/sync/provision_test.go internal/sync/setup.go
git commit -m "feat(sync): provisioning preflight with lock decision table and lifecycle"
```

---

### Task 4: Fix the copy P0 and add the size guard

**Files:**
- Modify: `internal/sync/worker.go:86-136`
- Test: `internal/sync/worker_test.go`

**Interfaces:**
- Produces: `var ErrObjectTooLarge = errors.New("object exceeds the 5 GiB CopyObject limit")`; `WorkerPool` gains a `srcClient *s3.Client` field so source HEADs use the source client.
- Consumes: nothing new.

- [ ] **Step 1: Write the failing tests**

```go
type fakeCopyAPI struct {
	copied    int
	headCalls int
	headErr   error
	headETag  string
	headLen   int64
}

func (f *fakeCopyAPI) HeadObject(ctx context.Context, in *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	f.headCalls++
	if f.headErr != nil { return nil, f.headErr }
	return &s3.HeadObjectOutput{ETag: aws.String(f.headETag), ContentLength: aws.Int64(f.headLen)}, nil
}

func (f *fakeCopyAPI) CopyObject(ctx context.Context, in *s3.CopyObjectInput, _ ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
	f.copied++
	return &s3.CopyObjectOutput{}, nil
}

func TestCopyObject_TargetMissingIsNotAnError(t *testing.T) {
	// Regression: a target HEAD returning NotFound used to be retried 3x and
	// returned as an error, so CopyObject was never issued for new objects.
	api := &fakeCopyAPI{headErr: &s3types.NotFound{}}
	wp := newTestWorkerPool(api)
	_, err := wp.copyObject(context.Background(), SyncAction{Type: ActionCopy, Key: "k", ETag: `"e1"`})
	if err != nil {
		t.Fatalf("copyObject returned %v; NotFound must mean needs-copy", err)
	}
	if api.copied != 1 {
		t.Fatalf("CopyObject called %d times, want 1", api.copied)
	}
}

func TestCopyObject_SkipsWhenETagMatches(t *testing.T) {
	api := &fakeCopyAPI{headETag: `"e1"`}
	wp := newTestWorkerPool(api)
	if _, err := wp.copyObject(context.Background(), SyncAction{Type: ActionCopy, Key: "k", ETag: `"e1"`}); err != nil {
		t.Fatal(err)
	}
	if api.copied != 0 {
		t.Fatalf("CopyObject called %d times, want 0", api.copied)
	}
}

func TestCopyObject_TooLarge(t *testing.T) {
	api := &fakeCopyAPI{headErr: &s3types.NotFound{}, headLen: 6 << 30}
	wp := newTestWorkerPool(api)
	_, err := wp.copyObject(context.Background(), SyncAction{Type: ActionCopy, Key: "big", ETag: `"e"`})
	if !errors.Is(err, ErrObjectTooLarge) {
		t.Fatalf("err = %v, want ErrObjectTooLarge", err)
	}
}
```

`newTestWorkerPool` constructs a `WorkerPool` with the fake injected in place of both clients and `NewThrottler(0)`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/sync/ -run TestCopyObject -v`
Expected: FAIL — `TestCopyObject_TargetMissingIsNotAnError` reports `CopyObject called 0 times`.

- [ ] **Step 3: Fix the copy path**

In `internal/sync/worker.go`, replace the target-HEAD precheck with a **source** HEAD (needed by Task 5 anyway). Behaviour:

- `HeadObject` on `wp.sourceBucket`; `errors.As(err, &s3types.NotFound{})` → log `debug "source object vanished"` and return `nil` (Review Focus 1).
- If `ContentLength > 5<<30` → return `ErrObjectTooLarge` wrapped with the key and size.
- The target-side ETag comparison is dropped: the differ already decided this object needs copying, and `x-amz-copy-source-if-match` is not evaluated by the target, so the check bought nothing. Record this in a code comment.
- Introduce a small interface for the client so tests can inject a fake:

```go
type copyAPI interface {
	HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	CopyObject(context.Context, *s3.CopyObjectInput, ...func(*s3.Options)) (*s3.CopyObjectOutput, error)
	GetObjectTagging(context.Context, *s3.GetObjectTaggingInput, ...func(*s3.Options)) (*s3.GetObjectTaggingOutput, error)
	PutObjectTagging(context.Context, *s3.PutObjectTaggingInput, ...func(*s3.Options)) (*s3.PutObjectTaggingOutput, error)
	DeleteObject(context.Context, *s3.DeleteObjectInput, ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
}
```

`WorkerPool` keeps `client`/`sourceBucket`/`targetBucket` but the fields become `copyAPI`-typed (`src` and `tgt`), and `NewWorkerPool` keeps its current signature by assigning `client` to both. `DeleteObject` keeps using `tgt`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/sync/ -v`
Expected: PASS, including the pre-existing `TestRetryS3_*` and `TestWorkerPoolCreation`.

- [ ] **Step 5: Commit**

```bash
git add internal/sync/worker.go internal/sync/worker_test.go
git commit -m "fix(sync): copy new objects (target HEAD 404 is needs-copy, not retry)"
```

---

### Task 5: Attribute pipeline and fingerprint

**Files:**
- Create: `internal/sync/attrs.go`, `internal/sync/attrs_test.go`
- Modify: `internal/sync/worker.go`, `internal/sync/engine.go:220-235` (cache write), `internal/sync/differ.go:20-26` (`SyncAction` fields)
- Test: `internal/sync/attrs_test.go`, `internal/sync/worker_test.go`

**Interfaces:**
- Consumes: `copyAPI` (Task 4), `cache.CachedObject.AttrsHash`/`PendingAttrs` (Task 1).
- Produces:

```go
type ObjectAttrs struct {
	ContentType, ContentDisposition, ContentEncoding, CacheControl string
	Metadata      map[string]string
	StorageClass  types.StorageClass
	LockMode      types.ObjectLockMode
	RetainUntil   *time.Time
	LegalHold     types.ObjectLockLegalHoldStatus
	Tags          []types.Tag
}

func AttrsFromHead(out *s3.HeadObjectOutput) ObjectAttrs
func (a ObjectAttrs) Hash() string
func (a ObjectAttrs) copyInput(bucket, key, copySource string, storageClass string) *s3.CopyObjectInput
```

`SyncAction` gains `AttrsHash string`, `PendingAttrs bool`; `WorkerPool.copyObject` returns `(SyncAction, error)` with those set.

- [ ] **Step 1: Write the failing tests**

```go
func TestAttrsHashCanonical(t *testing.T) {
	base := ObjectAttrs{
		ContentType: "text/plain",
		Metadata:    map[string]string{"B": "2", "a": "1"},
		Tags:        []types.Tag{{Key: aws.String("z"), Value: aws.String("9")}, {Key: aws.String("a"), Value: aws.String("1")}},
	}
	shuffled := base
	shuffled.Tags = []types.Tag{base.Tags[1], base.Tags[0]}
	shuffled.Metadata = map[string]string{"a": "1", "B": "2"}
	if base.Hash() != shuffled.Hash() {
		t.Fatal("hash must be order-insensitive")
	}

	changed := base
	changed.Tags = []types.Tag{{Key: aws.String("a"), Value: aws.String("2")}}
	if base.Hash() == changed.Hash() {
		t.Fatal("hash must change when a tag value changes")
	}
}

func TestAttrsHashOmitsPastRetention(t *testing.T) {
	now := time.Now().UTC()
	past := now.Add(-time.Hour)
	future := now.Add(time.Hour)
	a := ObjectAttrs{LockMode: types.ObjectLockModeGovernance, RetainUntil: &past}
	b := ObjectAttrs{}
	if a.Hash() != b.Hash() {
		t.Fatal("an expired retention must not affect the fingerprint")
	}
	c := ObjectAttrs{LockMode: types.ObjectLockModeGovernance, RetainUntil: &future}
	if c.Hash() == b.Hash() {
		t.Fatal("a live retention must affect the fingerprint")
	}
}

func TestCopyInputUsesReplaceAndLockFields(t *testing.T) {
	now := time.Now().UTC()
	future := now.Add(48 * time.Hour)
	a := ObjectAttrs{
		ContentType: "application/json", ContentEncoding: "gzip",
		CacheControl: "max-age=60", ContentDisposition: `attachment; filename="x"`,
		Metadata:   map[string]string{"k": "v"},
		LockMode:   types.ObjectLockModeCompliance,
		RetainUntil: &future,
		LegalHold:  types.ObjectLockLegalHoldStatusOn,
		StorageClass: types.StorageClassStandardIa,
	}
	in := a.copyInput("tgt", "key", "src/key", "")
	if in.MetadataDirective != types.MetadataDirectiveReplace {
		t.Fatalf("MetadataDirective = %v, want REPLACE", in.MetadataDirective)
	}
	if aws.ToString(in.ContentType) != "application/json" || aws.ToString(in.ContentEncoding) != "gzip" {
		t.Fatal("content headers not passed")
	}
	if in.Metadata["k"] != "v" {
		t.Fatal("user metadata not passed")
	}
	if in.ObjectLockMode != types.ObjectLockModeCompliance || in.ObjectLockRetainUntilDate == nil {
		t.Fatal("lock fields not passed")
	}
	if in.ObjectLockLegalHoldStatus != types.ObjectLockLegalHoldStatusOn {
		t.Fatal("legal hold not passed")
	}
	if in.StorageClass != types.StorageClassStandardIa {
		t.Fatalf("StorageClass = %v, want source class", in.StorageClass)
	}
	if in.Tagging != nil || in.TaggingDirective != "" {
		t.Fatal("tags must never ride on CopyObject")
	}

	// An override wins over the source class.
	if got := a.copyInput("tgt", "key", "src/key", "GLACIER").StorageClass; got != types.StorageClassGlacier {
		t.Fatalf("override StorageClass = %v", got)
	}

	// Expired retention is dropped entirely.
	past := now.Add(-time.Hour)
	expired := ObjectAttrs{LockMode: types.ObjectLockModeGovernance, RetainUntil: &past}
	ein := expired.copyInput("tgt", "k", "s/k", "")
	if ein.ObjectLockMode != "" || ein.ObjectLockRetainUntilDate != nil {
		t.Fatal("expired retention must not be sent")
	}
}

func TestCopyObject_AppliesTagsAndRecordsHash(t *testing.T) {
	api := &fakeCopyAPI{
		headErr: &s3types.NotFound{},
		headOut: &s3.HeadObjectOutput{ContentType: aws.String("text/csv"), Metadata: map[string]string{"a": "b"}},
		tags:    []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
	}
	wp := newTestWorkerPool(api)
	got, err := wp.copyObject(context.Background(), SyncAction{Type: ActionCopy, Key: "k", ETag: `"e"`})
	if err != nil {
		t.Fatal(err)
	}
	if api.putTags != 1 {
		t.Fatalf("PutObjectTagging called %d times, want 1", api.putTags)
	}
	if len(api.lastTags) != 1 || aws.ToString(api.lastTags[0].Key) != "env" {
		t.Fatalf("tags not applied: %+v", api.lastTags)
	}
	if got.AttrsHash == "" || got.PendingAttrs {
		t.Fatalf("action = %+v, want hash set and pending false", got)
	}
}

func TestCopyObject_TagFailureMarksPending(t *testing.T) {
	api := &fakeCopyAPI{headErr: &s3types.NotFound{}, putTagsErr: errors.New("nope")}
	wp := newTestWorkerPool(api)
	got, err := wp.copyObject(context.Background(), SyncAction{Type: ActionCopy, Key: "k", ETag: `"e"`})
	if err != nil {
		t.Fatalf("a tag failure must not fail the copy: %v", err)
	}
	if !got.PendingAttrs {
		t.Fatal("PendingAttrs must be true after a tag failure")
	}
	if api.copied != 1 {
		t.Fatalf("bytes were copied %d times, want 1", api.copied)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/sync/ -run 'TestAttrs|TestCopyInput|TestCopyObject_AppliesTags|TestCopyObject_TagFailure' -v`
Expected: FAIL — `undefined: ObjectAttrs`.

- [ ] **Step 3: Implement `internal/sync/attrs.go`**

`Hash()` serialises a canonical string: lowercased sorted metadata `k=v` joined by `\x00`; tags sorted by key as `k=v`; then the four content headers; then lock mode + `retain_until` (RFC3339, **omitted when not in the future relative to `time.Now().UTC()`**) + legal hold; then `storage_class`. Return `hex.EncodeToString(sha256.Sum256([]byte(canonical)))`. Tag and metadata keys are lowercased before sorting so `B`/`b` cannot produce two fingerprints for the same state.

`AttrsFromHead` copies `ContentType`, `ContentDisposition`, `ContentEncoding`, `CacheControl`, `Metadata`, `StorageClass`, `ObjectLockMode`, `ObjectLockRetainUntilDate`, `ObjectLockLegalHoldStatus` off the SDK output.

`copyInput` sets `MetadataDirective: types.MetadataDirectiveReplace`, all four content headers, `Metadata`, the storage class (override if non-empty, else `a.StorageClass`, else leave zero), and the three lock fields only when `RetainUntil` is in the future. It never sets `Tagging` or `TaggingDirective`.

- [ ] **Step 4: Wire the pipeline into `copyObject`**

Sequence: source `HeadObject` → `CopyObject` via `a.copyInput(...)` (throttled) → `GetObjectTagging` on source → `PutObjectTagging` on target with `VersionId` from the copy output → return `SyncAction` with `AttrsHash = attrs.Hash()` and `PendingAttrs = false`. A tag failure sets `PendingAttrs = true` and still returns `nil` error (bytes are correct). Skip steps 2–3 entirely when `!wp.replicateAttrs`; in that case leave `AttrsHash` empty so the cache never claims knowledge it does not have (Review Focus 3).

Add `replicateAttrs bool` to `WorkerPool` and extend `NewWorkerPool` with a trailing `replicateAttrs bool` parameter; update the two production call sites (`internal/sync/engine.go:77`, `internal/sync/worker_test.go:10`).

- [ ] **Step 5: Persist the result**

In `internal/sync/engine.go` where the cache slice is built, carry `AttrsHash: a.AttrsHash` and `PendingAttrs: a.PendingAttrs`.

- [ ] **Step 6: Run tests to verify they pass**

Run: `go test ./internal/sync/ ./internal/cache/ -v && go build ./...`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add internal/sync/attrs.go internal/sync/attrs_test.go internal/sync/worker.go internal/sync/worker_test.go internal/sync/engine.go internal/sync/differ.go
git commit -m "feat(sync): replicate metadata, tags and retention on copy with attrs fingerprint"
```

---

### Task 6: Attribute-only retry and drift sweep

**Files:**
- Modify: `internal/sync/differ.go:17-26,120-160`, `internal/sync/worker.go` (add `applyAttrs`)
- Create: `internal/sync/reconcile.go`, `internal/sync/reconcile_test.go`
- Test: `internal/sync/differ_test.go`, `internal/sync/reconcile_test.go`

**Interfaces:**
- Consumes: `ObjectAttrs`, `AttrsFromHead` (Task 5), `cache.CachedObject` fields (Task 1).
- Produces: `ActionCopyAttributes` action type; `func (wp *WorkerPool) applyAttrs(ctx context.Context, a SyncAction) (SyncAction, error)`; `func ShouldReconcile(pair *config.SyncPair, now time.Time) bool`.

- [ ] **Step 1: Write the failing tests**

In `internal/sync/differ_test.go`:

```go
func TestDifferEmitsCopyAttributesForPending(t *testing.T) {
	// listing has one unchanged object; cache says its attributes are pending.
	// Expect exactly one ActionCopyAttributes and no ActionCopy.
}

func TestDifferIgnoresUnknownAttrsHash(t *testing.T) {
	// cache entry with AttrsHash == "" and PendingAttrs == false must produce
	// no actions (Review Focus 3: pre-upgrade entries are unknown, not drift).
}
```

In `internal/sync/reconcile_test.go`:

```go
func TestShouldReconcile(t *testing.T) {
	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	last := now.Add(-30 * time.Minute)
	tests := []struct {
		name string
		pair *config.SyncPair
		want bool
	}{
		{"interval zero is always off", &config.SyncPair{}, false},
		{"never reconciled", &config.SyncPair{AttrsReconcileInterval: 3600}, true},
		{"interval not elapsed", &config.SyncPair{AttrsReconcileInterval: 3600, LastReconcileAt: &last}, false},
		{"interval elapsed", &config.SyncPair{AttrsReconcileInterval: 60, LastReconcileAt: &last}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ShouldReconcile(tt.pair, now); got != tt.want {
				t.Fatalf("ShouldReconcile = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReconcileEmitsCopyAttributesOnDrift(t *testing.T) {
	// Two cache entries; the source HEAD for one returns different tags.
	// Expect exactly one ActionCopyAttributes for the drifted key.
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/sync/ -run 'TestDiffer|TestShouldReconcile|TestReconcile' -v`
Expected: FAIL — `undefined: ActionCopyAttributes`.

- [ ] **Step 3: Implement the differ case and the sweep**

`differ.go`: add `ActionCopyAttributes` to the `ActionType` const block **after** `ActionDelete` so existing numeric values are unchanged. In the equal-key branch, when `co.ETag == listing ETag && co.LastModified.Equal(...)` and `co.PendingAttrs`, emit `SyncAction{Type: ActionCopyAttributes, Key: ..., ETag: ..., Size: ..., LastModified: ...}`. Keep the existing copy branch untouched.

`reconcile.go`:

```go
func ShouldReconcile(pair *config.SyncPair, now time.Time) bool
func (e *Engine) reconcileSweep(ctx context.Context, actions chan<- SyncAction) error
```

`reconcileSweep` walks the cache via `e.store.List(pair.ID)` in key order, HEADs each source object (throttled, `errors.As(err, &s3types.NotFound{})` → skip and log), compares `AttrsFromHead(out).WithEffectiveStorageClass(pair.TargetStorageClass).Hash()` against `entry.AttrsHash`, and emits `ActionCopyAttributes` on mismatch. **`WithEffectiveStorageClass` is mandatory on both sides of the comparison.** The writer (`WorkerPool.copyObject`) hashes the *effective target* storage class, not the source's, because that is what the copy actually applies; the reader must normalize the same way or every pair with `TargetStorageClass` set will reconcile forever. An empty `entry.AttrsHash` is skipped silently.

`worker.go`: `applyAttrs` runs the same steps as `copyObject` minus the byte copy — source `HeadObject`, `GetObjectTagging`, `PutObjectTagging` — and returns the action with a fresh `AttrsHash`. `Run` dispatches `case ActionCopyAttributes: a, err = wp.applyAttrs(ctx, a)`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/sync/ -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/sync/differ.go internal/sync/differ_test.go internal/sync/reconcile.go internal/sync/reconcile_test.go internal/sync/worker.go
git commit -m "feat(sync): attribute-only retry action and optional drift sweep"
```

---

### Task 7: Engine wiring — expiry guard, pending counter, provisioning

**Files:**
- Modify: `internal/sync/engine.go:13-30` (`Progress`), `:64-80` (`NewEngine`), `:120-135` (setupOnce), `:180-250` (RunOnce body)
- Test: `internal/sync/engine_test.go` (new file if absent)

**Interfaces:**
- Consumes: `ProvisionTarget` (Task 3), `ShouldReconcile` (Task 6).
- Produces: `var ErrPairExpired = errors.New("sync pair target expiry has passed")`; `Progress.Pending int` + `func (p *Progress) AddPending()`; `func (e *Engine) PendingAttrs() int`; `NewEngine` takes `srcS3, tgtS3` and passes `pair.ReplicateAttributes` into the pool.

- [ ] **Step 1: Write the failing tests**

```go
func TestRunOnceRefusesExpiredPair(t *testing.T) {
	past := time.Now().UTC().Add(-time.Hour)
	e := newTestEngine(&config.SyncPair{TargetExpireAt: &past})
	err := e.RunOnce(context.Background())
	if !errors.Is(err, ErrPairExpired) {
		t.Fatalf("err = %v, want ErrPairExpired", err)
	}
	if _, _, status, _, _ := e.Status(); status != "expired" {
		t.Fatalf("status = %q, want expired", status)
	}
}

func TestRunOnceAllowsPairWhoseExpiryIsInTheFuture(t *testing.T) {
	future := time.Now().UTC().Add(24 * time.Hour)
	e := newTestEngine(&config.SyncPair{TargetExpireAt: &future})
	// The run itself is allowed to proceed; assert it does not return ErrPairExpired.
	if err := e.RunOnce(context.Background()); errors.Is(err, ErrPairExpired) {
		t.Fatal("future expiry must not block the run")
	}
}

func TestProgressCountsPendingAttrs(t *testing.T) {
	p := &Progress{}
	p.AddPending()
	if total, completed, failed := p.Snapshot(); completed != 1 || failed != 0 {
		t.Fatalf("pending must count as completed: %d/%d", completed, failed)
	}
}
```

`newTestEngine` builds an `Engine` with an in-memory cache store and a fake S3 client; reuse the fake from Task 3 where possible.

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/sync/ -run 'TestRunOnce|TestProgressCountsPending' -v`
Expected: FAIL — `undefined: ErrPairExpired`.

- [ ] **Step 3: Implement**

- Add `Pending int` to `Progress`, increment it in `AddPending` alongside `Completed`, and report it in `Snapshot`-adjacent accessors without changing `Snapshot`'s three-value signature (add `func (p *Progress) PendingCount() int`).
- At the top of `RunOnce`, after the running guard: `if e.pair.TargetExpireAt != nil && time.Now().UTC().After(*e.pair.TargetExpireAt) { e.setResult("expired", ...); return ErrPairExpired }`.
- Replace the `setupOnce` body with `ProvisionTarget(setupCtx, e.tgtS3, e.src.Region, e.tgt, e.pair, ProvisionOptions{Now: time.Now().UTC()})`; on error set `pair.ProvisionError` and status `error` rather than only logging, and return the error so a mis-provisioned pair never silently reports `ok`. On success clear `ProvisionError` and set `ProvisionedAt`.
- After a successful run, when `ShouldReconcile(e.pair, time.Now().UTC())`, call `e.reconcileSweep` feeding the same `actions` channel **before** the diff starts, and set `LastReconcileAt`.
- In the result loop, count `a.PendingAttrs` actions with `progress.AddPending()` instead of `progress.Add(false)`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/sync/ -v && go build ./...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/sync/engine.go internal/sync/engine_test.go
git commit -m "feat(sync): expired-pair guard, pending-attrs counter, provisioning errors surfaced"
```

---

### Task 8: CLI and provisioning on pair creation

**Files:**
- Modify: `cmd/pair.go:16-50,120-213`, `internal/sync/run.go`
- Test: `cmd/pair_test.go` (new)

**Interfaces:**
- Consumes: `ProvisionTarget` (Task 3), `ValidateExpireAt` (Task 2), `config.SyncPair` fields (Task 1).
- Produces: `func ProvisionPair(ctx context.Context, repo *config.Repository, pair *config.SyncPair, opts ProvisionOptions) error` in `internal/sync/run.go`; `func ValidatePairOptions(pair *config.SyncPair, retentionMode string, acceptCompliance, confirmExpireEarlier bool, previousExpireAt *time.Time, now time.Time) ([]AdvisoryWarning, error)` and `type AdvisoryWarning struct { Code, Message string }` in `internal/sync/provision.go`.

- [ ] **Step 1: Write the failing tests**

```go
func TestValidatePairOptions(t *testing.T) {
	past := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	earlier := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	later := time.Date(2027, 6, 1, 0, 0, 0, 0, time.UTC)
	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)

	t.Run("compliance needs explicit acceptance", func(t *testing.T) {
		_, err := ValidatePairOptions(&config.SyncPair{}, "COMPLIANCE", false, false, nil, now)
		if !errors.Is(err, ErrComplianceNeedsConfirm) {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("past expiry rejected", func(t *testing.T) {
		p := &config.SyncPair{TargetExpireAt: &past}
		_, err := ValidatePairOptions(p, "GOVERNANCE", true, true, nil, now)
		if !errors.Is(err, ErrExpireInPast) {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("moving expiry earlier needs confirmation", func(t *testing.T) {
		p := &config.SyncPair{TargetExpireAt: &earlier}
		_, err := ValidatePairOptions(p, "GOVERNANCE", true, false, &later, now)
		if !errors.Is(err, ErrExpireEarlierNeedsConfirm) {
			t.Fatalf("err = %v", err)
		}
		if _, err := ValidatePairOptions(p, "GOVERNANCE", true, true, &later, now); err != nil {
			t.Fatalf("with confirmation: %v", err)
		}
	})

	t.Run("moving expiry later needs no confirmation", func(t *testing.T) {
		p := &config.SyncPair{TargetExpireAt: &later}
		if _, err := ValidatePairOptions(p, "GOVERNANCE", true, false, &earlier, now); err != nil {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("noncurrent days without newer versions is allowed", func(t *testing.T) {
		p := &config.SyncPair{NoncurrentExpireDays: 5}
		if _, err := ValidatePairOptions(p, "GOVERNANCE", true, true, nil, now); err != nil {
			t.Fatalf("err = %v", err)
		}
	})
}

func hasCode(warns []AdvisoryWarning, code string) bool {
	for _, w := range warns {
		if w.Code == code { return true }
	}
	return false
}

func TestValidatePairOptionsWarnings(t *testing.T) {
	expire := time.Date(2027, 6, 1, 0, 0, 0, 0, time.UTC)
	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)

	t.Run("noncurrent recommended when lock is required without noncurrent expiry", func(t *testing.T) {
		p := &config.SyncPair{RequireObjectLock: true, TargetExpireAt: &expire}
		warns, err := ValidatePairOptions(p, "GOVERNANCE", true, false, nil, now)
		if err != nil { t.Fatalf("err = %v", err) }
		if !hasCode(warns, "noncurrent-recommended") { t.Fatalf("warnings = %v", warns) }
	})

	t.Run("no noncurrent warning when noncurrent expiry is set", func(t *testing.T) {
		p := &config.SyncPair{RequireObjectLock: true, TargetExpireAt: &expire, NoncurrentExpireDays: 30}
		warns, _ := ValidatePairOptions(p, "GOVERNANCE", true, false, nil, now)
		if hasCode(warns, "noncurrent-recommended") { t.Fatalf("warnings = %v", warns) }
	})

	t.Run("retention past expiry warns", func(t *testing.T) {
		p := &config.SyncPair{TargetExpireAt: &expire}
		warns, err := ValidatePairOptions(p, "COMPLIANCE", true, false, nil, now)
		if err != nil { t.Fatalf("err = %v", err) }
		if !hasCode(warns, "retention-past-expiry") { t.Fatalf("warnings = %v", warns) }
	})

	t.Run("clearing an expiry warns that the lifecycle rule survives", func(t *testing.T) {
		p := &config.SyncPair{}
		warns, err := ValidatePairOptions(p, "GOVERNANCE", true, false, &expire, now)
		if err != nil { t.Fatalf("err = %v", err) }
		if !hasCode(warns, "expiry-cleared") { t.Fatalf("warnings = %v", warns) }
	})
}
```

Also a test for the compliance rule: `ValidatePairOptions` takes a `retentionMode string` parameter; `"COMPLIANCE"` with `acceptCompliance == false` returns `ErrComplianceNeedsConfirm`. Adjust the signature to `ValidatePairOptions(pair *config.SyncPair, retentionMode string, acceptCompliance, confirmExpireEarlier bool, previousExpireAt *time.Time, now time.Time) ([]AdvisoryWarning, error)`.

**Advisory warnings (spec §7.2), added by controller ruling during Task 3 review.** `ValidatePairOptions` also returns `[]AdvisoryWarning` with three codes, none of which are errors and none of which block pair creation:

- `noncurrent-recommended` — `RequireObjectLock` is set but `NoncurrentExpireDays == 0`, so versioning-plus-lock accumulates noncurrent versions with no expiry signal.
- `retention-past-expiry` — the bucket's default retention applied today would outlast `TargetExpireAt` (`retentionMode != ""`, `RetentionDays > 0`, `now + RetentionDays*24h > *TargetExpireAt`). The message must say the retention will outlive the expiry and that lifecycle deletion skips locked versions rather than deleting them.
- `expiry-cleared` — `previousExpireAt != nil && pair.TargetExpireAt == nil`. The message must say the existing `s3sync-expire` lifecycle rule is NOT removed by this change (an empty rule set is skipped rather than written, because an empty `Rules` list is rejected as `MalformedXML`) and that the user must clear it on the bucket manually.

Both CLI commands print every warning to stderr as `warning: <message>` before proceeding. `now` is a parameter for determinism; when zero, use `time.Now().UTC()`. See the task brief for the exact test table.

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./cmd/ -run TestValidatePairOptions -v`
Expected: FAIL — `undefined: ValidatePairOptions`.

- [ ] **Step 3: Implement validation and provisioning helpers**

In `provision.go`: `ErrComplianceNeedsConfirm`, `ErrExpireEarlierNeedsConfirm`; `ValidatePairOptions` checks in order — compliance, `ValidateExpireAt` when `TargetExpireAt != nil`, then the earlier-date rule (`previousExpireAt != nil && pair.TargetExpireAt.Before(*previousExpireAt) && !confirmExpireEarlier`).

In `run.go`: `ProvisionPair` loads the target bucket, builds an `s3.Client` via `s3client.NewClient`, and calls `ProvisionTarget` with `ProvisionOptions{ForceRecreate: opts.ForceRecreate, Now: time.Now().UTC()}`.

In `cmd/pair.go`: add flags to `pairAddCmd` — `--replicate-attributes`, `--target-expire-at` (string `YYYY-MM-DD`), `--noncurrent-expire-days`, `--newer-noncurrent-versions`, `--require-object-lock`, `--on-lock-missing` (default `fail`), `--force-recreate`, `--accept-compliance`, `--attrs-reconcile-interval`. `pairAddCmd` parses the date with `time.Parse("2006-01-02", ...)`, calls `ValidatePairOptions`, then `ProvisionPair` **before** `repo.CreateSyncPair`; any error exits non-zero and no pair is written. Add the mutable subset to `pairUpdateCmd` (`--replicate-attributes`, `--attrs-reconcile-interval`, `--target-expire-at`, `--confirm-expire-earlier`) with the same validation, passing the pair's current `TargetExpireAt` as `previousExpireAt`. `pairUpdateCmd` must reject `--require-object-lock` and `--on-lock-missing` with an error stating they are immutable after creation.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./cmd/ -v && go build ./... && go vet ./...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add cmd/pair.go cmd/pair_test.go internal/sync/run.go internal/sync/provision.go
git commit -m "feat(cli): pair flags for attributes and lock, preflight before pair creation"
```

---

### Task 9: API and web UI

**Files:**
- Modify: `internal/api/handlers.go:87-96,99-115,164-207`, `internal/api/web/static/app.js`, `internal/api/web/templates/*.html`
- Test: `tests/api_test.go` (`-tags=integration`), plus a unit test for the handler validation path if one already exists for pairs.

**Interfaces:**
- Consumes: `ProvisionPair`, `ValidatePairOptions` (Task 8), `config.SyncPair` fields (Task 1), `Engine.PendingAttrs` (Task 7).
- Produces: `POST /api/sync-pairs` returns `422` with `{"error": "...", "code": "preflight_failed"}` when provisioning fails and does not persist the pair; `syncPairResponse` gains `PendingAttrs int` and `Expired bool`.

- [ ] **Step 1: Write the failing test**

In `tests/api_test.go` (integration build tag), add a test that POSTs a pair with `"target_expire_at": "2020-01-01T00:00:00Z"` and asserts status `422`, `code == "preflight_failed"`, and that `GET /api/sync-pairs` does not list it.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -tags=integration ./tests/ -run TestCreatePairRejectsPastExpiry -v`
Expected: FAIL — status 201 (the handler persists anything).

- [ ] **Step 3: Implement the handler changes**

`createSyncPair`: bind JSON, load the target bucket, call `warnings, err := ValidatePairOptions(&p, tgt.RetentionMode, in.AcceptCompliance, true, nil, time.Now().UTC())`, return the warnings in the response body as `warnings: [{code, message}]`, then call `sync.ProvisionPair`, returning `422` with `code: "preflight_failed"` on error and persisting only on success. `updateSyncPair`: extend the anonymous input struct with `ReplicateAttributes *bool`, `AttrsReconcileInterval *int`, `TargetExpireAt *string`, `ConfirmExpireEarlier *bool`; call `ValidatePairOptions` with the pair's current `TargetExpireAt` as `previousExpireAt` and `time.Now().UTC()` as `now`, returning its warnings in the response body the same way; reject any attempt to change lock fields with `400`. `enrichPair`: set `PendingAttrs` from `s.GetEngine(p.ID)` and `Expired` from `p.TargetExpireAt` being in the past.

- [ ] **Step 4: Implement the UI changes**

In the pair form template, add: a "Replicate object characteristics" checkbox, an "Expire target on" date input, "Noncurrent versions to keep" + "Noncurrent expire days" number inputs, a "Require object lock" checkbox with a `fail|recreate` select, and an "Attribute reconcile interval (seconds)" number input. In `app.js`: render `lock`, `attrs`, and `expires <date>` badges on the pair card, plus an `attrs_pending` pill when `progress.pending > 0`; add confirmations — recreate requires typing the bucket name, COMPLIANCE requires a checkbox, and moving the expiry earlier shows a confirm naming both dates.

- [ ] **Step 5: Verify manually**

Run: `go build ./... && go test ./...`
Then start `./s3sync serve`, create a pair with lock + expiry against the Cubbit endpoint, and confirm: the dashboard shows the badges, the pair reports `provisioned_at`, and `pair sync` copies an object with tags visible via `aws s3api get-object-tagging`.

- [ ] **Step 6: Commit**

```bash
git add internal/api tests/api_test.go
git commit -m "feat(api,ui): expose attribute replication and lock provisioning"
```

---

## Self-Review

**Spec coverage:** §5 data model → Task 1. §6.1 pipeline → Task 5. §6.2 action type → Task 6. §6.3 fingerprint → Task 5. §6.4 non-goals → enforced by absence (no ACL/SSE code paths added). §7.1 preflight → Task 3. §7.2 lifecycle → Task 2. §7.3 replacement guard → Task 3 step 6. §7.4 resurrection → Task 7. §8.2 drift → Task 6. §9 P0 → Task 4. §10 CLI/API/UI → Tasks 8–9. §11 testing → distributed across tasks; migration test in Task 1 step 5.

**Type consistency:** `ProvisionTarget`'s signature is fixed in Task 3 and used identically in Tasks 3, 7, 8. `ValidatePairOptions`'s final signature (with `retentionMode`) is pinned in Task 8 and consumed in Task 9. `NewWorkerPool`'s trailing `replicateAttrs bool` is added in Task 5 and its two call sites named there. `SyncAction.AttrsHash`/`PendingAttrs` are introduced in Task 5 and read in Task 6.

**Gaps found and fixed:** the original sketch had no `LastReconcileAt` column — added in Task 1. `Progress.Snapshot`'s three-value signature is preserved by adding `PendingCount` rather than a fourth return. The compliance check initially sat inside `ProvisionTarget`, which would have made serve-mode re-provisioning fail; it moved to `ValidatePairOptions`.
