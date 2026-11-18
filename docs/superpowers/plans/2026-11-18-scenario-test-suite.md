# Scenario Test Suite Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove attribute replication and object-locked, expiring targets behave correctly across realistic whole-bucket sync scenarios against a real S3 endpoint.

**Architecture:** A new `tests/scenario` package drives the real `Engine`, `WorkerPool`, `differ`, and `ProvisionTarget` in-process against a live endpoint, using deterministic synthetic bytes so any object — including a 6 GiB one — is verifiable from a few hundred KB of range reads. Each scenario owns its buckets, cache directory, and pair, and asserts outcome (counts, metadata, tags, retention, lifecycle) plus idempotence (a second sync copies zero objects).

**Tech Stack:** Go 1.x, aws-sdk-go-v2 (`service/s3`), bbolt (via `internal/cache`), `testing` subtests, `//go:build integration`.

**Spec:** `docs/superpowers/specs/2026-11-18-scenario-test-suite-design.md`

## Global Constraints

- Package path is `tests/scenario`; every file except `synth.go`/`synth_test.go` carries `//go:build integration`.
- `synth.go` and `synth_test.go` are **untagged**: they import only the standard library, so the deterministic-data core gets fast unit tests in the default `go test ./...` run.
- Every scenario skips (never fails) when `S3_TEST_ENDPOINT` is unset, matching `tests/sync_test.go:20-31`.
- Credentials come from `S3_TEST_ACCESS_KEY` / `S3_TEST_SECRET_KEY`; never written to a file, never committed, never logged.
- **Object lock retention is 1 day maximum** for GOVERNANCE; COMPLIANCE is capped at **2 minutes** because it cannot be bypassed (`x-amz-bypass-governance-retention: true` returns 403) and cleanup would otherwise block for a day.
- Only `STANDARD` is a valid storage class on this endpoint; every other value returns 400 `InvalidStorageClass`.
- `UploadPartCopy` requires an explicit closed range (`bytes=a-b`); `bytes=a-` returns `InvalidRange`.
- `CompleteMultipartUpload` requires **quoted** ETags.
- `Expiration.Date` must be midnight UTC and in the future (`internal/sync/lifecycle.go:22-26`).
- Part size for composed objects is 64 MiB, so 6 GiB is 96 parts (limit is 10,000).
- No scenario may leave a bucket, a locked version, or an incomplete multipart upload behind; `destroyBucket` verifies `HeadBucket` returns 404.
- Timings are logged, never asserted.
- `go test ./...` and `go vet ./...` must stay clean; the two pre-existing copylocks warnings (`internal/sync/engine.go:419`, `internal/api/handlers.go:455`) are the only permitted vet output.

## Review Focus

Each line names an input the spec implies but does not test, and the task that pins it.

1. **A source object deleted between listing and HEAD.** The engine must skip it and report neither success nor failure — never retry three times, never cache it. Pinned in Task 3 (`SourceVanishesMidRun`).
2. **Object keys that stress path-style URLs and the copy-source header** — spaces, `+`, `%`, unicode, a leading `/`, and a key that is a prefix of another. A copy of `a/b` must not read `a/bc`. Pinned in Task 3 (`AwkwardKeys`).
3. **Zero-byte and one-byte objects.** Size 0 must never take the multipart path, must round-trip as 0 bytes, and must not be dropped by the differ. Pinned in Task 3 (`Tiny`) and Task 4 (`ZeroByteNeverMultipart`).
4. **A leftover bucket from an earlier failed run.** The harness must fail loudly with the bucket name rather than reusing stale contents, or a green run can be built on garbage. Pinned in Task 1 (`TestNewScenarioEnvRefusesExistingBucket`).
5. **Clock skew between the local process and the endpoint** for retain-until dates computed locally. A retain-until a few seconds in the past must not be treated as live. Pinned in Task 6 (`RetentionJustExpired`).

---

## File Structure

| File | Responsibility |
| --- | --- |
| `tests/scenario/synth.go` | Deterministic byte generation and object shapes. No network, no build tag. |
| `tests/scenario/synth_test.go` | Determinism and range-consistency unit tests. No build tag. |
| `tests/scenario/harness.go` | `scenarioEnv`, bucket factory, `destroyBucket`, `sync`, `syncExpectingError`, assertion helpers. |
| `tests/scenario/counting.go` | `countingTransport` — an `http.RoundTripper` that counts S3 request kinds. |
| `tests/scenario/large.go` | `composeLarge` — builds an arbitrarily large object from one seeded chunk. |
| `tests/scenario/scenario_small_test.go` | Small/tiny/many-object scenarios. |
| `tests/scenario/scenario_large_test.go` | Large-object and multipart scenarios. |
| `tests/scenario/scenario_versioning_test.go` | Versioning and delete-marker scenarios. |
| `tests/scenario/scenario_lock_test.go` | Object lock and provisioning scenarios. |
| `tests/scenario/scenario_attrs_test.go` | Attribute replication and drift scenarios. |
| `tests/scenario/scenario_expiry_test.go` | Expiry and lifecycle scenarios. |
| `tests/scenario/scenario_matrix_test.go` | Feature combinations — the seams. |
| `tests/scenario/scenario_cli_test.go` | One binary-level CLI smoke test. |
| `tests/run-scenarios.sh` | Cross-compile, ship, run, summarise. |

`setupS3Client` in `tests/sync_test.go:16` is unexported in package `tests` and cannot be imported by `tests/scenario`. Duplicate the ~25 lines as `newClient(t *testing.T) (*s3.Client, string)` in `harness.go` rather than restructuring the existing package — the existing file is stable and the duplication is bounded.

---

### Task 1: Harness foundation — client, bucket factory, bulletproof teardown

**Files:**
- Create: `tests/scenario/harness.go`
- Test: `tests/scenario/harness_test.go`

**Interfaces:**
- Consumes: `internal/config` (`Bucket`, `SyncPair`), `internal/cache` (`Open`, `Store`), `internal/s3client` (`NewClient`).
- Produces:
  - `func newClient(t *testing.T) (*s3.Client, string)` — returns a path-style client and the configured endpoint; skips when unset.
  - `func createBucket(t *testing.T, client *s3.Client, objectLock bool) string` — creates a uniquely named scratch bucket, registers teardown via `destroyBucket`, returns the name.
  - `func destroyBucket(t *testing.T, client *s3.Client, name string)` — paginates all versions **and delete markers**, deletes each (retrying with `x-amz-bypass-governance-retention: true`), aborts incomplete multipart uploads, deletes the bucket, then verifies `HeadBucket` → 404.
  - `type envOptions struct { ObjectLock, Versioning, ReplicateAttrs bool; TargetExpireIn time.Duration; RetentionMode string; Retention time.Duration; WorkerCount int; StorageClass string; AttrsReconcileInterval int }`
  - `type scenarioEnv struct { T *testing.T; Client *s3.Client; Source, Target string; Pair *config.SyncPair; CacheDir string; Store *cache.Store; Counter *countingTransport; literalDigest map[string][32]byte; largeSeed map[string]string }`
  - `func newScenarioEnv(t *testing.T, opts envOptions) *scenarioEnv`

- [ ] **Step 1: Write the failing test**

```go
// harness_test.go
//go:build integration

func TestDestroyBucketRemovesVersionsAndDeleteMarkers(t *testing.T) {
	client, _ := newClient(t)
	name := createBucket(t, client, true)
	ctx := context.Background()

	// Three versions of one key, one delete marker, one locked object.
	for i := 0; i < 3; i++ {
		putBody(t, client, name, "multi.txt", fmt.Sprintf("v%d", i))
	}
	_, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(name), Key: aws.String("multi.txt"),
	})
	if err != nil {
		t.Fatalf("delete marker: %v", err)
	}
	putBody(t, client, name, "locked.txt", "held", func(in *s3.PutObjectInput) {
		in.ObjectLockMode = types.ObjectLockModeGovernance
		in.ObjectLockRetainUntilDate = aws.Time(time.Now().Add(24 * time.Hour))
	})

	// The scratch bucket was registered with t.Cleanup, so destroy it by hand
	// here and then assert on the result rather than waiting for cleanup.
	destroyBucket(t, client, name)

	_, err = client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(name)})
	if !isNoSuchBucket(err) {
		t.Fatalf("bucket %q still exists after destroyBucket: %v", name, err)
	}
}

func TestNewScenarioEnvRefusesExistingBucket(t *testing.T) {
	client, _ := newClient(t)
	name := createBucket(t, client, false)
	putBody(t, client, name, "stale.txt", "left over")

	err := newScenarioEnvWithBuckets(t, client, name, "").Err
	if err == nil {
		t.Fatal("newScenarioEnv accepted a source bucket that already holds objects")
	}
	if !strings.Contains(err.Error(), name) {
		t.Fatalf("error does not name the offending bucket %q: %v", name, err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -tags=integration ./tests/scenario/ -run 'TestDestroyBucket|TestNewScenarioEnv' -v`
Expected: FAIL — `undefined: newClient`, `undefined: destroyBucket`, `undefined: newScenarioEnvWithBuckets`.

- [ ] **Step 3: Implement the harness**

`newClient` mirrors `tests/sync_test.go:16-45` with a 5-minute HTTP timeout (multipart copies of large objects exceed the 30s used by `internal/s3client/client.go:22`, so the scenario client needs its own).

`createBucket` names buckets `s3sync-sc-<unixnano>` and passes `ObjectLockEnabledForBucket` when `objectLock` is true.

`destroyBucket` is the piece that must not be naive. Algorithm:

```go
// Paginate over ListObjectVersions, not ListObjectsV2: a versioned bucket
// cannot be deleted by key alone, and a delete marker holds a VersionId that
// must itself be deleted before the bucket empties.
var keyMarker, versionMarker *string
for {
	out, err := client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
		Bucket: aws.String(name), KeyMarker: keyMarker, VersionIdMarker: versionMarker,
	})
	if isNoSuchBucket(err) { return }
	if err != nil { t.Fatalf(...) }
	for _, v := range out.Versions       { delVersion(v.Key, v.VersionId) }
	for _, d := range out.DeleteMarkers  { delVersion(d.Key, d.VersionId) }
	if !aws.ToBool(out.IsTruncated) { break }
	keyMarker, versionMarker = out.NextKeyMarker, out.NextVersionIdMarker
}
```

`delVersion` calls `DeleteObject` with `VersionId`; on failure it retries once with `BypassGovernanceRetention: aws.Bool(true)`; if that also fails the version is COMPLIANCE-held and the helper `t.Fatalf`s naming the key, the version, and the retain-until date it read from `HeadObject` — hanging is not an acceptable outcome.

Then: abort every entry from `ListMultipartUploads` (`ListMultipartUploadsInput`, paginate on `KeyMarker`/`UploadIdMarker`), `DeleteBucket`, and verify with `HeadBucket` retrying up to 5 times with 200ms backoff for eventual consistency; if it still exists, `t.Fatalf`.

`isNoSuchBucket(err error) bool` matches code `NoSuchBucket` or a bare 404, mirroring `internal/sync/provision.go:258-269`'s `isNotFound` logic (which is unexported and in another module path).

`putBody(t, client, bucket, key, body string, mutate ...func(*s3.PutObjectInput))` is a local helper; tags go through `PutObjectTagging` after the put, because **`x-amz-tagging` is ignored on `PutObject` as well as on `CopyObject`** on this endpoint.

`newScenarioEnv` calls `newScenarioEnvWithBuckets` after creating fresh buckets, and `newScenarioEnvWithBuckets` is the testable core: it HEADs the source, and if the source holds any object it returns an error naming the bucket (Review Focus 4). It opens `cache.Open(filepath.Join(t.TempDir(), "cache.bolt"))`, sets `t.Cleanup` to close it, and builds the `SyncPair` from `envOptions` with `OnLockMissing: "fail"`.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -tags=integration ./tests/scenario/ -run 'TestDestroyBucket|TestNewScenarioEnv' -v`
Expected: PASS, both tests, ~5s.

- [ ] **Step 5: Commit**

```bash
git add tests/scenario/harness.go tests/scenario/harness_test.go
git commit -m "test(scenario): harness with version-aware bucket teardown"
```

---

### Task 2: Deterministic synthetic data and the counting transport

**Files:**
- Create: `tests/scenario/synth.go`, `tests/scenario/counting.go`
- Test: `tests/scenario/synth_test.go`

**Interfaces:**
- Consumes: standard library only (`synth.go`); `net/http` (`counting.go`).
- Produces:
  - `func synthBytesAt(key string, off, n int64) []byte`
  - `func newSynthReader(key string, size int64) io.Reader`
  - `func synthChecksum(key string, size int64) [32]byte`
  - `func synthKey(i int, shape string) string` — e.g. `synthKey(42, "small")` → `"obj/0042-small.bin"`.
  - `type objectShape struct { Name string; Min, Max int64 }` and `var shapes = []objectShape{...}`
  - `type countingTransport struct { base http.RoundTripper; mu sync.Mutex; counts map[string]int }` with `func newCountingTransport(base http.RoundTripper) *countingTransport`, `func (c *countingTransport) Count(kind string) int`, `func (c *countingTransport) Reset()`, and kind constants `kindCopyObject`, `kindUploadPartCopy`, `kindPutObject`, `kindPutTagging`, `kindGetTagging`, `kindCreateMPU`, `kindCompleteMPU`.

- [ ] **Step 1: Write the failing test**

```go
// synth_test.go — NO build tag; runs in the default suite.
package scenario

func TestSynthBytesAreDeterministicAndRangeConsistent(t *testing.T) {
	a := synthBytesAt("obj/0001-small.bin", 0, 64)
	b := synthBytesAt("obj/0001-small.bin", 0, 64)
	if !bytes.Equal(a, b) {
		t.Fatal("the same key and offset produced different bytes")
	}
	// A window read must equal the same window of a larger read — this is the
	// property that lets a 6 GiB object be verified from a few hundred KB.
	full := synthBytesAt("obj/0001-small.bin", 0, 4096)
	if !bytes.Equal(synthBytesAt("obj/0001-small.bin", 1000, 64), full[1000:1064]) {
		t.Fatal("a range read disagrees with the same window of a full read")
	}
}

func TestSynthBytesDifferPerKey(t *testing.T) {
	if bytes.Equal(synthBytesAt("a", 0, 64), synthBytesAt("b", 0, 64)) {
		t.Fatal("two different keys produced identical bytes")
	}
}

func TestSynthChecksumMatchesReader(t *testing.T) {
	const size = 1 << 20
	got := synthChecksum("obj/0002-medium.bin", size)
	want := sha256.Sum256(readAll(t, newSynthReader("obj/0002-medium.bin", size)))
	if got != want {
		t.Fatal("streamed checksum disagrees with the reader")
	}
}

func TestCountingTransportClassifiesCopy(t *testing.T) {
	c := newCountingTransport(roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(""))}, nil
	}))
	req, _ := http.NewRequest("PUT", "https://s3.example/t/b/k?partNumber=3&uploadId=x", nil)
	req.Header.Set("X-Amz-Copy-Source", "t/b/src")
	_, _ = c.RoundTrip(req)
	if c.Count(kindUploadPartCopy) != 1 {
		t.Fatalf("upload part copy count = %d, want 1", c.Count(kindUploadPartCopy))
	}
	if c.Count(kindCopyObject) != 0 {
		t.Fatalf("plain copy count = %d, want 0", c.Count(kindCopyObject))
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./tests/scenario/ -v -run TestSynth`
Expected: FAIL — `undefined: synthBytesAt`, `undefined: newCountingTransport`.

- [ ] **Step 3: Implement**

`synth.go`: bytes derive from `splitmix64(seed ^ uint64(offset))`, where `seed` is `fnv64a(key)`. `splitmix64` is six lines of shifts and multiplies; it needs no state, no seeding, and is identical on every platform and Go version (unlike `math/rand`, whose sequence is only stable per version). One `uint64` yields 8 bytes, so a 64-byte window is 8 mixes.

`newSynthReader` returns an `io.Reader` yielding exactly `size` bytes; it must be safe to read sequentially in any chunk size (the S3 SDK decides chunking, and multipart parts read it independently).

`synthChecksum` streams the reader through `sha256`.

`synthKey(i int, shape string)` returns `fmt.Sprintf("obj/%04d-%s.bin", i, shape)`.

`shapes` covers the spec's table: `{"tiny", 0, 1}`, `{"small", 1 << 10, 64 << 10}`, `{"medium", 1 << 20, 8 << 20}`, `{"large", 100 << 20, 100 << 20}`, `{"huge", 6 << 30, 6 << 30}`. Size within a shape is derived from the index so it is reproducible: `Min + int64(fnv64a(key))%(Max-Min+1)`.

`counting.go`: `RoundTrip` classifies by method, the `X-Amz-Copy-Source` header, and query parameters (`partNumber`, `tagging`, `uploads`, `uploadId`), then delegates to `base`. Classification order matters: a `PUT` with `X-Amz-Copy-Source` and `partNumber` is `kindUploadPartCopy`; with the header but no `partNumber` it is `kindCopyObject`. `objectShape`/`shapes` live in `synth.go` (untagged) so the size table is unit-testable.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./tests/scenario/ -v -run 'TestSynth|TestCounting'`
Expected: PASS, no endpoint needed — these run in the default suite.

- [ ] **Step 5: Commit**

```bash
git add tests/scenario/synth.go tests/scenario/synth_test.go tests/scenario/counting.go
git commit -m "test(scenario): deterministic synthetic data and request counting"
```

---

### Task 3: Sync runner, assertions, and the small-object scenarios

**Files:**
- Modify: `tests/scenario/harness.go` (add runner and assertions)
- Create: `tests/scenario/scenario_small_test.go`

**Interfaces:**
- Consumes: Task 1's `scenarioEnv`, Task 2's `synthBytesAt`/`synthChecksum`/`countingTransport`.
- Produces:
  - `func (e *scenarioEnv) sync() *sync.Progress` — one `RunOnce`, `t.Fatalf` on error, returns the progress snapshot.
  - `func (e *scenarioEnv) syncExpectingError() error` — one `RunOnce`, returns the error without failing.
  - `func (e *scenarioEnv) assertNoWorkOnSecondSync()` — resets the counter, syncs, requires `Count(kindCopyObject)+Count(kindUploadPartCopy)+Count(kindPutObject) == 0`.
  - `func (e *scenarioEnv) targetKeys() []string` — full paginated listing of the target.
  - `func (e *scenarioEnv) assertTargetCount(want int)`
  - `func (e *scenarioEnv) assertBytesMatch(key string)` — HEAD both sides, compare size and `sha256` of the target body against `synthChecksum(key, size)`.
  - `type objectFacts struct { ContentType string; Metadata map[string]string; Tags map[string]string; RetentionMode string; RetainUntil *time.Time; LegalHold string }`
  - `func fetchFacts(t *testing.T, client *s3.Client, bucket, key string) objectFacts`
  - `func (e *scenarioEnv) assertFactsMatch(key string)`
  - `func (e *scenarioEnv) seed(keys []string)`
  - `func listAll(t *testing.T, client *s3.Client, bucket string) []string` — paginated `ListObjectsV2`, sorted keys.
  - `func contains(ss []string, want string) bool`

The engine needs no repository: `NewEngine` (`internal/sync/engine.go:126-128`) takes only the pair, both buckets, both clients, a store, and a settings function. `scenarioEnv.sync` builds it as
`sync.NewEngine(e.Pair, e.srcBucket, e.tgtBucket, e.srcClient, e.tgtClient, e.Store, func() config.Settings { return config.Settings{RunTimeoutSeconds: 300} })`.

- [ ] **Step 1: Write the failing tests**

```go
// scenario_small_test.go
//go:build integration

func TestScenarioEmptySource(t *testing.T) {
	e := newScenarioEnv(t, envOptions{})
	e.sync()
	e.assertTargetCount(0)
}

func TestScenarioTiny(t *testing.T) {
	e := newScenarioEnv(t, envOptions{})
	e.seed([]string{"obj/zero.bin", "obj/one.bin"})
	e.sync()
	e.assertTargetCount(2)
	e.assertBytesMatch("obj/zero.bin")
	e.assertBytesMatch("obj/one.bin")
}

func TestScenarioSingleObject(t *testing.T) {
	e := newScenarioEnv(t, envOptions{})
	e.seed([]string{"obj/0001-small.bin"})
	e.sync()
	e.assertBytesMatch("obj/0001-small.bin")
	e.assertNoWorkOnSecondSync()
}

func TestScenarioManySmall(t *testing.T) {
	if testing.Short() { t.Skip("5,000 objects") }
	e := newScenarioEnv(t, envOptions{WorkerCount: 8})
	keys := manyKeys(5000)
	e.seed(keys)
	e.sync()
	e.assertTargetCount(5000)
	for _, k := range []string{keys[0], keys[1234], keys[4999]} {
		e.assertBytesMatch(k)
	}
	e.assertNoWorkOnSecondSync()
}

func TestScenarioManySmallWithDeletes(t *testing.T) {
	e := newScenarioEnv(t, envOptions{})
	e.Pair.DeletePropagation = true
	keys := manyKeys(200)
	e.seed(keys)
	e.sync()
	deleteFromSource(t, e.Client, e.Source, keys[10:20])
	e.sync()
	e.assertTargetCount(190)
	e.assertNoWorkOnSecondSync()
}

func TestScenarioSourceChanges(t *testing.T) {
	e := newScenarioEnv(t, envOptions{})
	keys := manyKeys(100)
	e.seed(keys)
	e.sync()
	for _, k := range keys[:10] {
		putBody(t, e.Client, e.Source, k, "changed")
	}
	e.Counter.Reset()
	e.sync()
	if got := e.Counter.Count(kindCopyObject); got != 10 {
		t.Fatalf("copied %d objects after changing 10, want 10", got)
	}
}

func TestScenarioSourceVanishesMidRun(t *testing.T) {
	// Review Focus 1: a key listed but gone by the time it is HEADed must be
	// skipped, never retried into a failure and never cached.
	e := newScenarioEnv(t, envOptions{})
	e.seed([]string{"obj/keep.bin", "obj/gone.bin"})
	deleteFromSource(t, e.Client, e.Source, []string{"obj/gone.bin"})
	// Prime the cache as if a previous run had listed it.
	e.seedCacheOnly([]string{"obj/gone.bin"})
	e.sync()
	e.assertTargetCount(1)
	if _, ok := e.cachedKey("obj/gone.bin"); ok {
		t.Fatal("a vanished object was cached, so it will never be retried")
	}
}

func TestScenarioSourceDeletesThenRestores(t *testing.T) {
	// A versioned source: delete the key (marker), then re-upload identical
	// bytes. ETag and LastModified both come back unchanged, so StreamingDiff
	// never re-emits a copy — the vanished-object path must not leave the key
	// permanently absent from the target.
	e := newScenarioEnv(t, envOptions{Versioning: true})
	e.Pair.DeletePropagation = true
	putBody(t, e.Client, e.Source, "flip.txt", "stable-body")
	e.sync()
	if keys := listAll(t, e.Client, e.Target); len(keys) != 1 {
		t.Fatalf("target after first sync = %v, want [flip.txt]", keys)
	}
	// Delete the current version on the source, leaving only a marker, then
	// restore the identical body.
	versions, err := e.Client.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(e.Source), Prefix: aws.String("flip.txt"),
	})
	if err != nil { t.Fatalf("list source versions: %v", err) }
	for _, v := range versions.Versions {
		if _, err := e.Client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
			Bucket: aws.String(e.Source), Key: aws.String("flip.txt"), VersionId: v.VersionId,
		}); err != nil {
			t.Fatalf("delete source version: %v", err)
		}
	}
	putBody(t, e.Client, e.Source, "flip.txt", "stable-body")
	e.sync()
	// Whatever the intermediate state, the key must exist in the target at the end.
	if keys := listAll(t, e.Client, e.Target); len(keys) != 1 || keys[0] != "flip.txt" {
		t.Fatalf("target after delete-then-restore = %v, want [flip.txt]", keys)
	}
}

func TestScenarioAwkwardKeys(t *testing.T) {
	// Review Focus 2.
	e := newScenarioEnv(t, envOptions{})
	keys := []string{
		"a/b", "a/bc", "with space.txt", "plus+sign.txt", "percent%20.txt",
		"unicode-é.txt", "/leading-slash.txt", "deep/a/b/c/d.txt",
	}
	e.seed(keys)
	e.sync()
	e.assertTargetCount(len(keys))
	for _, k := range keys {
		e.assertBytesMatch(k)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test -tags=integration ./tests/scenario/ -run TestScenario -v`
Expected: FAIL — `e.sync undefined`, `e.Counter undefined`, `undefined: manyKeys`.

- [ ] **Step 3: Implement the runner and assertions**

`sync()` builds the engine, wraps both clients' transport with the same `countingTransport` (constructed once in `newScenarioEnv` and stored as `e.Counter`), calls `RunOnce(context.Background())`, and on error calls `t.Fatalf("sync: %v", err)`.

`assertNoWorkOnSecondSync()` is the idempotence assertion and the reason this suite exists: it resets `e.Counter`, syncs, and fails if **any** copy was issued. Counting requests is deliberately stronger than reading `Engine.LastResult()` — a copy that happens and is then cached reports success, which is exactly the shape of the dry-run-poisoning and stale-fingerprint bugs.

`assertBytesMatch(key)` HEADs the target for size, then `GetObject`s and streams through `sha256`, comparing to `synthChecksum(key, size)`. For keys seeded with literal bodies (`putBody`), it compares against `sha256` of the literal instead — the helper takes the expected digest from a map `e.literalDigest[key]` populated by `seed`/`putBody`.

`fetchFacts` HEADs and `GetObjectTagging`s one object into `objectFacts`. `assertFactsMatch(key)` compares source facts to target facts field by field, with messages naming the key and field: `key %q: metadata %q = %q, want %q`.

`seed(keys)` generates each object with `newSynthReader(key, sizeFor(key))` and a `ContentType` derived from the shape, and records the digest.

`manyKeys(n)` returns `synthKey(i, "small")` for `i < n`.

`seedCacheOnly(keys)` writes `CachedObject` rows directly via `e.Store.PutMany` with the source's real ETag and LastModified, so the differ sees them as previously synced. It is used only by `SourceVanishesMidRun`, where the listing must produce an `ActionCopy` for a key that no longer exists.

`deleteFromSource` deletes keys from the source bucket. `listAll` paginates `ListObjectsV2` on `ContinuationToken` and returns sorted keys — several scenarios list a 500-object target, so it must not stop at the first page. `contains` is a plain membership test used by the lifecycle assertions in Tasks 5, 8 and 9.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -tags=integration ./tests/scenario/ -run TestScenario -v`
Expected: PASS. `TestScenarioManySmall` takes 2–4 minutes; every other test is seconds.

- [ ] **Step 5: Commit**

```bash
git add tests/scenario/harness.go tests/scenario/scenario_small_test.go
git commit -m "test(scenario): sync runner, assertions, and small-object scenarios"
```

---

### Task 4: Large objects via server-side composition

**Files:**
- Create: `tests/scenario/large.go`, `tests/scenario/scenario_large_test.go`

**Interfaces:**
- Consumes: Task 1's harness, Task 2's `synthBytesAt`.
- Produces:
  - `func composeLarge(t *testing.T, client *s3.Client, bucket, key string, size int64, mutate func(*s3.CreateMultipartUploadInput))`
  - `func (e *scenarioEnv) assertRangeMatches(key string, offsets ...int64)` — for each offset, `GetObject` with `Range: bytes=<off>-<off+65535>` and compare against `synthBytesAt(key, off, n)`.
  - `func (e *scenarioEnv) startSyncInBackground(ctx context.Context)` — runs one `RunOnce` on a goroutine, recording its error for `waitForSync`.
  - `func (e *scenarioEnv) waitForSync() error` — blocks until the background sync returns and yields its error.
  - `func tier() string`, `const tierFast, tierFull, tierAll = "fast", "full", "all"`

- [ ] **Step 1: Write the failing tests**

```go
// scenario_large_test.go
//go:build integration

func TestScenarioFiveGiBMinusOne(t *testing.T) {
	if tier() == tierFast { t.Skip("large object") }
	e := newScenarioEnv(t, envOptions{})
	composeLarge(t, e.Client, e.Source, "big/below.bin", (5<<30)-1, nil)
	e.sync()
	e.assertTargetCount(1)
	e.assertRangeMatches("big/below.bin", 0, 1<<30, (5<<30)-65536)
	if got := e.Counter.Count(kindUploadPartCopy); got != 0 {
		t.Fatalf("an object under the single-copy limit used %d multipart part copies", got)
	}
}

func TestScenarioFiveGiBPlusOne(t *testing.T) {
	if tier() == tierFast { t.Skip("large object") }
	e := newScenarioEnv(t, envOptions{})
	composeLarge(t, e.Client, e.Source, "big/above.bin", (5<<30)+1, nil)
	e.sync()
	e.assertRangeMatches("big/above.bin", 0, 5<<30, (5<<30)+1-65536)
	if got := e.Counter.Count(kindUploadPartCopy); got == 0 {
		t.Fatal("an object over the single-copy limit did not use multipart")
	}
}

func TestScenarioSixGiB(t *testing.T) {
	if tier() != tierAll { t.Skip("6 GiB; tier=all only") }
	e := newScenarioEnv(t, envOptions{})
	composeLarge(t, e.Client, e.Source, "big/six.bin", 6<<30, nil)
	e.sync()
	e.assertTargetCount(1)
	// Verified by range: the object cannot be downloaded on a 5 GB disk.
	e.assertRangeMatches("big/six.bin", 0, 3<<30, (6<<30)-65536)
	if e.Counter.Count(kindUploadPartCopy) == 0 {
		t.Fatal("a 6 GiB object did not use multipart")
	}
}

// TestScenarioLargeWithAttrs pins that the characteristics of a >5 GiB object
// travel on CreateMultipartUpload rather than on any copy call.
func TestScenarioLargeWithAttrs(t *testing.T) {
	if tier() == tierFast { t.Skip("large object") }
	e := newScenarioEnv(t, envOptions{ReplicateAttrs: true})
	composeLarge(t, e.Client, e.Source, "big/meta.bin", 6<<30, func(in *s3.CreateMultipartUploadInput) {
		in.ContentType = aws.String("application/x-large")
		in.Metadata = map[string]string{"origin": "composed"}
	})
	putTags(t, e.Client, e.Source, "big/meta.bin", map[string]string{"size": "huge"})
	e.sync()
	f := fetchFacts(t, e.Client, e.Target, "big/meta.bin")
	if f.ContentType != "application/x-large" || f.Metadata["origin"] != "composed" || f.Tags["size"] != "huge" {
		t.Fatalf("target facts = %+v, want content-type, metadata and tag all replicated", f)
	}
}

func TestScenarioLargeSecondSyncIsNoop(t *testing.T) {
	if tier() == tierFast { t.Skip("large object") }
	e := newScenarioEnv(t, envOptions{ReplicateAttrs: true, WorkerCount: 4})
	composeLarge(t, e.Client, e.Source, "big/attrs.bin", 6<<30, nil)
	e.sync()
	e.assertNoWorkOnSecondSync()
}

func TestScenarioLargeCancelMidCopy(t *testing.T) {
	if tier() != tierAll { t.Skip("cancellation case; tier=all only") }
	e := newScenarioEnv(t, envOptions{})
	composeLarge(t, e.Client, e.Source, "big/cancel.bin", 6<<30, nil)

	ctx, cancel := context.WithCancel(context.Background())
	e.startSyncInBackground(ctx)
	time.Sleep(2 * time.Second) // let the multipart upload open
	cancel()
	e.waitForSync()

	// A cancelled copy must abort its upload, or the orphaned parts bill forever
	// (this endpoint supports no AbortIncompleteMultipartUpload lifecycle rule).
	ups, err := e.Client.ListMultipartUploads(context.Background(), &s3.ListMultipartUploadsInput{
		Bucket: aws.String(e.Target),
	})
	if err != nil { t.Fatalf("list target uploads: %v", err) }
	if len(ups.Uploads) != 0 {
		t.Fatalf("%d incomplete uploads left on the target after cancellation", len(ups.Uploads))
	}
}

func TestScenarioZeroByteNeverMultipart(t *testing.T) {
	// Review Focus 3.
	e := newScenarioEnv(t, envOptions{})
	e.seed([]string{"obj/zero.bin"})
	e.sync()
	e.assertBytesMatch("obj/zero.bin")
	if e.Counter.Count(kindCreateMPU) != 0 {
		t.Fatal("a zero-byte object opened a multipart upload")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test -tags=integration ./tests/scenario/ -run 'TestScenarioFiveGiB|TestScenarioLarge|TestScenarioZeroByte' -v`
Expected: FAIL — `undefined: composeLarge`, `e.assertRangeMatches undefined`, `undefined: tier`.

- [ ] **Step 3: Implement `composeLarge`**

Upload one 64 MiB seeded chunk to `<bucket>/_seed/chunk.bin` (skip if present), then:

```go
// Parts are 64 MiB so a 6 GiB object is 96 parts — far under the 10,000 limit
// — and every range is closed: this endpoint rejects "bytes=a-" with
// InvalidRange, so an open-ended range is not an option.
const partSize = 64 << 20
numParts := int32((size + partSize - 1) / partSize)
create := &s3.CreateMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key)}
if mutate != nil { mutate(create) }   // storage class, lock headers
up, err := client.CreateMultipartUpload(ctx, create)
parts := make([]types.CompletedPart, 0, numParts)
for i := int32(0); i < numParts; i++ {
	start := int64(i) * partSize
	end := start + partSize - 1
	if end > size-1 { end = size - 1 }
	out, err := client.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
		UploadId: up.UploadId, PartNumber: aws.Int32(i + 1),
		CopySource:      aws.String(bucket + "/_seed/chunk.bin"),
		CopySourceRange: aws.String(fmt.Sprintf("bytes=%d-%d", start%chunkSize, (start%chunkSize)+end-start)),
	})
	parts = append(parts, types.CompletedPart{ETag: out.CopyPartResult.ETag, PartNumber: aws.Int32(i + 1)})
}
_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
	Bucket: aws.String(bucket), Key: aws.String(key), UploadId: up.UploadId,
	MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
})
```

The composed object is a repetition of the 64 MiB chunk, so `assertRangeMatches` must compare against the chunk's bytes, not `synthBytesAt(key, off, n)` — it compares against `synthBytesAt("_seed/chunk.bin", off%chunkSize, n)` adjusted for chunk boundaries. `assertRangeMatches` takes the seed key from `e.largeSeed[key]`, populated by `composeLarge`; for ordinary objects it uses the key itself. `t.Cleanup` aborts the upload if `CompleteMultipartUpload` was never reached.

`tier()` reads `SCENARIO_TIER` (`fast`/`full`/`all`, default `full`).

`startSyncInBackground`/`waitForSync` exist because a cancellation case cannot be written as a blocking `e.sync()`: the test must cancel while the copy is in flight. `startSyncInBackground` stores the error in an `e.bgErr` field guarded by an `e.bgDone` channel; `waitForSync` returns it after receiving from `e.bgDone` (a cancellation is an expected error, so the caller decides whether to assert on it).

- [ ] **Step 4: Run tests to verify they pass**

Run: `SCENARIO_TIER=full go test -tags=integration ./tests/scenario/ -run 'TestScenarioFiveGiB|TestScenarioLarge|TestScenarioZeroByte' -v`
Expected: PASS. Composition is ~15s per large object; the syncs dominate at ~450 MB/s server-side. `TestScenarioSixGiB` skips at this tier — it runs only with `SCENARIO_TIER=all`.

- [ ] **Step 5: Commit**

```bash
git add tests/scenario/large.go tests/scenario/scenario_large_test.go
git commit -m "test(scenario): compose large objects server-side and verify by range"
```

---

### Task 5: Versioning scenarios

**Files:**
- Create: `tests/scenario/scenario_versioning_test.go`

**Interfaces:**
- Consumes: Task 1–3 harness. No new production API.

- [ ] **Step 1: Write the failing tests**

```go
// scenario_versioning_test.go
//go:build integration

func TestScenarioVersionedSource(t *testing.T) {
	e := newScenarioEnv(t, envOptions{Versioning: true})
	putBody(t, e.Client, e.Source, "doc.txt", "v1")
	putBody(t, e.Client, e.Source, "doc.txt", "v2")
	e.sync()
	e.assertTargetCount(1)
	e.assertBytesMatch("doc.txt")   // the current version only
}

func TestScenarioVersionedTarget(t *testing.T) {
	e := newScenarioEnv(t, envOptions{Versioning: true})
	putBody(t, e.Client, e.Source, "doc.txt", "v1")
	e.sync()
	putBody(t, e.Client, e.Source, "doc.txt", "v2")
	e.sync()
	vs, err := e.Client.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(e.Target), Prefix: aws.String("doc.txt"),
	})
	if err != nil { t.Fatalf("list target versions: %v", err) }
	if len(vs.Versions) != 2 {
		t.Fatalf("target holds %d versions of doc.txt, want 2 (an overwrite must not replace)", len(vs.Versions))
	}
}

func TestScenarioDeleteMarkerOnSource(t *testing.T) {
	e := newScenarioEnv(t, envOptions{Versioning: true})
	e.Pair.DeletePropagation = true
	putBody(t, e.Client, e.Source, "doc.txt", "v1")
	e.sync()
	_, err := e.Client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String(e.Source), Key: aws.String("doc.txt"),
	})
	if err != nil { t.Fatalf("delete marker: %v", err) }
	e.sync()
	e.assertTargetCount(0)
}

func TestScenarioNoncurrentLifecycle(t *testing.T) {
	e := newScenarioEnv(t, envOptions{Versioning: true})
	e.Pair.NoncurrentExpireDays = 7
	e.Pair.NewerNoncurrentVersions = 3
	e.sync()
	cfg, err := e.Client.GetBucketLifecycleConfiguration(context.Background(), &s3.GetBucketLifecycleConfigurationInput{
		Bucket: aws.String(e.Target),
	})
	if err != nil { t.Fatalf("get lifecycle: %v", err) }
	if len(cfg.Rules) != 1 || aws.ToString(cfg.Rules[0].ID) != "s3sync-noncurrent" {
		t.Fatalf("rules = %v, want exactly one s3sync-noncurrent rule", ruleIDs(cfg.Rules))
	}
	nc := cfg.Rules[0].NoncurrentVersionExpiration
	if nc == nil || aws.ToInt32(nc.NoncurrentDays) != 7 || aws.ToInt32(nc.NewerNoncurrentVersions) != 3 {
		t.Fatalf("noncurrent expiration = %+v, want NoncurrentDays=7 NewerNoncurrentVersions=3", nc)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test -tags=integration ./tests/scenario/ -run 'TestScenarioVersioned|TestScenarioDeleteMarker|TestScenarioNoncurrent' -v`
Expected: FAIL — `undefined: ruleIDs` (and the scenarios themselves have not been proven).

- [ ] **Step 3: Implement**

Add `ruleIDs(rules []types.LifecycleRule) []string` to `harness.go`. `envOptions.Versioning` must enable versioning on **both** buckets at creation (`PutBucketVersioning`), because a versioned target is what makes `VersionedTarget` meaningful and a versioned source is what makes delete markers possible. (`contains` and `listAll` already exist from Tasks 3.)

No production code changes: these scenarios exercise existing behaviour and are expected to pass once the harness is correct. If any fails, the failure is a real defect — stop and report it rather than adjusting the test.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -tags=integration ./tests/scenario/ -run 'TestScenarioVersioned|TestScenarioDeleteMarker|TestScenarioNoncurrent' -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/scenario/harness.go tests/scenario/scenario_versioning_test.go
git commit -m "test(scenario): versioning and delete-marker scenarios"
```

---

### Task 6: Object lock scenarios

**Files:**
- Create: `tests/scenario/scenario_lock_test.go`

**Interfaces:**
- Consumes: Task 1–3 harness, `sync.ProvisionTarget`, `sync.ProvisionOptions`, `sync.ErrLockMissing`, `sync.ErrTargetNotEmpty`.
- Produces: `func (e *scenarioEnv) provision(opts sync.ProvisionOptions) error` — calls `sync.ProvisionTarget(context.Background(), e.tgtClient, "us-east-1", e.tgtBucket, e.Pair, opts)`.

- [ ] **Step 1: Write the failing tests**

```go
// scenario_lock_test.go
//go:build integration

func TestScenarioGovernanceOneDay(t *testing.T) {
	e := newScenarioEnv(t, envOptions{ObjectLock: true, ReplicateAttrs: true})
	putBody(t, e.Client, e.Source, "held.txt", "body", func(in *s3.PutObjectInput) {
		in.ObjectLockMode = types.ObjectLockModeGovernance
		in.ObjectLockRetainUntilDate = aws.Time(time.Now().Add(24 * time.Hour))
	})
	e.sync()
	f := fetchFacts(t, e.Client, e.Target, "held.txt")
	if f.RetentionMode != string(types.ObjectLockModeGovernance) {
		t.Fatalf("target retention mode = %q, want GOVERNANCE", f.RetentionMode)
	}
	_, err := e.Client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String(e.Target), Key: aws.String("held.txt"),
	})
	if err == nil {
		t.Fatal("the target allowed deletion of an object under GOVERNANCE retention")
	}
}

func TestScenarioComplianceTwoMinutes(t *testing.T) {
	// COMPLIANCE cannot be bypassed, so the hold is minutes, not a day.
	e := newScenarioEnv(t, envOptions{ObjectLock: true, ReplicateAttrs: true})
	putBody(t, e.Client, e.Source, "strict.txt", "body", func(in *s3.PutObjectInput) {
		in.ObjectLockMode = types.ObjectLockModeCompliance
		in.ObjectLockRetainUntilDate = aws.Time(time.Now().Add(2 * time.Minute))
	})
	e.sync()
	f := fetchFacts(t, e.Client, e.Target, "strict.txt")
	if f.RetentionMode != string(types.ObjectLockModeCompliance) {
		t.Fatalf("target retention mode = %q, want COMPLIANCE", f.RetentionMode)
	}
	_, err := e.Client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String(e.Target), Key: aws.String("strict.txt"),
		BypassGovernanceRetention: aws.Bool(true),
	})
	if err == nil {
		t.Fatal("a COMPLIANCE hold was bypassed")
	}
}

func TestScenarioLegalHold(t *testing.T) {
	e := newScenarioEnv(t, envOptions{ObjectLock: true, ReplicateAttrs: true})
	putBody(t, e.Client, e.Source, "hold.txt", "body", func(in *s3.PutObjectInput) {
		in.ObjectLockLegalHoldStatus = types.ObjectLockLegalHoldStatusOn
	})
	e.sync()
	if f := fetchFacts(t, e.Client, e.Target, "hold.txt"); f.LegalHold != "ON" {
		t.Fatalf("target legal hold = %q, want ON", f.LegalHold)
	}
}

func TestScenarioLockWithoutTargetLock(t *testing.T) {
	e := newScenarioEnv(t, envOptions{ObjectLock: false, ReplicateAttrs: true})
	putBody(t, e.Client, e.Source, "held.txt", "body", func(in *s3.PutObjectInput) {
		in.ObjectLockMode = types.ObjectLockModeGovernance
		in.ObjectLockRetainUntilDate = aws.Time(time.Now().Add(time.Hour))
	})
	err := e.syncExpectingError()
	if err == nil {
		t.Fatal("copying a locked object onto an unlocked target succeeded")
	}
	if !strings.Contains(err.Error(), "object lock") {
		t.Fatalf("error does not name object lock: %v", err)
	}
	e.assertTargetCount(0)
}

func TestScenarioRetentionJustExpired(t *testing.T) {
	// Review Focus 5: a retain-until already in the past must not be replicated.
	e := newScenarioEnv(t, envOptions{ObjectLock: true, ReplicateAttrs: true})
	putBody(t, e.Client, e.Source, "stale.txt", "body", func(in *s3.PutObjectInput) {
		in.ObjectLockMode = types.ObjectLockModeGovernance
		in.ObjectLockRetainUntilDate = aws.Time(time.Now().Add(2 * time.Second))
	})
	time.Sleep(3 * time.Second)
	e.sync()
	if f := fetchFacts(t, e.Client, e.Target, "stale.txt"); f.RetentionMode != "" {
		t.Fatalf("an expired retention was replicated: mode = %q", f.RetentionMode)
	}
}

func TestScenarioLockAtCreation(t *testing.T) {
	// require-object-lock on a target that does not exist yet: provisioning must
	// CREATE it with lock enabled, which is the only moment this endpoint allows.
	e := newScenarioEnv(t, envOptions{ObjectLock: false})
	e.Pair.RequireObjectLock = true
	e.Pair.OnLockMissing = "fail"
	// Drop the target bucket so provisioning has to create it.
	destroyBucket(t, e.Client, e.Target)

	if err := e.provision(sync.ProvisionOptions{Now: time.Now().UTC()}); err != nil {
		t.Fatalf("provisioning a missing target with require-object-lock failed: %v", err)
	}
	cfg, err := e.Client.GetObjectLockConfiguration(context.Background(), &s3.GetObjectLockConfigurationInput{
		Bucket: aws.String(e.Target),
	})
	if err != nil { t.Fatalf("get lock config: %v", err) }
	if cfg.ObjectLockConfiguration == nil || cfg.ObjectLockConfiguration.ObjectLockEnabled != types.ObjectLockEnabledEnabled {
		t.Fatal("the created target does not have object lock enabled")
	}
}

func TestScenarioLockMissingFail(t *testing.T) {
	e := newScenarioEnv(t, envOptions{ObjectLock: false})
	e.Pair.RequireObjectLock = true
	e.Pair.OnLockMissing = "fail"
	err := e.provision(sync.ProvisionOptions{Now: time.Now().UTC()})
	if !errors.Is(err, sync.ErrLockMissing) {
		t.Fatalf("provision error = %v, want ErrLockMissing", err)
	}
}

func TestScenarioLockMissingRecreateEmpty(t *testing.T) {
	e := newScenarioEnv(t, envOptions{ObjectLock: false})
	e.Pair.RequireObjectLock = true
	e.Pair.OnLockMissing = "recreate"
	if err := e.provision(sync.ProvisionOptions{ForceRecreate: true, Now: time.Now().UTC()}); err != nil {
		t.Fatalf("recreate of an empty unlocked target failed: %v", err)
	}
	cfg, err := e.Client.GetObjectLockConfiguration(context.Background(), &s3.GetObjectLockConfigurationInput{
		Bucket: aws.String(e.Target),
	})
	if err != nil { t.Fatalf("get lock config: %v", err) }
	if cfg.ObjectLockConfiguration == nil || cfg.ObjectLockConfiguration.ObjectLockEnabled != types.ObjectLockEnabledEnabled {
		t.Fatal("the recreated target does not have object lock enabled")
	}
}

func TestScenarioLockMissingRecreateNonEmpty(t *testing.T) {
	e := newScenarioEnv(t, envOptions{ObjectLock: false})
	putBody(t, e.Client, e.Target, "precious.txt", "do not delete")
	e.Pair.RequireObjectLock = true
	e.Pair.OnLockMissing = "recreate"
	err := e.provision(sync.ProvisionOptions{ForceRecreate: true, Now: time.Now().UTC()})
	if !errors.Is(err, sync.ErrTargetNotEmpty) {
		t.Fatalf("provision error = %v, want ErrTargetNotEmpty", err)
	}
	// The guard must have refused before deleting anything.
	if keys := listAll(t, e.Client, e.Target); len(keys) != 1 || keys[0] != "precious.txt" {
		t.Fatalf("target contents after a refused recreate = %v, want [precious.txt]", keys)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test -tags=integration ./tests/scenario/ -run TestScenarioLock\|TestScenarioRetention -v`
Expected: FAIL — `e.provision undefined`, `undefined: listAll`.

- [ ] **Step 3: Implement**

`scenarioEnv.provision` needs the target bucket name and a `*s3.Client`; `sync.ProvisionTarget` is exported and takes an unexported interface, which a `*s3.Client` satisfies implicitly — no production change needed. (`listAll` already exists from Task 3.)

`envOptions.ObjectLock` must create the bucket with `ObjectLockEnabledForBucket: true` (Task 1). `LockMissingRecreateNonEmpty` deliberately writes to the **target** before provisioning, which is what makes the `ErrTargetNotEmpty` guard reachable. `LockAtCreation` destroys the target first so `ProvisionTarget` takes its create path — the destroy is safe because `createBucket` already registered a `t.Cleanup` and `destroyBucket` tolerates an absent bucket.

If `RetentionJustExpired` fails because the engine replicates an expired retention, that is a real defect in `liveRetention` (`internal/sync/attrs.go:57`) — report it, do not widen the sleep.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -tags=integration ./tests/scenario/ -run 'TestScenarioLock|TestScenarioRetention' -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/scenario/scenario_lock_test.go tests/scenario/harness.go
git commit -m "test(scenario): object lock, provisioning, and retention-expiry scenarios"
```

---

### Task 7: Attribute replication scenarios

**Files:**
- Create: `tests/scenario/scenario_attrs_test.go`

**Interfaces:**
- Consumes: Task 1–3 harness, `sync.ShouldReconcile`.
- Produces: `func (e *scenarioEnv) reconcile() ` — sets `e.Pair.LastReconcileAt` to a time far enough in the past that `ShouldReconcile` is true, then syncs.
- Produces: `func (e *scenarioEnv) markPending(key string)` — rewrites the cached row for `key` with `PendingAttrs: true` and an empty `AttrsHash`, reproducing the state a failed tag write leaves behind.

- [ ] **Step 1: Write the failing tests**

```go
// scenario_attrs_test.go
//go:build integration

func TestScenarioMetadata(t *testing.T) {
	e := newScenarioEnv(t, envOptions{ReplicateAttrs: true})
	putBody(t, e.Client, e.Source, "doc.txt", "body", func(in *s3.PutObjectInput) {
		in.ContentType = aws.String("text/csv")
		in.ContentDisposition = aws.String(`attachment; filename="doc.csv"`)
		in.CacheControl = aws.String("max-age=3600")
		in.Metadata = map[string]string{"origin": "prod", "tier": "hot"}
	})
	e.sync()
	f := fetchFacts(t, e.Client, e.Target, "doc.txt")
	if f.ContentType != "text/csv" { t.Fatalf("content-type = %q, want text/csv", f.ContentType) }
	if f.Metadata["origin"] != "prod" || f.Metadata["tier"] != "hot" {
		t.Fatalf("metadata = %v, want origin=prod tier=hot", f.Metadata)
	}
}

func TestScenarioTags(t *testing.T) {
	e := newScenarioEnv(t, envOptions{ReplicateAttrs: true})
	putBody(t, e.Client, e.Source, "doc.txt", "body")
	putTags(t, e.Client, e.Source, "doc.txt", map[string]string{"team": "alpha", "env": "prod"})
	e.sync()
	if f := fetchFacts(t, e.Client, e.Target, "doc.txt"); f.Tags["team"] != "alpha" || f.Tags["env"] != "prod" {
		t.Fatalf("target tags = %v, want team=alpha env=prod", f.Tags)
	}
}

func TestScenarioEmptyTagsClearsTarget(t *testing.T) {
	e := newScenarioEnv(t, envOptions{ReplicateAttrs: true})
	putBody(t, e.Client, e.Source, "doc.txt", "body")
	putTags(t, e.Client, e.Source, "doc.txt", map[string]string{"team": "alpha"})
	e.sync()
	deleteTags(t, e.Client, e.Source, "doc.txt")
	e.reconcile()
	if f := fetchFacts(t, e.Client, e.Target, "doc.txt"); len(f.Tags) != 0 {
		t.Fatalf("target tags = %v after the source's tags were removed, want none", f.Tags)
	}
}

func TestScenarioTagDriftRepaired(t *testing.T) {
	e := newScenarioEnv(t, envOptions{ReplicateAttrs: true, AttrsReconcileInterval: 60})
	putBody(t, e.Client, e.Source, "doc.txt", "body")
	putTags(t, e.Client, e.Source, "doc.txt", map[string]string{"team": "alpha"})
	e.sync()
	putTags(t, e.Client, e.Source, "doc.txt", map[string]string{"team": "beta"})
	e.reconcile()
	if f := fetchFacts(t, e.Client, e.Target, "doc.txt"); f.Tags["team"] != "beta" {
		t.Fatalf("target tag team = %q after drift, want beta", f.Tags["team"])
	}
}

func TestScenarioMetadataDriftRepaired(t *testing.T) {
	// Metadata changes bump neither ETag nor LastModified, so only the
	// fingerprint can see this.
	e := newScenarioEnv(t, envOptions{ReplicateAttrs: true, AttrsReconcileInterval: 60})
	putBody(t, e.Client, e.Source, "doc.txt", "body", func(in *s3.PutObjectInput) {
		in.Metadata = map[string]string{"rev": "1"}
	})
	e.sync()
	copySelfWithMetadata(t, e.Client, e.Source, "doc.txt", map[string]string{"rev": "2"})
	e.reconcile()
	if f := fetchFacts(t, e.Client, e.Target, "doc.txt"); f.Metadata["rev"] != "2" {
		t.Fatalf("target metadata rev = %q after drift, want 2", f.Metadata["rev"])
	}
}

func TestScenarioStorageClassOverride(t *testing.T) {
	e := newScenarioEnv(t, envOptions{ReplicateAttrs: true, AttrsReconcileInterval: 60})
	e.Pair.TargetStorageClass = "STANDARD"
	putBody(t, e.Client, e.Source, "doc.txt", "body")
	e.sync()
	e.assertNoWorkOnSecondSync()      // the fingerprint must record the effective class
	e.reconcile()
	e.assertNoWorkOnSecondSync()      // and the sweep must compute the same thing
}

func TestScenarioUnreplicableFields(t *testing.T) {
	// Content-Language and Expires are not stored by this endpoint, so they are
	// deliberately not replicated. If they were fed into the fingerprint, the
	// source would differ from the target on every sweep forever.
	e := newScenarioEnv(t, envOptions{ReplicateAttrs: true, AttrsReconcileInterval: 60})
	putBody(t, e.Client, e.Source, "doc.txt", "body", func(in *s3.PutObjectInput) {
		in.ContentLanguage = aws.String("en-GB")
		in.Expires = aws.Time(time.Now().Add(24 * time.Hour))
		in.ContentType = aws.String("text/plain")
	})
	e.sync()
	e.assertNoWorkOnSecondSync()
	e.reconcile()
	e.assertNoWorkOnSecondSync()   // perpetual drift would show up here
}

func TestScenarioPendingAttrsRecovery(t *testing.T) {
	// When the tag write fails, the object is marked pending and the next run
	// retries ATTRIBUTES ONLY — the bytes are not copied again.
	e := newScenarioEnv(t, envOptions{ReplicateAttrs: true})
	putBody(t, e.Client, e.Source, "doc.txt", "body")
	putTags(t, e.Client, e.Source, "doc.txt", map[string]string{"team": "alpha"})
	e.sync()

	// Simulate the failed tag write the engine would have recorded.
	e.markPending("doc.txt")
	e.Counter.Reset()
	e.sync()
	if got := e.Counter.Count(kindPutObject) + e.Counter.Count(kindCopyObject); got != 0 {
		t.Fatalf("a pending-attributes object was re-copied (%d byte writes), want attributes only", got)
	}
	if got := e.Counter.Count(kindPutTagging); got == 0 {
		t.Fatal("a pending-attributes object did not get its tags written")
	}
	if f := fetchFacts(t, e.Client, e.Target, "doc.txt"); f.Tags["team"] != "alpha" {
		t.Fatalf("target tag team = %q after recovery, want alpha", f.Tags["team"])
	}
}

func TestScenarioAttrsOff(t *testing.T) {
	e := newScenarioEnv(t, envOptions{ReplicateAttrs: false})
	putBody(t, e.Client, e.Source, "doc.txt", "body")
	putTags(t, e.Client, e.Source, "doc.txt", map[string]string{"team": "alpha"})
	e.sync()
	if got := e.Counter.Count(kindGetTagging); got != 0 {
		t.Fatalf("replication is off but %d GetObjectTagging calls were made", got)
	}
	if f := fetchFacts(t, e.Client, e.Target, "doc.txt"); len(f.Tags) != 0 {
		t.Fatalf("tags were replicated with replication off: %v", f.Tags)
	}
}

func TestScenarioInvalidStorageClass(t *testing.T) {
	e := newScenarioEnv(t, envOptions{})
	e.Pair.TargetStorageClass = "GLACIER"
	putBody(t, e.Client, e.Source, "doc.txt", "body")
	err := e.syncExpectingError()
	if err == nil {
		t.Fatal("an invalid storage class was accepted")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "storage class") {
		t.Fatalf("error does not mention the storage class: %v", err)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test -tags=integration ./tests/scenario/ -run TestScenarioMetadata\|TestScenarioTag -v`
Expected: FAIL — `e.reconcile undefined`, `undefined: putTags`.

- [ ] **Step 3: Implement**

`reconcile()` sets `e.Pair.LastReconcileAt = aws.Time(time.Now().Add(-2 * e.Pair.AttrsReconcileInterval * time.Second))` so `ShouldReconcile` (`internal/sync/reconcile.go:17-25`) is true, then syncs.

Helpers in `harness.go`: `putTags`, `deleteTags` (via `PutObjectTagging` with an empty `TagSet` — verified accepted on this endpoint), `copySelfWithMetadata` (a self-`CopyObject` with `MetadataDirective: REPLACE`, which changes metadata without changing the bytes, so the ETag stays the same for a body small enough not to be multipart).

`markPending` reads the cached row, sets `PendingAttrs: true` and clears `AttrsHash`, and writes it back through `e.Store.Put`. It is the only way to reach the attribute-only retry path without injecting a fault into a real S3 client.

`StorageClassOverride` is the scenario that would have caught the writer/reader fingerprint mismatch fixed in this project: `assertNoWorkOnSecondSync` after the first sync, and again after a sweep, is the whole assertion.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -tags=integration ./tests/scenario/ -run 'TestScenarioMetadata|TestScenarioTag|TestScenarioStorageClass|TestScenarioAttrs|TestScenarioInvalid' -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/scenario/scenario_attrs_test.go tests/scenario/harness.go
git commit -m "test(scenario): attribute replication, drift, and storage-class scenarios"
```

---

### Task 8: Expiry scenarios

**Files:**
- Create: `tests/scenario/scenario_expiry_test.go`

**Interfaces:**
- Consumes: Task 1–3 harness, `sync.ErrPairExpired`, `sync.ErrExpireNotMidnight`, `sync.ErrExpireInPast`.

- [ ] **Step 1: Write the failing tests**

```go
// scenario_expiry_test.go
//go:build integration

func TestScenarioExpireDateRule(t *testing.T) {
	e := newScenarioEnv(t, envOptions{TargetExpireIn: 60 * 24 * time.Hour})
	e.seed([]string{"obj/0001-small.bin"})
	e.sync()
	cfg, err := e.Client.GetBucketLifecycleConfiguration(context.Background(), &s3.GetBucketLifecycleConfigurationInput{
		Bucket: aws.String(e.Target),
	})
	if err != nil { t.Fatalf("get lifecycle: %v", err) }
	if len(cfg.Rules) != 1 || aws.ToString(cfg.Rules[0].ID) != "s3sync-expire" {
		t.Fatalf("rules = %v, want exactly one s3sync-expire rule", ruleIDs(cfg.Rules))
	}
	r := cfg.Rules[0]
	if r.Expiration == nil || r.Expiration.Date == nil {
		t.Fatalf("rule expiration = %+v, want a Date", r.Expiration)
	}
	if r.Filter != nil || r.Prefix != nil {
		t.Fatal("the rule carries a Filter or Prefix, which this endpoint does not support")
	}
}

func TestScenarioExpiredPairRefuses(t *testing.T) {
	e := newScenarioEnv(t, envOptions{})
	e.seed([]string{"obj/0001-small.bin"})
	e.Pair.TargetExpireAt = aws.Time(time.Now().Add(-24 * time.Hour).UTC().Truncate(24 * time.Hour))
	err := e.syncExpectingError()
	if !errors.Is(err, sync.ErrPairExpired) {
		t.Fatalf("sync error = %v, want ErrPairExpired", err)
	}
	e.assertTargetCount(0)
	if got := e.Counter.Count(kindPutObject) + e.Counter.Count(kindCopyObject); got != 0 {
		t.Fatalf("an expired pair issued %d writes", got)
	}
}

func TestScenarioExpiryRecovery(t *testing.T) {
	e := newScenarioEnv(t, envOptions{})
	e.seed([]string{"obj/0001-small.bin"})
	e.Pair.TargetExpireAt = aws.Time(time.Now().Add(-24 * time.Hour).UTC().Truncate(24 * time.Hour))
	if err := e.syncExpectingError(); !errors.Is(err, sync.ErrPairExpired) {
		t.Fatalf("sync error = %v, want ErrPairExpired", err)
	}
	e.Pair.TargetExpireAt = aws.Time(midnightUTC(30))
	e.sync()
	e.assertTargetCount(1)
}

func TestScenarioExpiryCleared(t *testing.T) {
	e := newScenarioEnv(t, envOptions{TargetExpireIn: 30 * 24 * time.Hour})
	e.sync()
	// Clearing the date does NOT remove the existing rule: an empty rule set is
	// skipped rather than written, so the operator must clear it by hand.
	e.Pair.TargetExpireAt = nil
	e.sync()
	cfg, err := e.Client.GetBucketLifecycleConfiguration(context.Background(), &s3.GetBucketLifecycleConfigurationInput{
		Bucket: aws.String(e.Target),
	})
	if err != nil { t.Fatalf("get lifecycle: %v", err) }
	if !contains(ruleIDs(cfg.Rules), "s3sync-expire") {
		t.Fatalf("rules = %v, want the s3sync-expire rule still present", ruleIDs(cfg.Rules))
	}
}

func TestScenarioNonMidnightRejected(t *testing.T) {
	e := newScenarioEnv(t, envOptions{})
	e.Pair.TargetExpireAt = aws.Time(time.Now().Add(30 * 24 * time.Hour).UTC().Truncate(time.Hour).Add(time.Hour))
	err := e.syncExpectingError()
	if !errors.Is(err, sync.ErrExpireNotMidnight) {
		t.Fatalf("sync error = %v, want ErrExpireNotMidnight", err)
	}
	// The error must quote the offending value so the operator can see it.
	if !strings.Contains(err.Error(), "T01:00:00Z") {
		t.Fatalf("error does not quote the rejected timestamp: %v", err)
	}
}

func TestScenarioPastDateRejected(t *testing.T) {
	e := newScenarioEnv(t, envOptions{})
	e.Pair.TargetExpireAt = aws.Time(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC))
	if err := e.syncExpectingError(); !errors.Is(err, sync.ErrExpireInPast) {
		t.Fatalf("sync error = %v, want ErrExpireInPast", err)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test -tags=integration ./tests/scenario/ -run TestScenarioExpir -v`
Expected: FAIL — `undefined: midnightUTC`.

- [ ] **Step 3: Implement**

`midnightUTC(days int) time.Time` returns `time.Now().UTC().AddDate(0, 0, days).Truncate(24 * time.Hour)`. `envOptions.TargetExpireIn` sets `Pair.TargetExpireAt = aws.Time(midnightUTC(int(d/24h)))`.

`ExpiredPairRefuses` asserts zero writes, which pins the guard's *position* in `RunOnce` (`internal/sync/engine.go:222`) rather than just its presence — a guard placed after provisioning would still return the error but would have already touched the target.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -tags=integration ./tests/scenario/ -run 'TestScenarioExpir|TestScenarioNonMidnight|TestScenarioPastDate' -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/scenario/scenario_expiry_test.go tests/scenario/harness.go
git commit -m "test(scenario): expiry rule, guard, and recovery scenarios"
```

---

### Task 9: Matrix combinations — the seams

**Files:**
- Create: `tests/scenario/scenario_matrix_test.go`

**Interfaces:**
- Consumes: Tasks 1–8. No new API.

- [ ] **Step 1: Write the failing tests**

```go
// scenario_matrix_test.go
//go:build integration

func TestScenarioDryRunThenReal(t *testing.T) {
	e := newScenarioEnv(t, envOptions{})
	e.Pair.DryRun = true
	e.seed([]string{"obj/a.bin", "obj/b.bin"})
	e.sync()
	if keys := listAll(t, e.Client, e.Target); len(keys) != 0 {
		t.Fatalf("a dry run copied %v", keys)
	}
	e.Pair.DryRun = false
	e.sync()
	e.assertTargetCount(2)      // the dry run must not have cached them
}

func TestScenarioDryRunDoesNotProvision(t *testing.T) {
	e := newScenarioEnv(t, envOptions{TargetExpireIn: 30 * 24 * time.Hour})
	e.Pair.DryRun = true
	e.sync()
	_, err := e.Client.GetBucketLifecycleConfiguration(context.Background(), &s3.GetBucketLifecycleConfigurationInput{
		Bucket: aws.String(e.Target),
	})
	if err == nil {
		t.Fatal("a dry run wrote a lifecycle configuration")
	}
}

func TestScenarioLockedVersionedAttrs(t *testing.T) {
	if tier() == tierFast { t.Skip("large object") }
	e := newScenarioEnv(t, envOptions{
		ObjectLock: true, Versioning: true, ReplicateAttrs: true, WorkerCount: 4,
	})
	composeLarge(t, e.Client, e.Source, "big/all.bin", 6<<30, func(in *s3.CreateMultipartUploadInput) {
		in.ContentType = aws.String("application/x-custom")
		in.ObjectLockMode = types.ObjectLockModeGovernance
		in.ObjectLockRetainUntilDate = aws.Time(time.Now().Add(24 * time.Hour))
	})
	putTags(t, e.Client, e.Source, "big/all.bin", map[string]string{"kind": "matrix"})
	e.sync()
	f := fetchFacts(t, e.Client, e.Target, "big/all.bin")
	if f.ContentType != "application/x-custom" || f.RetentionMode != "GOVERNANCE" || f.Tags["kind"] != "matrix" {
		t.Fatalf("target facts = %+v, want content-type, GOVERNANCE and tag all replicated", f)
	}
}

func TestScenarioExpiryWithLockAndVersioning(t *testing.T) {
	e := newScenarioEnv(t, envOptions{
		ObjectLock: true, Versioning: true, TargetExpireIn: 60 * 24 * time.Hour,
	})
	e.Pair.NoncurrentExpireDays = 7
	e.sync()
	cfg, err := e.Client.GetBucketLifecycleConfiguration(context.Background(), &s3.GetBucketLifecycleConfigurationInput{
		Bucket: aws.String(e.Target),
	})
	if err != nil { t.Fatalf("get lifecycle: %v", err) }
	ids := ruleIDs(cfg.Rules)
	if len(ids) != 2 || !contains(ids, "s3sync-expire") || !contains(ids, "s3sync-noncurrent") {
		t.Fatalf("rules = %v, want both s3sync-expire and s3sync-noncurrent", ids)
	}
}

func TestScenarioManySmallLocked(t *testing.T) {
	if tier() == tierFast { t.Skip("500 locked objects") }
	e := newScenarioEnv(t, envOptions{ObjectLock: true, ReplicateAttrs: true, WorkerCount: 8})
	for i := 0; i < 500; i++ {
		k := synthKey(i, "small")
		putBody(t, e.Client, e.Source, k, "body", func(in *s3.PutObjectInput) {
			in.ObjectLockMode = types.ObjectLockModeGovernance
			in.ObjectLockRetainUntilDate = aws.Time(time.Now().Add(24 * time.Hour))
		})
	}
	e.sync()
	e.assertTargetCount(500)
	e.assertNoWorkOnSecondSync()
}

func TestScenarioWorkerCountIndependence(t *testing.T) {
	for _, workers := range []int{1, 8} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			e := newScenarioEnv(t, envOptions{WorkerCount: workers, ReplicateAttrs: true})
			keys := manyKeys(50)
			e.seed(keys)
			e.sync()
			e.assertTargetCount(50)
			for _, k := range keys { e.assertFactsMatch(k) }
			e.assertNoWorkOnSecondSync()
		})
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test -tags=integration ./tests/scenario/ -run TestScenarioDryRun\|TestScenarioLockedVersioned\|TestScenarioExpiryWith\|TestScenarioManySmallLocked\|TestScenarioWorkerCount -v`
Expected: FAIL — `undefined: contains`, and the scenarios themselves unproven.

- [ ] **Step 3: Implement**

Add nothing to `harness.go`; `contains` and `ruleIDs` already exist from Tasks 3 and 5. No production changes: every scenario here exercises shipped behaviour.

`DryRunThenReal` is the direct regression test for the cache-poisoning bug fixed in this project; `DryRunDoesNotProvision` for the provisioning-in-dry-run bug. Both must fail against the pre-fix code — verify by `git stash`ing the fix if either passes unexpectedly.

- [ ] **Step 4: Run tests to verify they pass**

Run: `SCENARIO_TIER=full go test -tags=integration ./tests/scenario/ -run 'TestScenarioDryRun|TestScenarioLockedVersioned|TestScenarioExpiryWith|TestScenarioManySmallLocked|TestScenarioWorkerCount' -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/scenario/scenario_matrix_test.go tests/scenario/harness.go
git commit -m "test(scenario): feature-combination scenarios for the cross-feature seams"
```

---

### Task 10: CLI smoke test and the runner script

**Files:**
- Create: `tests/scenario/scenario_cli_test.go`, `tests/run-scenarios.sh`
- Modify: `README.md` (Testing section)

**Interfaces:**
- Consumes: the built binary, path from `SCENARIO_CLI_BIN`; skips when unset or not executable. `internal/config` (`Open`, `NewRepository`, `ListBuckets`, `ListSyncPairs`) to recover the full UUIDs the CLI needs.
- Produces: `tests/run-scenarios.sh [tier] [host]`.

- [ ] **Step 1: Write the failing test**

```go
// scenario_cli_test.go
//go:build integration

func TestScenarioCLIEndToEnd(t *testing.T) {
	bin := os.Getenv("SCENARIO_CLI_BIN")
	if bin == "" { t.Skip("SCENARIO_CLI_BIN not set") }
	if _, err := os.Stat(bin); err != nil { t.Skipf("no CLI binary: %v", err) }

	client, _ := newClient(t)
	src := createBucket(t, client, true)
	tgt := createBucket(t, client, false)
	putBody(t, client, src, "cli/doc.txt", "body")
	putTags(t, client, src, "cli/doc.txt", map[string]string{"via": "cli"})

	// The config directory is the global --config-dir flag (cmd/root.go:34),
	// not an environment variable, and every command needs it.
	cfgDir := t.TempDir()
	run := func(args ...string) (string, error) {
		full := append([]string{"--config-dir", cfgDir}, args...)
		cmd := exec.Command(bin, full...)
		out, err := cmd.CombinedOutput()
		return string(out), err
	}

	// bucket add takes the pair-facing name POSITIONALLY (cmd/bucket.go:18-20),
	// and pair add needs the full bucket UUID, not a short prefix.
	for _, b := range []struct{ name, bucket string; lock bool }{
		{"src", src, true}, {"tgt", tgt, false},
	} {
		args := []string{"bucket", "add", b.name,
			"--endpoint", os.Getenv("S3_TEST_ENDPOINT"),
			"--access-key", os.Getenv("S3_TEST_ACCESS_KEY"),
			"--secret-key", os.Getenv("S3_TEST_SECRET_KEY"),
			"--bucket-name", b.bucket}
		if b.lock { args = append(args, "--object-lock", "--versioning") }
		if out, err := run(args...); err != nil {
			t.Fatalf("bucket add %s: %v\n%s", b.name, err, out)
		}
	}

	// Recover the bucket IDs the pair needs. `bucket list` prints a TRUNCATED
	// 8-char id (cmd/bucket.go:46-49), which pair add rejects, so read the
	// config database directly instead of parsing CLI output.
	srcID, tgtID := bucketIDs(t, cfgDir, "src", "tgt")
	expire := midnightUTC(60).Format("2006-01-02")
	out, err := run("pair", "add", "cli",
		"--source-bucket", srcID, "--target-bucket", tgtID,
		"--replicate-attributes", "--target-expire-at", expire)
	if err != nil { t.Fatalf("pair add: %v\n%s", err, out) }

	// pair sync takes the pair id, not its name.
	pairID := pairIDByName(t, cfgDir, "cli")
	out, err = run("pair", "sync", pairID)
	if err != nil { t.Fatalf("pair sync: %v\n%s", err, out) }
	if !strings.Contains(out, "Sync complete") {
		t.Fatalf("pair sync did not report completion:\n%s", out)
	}

	// Assert on the target's contents, not on log text.
	if keys := listAll(t, client, tgt); len(keys) != 1 || keys[0] != "cli/doc.txt" {
		t.Fatalf("target contents = %v, want [cli/doc.txt]", keys)
	}
	if f := fetchFacts(t, client, tgt, "cli/doc.txt"); f.Tags["via"] != "cli" {
		t.Fatalf("target tags = %v, want via=cli", f.Tags)
	}
}

// bucketIDs opens the CLI's own config database and returns the full UUIDs of
// the named buckets, in the order the names were given.
func bucketIDs(t *testing.T, cfgDir string, names ...string) []string {
	t.Helper()
	db, err := config.Open(filepath.Join(cfgDir, "config.db"))
	if err != nil { t.Fatalf("open CLI config: %v", err) }
	defer db.Close()
	buckets, err := config.NewRepository(db).ListBuckets()
	if err != nil { t.Fatalf("list buckets: %v", err) }
	ids := make([]string, len(names))
	for i, want := range names {
		for _, b := range buckets {
			if b.Name == want { ids[i] = b.ID; break }
		}
		if ids[i] == "" { t.Fatalf("no bucket named %q in the CLI config", want) }
	}
	return ids
}

func pairIDByName(t *testing.T, cfgDir, name string) string {
	t.Helper()
	db, err := config.Open(filepath.Join(cfgDir, "config.db"))
	if err != nil { t.Fatalf("open CLI config: %v", err) }
	defer db.Close()
	pairs, err := config.NewRepository(db).ListSyncPairs()
	if err != nil { t.Fatalf("list pairs: %v", err) }
	for _, p := range pairs {
		if p.Name == name { return p.ID }
	}
	t.Fatalf("no pair named %q in the CLI config", name)
	return ""
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `SCENARIO_CLI_BIN=/tmp/nonexistent go test -tags=integration ./tests/scenario/ -run TestScenarioCLI -v`
Expected: SKIP (binary absent) — then build the binary and run again to see it exercise the CLI. The step is "fails" in the sense that the scenario has not been proven; confirm it exercises a real `pair sync` by checking the output contains `Sync complete`.

- [ ] **Step 3: Implement the test and the runner**

Finish `TestScenarioCLIEndToEnd`. Facts the implementation must respect, verified in the source: `bucket add` takes the name **positionally** (`cmd/bucket.go:18-20`); the config directory is the global `--config-dir` flag (`cmd/root.go:34`), not an environment variable; `pair sync` takes the **pair id** (`cmd/pair.go:299-302`); `pair add` needs the **full bucket UUID**, not the 8-char prefix `bucket list` prints (`cmd/bucket.go:46-49`). Assert on the target's contents and tags, not on log text — except one `Sync complete` check to prove the command actually ran a sync.

`tests/run-scenarios.sh`:

```bash
#!/usr/bin/env bash
# Cross-compile the scenario suite, ship it to a Linux host, and run it there.
# Credentials come from the caller's environment and are passed inline over
# SSH; they are never written to a file on either side.
set -euo pipefail
tier="${1:-full}"
host="${2:-root@51.159.133.103}"
out=/tmp/scenario-linux
GOOS=linux GOARCH=amd64 go test -c -tags=integration -o "$out" ./tests/scenario/
GOOS=linux GOARCH=amd64 go build -o /tmp/s3sync-linux ./
scp -q "$out" /tmp/s3sync-linux "$host":/root/
ssh "$host" "SCENARIO_TIER=$tier SCENARIO_CLI_BIN=/root/s3sync-linux \
  S3_TEST_ENDPOINT='$S3_TEST_ENDPOINT' S3_TEST_ACCESS_KEY='$S3_TEST_ACCESS_KEY' \
  S3_TEST_SECRET_KEY='$S3_TEST_SECRET_KEY' /root/scenario-linux -test.v -test.timeout 40m" \
  | tee /tmp/scenario-$(date +%s).log
```

Add a README "Scenario tests" subsection: what the suite proves, the three tiers, the host requirement, and that it is not wired into CI.

- [ ] **Step 4: Run the full suite**

Run: `./tests/run-scenarios.sh fast` then `./tests/run-scenarios.sh full`
Expected: `fast` under 3 minutes, `full` under 15, zero failures, and no buckets left behind (`ssh host 'aws s3 ls'` equivalent is not available, so rely on the verified `destroyBucket`).

- [ ] **Step 5: Commit**

```bash
git add tests/scenario/scenario_cli_test.go tests/run-scenarios.sh README.md
git commit -m "test(scenario): CLI smoke test, cross-compile runner, and README section"
```
