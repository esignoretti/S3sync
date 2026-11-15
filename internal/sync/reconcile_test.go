package sync

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/esignoretti/S3sync/internal/cache"
	"github.com/esignoretti/S3sync/internal/config"
)

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

// fakeSweepAPI answers the source reads a sweep makes, per key.
type fakeSweepAPI struct {
	heads    map[string]*s3.HeadObjectOutput
	headErrs map[string]error
	tags     map[string][]s3types.Tag
	tagErrs  map[string]error
	headRead []string
	tagRead  []string
	copyRead []string
	// tagHook, when set, runs after the fake records a tag read. It lets a test
	// act while the sweep is mid-flight (e.g. cancel the context).
	tagHook func()
}

func (f *fakeSweepAPI) HeadObject(_ context.Context, in *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	key := aws.ToString(in.Key)
	f.headRead = append(f.headRead, key)
	if err := f.headErrs[key]; err != nil {
		return nil, err
	}
	if out := f.heads[key]; out != nil {
		return out, nil
	}
	return &s3.HeadObjectOutput{}, nil
}

func (f *fakeSweepAPI) GetObjectTagging(_ context.Context, in *s3.GetObjectTaggingInput, _ ...func(*s3.Options)) (*s3.GetObjectTaggingOutput, error) {
	key := aws.ToString(in.Key)
	f.tagRead = append(f.tagRead, key)
	if f.tagHook != nil {
		f.tagHook()
	}
	if err := f.tagErrs[key]; err != nil {
		return nil, err
	}
	return &s3.GetObjectTaggingOutput{TagSet: f.tags[key]}, nil
}

func (f *fakeSweepAPI) CopyObject(_ context.Context, in *s3.CopyObjectInput, _ ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
	f.copyRead = append(f.copyRead, aws.ToString(in.Key))
	return &s3.CopyObjectOutput{}, nil
}

func (f *fakeSweepAPI) PutObjectTagging(context.Context, *s3.PutObjectTaggingInput, ...func(*s3.Options)) (*s3.PutObjectTaggingOutput, error) {
	return &s3.PutObjectTaggingOutput{}, nil
}

func (f *fakeSweepAPI) DeleteObject(context.Context, *s3.DeleteObjectInput, ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	return &s3.DeleteObjectOutput{}, nil
}

var _ copyAPI = (*fakeSweepAPI)(nil)

func newReconcileEngine(t *testing.T, pair *config.SyncPair, api copyAPI, entries ...cache.CachedObject) *Engine {
	t.Helper()
	store, err := cache.Open(filepath.Join(t.TempDir(), "cache.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })

	ptrs := make([]*cache.CachedObject, 0, len(entries))
	for i := range entries {
		ptrs = append(ptrs, &entries[i])
	}
	if err := store.PutMany(ptrs); err != nil {
		t.Fatal(err)
	}

	pool := newTestWorkerPool(api)
	pool.storageClass = pair.TargetStorageClass
	return &Engine{pair: pair, store: store, pool: pool, throttler: pool.throttler}
}

// sweep runs one sweep and returns the actions it emitted.
func sweep(t *testing.T, e *Engine) ([]SyncAction, error) {
	t.Helper()
	actions := make(chan SyncAction, 16)
	if err := e.reconcileSweep(context.Background(), actions); err != nil {
		return nil, err
	}
	close(actions)
	return collectActions(t, actions), nil
}

// actionChannel feeds the given actions to a pool and closes the channel, which
// is how a sweep's output reaches the workers in production.
func actionChannel(actions ...SyncAction) <-chan SyncAction {
	ch := make(chan SyncAction, len(actions))
	for _, a := range actions {
		ch <- a
	}
	close(ch)
	return ch
}

// writerHash is the fingerprint copyObject records for the given source state.
// It runs the real copy path rather than re-deriving the hash, so a sweep that
// disagrees with the writer fails here instead of looping in production.
func writerHash(t *testing.T, pair *config.SyncPair, api *fakeSweepAPI, key string) string {
	t.Helper()
	pool := newTestWorkerPool(api)
	pool.storageClass = pair.TargetStorageClass
	got, err := pool.copyObject(context.Background(), SyncAction{Type: ActionCopy, Key: key, ETag: `"e1"`})
	if err != nil {
		t.Fatalf("copyObject(%q): %v", key, err)
	}
	if got.AttrsHash == "" {
		t.Fatalf("copyObject(%q) recorded no fingerprint", key)
	}
	// The writer's own reads and copy are not what the sweep assertions count.
	api.headRead = nil
	api.tagRead = nil
	api.copyRead = nil
	return got.AttrsHash
}

// A source whose characteristics still match the cache must produce no work.
// The tags are the part a HEAD-only comparison cannot see, so this is the
// regression test for a sweep that reconciles forever.
func TestReconcileQuietWhenAttributesMatch(t *testing.T) {
	pair := &config.SyncPair{ID: "p1"}
	api := &fakeSweepAPI{
		heads: map[string]*s3.HeadObjectOutput{"k": {ContentType: aws.String("text/csv")}},
		tags:  map[string][]s3types.Tag{"k": {{Key: aws.String("env"), Value: aws.String("prod")}}},
	}
	hash := writerHash(t, pair, api, "k")
	e := newReconcileEngine(t, pair, api, cache.CachedObject{PairID: "p1", Key: "k", ETag: `"e1"`, AttrsHash: hash})

	got, err := sweep(t, e)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("sweep emitted %d actions for a matching object: %+v", len(got), got)
	}
	if len(api.tagRead) != 1 {
		t.Fatalf("GetObjectTagging called %d times, want 1: the tag component must be compared", len(api.tagRead))
	}
}

// Tags are the drift the differ can never see: a tag change bumps neither the
// ETag nor LastModified, so the sweep is the only thing that can catch it. The
// repair is a full copy: only a server-side CopyObject carries every
// characteristic, so only it can repair drift the tag write cannot.
func TestReconcileEmitsCopyOnTagDrift(t *testing.T) {
	pair := &config.SyncPair{ID: "p1"}
	api := &fakeSweepAPI{
		heads: map[string]*s3.HeadObjectOutput{"k": {ContentType: aws.String("text/csv")}},
		tags:  map[string][]s3types.Tag{"k": {{Key: aws.String("a"), Value: aws.String("1")}}},
	}
	hash := writerHash(t, pair, api, "k")

	api.tags["k"] = []s3types.Tag{{Key: aws.String("a"), Value: aws.String("2")}}
	e := newReconcileEngine(t, pair, api, cache.CachedObject{
		PairID: "p1", Key: "k", ETag: `"e1"`, Size: 7,
		LastModified: time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC), AttrsHash: hash,
	})

	got, err := sweep(t, e)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Type != ActionCopy || got[0].Key != "k" {
		t.Fatalf("sweep emitted %+v, want one ActionCopy for k", got)
	}
	if got[0].ETag != `"e1"` || got[0].Size != 7 || got[0].LastModified.IsZero() {
		t.Fatalf("action lost the cached identity: %+v", got[0])
	}
}

// Drift in a HEAD-visible characteristic is caught the same way, and must be
// repaired the same way. applyAttrs writes tags only, so using it here would
// report a repaired object whose content type is still stale.
func TestReconcileEmitsCopyOnMetadataDrift(t *testing.T) {
	pair := &config.SyncPair{ID: "p1"}
	api := &fakeSweepAPI{heads: map[string]*s3.HeadObjectOutput{
		"k": {ContentType: aws.String("text/csv")},
	}}
	hash := writerHash(t, pair, api, "k")

	api.heads["k"] = &s3.HeadObjectOutput{ContentType: aws.String("application/json")}
	e := newReconcileEngine(t, pair, api, cache.CachedObject{PairID: "p1", Key: "k", AttrsHash: hash})

	got, err := sweep(t, e)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Type != ActionCopy || got[0].Key != "k" {
		t.Fatalf("sweep emitted %+v, want one ActionCopy for k", got)
	}
}

// Only the drifted key is work. A sweep that emits for every entry re-copies the
// whole bucket on every interval, which is the cost the interval gating exists
// to avoid.
func TestReconcileEmitsCopyOnlyForDriftedKey(t *testing.T) {
	pair := &config.SyncPair{ID: "p1"}
	api := &fakeSweepAPI{
		heads: map[string]*s3.HeadObjectOutput{
			"a": {ContentType: aws.String("text/csv")},
			"b": {ContentType: aws.String("text/csv")},
		},
		tags: map[string][]s3types.Tag{
			"a": {{Key: aws.String("env"), Value: aws.String("prod")}},
			"b": {{Key: aws.String("env"), Value: aws.String("prod")}},
		},
	}
	hashA := writerHash(t, pair, api, "a")
	hashB := writerHash(t, pair, api, "b")

	// b drifts; a does not.
	api.tags["b"] = []s3types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}
	e := newReconcileEngine(t, pair, api,
		cache.CachedObject{PairID: "p1", Key: "a", ETag: `"ea"`, AttrsHash: hashA},
		cache.CachedObject{PairID: "p1", Key: "b", ETag: `"eb"`, AttrsHash: hashB},
	)

	got, err := sweep(t, e)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Type != ActionCopy || got[0].Key != "b" {
		t.Fatalf("sweep emitted %+v, want exactly one ActionCopy for the drifted key b", got)
	}
}

// A pair that overrides the storage class hashes the effective target class on
// both sides. Hashing the source's class on either side makes every object
// reconcile on every pass, and the copy can never converge.
//
// Convergence is proved through the real write-back, not by sweeping twice with
// the same inputs: the cache starts with the fingerprint a source-class reader
// would have recorded, the sweep must emit exactly one ActionCopy, that action
// runs through the pool (copyObject recomputes the hash from a fresh source
// HEAD), and only the hash it returns is allowed to end the loop.
func TestReconcileStorageClassOverrideConverges(t *testing.T) {
	pair := &config.SyncPair{ID: "p1", TargetStorageClass: "GLACIER"}
	api := &fakeSweepAPI{
		heads: map[string]*s3.HeadObjectOutput{"k": {StorageClass: s3types.StorageClassStandardIa}},
		tags:  map[string][]s3types.Tag{"k": {{Key: aws.String("env"), Value: aws.String("prod")}}},
	}
	hash := writerHash(t, pair, api, "k")
	if hash == AttrsFromHead(api.heads["k"]).withTags(api.tags["k"]).Hash() {
		t.Fatal("the writer recorded the source class; this test would be vacuous")
	}

	// The stale fingerprint is the source-class one: a reader that forgot to
	// normalize would call this a match and never repair it.
	stale := AttrsFromHead(api.heads["k"]).withTags(api.tags["k"]).Hash()
	e := newReconcileEngine(t, pair, api, cache.CachedObject{
		PairID: "p1", Key: "k", ETag: `"e1"`, AttrsHash: stale,
	})

	got, err := sweep(t, e)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Type != ActionCopy || got[0].Key != "k" {
		t.Fatalf("sweep emitted %+v, want one ActionCopy: the stale fingerprint is drift", got)
	}

	// The repair runs the real copy path, so the fingerprint that ends up in the
	// cache is the one copyObject computes from a fresh source HEAD.
	succeeded, _, failed := e.pool.Run(context.Background(), actionChannel(got...))
	if failed != 0 || len(succeeded) != 1 {
		t.Fatalf("pool: succeeded %d, failed %d", len(succeeded), failed)
	}
	if len(api.copyRead) != 1 || api.copyRead[0] != "k" {
		t.Fatalf("CopyObject calls = %v, want one for k", api.copyRead)
	}
	if succeeded[0].AttrsHash != hash {
		t.Fatalf("write-back hash = %q, want the effective-class hash %q", succeeded[0].AttrsHash, hash)
	}

	// Persist what the worker wrote back, as engine.go does, then sweep again.
	if err := e.store.PutMany([]*cache.CachedObject{{
		PairID: "p1", Key: "k", ETag: `"e1"`, AttrsHash: succeeded[0].AttrsHash,
	}}); err != nil {
		t.Fatal(err)
	}
	again, err := sweep(t, e)
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 0 {
		t.Fatalf("the sweep after the repair emitted %+v, want none: an overridden storage class must converge", again)
	}
}

// Entries written before the feature existed carry no fingerprint. Unknown is
// not drift, and must not cost a source read.
func TestReconcileSkipsUnknownAttrsHash(t *testing.T) {
	pair := &config.SyncPair{ID: "p1"}
	api := &fakeSweepAPI{}
	e := newReconcileEngine(t, pair, api,
		cache.CachedObject{PairID: "p1", Key: "a"},
		cache.CachedObject{PairID: "p1", Key: "b"},
	)

	got, err := sweep(t, e)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("sweep emitted %+v for entries with an unknown fingerprint", got)
	}
	if len(api.headRead) != 0 {
		t.Fatalf("HEAD issued for %v; an unknown fingerprint needs no source read", api.headRead)
	}
}

// A source object that vanished since the copy is a skip: it is not drift, and
// the differ will notice the deletion.
func TestReconcileSkipsVanishedSource(t *testing.T) {
	pair := &config.SyncPair{ID: "p1"}
	api := &fakeSweepAPI{
		headErrs: map[string]error{"gone": &s3types.NotFound{}},
		tagErrs:  map[string]error{"gone": &s3types.NotFound{}},
	}
	e := newReconcileEngine(t, pair, api, cache.CachedObject{PairID: "p1", Key: "gone", AttrsHash: "stale"})

	got, err := sweep(t, e)
	if err != nil {
		t.Fatalf("sweep returned %v; a vanished source is a skip, not a failure", err)
	}
	if len(got) != 0 {
		t.Fatalf("sweep emitted %+v for a vanished source", got)
	}
}

// A failed tag read is an error, not drift: the object may be perfectly fine.
// Emitting an action would fabricate work from a transient failure.
func TestReconcileSkipsOnTagReadFailure(t *testing.T) {
	pair := &config.SyncPair{ID: "p1"}
	api := &fakeSweepAPI{
		heads:   map[string]*s3.HeadObjectOutput{"k": {ContentType: aws.String("text/csv")}},
		tagErrs: map[string]error{"k": errors.New("connection reset")},
	}
	e := newReconcileEngine(t, pair, api, cache.CachedObject{PairID: "p1", Key: "k", AttrsHash: "whatever"})

	got, err := sweep(t, e)
	if err != nil {
		t.Fatalf("sweep returned %v; one unreadable object must not abort the sweep", err)
	}
	if len(got) != 0 {
		t.Fatalf("sweep emitted %+v after a failed tag read", got)
	}
}

// The sweep runs before the diff on the same channel, so it must not block
// forever when the consumer is gone. The ctx is cancelled while the sweep is
// mid-flight (its first HEAD has already happened), so the send's select is what
// returns: a pre-cancelled ctx would exit at the loop-top check and prove
// nothing about the send.
func TestReconcileStopsOnCancel(t *testing.T) {
	pair := &config.SyncPair{ID: "p1"}
	api := &fakeSweepAPI{heads: map[string]*s3.HeadObjectOutput{"k": {}}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	api.tagHook = cancel

	e := newReconcileEngine(t, pair, api, cache.CachedObject{PairID: "p1", Key: "k", AttrsHash: "drifted"})

	// Unbuffered and unread: the send can only return through ctx.Done().
	unbuffered := make(chan SyncAction)
	errCh := make(chan error, 1)
	go func() { errCh <- e.reconcileSweep(ctx, unbuffered) }()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("reconcileSweep = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("reconcileSweep did not return after cancellation; the send blocked forever")
	}
	if len(api.headRead) != 1 {
		t.Fatalf("HEAD calls = %v, want one: cancellation must land mid-sweep, not before it starts", api.headRead)
	}
}
