package sync

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

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

// A map literal does not prove order-insensitivity: Go randomises map iteration
// on every range, so the same state hashed repeatedly must produce one value.
func TestAttrsHashStableAcrossMapIterationOrder(t *testing.T) {
	keys := []string{"a", "b", "c", "d", "e", "f"}
	tagKeys := []string{"env", "owner", "team"}

	first := ""
	for i := 0; i < 50; i++ {
		metadata := make(map[string]string, len(keys))
		for j := range keys {
			k := keys[(i+j)%len(keys)]
			metadata[k] = "v-" + k
		}
		tags := make([]types.Tag, 0, len(tagKeys))
		for j := range tagKeys {
			k := tagKeys[(i+j)%len(tagKeys)]
			tags = append(tags, types.Tag{Key: aws.String(k), Value: aws.String("v-" + k)})
		}

		a := ObjectAttrs{ContentType: "text/plain", Metadata: metadata, Tags: tags}
		if i == 0 {
			first = a.Hash()
			continue
		}
		if got := a.Hash(); got != first {
			t.Fatalf("iteration %d: hash %q, want %q — the fingerprint is not canonical", i, got, first)
		}
	}
}

// Every component the pipeline replicates must feed the fingerprint, or drift in
// that component is invisible to the reconciler.
func TestAttrsHashChangesWithEveryComponent(t *testing.T) {
	future := time.Now().UTC().Add(time.Hour)
	base := ObjectAttrs{
		ContentType:        "text/plain",
		ContentDisposition: "inline",
		ContentEncoding:    "gzip",
		CacheControl:       "max-age=60",
		Metadata:           map[string]string{"a": "1"},
		StorageClass:       types.StorageClassStandard,
		LockMode:           types.ObjectLockModeGovernance,
		RetainUntil:        &future,
		LegalHold:          types.ObjectLockLegalHoldStatusOn,
		Tags:               []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
	}
	baseHash := base.Hash()

	mutators := map[string]func(*ObjectAttrs){
		"content-type":        func(a *ObjectAttrs) { a.ContentType = "text/html" },
		"content-disposition": func(a *ObjectAttrs) { a.ContentDisposition = "attachment" },
		"content-encoding":    func(a *ObjectAttrs) { a.ContentEncoding = "identity" },
		"cache-control":       func(a *ObjectAttrs) { a.CacheControl = "no-store" },
		"metadata-value":      func(a *ObjectAttrs) { a.Metadata["a"] = "2" },
		"metadata-key":        func(a *ObjectAttrs) { a.Metadata["b"] = "1" },
		"storage-class":       func(a *ObjectAttrs) { a.StorageClass = types.StorageClassStandardIa },
		"lock-mode":           func(a *ObjectAttrs) { a.LockMode = types.ObjectLockModeCompliance },
		"retain-until":        func(a *ObjectAttrs) { later := future.Add(time.Hour); a.RetainUntil = &later },
		"legal-hold":          func(a *ObjectAttrs) { a.LegalHold = types.ObjectLockLegalHoldStatusOff },
		"tag-value":           func(a *ObjectAttrs) { a.Tags = []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}} },
		"tag-key":             func(a *ObjectAttrs) { a.Tags = []types.Tag{{Key: aws.String("env2"), Value: aws.String("prod")}} },
		"tags-dropped":        func(a *ObjectAttrs) { a.Tags = nil },
	}

	for name, mutate := range mutators {
		changed := base
		changed.Metadata = map[string]string{"a": "1"}
		changed.Tags = append([]types.Tag(nil), base.Tags...)
		mutate(&changed)
		if changed.Hash() == baseHash {
			t.Errorf("changing %s left the fingerprint unchanged", name)
		}
	}
}

// Metadata keys are case-insensitive on the wire, so `B` and `b` are the same
// state and must not fingerprint differently.
func TestAttrsHashLowercasesMetadataKeys(t *testing.T) {
	a := ObjectAttrs{Metadata: map[string]string{"Key": "v"}}
	b := ObjectAttrs{Metadata: map[string]string{"key": "v"}}
	if a.Hash() != b.Hash() {
		t.Fatal("metadata keys must be lowercased before hashing")
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

// An empty hash is the cache's "unknown" marker for entries written before this
// feature; a real object state must never hash to it.
func TestAttrsHashIsNotEmpty(t *testing.T) {
	if got := (ObjectAttrs{}).Hash(); got == "" {
		t.Fatal("Hash must always return a value")
	}
}

func TestAttrsFromHead(t *testing.T) {
	until := time.Now().UTC().Add(24 * time.Hour)
	out := &s3.HeadObjectOutput{
		ContentType:               aws.String("application/json"),
		ContentDisposition:        aws.String(`attachment; filename="x"`),
		ContentEncoding:           aws.String("gzip"),
		CacheControl:              aws.String("max-age=60"),
		Metadata:                  map[string]string{"k": "v"},
		StorageClass:              types.StorageClassStandardIa,
		ObjectLockMode:            types.ObjectLockModeCompliance,
		ObjectLockRetainUntilDate: &until,
		ObjectLockLegalHoldStatus: types.ObjectLockLegalHoldStatusOn,
	}
	got := AttrsFromHead(out)

	if got.ContentType != "application/json" || got.ContentDisposition != `attachment; filename="x"` ||
		got.ContentEncoding != "gzip" || got.CacheControl != "max-age=60" {
		t.Fatalf("content headers = %+v", got)
	}
	if got.Metadata["k"] != "v" {
		t.Fatalf("metadata = %+v", got.Metadata)
	}
	if got.StorageClass != types.StorageClassStandardIa {
		t.Fatalf("StorageClass = %v", got.StorageClass)
	}
	if got.LockMode != types.ObjectLockModeCompliance || got.RetainUntil == nil || !got.RetainUntil.Equal(until) {
		t.Fatalf("lock fields = %v / %v", got.LockMode, got.RetainUntil)
	}
	if got.LegalHold != types.ObjectLockLegalHoldStatusOn {
		t.Fatalf("LegalHold = %v", got.LegalHold)
	}
	// HeadObject does not return tags; copyObject fills them from
	// GetObjectTagging before hashing.
	if got.Tags != nil {
		t.Fatalf("Tags = %+v, want nil: HEAD cannot see tags", got.Tags)
	}
}

func TestCopyInputUsesReplaceAndLockFields(t *testing.T) {
	now := time.Now().UTC()
	future := now.Add(48 * time.Hour)
	a := ObjectAttrs{
		ContentType: "application/json", ContentEncoding: "gzip",
		CacheControl: "max-age=60", ContentDisposition: `attachment; filename="x"`,
		Metadata:     map[string]string{"k": "v"},
		LockMode:     types.ObjectLockModeCompliance,
		RetainUntil:  &future,
		LegalHold:    types.ObjectLockLegalHoldStatusOn,
		StorageClass: types.StorageClassStandardIa,
	}
	in := a.copyInput("tgt", "key", "src/key", "")
	if in.MetadataDirective != types.MetadataDirectiveReplace {
		t.Fatalf("MetadataDirective = %v, want REPLACE", in.MetadataDirective)
	}
	if aws.ToString(in.ContentType) != "application/json" || aws.ToString(in.ContentEncoding) != "gzip" {
		t.Fatal("content headers not passed")
	}
	if aws.ToString(in.CacheControl) != "max-age=60" || aws.ToString(in.ContentDisposition) != `attachment; filename="x"` {
		t.Fatal("cache-control / content-disposition not passed")
	}
	if in.Metadata["k"] != "v" {
		t.Fatal("user metadata not passed")
	}
	if in.ObjectLockMode != types.ObjectLockModeCompliance || in.ObjectLockRetainUntilDate == nil {
		t.Fatal("lock fields not passed")
	}
	if !in.ObjectLockRetainUntilDate.Equal(future) {
		t.Fatalf("ObjectLockRetainUntilDate = %v, want %v", in.ObjectLockRetainUntilDate, future)
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

// A legal hold has no date of its own and is valid without a retention, so it
// survives an expired retention even though the mode and date are dropped.
func TestCopyInputKeepsLegalHoldWithoutLiveRetention(t *testing.T) {
	past := time.Now().UTC().Add(-time.Hour)
	a := ObjectAttrs{LockMode: types.ObjectLockModeGovernance, RetainUntil: &past, LegalHold: types.ObjectLockLegalHoldStatusOn}
	in := a.copyInput("tgt", "k", "s/k", "")
	if in.ObjectLockMode != "" || in.ObjectLockRetainUntilDate != nil {
		t.Fatal("expired retention must not be sent")
	}
	if in.ObjectLockLegalHoldStatus != types.ObjectLockLegalHoldStatusOn {
		t.Fatal("legal hold must be sent independently of the retention date")
	}
}

// An object with no user metadata and no lock must not send empty values: with
// REPLACE, an explicitly empty content-type would clear it on the target.
func TestCopyInputOmitsAbsentValues(t *testing.T) {
	in := (ObjectAttrs{}).copyInput("tgt", "k", "s/k", "")
	if in.ContentType != nil || in.ContentDisposition != nil || in.ContentEncoding != nil || in.CacheControl != nil {
		t.Fatalf("content headers set for an empty attrs: %+v", in)
	}
	if in.Metadata != nil {
		t.Fatalf("Metadata = %+v, want nil", in.Metadata)
	}
	if in.StorageClass != "" || in.ObjectLockMode != "" || in.ObjectLockRetainUntilDate != nil || in.ObjectLockLegalHoldStatus != "" {
		t.Fatalf("unexpected fields on an empty attrs: %+v", in)
	}
	if aws.ToString(in.Bucket) != "tgt" || aws.ToString(in.Key) != "k" || aws.ToString(in.CopySource) != "s/k" {
		t.Fatal("target coordinates not set")
	}
}
