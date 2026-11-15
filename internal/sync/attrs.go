package sync

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// ObjectAttrs is the set of per-object characteristics this tool replicates.
//
// ContentLanguage and Expires are deliberately absent: Cubbit DS3 does not store
// them, so there is nothing to read on the source and nothing to write on the
// target. ACLs, SSE settings, checksums and version history are not per-object
// data the copy can carry, and LastModified is not writable by S3 at all.
type ObjectAttrs struct {
	ContentType        string
	ContentDisposition string
	ContentEncoding    string
	CacheControl       string
	Metadata           map[string]string
	StorageClass       types.StorageClass
	LockMode           types.ObjectLockMode
	RetainUntil        *time.Time
	LegalHold          types.ObjectLockLegalHoldStatus
	Tags               []types.Tag
}

// AttrsFromHead reads the attributes a source HEAD can see.
//
// Tags are not among them: HeadObject never returns object tags, they come from
// GetObjectTagging. The caller fills Tags in before hashing (see copyObject), or
// the tag component of the fingerprint is silently dropped and tag drift becomes
// invisible.
func AttrsFromHead(out *s3.HeadObjectOutput) ObjectAttrs {
	return ObjectAttrs{
		ContentType:        aws.ToString(out.ContentType),
		ContentDisposition: aws.ToString(out.ContentDisposition),
		ContentEncoding:    aws.ToString(out.ContentEncoding),
		CacheControl:       aws.ToString(out.CacheControl),
		Metadata:           out.Metadata,
		StorageClass:       out.StorageClass,
		LockMode:           out.ObjectLockMode,
		RetainUntil:        out.ObjectLockRetainUntilDate,
		LegalHold:          out.ObjectLockLegalHoldStatus,
	}
}

// liveRetention reports whether a retention deadline is still in force. A
// RetainUntilDate in the past makes S3 reject the whole copy, and it protects
// nothing, so it is dropped from both the copy request and the fingerprint.
func (a ObjectAttrs) liveRetention(now time.Time) bool {
	return a.RetainUntil != nil && a.RetainUntil.After(now)
}

// WithEffectiveStorageClass returns a copy of a with StorageClass replaced by sc
// when sc is non-empty. Callers must apply this before Hash() so that the
// fingerprint describes what the target will actually carry, not what the
// source had.
func (a ObjectAttrs) WithEffectiveStorageClass(sc string) ObjectAttrs {
	if sc == "" {
		return a
	}
	a.StorageClass = types.StorageClass(sc)
	return a
}

// withTags returns a copy of a with the source's tags attached. It is the one
// place the input to Hash() is assembled, so the copy path, the attribute retry
// and the drift sweep cannot fingerprint different states.
func (a ObjectAttrs) withTags(tags []types.Tag) ObjectAttrs {
	a.Tags = tags
	return a
}

// Hash is a canonical fingerprint of the replicated characteristics. It is
// stable across map iteration order and metadata key case, so the same object
// state always yields the same value; an empty hash means "unknown" (an entry
// written before this feature existed) and never "matches".
//
// StorageClass is hashed as given, so callers must set it to the effective target
// class first with WithEffectiveStorageClass: the writer and the reconciler must
// fingerprint the same state or they never agree.
func (a ObjectAttrs) Hash() string {
	now := time.Now().UTC()

	// Metadata keys are lowercased because S3 treats them case-insensitively:
	// `B` and `b` are the same header, and must not fingerprint differently.
	meta := make([]string, 0, len(a.Metadata))
	for k, v := range a.Metadata {
		meta = append(meta, strings.ToLower(k)+"="+v)
	}
	sort.Strings(meta)

	tags := make([]string, 0, len(a.Tags))
	for _, t := range a.Tags {
		tags = append(tags, aws.ToString(t.Key)+"="+aws.ToString(t.Value))
	}
	sort.Strings(tags)

	fields := []string{
		"metadata", strings.Join(meta, "\x00"),
		"tags", strings.Join(tags, "\x00"),
		"content-type", a.ContentType,
		"content-disposition", a.ContentDisposition,
		"content-encoding", a.ContentEncoding,
		"cache-control", a.CacheControl,
	}
	if a.liveRetention(now) {
		fields = append(fields,
			"lock-mode", string(a.LockMode),
			"retain-until", a.RetainUntil.UTC().Format(time.RFC3339),
		)
	}
	fields = append(fields,
		"legal-hold", string(a.LegalHold),
		"storage-class", string(a.StorageClass),
	)

	sum := sha256.Sum256([]byte(strings.Join(fields, "\x00")))
	return hex.EncodeToString(sum[:])
}

// copyInput builds the CopyObject request that carries the attributes.
//
// Metadata only replicates with x-amz-metadata-directive: REPLACE — COPY is not
// reliable on Cubbit DS3 — so every header is passed explicitly and an absent
// value is left unset rather than sent empty. Object lock is enforced on
// CopyObject itself as directives; there is no follow-up PutObjectRetention.
// Tags never ride on this request: the endpoint does not apply x-amz-tagging, so
// copyObject writes them with PutObjectTagging instead.
func (a ObjectAttrs) copyInput(bucket, key, copySource string, storageClass string) *s3.CopyObjectInput {
	in := &s3.CopyObjectInput{
		Bucket:            &bucket,
		Key:               &key,
		CopySource:        &copySource,
		MetadataDirective: types.MetadataDirectiveReplace,
	}
	if a.ContentType != "" {
		in.ContentType = &a.ContentType
	}
	if a.ContentDisposition != "" {
		in.ContentDisposition = &a.ContentDisposition
	}
	if a.ContentEncoding != "" {
		in.ContentEncoding = &a.ContentEncoding
	}
	if a.CacheControl != "" {
		in.CacheControl = &a.CacheControl
	}
	if len(a.Metadata) > 0 {
		in.Metadata = a.Metadata
	}

	// The pair's override wins; otherwise the source class is preserved so a
	// re-copy does not silently move the object to a different tier.
	switch {
	case storageClass != "":
		in.StorageClass = types.StorageClass(storageClass)
	case a.StorageClass != "":
		in.StorageClass = a.StorageClass
	}

	if a.liveRetention(time.Now().UTC()) {
		in.ObjectLockMode = a.LockMode
		in.ObjectLockRetainUntilDate = a.RetainUntil
	}
	// A legal hold has no date of its own, so it is valid without a retention.
	in.ObjectLockLegalHoldStatus = a.LegalHold

	return in
}
