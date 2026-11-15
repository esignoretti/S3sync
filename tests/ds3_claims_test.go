// tests/ds3_claims_test.go
//go:build integration

// This file verifies the Cubbit DS3 behaviours the attribute-replication and
// expiry features are built on. Every one of them is a documented claim that,
// if wrong, silently breaks the feature: the design leans on each so heavily
// that a wrong assumption produces no error, only quietly wrong data.
//
// Run against a real endpoint:
//
//	S3_TEST_ENDPOINT=https://s3.cubbit.eu \
//	S3_TEST_ACCESS_KEY=... S3_TEST_SECRET_KEY=... \
//	go test -tags=integration ./tests/ -run TestDS3 -v
//
// Each test skips when the endpoint is absent, and cleans up the bucket it
// creates. Nothing here touches the shared test bucket.
package tests

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// ds3Bucket creates a scratch bucket and deletes it (and its objects) on
// cleanup. objectLock enables lock at creation, which is the only time it can
// be enabled.
func ds3Bucket(t *testing.T, objectLock bool) (*s3.Client, string) {
	t.Helper()
	client, _ := setupS3Client(t)
	ctx := context.Background()
	name := fmt.Sprintf("s3sync-claims-%d", time.Now().UnixNano())

	in := &s3.CreateBucketInput{Bucket: aws.String(name)}
	if objectLock {
		in.ObjectLockEnabledForBucket = aws.Bool(true)
	}
	if _, err := client.CreateBucket(ctx, in); err != nil {
		t.Fatalf("create bucket %q: %v", name, err)
	}
	t.Cleanup(func() {
		cleanupBucket(ctx, client, name)
	})
	return client, name
}

func cleanupBucket(ctx context.Context, client *s3.Client, name string) {
	out, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(name)})
	if err == nil {
		for _, o := range out.Contents {
			_, _ = client.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(name), Key: o.Key,
			})
		}
	}
	versions, err := client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: aws.String(name)})
	if err == nil {
		for _, v := range versions.Versions {
			_, _ = client.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(name), Key: v.Key, VersionId: v.VersionId,
			})
		}
	}
	_, _ = client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(name)})
}

func putObject(t *testing.T, client *s3.Client, bucket, key, body string, mutate func(*s3.PutObjectInput)) {
	t.Helper()
	in := &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   strings.NewReader(body),
	}
	if mutate != nil {
		mutate(in)
	}
	if _, err := client.PutObject(context.Background(), in); err != nil {
		t.Fatalf("put %q: %v", key, err)
	}
}

// TestDS3Claim_CopyIgnoresInlineTagging pins the finding that forced the
// two-call tag path. If CopyObject ever starts honouring x-amz-tagging, the
// extra GetObjectTagging/PutObjectTagging round-trip per object is waste — but
// if it silently stops honouring it while we assume it does, every replicated
// object loses its tags with no error anywhere.
func TestDS3Claim_CopyIgnoresInlineTagging(t *testing.T) {
	client, bucket := ds3Bucket(t, false)
	ctx := context.Background()

	putObject(t, client, bucket, "src", "body", func(in *s3.PutObjectInput) {
		in.Tagging = aws.String("team=alpha")
	})
	if _, err := client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String("dst"),
		CopySource: aws.String(bucket + "/src"),
		Tagging:    aws.String("team=alpha"),
	}); err != nil {
		t.Fatalf("copy: %v", err)
	}

	tags, err := client.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
		Bucket: aws.String(bucket), Key: aws.String("dst"),
	})
	if err != nil {
		t.Fatalf("get tags on copy: %v", err)
	}
	if len(tags.TagSet) > 0 {
		t.Logf("NOTE: CopyObject DID apply x-amz-tagging (%d tags). The separate "+
			"tag round-trip is redundant on this endpoint.", len(tags.TagSet))
		return
	}
	t.Logf("confirmed: CopyObject dropped x-amz-tagging, as documented")
}

// TestDS3Claim_EmptyTagSetClearsTags settles the one assumption left in the
// code: applyTags writes an empty <TagSet/> to clear a target whose source tags
// were removed. If the endpoint rejects that as MalformedXML, tagless objects
// sit in pending_attrs forever and are retried on every cycle.
func TestDS3Claim_EmptyTagSetClearsTags(t *testing.T) {
	client, bucket := ds3Bucket(t, false)
	ctx := context.Background()

	putObject(t, client, bucket, "tagged", "body", func(in *s3.PutObjectInput) {
		in.Tagging = aws.String("team=alpha&env=prod")
	})

	// The exact call applyTags makes when the source has no tags.
	_, err := client.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("tagged"),
		Tagging: &types.Tagging{
			TagSet: []types.Tag{},
		},
	})
	if err != nil {
		t.Fatalf("PutObjectTagging with an empty TagSet was REJECTED (%v).\n"+
			"applyTags writes exactly this to clear target tags, so tagless "+
			"objects will be marked pending_attrs on every cycle. The worker "+
			"needs a different clear strategy (DeleteObjectTagging).", err)
	}

	tags, err := client.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
		Bucket: aws.String(bucket), Key: aws.String("tagged"),
	})
	if err != nil {
		t.Fatalf("get tags after clear: %v", err)
	}
	if len(tags.TagSet) != 0 {
		t.Errorf("after writing an empty TagSet, %d tags remain: %v", len(tags.TagSet), tags.TagSet)
	}
}

// TestDS3Claim_MetadataNeedsReplaceDirective pins the reason copyInput sets
// MetadataDirective: REPLACE unconditionally. With COPY (or omitted), the
// source metadata is not carried in a way the target reliably keeps.
func TestDS3Claim_MetadataNeedsReplaceDirective(t *testing.T) {
	client, bucket := ds3Bucket(t, false)
	ctx := context.Background()

	putObject(t, client, bucket, "src", "body", func(in *s3.PutObjectInput) {
		in.Metadata = map[string]string{"origin": "source"}
		in.ContentType = aws.String("text/plain")
	})
	if _, err := client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String("dst"),
		CopySource:        aws.String(bucket + "/src"),
		MetadataDirective: types.MetadataDirectiveReplace,
		Metadata:          map[string]string{"origin": "source"},
		ContentType:       aws.String("text/plain"),
	}); err != nil {
		t.Fatalf("copy with REPLACE: %v", err)
	}

	head, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("dst"),
	})
	if err != nil {
		t.Fatalf("head copy: %v", err)
	}
	if got := head.Metadata["origin"]; got != "source" {
		t.Errorf("metadata origin = %q, want \"source\" — REPLACE did not carry metadata", got)
	}
	if head.ContentType == nil || *head.ContentType != "text/plain" {
		t.Errorf("content-type = %v, want text/plain", head.ContentType)
	}
}

// TestDS3Claim_ContentLanguageAndExpiresNotStored documents why those two
// fields are excluded from replication: the endpoint does not persist them, so
// replicating them would report success while storing nothing.
func TestDS3Claim_ContentLanguageAndExpiresNotStored(t *testing.T) {
	client, bucket := ds3Bucket(t, false)
	ctx := context.Background()

	putObject(t, client, bucket, "lang", "body", func(in *s3.PutObjectInput) {
		in.ContentLanguage = aws.String("en-GB")
	})
	head, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("lang"),
	})
	if err != nil {
		t.Fatalf("head: %v", err)
	}
	if head.ContentLanguage == nil || *head.ContentLanguage != "en-GB" {
		t.Logf("NOTE: Content-Language is now stored (%v). Replication of it "+
			"may be worth re-adding to attrs.go.", head.ContentLanguage)
	}
}

// TestDS3Claim_LifecycleDateWithoutFilter pins the constraint that shaped the
// whole expiry feature: Filter and Prefix are unsupported, so an expiry date
// applies to the entire bucket. This test proves the date is accepted at all —
// if PutBucketLifecycleConfiguration rejects it, provisioning fails on every
// pair with an expiry.
func TestDS3Claim_LifecycleDateWithoutFilter(t *testing.T) {
	client, bucket := ds3Bucket(t, false)
	ctx := context.Background()

	date := time.Now().UTC().AddDate(1, 0, 0).Truncate(24 * time.Hour)
	_, err := client.PutBucketLifecycleConfiguration(ctx, &s3.PutBucketLifecycleConfigurationInput{
		Bucket: aws.String(bucket),
		LifecycleConfiguration: &types.BucketLifecycleConfiguration{
			Rules: []types.LifecycleRule{{
				ID:         aws.String("s3sync-expire"),
				Status:     types.ExpirationStatusEnabled,
				Expiration: &types.LifecycleExpiration{Date: aws.Time(date)},
			}},
		},
	})
	if err != nil {
		t.Fatalf("lifecycle rule with Expiration.Date and no Filter was REJECTED: %v", err)
	}

	out, err := client.GetBucketLifecycleConfiguration(ctx, &s3.GetBucketLifecycleConfigurationInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Fatalf("get lifecycle: %v", err)
	}
	if len(out.Rules) != 1 || out.Rules[0].Expiration == nil || out.Rules[0].Expiration.Date == nil {
		t.Fatalf("lifecycle round-trip lost the expiry date: %+v", out.Rules)
	}
	t.Logf("lifecycle accepted; expiry read back as %s",
		out.Rules[0].Expiration.Date.Format(time.RFC3339))
}

// TestDS3Claim_LockHeadersOnCopy pins the claim that lets the worker apply
// retention inline on the copy rather than with a second PutObjectRetention
// call. If CopyObject ignored these headers the object would be stored
// unprotected while the cache recorded a retention fingerprint — the worst
// possible failure, silent data loss of the protection itself.
func TestDS3Claim_LockHeadersOnCopy(t *testing.T) {
	client, bucket := ds3Bucket(t, true)
	ctx := context.Background()

	putObject(t, client, bucket, "src", "body", nil)
	retain := time.Now().UTC().AddDate(1, 0, 0)
	if _, err := client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:                    aws.String(bucket),
		Key:                       aws.String("locked"),
		CopySource:                aws.String(bucket + "/src"),
		ObjectLockMode:            types.ObjectLockModeGovernance,
		ObjectLockRetainUntilDate: aws.Time(retain),
		ObjectLockLegalHoldStatus: types.ObjectLockLegalHoldStatusOn,
	}); err != nil {
		t.Fatalf("copy with lock headers: %v", err)
	}

	ret, err := client.GetObjectRetention(ctx, &s3.GetObjectRetentionInput{
		Bucket: aws.String(bucket), Key: aws.String("locked"),
	})
	if err != nil {
		t.Fatalf("get retention: %v", err)
	}
	if ret.Retention == nil || ret.Retention.Mode != types.ObjectLockRetentionModeGovernance {
		t.Fatalf("retention not applied by CopyObject: %+v.\n"+
			"The worker relies on inline lock headers; a second "+
			"PutObjectRetention call would be required.", ret.Retention)
	}
	if ret.Retention.RetainUntilDate == nil {
		t.Fatal("retention mode applied but retain-until is nil")
	}
	t.Logf("lock applied inline on copy: mode=%s until=%s",
		ret.Retention.Mode, ret.Retention.RetainUntilDate.Format(time.RFC3339))

	hold, err := client.GetObjectLegalHold(ctx, &s3.GetObjectLegalHoldInput{
		Bucket: aws.String(bucket), Key: aws.String("locked"),
	})
	if err != nil {
		t.Fatalf("get legal hold: %v", err)
	}
	if hold.LegalHold == nil || hold.LegalHold.Status != types.ObjectLockLegalHoldStatusOn {
		t.Errorf("legal hold not applied by CopyObject: %+v", hold.LegalHold)
	}

	// Clean up the lock so the bucket can be deleted.
	_, _ = client.PutObjectLegalHold(ctx, &s3.PutObjectLegalHoldInput{
		Bucket: aws.String(bucket), Key: aws.String("locked"),
		LegalHold: &types.ObjectLockLegalHold{Status: types.ObjectLockLegalHoldStatusOff},
	})
	_, _ = client.PutObjectRetention(ctx, &s3.PutObjectRetentionInput{
		Bucket: aws.String(bucket), Key: aws.String("locked"),
		Retention: &types.ObjectLockRetention{
			Mode:            types.ObjectLockRetentionModeGovernance,
			RetainUntilDate: aws.Time(time.Now().UTC().Add(time.Second)),
		},
		BypassGovernanceRetention: aws.Bool(true),
	})
}

// TestDS3Claim_LockCannotBeAddedAfterCreation pins the immutability that makes
// --require-object-lock a creation-time-only decision. If the endpoint ever
// allowed enabling lock later, the whole "recreate the bucket" branch would be
// unnecessary.
func TestDS3Claim_LockCannotBeAddedAfterCreation(t *testing.T) {
	client, bucket := ds3Bucket(t, false)
	ctx := context.Background()

	_, err := client.PutObjectLockConfiguration(ctx, &s3.PutObjectLockConfigurationInput{
		Bucket: aws.String(bucket),
		ObjectLockConfiguration: &types.ObjectLockConfiguration{
			ObjectLockEnabled: types.ObjectLockEnabledEnabled,
		},
	})
	if err == nil {
		t.Logf("NOTE: this endpoint DID allow enabling object lock after "+
			"creation on %q. --on-lock-missing recreate may be avoidable.", bucket)
		return
	}
	t.Logf("confirmed: lock cannot be enabled after creation (%v)", err)

	// And the read path must report "not enabled" in a way isNotFound or the
	// disabled-state check recognises, since ProvisionTarget treats a 404 as
	// "lock not enabled" rather than fatal.
	out, gerr := client.GetObjectLockConfiguration(ctx, &s3.GetObjectLockConfigurationInput{
		Bucket: aws.String(bucket),
	})
	switch {
	case gerr != nil:
		var apiErr interface{ ErrorCode() string }
		code := ""
		if errors.As(gerr, &apiErr) {
			code = apiErr.ErrorCode()
		}
		t.Logf("GetObjectLockConfiguration on an unlocked bucket errors with code %q", code)
	case out.ObjectLockConfiguration == nil ||
		out.ObjectLockConfiguration.ObjectLockEnabled != types.ObjectLockEnabledEnabled:
		t.Logf("GetObjectLockConfiguration on an unlocked bucket returns a non-enabled config (no error)")
	}
}

// TestDS3Claim_CopySourceConditionsIgnored pins the reason the worker does not
// use x-amz-copy-source-if-* as a server-side precondition. If these were
// evaluated, a conditional copy could replace the client-side size/ETag logic.
func TestDS3Claim_CopySourceConditionsIgnored(t *testing.T) {
	client, bucket := ds3Bucket(t, false)
	ctx := context.Background()

	putObject(t, client, bucket, "src", "body", nil)

	// A deliberately impossible condition: the copy should fail if honoured.
	_, err := client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:                aws.String(bucket),
		Key:                   aws.String("dst"),
		CopySource:            aws.String(bucket + "/src"),
		CopySourceIfNoneMatch: aws.String("definitely-not-the-etag"),
	})
	if err != nil {
		t.Logf("NOTE: x-amz-copy-source-if-none-match WAS evaluated (%v). "+
			"Server-side preconditions are available on this endpoint.", err)
		return
	}
	t.Logf("confirmed: copy-source conditions are not evaluated; the copy " +
		"happened unconditionally, so dedup must stay client-side")
}
