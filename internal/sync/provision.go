package sync

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/esignoretti/S3sync/internal/config"
)

// targetAPI is the subset of the S3 API the provisioning preflight needs.
// *s3.Client satisfies it directly; tests substitute a fake.
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

// ProvisionOptions carries the operator's explicit consent to destroy a target
// bucket, plus the clock the expiry validation runs against.
type ProvisionOptions struct {
	ForceRecreate bool
	Now           time.Time
}

var (
	// ErrLockMissing indicates the target bucket exists without object lock.
	// Cubbit DS3 can only enable lock at CreateBucket, so such a bucket cannot
	// be converted in place.
	ErrLockMissing = errors.New("target bucket exists without object lock and cannot be converted")
	// ErrTargetNotEmpty indicates a recreate was requested for a bucket that
	// still holds objects.
	ErrTargetNotEmpty = errors.New("refusing to recreate a non-empty target bucket")
)

// ProvisionTarget makes the target bucket satisfy the pair's provisioning
// requirements or fails with an actionable error. It is idempotent: a target
// that already meets the requirements is left in place.
func ProvisionTarget(ctx context.Context, api targetAPI, srcRegion string, tgt *config.Bucket, pair *config.SyncPair, opts ProvisionOptions) error {
	now := opts.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}

	// Build (and therefore validate) the lifecycle rules before touching the
	// bucket, so an invalid expire_at can never mutate anything.
	rules, err := BuildLifecycleRules(pair, now)
	if err != nil {
		return fmt.Errorf("lifecycle rules: %w", err)
	}

	exists, err := bucketExists(ctx, api, tgt.BucketName)
	if err != nil {
		return fmt.Errorf("check target bucket: %w", err)
	}

	lockRequired := tgt.ObjectLock || pair.RequireObjectLock

	if exists && pair.RequireObjectLock {
		locked, err := objectLockEnabled(ctx, api, tgt.BucketName)
		if err != nil {
			return fmt.Errorf("check target object lock: %w", err)
		}
		if !locked {
			if pair.OnLockMissing != "recreate" {
				return fmt.Errorf("%w: use --on-lock-missing recreate with --force-recreate on an empty bucket", ErrLockMissing)
			}
			if !opts.ForceRecreate {
				return fmt.Errorf("%w: --on-lock-missing recreate requires --force-recreate", ErrLockMissing)
			}
			if err := ensureBucketEmpty(ctx, api, tgt.BucketName); err != nil {
				return err
			}
			slog.Warn("deleting unlocked target bucket to recreate it with object lock",
				"bucket", tgt.BucketName)
			if _, err := api.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(tgt.BucketName)}); err != nil {
				return fmt.Errorf("delete unlocked target bucket: %w", err)
			}
			exists = false
		}
	}

	if !exists {
		if err := createAndConfigureBucket(ctx, api, srcRegion, tgt, lockRequired); err != nil {
			return err
		}
	}
	if err := applyLifecycle(ctx, api, tgt.BucketName, rules); err != nil {
		return err
	}

	if exists {
		warnIfMismatch(ctx, api, tgt)
	}
	return nil
}

func bucketExists(ctx context.Context, api targetAPI, bucket string) (bool, error) {
	_, err := api.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)})
	if err != nil {
		// Shared with the lifecycle checks: this endpoint answers a missing
		// bucket with a bare 404 as often as with a modelled NotFound, and the
		// provisioning step now fails the whole run, so a 404 that is not
		// recognised here would stop a pair from ever creating its bucket.
		if isNotFound(err, "NotFound", "NoSuchBucket") {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func objectLockEnabled(ctx context.Context, api targetAPI, bucket string) (bool, error) {
	out, err := api.GetObjectLockConfiguration(ctx, &s3.GetObjectLockConfigurationInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		// An endpoint that has no lock configuration to report says so with
		// either the modelled code or a bare 404; both mean "not enabled",
		// not "the check failed".
		if isNotFound(err, "ObjectLockConfigurationNotFoundError") {
			return false, nil
		}
		return false, err
	}
	if out.ObjectLockConfiguration == nil {
		return false, nil
	}
	return out.ObjectLockConfiguration.ObjectLockEnabled == s3types.ObjectLockEnabledEnabled, nil
}

func ensureBucketEmpty(ctx context.Context, api targetAPI, bucket string) error {
	out, err := api.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:  aws.String(bucket),
		MaxKeys: aws.Int32(1),
	})
	if err != nil {
		return fmt.Errorf("list target bucket objects: %w", err)
	}
	if len(out.Contents) > 0 {
		return fmt.Errorf("%w: %s holds at least one object", ErrTargetNotEmpty, bucket)
	}
	return nil
}

func createAndConfigureBucket(ctx context.Context, api targetAPI, srcRegion string, tgt *config.Bucket, lockRequired bool) error {
	slog.Info("creating target bucket", "bucket", tgt.BucketName, "region", srcRegion)

	input := &s3.CreateBucketInput{Bucket: aws.String(tgt.BucketName)}
	if lockRequired {
		input.ObjectLockEnabledForBucket = aws.Bool(true)
	}
	if srcRegion != "us-east-1" {
		input.CreateBucketConfiguration = &s3types.CreateBucketConfiguration{
			LocationConstraint: s3types.BucketLocationConstraint(srcRegion),
		}
	}
	if _, err := api.CreateBucket(ctx, input); err != nil {
		return fmt.Errorf("create bucket: %w", err)
	}

	if tgt.Versioning || lockRequired {
		slog.Info("enabling versioning", "bucket", tgt.BucketName)
		if _, err := api.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
			Bucket: aws.String(tgt.BucketName),
			VersioningConfiguration: &s3types.VersioningConfiguration{
				Status: s3types.BucketVersioningStatusEnabled,
			},
		}); err != nil {
			return fmt.Errorf("enable versioning: %w", err)
		}
	}

	if lockRequired {
		slog.Info("enabling object lock", "bucket", tgt.BucketName)
		if _, err := api.PutObjectLockConfiguration(ctx, &s3.PutObjectLockConfigurationInput{
			Bucket: aws.String(tgt.BucketName),
			ObjectLockConfiguration: &s3types.ObjectLockConfiguration{
				ObjectLockEnabled: s3types.ObjectLockEnabledEnabled,
				Rule:              retentionRule(tgt),
			},
		}); err != nil {
			return fmt.Errorf("enable object lock: %w", err)
		}
	}

	return nil
}

func retentionRule(tgt *config.Bucket) *s3types.ObjectLockRule {
	if tgt.RetentionMode == "" {
		return nil
	}
	days := int32(tgt.RetentionDays)
	if days < 1 {
		days = 1
	}
	return &s3types.ObjectLockRule{
		DefaultRetention: &s3types.DefaultRetention{
			Mode: s3types.ObjectLockRetentionMode(tgt.RetentionMode),
			Days: aws.Int32(days),
		},
	}
}

// applyLifecycle replaces the target's lifecycle configuration. Because
// PutBucketLifecycleConfiguration replaces the whole config, the previous one
// is logged first so an accidental overwrite is at least visible. A pair with
// no expiry settings produces no rules and therefore touches nothing: the
// target is left exactly as it was.
func applyLifecycle(ctx context.Context, api targetAPI, bucket string, rules []s3types.LifecycleRule) error {
	if len(rules) == 0 {
		return nil
	}

	prev, err := api.GetBucketLifecycleConfiguration(ctx, &s3.GetBucketLifecycleConfigurationInput{
		Bucket: aws.String(bucket),
	})
	if err != nil && !isNotFound(err, "NoSuchLifecycleConfiguration") {
		return fmt.Errorf("read target lifecycle configuration: %w", err)
	}
	if err == nil {
		for _, r := range prev.Rules {
			slog.Warn("replacing existing target lifecycle rule",
				"bucket", bucket, "rule", aws.ToString(r.ID),
				"expiration", r.Expiration, "noncurrent_expiration", r.NoncurrentVersionExpiration)
		}
	}

	if _, err := api.PutBucketLifecycleConfiguration(ctx, &s3.PutBucketLifecycleConfigurationInput{
		Bucket:                 aws.String(bucket),
		LifecycleConfiguration: &s3types.BucketLifecycleConfiguration{Rules: rules},
	}); err != nil {
		return fmt.Errorf("apply target lifecycle configuration: %w", err)
	}
	slog.Info("applied target lifecycle configuration", "bucket", bucket, "rules", len(rules))
	return nil
}

// isNotFound reports whether err is the endpoint's "the resource you asked
// about is absent" answer for an operation that uses one of codes. The S3
// deserializers for these operations have no typed error and fall back to
// smithy.GenericAPIError, and an empty error body is given a code derived from
// the status text ("NotFound" for 404), so a bare 404 status counts as absent
// too.
func isNotFound(err error, codes ...string) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		for _, code := range codes {
			if apiErr.ErrorCode() == code {
				return true
			}
		}
	}
	var respErr *smithyhttp.ResponseError
	return errors.As(err, &respErr) && respErr.HTTPStatusCode() == http.StatusNotFound
}

func warnIfMismatch(ctx context.Context, api targetAPI, tgt *config.Bucket) {
	if tgt.Versioning {
		vOut, err := api.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: aws.String(tgt.BucketName)})
		if err != nil {
			slog.Warn("cannot check target versioning", "bucket", tgt.BucketName, "error", err)
		} else if vOut.Status != s3types.BucketVersioningStatusEnabled {
			slog.Warn("target bucket missing versioning (requested but not configured)", "bucket", tgt.BucketName)
		}
	}

	if tgt.ObjectLock {
		olOut, err := api.GetObjectLockConfiguration(ctx, &s3.GetObjectLockConfigurationInput{Bucket: aws.String(tgt.BucketName)})
		if err != nil {
			slog.Warn("cannot check target object lock", "bucket", tgt.BucketName, "error", err)
		} else if olOut.ObjectLockConfiguration == nil || olOut.ObjectLockConfiguration.ObjectLockEnabled != s3types.ObjectLockEnabledEnabled {
			slog.Warn("target bucket missing object lock (requested but not configured)", "bucket", tgt.BucketName)
		}
	}
}
