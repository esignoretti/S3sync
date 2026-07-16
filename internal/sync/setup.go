package sync

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type TargetConfig struct {
	BucketName    string
	Versioning    bool
	ObjectLock    bool
	RetentionMode string
	RetentionDays int
}

// SetupTargetBucket ensures the target bucket exists with the desired config.
// 1. HEAD bucket — if 404, create it
// 2. Configure versioning if requested
// 3. Configure object lock if requested (requires versioning)
// 4. If bucket exists but config doesn't match, log warnings (non-fatal)
func SetupTargetBucket(ctx context.Context, client *s3.Client, srcRegion string, cfg TargetConfig) error {
	exists, err := bucketExists(ctx, client, cfg.BucketName)
	if err != nil {
		return fmt.Errorf("check target bucket: %w", err)
	}

	if !exists {
		return createAndConfigureBucket(ctx, client, srcRegion, cfg)
	}

	warnIfMismatch(ctx, client, cfg)
	return nil
}

func bucketExists(ctx context.Context, client *s3.Client, bucket string) (bool, error) {
	_, err := client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		var nf *s3types.NotFound
		if errors.As(err, &nf) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func createAndConfigureBucket(ctx context.Context, client *s3.Client, srcRegion string, cfg TargetConfig) error {
	slog.Info("creating target bucket", "bucket", cfg.BucketName, "region", srcRegion)

	createInput := &s3.CreateBucketInput{
		Bucket: aws.String(cfg.BucketName),
	}

	if cfg.ObjectLock {
		createInput.ObjectLockEnabledForBucket = aws.Bool(true)
	}

	if srcRegion != "us-east-1" {
		createInput.CreateBucketConfiguration = &s3types.CreateBucketConfiguration{
			LocationConstraint: s3types.BucketLocationConstraint(srcRegion),
		}
	}

	if _, err := client.CreateBucket(ctx, createInput); err != nil {
		return fmt.Errorf("create bucket: %w", err)
	}

	if cfg.Versioning {
		slog.Info("enabling versioning", "bucket", cfg.BucketName)
		if _, err := client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
			Bucket: aws.String(cfg.BucketName),
			VersioningConfiguration: &s3types.VersioningConfiguration{
				Status: s3types.BucketVersioningStatusEnabled,
			},
		}); err != nil {
			return fmt.Errorf("enable versioning: %w", err)
		}
	}

	if cfg.ObjectLock {
		slog.Info("enabling object lock", "bucket", cfg.BucketName)
		if _, err := client.PutObjectLockConfiguration(ctx, &s3.PutObjectLockConfigurationInput{
			Bucket: aws.String(cfg.BucketName),
			ObjectLockConfiguration: &s3types.ObjectLockConfiguration{
				ObjectLockEnabled: s3types.ObjectLockEnabledEnabled,
				Rule:              retentionRule(cfg),
			},
		}); err != nil {
			return fmt.Errorf("enable object lock: %w", err)
		}
	}

	return nil
}

func retentionRule(cfg TargetConfig) *s3types.ObjectLockRule {
	if cfg.RetentionMode == "" {
		return nil
	}
	mode := s3types.ObjectLockRetentionMode(cfg.RetentionMode)
	days := int32(cfg.RetentionDays)
	if days < 1 {
		days = 1
	}
	return &s3types.ObjectLockRule{
		DefaultRetention: &s3types.DefaultRetention{
			Mode: mode,
			Days: aws.Int32(days),
		},
	}
}

func warnIfMismatch(ctx context.Context, client *s3.Client, cfg TargetConfig) {
	if !cfg.Versioning && !cfg.ObjectLock {
		return
	}

	if cfg.Versioning {
		vOut, err := client.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{
			Bucket: aws.String(cfg.BucketName),
		})
		if err != nil {
			slog.Warn("cannot check target versioning", "bucket", cfg.BucketName, "error", err)
		} else if vOut.Status != s3types.BucketVersioningStatusEnabled {
			slog.Warn("target bucket missing versioning (requested but not configured)",
				"bucket", cfg.BucketName)
		}
	}

	if cfg.ObjectLock {
		olOut, err := client.GetObjectLockConfiguration(ctx, &s3.GetObjectLockConfigurationInput{
			Bucket: aws.String(cfg.BucketName),
		})
		if err != nil {
			slog.Warn("cannot check target object lock", "bucket", cfg.BucketName, "error", err)
		} else if olOut.ObjectLockConfiguration == nil ||
			olOut.ObjectLockConfiguration.ObjectLockEnabled != s3types.ObjectLockEnabledEnabled {
			slog.Warn("target bucket missing object lock (requested but not configured)",
				"bucket", cfg.BucketName)
		}
	}
}
