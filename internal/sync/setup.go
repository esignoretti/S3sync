package sync

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/esignoretti/S3sync/internal/config"
)

func SetupTargetBucket(ctx context.Context, client *s3.Client, srcRegion string, tgt *config.Bucket) error {
	exists, err := bucketExists(ctx, client, tgt.BucketName)
	if err != nil {
		return fmt.Errorf("check target bucket: %w", err)
	}
	if !exists {
		return createAndConfigureBucket(ctx, client, srcRegion, tgt)
	}
	warnIfMismatch(ctx, client, tgt)
	return nil
}

func bucketExists(ctx context.Context, client *s3.Client, bucket string) (bool, error) {
	_, err := client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)})
	if err != nil {
		var nf *s3types.NotFound
		if errors.As(err, &nf) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func createAndConfigureBucket(ctx context.Context, client *s3.Client, srcRegion string, tgt *config.Bucket) error {
	slog.Info("creating target bucket", "bucket", tgt.BucketName, "region", srcRegion)

	input := &s3.CreateBucketInput{Bucket: aws.String(tgt.BucketName)}
	if tgt.ObjectLock {
		input.ObjectLockEnabledForBucket = aws.Bool(true)
	}
	if srcRegion != "us-east-1" {
		input.CreateBucketConfiguration = &s3types.CreateBucketConfiguration{
			LocationConstraint: s3types.BucketLocationConstraint(srcRegion),
		}
	}
	if _, err := client.CreateBucket(ctx, input); err != nil {
		return fmt.Errorf("create bucket: %w", err)
	}

	if tgt.Versioning {
		slog.Info("enabling versioning", "bucket", tgt.BucketName)
		if _, err := client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
			Bucket: aws.String(tgt.BucketName),
			VersioningConfiguration: &s3types.VersioningConfiguration{
				Status: s3types.BucketVersioningStatusEnabled,
			},
		}); err != nil {
			return fmt.Errorf("enable versioning: %w", err)
		}
	}

	if tgt.ObjectLock {
		slog.Info("enabling object lock", "bucket", tgt.BucketName)
		if _, err := client.PutObjectLockConfiguration(ctx, &s3.PutObjectLockConfigurationInput{
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

func warnIfMismatch(ctx context.Context, client *s3.Client, tgt *config.Bucket) {
	if tgt.Versioning {
		vOut, err := client.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: aws.String(tgt.BucketName)})
		if err != nil {
			slog.Warn("cannot check target versioning", "bucket", tgt.BucketName, "error", err)
		} else if vOut.Status != s3types.BucketVersioningStatusEnabled {
			slog.Warn("target bucket missing versioning (requested but not configured)", "bucket", tgt.BucketName)
		}
	}

	if tgt.ObjectLock {
		olOut, err := client.GetObjectLockConfiguration(ctx, &s3.GetObjectLockConfigurationInput{Bucket: aws.String(tgt.BucketName)})
		if err != nil {
			slog.Warn("cannot check target object lock", "bucket", tgt.BucketName, "error", err)
		} else if olOut.ObjectLockConfiguration == nil || olOut.ObjectLockConfiguration.ObjectLockEnabled != s3types.ObjectLockEnabledEnabled {
			slog.Warn("target bucket missing object lock (requested but not configured)", "bucket", tgt.BucketName)
		}
	}
}
