package sync

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/esignoretti/S3sync/internal/config"
)

// SetupTargetBucket provisions the target bucket with no pair-level
// requirements. See ProvisionTarget for the decision table.
func SetupTargetBucket(ctx context.Context, client *s3.Client, srcRegion string, tgt *config.Bucket) error {
	return ProvisionTarget(ctx, client, srcRegion, tgt, &config.SyncPair{}, ProvisionOptions{Now: time.Now().UTC()})
}
