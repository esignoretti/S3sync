package sync

import (
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/esignoretti/S3sync/internal/config"
)

var (
	// ErrExpireNotMidnight indicates TargetExpireAt is not midnight UTC after conversion.
	ErrExpireNotMidnight = errors.New("expire_at is not midnight UTC")
	// ErrExpireInPast indicates TargetExpireAt is not in the future relative to now.
	ErrExpireInPast = errors.New("expire_at is not in the future")
)

// ValidateExpireAt checks t is midnight UTC (after conversion) and after now,
// returning t in UTC.
func ValidateExpireAt(t time.Time, now time.Time) (time.Time, error) {
	utc := t.UTC()
	if utc.Hour() != 0 || utc.Minute() != 0 || utc.Second() != 0 || utc.Nanosecond() != 0 {
		return time.Time{}, fmt.Errorf("%w: %s (must be midnight UTC)", ErrExpireNotMidnight, utc.Format(time.RFC3339))
	}
	if !utc.After(now) {
		return time.Time{}, fmt.Errorf("%w: %s (must be after %s)", ErrExpireInPast, utc.Format(time.RFC3339), now.UTC().Format(time.RFC3339))
	}
	return utc, nil
}

// BuildLifecycleRules builds target bucket lifecycle rules from the pair's
// provisioning settings. Rules never set Filter or Prefix because the target
// S3 implementation rejects them.
func BuildLifecycleRules(p *config.SyncPair, now time.Time) ([]s3types.LifecycleRule, error) {
	var rules []s3types.LifecycleRule
	if p.TargetExpireAt != nil {
		exp, err := ValidateExpireAt(*p.TargetExpireAt, now)
		if err != nil {
			return nil, err
		}
		id := "s3sync-expire"
		rules = append(rules, s3types.LifecycleRule{
			ID:     &id,
			Status: s3types.ExpirationStatusEnabled,
			Expiration: &s3types.LifecycleExpiration{
				Date: &exp,
			},
		})
	}
	if p.NoncurrentExpireDays > 0 {
		id := "s3sync-noncurrent"
		nve := &s3types.NoncurrentVersionExpiration{
			NoncurrentDays: aws.Int32(int32(p.NoncurrentExpireDays)),
		}
		if p.NewerNoncurrentVersions > 0 {
			nve.NewerNoncurrentVersions = aws.Int32(int32(p.NewerNoncurrentVersions))
		}
		rules = append(rules, s3types.LifecycleRule{
			ID:                          &id,
			Status:                      s3types.ExpirationStatusEnabled,
			NoncurrentVersionExpiration: nve,
		})
	}
	return rules, nil
}
