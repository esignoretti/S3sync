package sync

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/esignoretti/S3sync/internal/config"
)

var (
	// ErrComplianceNeedsConfirm indicates a COMPLIANCE default retention was
	// requested without the operator's explicit acceptance.
	ErrComplianceNeedsConfirm = errors.New("compliance retention needs explicit confirmation")
	// ErrExpireEarlierNeedsConfirm indicates a target expiry was moved earlier
	// without the operator's explicit confirmation.
	ErrExpireEarlierNeedsConfirm = errors.New("moving target expiry earlier needs confirmation")
)

// AdvisoryWarning is a condition worth telling the operator about that must not
// block the change. It is returned only on the success path; when validation
// fails, no warnings are produced.
type AdvisoryWarning struct {
	Code    string // "noncurrent-recommended", "retention-past-expiry", "expiry-cleared"
	Message string
}

// ValidatePairOptions checks a pair's attribute and lock settings before they are
// written, and returns the advisory warnings that should be shown with them.
//
// Errors are blocking. Warnings never are: they describe a configuration that
// works but will surprise the operator later, so the pair is still created.
//
// retentionMode and retentionDays are the target bucket's default retention, not
// the pair's: they live on config.Bucket, and the retention-past-expiry warning
// is about the bucket's default outliving the pair's expiry. retentionMode is
// empty when the bucket has no default. previousExpireAt is the pair's current
// TargetExpireAt, nil for a new pair. now may be zero, in which case the wall
// clock is used.
func ValidatePairOptions(pair *config.SyncPair, retentionMode string, retentionDays int,
	acceptCompliance, confirmExpireEarlier bool, previousExpireAt *time.Time, now time.Time) ([]AdvisoryWarning, error) {

	if now.IsZero() {
		now = time.Now().UTC()
	}
	now = now.UTC()

	// COMPLIANCE retention cannot be shortened or bypassed by anyone, including
	// the operator, so accepting it is a decision that has to be deliberate.
	// Compared case-insensitively: the bucket CLI stores whatever the operator
	// typed, so "compliance" is reachable and would otherwise slip past the gate
	// while still being sent to the endpoint verbatim.
	if strings.EqualFold(retentionMode, "COMPLIANCE") && !acceptCompliance {
		return nil, fmt.Errorf("%w: COMPLIANCE retention cannot be shortened or bypassed, "+
			"not even by an administrator; pass --accept-compliance to proceed", ErrComplianceNeedsConfirm)
	}

	if pair.TargetExpireAt != nil {
		if _, err := ValidateExpireAt(*pair.TargetExpireAt, now); err != nil {
			return nil, err
		}
		// Shortening the life of a target destroys data sooner than promised.
		if previousExpireAt != nil && pair.TargetExpireAt.Before(*previousExpireAt) && !confirmExpireEarlier {
			return nil, fmt.Errorf("%w: %s is earlier than the current expiry %s; "+
				"pass --confirm-expire-earlier to proceed", ErrExpireEarlierNeedsConfirm,
				pair.TargetExpireAt.UTC().Format(time.RFC3339), previousExpireAt.UTC().Format(time.RFC3339))
		}
	}

	return advisoryWarnings(pair, retentionMode, retentionDays, previousExpireAt, now), nil
}

// advisoryWarnings computes the non-blocking notices, in a stable order so the
// CLI prints them predictably.
func advisoryWarnings(pair *config.SyncPair, retentionMode string, retentionDays int, previousExpireAt *time.Time, now time.Time) []AdvisoryWarning {
	var warns []AdvisoryWarning

	// Lock without a noncurrent-version rule means every overwrite keeps an
	// undeletable old version forever: retention outlives the object's own life
	// and lifecycle deletion skips a version that is still locked.
	if pair.RequireObjectLock && pair.NoncurrentExpireDays == 0 {
		warns = append(warns, AdvisoryWarning{
			Code: "noncurrent-recommended",
			Message: "object lock is required but no noncurrent version expiry is set: " +
				"each overwrite will keep a locked old version indefinitely; " +
				"consider --noncurrent-expire-days",
		})
	}

	// A default retention that outlasts the target expiry means lifecycle
	// deletion silently skips the very objects the expiry was set to remove.
	if retentionMode != "" && retentionDays > 0 && pair.TargetExpireAt != nil {
		retained := now.Add(time.Duration(retentionDays) * 24 * time.Hour)
		if retained.After(*pair.TargetExpireAt) {
			warns = append(warns, AdvisoryWarning{
				Code: "retention-past-expiry",
				Message: fmt.Sprintf("the bucket's %d-day %s retention outlives the target expiry %s: "+
					"lifecycle deletion skips versions whose retention has not expired, "+
					"so objects will stay in the target past the expiry date",
					retentionDays, retentionMode, pair.TargetExpireAt.UTC().Format(time.RFC3339)),
			})
		}
	}

	// Clearing the date does not remove the lifecycle rule that was written for
	// it: an empty rule set is skipped rather than PUT, so the existing
	// s3sync-expire rule keeps expiring the whole bucket.
	if previousExpireAt != nil && pair.TargetExpireAt == nil {
		warns = append(warns, AdvisoryWarning{
			Code: "expiry-cleared",
			Message: "the target expiry date was cleared, but the existing s3sync-expire lifecycle " +
				"rule is NOT removed by this change: it expires every object in the target bucket. " +
				"Clear it on the bucket manually if that is not what you want",
		})
	}

	return warns
}
