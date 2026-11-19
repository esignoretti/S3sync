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
	// MaxWorkerCount bounds the worker pool. Each worker holds one in-flight
	// request, so the practical limit is the endpoint's connection budget; the
	// cap exists to reject typos, not to tune throughput.
	MaxWorkerCount = 256

	// ErrInvalidWorkerCount indicates a worker count outside 1..MaxWorkerCount.
	// Zero is the dangerous one: no goroutine drains the actions channel, so a
	// run silently copies nothing and reports status "ok".
	ErrInvalidWorkerCount = errors.New("invalid worker count")
	// ErrInvalidSyncInterval indicates a sync interval that is not positive.
	// Zero makes the serve loop spin as fast as it can, which hammers both
	// buckets and the config database.
	ErrInvalidSyncInterval = errors.New("invalid sync interval")
	// ErrExpireEarlierNeedsConfirm indicates a target expiry was moved earlier
	// without the operator's explicit confirmation.
	ErrExpireEarlierNeedsConfirm = errors.New("moving target expiry earlier needs confirmation")
)

// AdvisoryWarning is a condition worth telling the operator about that must not
// block the change. It is returned only on the success path; when validation
// fails, no warnings are produced.
type AdvisoryWarning struct {
	Code    string `json:"code"` // "noncurrent-recommended", "retention-past-expiry", "expiry-cleared"
	Message string `json:"message"`
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
// TargetExpireAt, nil for a new pair. replicationWasOff reports that this call
// is turning replicate_attributes on, which is what makes the backfill advisory
// fire; callers with no previous value pass false. now may be zero, in which
// case the wall clock is used.
func ValidatePairOptions(pair *config.SyncPair, retentionMode string, retentionDays int,
	acceptCompliance, confirmExpireEarlier bool, previousExpireAt *time.Time,
	replicationWasOff bool, now time.Time) ([]AdvisoryWarning, error) {

	if now.IsZero() {
		now = time.Now().UTC()
	}
	now = now.UTC()

	// A zero or negative worker count is not a small problem: with no goroutine
	// draining the actions channel the run copies NOTHING and still reports
	// status "ok", so a pair created with --workers 0 looks healthy forever while
	// the target stays empty. Negative values are equally meaningless.
	if pair.WorkerCount < 1 || pair.WorkerCount > MaxWorkerCount {
		return nil, fmt.Errorf("%w: %d is outside 1..%d", ErrInvalidWorkerCount,
			pair.WorkerCount, MaxWorkerCount)
	}

	// A non-positive interval makes the serve loop fire back to back with no
	// delay: measured at 79 sync cycles in 8 seconds for interval=0, each one
	// listing both buckets and writing the config database.
	if pair.SyncInterval <= 0 {
		return nil, fmt.Errorf("%w: %ds must be positive", ErrInvalidSyncInterval, pair.SyncInterval)
	}

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

	return advisoryWarnings(pair, retentionMode, retentionDays, previousExpireAt, replicationWasOff, now), nil
}

// advisoryWarnings computes the non-blocking notices, in a stable order so the
// CLI prints them predictably.
func advisoryWarnings(pair *config.SyncPair, retentionMode string, retentionDays int,
	previousExpireAt *time.Time, replicationWasOff bool, now time.Time) []AdvisoryWarning {
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

	// The lifecycle configuration is rewritten wholesale from the pair's own
	// fields. A pair with a noncurrent rule but no expiry therefore PUTs a config
	// that contains only s3sync-noncurrent, silently dropping a foreign
	// s3sync-expire rule that was written when the pair still had a date. The
	// previous config is logged at warn, but nothing else tells the operator.
	if pair.NoncurrentExpireDays > 0 && pair.TargetExpireAt == nil {
		warns = append(warns, AdvisoryWarning{
			Code: "expire-rule-dropped",
			Message: "this pair writes only the noncurrent-version lifecycle rule, so any " +
				"s3sync-expire rule left on the target bucket from an earlier expiry date is " +
				"removed: the target will no longer expire its objects on that date",
		})
	}

	// Objects cached before replication was enabled carry no attribute
	// fingerprint, and the sweep deliberately treats an empty fingerprint as
	// "unknown" rather than "drifted". Nothing backfills them, so their
	// characteristics only reach the target when their bytes change.
	//
	// The wording names metadata and tags explicitly rather than saying "their
	// characteristics": a CopyObject with no MetadataDirective still carries both
	// by endpoint default, so those did arrive. What did not arrive is the
	// fingerprint, which means nothing ever checks or repairs them — retention
	// and any later edit are what actually go missing.
	if pair.ReplicateAttributes && replicationWasOff {
		warns = append(warns, AdvisoryWarning{
			Code: "attrs-not-backfilled",
			Message: "objects synced before attribute replication was enabled have no " +
				"attribute fingerprint, so their retention and any later metadata or tag " +
				"edits are never checked or repaired; run `pair reset` to force a full " +
				"re-copy if you need them reconciled",
		})
	}

	return warns
}
