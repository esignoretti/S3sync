// tests/scenario/scenario_expiry_test.go
//go:build integration

package scenario

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/esignoretti/S3sync/internal/sync"
)

// TestScenarioExpireDateRule pins the rule shape the endpoint accepts: a Date
// with NO Filter and NO Prefix. This endpoint rejects both, and a Prefix would
// in any case be wrong — the rule expires the whole bucket.
func TestScenarioExpireDateRule(t *testing.T) {
	e := newScenarioEnv(t, envOptions{TargetExpireIn: 60 * 24 * time.Hour})
	e.seed([]string{"obj/0001-small.bin"})
	e.sync()
	cfg, err := e.Client.GetBucketLifecycleConfiguration(context.Background(), &s3.GetBucketLifecycleConfigurationInput{
		Bucket: aws.String(e.Target),
	})
	if err != nil {
		t.Fatalf("get lifecycle: %v", err)
	}
	if len(cfg.Rules) != 1 || aws.ToString(cfg.Rules[0].ID) != "s3sync-expire" {
		t.Fatalf("rules = %v, want exactly one s3sync-expire rule", ruleIDs(cfg.Rules))
	}
	r := cfg.Rules[0]
	if r.Expiration == nil || r.Expiration.Date == nil {
		t.Fatalf("rule expiration = %+v, want a Date", r.Expiration)
	}
	if r.Filter != nil || r.Prefix != nil {
		t.Fatal("the rule carries a Filter or Prefix, which this endpoint does not support")
	}
}

// TestScenarioExpiredPairRefuses asserts ZERO writes, which pins the guard's
// POSITION in RunOnce rather than just its presence: a guard placed after
// provisioning would still return the error but would already have touched the
// target.
func TestScenarioExpiredPairRefuses(t *testing.T) {
	e := newScenarioEnv(t, envOptions{})
	e.seed([]string{"obj/0001-small.bin"})
	past := midnightUTC(-1)
	e.Pair.TargetExpireAt = &past
	err := e.syncExpectingError()
	if !errors.Is(err, sync.ErrPairExpired) {
		t.Fatalf("sync error = %v, want ErrPairExpired", err)
	}
	e.assertTargetCount(0)
	if got := e.Counter.Count(kindPutObject) + e.Counter.Count(kindCopyObject); got != 0 {
		t.Fatalf("an expired pair issued %d writes", got)
	}
}

// TestScenarioExpiryRecovery is the documented recovery path from the design:
// move the date into the future and the pair syncs again.
func TestScenarioExpiryRecovery(t *testing.T) {
	e := newScenarioEnv(t, envOptions{})
	e.seed([]string{"obj/0001-small.bin"})
	past := midnightUTC(-1)
	e.Pair.TargetExpireAt = &past
	if err := e.syncExpectingError(); !errors.Is(err, sync.ErrPairExpired) {
		t.Fatalf("sync error = %v, want ErrPairExpired", err)
	}
	future := midnightUTC(30)
	e.Pair.TargetExpireAt = &future
	e.sync()
	e.assertTargetCount(1)
}

// TestScenarioExpiryCleared records a deliberate limitation: clearing the date
// does NOT remove the existing rule, because an empty rule set is skipped rather
// than written (writing an empty Rules list is rejected by the endpoint). The
// operator has to clear it by hand, and the CLI says so in an advisory.
func TestScenarioExpiryCleared(t *testing.T) {
	e := newScenarioEnv(t, envOptions{TargetExpireIn: 30 * 24 * time.Hour})
	e.sync()
	e.Pair.TargetExpireAt = nil
	e.sync()
	cfg, err := e.Client.GetBucketLifecycleConfiguration(context.Background(), &s3.GetBucketLifecycleConfigurationInput{
		Bucket: aws.String(e.Target),
	})
	if err != nil {
		t.Fatalf("get lifecycle: %v", err)
	}
	if !contains(ruleIDs(cfg.Rules), "s3sync-expire") {
		t.Fatalf("rules = %v, want the s3sync-expire rule still present", ruleIDs(cfg.Rules))
	}
}

func TestScenarioNonMidnightRejected(t *testing.T) {
	e := newScenarioEnv(t, envOptions{})
	// One hour past midnight UTC: a date the endpoint would reject opaquely.
	e.Pair.TargetExpireAt = aws.Time(midnightUTC(30).Add(time.Hour))
	err := e.syncExpectingError()
	if !errors.Is(err, sync.ErrExpireNotMidnight) {
		t.Fatalf("sync error = %v, want ErrExpireNotMidnight", err)
	}
	// The error must quote the offending value so the operator can see it.
	if !strings.Contains(err.Error(), "T01:00:00Z") {
		t.Fatalf("error does not quote the rejected timestamp: %v", err)
	}
}

// TestScenarioPastDateRejected pins which guard answers a past date at RUN time.
//
// It is ErrPairExpired, not ErrExpireInPast: the engine's expiry guard fires
// before provisioning, so a past date means "this pair has expired, do not run"
// rather than "your input is malformed". ErrExpireInPast is the preflight's
// answer (ValidatePairOptions, internal/sync/validate.go:63), pinned by the unit
// tests in cmd/pair_test.go — reaching it here would mean the guard had moved
// after provisioning, which is the ordering the expiry scenario exists to
// protect.
func TestScenarioPastDateRejected(t *testing.T) {
	e := newScenarioEnv(t, envOptions{})
	e.Pair.TargetExpireAt = aws.Time(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC))
	err := e.syncExpectingError()
	if !errors.Is(err, sync.ErrPairExpired) {
		t.Fatalf("sync error = %v, want ErrPairExpired", err)
	}
	// And it must have touched nothing.
	e.assertTargetCount(0)
}

// TestScenarioPreflightRejectsPastDate covers the other half: the same date is
// ErrExpireInPast at pair-creation time, which is where the operator can act on
// it before any pair is stored.
func TestScenarioPreflightRejectsPastDate(t *testing.T) {
	e := newScenarioEnv(t, envOptions{})
	past := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	e.Pair.TargetExpireAt = &past
	_, err := sync.ValidatePairOptions(e.Pair, "", 0, false, true, nil, false, time.Now().UTC())
	if !errors.Is(err, sync.ErrExpireInPast) {
		t.Fatalf("preflight error = %v, want ErrExpireInPast", err)
	}
}

// TestScenarioNoncurrentAndExpireRulesTogether is the only way both rules can
// coexist: BuildLifecycleRules emits s3sync-expire and s3sync-noncurrent, and
// the endpoint must accept them in one configuration.
func TestScenarioNoncurrentAndExpireRulesTogether(t *testing.T) {
	e := newScenarioEnv(t, envOptions{Versioning: true, TargetExpireIn: 60 * 24 * time.Hour})
	e.Pair.NoncurrentExpireDays = 7
	e.sync()
	cfg, err := e.Client.GetBucketLifecycleConfiguration(context.Background(), &s3.GetBucketLifecycleConfigurationInput{
		Bucket: aws.String(e.Target),
	})
	if err != nil {
		t.Fatalf("get lifecycle: %v", err)
	}
	ids := ruleIDs(cfg.Rules)
	if len(ids) != 2 || !contains(ids, "s3sync-expire") || !contains(ids, "s3sync-noncurrent") {
		t.Fatalf("rules = %v, want both s3sync-expire and s3sync-noncurrent", ids)
	}
	// Each rule must carry only the field it exists for: this endpoint rejects
	// ExpiredObjectDeleteMarker alongside a Date.
	for _, r := range cfg.Rules {
		if aws.ToString(r.ID) == "s3sync-expire" && r.Expiration != nil && r.Expiration.ExpiredObjectDeleteMarker != nil {
			t.Fatal("the expire rule carries ExpiredObjectDeleteMarker, which is forbidden with a Date")
		}
	}
}
