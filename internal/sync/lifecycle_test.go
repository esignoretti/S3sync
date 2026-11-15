package sync

import (
	"errors"
	"testing"
	"time"

	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/esignoretti/S3sync/internal/config"
)

func TestValidateExpireAt(t *testing.T) {
	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name    string
		in      time.Time
		wantErr error
	}{
		{"midnight future", time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC), nil},
		{"midnight future other tz", time.Date(2027, 1, 1, 0, 0, 0, 0, time.FixedZone("x", 3600)), nil},
		{"not midnight", time.Date(2027, 1, 1, 9, 30, 0, 0, time.UTC), ErrExpireNotMidnight},
		{"past", time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), ErrExpireInPast},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ValidateExpireAt(tt.in, now)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestBuildLifecycleRules(t *testing.T) {
	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	exp := time.Date(2027, 3, 1, 0, 0, 0, 0, time.UTC)

	t.Run("both rules", func(t *testing.T) {
		rules, err := BuildLifecycleRules(&config.SyncPair{
			TargetExpireAt: &exp, NoncurrentExpireDays: 7, NewerNoncurrentVersions: 2,
		}, now)
		if err != nil {
			t.Fatal(err)
		}
		if len(rules) != 2 {
			t.Fatalf("got %d rules, want 2", len(rules))
		}
		if rules[0].ID == nil || *rules[0].ID != "s3sync-expire" {
			t.Fatalf("rule 0 id = %v", rules[0].ID)
		}
		if rules[0].Expiration == nil || rules[0].Expiration.Date == nil || !rules[0].Expiration.Date.Equal(exp) {
			t.Fatalf("rule 0 expiration = %+v", rules[0].Expiration)
		}
		if rules[0].Expiration.Days != nil || rules[0].Expiration.ExpiredObjectDeleteMarker != nil {
			t.Fatal("expiration must carry only Date")
		}
		if rules[1].NoncurrentVersionExpiration == nil ||
			*rules[1].NoncurrentVersionExpiration.NoncurrentDays != 7 ||
			*rules[1].NoncurrentVersionExpiration.NewerNoncurrentVersions != 2 {
			t.Fatalf("rule 1 noncurrent = %+v", rules[1].NoncurrentVersionExpiration)
		}
		for _, r := range rules {
			if r.Status != s3types.ExpirationStatusEnabled {
				t.Fatalf("rule %v status = %v", *r.ID, r.Status)
			}
			if r.Filter != nil || r.Prefix != nil {
				t.Fatal("filters and prefixes are unsupported by the target")
			}
		}
	})

	t.Run("noncurrent only", func(t *testing.T) {
		rules, err := BuildLifecycleRules(&config.SyncPair{NoncurrentExpireDays: 3}, now)
		if err != nil {
			t.Fatal(err)
		}
		if len(rules) != 1 || rules[0].NoncurrentVersionExpiration == nil {
			t.Fatalf("rules = %+v", rules)
		}
		if rules[0].NoncurrentVersionExpiration.NewerNoncurrentVersions != nil {
			t.Fatal("NewerNoncurrentVersions must be omitted when zero")
		}
	})

	t.Run("neither set", func(t *testing.T) {
		rules, err := BuildLifecycleRules(&config.SyncPair{}, now)
		if err != nil {
			t.Fatal(err)
		}
		if len(rules) != 0 {
			t.Fatalf("got %d rules, want 0", len(rules))
		}
	})

	t.Run("invalid date propagates", func(t *testing.T) {
		bad := time.Date(2027, 3, 1, 5, 0, 0, 0, time.UTC)
		_, err := BuildLifecycleRules(&config.SyncPair{TargetExpireAt: &bad}, now)
		if !errors.Is(err, ErrExpireNotMidnight) {
			t.Fatalf("err = %v", err)
		}
	})
}
