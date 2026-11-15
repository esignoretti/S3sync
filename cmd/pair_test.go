package cmd

import (
	"errors"
	"testing"
	"time"

	"github.com/esignoretti/S3sync/internal/config"
	"github.com/esignoretti/S3sync/internal/sync"
)

func TestValidatePairOptions(t *testing.T) {
	past := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	earlier := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	later := time.Date(2027, 6, 1, 0, 0, 0, 0, time.UTC)
	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)

	t.Run("compliance needs explicit acceptance", func(t *testing.T) {
		_, err := sync.ValidatePairOptions(&config.SyncPair{}, "COMPLIANCE", 0, false, false, nil, now)
		if !errors.Is(err, sync.ErrComplianceNeedsConfirm) {
			t.Fatalf("err = %v, want ErrComplianceNeedsConfirm", err)
		}
	})

	t.Run("compliance accepted", func(t *testing.T) {
		if _, err := sync.ValidatePairOptions(&config.SyncPair{}, "COMPLIANCE", 0, true, false, nil, now); err != nil {
			t.Fatalf("err = %v", err)
		}
	})

	// The bucket CLI stores whatever the operator typed, so a lowercase mode
	// reaches validation verbatim. Comparing case-sensitively would let it
	// through the gate while still being sent to the endpoint unchanged.
	t.Run("compliance is matched case-insensitively", func(t *testing.T) {
		_, err := sync.ValidatePairOptions(&config.SyncPair{}, "compliance", 0, false, false, nil, now)
		if !errors.Is(err, sync.ErrComplianceNeedsConfirm) {
			t.Fatalf("err = %v, want ErrComplianceNeedsConfirm", err)
		}
	})

	t.Run("past expiry rejected", func(t *testing.T) {
		p := &config.SyncPair{TargetExpireAt: &past}
		_, err := sync.ValidatePairOptions(p, "GOVERNANCE", 0, true, true, nil, now)
		if !errors.Is(err, sync.ErrExpireInPast) {
			t.Fatalf("err = %v, want ErrExpireInPast", err)
		}
	})

	t.Run("non-midnight expiry rejected", func(t *testing.T) {
		noon := time.Date(2027, 6, 1, 12, 0, 0, 0, time.UTC)
		p := &config.SyncPair{TargetExpireAt: &noon}
		_, err := sync.ValidatePairOptions(p, "GOVERNANCE", 0, true, true, nil, now)
		if !errors.Is(err, sync.ErrExpireNotMidnight) {
			t.Fatalf("err = %v, want ErrExpireNotMidnight", err)
		}
	})

	t.Run("moving expiry earlier needs confirmation", func(t *testing.T) {
		p := &config.SyncPair{TargetExpireAt: &earlier}
		_, err := sync.ValidatePairOptions(p, "GOVERNANCE", 0, true, false, &later, now)
		if !errors.Is(err, sync.ErrExpireEarlierNeedsConfirm) {
			t.Fatalf("err = %v, want ErrExpireEarlierNeedsConfirm", err)
		}
		if _, err := sync.ValidatePairOptions(p, "GOVERNANCE", 0, true, true, &later, now); err != nil {
			t.Fatalf("with confirmation: %v", err)
		}
	})

	t.Run("moving expiry later needs no confirmation", func(t *testing.T) {
		p := &config.SyncPair{TargetExpireAt: &later}
		if _, err := sync.ValidatePairOptions(p, "GOVERNANCE", 0, true, false, &earlier, now); err != nil {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("noncurrent days without newer versions is allowed", func(t *testing.T) {
		p := &config.SyncPair{NoncurrentExpireDays: 5}
		if _, err := sync.ValidatePairOptions(p, "GOVERNANCE", 0, true, true, nil, now); err != nil {
			t.Fatalf("err = %v", err)
		}
	})
}

func hasCode(warns []sync.AdvisoryWarning, code string) bool {
	for _, w := range warns {
		if w.Code == code {
			return true
		}
	}
	return false
}

func TestValidatePairOptionsWarnings(t *testing.T) {
	expire := time.Date(2027, 6, 1, 0, 0, 0, 0, time.UTC)
	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)

	t.Run("noncurrent recommended when lock is required without noncurrent expiry", func(t *testing.T) {
		p := &config.SyncPair{RequireObjectLock: true, TargetExpireAt: &expire}
		warns, err := sync.ValidatePairOptions(p, "GOVERNANCE", 0, true, false, nil, now)
		if err != nil {
			t.Fatalf("err = %v", err)
		}
		if !hasCode(warns, "noncurrent-recommended") {
			t.Fatalf("warnings = %v", warns)
		}
	})

	t.Run("no noncurrent warning when noncurrent expiry is set", func(t *testing.T) {
		p := &config.SyncPair{RequireObjectLock: true, TargetExpireAt: &expire, NoncurrentExpireDays: 30}
		warns, _ := sync.ValidatePairOptions(p, "GOVERNANCE", 0, true, false, nil, now)
		if hasCode(warns, "noncurrent-recommended") {
			t.Fatalf("warnings = %v", warns)
		}
	})

	t.Run("retention past expiry warns", func(t *testing.T) {
		p := &config.SyncPair{TargetExpireAt: &expire}
		// 200 days of retention from 2026-11-15 lands in 2027-06, past the expiry.
		warns, err := sync.ValidatePairOptions(p, "COMPLIANCE", 200, true, false, nil, now)
		if err != nil {
			t.Fatalf("err = %v", err)
		}
		if !hasCode(warns, "retention-past-expiry") {
			t.Fatalf("warnings = %v", warns)
		}
	})

	t.Run("retention inside the expiry does not warn", func(t *testing.T) {
		p := &config.SyncPair{TargetExpireAt: &expire}
		warns, err := sync.ValidatePairOptions(p, "GOVERNANCE", 30, true, false, nil, now)
		if err != nil {
			t.Fatalf("err = %v", err)
		}
		if hasCode(warns, "retention-past-expiry") {
			t.Fatalf("warnings = %v", warns)
		}
	})

	t.Run("clearing an expiry warns that the lifecycle rule survives", func(t *testing.T) {
		p := &config.SyncPair{}
		warns, err := sync.ValidatePairOptions(p, "GOVERNANCE", 0, true, false, &expire, now)
		if err != nil {
			t.Fatalf("err = %v", err)
		}
		if !hasCode(warns, "expiry-cleared") {
			t.Fatalf("warnings = %v", warns)
		}
	})
}
