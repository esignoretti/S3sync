package cmd

import (
	"errors"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"github.com/esignoretti/S3sync/internal/config"
	"github.com/esignoretti/S3sync/internal/sync"
)

func TestValidatePairOptions(t *testing.T) {
	past := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	earlier := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	later := time.Date(2027, 6, 1, 0, 0, 0, 0, time.UTC)
	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)

	t.Run("compliance needs explicit acceptance", func(t *testing.T) {
		_, err := sync.ValidatePairOptions(&config.SyncPair{}, "COMPLIANCE", 0, false, false, nil, false, now)
		if !errors.Is(err, sync.ErrComplianceNeedsConfirm) {
			t.Fatalf("err = %v, want ErrComplianceNeedsConfirm", err)
		}
	})

	t.Run("compliance accepted", func(t *testing.T) {
		if _, err := sync.ValidatePairOptions(&config.SyncPair{}, "COMPLIANCE", 0, true, false, nil, false, now); err != nil {
			t.Fatalf("err = %v", err)
		}
	})

	// The bucket CLI stores whatever the operator typed, so a lowercase mode
	// reaches validation verbatim. Comparing case-sensitively would let it
	// through the gate while still being sent to the endpoint unchanged.
	t.Run("compliance is matched case-insensitively", func(t *testing.T) {
		_, err := sync.ValidatePairOptions(&config.SyncPair{}, "compliance", 0, false, false, nil, false, now)
		if !errors.Is(err, sync.ErrComplianceNeedsConfirm) {
			t.Fatalf("err = %v, want ErrComplianceNeedsConfirm", err)
		}
	})

	t.Run("past expiry rejected", func(t *testing.T) {
		p := &config.SyncPair{TargetExpireAt: &past}
		_, err := sync.ValidatePairOptions(p, "GOVERNANCE", 0, true, true, nil, false, now)
		if !errors.Is(err, sync.ErrExpireInPast) {
			t.Fatalf("err = %v, want ErrExpireInPast", err)
		}
	})

	t.Run("non-midnight expiry rejected", func(t *testing.T) {
		noon := time.Date(2027, 6, 1, 12, 0, 0, 0, time.UTC)
		p := &config.SyncPair{TargetExpireAt: &noon}
		_, err := sync.ValidatePairOptions(p, "GOVERNANCE", 0, true, true, nil, false, now)
		if !errors.Is(err, sync.ErrExpireNotMidnight) {
			t.Fatalf("err = %v, want ErrExpireNotMidnight", err)
		}
	})

	t.Run("moving expiry earlier needs confirmation", func(t *testing.T) {
		p := &config.SyncPair{TargetExpireAt: &earlier}
		_, err := sync.ValidatePairOptions(p, "GOVERNANCE", 0, true, false, &later, false, now)
		if !errors.Is(err, sync.ErrExpireEarlierNeedsConfirm) {
			t.Fatalf("err = %v, want ErrExpireEarlierNeedsConfirm", err)
		}
		if _, err := sync.ValidatePairOptions(p, "GOVERNANCE", 0, true, true, &later, false, now); err != nil {
			t.Fatalf("with confirmation: %v", err)
		}
	})

	t.Run("moving expiry later needs no confirmation", func(t *testing.T) {
		p := &config.SyncPair{TargetExpireAt: &later}
		if _, err := sync.ValidatePairOptions(p, "GOVERNANCE", 0, true, false, &earlier, false, now); err != nil {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("noncurrent days without newer versions is allowed", func(t *testing.T) {
		p := &config.SyncPair{NoncurrentExpireDays: 5}
		if _, err := sync.ValidatePairOptions(p, "GOVERNANCE", 0, true, true, nil, false, now); err != nil {
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
		warns, err := sync.ValidatePairOptions(p, "GOVERNANCE", 0, true, false, nil, false, now)
		if err != nil {
			t.Fatalf("err = %v", err)
		}
		if !hasCode(warns, "noncurrent-recommended") {
			t.Fatalf("warnings = %v", warns)
		}
	})

	t.Run("no noncurrent warning when noncurrent expiry is set", func(t *testing.T) {
		p := &config.SyncPair{RequireObjectLock: true, TargetExpireAt: &expire, NoncurrentExpireDays: 30}
		warns, _ := sync.ValidatePairOptions(p, "GOVERNANCE", 0, true, false, nil, false, now)
		if hasCode(warns, "noncurrent-recommended") {
			t.Fatalf("warnings = %v", warns)
		}
	})

	t.Run("retention past expiry warns", func(t *testing.T) {
		p := &config.SyncPair{TargetExpireAt: &expire}
		// 200 days of retention from 2026-11-15 lands in 2027-06, past the expiry.
		warns, err := sync.ValidatePairOptions(p, "COMPLIANCE", 200, true, false, nil, false, now)
		if err != nil {
			t.Fatalf("err = %v", err)
		}
		if !hasCode(warns, "retention-past-expiry") {
			t.Fatalf("warnings = %v", warns)
		}
	})

	t.Run("retention inside the expiry does not warn", func(t *testing.T) {
		p := &config.SyncPair{TargetExpireAt: &expire}
		warns, err := sync.ValidatePairOptions(p, "GOVERNANCE", 30, true, false, nil, false, now)
		if err != nil {
			t.Fatalf("err = %v", err)
		}
		if hasCode(warns, "retention-past-expiry") {
			t.Fatalf("warnings = %v", warns)
		}
	})

	t.Run("clearing an expiry warns that the lifecycle rule survives", func(t *testing.T) {
		p := &config.SyncPair{}
		warns, err := sync.ValidatePairOptions(p, "GOVERNANCE", 0, true, false, &expire, false, now)
		if err != nil {
			t.Fatalf("err = %v", err)
		}
		if !hasCode(warns, "expiry-cleared") {
			t.Fatalf("warnings = %v", warns)
		}
	})

	t.Run("enabling replication warns that earlier objects are not backfilled", func(t *testing.T) {
		p := &config.SyncPair{ReplicateAttributes: true}
		warns, err := sync.ValidatePairOptions(p, "GOVERNANCE", 0, true, false, nil, true, now)
		if err != nil {
			t.Fatalf("err = %v", err)
		}
		if !hasCode(warns, "attrs-not-backfilled") {
			t.Fatalf("warnings = %v", warns)
		}
	})

	t.Run("noncurrent without expiry warns that the expire rule is dropped", func(t *testing.T) {
		p := &config.SyncPair{NoncurrentExpireDays: 30}
		warns, err := sync.ValidatePairOptions(p, "GOVERNANCE", 0, true, false, nil, false, now)
		if err != nil {
			t.Fatalf("err = %v", err)
		}
		if !hasCode(warns, "expire-rule-dropped") {
			t.Fatalf("warnings = %v", warns)
		}
	})

	t.Run("replication already on does not warn about backfill", func(t *testing.T) {
		// Saving an unrelated edit on a pair that already replicates must not
		// re-nag about a backfill the operator was told about once already.
		p := &config.SyncPair{ReplicateAttributes: true}
		warns, err := sync.ValidatePairOptions(p, "GOVERNANCE", 0, true, false, nil, false, now)
		if err != nil {
			t.Fatalf("err = %v", err)
		}
		if hasCode(warns, "attrs-not-backfilled") {
			t.Fatalf("warnings = %v", warns)
		}
	})
}

// A flag that is applied but never registered is invisible; a flag that is
// registered but never applied is a silent no-op. Both halves are asserted.
func TestApplyPairFlagsDryRun(t *testing.T) {
	for _, c := range []*cobra.Command{pairAddCmd, pairUpdateCmd} {
		if c.Flags().Lookup("dry-run") == nil {
			t.Fatalf("%s: --dry-run is not registered", c.Name())
		}
	}

	c := pairUpdateCmd
	if err := c.Flags().Set("dry-run", "true"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Flags().Set("dry-run", "false") })

	p := &config.SyncPair{}
	if err := applyPairFlags(c, p, false); err != nil {
		t.Fatal(err)
	}
	if !p.DryRun {
		t.Fatal("dry-run flag was not applied")
	}
}

// An unknown subcommand used to print help and exit 0, so a typo in a cron entry
// ("pair synk <id>") silently did nothing and reported success. This asserts the
// exit path a shell actually sees, not just that cobra has the subcommand.
func TestUnknownSubcommandIsAnError(t *testing.T) {
	// Args must be set on the ROOT: Command.Execute on a child delegates to the
	// root, which reads the root's args. Setting them on the child silently
	// leaves the root parsing the test binary's own flags, and every assertion
	// below would pass for the wrong reason.
	for _, group := range []string{"pair", "bucket", "config"} {
		rootCmd.SetArgs([]string{group, "wibble"})
		if err := rootCmd.Execute(); err == nil {
			t.Errorf("%s wibble: err = nil, want an error: a typo must not exit 0", group)
		}
	}

	// A bare group command must still succeed: that is how its help is reached.
	for _, group := range []string{"pair", "bucket", "config"} {
		rootCmd.SetArgs([]string{group})
		if err := rootCmd.Execute(); err != nil {
			t.Errorf("%s with no subcommand: err = %v, want nil", group, err)
		}
	}
	rootCmd.SetArgs(nil)
}
