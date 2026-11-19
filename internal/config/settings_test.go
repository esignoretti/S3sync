package config

import (
	"path/filepath"
	"testing"
)

func settingsTestRepo(t *testing.T) *Repository {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "config.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return NewRepository(db)
}

func TestGetSettingsDefaultsWhenAbsent(t *testing.T) {
	s, err := settingsTestRepo(t).GetSettings()
	if err != nil {
		t.Fatal(err)
	}
	if s.RunTimeoutSeconds != DefaultRunTimeoutSeconds {
		t.Fatalf("RunTimeoutSeconds = %d, want %d", s.RunTimeoutSeconds, DefaultRunTimeoutSeconds)
	}
}

func TestSetSettingRejectsUnknownKey(t *testing.T) {
	if err := settingsTestRepo(t).SetSetting("nope", "1"); err == nil {
		t.Fatal("unknown key was accepted")
	}
}

func TestSetSettingRejectsOutOfRange(t *testing.T) {
	r := settingsTestRepo(t)
	for _, v := range []string{"0", "29", "86401", "abc"} {
		if err := r.SetSetting(SettingRunTimeoutSeconds, v); err == nil {
			t.Fatalf("value %q was accepted", v)
		}
	}
}

func TestSetSettingRoundTrips(t *testing.T) {
	r := settingsTestRepo(t)
	if err := r.SetSetting(SettingRunTimeoutSeconds, "1800"); err != nil {
		t.Fatal(err)
	}
	s, err := r.GetSettings()
	if err != nil {
		t.Fatal(err)
	}
	if s.RunTimeoutSeconds != 1800 {
		t.Fatalf("RunTimeoutSeconds = %d, want 1800", s.RunTimeoutSeconds)
	}
}

func TestGetSettingsUnparseableValueFallsBack(t *testing.T) {
	r := settingsTestRepo(t)
	// Write past validation to prove a corrupt row cannot stop a run. Going
	// through SetSetting cannot reach this state (it validates), so a row that
	// is already corrupt — written by an older build, or by hand — is what this
	// reproduces.
	if _, err := r.db.Exec(`INSERT INTO settings (key, value) VALUES (?, ?)`,
		SettingRunTimeoutSeconds, "garbage"); err != nil {
		t.Fatal(err)
	}
	s, err := r.GetSettings()
	if err != nil {
		t.Fatalf("GetSettings returned an error for a corrupt row: %v", err)
	}
	if s.RunTimeoutSeconds != DefaultRunTimeoutSeconds {
		t.Fatalf("RunTimeoutSeconds = %d, want the default", s.RunTimeoutSeconds)
	}
}
