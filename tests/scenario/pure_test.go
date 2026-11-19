// tests/scenario/pure_test.go
//
// Deliberately NOT behind //go:build integration: these assertions are pure
// functions with no endpoint involved, so gating them would mean they never run
// in the default suite — the one place a cheap regression check is worth most.
package scenario

import (
	"errors"
	"testing"
	"time"

	appconfig "github.com/esignoretti/S3sync/internal/config"
	appsync "github.com/esignoretti/S3sync/internal/sync"
)

// TestReconcileOffByDefault pins that no sweep runs when the interval is unset,
// so the opt-in stays opt-in.
func TestReconcileOffByDefault(t *testing.T) {
	pair := &appconfig.SyncPair{}
	if appsync.ShouldReconcile(pair, time.Now().UTC()) {
		t.Fatal("ShouldReconcile is true with no interval configured")
	}
	pair.AttrsReconcileInterval = 60
	pair.LastReconcileAt = nil
	if !appsync.ShouldReconcile(pair, time.Now().UTC()) {
		t.Fatal("ShouldReconcile is false for a due pair with no previous reconcile")
	}
	now := time.Now().UTC()
	pair.LastReconcileAt = &now
	if appsync.ShouldReconcile(pair, now.Add(30*time.Second)) {
		t.Fatal("ShouldReconcile is true before the interval has elapsed")
	}
	if !appsync.ShouldReconcile(pair, now.Add(61*time.Second)) {
		t.Fatal("ShouldReconcile is false after the interval has elapsed")
	}
}

// TestPreflightRejectsPastDate covers the preflight half of the expiry rule: a
// past date is ErrExpireInPast at pair-creation time, where the operator can act
// on it before any pair is stored. At RUN time the engine's own guard answers
// with ErrPairExpired instead, because a past date then means "this pair has
// expired" rather than "your input is malformed".
func TestPreflightRejectsPastDate(t *testing.T) {
	past := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	pair := &appconfig.SyncPair{TargetExpireAt: &past}
	_, err := appsync.ValidatePairOptions(pair, "", 0, false, true, nil, false, time.Now().UTC())
	if !errors.Is(err, appsync.ErrExpireInPast) {
		t.Fatalf("preflight error = %v, want ErrExpireInPast", err)
	}
}

// TestPreflightRejectsNonMidnightDate pins the other expiry validation: a Date
// that is not midnight UTC is rejected here rather than deferred into an opaque
// provisioning failure from the endpoint.
func TestPreflightRejectsNonMidnightDate(t *testing.T) {
	at := time.Now().UTC().AddDate(0, 0, 30).Truncate(24 * time.Hour).Add(time.Hour)
	pair := &appconfig.SyncPair{TargetExpireAt: &at}
	_, err := appsync.ValidatePairOptions(pair, "", 0, false, true, nil, false, time.Now().UTC())
	if !errors.Is(err, appsync.ErrExpireNotMidnight) {
		t.Fatalf("preflight error = %v, want ErrExpireNotMidnight", err)
	}
}
