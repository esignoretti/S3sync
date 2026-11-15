package config

import (
	"database/sql"
	"testing"
	"time"
)

func setupDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestMigrate(t *testing.T) {
	db := setupDB(t)
	rows, err := db.Query("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var tables []string
	for rows.Next() {
		var n string
		rows.Scan(&n)
		tables = append(tables, n)
	}
	if len(tables) < 2 {
		t.Fatalf("expected >=2 tables, got %d", len(tables))
	}
}

// TestMigrationLegacySyncPairs verifies that a database file created before the
// attribute/lock columns existed is upgraded in place: existing rows read back
// with the SQL-level defaults.
func TestMigrationLegacySyncPairs(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/legacy.db"

	// Build a pre-change database with the old sync_pairs schema and one row.
	legacy, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Format(time.RFC3339)
	for _, m := range []string{
		`CREATE TABLE buckets (
			id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE,
			endpoint TEXT NOT NULL, region TEXT NOT NULL,
			access_key TEXT NOT NULL, secret_key TEXT NOT NULL,
			bucket_name TEXT NOT NULL,
			object_lock INTEGER NOT NULL DEFAULT 0,
			versioning INTEGER NOT NULL DEFAULT 0,
			retention_mode TEXT, retention_days INTEGER,
			created_at TEXT NOT NULL, updated_at TEXT NOT NULL)`,
		`CREATE TABLE sync_pairs (
			id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE,
			source_bucket_id TEXT NOT NULL REFERENCES buckets(id),
			target_bucket_id TEXT NOT NULL REFERENCES buckets(id),
			sync_interval INTEGER NOT NULL DEFAULT 300,
			worker_count INTEGER NOT NULL DEFAULT 10,
			max_get_ops_per_minute INTEGER NOT NULL DEFAULT 0,
			delete_propagation INTEGER NOT NULL DEFAULT 1,
			target_storage_class TEXT,
			enabled INTEGER NOT NULL DEFAULT 1,
			last_sync_at TEXT, last_sync_status TEXT NOT NULL DEFAULT '',
			consecutive_errors INTEGER NOT NULL DEFAULT 0,
			created_at TEXT NOT NULL, updated_at TEXT NOT NULL)`,
	} {
		if _, err := legacy.Exec(m); err != nil {
			legacy.Close()
			t.Fatal(err)
		}
	}
	_, err = legacy.Exec(
		`INSERT INTO sync_pairs (id,name,source_bucket_id,target_bucket_id,
		 sync_interval,worker_count,max_get_ops_per_minute,delete_propagation,
		 target_storage_class,enabled,last_sync_at,last_sync_status,
		 consecutive_errors,created_at,updated_at)
		 VALUES ('p1','old pair','s','t',300,10,0,1,NULL,1,NULL,'',0,?,?)`,
		now, now)
	if err != nil {
		legacy.Close()
		t.Fatal(err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}

	// Open upgrades the schema via migrate().
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	r := NewRepository(db)
	got, err := r.GetSyncPair("p1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "old pair" {
		t.Fatalf("legacy row not read: %+v", got)
	}
	if got.OnLockMissing != "fail" {
		t.Fatalf("OnLockMissing = %q, want fail", got.OnLockMissing)
	}
	if got.ReplicateAttributes || got.RequireObjectLock {
		t.Fatalf("bool fields must default off: %+v", got)
	}
	if got.NoncurrentExpireDays != 0 || got.NewerNoncurrentVersions != 0 || got.AttrsReconcileInterval != 0 {
		t.Fatalf("int fields must default 0: %+v", got)
	}
	if got.ProvisionError != "" {
		t.Fatalf("ProvisionError = %q, want empty", got.ProvisionError)
	}
	if got.TargetExpireAt != nil || got.ProvisionedAt != nil || got.LastReconcileAt != nil {
		t.Fatalf("nullable times must be nil: %+v", got)
	}
}
