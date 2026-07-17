package config

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

func Open(path string) (*sql.DB, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("mkdir: %w", err)
	}
	db, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("ping: %w", err)
	}
	if err := migrate(db); err != nil {
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return db, nil
}

func migrate(db *sql.DB) error {
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS buckets (
		id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE,
		endpoint TEXT NOT NULL, region TEXT NOT NULL,
		access_key TEXT NOT NULL, secret_key TEXT NOT NULL,
		bucket_name TEXT NOT NULL,
		object_lock INTEGER NOT NULL DEFAULT 0,
		versioning INTEGER NOT NULL DEFAULT 0,
		retention_mode TEXT, retention_days INTEGER,
		created_at TEXT NOT NULL, updated_at TEXT NOT NULL
	)`); err != nil {
		return err
	}

	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS sync_pairs (
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
		created_at TEXT NOT NULL, updated_at TEXT NOT NULL
	)`); err != nil {
		return err
	}

	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS sync_logs (
		id TEXT PRIMARY KEY, pair_id TEXT NOT NULL REFERENCES sync_pairs(id),
		status TEXT NOT NULL, error_msg TEXT NOT NULL DEFAULT '',
		succeeded INTEGER NOT NULL DEFAULT 0, failed INTEGER NOT NULL DEFAULT 0,
		started_at TEXT NOT NULL, completed_at TEXT NOT NULL
	)`); err != nil {
		return err
	}

	// Migrations for columns added after initial schema
	migrations := []string{
		`ALTER TABLE sync_pairs ADD COLUMN dry_run INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE sync_pairs ADD COLUMN webhook_url TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE sync_pairs ADD COLUMN webhook_events TEXT NOT NULL DEFAULT 'error'`,
	}
	for _, m := range migrations {
		db.Exec(m) // ignore error if column already exists
	}

	return nil
}
