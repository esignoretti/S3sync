package config

import (
	"fmt"
	"log/slog"
	"strconv"
)

const (
	SettingRunTimeoutSeconds = "run_timeout_seconds"
	DefaultRunTimeoutSeconds = 300
	MinRunTimeoutSeconds     = 30
	MaxRunTimeoutSeconds     = 86400
)

type Settings struct {
	RunTimeoutSeconds int
}

// GetSettings returns the stored settings, falling back to defaults for absent
// or unparseable rows. A corrupt settings row must never stop a sync, so an
// unreadable value is logged and replaced rather than returned as an error.
func (r *Repository) GetSettings() (Settings, error) {
	s := Settings{RunTimeoutSeconds: DefaultRunTimeoutSeconds}

	rows, err := r.db.Query(`SELECT key, value FROM settings`)
	if err != nil {
		return s, fmt.Errorf("query settings: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return s, fmt.Errorf("scan settings: %w", err)
		}
		if k != SettingRunTimeoutSeconds {
			continue
		}
		n, err := strconv.Atoi(v)
		if err != nil || n < MinRunTimeoutSeconds || n > MaxRunTimeoutSeconds {
			slog.Warn("settings: ignoring unusable value",
				"key", k, "value", v, "fallback", DefaultRunTimeoutSeconds)
			continue
		}
		s.RunTimeoutSeconds = n
	}
	return s, rows.Err()
}

// SetSetting validates and stores one setting. Unknown keys and out-of-range
// values are rejected so a typo cannot silently become a no-op.
func (r *Repository) SetSetting(key, value string) error {
	switch key {
	case SettingRunTimeoutSeconds:
		n, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("%s: %q is not a number", key, value)
		}
		if n < MinRunTimeoutSeconds || n > MaxRunTimeoutSeconds {
			return fmt.Errorf("%s: %d is outside %d..%d", key, n, MinRunTimeoutSeconds, MaxRunTimeoutSeconds)
		}
	default:
		return fmt.Errorf("unknown setting %q", key)
	}
	_, err := r.db.Exec(
		`INSERT INTO settings (key, value) VALUES (?, ?)
		 ON CONFLICT(key) DO UPDATE SET value = excluded.value`, key, value)
	if err != nil {
		return fmt.Errorf("set setting: %w", err)
	}
	return nil
}
