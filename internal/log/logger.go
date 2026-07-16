package log

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type Config struct {
	Level  string
	Format string
	File   string
}

var closeOnce sync.Once
var fileWriter io.WriteCloser

func Init(cfg Config) func() {
	lvl := parseLevel(cfg.Level)
	var w io.Writer = os.Stdout

	if cfg.File != "" {
		dir := filepath.Dir(cfg.File)
		os.MkdirAll(dir, 0755)
		f, err := os.OpenFile(cfg.File, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err == nil {
			w = io.MultiWriter(os.Stdout, f)
			fileWriter = f
		}
	}

	var h slog.Handler
	switch cfg.Format {
	case "json":
		h = slog.NewJSONHandler(w, &slog.HandlerOptions{Level: lvl})
	default:
		h = slog.NewTextHandler(w, &slog.HandlerOptions{
			Level: lvl,
			ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
				if a.Key == slog.TimeKey && len(groups) == 0 {
					return slog.String(slog.TimeKey, a.Value.Time().Format("15:04:05"))
				}
				return a
			},
		})
	}

	slog.SetDefault(slog.New(h))
	return func() {
		closeOnce.Do(func() {
			if fileWriter != nil {
				fileWriter.Close()
			}
		})
	}
}

func parseLevel(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
