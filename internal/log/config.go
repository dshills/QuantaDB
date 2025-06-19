package log

import (
	"log/slog"
	"strings"
)

// Config represents logging configuration.
type Config struct {
	Level  string `json:"level"`
	Format string `json:"format"`
}

// DefaultConfig returns default logging configuration.
func DefaultConfig() Config {
	return Config{
		Level:  "info",
		Format: "json",
	}
}

// ParseLevel parses string log level to slog.Level.
func ParseLevel(level string) slog.Level {
	switch strings.ToLower(level) {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// Configure sets up the logger based on config.
func Configure(cfg Config) {
	level := ParseLevel(cfg.Level)

	var logger Logger
	switch strings.ToLower(cfg.Format) {
	case "text":
		logger = NewTextLogger(level)
	case "json":
		logger = NewJSONLogger(level)
	default:
		logger = NewJSONLogger(level)
	}

	SetDefault(logger)
}
