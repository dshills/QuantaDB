package log

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"time"
)

// Logger is the interface for QuantaDB logging
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
	Fatal(msg string, args ...any)
	With(args ...any) Logger
	WithContext(ctx context.Context) Logger
}

// logger wraps slog.Logger
type logger struct {
	slog *slog.Logger
}

var (
	// Default logger instance
	defaultLogger Logger
)

func init() {
	// Initialize with JSON handler by default
	opts := &slog.HandlerOptions{
		Level:     slog.LevelInfo,
		AddSource: true,
	}
	handler := slog.NewJSONHandler(os.Stdout, opts)
	defaultLogger = &logger{slog: slog.New(handler)}
}

// SetDefault sets the default logger
func SetDefault(l Logger) {
	defaultLogger = l
}

// Default returns the default logger
func Default() Logger {
	return defaultLogger
}

// New creates a new logger with the given handler
func New(handler slog.Handler) Logger {
	return &logger{slog: slog.New(handler)}
}

// NewTextLogger creates a new text logger
func NewTextLogger(level slog.Level) Logger {
	opts := &slog.HandlerOptions{
		Level:     level,
		AddSource: true,
	}
	handler := slog.NewTextHandler(os.Stdout, opts)
	return &logger{slog: slog.New(handler)}
}

// NewJSONLogger creates a new JSON logger
func NewJSONLogger(level slog.Level) Logger {
	opts := &slog.HandlerOptions{
		Level:     level,
		AddSource: true,
	}
	handler := slog.NewJSONHandler(os.Stdout, opts)
	return &logger{slog: slog.New(handler)}
}

func (l *logger) Debug(msg string, args ...any) {
	l.slog.Debug(msg, args...)
}

func (l *logger) Info(msg string, args ...any) {
	l.slog.Info(msg, args...)
}

func (l *logger) Warn(msg string, args ...any) {
	l.slog.Warn(msg, args...)
}

func (l *logger) Error(msg string, args ...any) {
	l.slog.Error(msg, args...)
}

func (l *logger) Fatal(msg string, args ...any) {
	l.slog.Error(msg, args...)
	os.Exit(1)
}

func (l *logger) With(args ...any) Logger {
	return &logger{slog: l.slog.With(args...)}
}

func (l *logger) WithContext(ctx context.Context) Logger {
	// slog doesn't have WithContext method, just return self for now
	// Context can be passed to individual log calls if needed
	return l
}

// Helper functions for structured logging

// String returns a string attribute
func String(key, value string) slog.Attr {
	return slog.String(key, value)
}

// Int returns an int attribute
func Int(key string, value int) slog.Attr {
	return slog.Int(key, value)
}

// Int64 returns an int64 attribute
func Int64(key string, value int64) slog.Attr {
	return slog.Int64(key, value)
}

// Bool returns a bool attribute
func Bool(key string, value bool) slog.Attr {
	return slog.Bool(key, value)
}

// Duration returns a duration attribute
func Duration(key string, value time.Duration) slog.Attr {
	return slog.Duration(key, value)
}

// Time returns a time attribute
func Time(key string, value time.Time) slog.Attr {
	return slog.Time(key, value)
}

// Any returns an any attribute
func Any(key string, value any) slog.Attr {
	return slog.Any(key, value)
}

// Group returns a group attribute
func Group(key string, attrs ...slog.Attr) slog.Attr {
	// Convert []slog.Attr to []any
	args := make([]any, len(attrs))
	for i, attr := range attrs {
		args[i] = attr
	}
	return slog.Group(key, args...)
}

// Latency logs the latency of an operation
func Latency(start time.Time, operation string) {
	latency := time.Since(start)
	_, file, line, _ := runtime.Caller(1)
	defaultLogger.Info("operation completed",
		String("operation", operation),
		Duration("latency", latency),
		String("caller", fmt.Sprintf("%s:%d", file, line)),
	)
}

// Package-level convenience functions

func Debug(msg string, args ...any) {
	defaultLogger.Debug(msg, args...)
}

func Info(msg string, args ...any) {
	defaultLogger.Info(msg, args...)
}

func Warn(msg string, args ...any) {
	defaultLogger.Warn(msg, args...)
}

func Error(msg string, args ...any) {
	defaultLogger.Error(msg, args...)
}

func Fatal(msg string, args ...any) {
	defaultLogger.Fatal(msg, args...)
}
