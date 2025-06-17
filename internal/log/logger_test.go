package log

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/dshills/QuantaDB/internal/testutil"
)

func TestLoggerCreation(t *testing.T) {
	// Test JSON logger
	jsonLogger := NewJSONLogger(slog.LevelDebug)
	testutil.AssertTrue(t, jsonLogger != nil, "JSON logger should not be nil")

	// Test text logger
	textLogger := NewTextLogger(slog.LevelInfo)
	testutil.AssertTrue(t, textLogger != nil, "Text logger should not be nil")
}

func TestLoggerWithCapture(t *testing.T) {
	// Create a buffer to capture output
	var buf bytes.Buffer

	// Create custom handler that writes to buffer
	opts := &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}
	handler := slog.NewJSONHandler(&buf, opts)
	logger := New(handler)

	// Test different log levels
	logger.Debug("debug message", String("key", "value"))
	logger.Info("info message", Int("count", 42))
	logger.Warn("warn message", Bool("flag", true))
	logger.Error("error message", Duration("elapsed", time.Second))

	// Check that messages were logged
	output := buf.String()
	testutil.AssertTrue(t, strings.Contains(output, "debug message"), "should contain debug message")
	testutil.AssertTrue(t, strings.Contains(output, "info message"), "should contain info message")
	testutil.AssertTrue(t, strings.Contains(output, "warn message"), "should contain warn message")
	testutil.AssertTrue(t, strings.Contains(output, "error message"), "should contain error message")

	// Verify structured fields
	lines := strings.Split(strings.TrimSpace(output), "\n")
	for _, line := range lines {
		var entry map[string]interface{}
		err := json.Unmarshal([]byte(line), &entry)
		testutil.AssertNoError(t, err)
		testutil.AssertTrue(t, entry["msg"] != nil, "should have msg field")
		testutil.AssertTrue(t, entry["level"] != nil, "should have level field")
	}
}

func TestLoggerWith(t *testing.T) {
	var buf bytes.Buffer
	handler := slog.NewJSONHandler(&buf, nil)
	logger := New(handler)

	// Create logger with additional context
	ctxLogger := logger.With(
		String("service", "quantadb"),
		String("version", "1.0.0"),
	)

	ctxLogger.Info("test message")

	// Verify context fields are included
	var entry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &entry)
	testutil.AssertNoError(t, err)
	testutil.AssertEqual(t, "quantadb", entry["service"])
	testutil.AssertEqual(t, "1.0.0", entry["version"])
}

func TestLoggerWithContext(t *testing.T) {
	var buf bytes.Buffer
	handler := slog.NewJSONHandler(&buf, nil)
	logger := New(handler)

	// Create context with values
	type contextKey string
	ctx := context.WithValue(context.Background(), contextKey("request_id"), "12345")
	ctxLogger := logger.WithContext(ctx)

	ctxLogger.Info("context test")

	// Verify log was written (context values aren't automatically included in slog)
	testutil.AssertTrue(t, buf.Len() > 0, "should have logged message")
}

func TestParseLevel(t *testing.T) {
	tests := []struct {
		input    string
		expected slog.Level
	}{
		{"debug", slog.LevelDebug},
		{"DEBUG", slog.LevelDebug},
		{"info", slog.LevelInfo},
		{"INFO", slog.LevelInfo},
		{"warn", slog.LevelWarn},
		{"warning", slog.LevelWarn},
		{"error", slog.LevelError},
		{"ERROR", slog.LevelError},
		{"invalid", slog.LevelInfo}, // default
	}

	for _, tt := range tests {
		level := ParseLevel(tt.input)
		testutil.AssertEqual(t, tt.expected, level)
	}
}

func TestConfigure(t *testing.T) {
	// Test JSON configuration
	Configure(Config{
		Level:  "debug",
		Format: "json",
	})

	logger := Default()
	testutil.AssertTrue(t, logger != nil, "default logger should be set")

	// Test text configuration
	Configure(Config{
		Level:  "info",
		Format: "text",
	})

	logger = Default()
	testutil.AssertTrue(t, logger != nil, "default logger should be set")
}

func TestStructuredLoggingHelpers(t *testing.T) {
	// Test attribute helpers
	strAttr := String("key", "value")
	testutil.AssertEqual(t, "key", strAttr.Key)
	testutil.AssertEqual(t, "value", strAttr.Value.String())

	intAttr := Int("count", 42)
	testutil.AssertEqual(t, "count", intAttr.Key)
	testutil.AssertEqual(t, int64(42), intAttr.Value.Int64())

	boolAttr := Bool("flag", true)
	testutil.AssertEqual(t, "flag", boolAttr.Key)
	testutil.AssertEqual(t, true, boolAttr.Value.Bool())

	now := time.Now()
	timeAttr := Time("timestamp", now)
	testutil.AssertEqual(t, "timestamp", timeAttr.Key)
	testutil.AssertEqual(t, now.Unix(), timeAttr.Value.Time().Unix())

	durAttr := Duration("elapsed", time.Second)
	testutil.AssertEqual(t, "elapsed", durAttr.Key)
	testutil.AssertEqual(t, time.Second, durAttr.Value.Duration())
}

func TestLogLatency(t *testing.T) {
	var buf bytes.Buffer
	handler := slog.NewJSONHandler(&buf, nil)
	SetDefault(New(handler))

	start := time.Now()
	time.Sleep(10 * time.Millisecond) // Small delay
	Latency(start, "test_operation")

	// Verify latency was logged
	var entry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &entry)
	testutil.AssertNoError(t, err)
	testutil.AssertEqual(t, "operation completed", entry["msg"])
	testutil.AssertEqual(t, "test_operation", entry["operation"])
	testutil.AssertTrue(t, entry["latency"] != nil, "should have latency field")
}

func TestPackageLevelFunctions(t *testing.T) {
	var buf bytes.Buffer
	opts := &slog.HandlerOptions{
		Level: slog.LevelDebug, // Enable debug level
	}
	handler := slog.NewJSONHandler(&buf, opts)
	SetDefault(New(handler))

	// Test package-level logging functions
	Debug("debug")
	Info("info")
	Warn("warn")
	Error("error")

	output := buf.String()
	testutil.AssertTrue(t, strings.Contains(output, "debug"), "should contain debug")
	testutil.AssertTrue(t, strings.Contains(output, "info"), "should contain info")
	testutil.AssertTrue(t, strings.Contains(output, "warn"), "should contain warn")
	testutil.AssertTrue(t, strings.Contains(output, "error"), "should contain error")
}
