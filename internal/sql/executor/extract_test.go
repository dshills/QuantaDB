package executor

import (
	"testing"
	"time"

	"github.com/dshills/QuantaDB/internal/sql/types"
)

func TestExtractEvaluator(t *testing.T) {
	// Test the EXTRACT evaluator directly

	testDate := time.Date(2023, 12, 25, 14, 30, 45, 0, time.UTC)

	tests := []struct {
		name     string
		field    string
		expected int32
	}{
		{"extract year", "YEAR", 2023},
		{"extract month", "MONTH", 12},
		{"extract day", "DAY", 25},
		{"extract hour", "HOUR", 14},
		{"extract minute", "MINUTE", 30},
		{"extract second", "SECOND", 45},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a literal evaluator for the date
			dateEval := &literalEvaluator{
				value: types.NewTimestampValue(testDate),
			}

			// Create the EXTRACT evaluator
			extractEval := &extractEvaluator{
				field:    tt.field,
				fromEval: dateEval,
			}

			// Create a mock row and context
			row := &Row{Values: []types.Value{}}
			ctx := &ExecContext{}

			// Evaluate
			result, err := extractEval.Eval(row, ctx)
			if err != nil {
				t.Fatalf("Evaluation failed: %v", err)
			}

			if result.IsNull() {
				t.Fatal("Result should not be null")
			}

			value, ok := result.Data.(int32)
			if !ok {
				t.Fatalf("Expected int32, got %T", result.Data)
			}

			if value != tt.expected {
				t.Errorf("Expected %d, got %d", tt.expected, value)
			}
		})
	}
}

func TestExtractEvaluatorWithNullValue(t *testing.T) {
	// Test EXTRACT with NULL input
	nullEval := &literalEvaluator{
		value: types.NewNullValue(),
	}

	extractEval := &extractEvaluator{
		field:    "YEAR",
		fromEval: nullEval,
	}

	row := &Row{Values: []types.Value{}}
	ctx := &ExecContext{}

	result, err := extractEval.Eval(row, ctx)
	if err != nil {
		t.Fatalf("Evaluation failed: %v", err)
	}

	if !result.IsNull() {
		t.Fatal("Result should be null when input is null")
	}
}

func TestExtractEvaluatorWithInvalidType(t *testing.T) {
	// Test EXTRACT with non-date input
	stringEval := &literalEvaluator{
		value: types.NewValue("not a date"),
	}

	extractEval := &extractEvaluator{
		field:    "YEAR",
		fromEval: stringEval,
	}

	row := &Row{Values: []types.Value{}}
	ctx := &ExecContext{}

	_, err := extractEval.Eval(row, ctx)
	if err == nil {
		t.Fatal("Expected error when extracting from non-date value")
	}

	expectedMsg := "EXTRACT requires date/timestamp value"
	if !containsSubstring(err.Error(), expectedMsg) {
		t.Errorf("Expected error containing '%s', got: %v", expectedMsg, err)
	}
}

func TestExtractEvaluatorWithInvalidField(t *testing.T) {
	// Test EXTRACT with invalid field
	testDate := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	dateEval := &literalEvaluator{
		value: types.NewTimestampValue(testDate),
	}

	extractEval := &extractEvaluator{
		field:    "INVALID",
		fromEval: dateEval,
	}

	row := &Row{Values: []types.Value{}}
	ctx := &ExecContext{}

	_, err := extractEval.Eval(row, ctx)
	if err == nil {
		t.Fatal("Expected error when using invalid field")
	}

	expectedMsg := "unsupported EXTRACT field"
	if !containsSubstring(err.Error(), expectedMsg) {
		t.Errorf("Expected error containing '%s', got: %v", expectedMsg, err)
	}
}

// Helper function to check if a string contains a substring
func containsSubstring(s, substr string) bool {
	return len(s) >= len(substr) && s[:len(substr)] == substr ||
		(len(s) > len(substr) && s[len(s)-len(substr):] == substr) ||
		indexOf(s, substr) >= 0
}

func indexOf(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}
