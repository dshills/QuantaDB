package executor

import (
	"testing"
	"time"

	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

func TestDateArithmetic(t *testing.T) {
	tests := []struct {
		name     string
		left     types.Value
		right    types.Value
		operator planner.BinaryOperator
		expected types.Value
		wantErr  bool
	}{
		{
			name:     "Date + Interval (days)",
			left:     types.NewDateValue(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)),
			right:    types.NewIntervalValue(types.NewInterval(0, 5, 0)),
			operator: planner.OpAdd,
			expected: types.NewDateValue(time.Date(2024, 1, 6, 0, 0, 0, 0, time.UTC)),
		},
		{
			name:     "Date - Interval (days)",
			left:     types.NewDateValue(time.Date(2024, 1, 10, 0, 0, 0, 0, time.UTC)),
			right:    types.NewIntervalValue(types.NewInterval(0, 5, 0)),
			operator: planner.OpSubtract,
			expected: types.NewDateValue(time.Date(2024, 1, 5, 0, 0, 0, 0, time.UTC)),
		},
		{
			name:     "Date + Interval (months)",
			left:     types.NewDateValue(time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)),
			right:    types.NewIntervalValue(types.NewInterval(2, 0, 0)),
			operator: planner.OpAdd,
			expected: types.NewDateValue(time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC)),
		},
		{
			name:     "Date + Interval (years)",
			left:     types.NewDateValue(time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC)),
			right:    types.NewIntervalValue(types.NewInterval(12, 0, 0)), // 1 year
			operator: planner.OpAdd,
			expected: types.NewDateValue(time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC)), // Go's AddDate behavior for invalid dates
		},
		{
			name:     "Timestamp + Interval (hours)",
			left:     types.NewTimestampValue(time.Date(2024, 1, 1, 10, 30, 0, 0, time.UTC)),
			right:    types.NewIntervalValue(types.NewInterval(0, 0, 2*time.Hour)),
			operator: planner.OpAdd,
			expected: types.NewTimestampValue(time.Date(2024, 1, 1, 12, 30, 0, 0, time.UTC)),
		},
		{
			name:     "Timestamp - Interval (mixed)",
			left:     types.NewTimestampValue(time.Date(2024, 3, 15, 14, 30, 0, 0, time.UTC)),
			right:    types.NewIntervalValue(types.NewInterval(1, 10, 3*time.Hour+15*time.Minute)),
			operator: planner.OpSubtract,
			expected: types.NewTimestampValue(time.Date(2024, 2, 5, 11, 15, 0, 0, time.UTC)),
		},
		{
			name:     "Date - Date = Interval",
			left:     types.NewDateValue(time.Date(2024, 1, 10, 0, 0, 0, 0, time.UTC)),
			right:    types.NewDateValue(time.Date(2024, 1, 5, 0, 0, 0, 0, time.UTC)),
			operator: planner.OpSubtract,
			expected: types.NewIntervalValue(types.NewInterval(0, 5, 0)),
		},
		{
			name:     "Timestamp - Timestamp = Interval",
			left:     types.NewTimestampValue(time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)),
			right:    types.NewTimestampValue(time.Date(2024, 1, 1, 10, 30, 0, 0, time.UTC)),
			operator: planner.OpSubtract,
			expected: types.NewIntervalValue(types.NewInterval(0, 0, 90*time.Minute)),
		},
		{
			name:     "Interval + Interval",
			left:     types.NewIntervalValue(types.NewInterval(1, 5, 2*time.Hour)),
			right:    types.NewIntervalValue(types.NewInterval(2, 10, 3*time.Hour)),
			operator: planner.OpAdd,
			expected: types.NewIntervalValue(types.NewInterval(3, 15, 5*time.Hour)),
		},
		{
			name:     "Interval - Interval",
			left:     types.NewIntervalValue(types.NewInterval(3, 15, 5*time.Hour)),
			right:    types.NewIntervalValue(types.NewInterval(1, 5, 2*time.Hour)),
			operator: planner.OpSubtract,
			expected: types.NewIntervalValue(types.NewInterval(2, 10, 3*time.Hour)),
		},
		{
			name:     "Interval * Scalar",
			left:     types.NewIntervalValue(types.NewInterval(1, 5, 2*time.Hour)),
			right:    types.NewValue(int32(3)),
			operator: planner.OpMultiply,
			expected: types.NewIntervalValue(types.NewInterval(3, 15, 6*time.Hour)),
		},
		{
			name:     "Scalar * Interval",
			left:     types.NewValue(float64(2.5)),
			right:    types.NewIntervalValue(types.NewInterval(0, 4, 0)),
			operator: planner.OpMultiply,
			expected: types.NewIntervalValue(types.NewInterval(0, 10, 0)),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create expression evaluator
			leftEval := &literalEvaluator{value: tt.left}
			rightEval := &literalEvaluator{value: tt.right}

			binOp := &binaryOpEvaluator{
				left:     leftEval,
				right:    rightEval,
				operator: tt.operator,
				dataType: types.Unknown,
			}

			// Evaluate
			result, err := binOp.Eval(nil, &ExecContext{})
			if tt.wantErr {
				if err == nil {
					t.Error("expected error but got none")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// Compare results
			if !compareDateValues(result, tt.expected) {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func compareDateValues(a, b types.Value) bool {
	if a.IsNull() != b.IsNull() {
		return false
	}
	if a.IsNull() {
		return true
	}

	switch aVal := a.Data.(type) {
	case time.Time:
		if bVal, ok := b.Data.(time.Time); ok {
			return aVal.Equal(bVal)
		}
	case types.Interval:
		if bVal, ok := b.Data.(types.Interval); ok {
			return aVal.Months == bVal.Months && aVal.Days == bVal.Days && aVal.Seconds == bVal.Seconds
		}
	default:
		return a.Equal(b)
	}
	return false
}

func TestDateComparison(t *testing.T) {
	tests := []struct {
		name     string
		left     types.Value
		right    types.Value
		operator planner.BinaryOperator
		expected bool
	}{
		{
			name:     "Date < Date",
			left:     types.NewDateValue(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)),
			right:    types.NewDateValue(time.Date(2024, 1, 5, 0, 0, 0, 0, time.UTC)),
			operator: planner.OpLess,
			expected: true,
		},
		{
			name:     "Date > Date",
			left:     types.NewDateValue(time.Date(2024, 1, 10, 0, 0, 0, 0, time.UTC)),
			right:    types.NewDateValue(time.Date(2024, 1, 5, 0, 0, 0, 0, time.UTC)),
			operator: planner.OpGreater,
			expected: true,
		},
		{
			name:     "Date = Date",
			left:     types.NewDateValue(time.Date(2024, 1, 5, 0, 0, 0, 0, time.UTC)),
			right:    types.NewDateValue(time.Date(2024, 1, 5, 0, 0, 0, 0, time.UTC)),
			operator: planner.OpEqual,
			expected: true,
		},
		{
			name:     "Timestamp <= Timestamp",
			left:     types.NewTimestampValue(time.Date(2024, 1, 1, 10, 30, 0, 0, time.UTC)),
			right:    types.NewTimestampValue(time.Date(2024, 1, 1, 10, 30, 0, 0, time.UTC)),
			operator: planner.OpLessEqual,
			expected: true,
		},
		{
			name:     "Interval comparison",
			left:     types.NewIntervalValue(types.NewInterval(1, 5, 0)),
			right:    types.NewIntervalValue(types.NewInterval(1, 10, 0)),
			operator: planner.OpLess,
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create expression evaluator
			leftEval := &literalEvaluator{value: tt.left}
			rightEval := &literalEvaluator{value: tt.right}

			binOp := &binaryOpEvaluator{
				left:     leftEval,
				right:    rightEval,
				operator: tt.operator,
				dataType: types.Boolean,
			}

			// Evaluate
			result, err := binOp.Eval(nil, &ExecContext{})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// Check result
			boolResult, ok := result.Data.(bool)
			if !ok {
				t.Fatalf("expected boolean result, got %T", result.Data)
			}
			if boolResult != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, boolResult)
			}
		})
	}
}
