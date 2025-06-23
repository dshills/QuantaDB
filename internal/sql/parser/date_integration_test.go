package parser

import (
	"testing"
	"time"

	"github.com/dshills/QuantaDB/internal/sql/types"
)

func TestDateIntegration(t *testing.T) {
	// Test that we can parse and evaluate date expressions
	tests := []struct {
		name     string
		expr     string
		expected bool
	}{
		{
			name:     "Date less than comparison",
			expr:     "date '1995-03-14' < date '1995-03-15'",
			expected: true,
		},
		{
			name:     "Date greater than comparison",
			expr:     "date '1995-03-16' > date '1995-03-15'",
			expected: true,
		},
		{
			name:     "Date equal comparison",
			expr:     "date '1995-03-15' = date '1995-03-15'",
			expected: true,
		},
		{
			name:     "Date not equal comparison",
			expr:     "date '1995-03-14' != date '1995-03-15'",
			expected: true,
		},
		{
			name:     "Date less than or equal - equal case",
			expr:     "date '1995-03-15' <= date '1995-03-15'",
			expected: true,
		},
		{
			name:     "Date less than or equal - less case",
			expr:     "date '1995-03-14' <= date '1995-03-15'",
			expected: true,
		},
		{
			name:     "Date greater than or equal - greater case",
			expr:     "date '1995-03-16' >= date '1995-03-15'",
			expected: true,
		},
		{
			name:     "False comparison",
			expr:     "date '1995-03-15' < date '1995-03-14'",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// We need to parse this as part of a SELECT to get a proper expression
			selectSQL := "SELECT " + tt.expr
			p := NewParser(selectSQL)
			stmt, err := p.Parse()
			if err != nil {
				t.Fatalf("Failed to parse expression: %v", err)
			}

			selectStmt, ok := stmt.(*SelectStmt)
			if !ok {
				t.Fatalf("Expected SelectStmt, got %T", stmt)
			}

			if len(selectStmt.Columns) != 1 {
				t.Fatalf("Expected 1 column, got %d", len(selectStmt.Columns))
			}

			// Get the expression
			expr := selectStmt.Columns[0].Expr

			// For now, we'll just verify the expression was parsed correctly
			// In a real integration test, we'd use the executor to evaluate this
			compExpr, ok := expr.(*ComparisonExpr)
			if !ok {
				t.Fatalf("Expected ComparisonExpr, got %T", expr)
			}

			// Verify both sides are date literals
			leftLit, ok := compExpr.Left.(*Literal)
			if !ok {
				t.Fatalf("Expected left side to be Literal, got %T", compExpr.Left)
			}
			rightLit, ok := compExpr.Right.(*Literal)
			if !ok {
				t.Fatalf("Expected right side to be Literal, got %T", compExpr.Right)
			}

			// Verify they're time values
			_, ok = leftLit.Value.Data.(time.Time)
			if !ok {
				t.Fatalf("Expected left literal to be time.Time, got %T", leftLit.Value.Data)
			}
			_, ok = rightLit.Value.Data.(time.Time)
			if !ok {
				t.Fatalf("Expected right literal to be time.Time, got %T", rightLit.Value.Data)
			}

			// Manually evaluate the comparison using CompareValues
			cmp := types.CompareValues(leftLit.Value, rightLit.Value)
			var boolResult bool
			switch compExpr.Operator {
			case TokenEqual:
				boolResult = cmp == 0
			case TokenNotEqual:
				boolResult = cmp != 0
			case TokenLess:
				boolResult = cmp < 0
			case TokenLessEqual:
				boolResult = cmp <= 0
			case TokenGreater:
				boolResult = cmp > 0
			case TokenGreaterEqual:
				boolResult = cmp >= 0
			default:
				t.Fatalf("Unexpected operator: %v", compExpr.Operator)
			}

			if boolResult != tt.expected {
				t.Errorf("Expression %s: expected %v, got %v", tt.expr, tt.expected, boolResult)
			}
		})
	}
}

func TestDateStorageRoundtrip(t *testing.T) {
	// Test that dates can be serialized and deserialized correctly
	testDates := []string{
		"1995-03-15",
		"2000-01-01",
		"2023-12-31",
		"1970-01-01",
		"2038-01-19", // Near 32-bit Unix timestamp limit
	}

	for _, dateStr := range testDates {
		t.Run(dateStr, func(t *testing.T) {
			// Parse the date
			dateVal, err := types.ParseDate(dateStr)
			if err != nil {
				t.Fatalf("Failed to parse date %s: %v", dateStr, err)
			}

			// Serialize it
			data, err := types.Date.Serialize(dateVal)
			if err != nil {
				t.Fatalf("Failed to serialize date: %v", err)
			}

			// Deserialize it
			deserialized, err := types.Date.Deserialize(data)
			if err != nil {
				t.Fatalf("Failed to deserialize date: %v", err)
			}

			// Compare
			if types.Date.Compare(dateVal, deserialized) != 0 {
				t.Errorf("Date %s: roundtrip failed, got different value after deserialize", dateStr)
			}

			// Verify the actual date matches
			origTime := dateVal.Data.(time.Time)
			deserTime := deserialized.Data.(time.Time)

			if !origTime.Equal(deserTime) {
				t.Errorf("Date %s: times not equal after roundtrip. Original: %v, Deserialized: %v",
					dateStr, origTime, deserTime)
			}
		})
	}
}
