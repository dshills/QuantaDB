package catalog

import (
	"testing"

	"github.com/dshills/QuantaDB/internal/sql/types"
)

func TestSelectivityEstimation(t *testing.T) {
	tests := []struct {
		name     string
		stats    *ColumnStats
		op       ComparisonOp
		value    types.Value
		expected Selectivity
	}{
		{
			name:     "Equality with distinct count",
			stats:    &ColumnStats{DistinctCount: 10},
			op:       OpEqual,
			value:    types.NewValue(5),
			expected: 0.1, // 1/10
		},
		{
			name:     "Equality without stats",
			stats:    nil,
			op:       OpEqual,
			value:    types.NewValue(5),
			expected: 0.1, // default
		},
		{
			name:     "Not equal with distinct count",
			stats:    &ColumnStats{DistinctCount: 10},
			op:       OpNotEqual,
			value:    types.NewValue(5),
			expected: 0.9, // 1 - 1/10
		},
		{
			name:     "Less than without histogram",
			stats:    &ColumnStats{MinValue: types.NewValue(0), MaxValue: types.NewValue(100)},
			op:       OpLess,
			value:    types.NewValue(30),
			expected: 0.3, // simplified estimate
		},
		{
			name:     "Is null",
			stats:    &ColumnStats{},
			op:       OpIsNull,
			value:    types.NewNullValue(),
			expected: 0.05, // default for IS NULL
		},
		{
			name:     "Is not null",
			stats:    &ColumnStats{},
			op:       OpIsNotNull,
			value:    types.NewNullValue(),
			expected: 0.95, // default for IS NOT NULL
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := EstimateSelectivity(tt.stats, tt.op, tt.value)
			if result != tt.expected {
				t.Errorf("Expected selectivity %f, got %f", tt.expected, result)
			}
		})
	}
}

func TestCombineSelectivity(t *testing.T) {
	tests := []struct {
		name          string
		op            LogicalOp
		selectivities []Selectivity
		expected      Selectivity
	}{
		{
			name:          "AND with two selectivities",
			op:            LogicalAnd,
			selectivities: []Selectivity{0.5, 0.4},
			expected:      0.2, // 0.5 * 0.4
		},
		{
			name:          "AND with three selectivities",
			op:            LogicalAnd,
			selectivities: []Selectivity{0.5, 0.4, 0.5},
			expected:      0.1, // 0.5 * 0.4 * 0.5
		},
		{
			name:          "OR with two selectivities",
			op:            LogicalOr,
			selectivities: []Selectivity{0.3, 0.4},
			expected:      0.58, // 0.3 + 0.4 - (0.3 * 0.4)
		},
		{
			name:          "Empty selectivities",
			op:            LogicalAnd,
			selectivities: []Selectivity{},
			expected:      1.0,
		},
		{
			name:          "Single selectivity",
			op:            LogicalAnd,
			selectivities: []Selectivity{0.7},
			expected:      0.7,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CombineSelectivity(tt.op, tt.selectivities...)
			// Use approximate equality for floating point
			diff := result - tt.expected
			if diff < 0 {
				diff = -diff
			}
			if diff > 0.001 {
				t.Errorf("Expected selectivity %f, got %f", tt.expected, result)
			}
		})
	}
}

func TestDefaultSelectivity(t *testing.T) {
	tests := []struct {
		op       ComparisonOp
		expected Selectivity
	}{
		{OpEqual, 0.1},
		{OpNotEqual, 0.9},
		{OpLess, 0.3},
		{OpLessEqual, 0.3},
		{OpGreater, 0.3},
		{OpGreaterEqual, 0.3},
		{OpIsNull, 0.05},
		{OpIsNotNull, 0.95},
		{ComparisonOp(999), 0.5}, // Unknown operator
	}

	for _, tt := range tests {
		t.Run(tt.op.String(), func(t *testing.T) {
			result := defaultSelectivity(tt.op)
			if result != tt.expected {
				t.Errorf("Expected selectivity %f, got %f", tt.expected, result)
			}
		})
	}
}

// String methods for test output.
func (op ComparisonOp) String() string {
	switch op {
	case OpEqual:
		return "Equal"
	case OpNotEqual:
		return "NotEqual"
	case OpLess:
		return "Less"
	case OpLessEqual:
		return "LessEqual"
	case OpGreater:
		return "Greater"
	case OpGreaterEqual:
		return "GreaterEqual"
	case OpIsNull:
		return "IsNull"
	case OpIsNotNull:
		return "IsNotNull"
	default:
		return "Unknown"
	}
}

func TestHistogramBucket(t *testing.T) {
	bucket := HistogramBucket{
		Frequency:     1000,
		DistinctCount: 50,
		UpperBound:    types.NewValue(int64(100)),
	}

	if bucket.Frequency != 1000 {
		t.Errorf("Expected frequency 1000, got %d", bucket.Frequency)
	}

	if bucket.DistinctCount != 50 {
		t.Errorf("Expected distinct count 50, got %d", bucket.DistinctCount)
	}

	if bucket.UpperBound.Data != int64(100) {
		t.Errorf("Expected upper bound 100, got %v", bucket.UpperBound.Data)
	}
}

func TestConstraintStrings(t *testing.T) {
	tests := []struct {
		name       string
		constraint Constraint
		expected   string
	}{
		{
			name:       "Single column primary key",
			constraint: PrimaryKeyConstraint{Columns: []string{"id"}},
			expected:   "PRIMARY KEY",
		},
		{
			name:       "Multi-column primary key",
			constraint: PrimaryKeyConstraint{Columns: []string{"user_id", "product_id"}},
			expected:   "PRIMARY KEY (user_id, product_id)",
		},
		{
			name:       "Single column unique",
			constraint: UniqueConstraint{Columns: []string{"email"}},
			expected:   "UNIQUE",
		},
		{
			name:       "Multi-column unique",
			constraint: UniqueConstraint{Columns: []string{"name", "org_id"}},
			expected:   "UNIQUE (name, org_id)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.constraint.String()
			if result != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestColumnConstraintStrings(t *testing.T) {
	tests := []struct {
		name       string
		constraint ColumnConstraint
		expected   string
	}{
		{
			name:       "NOT NULL",
			constraint: NotNullConstraint{},
			expected:   "NOT NULL",
		},
		{
			name:       "DEFAULT with string",
			constraint: DefaultConstraint{Value: types.NewValue("active")},
			expected:   "DEFAULT active",
		},
		{
			name:       "DEFAULT with number",
			constraint: DefaultConstraint{Value: types.NewValue(int64(0))},
			expected:   "DEFAULT 0",
		},
		{
			name:       "DEFAULT with NULL",
			constraint: DefaultConstraint{Value: types.NewNullValue()},
			expected:   "DEFAULT NULL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.constraint.String()
			if result != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestIndexTypeString(t *testing.T) {
	tests := []struct {
		indexType IndexType
		expected  string
	}{
		{BTreeIndex, "BTREE"},
		{HashIndex, "HASH"},
		{IndexType(999), "Unknown(999)"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := tt.indexType.String()
			if result != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestSortOrderString(t *testing.T) {
	if Ascending.String() != "ASC" {
		t.Errorf("Expected 'ASC', got %q", Ascending.String())
	}

	if Descending.String() != "DESC" {
		t.Errorf("Expected 'DESC', got %q", Descending.String())
	}
}
