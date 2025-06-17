package executor

import (
	"testing"

	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

func TestSortOperator(t *testing.T) {
	// Create test data
	rows := []*Row{
		{Values: []types.Value{types.NewValue(int64(3)), types.NewValue("Charlie"), types.NewValue(int64(35))}},
		{Values: []types.Value{types.NewValue(int64(1)), types.NewValue("Alice"), types.NewValue(int64(25))}},
		{Values: []types.Value{types.NewValue(int64(4)), types.NewValue("David"), types.NewValue(int64(30))}},
		{Values: []types.Value{types.NewValue(int64(2)), types.NewValue("Bob"), types.NewValue(int64(30))}},
		{Values: []types.Value{types.NewValue(int64(5)), types.NewValue("Eve"), types.NewValue(int64(25))}},
	}

	schema := &Schema{
		Columns: []Column{
			{Name: "id", Type: types.Integer},
			{Name: "name", Type: types.Text},
			{Name: "age", Type: types.Integer},
		},
	}

	t.Run("Sort by single column ASC", func(t *testing.T) {
		// Create mock operator
		mock := newMockOperator(rows, schema)

		// Sort by name ASC
		orderBy := []OrderByExpr{
			{
				Expr:  &columnRefEvaluator{columnIdx: 1, resolved: true}, // name column
				Order: planner.Ascending,
			},
		}

		sortOp := NewSortOperator(mock, orderBy)
		ctx := &ExecContext{Stats: &ExecStats{}}

		if err := sortOp.Open(ctx); err != nil {
			t.Fatalf("Failed to open sort operator: %v", err)
		}
		defer sortOp.Close()

		// Expected order: Alice, Bob, Charlie, David, Eve
		expectedNames := []string{"Alice", "Bob", "Charlie", "David", "Eve"}

		for i, expected := range expectedNames {
			row, err := sortOp.Next()
			if err != nil {
				t.Fatalf("Error getting row %d: %v", i, err)
			}
			if row == nil {
				t.Fatalf("Expected row %d, got nil", i)
			}

			name := row.Values[1].Data.(string)
			if name != expected {
				t.Errorf("Row %d: expected name %s, got %s", i, expected, name)
			}
		}

		// Check EOF
		row, err := sortOp.Next()
		if err != nil {
			t.Fatalf("Error checking EOF: %v", err)
		}
		if row != nil {
			t.Error("Expected EOF, got row")
		}
	})

	t.Run("Sort by single column DESC", func(t *testing.T) {
		// Create mock operator
		mock := newMockOperator(rows, schema)

		// Sort by age DESC
		orderBy := []OrderByExpr{
			{
				Expr:  &columnRefEvaluator{columnIdx: 2, resolved: true}, // age column
				Order: planner.Descending,
			},
		}

		sortOp := NewSortOperator(mock, orderBy)
		ctx := &ExecContext{Stats: &ExecStats{}}

		if err := sortOp.Open(ctx); err != nil {
			t.Fatalf("Failed to open sort operator: %v", err)
		}
		defer sortOp.Close()

		// Expected order by age DESC: 35, 30, 30, 25, 25
		expectedAges := []int64{35, 30, 30, 25, 25}

		for i, expected := range expectedAges {
			row, err := sortOp.Next()
			if err != nil {
				t.Fatalf("Error getting row %d: %v", i, err)
			}
			if row == nil {
				t.Fatalf("Expected row %d, got nil", i)
			}

			age := row.Values[2].Data.(int64)
			if age != expected {
				t.Errorf("Row %d: expected age %d, got %d", i, expected, age)
			}
		}
	})

	t.Run("Sort by multiple columns", func(t *testing.T) {
		// Create mock operator
		mock := newMockOperator(rows, schema)

		// Sort by age DESC, then name ASC
		orderBy := []OrderByExpr{
			{
				Expr:  &columnRefEvaluator{columnIdx: 2, resolved: true}, // age column
				Order: planner.Descending,
			},
			{
				Expr:  &columnRefEvaluator{columnIdx: 1, resolved: true}, // name column
				Order: planner.Ascending,
			},
		}

		sortOp := NewSortOperator(mock, orderBy)
		ctx := &ExecContext{Stats: &ExecStats{}}

		if err := sortOp.Open(ctx); err != nil {
			t.Fatalf("Failed to open sort operator: %v", err)
		}
		defer sortOp.Close()

		// Expected order:
		// Age 35: Charlie
		// Age 30: Bob, David (alphabetical)
		// Age 25: Alice, Eve (alphabetical)
		expectedData := []struct {
			name string
			age  int64
		}{
			{"Charlie", 35},
			{"Bob", 30},
			{"David", 30},
			{"Alice", 25},
			{"Eve", 25},
		}

		for i, expected := range expectedData {
			row, err := sortOp.Next()
			if err != nil {
				t.Fatalf("Error getting row %d: %v", i, err)
			}
			if row == nil {
				t.Fatalf("Expected row %d, got nil", i)
			}

			name := row.Values[1].Data.(string)
			age := row.Values[2].Data.(int64)

			if name != expected.name || age != expected.age {
				t.Errorf("Row %d: expected (%s, %d), got (%s, %d)",
					i, expected.name, expected.age, name, age)
			}
		}
	})

	t.Run("Sort with NULL values", func(t *testing.T) {
		// Create rows with NULL values
		rowsWithNull := []*Row{
			{Values: []types.Value{types.NewValue(int64(1)), types.NewValue("Alice"), types.NewNullValue()}},
			{Values: []types.Value{types.NewValue(int64(2)), types.NewValue("Bob"), types.NewValue(int64(30))}},
			{Values: []types.Value{types.NewValue(int64(3)), types.NewNullValue(), types.NewValue(int64(25))}},
			{Values: []types.Value{types.NewValue(int64(4)), types.NewValue("David"), types.NewNullValue()}},
		}

		mock := newMockOperator(rowsWithNull, schema)

		// Sort by age ASC (NULLs should come first)
		orderBy := []OrderByExpr{
			{
				Expr:  &columnRefEvaluator{columnIdx: 2, resolved: true}, // age column
				Order: planner.Ascending,
			},
		}

		sortOp := NewSortOperator(mock, orderBy)
		ctx := &ExecContext{Stats: &ExecStats{}}

		if err := sortOp.Open(ctx); err != nil {
			t.Fatalf("Failed to open sort operator: %v", err)
		}
		defer sortOp.Close()

		// Expected: NULL, NULL, 25, 30
		expectedAges := []interface{}{nil, nil, int64(25), int64(30)}

		for i, expected := range expectedAges {
			row, err := sortOp.Next()
			if err != nil {
				t.Fatalf("Error getting row %d: %v", i, err)
			}
			if row == nil {
				t.Fatalf("Expected row %d, got nil", i)
			}

			if expected == nil {
				if !row.Values[2].IsNull() {
					t.Errorf("Row %d: expected NULL, got %v", i, row.Values[2].Data)
				}
			} else {
				if row.Values[2].IsNull() {
					t.Errorf("Row %d: expected %v, got NULL", i, expected)
				} else if row.Values[2].Data.(int64) != expected.(int64) {
					t.Errorf("Row %d: expected %v, got %v", i, expected, row.Values[2].Data)
				}
			}
		}
	})

	t.Run("Empty result set", func(t *testing.T) {
		// Create mock operator with no rows
		mock := newMockOperator([]*Row{}, schema)

		orderBy := []OrderByExpr{
			{
				Expr:  &columnRefEvaluator{columnIdx: 0, resolved: true},
				Order: planner.Ascending,
			},
		}

		sortOp := NewSortOperator(mock, orderBy)
		ctx := &ExecContext{Stats: &ExecStats{}}

		if err := sortOp.Open(ctx); err != nil {
			t.Fatalf("Failed to open sort operator: %v", err)
		}
		defer sortOp.Close()

		// Should immediately return EOF
		row, err := sortOp.Next()
		if err != nil {
			t.Fatalf("Error getting row: %v", err)
		}
		if row != nil {
			t.Error("Expected nil row for empty result set")
		}
	})
}

func TestCompareValues(t *testing.T) {
	tests := []struct {
		name     string
		a        types.Value
		b        types.Value
		expected int
	}{
		// Integer comparisons
		{"int less than", types.NewValue(int64(1)), types.NewValue(int64(2)), -1},
		{"int equal", types.NewValue(int64(5)), types.NewValue(int64(5)), 0},
		{"int greater than", types.NewValue(int64(10)), types.NewValue(int64(3)), 1},

		// Float comparisons
		{"float less than", types.NewValue(1.5), types.NewValue(2.5), -1},
		{"float equal", types.NewValue(3.14), types.NewValue(3.14), 0},
		{"float greater than", types.NewValue(10.5), types.NewValue(3.2), 1},

		// String comparisons
		{"string less than", types.NewValue("apple"), types.NewValue("banana"), -1},
		{"string equal", types.NewValue("hello"), types.NewValue("hello"), 0},
		{"string greater than", types.NewValue("zebra"), types.NewValue("ant"), 1},

		// Boolean comparisons
		{"bool false < true", types.NewValue(false), types.NewValue(true), -1},
		{"bool equal false", types.NewValue(false), types.NewValue(false), 0},
		{"bool equal true", types.NewValue(true), types.NewValue(true), 0},
		{"bool true > false", types.NewValue(true), types.NewValue(false), 1},

		// NULL comparisons
		{"both null", types.NewNullValue(), types.NewNullValue(), 0},
		{"null < non-null", types.NewNullValue(), types.NewValue(int64(1)), -1},
		{"non-null > null", types.NewValue("hello"), types.NewNullValue(), 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := compareValues(tt.a, tt.b)
			if result != tt.expected {
				t.Errorf("compareValues(%v, %v) = %d, want %d",
					tt.a, tt.b, result, tt.expected)
			}
		})
	}
}
