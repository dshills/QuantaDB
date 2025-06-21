package executor

import (
	"fmt"
	"testing"

	"github.com/dshills/QuantaDB/internal/sql/types"
)

// Comprehensive tests for all new join functionality

func TestMergeJoinAllTypes(t *testing.T) {
	// Test all join types with merge join
	testCases := []struct {
		name      string
		joinType  JoinType
		leftData  []*Row
		rightData []*Row
		expected  []string // Expected results as formatted strings
	}{
		{
			name:     "InnerJoin",
			joinType: InnerJoin,
			leftData: []*Row{
				{Values: []types.Value{types.NewIntegerValue(1), types.NewTextValue("A")}},
				{Values: []types.Value{types.NewIntegerValue(2), types.NewTextValue("B")}},
				{Values: []types.Value{types.NewIntegerValue(3), types.NewTextValue("C")}},
			},
			rightData: []*Row{
				{Values: []types.Value{types.NewIntegerValue(1), types.NewTextValue("X")}},
				{Values: []types.Value{types.NewIntegerValue(3), types.NewTextValue("Z")}},
			},
			expected: []string{"1:A:1:X", "3:C:3:Z"},
		},
		{
			name:     "LeftJoin",
			joinType: LeftJoin,
			leftData: []*Row{
				{Values: []types.Value{types.NewIntegerValue(1), types.NewTextValue("A")}},
				{Values: []types.Value{types.NewIntegerValue(2), types.NewTextValue("B")}},
			},
			rightData: []*Row{
				{Values: []types.Value{types.NewIntegerValue(1), types.NewTextValue("X")}},
			},
			expected: []string{"1:A:1:X", "2:B:NULL:NULL"},
		},
		{
			name:     "RightJoin",
			joinType: RightJoin,
			leftData: []*Row{
				{Values: []types.Value{types.NewIntegerValue(1), types.NewTextValue("A")}},
			},
			rightData: []*Row{
				{Values: []types.Value{types.NewIntegerValue(1), types.NewTextValue("X")}},
				{Values: []types.Value{types.NewIntegerValue(2), types.NewTextValue("Y")}},
			},
			expected: []string{"1:A:1:X", "NULL:NULL:2:Y"},
		},
		{
			name:     "FullJoin",
			joinType: FullJoin,
			leftData: []*Row{
				{Values: []types.Value{types.NewIntegerValue(1), types.NewTextValue("A")}},
				{Values: []types.Value{types.NewIntegerValue(2), types.NewTextValue("B")}},
			},
			rightData: []*Row{
				{Values: []types.Value{types.NewIntegerValue(2), types.NewTextValue("Y")}},
				{Values: []types.Value{types.NewIntegerValue(3), types.NewTextValue("Z")}},
			},
			expected: []string{"1:A:NULL:NULL", "2:B:2:Y", "NULL:NULL:3:Z"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			schema := &Schema{
				Columns: []Column{
					{Name: "id", Type: types.Integer},
					{Name: "value", Type: types.Text},
				},
			}

			leftOp := NewMockOperator(tc.leftData, schema)
			rightOp := NewMockOperator(tc.rightData, schema)

			mergeJoin := NewMergeJoinOperator(leftOp, rightOp, tc.joinType, nil, []int{0}, []int{0})

			ctx := &ExecContext{}
			if err := mergeJoin.Open(ctx); err != nil {
				t.Fatalf("Failed to open merge join: %v", err)
			}

			var results []string
			for {
				row, err := mergeJoin.Next()
				if err != nil {
					t.Fatalf("Error during merge join: %v", err)
				}
				if row == nil {
					break
				}
				results = append(results, formatJoinRow(row))
			}

			if len(results) != len(tc.expected) {
				t.Errorf("Expected %d results, got %d", len(tc.expected), len(results))
			}

			for i, expected := range tc.expected {
				if i < len(results) && results[i] != expected {
					t.Errorf("Row %d: expected %s, got %s", i, expected, results[i])
				}
			}
		})
	}
}

func TestSemiAntiJoinTypes(t *testing.T) {
	testCases := []struct {
		name      string
		joinType  SemiJoinType
		leftData  []*Row
		rightData []*Row
		expected  []int32 // Expected IDs from left table
	}{
		{
			name:     "SemiJoin_Basic",
			joinType: SemiJoinTypeSemi,
			leftData: []*Row{
				{Values: []types.Value{types.NewIntegerValue(1), types.NewTextValue("A")}},
				{Values: []types.Value{types.NewIntegerValue(2), types.NewTextValue("B")}},
				{Values: []types.Value{types.NewIntegerValue(3), types.NewTextValue("C")}},
			},
			rightData: []*Row{
				{Values: []types.Value{types.NewIntegerValue(1)}},
				{Values: []types.Value{types.NewIntegerValue(3)}},
			},
			expected: []int32{1, 3},
		},
		{
			name:     "AntiJoin_Basic",
			joinType: SemiJoinTypeAnti,
			leftData: []*Row{
				{Values: []types.Value{types.NewIntegerValue(1), types.NewTextValue("A")}},
				{Values: []types.Value{types.NewIntegerValue(2), types.NewTextValue("B")}},
				{Values: []types.Value{types.NewIntegerValue(3), types.NewTextValue("C")}},
			},
			rightData: []*Row{
				{Values: []types.Value{types.NewIntegerValue(1)}},
				{Values: []types.Value{types.NewIntegerValue(3)}},
			},
			expected: []int32{2},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			leftSchema := &Schema{
				Columns: []Column{
					{Name: "id", Type: types.Integer},
					{Name: "value", Type: types.Text},
				},
			}
			rightSchema := &Schema{
				Columns: []Column{
					{Name: "id", Type: types.Integer},
				},
			}

			leftOp := NewMockOperator(tc.leftData, leftSchema)
			rightOp := NewMockOperator(tc.rightData, rightSchema)

			leftKey := &columnRefEvaluator{columnIdx: 0, resolved: true}
			rightKey := &columnRefEvaluator{columnIdx: 0, resolved: true}

			semiJoin := NewSemiJoinOperator(
				leftOp, rightOp,
				[]ExprEvaluator{leftKey},
				[]ExprEvaluator{rightKey},
				nil,
				tc.joinType,
				false,
			)

			ctx := &ExecContext{}
			if err := semiJoin.Open(ctx); err != nil {
				t.Fatalf("Failed to open semi join: %v", err)
			}

			var results []int32
			for {
				row, err := semiJoin.Next()
				if err != nil {
					t.Fatalf("Error during semi join: %v", err)
				}
				if row == nil {
					break
				}
				id, _ := row.Values[0].AsInt()
				results = append(results, id)
			}

			if len(results) != len(tc.expected) {
				t.Errorf("Expected %d results, got %d", len(tc.expected), len(results))
			}

			for i, expected := range tc.expected {
				if i < len(results) && results[i] != expected {
					t.Errorf("Position %d: expected ID %d, got %d", i, expected, results[i])
				}
			}
		})
	}
}

func TestExternalSortLargeDataset(t *testing.T) {
	// Test external sort with a dataset that forces spilling
	rows := make([]*Row, 1000)
	for i := 0; i < 1000; i++ {
		// Create rows in reverse order to test sorting
		rows[i] = &Row{
			Values: []types.Value{
				types.NewIntegerValue(int32(1000 - i)),
				types.NewTextValue(fmt.Sprintf("Row_%d", 1000-i)),
			},
		}
	}

	input := NewMemoryIterator(rows)
	sorter := NewExternalSort([]int{0}, func(a, b *Row) int {
		return compareJoinValues(a.Values[0], b.Values[0])
	})
	sorter.memoryLimit = 10240 // 10KB - force spilling
	defer sorter.Cleanup()

	sorted, err := sorter.Sort(input)
	if err != nil {
		t.Fatalf("Sort failed: %v", err)
	}
	defer sorted.Close()

	// Verify all rows are in order
	expectedID := int32(1)
	count := 0
	for {
		row, err := sorted.Next()
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}
		if row == nil {
			break
		}

		id, _ := row.Values[0].AsInt()
		if id != expectedID {
			t.Errorf("Expected ID %d at position %d, got %d", expectedID, count, id)
		}
		expectedID++
		count++
	}

	if count != 1000 {
		t.Errorf("Expected 1000 rows, got %d", count)
	}

	// Verify spill files were created
	if len(sorter.spillFiles) == 0 {
		t.Error("Expected spill files to be created for large dataset")
	}
}

func TestMergeJoinWithExternalSort(t *testing.T) {
	// Test merge join with unsorted inputs that require external sort
	leftSchema := &Schema{
		Columns: []Column{
			{Name: "id", Type: types.Integer},
			{Name: "dept", Type: types.Text},
		},
	}
	rightSchema := &Schema{
		Columns: []Column{
			{Name: "dept_id", Type: types.Integer},
			{Name: "name", Type: types.Text},
		},
	}

	// Create unsorted data
	leftRows := []*Row{
		{Values: []types.Value{types.NewIntegerValue(3), types.NewTextValue("Sales")}},
		{Values: []types.Value{types.NewIntegerValue(1), types.NewTextValue("Eng")}},
		{Values: []types.Value{types.NewIntegerValue(2), types.NewTextValue("HR")}},
	}
	rightRows := []*Row{
		{Values: []types.Value{types.NewIntegerValue(2), types.NewTextValue("Human Resources")}},
		{Values: []types.Value{types.NewIntegerValue(3), types.NewTextValue("Sales Dept")}},
		{Values: []types.Value{types.NewIntegerValue(1), types.NewTextValue("Engineering")}},
	}

	leftOp := NewMockOperator(leftRows, leftSchema)
	rightOp := NewMockOperator(rightRows, rightSchema)

	// The merge join will sort the inputs internally
	mergeJoin := NewMergeJoinOperator(leftOp, rightOp, InnerJoin, nil, []int{0}, []int{0})

	ctx := &ExecContext{}
	if err := mergeJoin.Open(ctx); err != nil {
		t.Fatalf("Failed to open merge join: %v", err)
	}

	// Collect results
	var results []string
	for {
		row, err := mergeJoin.Next()
		if err != nil {
			t.Fatalf("Error during merge join: %v", err)
		}
		if row == nil {
			break
		}
		id, _ := row.Values[0].AsInt()
		dept, _ := row.Values[1].AsString()
		name, _ := row.Values[3].AsString()
		results = append(results, fmt.Sprintf("%d:%s:%s", id, dept, name))
	}

	// Verify all matches found
	expected := []string{
		"1:Eng:Engineering",
		"2:HR:Human Resources",
		"3:Sales:Sales Dept",
	}

	if len(results) != len(expected) {
		t.Errorf("Expected %d results, got %d", len(expected), len(results))
	}

	for i, exp := range expected {
		if i < len(results) && results[i] != exp {
			t.Errorf("Row %d: expected %s, got %s", i, exp, results[i])
		}
	}
}

func TestNestedLoopSemiJoinComplexCondition(t *testing.T) {
	// Test nested loop semi join with non-equi join condition
	leftSchema := &Schema{
		Columns: []Column{
			{Name: "id", Type: types.Integer},
			{Name: "score", Type: types.Integer},
		},
	}
	rightSchema := &Schema{
		Columns: []Column{
			{Name: "threshold", Type: types.Integer},
			{Name: "category", Type: types.Text},
		},
	}

	leftRows := []*Row{
		{Values: []types.Value{types.NewIntegerValue(1), types.NewIntegerValue(75)}},
		{Values: []types.Value{types.NewIntegerValue(2), types.NewIntegerValue(85)}},
		{Values: []types.Value{types.NewIntegerValue(3), types.NewIntegerValue(95)}},
	}

	rightRows := []*Row{
		{Values: []types.Value{types.NewIntegerValue(80), types.NewTextValue("Good")}},
		{Values: []types.Value{types.NewIntegerValue(90), types.NewTextValue("Excellent")}},
	}

	leftOp := NewMockOperator(leftRows, leftSchema)
	rightOp := NewMockOperator(rightRows, rightSchema)

	// Condition: left.score >= right.threshold
	condition := &scoreThresholdEvaluator{}

	nestedJoin := NewNestedLoopSemiJoinOperator(
		leftOp, rightOp,
		condition,
		SemiJoinTypeSemi,
		false,
	)

	ctx := &ExecContext{}
	if err := nestedJoin.Open(ctx); err != nil {
		t.Fatalf("Failed to open nested loop semi join: %v", err)
	}

	var results []int32
	for {
		row, err := nestedJoin.Next()
		if err != nil {
			t.Fatalf("Error during nested loop semi join: %v", err)
		}
		if row == nil {
			break
		}
		id, _ := row.Values[0].AsInt()
		results = append(results, id)
	}

	// IDs 2 and 3 should match (scores 85 and 95 are >= 80)
	expected := []int32{2, 3}
	if len(results) != len(expected) {
		t.Errorf("Expected %d results, got %d", len(expected), len(results))
	}

	for i, exp := range expected {
		if i < len(results) && results[i] != exp {
			t.Errorf("Position %d: expected ID %d, got %d", i, exp, results[i])
		}
	}
}

// Helper functions

func formatJoinRow(row *Row) string {
	result := ""
	for i, val := range row.Values {
		if i > 0 {
			result += ":"
		}
		if val.IsNull() {
			result += "NULL"
		} else if val.Type() == types.Integer {
			v, _ := val.AsInt()
			result += fmt.Sprintf("%d", v)
		} else if val.Type() == types.Text {
			v, _ := val.AsString()
			result += v
		}
	}
	return result
}

// scoreThresholdEvaluator evaluates left.score >= right.threshold
type scoreThresholdEvaluator struct{}

func (s *scoreThresholdEvaluator) Eval(row *Row, ctx *ExecContext) (types.Value, error) {
	// Combined row has left.score at index 1, right.threshold at index 2
	if len(row.Values) < 3 {
		return types.NewBooleanValue(false), nil
	}

	leftScore, err := row.Values[1].AsInt()
	if err != nil {
		return types.NewBooleanValue(false), nil
	}

	rightThreshold, err := row.Values[2].AsInt()
	if err != nil {
		return types.NewBooleanValue(false), nil
	}

	return types.NewBooleanValue(leftScore >= rightThreshold), nil
}
