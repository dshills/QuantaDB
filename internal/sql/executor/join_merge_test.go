package executor

import (
	"testing"

	"github.com/dshills/QuantaDB/internal/sql/types"
)

func TestMergeJoinBasic(t *testing.T) {
	// Create test data
	leftSchema := &Schema{
		Columns: []Column{
			{Name: "id", Type: types.Integer},
			{Name: "name", Type: types.Text},
		},
	}
	
	rightSchema := &Schema{
		Columns: []Column{
			{Name: "id", Type: types.Integer},
			{Name: "dept", Type: types.Text},
		},
	}
	
	leftRows := []*Row{
		{Values: []types.Value{types.NewIntegerValue(1), types.NewTextValue("Alice")}},
		{Values: []types.Value{types.NewIntegerValue(2), types.NewTextValue("Bob")}},
		{Values: []types.Value{types.NewIntegerValue(3), types.NewTextValue("Charlie")}},
	}
	
	rightRows := []*Row{
		{Values: []types.Value{types.NewIntegerValue(1), types.NewTextValue("Sales")}},
		{Values: []types.Value{types.NewIntegerValue(2), types.NewTextValue("Engineering")}},
		{Values: []types.Value{types.NewIntegerValue(3), types.NewTextValue("Marketing")}},
	}
	
	// Create operators
	leftOp := NewMockOperator(leftRows, leftSchema)
	rightOp := NewMockOperator(rightRows, rightSchema)
	
	// Create merge join on id column (index 0 for both)
	mergeJoin := NewMergeJoinOperator(leftOp, rightOp, InnerJoin, nil, []int{0}, []int{0})
	
	// Execute join
	var results []*Row
	for {
		row, err := mergeJoin.Next()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if row == nil {
			break
		}
		results = append(results, row)
	}
	
	// Verify results
	if len(results) != 3 {
		t.Errorf("Expected 3 rows, got %d", len(results))
	}
	
	// Check first row
	if len(results) > 0 {
		row := results[0]
		if len(row.Values) != 4 {
			t.Errorf("Expected 4 columns, got %d", len(row.Values))
		}
		
		// Verify join worked correctly
		id1, _ := row.Values[0].AsInt()
		id2, _ := row.Values[2].AsInt()
		if id1 != id2 {
			t.Errorf("Join keys don't match: %d != %d", id1, id2)
		}
	}
}

func TestMergeJoinDuplicates(t *testing.T) {
	// Test handling of duplicate join keys
	leftSchema := &Schema{
		Columns: []Column{
			{Name: "id", Type: types.Integer},
			{Name: "name", Type: types.Text},
		},
	}
	
	rightSchema := &Schema{
		Columns: []Column{
			{Name: "id", Type: types.Integer},
			{Name: "project", Type: types.Text},
		},
	}
	
	// Left has duplicates for id=1
	leftRows := []*Row{
		{Values: []types.Value{types.NewIntegerValue(1), types.NewTextValue("Alice")}},
		{Values: []types.Value{types.NewIntegerValue(1), types.NewTextValue("Ann")}},
		{Values: []types.Value{types.NewIntegerValue(2), types.NewTextValue("Bob")}},
	}
	
	// Right has duplicates for id=1
	rightRows := []*Row{
		{Values: []types.Value{types.NewIntegerValue(1), types.NewTextValue("ProjectA")}},
		{Values: []types.Value{types.NewIntegerValue(1), types.NewTextValue("ProjectB")}},
		{Values: []types.Value{types.NewIntegerValue(2), types.NewTextValue("ProjectC")}},
	}
	
	leftOp := NewMockOperator(leftRows, leftSchema)
	rightOp := NewMockOperator(rightRows, rightSchema)
	
	mergeJoin := NewMergeJoinOperator(leftOp, rightOp, InnerJoin, nil, []int{0}, []int{0})
	
	var results []*Row
	for {
		row, err := mergeJoin.Next()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if row == nil {
			break
		}
		results = append(results, row)
	}
	
	// Should get cartesian product for id=1: 2x2 = 4 rows, plus 1 row for id=2
	if len(results) != 5 {
		t.Errorf("Expected 5 rows (4 for id=1, 1 for id=2), got %d", len(results))
	}
}

func TestMergeJoinLeftOuter(t *testing.T) {
	leftSchema := &Schema{
		Columns: []Column{
			{Name: "id", Type: types.Integer},
			{Name: "name", Type: types.Text},
		},
	}
	
	rightSchema := &Schema{
		Columns: []Column{
			{Name: "id", Type: types.Integer},
			{Name: "dept", Type: types.Text},
		},
	}
	
	// Left has more rows
	leftRows := []*Row{
		{Values: []types.Value{types.NewIntegerValue(1), types.NewTextValue("Alice")}},
		{Values: []types.Value{types.NewIntegerValue(2), types.NewTextValue("Bob")}},
		{Values: []types.Value{types.NewIntegerValue(3), types.NewTextValue("Charlie")}},
		{Values: []types.Value{types.NewIntegerValue(4), types.NewTextValue("David")}},
	}
	
	// Right missing id=4
	rightRows := []*Row{
		{Values: []types.Value{types.NewIntegerValue(1), types.NewTextValue("Sales")}},
		{Values: []types.Value{types.NewIntegerValue(2), types.NewTextValue("Engineering")}},
		{Values: []types.Value{types.NewIntegerValue(3), types.NewTextValue("Marketing")}},
	}
	
	leftOp := NewMockOperator(leftRows, leftSchema)
	rightOp := NewMockOperator(rightRows, rightSchema)
	
	mergeJoin := NewMergeJoinOperator(leftOp, rightOp, LeftJoin, nil, []int{0}, []int{0})
	
	var results []*Row
	for {
		row, err := mergeJoin.Next()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if row == nil {
			break
		}
		results = append(results, row)
	}
	
	// Should get 4 rows (including David with NULL dept)
	if len(results) != 4 {
		t.Errorf("Expected 4 rows, got %d", len(results))
	}
	
	// Check last row has NULL for right side
	if len(results) >= 4 {
		lastRow := results[3]
		if !lastRow.Values[2].IsNull() || !lastRow.Values[3].IsNull() {
			t.Error("Expected NULL values for unmatched left row")
		}
	}
}

func TestMergeJoinWithCondition(t *testing.T) {
	// Test additional join condition beyond key equality
	leftSchema := &Schema{
		Columns: []Column{
			{Name: "id", Type: types.Integer},
			{Name: "value", Type: types.Integer},
		},
	}
	
	rightSchema := &Schema{
		Columns: []Column{
			{Name: "id", Type: types.Integer},
			{Name: "limit", Type: types.Integer},
		},
	}
	
	leftRows := []*Row{
		{Values: []types.Value{types.NewIntegerValue(1), types.NewIntegerValue(10)}},
		{Values: []types.Value{types.NewIntegerValue(2), types.NewIntegerValue(20)}},
		{Values: []types.Value{types.NewIntegerValue(3), types.NewIntegerValue(30)}},
	}
	
	rightRows := []*Row{
		{Values: []types.Value{types.NewIntegerValue(1), types.NewIntegerValue(15)}},
		{Values: []types.Value{types.NewIntegerValue(2), types.NewIntegerValue(15)}},
		{Values: []types.Value{types.NewIntegerValue(3), types.NewIntegerValue(25)}},
	}
	
	leftOp := NewMockOperator(leftRows, leftSchema)
	rightOp := NewMockOperator(rightRows, rightSchema)
	
	// Join condition: left.value < right.limit
	// For testing, we'll create a mock evaluator that checks left.value < right.limit
	joinCond := &mockJoinCondition{}
	
	mergeJoin := NewMergeJoinOperator(leftOp, rightOp, InnerJoin, joinCond, []int{0}, []int{0})
	
	var results []*Row
	for {
		row, err := mergeJoin.Next()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if row == nil {
			break
		}
		results = append(results, row)
	}
	
	// Only rows where value < limit should match:
	// id=1: 10 < 15 ✓
	// id=2: 20 < 15 ✗
	// id=3: 30 < 25 ✗
	if len(results) != 1 {
		t.Errorf("Expected 1 row, got %d", len(results))
	}
}

func TestExternalSort(t *testing.T) {
	// Test external sort with spilling
	
	// Create unsorted data
	rows := []*Row{
		{Values: []types.Value{types.NewIntegerValue(3), types.NewTextValue("Charlie")}},
		{Values: []types.Value{types.NewIntegerValue(1), types.NewTextValue("Alice")}},
		{Values: []types.Value{types.NewIntegerValue(4), types.NewTextValue("David")}},
		{Values: []types.Value{types.NewIntegerValue(2), types.NewTextValue("Bob")}},
	}
	
	input := NewMemoryIterator(rows)
	
	// Create external sort with small memory limit to force spilling
	sorter := NewExternalSort([]int{0}, func(a, b *Row) int {
		return compareJoinValues(a.Values[0], b.Values[0])
	})
	sorter.memoryLimit = 100 // Force spilling
	
	sorted, err := sorter.Sort(input)
	if err != nil {
		t.Fatalf("Sort failed: %v", err)
	}
	defer sorted.Close()
	
	// Verify sorted order
	var results []int32
	for {
		row, err := sorted.Next()
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}
		if row == nil {
			break
		}
		id, _ := row.Values[0].AsInt()
		results = append(results, id)
	}
	
	expected := []int32{1, 2, 3, 4}
	if len(results) != len(expected) {
		t.Errorf("Expected %d rows, got %d", len(expected), len(results))
	}
	
	for i, id := range results {
		if id != expected[i] {
			t.Errorf("Position %d: expected %d, got %d", i, expected[i], id)
		}
	}
	
	// Cleanup
	sorter.Cleanup()
}

// MockOperator for testing
type MockOperator struct {
	rows     []*Row
	position int
	schema   *Schema
}

func NewMockOperator(rows []*Row, schema *Schema) *MockOperator {
	return &MockOperator{
		rows:   rows,
		schema: schema,
	}
}

func (m *MockOperator) Next() (*Row, error) {
	if m.position >= len(m.rows) {
		return nil, nil
	}
	row := m.rows[m.position]
	m.position++
	return row, nil
}

func (m *MockOperator) Schema() *Schema {
	return m.schema
}

func (m *MockOperator) Open(ctx *ExecContext) error {
	m.position = 0
	return nil
}

func (m *MockOperator) Close() error {
	return nil
}

// mockJoinCondition implements a test join condition for left.value < right.limit
type mockJoinCondition struct{}

func (m *mockJoinCondition) Eval(row *Row, ctx *ExecContext) (types.Value, error) {
	// The combined row has left columns followed by right columns
	// left.value is at index 1, right.limit is at index 3
	if len(row.Values) < 4 {
		return types.NewBooleanValue(false), nil
	}
	
	leftValue, err := row.Values[1].AsInt()
	if err != nil {
		return types.NewBooleanValue(false), nil
	}
	
	rightLimit, err := row.Values[3].AsInt()
	if err != nil {
		return types.NewBooleanValue(false), nil
	}
	
	return types.NewBooleanValue(leftValue < rightLimit), nil
}