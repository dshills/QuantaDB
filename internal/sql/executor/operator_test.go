package executor

import (
	"fmt"
	"testing"

	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

// mockOperator is a test helper that returns predefined rows.
type mockOperator struct {
	baseOperator
	rows     []*Row
	position int
	opened   bool
}

func newMockOperator(rows []*Row, schema *Schema) *mockOperator {
	return &mockOperator{
		baseOperator: baseOperator{schema: schema},
		rows:         rows,
		position:     0,
	}
}

func (m *mockOperator) Open(ctx *ExecContext) error {
	m.ctx = ctx
	m.position = 0
	m.opened = true
	return nil
}

func (m *mockOperator) Next() (*Row, error) {
	if !m.opened {
		return nil, fmt.Errorf("operator not opened")
	}
	
	if m.position >= len(m.rows) {
		return nil, nil // EOF
	}
	
	row := m.rows[m.position]
	m.position++
	return row, nil
}

func (m *mockOperator) Close() error {
	m.opened = false
	return nil
}

func TestFilterOperator(t *testing.T) {
	// Create test schema
	schema := &Schema{
		Columns: []Column{
			{Name: "id", Type: types.Integer, Nullable: false},
			{Name: "age", Type: types.Integer, Nullable: false},
			{Name: "active", Type: types.Boolean, Nullable: false},
		},
	}
	
	// Create test rows
	rows := []*Row{
		{Values: []types.Value{types.NewValue(int64(1)), types.NewValue(int64(25)), types.NewValue(true)}},
		{Values: []types.Value{types.NewValue(int64(2)), types.NewValue(int64(30)), types.NewValue(false)}},
		{Values: []types.Value{types.NewValue(int64(3)), types.NewValue(int64(20)), types.NewValue(true)}},
		{Values: []types.Value{types.NewValue(int64(4)), types.NewValue(int64(35)), types.NewValue(true)}},
		{Values: []types.Value{types.NewValue(int64(5)), types.NewValue(int64(18)), types.NewValue(false)}},
	}
	
	ctx := &ExecContext{Stats: &ExecStats{}}
	
	t.Run("Filter age > 25", func(t *testing.T) {
		// Create filter predicate: age > 25
		predicate := &binaryOpEvaluator{
			left: &columnRefEvaluator{
				columnName: "age",
				columnIdx:  1, // age is at index 1
				resolved:   true,
			},
			right:    &literalEvaluator{value: types.NewValue(int64(25))},
			operator: planner.OpGreater,
			dataType: types.Boolean,
		}
		
		child := newMockOperator(rows, schema)
		filter := NewFilterOperator(child, predicate)
		
		// Execute
		if err := filter.Open(ctx); err != nil {
			t.Fatalf("Failed to open filter: %v", err)
		}
		defer filter.Close()
		
		// Collect results
		var results []*Row
		for {
			row, err := filter.Next()
			if err != nil {
				t.Fatalf("Error getting next row: %v", err)
			}
			if row == nil {
				break
			}
			results = append(results, row)
		}
		
		// Verify results - should have rows with age > 25
		expected := 2 // rows with age 30 and 35
		if len(results) != expected {
			t.Errorf("Expected %d rows, got %d", expected, len(results))
		}
		
		// Verify the correct rows were returned
		for _, row := range results {
			age := row.Values[1].Data.(int64)
			if age <= 25 {
				t.Errorf("Got row with age %d, expected > 25", age)
			}
		}
	})
	
	t.Run("Filter with AND condition", func(t *testing.T) {
		// Create filter predicate: age > 20 AND active = true
		agePredicate := &binaryOpEvaluator{
			left: &columnRefEvaluator{
				columnName: "age",
				columnIdx:  1,
				resolved:   true,
			},
			right:    &literalEvaluator{value: types.NewValue(int64(20))},
			operator: planner.OpGreater,
			dataType: types.Boolean,
		}
		
		activePredicate := &binaryOpEvaluator{
			left: &columnRefEvaluator{
				columnName: "active",
				columnIdx:  2,
				resolved:   true,
			},
			right:    &literalEvaluator{value: types.NewValue(true)},
			operator: planner.OpEqual,
			dataType: types.Boolean,
		}
		
		andPredicate := &binaryOpEvaluator{
			left:     agePredicate,
			right:    activePredicate,
			operator: planner.OpAnd,
			dataType: types.Boolean,
		}
		
		child := newMockOperator(rows, schema)
		filter := NewFilterOperator(child, andPredicate)
		
		// Execute
		if err := filter.Open(ctx); err != nil {
			t.Fatalf("Failed to open filter: %v", err)
		}
		defer filter.Close()
		
		// Collect results
		var results []*Row
		for {
			row, err := filter.Next()
			if err != nil {
				t.Fatalf("Error getting next row: %v", err)
			}
			if row == nil {
				break
			}
			results = append(results, row)
		}
		
		// Verify results - should have rows with age > 20 AND active = true
		expected := 2 // rows with id 1 and 4
		if len(results) != expected {
			t.Errorf("Expected %d rows, got %d", expected, len(results))
		}
	})
}

func TestProjectOperator(t *testing.T) {
	// Create test schema
	inputSchema := &Schema{
		Columns: []Column{
			{Name: "id", Type: types.Integer, Nullable: false},
			{Name: "age", Type: types.Integer, Nullable: false},
			{Name: "name", Type: types.Text, Nullable: false},
		},
	}
	
	// Create test rows
	rows := []*Row{
		{Values: []types.Value{types.NewValue(int64(1)), types.NewValue(int64(25)), types.NewValue("Alice")}},
		{Values: []types.Value{types.NewValue(int64(2)), types.NewValue(int64(30)), types.NewValue("Bob")}},
	}
	
	ctx := &ExecContext{Stats: &ExecStats{}}
	
	t.Run("Project subset of columns", func(t *testing.T) {
		// Project only id and name (skip age)
		projections := []ExprEvaluator{
			&columnRefEvaluator{columnName: "id", columnIdx: 0, resolved: true},
			&columnRefEvaluator{columnName: "name", columnIdx: 2, resolved: true},
		}
		
		outputSchema := &Schema{
			Columns: []Column{
				{Name: "id", Type: types.Integer, Nullable: false},
				{Name: "name", Type: types.Text, Nullable: false},
			},
		}
		
		child := newMockOperator(rows, inputSchema)
		project := NewProjectOperator(child, projections, outputSchema)
		
		// Execute
		if err := project.Open(ctx); err != nil {
			t.Fatalf("Failed to open project: %v", err)
		}
		defer project.Close()
		
		// Collect results
		var results []*Row
		for {
			row, err := project.Next()
			if err != nil {
				t.Fatalf("Error getting next row: %v", err)
			}
			if row == nil {
				break
			}
			results = append(results, row)
		}
		
		// Verify results
		if len(results) != 2 {
			t.Errorf("Expected 2 rows, got %d", len(results))
		}
		
		for i, row := range results {
			if len(row.Values) != 2 {
				t.Errorf("Row %d: expected 2 values, got %d", i, len(row.Values))
			}
			
			// Verify we got id and name (not age)
			id := row.Values[0].Data.(int64)
			name := row.Values[1].Data.(string)
			
			if id != int64(i+1) {
				t.Errorf("Row %d: expected id %d, got %d", i, i+1, id)
			}
			
			expectedName := "Alice"
			if i == 1 {
				expectedName = "Bob"
			}
			if name != expectedName {
				t.Errorf("Row %d: expected name %s, got %s", i, expectedName, name)
			}
		}
	})
	
	t.Run("Project with expressions", func(t *testing.T) {
		// Project: id, age * 2 as double_age
		projections := []ExprEvaluator{
			&columnRefEvaluator{columnName: "id", columnIdx: 0, resolved: true},
			&binaryOpEvaluator{
				left:     &columnRefEvaluator{columnName: "age", columnIdx: 1, resolved: true},
				right:    &literalEvaluator{value: types.NewValue(int64(2))},
				operator: planner.OpMultiply,
				dataType: types.Integer,
			},
		}
		
		outputSchema := &Schema{
			Columns: []Column{
				{Name: "id", Type: types.Integer, Nullable: false},
				{Name: "double_age", Type: types.Integer, Nullable: false},
			},
		}
		
		child := newMockOperator(rows, inputSchema)
		project := NewProjectOperator(child, projections, outputSchema)
		
		// Execute
		if err := project.Open(ctx); err != nil {
			t.Fatalf("Failed to open project: %v", err)
		}
		defer project.Close()
		
		// Get first row
		row, err := project.Next()
		if err != nil {
			t.Fatalf("Error getting row: %v", err)
		}
		
		// Verify double_age calculation
		doubleAge := row.Values[1].Data.(int64)
		expectedDoubleAge := int64(50) // 25 * 2
		if doubleAge != expectedDoubleAge {
			t.Errorf("Expected double_age %d, got %d", expectedDoubleAge, doubleAge)
		}
	})
}

func TestLimitOperator(t *testing.T) {
	// Create test schema
	schema := &Schema{
		Columns: []Column{
			{Name: "id", Type: types.Integer, Nullable: false},
		},
	}
	
	// Create test rows
	rows := make([]*Row, 10)
	for i := 0; i < 10; i++ {
		rows[i] = &Row{Values: []types.Value{types.NewValue(int64(i + 1))}}
	}
	
	ctx := &ExecContext{Stats: &ExecStats{}}
	
	t.Run("Limit only", func(t *testing.T) {
		child := newMockOperator(rows, schema)
		limit := NewLimitOperator(child, 3, 0)
		
		// Execute
		if err := limit.Open(ctx); err != nil {
			t.Fatalf("Failed to open limit: %v", err)
		}
		defer limit.Close()
		
		// Collect results
		var results []*Row
		for {
			row, err := limit.Next()
			if err != nil {
				t.Fatalf("Error getting next row: %v", err)
			}
			if row == nil {
				break
			}
			results = append(results, row)
		}
		
		// Verify we got exactly 3 rows
		if len(results) != 3 {
			t.Errorf("Expected 3 rows, got %d", len(results))
		}
		
		// Verify we got the first 3 rows
		for i, row := range results {
			id := row.Values[0].Data.(int64)
			if id != int64(i+1) {
				t.Errorf("Row %d: expected id %d, got %d", i, i+1, id)
			}
		}
	})
	
	t.Run("Offset only", func(t *testing.T) {
		child := newMockOperator(rows, schema)
		limit := NewLimitOperator(child, -1, 5) // No limit, offset 5
		
		// Execute
		if err := limit.Open(ctx); err != nil {
			t.Fatalf("Failed to open limit: %v", err)
		}
		defer limit.Close()
		
		// Collect results
		var results []*Row
		for {
			row, err := limit.Next()
			if err != nil {
				t.Fatalf("Error getting next row: %v", err)
			}
			if row == nil {
				break
			}
			results = append(results, row)
		}
		
		// Verify we got 5 rows (10 - 5 offset)
		if len(results) != 5 {
			t.Errorf("Expected 5 rows, got %d", len(results))
		}
		
		// Verify we got rows starting from id 6
		for i, row := range results {
			id := row.Values[0].Data.(int64)
			expectedID := int64(i + 6)
			if id != expectedID {
				t.Errorf("Row %d: expected id %d, got %d", i, expectedID, id)
			}
		}
	})
	
	t.Run("Limit and offset", func(t *testing.T) {
		child := newMockOperator(rows, schema)
		limit := NewLimitOperator(child, 3, 2) // Limit 3, offset 2
		
		// Execute
		if err := limit.Open(ctx); err != nil {
			t.Fatalf("Failed to open limit: %v", err)
		}
		defer limit.Close()
		
		// Collect results
		var results []*Row
		for {
			row, err := limit.Next()
			if err != nil {
				t.Fatalf("Error getting next row: %v", err)
			}
			if row == nil {
				break
			}
			results = append(results, row)
		}
		
		// Verify we got exactly 3 rows
		if len(results) != 3 {
			t.Errorf("Expected 3 rows, got %d", len(results))
		}
		
		// Verify we got rows 3, 4, 5
		for i, row := range results {
			id := row.Values[0].Data.(int64)
			expectedID := int64(i + 3)
			if id != expectedID {
				t.Errorf("Row %d: expected id %d, got %d", i, expectedID, id)
			}
		}
	})
}