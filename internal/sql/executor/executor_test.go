package executor

import (
	"context"
	"testing"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/engine"
	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

// Helper function to create int pointer
func intPtr(i int) *int {
	return &i
}

func TestBasicExecutor(t *testing.T) {
	// Create test catalog and engine
	cat := catalog.NewMemoryCatalog()
	eng := engine.NewMemoryEngine()
	
	// Create a test table
	tableSchema := &catalog.TableSchema{
		SchemaName: "public",
		TableName:  "users",
		Columns: []catalog.ColumnDef{
			{Name: "id", DataType: types.Integer, IsNullable: false},
			{Name: "name", DataType: types.Text, IsNullable: false},
			{Name: "age", DataType: types.Integer, IsNullable: true},
		},
	}
	
	_, err := cat.CreateTable(tableSchema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	
	// Insert some test data using proper row serialization
	rowSchema := &Schema{
		Columns: []Column{
			{Name: "id", Type: types.Integer, Nullable: false},
			{Name: "name", Type: types.Text, Nullable: false},
			{Name: "age", Type: types.Integer, Nullable: true},
		},
	}
	rowFormat := NewRowFormat(rowSchema)
	keyFormat := &RowKeyFormat{
		TableName:  "users",
		SchemaName: "public",
	}

	testUsers := []struct {
		id   int
		name string
		age  *int
	}{
		{1, "Alice", intPtr(25)},
		{2, "Bob", intPtr(30)},
		{3, "Charlie", nil}, // NULL age
		{4, "David", intPtr(35)},
		{5, "Eve", intPtr(20)},
	}

	for _, user := range testUsers {
		row := &Row{
			Values: []types.Value{
				types.NewValue(int64(user.id)),
				types.NewValue(user.name),
			},
		}
		if user.age != nil {
			row.Values = append(row.Values, types.NewValue(int64(*user.age)))
		} else {
			row.Values = append(row.Values, types.NewNullValue())
		}

		value, err := rowFormat.Serialize(row)
		if err != nil {
			t.Fatalf("Failed to serialize row: %v", err)
		}

		key := keyFormat.GenerateRowKey(user.id)
		if err := eng.Put(context.Background(), key, value); err != nil {
			t.Fatalf("Failed to insert test data: %v", err)
		}
	}
	
	executor := NewBasicExecutor(cat, eng)
	
	t.Run("Simple scan", func(t *testing.T) {
		// Create a simple scan plan
		schema := &planner.Schema{
			Columns: []planner.Column{
				{Name: "id", DataType: types.Integer, Nullable: false},
				{Name: "name", DataType: types.Text, Nullable: false},
				{Name: "age", DataType: types.Integer, Nullable: true},
			},
		}
		
		plan := planner.NewLogicalScan("users", "users", schema)
		
		ctx := &ExecContext{
			Catalog: cat,
			Engine:  eng,
			Stats:   &ExecStats{},
		}
		
		result, err := executor.Execute(plan, ctx)
		if err != nil {
			t.Fatalf("Failed to execute plan: %v", err)
		}
		defer result.Close()
		
		// Count rows
		rowCount := 0
		for {
			row, err := result.Next()
			if err != nil {
				t.Fatalf("Error getting next row: %v", err)
			}
			if row == nil {
				break
			}
			rowCount++
		}
		
		if rowCount != 5 {
			t.Errorf("Expected 5 rows, got %d", rowCount)
		}
		
		// Check statistics
		if ctx.Stats.RowsRead != 5 {
			t.Errorf("Expected RowsRead=5, got %d", ctx.Stats.RowsRead)
		}
	})
	
	t.Run("Scan with limit", func(t *testing.T) {
		// Create scan with limit
		schema := &planner.Schema{
			Columns: []planner.Column{
				{Name: "id", DataType: types.Integer, Nullable: false},
				{Name: "name", DataType: types.Text, Nullable: false},
				{Name: "age", DataType: types.Integer, Nullable: true},
			},
		}
		
		scan := planner.NewLogicalScan("users", "users", schema)
		plan := planner.NewLogicalLimit(scan, 3, 0)
		
		ctx := &ExecContext{
			Catalog: cat,
			Engine:  eng,
			Stats:   &ExecStats{},
		}
		
		result, err := executor.Execute(plan, ctx)
		if err != nil {
			t.Fatalf("Failed to execute plan: %v", err)
		}
		defer result.Close()
		
		// Count rows
		rowCount := 0
		for {
			row, err := result.Next()
			if err != nil {
				t.Fatalf("Error getting next row: %v", err)
			}
			if row == nil {
				break
			}
			rowCount++
		}
		
		if rowCount != 3 {
			t.Errorf("Expected 3 rows (limit), got %d", rowCount)
		}
	})
	
	t.Run("Scan with offset", func(t *testing.T) {
		// Create scan with offset
		schema := &planner.Schema{
			Columns: []planner.Column{
				{Name: "id", DataType: types.Integer, Nullable: false},
			},
		}
		
		scan := planner.NewLogicalScan("users", "users", schema)
		plan := planner.NewLogicalLimit(scan, -1, 2) // Skip first 2 rows
		
		ctx := &ExecContext{
			Catalog: cat,
			Engine:  eng,
			Stats:   &ExecStats{},
		}
		
		result, err := executor.Execute(plan, ctx)
		if err != nil {
			t.Fatalf("Failed to execute plan: %v", err)
		}
		defer result.Close()
		
		// Count rows
		rowCount := 0
		for {
			row, err := result.Next()
			if err != nil {
				t.Fatalf("Error getting next row: %v", err)
			}
			if row == nil {
				break
			}
			rowCount++
		}
		
		if rowCount != 3 {
			t.Errorf("Expected 3 rows (5 - 2 offset), got %d", rowCount)
		}
	})
}

func TestExpressionEvaluation(t *testing.T) {
	// Test expression evaluators
	row := &Row{
		Values: []types.Value{
			types.NewValue(int64(25)),    // age
			types.NewValue("John"),        // name
			types.NewValue(true),          // active
			types.NewNullValue(),          // nullable field
		},
	}
	
	ctx := &ExecContext{}
	
	t.Run("Literal evaluation", func(t *testing.T) {
		lit := &literalEvaluator{value: types.NewValue(int64(42))}
		
		val, err := lit.Eval(row, ctx)
		if err != nil {
			t.Fatalf("Failed to evaluate literal: %v", err)
		}
		
		if val.Data != int64(42) {
			t.Errorf("Expected 42, got %v", val.Data)
		}
	})
	
	t.Run("Binary arithmetic", func(t *testing.T) {
		// Test addition: 10 + 20
		left := &literalEvaluator{value: types.NewValue(int64(10))}
		right := &literalEvaluator{value: types.NewValue(int64(20))}
		
		add := &binaryOpEvaluator{
			left:     left,
			right:    right,
			operator: planner.OpAdd,
			dataType: types.Integer,
		}
		
		val, err := add.Eval(row, ctx)
		if err != nil {
			t.Fatalf("Failed to evaluate addition: %v", err)
		}
		
		if val.Data != int64(30) {
			t.Errorf("Expected 30, got %v", val.Data)
		}
	})
	
	t.Run("Binary comparison", func(t *testing.T) {
		// Test comparison: 25 > 20
		left := &literalEvaluator{value: types.NewValue(int64(25))}
		right := &literalEvaluator{value: types.NewValue(int64(20))}
		
		gt := &binaryOpEvaluator{
			left:     left,
			right:    right,
			operator: planner.OpGreater,
			dataType: types.Boolean,
		}
		
		val, err := gt.Eval(row, ctx)
		if err != nil {
			t.Fatalf("Failed to evaluate comparison: %v", err)
		}
		
		if val.Data != true {
			t.Errorf("Expected true, got %v", val.Data)
		}
	})
	
	t.Run("Logical operations", func(t *testing.T) {
		// Test AND: true AND false
		left := &literalEvaluator{value: types.NewValue(true)}
		right := &literalEvaluator{value: types.NewValue(false)}
		
		and := &binaryOpEvaluator{
			left:     left,
			right:    right,
			operator: planner.OpAnd,
			dataType: types.Boolean,
		}
		
		val, err := and.Eval(row, ctx)
		if err != nil {
			t.Fatalf("Failed to evaluate AND: %v", err)
		}
		
		if val.Data != false {
			t.Errorf("Expected false, got %v", val.Data)
		}
		
		// Test OR: true OR false
		or := &binaryOpEvaluator{
			left:     left,
			right:    right,
			operator: planner.OpOr,
			dataType: types.Boolean,
		}
		
		val, err = or.Eval(row, ctx)
		if err != nil {
			t.Fatalf("Failed to evaluate OR: %v", err)
		}
		
		if val.Data != true {
			t.Errorf("Expected true, got %v", val.Data)
		}
	})
	
	t.Run("Unary operations", func(t *testing.T) {
		// Test NOT: NOT true
		operand := &literalEvaluator{value: types.NewValue(true)}
		
		not := &unaryOpEvaluator{
			operand:  operand,
			operator: planner.OpNot,
			dataType: types.Boolean,
		}
		
		val, err := not.Eval(row, ctx)
		if err != nil {
			t.Fatalf("Failed to evaluate NOT: %v", err)
		}
		
		if val.Data != false {
			t.Errorf("Expected false, got %v", val.Data)
		}
		
		// Test negate: -42
		num := &literalEvaluator{value: types.NewValue(int64(42))}
		
		neg := &unaryOpEvaluator{
			operand:  num,
			operator: planner.OpNegate,
			dataType: types.Integer,
		}
		
		val, err = neg.Eval(row, ctx)
		if err != nil {
			t.Fatalf("Failed to evaluate negate: %v", err)
		}
		
		if val.Data != int64(-42) {
			t.Errorf("Expected -42, got %v", val.Data)
		}
	})
	
	t.Run("NULL handling", func(t *testing.T) {
		// Test IS NULL
		null := &literalEvaluator{value: types.NewNullValue()}
		
		isNull := &unaryOpEvaluator{
			operand:  null,
			operator: planner.OpIsNull,
			dataType: types.Boolean,
		}
		
		val, err := isNull.Eval(row, ctx)
		if err != nil {
			t.Fatalf("Failed to evaluate IS NULL: %v", err)
		}
		
		if val.Data != true {
			t.Errorf("Expected true for IS NULL, got %v", val.Data)
		}
		
		// Test IS NOT NULL
		notNull := &unaryOpEvaluator{
			operand:  null,
			operator: planner.OpIsNotNull,
			dataType: types.Boolean,
		}
		
		val, err = notNull.Eval(row, ctx)
		if err != nil {
			t.Fatalf("Failed to evaluate IS NOT NULL: %v", err)
		}
		
		if val.Data != false {
			t.Errorf("Expected false for IS NOT NULL, got %v", val.Data)
		}
	})
	
	t.Run("NULL in arithmetic", func(t *testing.T) {
		// Test NULL + 10
		left := &literalEvaluator{value: types.NewNullValue()}
		right := &literalEvaluator{value: types.NewValue(int64(10))}
		
		add := &binaryOpEvaluator{
			left:     left,
			right:    right,
			operator: planner.OpAdd,
			dataType: types.Integer,
		}
		
		val, err := add.Eval(row, ctx)
		if err != nil {
			t.Fatalf("Failed to evaluate NULL arithmetic: %v", err)
		}
		
		if !val.IsNull() {
			t.Errorf("Expected NULL result, got %v", val.Data)
		}
	})
}