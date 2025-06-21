package executor

import (
	"testing"

	"github.com/dshills/QuantaDB/internal/sql/types"
)

func TestSemiJoinBasic(t *testing.T) {
	// Create test data
	ordersSchema := &Schema{
		Columns: []Column{
			{Name: "order_id", Type: types.Integer},
			{Name: "customer_id", Type: types.Integer},
		},
	}

	itemsSchema := &Schema{
		Columns: []Column{
			{Name: "item_id", Type: types.Integer},
			{Name: "order_id", Type: types.Integer},
		},
	}

	// Orders with IDs 1, 2, 3
	ordersRows := []*Row{
		{Values: []types.Value{types.NewIntegerValue(1), types.NewIntegerValue(100)}},
		{Values: []types.Value{types.NewIntegerValue(2), types.NewIntegerValue(101)}},
		{Values: []types.Value{types.NewIntegerValue(3), types.NewIntegerValue(102)}},
	}

	// Items only for orders 1 and 3
	itemsRows := []*Row{
		{Values: []types.Value{types.NewIntegerValue(1), types.NewIntegerValue(1)}},
		{Values: []types.Value{types.NewIntegerValue(2), types.NewIntegerValue(1)}}, // Multiple items for order 1
		{Values: []types.Value{types.NewIntegerValue(3), types.NewIntegerValue(3)}},
	}

	ordersOp := NewMockOperator(ordersRows, ordersSchema)
	itemsOp := NewMockOperator(itemsRows, itemsSchema)

	// Create evaluators for join keys
	leftKey := &columnRefEvaluator{columnIdx: 0, resolved: true}  // order_id from orders
	rightKey := &columnRefEvaluator{columnIdx: 1, resolved: true} // order_id from items

	// Test semi join (EXISTS)
	semiJoin := NewSemiJoinOperator(
		ordersOp, itemsOp,
		[]ExprEvaluator{leftKey},
		[]ExprEvaluator{rightKey},
		nil, // No additional condition
		SemiJoinTypeSemi,
		false, // No NULL handling needed
	)

	ctx := &ExecContext{}
	if err := semiJoin.Open(ctx); err != nil {
		t.Fatalf("Failed to open semi join: %v", err)
	}

	var results []*Row
	for {
		row, err := semiJoin.Next()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if row == nil {
			break
		}
		results = append(results, row)
	}

	// Should return orders 1 and 3 (which have items)
	if len(results) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(results))
	}

	// Verify the order IDs
	expectedIDs := []int32{1, 3}
	for i, row := range results {
		id, _ := row.Values[0].AsInt()
		if id != expectedIDs[i] {
			t.Errorf("Row %d: expected order_id %d, got %d", i, expectedIDs[i], id)
		}
	}
}

func TestAntiJoinBasic(t *testing.T) {
	// Same setup as semi join test
	ordersSchema := &Schema{
		Columns: []Column{
			{Name: "order_id", Type: types.Integer},
			{Name: "customer_id", Type: types.Integer},
		},
	}

	itemsSchema := &Schema{
		Columns: []Column{
			{Name: "item_id", Type: types.Integer},
			{Name: "order_id", Type: types.Integer},
		},
	}

	ordersRows := []*Row{
		{Values: []types.Value{types.NewIntegerValue(1), types.NewIntegerValue(100)}},
		{Values: []types.Value{types.NewIntegerValue(2), types.NewIntegerValue(101)}},
		{Values: []types.Value{types.NewIntegerValue(3), types.NewIntegerValue(102)}},
	}

	itemsRows := []*Row{
		{Values: []types.Value{types.NewIntegerValue(1), types.NewIntegerValue(1)}},
		{Values: []types.Value{types.NewIntegerValue(3), types.NewIntegerValue(3)}},
	}

	ordersOp := NewMockOperator(ordersRows, ordersSchema)
	itemsOp := NewMockOperator(itemsRows, itemsSchema)

	leftKey := &columnRefEvaluator{columnIdx: 0, resolved: true}
	rightKey := &columnRefEvaluator{columnIdx: 1, resolved: true}

	// Test anti join (NOT EXISTS)
	antiJoin := NewSemiJoinOperator(
		ordersOp, itemsOp,
		[]ExprEvaluator{leftKey},
		[]ExprEvaluator{rightKey},
		nil,
		SemiJoinTypeAnti,
		false,
	)

	ctx := &ExecContext{}
	if err := antiJoin.Open(ctx); err != nil {
		t.Fatalf("Failed to open anti join: %v", err)
	}

	var results []*Row
	for {
		row, err := antiJoin.Next()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if row == nil {
			break
		}
		results = append(results, row)
	}

	// Should return only order 2 (which has no items)
	if len(results) != 1 {
		t.Errorf("Expected 1 row, got %d", len(results))
	}

	if len(results) > 0 {
		id, _ := results[0].Values[0].AsInt()
		if id != 2 {
			t.Errorf("Expected order_id 2, got %d", id)
		}
	}
}

func TestSemiJoinWithNulls(t *testing.T) {
	leftSchema := &Schema{
		Columns: []Column{
			{Name: "id", Type: types.Integer},
			{Name: "value", Type: types.Integer},
		},
	}

	rightSchema := &Schema{
		Columns: []Column{
			{Name: "id", Type: types.Integer},
		},
	}

	// Left has values 1, 2, 3, NULL
	leftRows := []*Row{
		{Values: []types.Value{types.NewIntegerValue(1), types.NewIntegerValue(10)}},
		{Values: []types.Value{types.NewIntegerValue(2), types.NewIntegerValue(20)}},
		{Values: []types.Value{types.NewIntegerValue(3), types.NewIntegerValue(30)}},
		{Values: []types.Value{types.NewNullValue(), types.NewIntegerValue(40)}},
	}

	// Right has values 1, 2, NULL
	rightRows := []*Row{
		{Values: []types.Value{types.NewIntegerValue(1)}},
		{Values: []types.Value{types.NewIntegerValue(2)}},
		{Values: []types.Value{types.NewNullValue()}},
	}

	leftOp := NewMockOperator(leftRows, leftSchema)
	rightOp := NewMockOperator(rightRows, rightSchema)

	leftKey := &columnRefEvaluator{columnIdx: 0, resolved: true}
	rightKey := &columnRefEvaluator{columnIdx: 0, resolved: true}

	// Test semi join with NULLs
	semiJoin := NewSemiJoinOperator(
		leftOp, rightOp,
		[]ExprEvaluator{leftKey},
		[]ExprEvaluator{rightKey},
		nil,
		SemiJoinTypeSemi,
		false,
	)

	ctx := &ExecContext{}
	if err := semiJoin.Open(ctx); err != nil {
		t.Fatalf("Failed to open semi join: %v", err)
	}

	var results []*Row
	for {
		row, err := semiJoin.Next()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if row == nil {
			break
		}
		results = append(results, row)
	}

	// Should return rows with id 1 and 2 (NULLs don't match)
	if len(results) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(results))
	}
}

func TestAntiJoinWithNullHandling(t *testing.T) {
	leftSchema := &Schema{
		Columns: []Column{
			{Name: "id", Type: types.Integer},
		},
	}

	rightSchema := &Schema{
		Columns: []Column{
			{Name: "id", Type: types.Integer},
		},
	}

	// Left has values 1, 2, 3
	leftRows := []*Row{
		{Values: []types.Value{types.NewIntegerValue(1)}},
		{Values: []types.Value{types.NewIntegerValue(2)}},
		{Values: []types.Value{types.NewIntegerValue(3)}},
	}

	// Right has values 1, NULL (simulating NOT IN with NULL)
	rightRows := []*Row{
		{Values: []types.Value{types.NewIntegerValue(1)}},
		{Values: []types.Value{types.NewNullValue()}},
	}

	leftOp := NewMockOperator(leftRows, leftSchema)
	rightOp := NewMockOperator(rightRows, rightSchema)

	leftKey := &columnRefEvaluator{columnIdx: 0, resolved: true}
	rightKey := &columnRefEvaluator{columnIdx: 0, resolved: true}

	// Test anti join with NULL handling (NOT IN semantics)
	antiJoin := NewSemiJoinOperator(
		leftOp, rightOp,
		[]ExprEvaluator{leftKey},
		[]ExprEvaluator{rightKey},
		nil,
		SemiJoinTypeAnti,
		true, // Enable NULL handling for NOT IN
	)

	ctx := &ExecContext{}
	if err := antiJoin.Open(ctx); err != nil {
		t.Fatalf("Failed to open anti join: %v", err)
	}

	var results []*Row
	for {
		row, err := antiJoin.Next()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if row == nil {
			break
		}
		results = append(results, row)
	}

	// With NOT IN and NULL in right side, should return no rows
	if len(results) != 0 {
		t.Errorf("Expected 0 rows due to NULL in NOT IN, got %d", len(results))
	}
}

func TestNestedLoopSemiJoin(t *testing.T) {
	// Test with complex join condition
	leftSchema := &Schema{
		Columns: []Column{
			{Name: "id", Type: types.Integer},
			{Name: "score", Type: types.Integer},
		},
	}

	rightSchema := &Schema{
		Columns: []Column{
			{Name: "min_score", Type: types.Integer},
			{Name: "category", Type: types.Text},
		},
	}

	leftRows := []*Row{
		{Values: []types.Value{types.NewIntegerValue(1), types.NewIntegerValue(85)}},
		{Values: []types.Value{types.NewIntegerValue(2), types.NewIntegerValue(65)}},
		{Values: []types.Value{types.NewIntegerValue(3), types.NewIntegerValue(95)}},
	}

	rightRows := []*Row{
		{Values: []types.Value{types.NewIntegerValue(80), types.NewTextValue("A")}},
		{Values: []types.Value{types.NewIntegerValue(90), types.NewTextValue("B")}},
	}

	leftOp := NewMockOperator(leftRows, leftSchema)
	rightOp := NewMockOperator(rightRows, rightSchema)

	// Join condition: left.score >= right.min_score
	joinCond := &mockScoreComparison{}

	nestedJoin := NewNestedLoopSemiJoinOperator(
		leftOp, rightOp,
		joinCond,
		SemiJoinTypeSemi,
		false,
	)

	ctx := &ExecContext{}
	if err := nestedJoin.Open(ctx); err != nil {
		t.Fatalf("Failed to open nested loop semi join: %v", err)
	}

	var results []*Row
	for {
		row, err := nestedJoin.Next()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if row == nil {
			break
		}
		results = append(results, row)
	}

	// Rows 1 (85>=80) and 3 (95>=80 and 95>=90) should match
	if len(results) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(results))
	}
}

// mockScoreComparison evaluates left.score >= right.min_score
type mockScoreComparison struct{}

func (m *mockScoreComparison) Eval(row *Row, ctx *ExecContext) (types.Value, error) {
	// Combined row has left columns followed by right columns
	// left.score is at index 1, right.min_score is at index 2
	if len(row.Values) < 4 {
		return types.NewBooleanValue(false), nil
	}

	leftScore, err := row.Values[1].AsInt()
	if err != nil {
		return types.NewBooleanValue(false), nil
	}

	rightMinScore, err := row.Values[2].AsInt()
	if err != nil {
		return types.NewBooleanValue(false), nil
	}

	return types.NewBooleanValue(leftScore >= rightMinScore), nil
}
