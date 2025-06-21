package executor

import (
	"testing"

	"github.com/dshills/QuantaDB/internal/sql/types"
)

func TestSemiJoinPlannerIntegration(t *testing.T) {
	// This test demonstrates how semi/anti joins integrate with the query planner
	// In a real implementation, EXISTS/IN subqueries would be transformed to semi/anti joins

	t.Run("SemiJoin_Schema_Preservation", func(t *testing.T) {
		// Semi join should preserve only the left schema
		leftSchema := &Schema{
			Columns: []Column{
				{Name: "id", Type: types.Integer},
				{Name: "name", Type: types.Text},
			},
		}

		rightSchema := &Schema{
			Columns: []Column{
				{Name: "id", Type: types.Integer},
				{Name: "active", Type: types.Boolean},
			},
		}

		leftRows := []*Row{
			{Values: []types.Value{types.NewIntegerValue(1), types.NewTextValue("Alice")}},
			{Values: []types.Value{types.NewIntegerValue(2), types.NewTextValue("Bob")}},
		}

		rightRows := []*Row{
			{Values: []types.Value{types.NewIntegerValue(1), types.NewBooleanValue(true)}},
		}

		leftOp := NewMockOperator(leftRows, leftSchema)
		rightOp := NewMockOperator(rightRows, rightSchema)

		leftKey := &columnRefEvaluator{columnIdx: 0, resolved: true}
		rightKey := &columnRefEvaluator{columnIdx: 0, resolved: true}

		semiJoin := NewSemiJoinOperator(
			leftOp, rightOp,
			[]ExprEvaluator{leftKey},
			[]ExprEvaluator{rightKey},
			nil,
			SemiJoinTypeSemi,
			false,
		)

		// Verify schema is preserved
		if len(semiJoin.Schema().Columns) != 2 {
			t.Errorf("Expected 2 columns in output schema, got %d", len(semiJoin.Schema().Columns))
		}

		if semiJoin.Schema().Columns[0].Name != "id" || semiJoin.Schema().Columns[1].Name != "name" {
			t.Error("Semi join should preserve left schema")
		}
	})

	t.Run("QueryPattern_EXISTS", func(t *testing.T) {
		// Demonstrate how EXISTS would be transformed to semi join
		// Original query:
		// SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)

		// This would be transformed by the planner to:
		// SemiJoin(users, orders, u.id = o.user_id)

		// The key insight is that EXISTS doesn't care about the SELECT list in the subquery
		// It only cares about existence, making it a perfect fit for semi join

		usersSchema := &Schema{
			Columns: []Column{
				{Name: "id", Type: types.Integer},
				{Name: "username", Type: types.Text},
			},
		}

		ordersSchema := &Schema{
			Columns: []Column{
				{Name: "order_id", Type: types.Integer},
				{Name: "user_id", Type: types.Integer},
			},
		}

		// Users 1, 2, 3
		usersRows := []*Row{
			{Values: []types.Value{types.NewIntegerValue(1), types.NewTextValue("alice")}},
			{Values: []types.Value{types.NewIntegerValue(2), types.NewTextValue("bob")}},
			{Values: []types.Value{types.NewIntegerValue(3), types.NewTextValue("charlie")}},
		}

		// Orders only for users 1 and 3
		ordersRows := []*Row{
			{Values: []types.Value{types.NewIntegerValue(101), types.NewIntegerValue(1)}},
			{Values: []types.Value{types.NewIntegerValue(102), types.NewIntegerValue(3)}},
			{Values: []types.Value{types.NewIntegerValue(103), types.NewIntegerValue(1)}}, // User 1 has multiple orders
		}

		usersOp := NewMockOperator(usersRows, usersSchema)
		ordersOp := NewMockOperator(ordersRows, ordersSchema)

		// Join on users.id = orders.user_id
		leftKey := &columnRefEvaluator{columnIdx: 0, resolved: true}  // users.id
		rightKey := &columnRefEvaluator{columnIdx: 1, resolved: true} // orders.user_id

		semiJoin := NewSemiJoinOperator(
			usersOp, ordersOp,
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

		var results []string
		for {
			row, err := semiJoin.Next()
			if err != nil {
				t.Fatalf("Error: %v", err)
			}
			if row == nil {
				break
			}
			username, _ := row.Values[1].AsString()
			results = append(results, username)
		}

		// Should return alice and charlie (not bob)
		if len(results) != 2 {
			t.Errorf("Expected 2 users with orders, got %d", len(results))
		}

		expectedUsers := map[string]bool{"alice": true, "charlie": true}
		for _, user := range results {
			if !expectedUsers[user] {
				t.Errorf("Unexpected user in results: %s", user)
			}
		}
	})

	t.Run("QueryPattern_IN_Subquery", func(t *testing.T) {
		// Demonstrate how IN (subquery) would be transformed
		// Original query:
		// SELECT * FROM products WHERE category_id IN (SELECT id FROM categories WHERE active = true)

		// This would be transformed to:
		// SemiJoin(products, Filter(categories, active=true), products.category_id = categories.id)

		productsSchema := &Schema{
			Columns: []Column{
				{Name: "id", Type: types.Integer},
				{Name: "name", Type: types.Text},
				{Name: "category_id", Type: types.Integer},
			},
		}

		categoriesSchema := &Schema{
			Columns: []Column{
				{Name: "id", Type: types.Integer},
				{Name: "name", Type: types.Text},
				{Name: "active", Type: types.Boolean},
			},
		}

		// Products in categories 1, 2, 3
		productsRows := []*Row{
			{Values: []types.Value{types.NewIntegerValue(101), types.NewTextValue("Laptop"), types.NewIntegerValue(1)}},
			{Values: []types.Value{types.NewIntegerValue(102), types.NewTextValue("Mouse"), types.NewIntegerValue(2)}},
			{Values: []types.Value{types.NewIntegerValue(103), types.NewTextValue("Keyboard"), types.NewIntegerValue(2)}},
			{Values: []types.Value{types.NewIntegerValue(104), types.NewTextValue("Monitor"), types.NewIntegerValue(3)}},
		}

		// Only categories 1 and 2 are active
		categoriesRows := []*Row{
			{Values: []types.Value{types.NewIntegerValue(1), types.NewTextValue("Electronics"), types.NewBooleanValue(true)}},
			{Values: []types.Value{types.NewIntegerValue(2), types.NewTextValue("Accessories"), types.NewBooleanValue(true)}},
			{Values: []types.Value{types.NewIntegerValue(3), types.NewTextValue("Archived"), types.NewBooleanValue(false)}},
		}

		// First filter categories to only active ones
		categoriesOp := NewMockOperator(categoriesRows, categoriesSchema)
		activeFilter := &mockActiveFilter{}
		filteredCategories := NewFilterOperator(categoriesOp, activeFilter)

		productsOp := NewMockOperator(productsRows, productsSchema)

		// Join on products.category_id = categories.id
		leftKey := &columnRefEvaluator{columnIdx: 2, resolved: true}  // products.category_id
		rightKey := &columnRefEvaluator{columnIdx: 0, resolved: true} // categories.id

		semiJoin := NewSemiJoinOperator(
			productsOp, filteredCategories,
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

		var productNames []string
		for {
			row, err := semiJoin.Next()
			if err != nil {
				t.Fatalf("Error: %v", err)
			}
			if row == nil {
				break
			}
			name, _ := row.Values[1].AsString()
			productNames = append(productNames, name)
		}

		// Should return Laptop, Mouse, Keyboard (not Monitor from archived category)
		if len(productNames) != 3 {
			t.Errorf("Expected 3 products in active categories, got %d", len(productNames))
		}

		// Monitor should not be in results
		for _, name := range productNames {
			if name == "Monitor" {
				t.Error("Monitor from archived category should not be in results")
			}
		}
	})
}

// mockActiveFilter filters for active = true
type mockActiveFilter struct{}

func (m *mockActiveFilter) Eval(row *Row, ctx *ExecContext) (types.Value, error) {
	// Check the active column (index 2)
	if len(row.Values) > 2 {
		return row.Values[2], nil // Return the boolean value directly
	}
	return types.NewBooleanValue(false), nil
}
