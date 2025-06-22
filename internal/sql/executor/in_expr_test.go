package executor

import (
	"testing"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/types"
	"github.com/stretchr/testify/require"
)

func TestInExpressions(t *testing.T) {
	// Create a test schema
	schema := &Schema{
		Columns: []Column{
			{Name: "id", Type: types.Integer},
			{Name: "name", Type: types.Text},
			{Name: "category", Type: types.Text},
		},
	}

	// Create test rows
	rows := []*Row{
		{Values: []types.Value{
			types.NewValue(int64(1)),
			types.NewValue("Alice"),
			types.NewValue("A"),
		}},
		{Values: []types.Value{
			types.NewValue(int64(2)),
			types.NewValue("Bob"),
			types.NewValue("B"),
		}},
		{Values: []types.Value{
			types.NewValue(int64(3)),
			types.NewValue("Charlie"),
			types.NewValue("A"),
		}},
		{Values: []types.Value{
			types.NewValue(int64(4)),
			types.NewValue("David"),
			types.NewValue("C"),
		}},
	}

	tests := []struct {
		name     string
		expr     string
		expected []interface{} // Expected boolean values for each row
	}{
		// IN with value lists
		{
			name: "IN with integers",
			expr: "id IN (1, 3, 5)",
			expected: []interface{}{
				true,  // id=1 is in list
				false, // id=2 is not in list
				true,  // id=3 is in list
				false, // id=4 is not in list
			},
		},
		{
			name: "IN with strings",
			expr: "category IN ('A', 'C')",
			expected: []interface{}{
				true,  // category='A' is in list
				false, // category='B' is not in list
				true,  // category='A' is in list
				true,  // category='C' is in list
			},
		},
		{
			name: "NOT IN with integers",
			expr: "id NOT IN (2, 4)",
			expected: []interface{}{
				true,  // id=1 is not in list
				false, // id=2 is in list
				true,  // id=3 is not in list
				false, // id=4 is in list
			},
		},
		{
			name: "NOT IN with strings",
			expr: "name NOT IN ('Bob', 'David')",
			expected: []interface{}{
				true,  // Alice not in list
				false, // Bob is in list
				true,  // Charlie not in list
				false, // David is in list
			},
		},

		// Single value IN (equivalent to equality)
		{
			name: "IN with single value",
			expr: "id IN (2)",
			expected: []interface{}{
				false, // id=1
				true,  // id=2
				false, // id=3
				false, // id=4
			},
		},

		// Empty IN list (should always be false)
		{
			name: "IN with empty list",
			expr: "id IN ()",
			expected: []interface{}{
				false, // Always false for empty IN
				false,
				false,
				false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the expression using a SELECT wrapper to ensure proper parsing
			query := "SELECT " + tt.expr
			p := parser.NewParser(query)
			stmt, err := p.Parse()
			require.NoError(t, err)

			selectStmt, ok := stmt.(*parser.SelectStmt)
			require.True(t, ok)
			require.Len(t, selectStmt.Columns, 1)

			expr := selectStmt.Columns[0].Expr

			// Build evaluator with catalog columns
			catalogColumns := make([]catalog.Column, len(schema.Columns))
			for i, col := range schema.Columns {
				catalogColumns[i] = catalog.Column{
					Name:     col.Name,
					DataType: col.Type,
				}
			}

			ctx := &evalContext{
				columns: catalogColumns,
			}

			// Evaluate for each row
			for i, row := range rows {
				ctx.row = row
				result, err := evaluateExpression(expr, ctx)
				require.NoError(t, err)

				// Check expected result
				require.False(t, result.IsNull(), "Expected non-NULL for row %d", i)
				require.Equal(t, tt.expected[i], result.Data, "Row %d", i)
			}
		})
	}
}

func TestInExprNullHandling(t *testing.T) {
	// Create a test schema with nullable column
	schema := &Schema{
		Columns: []Column{
			{Name: "id", Type: types.Integer},
			{Name: "nullable_field", Type: types.Text},
		},
	}

	// Create test row with NULL value
	row := &Row{
		Values: []types.Value{
			types.NewValue(int64(1)),
			types.NewNullValue(), // NULL value
		},
	}

	tests := []struct {
		name     string
		expr     string
		expected interface{}
		isNull   bool
	}{
		// NULL handling in IN expressions
		{
			name:   "NULL IN value list (should be NULL)",
			expr:   "nullable_field IN ('A', 'B')",
			isNull: true, // NULL IN anything = NULL
		},
		{
			name:   "NULL NOT IN value list (should be NULL)",
			expr:   "nullable_field NOT IN ('A', 'B')",
			isNull: true, // NULL NOT IN anything = NULL
		},
		{
			name:   "value IN list with NULL (should be NULL if not found)",
			expr:   "id IN (2, NULL, 3)",
			isNull: true, // 1 not in (2,3) and NULL is present -> NULL
		},
		{
			name:     "value IN list with NULL (should be true if found)",
			expr:     "id IN (1, NULL, 3)",
			expected: true, // 1 is found, so result is true regardless of NULL
		},
	}

	catalogColumns := make([]catalog.Column, len(schema.Columns))
	for i, col := range schema.Columns {
		catalogColumns[i] = catalog.Column{
			Name:     col.Name,
			DataType: col.Type,
		}
	}

	ctx := &evalContext{
		row:     row,
		columns: catalogColumns,
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the expression
			query := "SELECT " + tt.expr
			p := parser.NewParser(query)
			stmt, err := p.Parse()
			require.NoError(t, err)

			selectStmt, ok := stmt.(*parser.SelectStmt)
			require.True(t, ok)
			require.Len(t, selectStmt.Columns, 1)

			expr := selectStmt.Columns[0].Expr

			// Evaluate
			result, err := evaluateExpression(expr, ctx)
			require.NoError(t, err)

			if tt.isNull {
				require.True(t, result.IsNull(), "Expected NULL")
			} else {
				require.False(t, result.IsNull(), "Expected non-NULL")
				require.Equal(t, tt.expected, result.Data)
			}
		})
	}
}

// Test IN with subqueries (these would need subquery evaluation to work)
func TestInSubqueryExpressions(t *testing.T) {
	// Note: This test would require full subquery execution capability
	// For now, we'll just test the parsing and planner integration

	testQueries := []string{
		"SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)",
		"SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM orders WHERE amount > 100)",
		"SELECT * FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE status = 'active')",
	}

	for _, query := range testQueries {
		t.Run(query, func(t *testing.T) {
			// Test that parsing works
			p := parser.NewParser(query)
			stmt, err := p.Parse()
			require.NoError(t, err)
			require.NotNil(t, stmt)

			selectStmt, ok := stmt.(*parser.SelectStmt)
			require.True(t, ok)
			require.NotNil(t, selectStmt.Where)

			// This confirms the parser can handle IN with subqueries
			// Full execution would require setting up tables and data
		})
	}
}
