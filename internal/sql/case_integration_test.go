package sql_test

import (
	"testing"

	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/stretchr/testify/require"
)

func TestCaseExpressionIntegration(t *testing.T) {

	// Test query with CASE expression
	query := `
		SELECT 
			id,
			CASE status 
				WHEN 'pending' THEN 'Waiting'
				WHEN 'shipped' THEN 'In Transit'
				WHEN 'delivered' THEN 'Complete'
				ELSE 'Unknown'
			END as order_status,
			CASE 
				WHEN amount > 100 THEN 'High Value'
				WHEN amount > 50 THEN 'Medium Value'
				ELSE 'Low Value'
			END as value_category
		FROM orders
	`

	// Parse the query
	p := parser.NewParser(query)
	stmt, err := p.Parse()
	require.NoError(t, err)

	// The parsed statement should contain CASE expressions
	selectStmt, ok := stmt.(*parser.SelectStmt)
	require.True(t, ok)
	require.Len(t, selectStmt.Columns, 3)

	// Verify the second column is a simple CASE
	col2 := selectStmt.Columns[1]
	require.Equal(t, "order_status", col2.Alias)
	caseExpr2, ok := col2.Expr.(*parser.CaseExpr)
	require.True(t, ok)
	require.NotNil(t, caseExpr2.Expr) // Simple CASE has an expression
	require.Len(t, caseExpr2.WhenList, 3)
	require.NotNil(t, caseExpr2.Else)

	// Verify the third column is a searched CASE
	col3 := selectStmt.Columns[2]
	require.Equal(t, "value_category", col3.Alias)
	caseExpr3, ok := col3.Expr.(*parser.CaseExpr)
	require.True(t, ok)
	require.Nil(t, caseExpr3.Expr) // Searched CASE has no main expression
	require.Len(t, caseExpr3.WhenList, 2)
	require.NotNil(t, caseExpr3.Else)
}

// Test that demonstrates CASE usage in aggregation (TPC-H style)
func TestCaseInAggregation(t *testing.T) {
	query := `
		SELECT 
			COUNT(CASE WHEN status = 'active' THEN 1 END) as active_count,
			COUNT(CASE WHEN status = 'inactive' THEN 1 END) as inactive_count,
			SUM(CASE WHEN status = 'active' THEN amount ELSE 0 END) as active_total
		FROM users
	`

	// Parse the query
	p := parser.NewParser(query)
	stmt, err := p.Parse()
	require.NoError(t, err)

	selectStmt, ok := stmt.(*parser.SelectStmt)
	require.True(t, ok)
	require.Len(t, selectStmt.Columns, 3)

	// Verify each column is an aggregate with CASE
	for i, col := range selectStmt.Columns {
		funcCall, ok := col.Expr.(*parser.FunctionCall)
		require.True(t, ok, "Column %d should be a function call", i)
		
		if i < 2 {
			require.Equal(t, "COUNT", funcCall.Name)
		} else {
			require.Equal(t, "SUM", funcCall.Name)
		}
		
		// The argument should contain a CASE expression
		require.Len(t, funcCall.Args, 1)
		caseExpr, ok := funcCall.Args[0].(*parser.CaseExpr)
		require.True(t, ok, "Function argument should be a CASE expression")
		require.Nil(t, caseExpr.Expr) // Searched CASE
	}
}

// Test nested CASE expressions
func TestNestedCaseExpressions(t *testing.T) {
	query := `
		SELECT 
			CASE 
				WHEN category = 'electronics' THEN
					CASE 
						WHEN price > 1000 THEN 'Premium Electronics'
						WHEN price > 500 THEN 'Mid-range Electronics'
						ELSE 'Budget Electronics'
					END
				WHEN category = 'clothing' THEN
					CASE 
						WHEN price > 100 THEN 'Designer Clothing'
						ELSE 'Regular Clothing'
					END
				ELSE 'Other'
			END as product_tier
		FROM products
	`

	// Parse the query
	p := parser.NewParser(query)
	stmt, err := p.Parse()
	require.NoError(t, err)

	selectStmt, ok := stmt.(*parser.SelectStmt)
	require.True(t, ok)
	require.Len(t, selectStmt.Columns, 1)

	// The column should be a CASE expression
	col := selectStmt.Columns[0]
	require.Equal(t, "product_tier", col.Alias)
	
	caseExpr, ok := col.Expr.(*parser.CaseExpr)
	require.True(t, ok)
	require.Nil(t, caseExpr.Expr) // Searched CASE
	require.Len(t, caseExpr.WhenList, 2)
	
	// The THEN clauses should contain nested CASE expressions
	for i, when := range caseExpr.WhenList {
		nestedCase, ok := when.Result.(*parser.CaseExpr)
		require.True(t, ok, "WHEN clause %d should have nested CASE in THEN", i)
		require.Nil(t, nestedCase.Expr) // Also searched CASE
	}
}