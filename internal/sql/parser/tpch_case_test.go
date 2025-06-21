package parser

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTPCHCaseQuery(t *testing.T) {
	// Test the TPC-H Query 8 which uses CASE WHEN
	query := `
		SELECT o_year,
			   sum(case when nation = 'BRAZIL' then volume else 0 end) / sum(volume) as mkt_share
		FROM (
		  SELECT extract(year from o_orderdate) as o_year,
				 l_extendedprice * (1 - l_discount) as volume,
				 n2.n_name as nation
		  FROM part, supplier, lineitem, orders, customer, nation n1, nation n2, region
		  WHERE p_partkey = l_partkey
			AND s_suppkey = l_suppkey
			AND l_orderkey = o_orderkey
			AND o_custkey = c_custkey
			AND c_nationkey = n1.n_nationkey
			AND n1.n_regionkey = r_regionkey
			AND r_name = 'AMERICA'
			AND s_nationkey = n2.n_nationkey
			AND o_orderdate between date '1995-01-01' and date '1996-12-31'
			AND p_type = 'ECONOMY ANODIZED STEEL'
		) as all_nations
		GROUP BY o_year
		ORDER BY o_year`

	p := NewParser(query)
	stmt, err := p.Parse()
	require.NoError(t, err)
	require.NotNil(t, stmt)

	// Verify it's a SELECT statement
	selectStmt, ok := stmt.(*SelectStmt)
	require.True(t, ok)

	// Check that we have 2 columns
	require.Len(t, selectStmt.Columns, 2)

	// The second column should contain a division of two SUM expressions
	// The first SUM should contain a CASE expression
	col2 := selectStmt.Columns[1]
	require.Equal(t, "mkt_share", col2.Alias)

	// Verify the expression structure contains CASE
	exprStr := col2.Expr.String()
	require.Contains(t, exprStr, "CASE")
	require.Contains(t, exprStr, "WHEN")
	require.Contains(t, exprStr, "THEN")
	require.Contains(t, exprStr, "ELSE")
}

func TestSimpleTPCHStyleCase(t *testing.T) {
	// Simplified version of TPC-H case usage
	query := `SELECT 
		sum(case when nation = 'BRAZIL' then volume else 0 end) as brazil_volume,
		sum(case when nation = 'USA' then volume else 0 end) as usa_volume
	FROM sales`

	p := NewParser(query)
	stmt, err := p.Parse()
	require.NoError(t, err)
	require.NotNil(t, stmt)

	selectStmt, ok := stmt.(*SelectStmt)
	require.True(t, ok)
	require.Len(t, selectStmt.Columns, 2)

	// Both columns should be SUM with CASE inside
	for i, expectedAlias := range []string{"brazil_volume", "usa_volume"} {
		col := selectStmt.Columns[i]
		require.Equal(t, expectedAlias, col.Alias)
		
		// Check it's a function call (SUM)
		funcCall, ok := col.Expr.(*FunctionCall)
		require.True(t, ok)
		require.Equal(t, "SUM", funcCall.Name)
		require.Len(t, funcCall.Args, 1)
		
		// The argument should be a CASE expression
		caseExpr, ok := funcCall.Args[0].(*CaseExpr)
		require.True(t, ok)
		require.Nil(t, caseExpr.Expr) // Searched CASE
		require.Len(t, caseExpr.WhenList, 1)
		require.NotNil(t, caseExpr.Else)
	}
}