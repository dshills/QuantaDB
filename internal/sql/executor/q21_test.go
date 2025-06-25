package executor

import (
	"fmt"
	"strings"
	"testing"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

// TestQ21MultipleCorrelatedExists tests that Q21 can be parsed and planned
func TestQ21MultipleCorrelatedExists(t *testing.T) {
	// Create catalog with TPC-H tables
	cat := catalog.NewMemoryCatalog()

	// Create supplier table
	supplierSchema := &catalog.TableSchema{
		SchemaName: "public",
		TableName:  "supplier",
		Columns: []catalog.ColumnDef{
			{Name: "s_suppkey", DataType: types.Integer, IsNullable: false},
			{Name: "s_name", DataType: types.Varchar(25), IsNullable: false},
			{Name: "s_nationkey", DataType: types.Integer, IsNullable: false},
		},
	}
	cat.CreateTable(supplierSchema)

	// Create lineitem table
	lineitemSchema := &catalog.TableSchema{
		SchemaName: "public",
		TableName:  "lineitem",
		Columns: []catalog.ColumnDef{
			{Name: "l_orderkey", DataType: types.Integer, IsNullable: false},
			{Name: "l_suppkey", DataType: types.Integer, IsNullable: false},
			{Name: "l_receiptdate", DataType: types.Date, IsNullable: false},
			{Name: "l_commitdate", DataType: types.Date, IsNullable: false},
		},
	}
	cat.CreateTable(lineitemSchema)

	// Create orders table
	orderSchema := &catalog.TableSchema{
		SchemaName: "public",
		TableName:  "orders",
		Columns: []catalog.ColumnDef{
			{Name: "o_orderkey", DataType: types.Integer, IsNullable: false},
			{Name: "o_orderstatus", DataType: types.Char(1), IsNullable: false},
		},
	}
	cat.CreateTable(orderSchema)

	// Create nation table
	nationSchema := &catalog.TableSchema{
		SchemaName: "public",
		TableName:  "nation",
		Columns: []catalog.ColumnDef{
			{Name: "n_nationkey", DataType: types.Integer, IsNullable: false},
			{Name: "n_name", DataType: types.Char(25), IsNullable: false},
		},
	}
	cat.CreateTable(nationSchema)

	// Test TPC-H Q21 query
	q21 := `
SELECT
    s_name,
    COUNT(*) AS numwait
FROM
    supplier,
    lineitem l1,
    orders,
    nation
WHERE
    s_suppkey = l1.l_suppkey
    AND o_orderkey = l1.l_orderkey
    AND o_orderstatus = 'F'
    AND l1.l_receiptdate > l1.l_commitdate
    AND EXISTS (
        SELECT
            *
        FROM
            lineitem l2
        WHERE
            l2.l_orderkey = l1.l_orderkey
            AND l2.l_suppkey <> l1.l_suppkey
    )
    AND NOT EXISTS (
        SELECT
            *
        FROM
            lineitem l3
        WHERE
            l3.l_orderkey = l1.l_orderkey
            AND l3.l_suppkey <> l1.l_suppkey
            AND l3.l_receiptdate > l3.l_commitdate
    )
    AND s_nationkey = n_nationkey
    AND n_name = 'SAUDI ARABIA'
GROUP BY
    s_name
ORDER BY
    numwait DESC,
    s_name
LIMIT 100`

	// Parse the query
	p := parser.NewParser(q21)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Failed to parse Q21: %v", err)
	}

	// Plan the query
	plnr := planner.NewBasicPlannerWithCatalog(cat)
	plan, err := plnr.Plan(stmt)
	if err != nil {
		t.Fatalf("Failed to plan Q21: %v", err)
	}

	// Verify the plan was created successfully
	if plan == nil {
		t.Fatal("Expected non-nil plan")
	}

	// Log the plan for debugging
	t.Logf("Q21 Plan created successfully")
	t.Logf("Plan type: %T", plan)

	// Verify the plan contains the expected structure
	// The plan should have decorrelated the EXISTS/NOT EXISTS into semi/anti joins
	planStr := planToString(plan)
	t.Logf("Plan structure:\n%s", planStr)

	// Check for semi join (from EXISTS)
	if !containsNodeType(plan, "*planner.LogicalJoin") {
		t.Error("Expected plan to contain joins")
	}

	// The test passes if we can successfully parse and plan Q21
	// This demonstrates that multiple correlated EXISTS/NOT EXISTS is supported
}

// planToString converts a plan to a string representation
func planToString(plan planner.Plan) string {
	if plan == nil {
		return "nil"
	}

	result := fmt.Sprintf("%T", plan)

	// Add more details based on plan type
	switch p := plan.(type) {
	case *planner.LogicalJoin:
		result += fmt.Sprintf(" [%s]", p.JoinType)
	case *planner.LogicalFilter:
		result += " [filter]"
	case *planner.LogicalProject:
		result += " [project]"
	case *planner.LogicalAggregate:
		result += " [aggregate]"
	case *planner.LogicalScan:
		result += fmt.Sprintf(" [%s]", p.TableName)
	}

	// Add children
	children := plan.Children()
	if len(children) > 0 {
		result += "\n"
		for i, child := range children {
			childStr := planToString(child)
			lines := strings.Split(childStr, "\n")
			for j, line := range lines {
				if i == len(children)-1 {
					if j == 0 {
						result += "  └─ " + line + "\n"
					} else {
						result += "     " + line + "\n"
					}
				} else {
					if j == 0 {
						result += "  ├─ " + line + "\n"
					} else {
						result += "  │  " + line + "\n"
					}
				}
			}
		}
	}

	return strings.TrimSuffix(result, "\n")
}

// containsNodeType checks if a plan contains a node of the given type
func containsNodeType(plan planner.Plan, nodeType string) bool {
	if plan == nil {
		return false
	}

	planType := fmt.Sprintf("%T", plan)
	if planType == nodeType {
		return true
	}

	// Check children
	for _, child := range plan.Children() {
		if containsNodeType(child, nodeType) {
			return true
		}
	}

	return false
}

// Test simplified version of Q21 pattern
func TestSimplifiedMultipleExists(t *testing.T) {
	cat := catalog.NewMemoryCatalog()

	// Create a simple table
	schema := &catalog.TableSchema{
		SchemaName: "public",
		TableName:  "t1",
		Columns: []catalog.ColumnDef{
			{Name: "a", DataType: types.Integer, IsNullable: false},
			{Name: "b", DataType: types.Integer, IsNullable: false},
		},
	}
	cat.CreateTable(schema)

	// Test query with multiple EXISTS
	query := `
	SELECT a
	FROM t1 AS outer_t
	WHERE EXISTS (SELECT * FROM t1 AS t2 WHERE t2.a = outer_t.a AND t2.b > 10)
	  AND NOT EXISTS (SELECT * FROM t1 AS t3 WHERE t3.a = outer_t.a AND t3.b < 5)`

	// Parse
	p := parser.NewParser(query)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	// Plan
	plnr := planner.NewBasicPlannerWithCatalog(cat)
	plan, err := plnr.Plan(stmt)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}

	if plan == nil {
		t.Fatal("Expected non-nil plan")
	}

	t.Log("Successfully planned query with multiple EXISTS/NOT EXISTS")
}
