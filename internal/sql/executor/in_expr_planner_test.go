package executor

import (
	"testing"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/sql/types"
	"github.com/stretchr/testify/require"
)

func TestInExpressionPlanning(t *testing.T) {
	// Setup test environment
	cat := catalog.NewMemoryCatalog()

	// Create a test table
	tableSchema := &catalog.TableSchema{
		SchemaName: "public",
		TableName:  "test_users",
		Columns: []catalog.ColumnDef{
			{Name: "id", DataType: types.Integer, IsNullable: false},
			{Name: "name", DataType: types.Text, IsNullable: false},
			{Name: "category", DataType: types.Text, IsNullable: true},
		},
	}

	_, err := cat.CreateTable(tableSchema)
	require.NoError(t, err)

	// Test IN expression planning
	testCases := []struct {
		name    string
		query   string
		wantErr bool
	}{
		{
			name:    "IN with integer list",
			query:   "SELECT id, name FROM test_users WHERE id IN (1, 3, 5)",
			wantErr: false,
		},
		{
			name:    "NOT IN with integer list",
			query:   "SELECT id, name FROM test_users WHERE id NOT IN (2, 4)",
			wantErr: false,
		},
		{
			name:    "IN with string list",
			query:   "SELECT id, name FROM test_users WHERE category IN ('A', 'C')",
			wantErr: false,
		},
		{
			name:    "NOT IN with string list",
			query:   "SELECT id, name FROM test_users WHERE category NOT IN ('A')",
			wantErr: false,
		},
		{
			name:    "IN with single value",
			query:   "SELECT id, name FROM test_users WHERE category IN ('B')",
			wantErr: false,
		},
		{
			name:    "IN with empty list",
			query:   "SELECT id, name FROM test_users WHERE id IN ()",
			wantErr: false,
		},
		{
			name:    "IN with subquery",
			query:   "SELECT id, name FROM test_users WHERE id IN (SELECT 1)",
			wantErr: false,
		},
		{
			name:    "NOT IN with subquery",
			query:   "SELECT id, name FROM test_users WHERE id NOT IN (SELECT 2)",
			wantErr: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Parse the query
			p := parser.NewParser(tc.query)
			stmt, err := p.Parse()
			require.NoError(t, err, "Failed to parse query: %s", tc.query)

			// Plan the query
			plnr := planner.NewBasicPlannerWithCatalog(cat)
			plan, err := plnr.Plan(stmt)

			if tc.wantErr {
				require.Error(t, err, "Expected error for query: %s", tc.query)
			} else {
				require.NoError(t, err, "Failed to plan query: %s", tc.query)
				require.NotNil(t, plan, "Plan should not be nil for query: %s", tc.query)
			}
		})
	}
}
