package executor

import (
	"testing"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/sql/types"
	"github.com/stretchr/testify/require"
)

func TestExistsExpressionPlanning(t *testing.T) {
	// Setup test environment
	cat := catalog.NewMemoryCatalog()

	// Create test tables
	usersSchema := &catalog.TableSchema{
		SchemaName: "public",
		TableName:  "users",
		Columns: []catalog.ColumnDef{
			{Name: "id", DataType: types.Integer, IsNullable: false},
			{Name: "name", DataType: types.Text, IsNullable: false},
		},
	}

	ordersSchema := &catalog.TableSchema{
		SchemaName: "public",
		TableName:  "orders",
		Columns: []catalog.ColumnDef{
			{Name: "id", DataType: types.Integer, IsNullable: false},
			{Name: "user_id", DataType: types.Integer, IsNullable: false},
			{Name: "amount", DataType: types.Integer, IsNullable: false},
		},
	}

	_, err := cat.CreateTable(usersSchema)
	require.NoError(t, err)

	_, err = cat.CreateTable(ordersSchema)
	require.NoError(t, err)

	// Test EXISTS expression planning
	testCases := []struct {
		name    string
		query   string
		wantErr bool
	}{
		{
			name:    "Simple EXISTS",
			query:   "SELECT id, name FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE user_id = 123)",
			wantErr: false,
		},
		{
			name:    "NOT EXISTS",
			query:   "SELECT id, name FROM users WHERE NOT EXISTS (SELECT 1 FROM orders WHERE user_id = 123)",
			wantErr: false,
		},
		{
			name:    "EXISTS with complex subquery",
			query:   "SELECT id, name FROM users WHERE EXISTS (SELECT * FROM orders WHERE user_id = 123 AND amount > 100)",
			wantErr: false,
		},
		{
			name:    "EXISTS in compound condition",
			query:   "SELECT id, name FROM users WHERE id > 5 AND EXISTS (SELECT 1 FROM orders WHERE user_id = 123)",
			wantErr: false,
		},
		{
			name:    "Multiple EXISTS",
			query:   "SELECT id, name FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE user_id = 123) AND NOT EXISTS (SELECT 1 FROM orders WHERE amount > 1000)",
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