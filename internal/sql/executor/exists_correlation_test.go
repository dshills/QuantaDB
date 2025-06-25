package executor

import (
	"sort"
	"testing"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/sql/types"
	"github.com/stretchr/testify/require"
)

// TestExistsCorrelationExecution tests EXISTS and NOT EXISTS correlated subqueries
// TODO: Fix this test to work with the current storage backend implementation
func TestExistsCorrelationExecution(t *testing.T) {
	t.Skip("Skipping EXISTS correlation test - needs to be updated for current storage backend")
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

	usersTable, err := cat.CreateTable(usersSchema)
	require.NoError(t, err)

	ordersTable, err := cat.CreateTable(ordersSchema)
	require.NoError(t, err)

	// Create mock storage with test data
	storage := newMockStorageBackend()
	storage.CreateTable(usersTable)
	storage.CreateTable(ordersTable)
	
	// Add users: 1 and 2 have orders, 3 has no orders
	storage.InsertRow(usersTable.ID, &Row{Values: []types.Value{
		types.NewIntegerValue(1),
		types.NewTextValue("Alice"),
	}})
	storage.InsertRow(usersTable.ID, &Row{Values: []types.Value{
		types.NewIntegerValue(2),
		types.NewTextValue("Bob"),
	}})
	storage.InsertRow(usersTable.ID, &Row{Values: []types.Value{
		types.NewIntegerValue(3),
		types.NewTextValue("Charlie"),
	}})

	// Add orders for users 1 and 2
	storage.InsertRow(ordersTable.ID, &Row{Values: []types.Value{
		types.NewIntegerValue(1),
		types.NewIntegerValue(1), // user_id = 1
		types.NewIntegerValue(100),
	}})
	storage.InsertRow(ordersTable.ID, &Row{Values: []types.Value{
		types.NewIntegerValue(2),
		types.NewIntegerValue(1), // user_id = 1
		types.NewIntegerValue(200),
	}})
	storage.InsertRow(ordersTable.ID, &Row{Values: []types.Value{
		types.NewIntegerValue(3),
		types.NewIntegerValue(2), // user_id = 2
		types.NewIntegerValue(300),
	}})

	// Test EXISTS with correlation
	t.Run("EXISTS with correlation", func(t *testing.T) {
		query := `SELECT u.id, u.name, 
		          EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id) as has_orders
		          FROM users u
`

		// Parse the query
		p := parser.NewParser(query)
		stmt, err := p.Parse()
		require.NoError(t, err)

		// Plan the query with optimization
		plnr := planner.NewBasicPlannerWithCatalog(cat)
		plan, err := plnr.Plan(stmt)
		require.NoError(t, err)

		// Create executor
		exec := NewBasicExecutor(cat, nil)
		exec.SetStorageBackend(storage)

		// Execute the query
		ctx := &ExecContext{
			Catalog: cat,
			Stats:   &ExecStats{},
		}
		results, err := exec.Execute(plan, ctx)
		require.NoError(t, err)
		require.NotNil(t, results)

		// Collect all rows
		var rows []*Row
		for {
			row, err := results.Next()
			require.NoError(t, err)
			if row == nil {
				break
			}
			rows = append(rows, row)
		}
		results.Close()
		
		require.Len(t, rows, 3, "Should return 3 users")

		// Sort rows by ID to ensure consistent ordering
		sort.Slice(rows, func(i, j int) bool {
			return rows[i].Values[0].Data.(int32) < rows[j].Values[0].Data.(int32)
		})

		// Check results
		// User 1 (Alice) has orders
		require.Equal(t, int32(1), rows[0].Values[0].Data)
		require.Equal(t, "Alice", rows[0].Values[1].Data)
		require.Equal(t, true, rows[0].Values[2].Data, "Alice should have orders")

		// User 2 (Bob) has orders
		require.Equal(t, int32(2), rows[1].Values[0].Data)
		require.Equal(t, "Bob", rows[1].Values[1].Data)
		require.Equal(t, true, rows[1].Values[2].Data, "Bob should have orders")

		// User 3 (Charlie) has NO orders
		require.Equal(t, int32(3), rows[2].Values[0].Data)
		require.Equal(t, "Charlie", rows[2].Values[1].Data)
		require.Equal(t, false, rows[2].Values[2].Data, "Charlie should NOT have orders")
	})

	// Test NOT EXISTS with correlation
	t.Run("NOT EXISTS with correlation", func(t *testing.T) {
		query := `SELECT u.id, u.name, 
		          NOT EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id) as no_orders
		          FROM users u
`

		// Parse the query
		p := parser.NewParser(query)
		stmt, err := p.Parse()
		require.NoError(t, err)

		// Plan the query with optimization
		plnr := planner.NewBasicPlannerWithCatalog(cat)
		plan, err := plnr.Plan(stmt)
		require.NoError(t, err)

		// Create executor
		exec := NewBasicExecutor(cat, nil)
		exec.SetStorageBackend(storage)

		// Execute the query
		ctx := &ExecContext{
			Catalog: cat,
			Stats:   &ExecStats{},
		}
		results, err := exec.Execute(plan, ctx)
		require.NoError(t, err)
		require.NotNil(t, results)

		// Collect all rows
		var rows []*Row
		for {
			row, err := results.Next()
			require.NoError(t, err)
			if row == nil {
				break
			}
			rows = append(rows, row)
		}
		results.Close()
		
		require.Len(t, rows, 3, "Should return 3 users")

		// Sort rows by ID to ensure consistent ordering
		sort.Slice(rows, func(i, j int) bool {
			return rows[i].Values[0].Data.(int32) < rows[j].Values[0].Data.(int32)
		})

		// Check results
		// User 1 and 2 have orders, so no_orders should be false
		require.Equal(t, int32(1), rows[0].Values[0].Data)
		require.Equal(t, false, rows[0].Values[2].Data, "Alice has orders")
		
		require.Equal(t, int32(2), rows[1].Values[0].Data)
		require.Equal(t, false, rows[1].Values[2].Data, "Bob has orders")

		// User 3 has no orders, so no_orders should be true
		require.Equal(t, int32(3), rows[2].Values[0].Data)
		require.Equal(t, true, rows[2].Values[2].Data, "Charlie has no orders")
	})

	// Test EXISTS with additional predicates
	t.Run("EXISTS with additional predicates", func(t *testing.T) {
		query := `SELECT u.id, u.name, 
		          EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id AND o.amount > 150) as has_large_orders
		          FROM users u
`

		// Parse the query
		p := parser.NewParser(query)
		stmt, err := p.Parse()
		require.NoError(t, err)

		// Plan the query with optimization
		plnr := planner.NewBasicPlannerWithCatalog(cat)
		plan, err := plnr.Plan(stmt)
		require.NoError(t, err)

		// Create executor
		exec := NewBasicExecutor(cat, nil)
		exec.SetStorageBackend(storage)

		// Execute the query
		ctx := &ExecContext{
			Catalog: cat,
			Stats:   &ExecStats{},
		}
		results, err := exec.Execute(plan, ctx)
		require.NoError(t, err)
		require.NotNil(t, results)

		// Collect all rows
		var rows []*Row
		for {
			row, err := results.Next()
			require.NoError(t, err)
			if row == nil {
				break
			}
			rows = append(rows, row)
		}
		results.Close()
		
		require.Len(t, rows, 3, "Should return 3 users")

		// Sort rows by ID to ensure consistent ordering
		sort.Slice(rows, func(i, j int) bool {
			return rows[i].Values[0].Data.(int32) < rows[j].Values[0].Data.(int32)
		})

		// User 1 has order with amount=200 > 150
		require.Equal(t, int32(1), rows[0].Values[0].Data)
		require.Equal(t, true, rows[0].Values[2].Data, "Alice should have large orders")

		// User 2 has order with amount=300 > 150
		require.Equal(t, int32(2), rows[1].Values[0].Data)
		require.Equal(t, true, rows[1].Values[2].Data, "Bob should have large orders")

		// User 3 has no orders at all
		require.Equal(t, int32(3), rows[2].Values[0].Data)
		require.Equal(t, false, rows[2].Values[2].Data, "Charlie should not have large orders")
	})
}