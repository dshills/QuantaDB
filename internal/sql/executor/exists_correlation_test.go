package executor

import (
	"testing"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/engine"
	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/sql/types"
	"github.com/dshills/QuantaDB/internal/storage"
	"github.com/dshills/QuantaDB/internal/testutil"
	"github.com/dshills/QuantaDB/internal/txn"
	"github.com/stretchr/testify/require"
)

func TestExistsCorrelationExecution(t *testing.T) {
	// Setup test environment
	cat := catalog.NewMemoryCatalog()
	eng := engine.NewMemoryEngine()
	defer eng.Close()

	// Create transaction manager and storage backend
	txnManager := txn.NewManager(eng, nil)
	diskManager, err := storage.NewDiskManager(":memory:")
	testutil.AssertNoError(t, err)
	defer diskManager.Close()

	bufferPool := storage.NewBufferPool(diskManager, 10)
	storageBackend := NewMVCCStorageBackend(bufferPool, cat, nil, txnManager)

	// Create executor
	exec := NewBasicExecutor(cat, eng)
	exec.SetStorageBackend(storageBackend)

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
	testutil.AssertNoError(t, err)
	err = storageBackend.CreateTable(usersTable)
	testutil.AssertNoError(t, err)

	ordersTable, err := cat.CreateTable(ordersSchema)
	testutil.AssertNoError(t, err)
	err = storageBackend.CreateTable(ordersTable)
	testutil.AssertNoError(t, err)

	// Insert test data - users: 1 and 2 have orders, 3 has no orders
	usersData := [][]types.Value{
		{types.NewIntegerValue(1), types.NewTextValue("Alice")},
		{types.NewIntegerValue(2), types.NewTextValue("Bob")},
		{types.NewIntegerValue(3), types.NewTextValue("Charlie")},
	}
	
	for _, values := range usersData {
		row := &Row{Values: values}
		_, err := storageBackend.InsertRow(usersTable.ID, row)
		testutil.AssertNoError(t, err)
	}

	// Insert orders for users 1 and 2  
	ordersData := [][]types.Value{
		{types.NewIntegerValue(1), types.NewIntegerValue(1), types.NewIntegerValue(100)},
		{types.NewIntegerValue(2), types.NewIntegerValue(1), types.NewIntegerValue(200)},
		{types.NewIntegerValue(3), types.NewIntegerValue(2), types.NewIntegerValue(300)},
	}
	
	for _, values := range ordersData {
		row := &Row{Values: values}
		_, err := storageBackend.InsertRow(ordersTable.ID, row)
		testutil.AssertNoError(t, err)
	}

	// Test EXISTS with correlation
	t.Run("EXISTS with correlation", func(t *testing.T) {
		query := `SELECT u.id, u.name, 
		          EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id) as has_orders
		          FROM users u
		          ORDER BY u.id`

		// Parse the query
		p := parser.NewParser(query)
		stmt, err := p.Parse()
		require.NoError(t, err)

		// Plan the query with optimization
		plnr := planner.NewBasicPlannerWithCatalog(cat)
		plan, err := plnr.Plan(stmt)
		require.NoError(t, err)

		// Execute the query
		ctx := &ExecContext{
			Catalog: cat,
			Engine:  eng,
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

		// Check results
		// User 1 (Alice) has orders
		require.Equal(t, int64(1), rows[0].Values[0].Data)
		require.Equal(t, "Alice", rows[0].Values[1].Data)
		require.Equal(t, true, rows[0].Values[2].Data, "Alice should have orders")

		// User 2 (Bob) has orders
		require.Equal(t, int64(2), rows[1].Values[0].Data)
		require.Equal(t, "Bob", rows[1].Values[1].Data)
		require.Equal(t, true, rows[1].Values[2].Data, "Bob should have orders")

		// User 3 (Charlie) has NO orders
		require.Equal(t, int64(3), rows[2].Values[0].Data)
		require.Equal(t, "Charlie", rows[2].Values[1].Data)
		require.Equal(t, false, rows[2].Values[2].Data, "Charlie should NOT have orders")
	})

	// Test NOT EXISTS with correlation
	t.Run("NOT EXISTS with correlation", func(t *testing.T) {
		query := `SELECT u.id, u.name, 
		          NOT EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id) as no_orders
		          FROM users u
		          ORDER BY u.id`

		// Parse the query
		p := parser.NewParser(query)
		stmt, err := p.Parse()
		require.NoError(t, err)

		// Plan the query with optimization
		plnr := planner.NewBasicPlannerWithCatalog(cat)
		plan, err := plnr.Plan(stmt)
		require.NoError(t, err)

		// Execute the query
		ctx := &ExecContext{
			Catalog: cat,
			Engine:  eng,
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

		// Check results
		// User 1 and 2 have orders, so no_orders should be false
		require.Equal(t, false, rows[0].Values[2].Data, "Alice has orders")
		require.Equal(t, false, rows[1].Values[2].Data, "Bob has orders")

		// User 3 has no orders, so no_orders should be true
		require.Equal(t, true, rows[2].Values[2].Data, "Charlie has no orders")
	})

	// Test EXISTS with additional predicates
	t.Run("EXISTS with additional predicates", func(t *testing.T) {
		query := `SELECT u.id, u.name, 
		          EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id AND o.amount > 150) as has_large_orders
		          FROM users u
		          ORDER BY u.id`

		// Parse the query
		p := parser.NewParser(query)
		stmt, err := p.Parse()
		require.NoError(t, err)

		// Plan the query with optimization
		plnr := planner.NewBasicPlannerWithCatalog(cat)
		plan, err := plnr.Plan(stmt)
		require.NoError(t, err)

		// Execute the query
		ctx := &ExecContext{
			Catalog: cat,
			Engine:  eng,
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

		// User 1 has order with amount=200 > 150
		require.Equal(t, true, rows[0].Values[2].Data, "Alice should have large orders")

		// User 2 has order with amount=300 > 150
		require.Equal(t, true, rows[1].Values[2].Data, "Bob should have large orders")

		// User 3 has no orders at all
		require.Equal(t, false, rows[2].Values[2].Data, "Charlie should not have large orders")
	})
}