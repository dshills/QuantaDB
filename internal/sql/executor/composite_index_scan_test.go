package executor

import (
	"context"
	"testing"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/index"
	"github.com/dshills/QuantaDB/internal/sql/types"
	"github.com/dshills/QuantaDB/internal/storage"
	"github.com/dshills/QuantaDB/internal/txn"
)

func TestCompositeIndexScan(t *testing.T) {
	// Create a test environment
	diskManager, err := storage.NewDiskManager(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatalf("failed to create disk manager: %v", err)
	}
	bufferPool := storage.NewBufferPool(diskManager, 100)

	// Create catalog
	cat := catalog.NewMemoryCatalog()

	// Create transaction manager
	txnMgr := txn.NewTransactionManager()

	// Create MVCC storage backend
	mvccStorage := NewMVCCStorageBackend(bufferPool, cat, nil, txnMgr)

	// Create index manager
	indexMgr := index.NewManager(cat)

	// Create table with multiple columns
	tableSchema := &catalog.TableSchema{
		SchemaName: "public",
		TableName:  "orders",
		Columns: []catalog.ColumnDef{
			{Name: "order_id", DataType: types.Integer},
			{Name: "customer_id", DataType: types.Integer},
			{Name: "order_date", DataType: types.Text},
			{Name: "status", DataType: types.Text},
			{Name: "total", DataType: types.Integer},
		},
	}

	table, err := cat.CreateTable(tableSchema)
	if err != nil {
		t.Fatal(err)
	}

	// Create table in storage
	err = mvccStorage.CreateTable(table)
	if err != nil {
		t.Fatal(err)
	}

	// Create composite index on (customer_id, order_date)
	indexSchema := &catalog.IndexSchema{
		SchemaName: "public",
		TableName:  "orders",
		IndexName:  "idx_customer_date",
		Type:       catalog.BTreeIndex,
		IsUnique:   false,
		Columns: []catalog.IndexColumnDef{
			{ColumnName: "customer_id", SortOrder: catalog.Ascending},
			{ColumnName: "order_date", SortOrder: catalog.Ascending},
		},
	}

	indexMeta, err := cat.CreateIndex(indexSchema)
	if err != nil {
		t.Fatal(err)
	}

	// Create the actual index in the index manager
	err = indexMgr.CreateIndex("public", "orders", "idx_customer_date", []string{"customer_id", "order_date"}, false)
	if err != nil {
		t.Fatal(err)
	}

	// Start a transaction
	tx, err := txnMgr.BeginTransaction(context.Background(), txn.ReadCommitted)
	if err != nil {
		t.Fatal(err)
	}

	// Insert test data
	testData := []struct {
		orderID    int32
		customerID int32
		orderDate  string
		status     string
		total      int32
	}{
		{1, 100, "2024-01-01", "pending", 1000},
		{2, 100, "2024-01-02", "completed", 1500},
		{3, 100, "2024-01-03", "pending", 2000},
		{4, 101, "2024-01-01", "completed", 500},
		{5, 101, "2024-01-02", "pending", 750},
		{6, 102, "2024-01-01", "pending", 3000},
	}

	mvccStorage.SetCurrentTransaction(tx.ID(), int64(tx.ReadTimestamp()))

	for _, data := range testData {
		row := &Row{
			Values: []types.Value{
				types.NewValue(data.orderID),
				types.NewValue(data.customerID),
				types.NewValue(data.orderDate),
				types.NewValue(data.status),
				types.NewValue(data.total),
			},
		}

		// Build row map for index insertion
		rowMap := map[string]types.Value{
			"order_id":    row.Values[0],
			"customer_id": row.Values[1],
			"order_date":  row.Values[2],
			"status":      row.Values[3],
			"total":       row.Values[4],
		}

		rowID, err := mvccStorage.InsertRow(table.ID, row)
		if err != nil {
			t.Fatal(err)
		}

		// Insert into indexes
		// Encode RowID as PageID (4 bytes) + SlotID (2 bytes)
		rowIDBytes := make([]byte, 6)
		rowIDBytes[0] = byte(rowID.PageID)
		rowIDBytes[1] = byte(rowID.PageID >> 8)
		rowIDBytes[2] = byte(rowID.PageID >> 16)
		rowIDBytes[3] = byte(rowID.PageID >> 24)
		rowIDBytes[4] = byte(rowID.SlotID)
		rowIDBytes[5] = byte(rowID.SlotID >> 8)

		err = indexMgr.InsertIntoIndexes("public", "orders", rowMap, rowIDBytes)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Commit transaction
	err = txnMgr.CommitTransaction(tx)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("CompositeIndexRangeScan", func(t *testing.T) {
		// Start new transaction for reading
		readTx, err := txnMgr.BeginTransaction(context.Background(), txn.ReadCommitted)
		if err != nil {
			t.Fatal(err)
		}
		defer readTx.Rollback()

		readCtx := &ExecContext{
			Catalog:    cat,
			SnapshotTS: int64(readTx.ReadTimestamp()),
			Stats:      &ExecStats{},
		}

		// Create composite index scan for customer_id = 100 and order_date between 2024-01-01 and 2024-01-02
		startValues := []types.Value{
			types.NewValue(int32(100)),
			types.NewValue("2024-01-01"),
		}
		endValues := []types.Value{
			types.NewValue(int32(100)),
			types.NewValue("2024-01-02"),
		}

		scanOp := NewCompositeIndexScanOperator(table, indexMeta, indexMgr, mvccStorage, startValues, endValues)

		err = scanOp.Open(readCtx)
		if err != nil {
			t.Fatal(err)
		}

		// Collect results
		var results []int32
		for {
			row, err := scanOp.Next()
			if err != nil {
				t.Fatal(err)
			}
			if row == nil {
				break
			}

			orderID := row.Values[0].Data.(int32)
			results = append(results, orderID)
		}

		err = scanOp.Close()
		if err != nil {
			t.Fatal(err)
		}

		// Should get orders 1 and 2 (customer 100, dates 2024-01-01 and 2024-01-02)
		if len(results) != 2 {
			t.Errorf("expected 2 results, got %d", len(results))
		}
		expectedSet := map[int32]bool{1: true, 2: true}
		for _, r := range results {
			if !expectedSet[r] {
				t.Errorf("unexpected result: %d", r)
			}
		}
	})

	t.Run("CompositeIndexPrefixScan", func(t *testing.T) {
		// Start new transaction for reading
		readTx, err := txnMgr.BeginTransaction(context.Background(), txn.ReadCommitted)
		if err != nil {
			t.Fatal(err)
		}
		defer readTx.Rollback()

		readCtx := &ExecContext{
			Catalog:    cat,
			SnapshotTS: int64(readTx.ReadTimestamp()),
			Stats:      &ExecStats{},
		}

		// Create composite index scan for customer_id = 101 (prefix scan)
		startValues := []types.Value{
			types.NewValue(int32(101)),
		}
		endValues := []types.Value{
			types.NewValue(int32(101)),
			types.NewValue("\xFF"), // High value to include all dates for customer 101
		}

		scanOp := NewCompositeIndexScanOperator(table, indexMeta, indexMgr, mvccStorage, startValues, endValues)

		err = scanOp.Open(readCtx)
		if err != nil {
			t.Fatal(err)
		}

		// Collect results
		var results []int32
		for {
			row, err := scanOp.Next()
			if err != nil {
				t.Fatal(err)
			}
			if row == nil {
				break
			}

			orderID := row.Values[0].Data.(int32)
			results = append(results, orderID)
		}

		err = scanOp.Close()
		if err != nil {
			t.Fatal(err)
		}

		// Should get all orders for customer 101 (orders 4 and 5)
		if len(results) != 2 {
			t.Errorf("expected 2 results, got %d", len(results))
		}
		expectedSet := map[int32]bool{4: true, 5: true}
		for _, r := range results {
			if !expectedSet[r] {
				t.Errorf("unexpected result: %d", r)
			}
		}
	})
}

func TestCompositeIndexScanWithNulls(t *testing.T) {
	// Create a test environment
	diskManager, err := storage.NewDiskManager(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatalf("failed to create disk manager: %v", err)
	}
	bufferPool := storage.NewBufferPool(diskManager, 100)

	// Create catalog
	cat := catalog.NewMemoryCatalog()

	// Create transaction manager
	txnMgr := txn.NewTransactionManager()

	// Create MVCC storage backend
	mvccStorage := NewMVCCStorageBackend(bufferPool, cat, nil, txnMgr)

	// Create index manager
	indexMgr := index.NewManager(cat)

	// Create table with nullable columns
	tableSchema := &catalog.TableSchema{
		SchemaName: "public",
		TableName:  "products",
		Columns: []catalog.ColumnDef{
			{Name: "product_id", DataType: types.Integer},
			{Name: "category_id", DataType: types.Integer, IsNullable: true},
			{Name: "name", DataType: types.Text},
			{Name: "price", DataType: types.Integer, IsNullable: true},
		},
	}

	table, err := cat.CreateTable(tableSchema)
	if err != nil {
		t.Fatal(err)
	}

	// Create table in storage
	err = mvccStorage.CreateTable(table)
	if err != nil {
		t.Fatal(err)
	}

	// Create composite index on nullable columns (category_id, price)
	indexSchema := &catalog.IndexSchema{
		SchemaName: "public",
		TableName:  "products",
		IndexName:  "idx_category_price",
		Type:       catalog.BTreeIndex,
		IsUnique:   false,
		Columns: []catalog.IndexColumnDef{
			{ColumnName: "category_id", SortOrder: catalog.Ascending},
			{ColumnName: "price", SortOrder: catalog.Ascending},
		},
	}

	indexMeta, err := cat.CreateIndex(indexSchema)
	if err != nil {
		t.Fatal(err)
	}

	// Create the actual index in the index manager
	err = indexMgr.CreateIndex("public", "products", "idx_category_price", []string{"category_id", "price"}, false)
	if err != nil {
		t.Fatal(err)
	}

	// Start a transaction
	tx, err := txnMgr.BeginTransaction(context.Background(), txn.ReadCommitted)
	if err != nil {
		t.Fatal(err)
	}

	mvccStorage.SetCurrentTransaction(tx.ID(), int64(tx.ReadTimestamp()))

	// Insert test data with NULLs
	testData := []struct {
		productID  int32
		categoryID *int32
		name       string
		price      *int32
	}{
		{1, ptr(int32(1)), "Product A", ptr(int32(100))},
		{2, ptr(int32(1)), "Product B", ptr(int32(200))},
		{3, ptr(int32(1)), "Product C", nil},   // NULL price
		{4, nil, "Product D", ptr(int32(300))}, // NULL category
		{5, ptr(int32(2)), "Product E", ptr(int32(150))},
	}

	for _, data := range testData {
		values := []types.Value{
			types.NewValue(data.productID),
			types.NewNullValue(),
			types.NewValue(data.name),
			types.NewNullValue(),
		}

		if data.categoryID != nil {
			values[1] = types.NewValue(*data.categoryID)
		}
		if data.price != nil {
			values[3] = types.NewValue(*data.price)
		}

		row := &Row{Values: values}

		// Build row map for index insertion
		rowMap := map[string]types.Value{
			"product_id":  values[0],
			"category_id": values[1],
			"name":        values[2],
			"price":       values[3],
		}

		rowID, err := mvccStorage.InsertRow(table.ID, row)
		if err != nil {
			t.Fatal(err)
		}

		// Insert into indexes
		// Encode RowID as PageID (4 bytes) + SlotID (2 bytes)
		rowIDBytes := make([]byte, 6)
		rowIDBytes[0] = byte(rowID.PageID)
		rowIDBytes[1] = byte(rowID.PageID >> 8)
		rowIDBytes[2] = byte(rowID.PageID >> 16)
		rowIDBytes[3] = byte(rowID.PageID >> 24)
		rowIDBytes[4] = byte(rowID.SlotID)
		rowIDBytes[5] = byte(rowID.SlotID >> 8)

		err = indexMgr.InsertIntoIndexes("public", "products", rowMap, rowIDBytes)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Commit transaction
	err = txnMgr.CommitTransaction(tx)
	if err != nil {
		t.Fatal(err)
	}

	// Test scanning with NULL handling
	t.Run("CompositeIndexScanWithNonNullValues", func(t *testing.T) {
		// Start new transaction for reading
		readTx, err := txnMgr.BeginTransaction(context.Background(), txn.ReadCommitted)
		if err != nil {
			t.Fatal(err)
		}
		defer readTx.Rollback()

		readCtx := &ExecContext{
			Catalog:    cat,
			SnapshotTS: int64(readTx.ReadTimestamp()),
			Stats:      &ExecStats{},
		}

		// Scan for category_id = 1 and price between 100 and 200
		startValues := []types.Value{
			types.NewValue(int32(1)),
			types.NewValue(int32(100)),
		}
		endValues := []types.Value{
			types.NewValue(int32(1)),
			types.NewValue(int32(200)),
		}

		scanOp := NewCompositeIndexScanOperator(table, indexMeta, indexMgr, mvccStorage, startValues, endValues)

		err = scanOp.Open(readCtx)
		if err != nil {
			t.Fatal(err)
		}

		// Collect results
		var results []int32
		for {
			row, err := scanOp.Next()
			if err != nil {
				t.Fatal(err)
			}
			if row == nil {
				break
			}

			productID := row.Values[0].Data.(int32)
			results = append(results, productID)
		}

		err = scanOp.Close()
		if err != nil {
			t.Fatal(err)
		}

		// Should get products 1 and 2 (category 1, prices 100 and 200)
		// Product 3 has NULL price so shouldn't be included
		if len(results) != 2 {
			t.Errorf("expected 2 results, got %d", len(results))
		}
		expectedSet := map[int32]bool{1: true, 2: true}
		for _, r := range results {
			if !expectedSet[r] {
				t.Errorf("unexpected result: %d", r)
			}
		}
	})
}

// Helper function to create pointers
func ptr[T any](v T) *T {
	return &v
}
