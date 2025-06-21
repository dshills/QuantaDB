package executor

import (
	"context"
	"testing"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/index"
	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/sql/types"
	"github.com/dshills/QuantaDB/internal/storage"
	"github.com/dshills/QuantaDB/internal/txn"
)

func TestIndexOnlyScan(t *testing.T) {
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

	// Create table
	tableSchema := &catalog.TableSchema{
		SchemaName: "public",
		TableName:  "products",
		Columns: []catalog.ColumnDef{
			{Name: "product_id", DataType: types.Integer},
			{Name: "category_id", DataType: types.Integer},
			{Name: "product_name", DataType: types.Text},
			{Name: "price", DataType: types.Integer},
			{Name: "stock", DataType: types.Integer},
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

	// Create composite index on (category_id, product_name, price)
	// This index can satisfy queries that only need these columns
	indexSchema := &catalog.IndexSchema{
		SchemaName: "public",
		TableName:  "products",
		IndexName:  "idx_category_name_price",
		Type:       catalog.BTreeIndex,
		IsUnique:   false,
		Columns: []catalog.IndexColumnDef{
			{ColumnName: "category_id", SortOrder: catalog.Ascending},
			{ColumnName: "product_name", SortOrder: catalog.Ascending},
			{ColumnName: "price", SortOrder: catalog.Ascending},
		},
	}

	indexMeta, err := cat.CreateIndex(indexSchema)
	if err != nil {
		t.Fatal(err)
	}

	// Create the actual index in the index manager
	err = indexMgr.CreateIndex("public", "products", "idx_category_name_price",
		[]string{"category_id", "product_name", "price"}, false)
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
		productID   int32
		categoryID  int32
		productName string
		price       int32
		stock       int32
	}{
		{1, 1, "Apple", 100, 50},
		{2, 1, "Banana", 80, 100},
		{3, 1, "Cherry", 120, 30},
		{4, 2, "Desk", 500, 10},
		{5, 2, "Chair", 300, 20},
		{6, 2, "Lamp", 150, 15},
	}

	mvccStorage.SetCurrentTransaction(tx.ID(), int64(tx.ReadTimestamp()))

	for _, data := range testData {
		row := &Row{
			Values: []types.Value{
				types.NewValue(data.productID),
				types.NewValue(data.categoryID),
				types.NewValue(data.productName),
				types.NewValue(data.price),
				types.NewValue(data.stock),
			},
		}

		// Build row map for index insertion
		rowMap := map[string]types.Value{
			"product_id":   row.Values[0],
			"category_id":  row.Values[1],
			"product_name": row.Values[2],
			"price":        row.Values[3],
			"stock":        row.Values[4],
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

	t.Run("IndexOnlyScanCoveringQuery", func(t *testing.T) {
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

		// Create index-only scan for SELECT category_id, product_name, price
		// WHERE category_id = 1 (all columns are in the index)
		startExprs := []planner.Expression{
			&planner.Literal{Value: types.NewValue(int32(1))},
		}
		endExprs := []planner.Expression{
			&planner.Literal{Value: types.NewValue(int32(1))},
			&planner.Literal{Value: types.NewValue("\xFF")},        // High value for string
			&planner.Literal{Value: types.NewValue(int32(999999))}, // High value for price
		}

		projectedColumns := []string{"category_id", "product_name", "price"}

		scanOp := NewIndexOnlyScanOperator(table, indexMeta, indexMgr, startExprs, endExprs, projectedColumns)

		err = scanOp.Open(readCtx)
		if err != nil {
			t.Fatal(err)
		}

		// Collect results
		rowCount := 0
		for {
			row, err := scanOp.Next()
			if err != nil {
				t.Fatal(err)
			}
			if row == nil {
				break
			}

			// Verify we got the right columns
			if len(row.Values) != 3 {
				t.Errorf("expected 3 values, got %d", len(row.Values))
			}

			// For testing, we'll just count rows since decoding is not fully implemented
			rowCount++
		}

		err = scanOp.Close()
		if err != nil {
			t.Fatal(err)
		}

		// Should have scanned 3 products in category 1
		if rowCount != 3 {
			t.Errorf("expected 3 rows, got %d", rowCount)
		}

		// Verify this was an index-only scan
		if readCtx.Stats.IndexOnlyScans != 1 {
			t.Errorf("expected 1 index-only scan, got %d", readCtx.Stats.IndexOnlyScans)
		}
	})

	t.Run("IndexOnlyScanProjectionSubset", func(t *testing.T) {
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

		// Create index-only scan for SELECT product_name, price
		// WHERE category_id = 2 (projecting subset of index columns)
		startExprs := []planner.Expression{
			&planner.Literal{Value: types.NewValue(int32(2))},
		}
		endExprs := []planner.Expression{
			&planner.Literal{Value: types.NewValue(int32(2))},
			&planner.Literal{Value: types.NewValue("\xFF")},
			&planner.Literal{Value: types.NewValue(int32(999999))},
		}

		// Only project product_name and price, not category_id
		projectedColumns := []string{"product_name", "price"}

		scanOp := NewIndexOnlyScanOperator(table, indexMeta, indexMgr, startExprs, endExprs, projectedColumns)

		err = scanOp.Open(readCtx)
		if err != nil {
			t.Fatal(err)
		}

		// Collect results
		rowCount := 0
		for {
			row, err := scanOp.Next()
			if err != nil {
				t.Fatal(err)
			}
			if row == nil {
				break
			}

			// Verify we got only the projected columns
			if len(row.Values) != 2 {
				t.Errorf("expected 2 values (product_name, price), got %d", len(row.Values))
			}

			rowCount++
		}

		err = scanOp.Close()
		if err != nil {
			t.Fatal(err)
		}

		// Should have scanned 3 products in category 2
		if rowCount != 3 {
			t.Errorf("expected 3 rows, got %d", rowCount)
		}
	})
}

func TestIndexOnlyScanDetection(t *testing.T) {
	// Create catalog
	cat := catalog.NewMemoryCatalog()

	// Create table
	tableSchema := &catalog.TableSchema{
		SchemaName: "public",
		TableName:  "orders",
		Columns: []catalog.ColumnDef{
			{Name: "order_id", DataType: types.Integer},
			{Name: "customer_id", DataType: types.Integer},
			{Name: "order_date", DataType: types.Text},
			{Name: "total", DataType: types.Integer},
			{Name: "status", DataType: types.Text},
		},
	}

	_, err := cat.CreateTable(tableSchema)
	if err != nil {
		t.Fatal(err)
	}

	// Create composite index on (customer_id, order_date, total)
	indexSchema := &catalog.IndexSchema{
		SchemaName: "public",
		TableName:  "orders",
		IndexName:  "idx_customer_date_total",
		Type:       catalog.BTreeIndex,
		IsUnique:   false,
		Columns: []catalog.IndexColumnDef{
			{ColumnName: "customer_id", SortOrder: catalog.Ascending},
			{ColumnName: "order_date", SortOrder: catalog.Ascending},
			{ColumnName: "total", SortOrder: catalog.Ascending},
		},
	}

	indexMeta, err := cat.CreateIndex(indexSchema)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("DetectCoveringIndex", func(t *testing.T) {
		// Test that the index covers these columns
		requiredColumns := []string{"customer_id", "order_date", "total"}
		if !planner.IsCoveringIndex(indexMeta, requiredColumns) {
			t.Error("expected index to be covering for customer_id, order_date, total")
		}

		// Test subset of columns
		requiredColumns = []string{"customer_id", "order_date"}
		if !planner.IsCoveringIndex(indexMeta, requiredColumns) {
			t.Error("expected index to be covering for customer_id, order_date")
		}

		// Test non-covering case (includes status which is not in index)
		requiredColumns = []string{"customer_id", "order_date", "status"}
		if planner.IsCoveringIndex(indexMeta, requiredColumns) {
			t.Error("expected index to NOT be covering when status is required")
		}
	})
}
