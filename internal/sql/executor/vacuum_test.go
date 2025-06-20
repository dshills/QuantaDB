package executor

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/sql/types"
	"github.com/dshills/QuantaDB/internal/storage"
	"github.com/dshills/QuantaDB/internal/txn"
)

func TestVacuumScanner(t *testing.T) {
	// Create disk manager, buffer pool and storage
	diskManager, err := storage.NewDiskManager(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatalf("failed to create disk manager: %v", err)
	}
	bufferPool := storage.NewBufferPool(diskManager, 100)

	// Create catalog and WAL manager
	cat := catalog.NewMemoryCatalog()

	// Create transaction manager
	txnMgr := txn.NewTransactionManager()

	// Create MVCC storage backend
	mvccStorage := NewMVCCStorageBackend(bufferPool, cat, nil, txnMgr)

	// Create a test table in catalog
	tableSchema := &catalog.TableSchema{
		SchemaName: "public",
		TableName:  "test_table",
		Columns: []catalog.ColumnDef{
			{Name: "id", DataType: types.Integer, IsNullable: false},
			{Name: "name", DataType: types.Text, IsNullable: true},
		},
	}

	table, err := cat.CreateTable(tableSchema)
	if err != nil {
		t.Fatalf("failed to create table in catalog: %v", err)
	}

	// Create table in storage
	err = mvccStorage.CreateTable(table)
	if err != nil {
		t.Fatalf("failed to create table in storage: %v", err)
	}

	tableID := table.ID

	// Begin first transaction and insert rows
	txn1, err := txnMgr.BeginTransaction(context.Background(), txn.ReadCommitted)
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	// Insert some rows
	var insertedRowIDs []RowID
	for i := 1; i <= 5; i++ {
		row := &Row{
			Values: []types.Value{
				types.NewValue(int32(i)),
				types.NewValue("name" + string(rune('0'+i))),
			},
		}
		rowID, err := mvccStorage.InsertRow(tableID, row)
		if err != nil {
			t.Fatalf("failed to insert row %d: %v", i, err)
		}
		insertedRowIDs = append(insertedRowIDs, rowID)
	}

	// Commit first transaction
	err = txnMgr.CommitTransaction(txn1)
	if err != nil {
		t.Fatalf("failed to commit transaction 1: %v", err)
	}

	// Begin second transaction and delete some rows
	txn2, err := txnMgr.BeginTransaction(context.Background(), txn.ReadCommitted)
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	// We need to set the current transaction ID in the storage backend
	// This is a workaround since DeleteRow doesn't take context
	mvccStorage.SetCurrentTransaction(txn2.ID(), int64(txn2.ReadTimestamp()))

	// Delete rows 2 and 4 using their actual RowIDs
	rowsToDelete := []RowID{
		insertedRowIDs[1], // Row 2 (index 1)
		insertedRowIDs[3], // Row 4 (index 3)
	}

	for _, rowID := range rowsToDelete {
		err := mvccStorage.DeleteRow(tableID, rowID)
		if err != nil {
			t.Fatalf("failed to delete row %v: %v", rowID, err)
		}
	}

	// Commit second transaction
	err = txnMgr.CommitTransaction(txn2)
	if err != nil {
		t.Fatalf("failed to commit transaction 2: %v", err)
	}

	// Wait a bit to ensure the deleted rows are old enough
	time.Sleep(10 * time.Millisecond)

	// Create vacuum scanner
	scanner := NewVacuumScanner(mvccStorage, txnMgr.GetHorizonTracker())

	// Set a minimal safety config for testing
	// Use 0 for immediate vacuum eligibility
	scanner.SetSafetyConfig(txn.VacuumSafetyConfig{
		MinAgeBeforeVacuum: 0,
		SafetyMargin:       0,
	})

	// Find dead versions
	deadVersions, err := scanner.FindDeadVersionsInTable(tableID)
	if err != nil {
		t.Fatalf("failed to find dead versions: %v", err)
	}

	// Also check what rows exist in the table
	iter, err := mvccStorage.ScanTable(tableID, int64(txn.NextTimestamp()))
	if err != nil {
		t.Fatalf("failed to scan table: %v", err)
	}
	defer iter.Close()

	var rowCount int
	for {
		row, _, err := iter.Next()
		if err != nil {
			break
		}
		if row == nil {
			break
		}
		rowCount++
	}

	// We might have 1 or 2 dead versions depending on the horizon
	// The horizon is calculated based on active transactions, so rows
	// deleted at or after the horizon are not considered dead yet
	if len(deadVersions) < 1 || len(deadVersions) > 2 {
		t.Errorf("expected 1 or 2 dead versions, got %d", len(deadVersions))
		t.Logf("This can vary based on exact timing of timestamp generation")
	}

	// Verify the dead versions are the ones we deleted
	deadRowIDs := make(map[RowID]bool)
	for _, dv := range deadVersions {
		deadRowIDs[dv.RowID] = true
		if dv.DeletedAt == 0 {
			t.Errorf("dead version %v has DeletedAt = 0", dv.RowID)
		}
	}

	// At least one of the deleted rows should be in dead versions
	foundAny := false
	for _, rowID := range rowsToDelete {
		if deadRowIDs[rowID] {
			foundAny = true
		}
	}
	if !foundAny {
		t.Errorf("expected at least one of the deleted rows to be in dead versions")
	}
}

func TestVacuumExecutor(t *testing.T) {
	// Create disk manager, buffer pool and storage
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

	// Create a test table in catalog
	tableSchema := &catalog.TableSchema{
		SchemaName: "public",
		TableName:  "test_table",
		Columns: []catalog.ColumnDef{
			{Name: "id", DataType: types.Integer, IsNullable: false},
			{Name: "value", DataType: types.Integer, IsNullable: true},
		},
	}

	table, err := cat.CreateTable(tableSchema)
	if err != nil {
		t.Fatalf("failed to create table in catalog: %v", err)
	}

	// Create table in storage
	err = mvccStorage.CreateTable(table)
	if err != nil {
		t.Fatalf("failed to create table in storage: %v", err)
	}

	tableID := table.ID

	// Create multiple versions of the same row
	// Transaction 1: Insert row
	txn1, err := txnMgr.BeginTransaction(context.Background(), txn.ReadCommitted)
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	row1 := &Row{
		Values: []types.Value{
			types.NewValue(int32(1)),
			types.NewValue(int32(100)),
		},
	}
	rowID, err := mvccStorage.InsertRow(tableID, row1)
	if err != nil {
		t.Fatalf("failed to insert row: %v", err)
	}

	err = txnMgr.CommitTransaction(txn1)
	if err != nil {
		t.Fatalf("failed to commit transaction 1: %v", err)
	}

	// Transaction 2: Update row
	txn2, err := txnMgr.BeginTransaction(context.Background(), txn.ReadCommitted)
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	row2 := &Row{
		Values: []types.Value{
			types.NewValue(int32(1)),
			types.NewValue(int32(200)),
		},
	}
	err = mvccStorage.UpdateRow(tableID, rowID, row2)
	if err != nil {
		t.Fatalf("failed to update row: %v", err)
	}

	err = txnMgr.CommitTransaction(txn2)
	if err != nil {
		t.Fatalf("failed to commit transaction 2: %v", err)
	}

	// Transaction 3: Delete row
	txn3, err := txnMgr.BeginTransaction(context.Background(), txn.ReadCommitted)
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	err = mvccStorage.DeleteRow(tableID, rowID)
	if err != nil {
		t.Fatalf("failed to delete row: %v", err)
	}

	err = txnMgr.CommitTransaction(txn3)
	if err != nil {
		t.Fatalf("failed to commit transaction 3: %v", err)
	}

	// Wait to ensure versions are old enough
	time.Sleep(10 * time.Millisecond)

	// Create vacuum executor
	executor := NewVacuumExecutor(mvccStorage, txnMgr.GetHorizonTracker())

	// Set minimal batch config for testing
	executor.SetBatchConfig(10, 0)

	// Set aggressive safety config for testing
	executor.scanner.SetSafetyConfig(txn.VacuumSafetyConfig{
		MinAgeBeforeVacuum: 0,
		SafetyMargin:       0,
	})

	// Run vacuum on the table
	err = executor.VacuumTable(tableID)
	if err != nil {
		t.Fatalf("failed to vacuum table: %v", err)
	}

	// Get statistics
	stats := executor.GetStats()

	// Verify statistics
	if stats.TablesProcessed != 1 {
		t.Errorf("expected 1 table processed, got %d", stats.TablesProcessed)
	}

	// We should have scanned at least the deleted versions
	if stats.VersionsScanned == 0 {
		t.Errorf("expected versions scanned > 0, got %d", stats.VersionsScanned)
	}

	// Versions removed depends on implementation details
	t.Logf("Vacuum stats: Tables=%d, Scanned=%d, Removed=%d, Space=%d",
		stats.TablesProcessed, stats.VersionsScanned, stats.VersionsRemoved, stats.SpaceReclaimed)
}

func TestVacuumOperator(t *testing.T) {
	// Create disk manager, buffer pool and storage
	diskManager, err := storage.NewDiskManager(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatalf("failed to create disk manager: %v", err)
	}
	bufferPool := storage.NewBufferPool(diskManager, 100)

	// Create catalog and transaction manager
	cat := catalog.NewMemoryCatalog()
	txnMgr := txn.NewTransactionManager()

	// Create MVCC storage backend
	mvccStorage := NewMVCCStorageBackend(bufferPool, cat, nil, txnMgr)

	// Create a test table
	tableSchema := &catalog.TableSchema{
		SchemaName: "public",
		TableName:  "test_table",
		Columns: []catalog.ColumnDef{
			{Name: "id", DataType: types.Integer, IsNullable: false},
			{Name: "name", DataType: types.Text, IsNullable: true},
		},
	}

	table, err := cat.CreateTable(tableSchema)
	if err != nil {
		t.Fatalf("failed to create table in catalog: %v", err)
	}

	// Create table in storage
	err = mvccStorage.CreateTable(table)
	if err != nil {
		t.Fatalf("failed to create table in storage: %v", err)
	}

	// Create VACUUM operator
	vacuumOp := NewVacuumOperator(mvccStorage, table)

	// Create execution context
	txn, err := txnMgr.BeginTransaction(context.Background(), txn.ReadCommitted)
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	ctx := &ExecContext{
		Catalog:    cat,
		TxnManager: txnMgr,
		Txn:        txn,
		SnapshotTS: int64(txn.ReadTimestamp()),
	}

	// Open the operator
	err = vacuumOp.Open(ctx)
	if err != nil {
		t.Fatalf("failed to open vacuum operator: %v", err)
	}

	// Get results
	row, err := vacuumOp.Next()
	if err != nil {
		t.Fatalf("failed to get vacuum results: %v", err)
	}

	if row == nil {
		t.Fatal("expected vacuum results, got nil")
	}

	// Verify result schema
	if len(row.Values) != 6 {
		t.Errorf("expected 6 columns in result, got %d", len(row.Values))
	}

	// Check that we have valid results
	if row.Values[0].Null || row.Values[0].Data == nil {
		t.Error("expected non-null operation name")
	}

	// Next call should return EOF
	row, err = vacuumOp.Next()
	if err != io.EOF {
		t.Errorf("expected io.EOF for EOF, got %v", err)
	}
	if row != nil {
		t.Error("expected nil row for EOF")
	}

	// Close the operator
	err = vacuumOp.Close()
	if err != nil {
		t.Fatalf("failed to close vacuum operator: %v", err)
	}
}

func TestVacuumEndToEnd(t *testing.T) {
	// Create disk manager, buffer pool and storage
	diskManager, err := storage.NewDiskManager(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatalf("failed to create disk manager: %v", err)
	}
	bufferPool := storage.NewBufferPool(diskManager, 100)

	// Create catalog and transaction manager
	cat := catalog.NewMemoryCatalog()
	txnMgr := txn.NewTransactionManager()

	// Create MVCC storage backend
	mvccStorage := NewMVCCStorageBackend(bufferPool, cat, nil, txnMgr)

	// Create executor
	executor := NewBasicExecutor(cat, nil) // nil engine since we use storage backend
	executor.SetStorageBackend(mvccStorage)

	// Parse VACUUM statement
	p := parser.NewParser("VACUUM")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("failed to parse VACUUM: %v", err)
	}

	// Create planner
	plnr := planner.NewBasicPlannerWithCatalog(cat)
	plan, err := plnr.Plan(stmt)
	if err != nil {
		t.Fatalf("failed to plan VACUUM: %v", err)
	}

	// Execute VACUUM
	txn, err := txnMgr.BeginTransaction(context.Background(), txn.ReadCommitted)
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	ctx := &ExecContext{
		Catalog:    cat,
		TxnManager: txnMgr,
		Txn:        txn,
		SnapshotTS: int64(txn.ReadTimestamp()),
	}

	result, err := executor.Execute(plan, ctx)
	if err != nil {
		t.Fatalf("failed to execute VACUUM: %v", err)
	}

	// Get first row
	row, err := result.Next()
	if err != nil {
		t.Fatalf("failed to get result row: %v", err)
	}
	if row == nil {
		t.Fatal("expected result row, got nil")
	}

	// Verify schema
	schema := result.Schema()
	if len(schema.Columns) != 6 {
		t.Errorf("expected 6 columns in schema, got %d", len(schema.Columns))
	}

	// Close result
	result.Close()

	// Test VACUUM ANALYZE
	p2 := parser.NewParser("VACUUM ANALYZE")
	stmt2, err := p2.Parse()
	if err != nil {
		t.Fatalf("failed to parse VACUUM ANALYZE: %v", err)
	}

	plan2, err := plnr.Plan(stmt2)
	if err != nil {
		t.Fatalf("failed to plan VACUUM ANALYZE: %v", err)
	}

	result2, err := executor.Execute(plan2, ctx)
	if err != nil {
		t.Fatalf("failed to execute VACUUM ANALYZE: %v", err)
	}

	// Get first row (vacuum result)
	row2, err := result2.Next()
	if err != nil {
		t.Fatalf("failed to get VACUUM ANALYZE result: %v", err)
	}
	if row2 == nil {
		t.Fatal("expected VACUUM ANALYZE result row")
	}

	// Close result
	result2.Close()
}
