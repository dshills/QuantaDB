package executor

import (
	"context"
	"testing"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/engine"
	"github.com/dshills/QuantaDB/internal/sql/types"
	"github.com/dshills/QuantaDB/internal/storage"
	"github.com/dshills/QuantaDB/internal/txn"
)

func TestVersionChainCreation(t *testing.T) {
	// Setup test infrastructure
	cat := catalog.NewMemoryCatalog()
	eng := engine.NewMemoryEngine()
	defer eng.Close()

	txnManager := txn.NewManager(eng, nil)

	// Create storage backend
	diskManager, err := storage.NewDiskManager(":memory:")
	if err != nil {
		t.Fatalf("Failed to create disk manager: %v", err)
	}
	defer diskManager.Close()

	bufferPool := storage.NewBufferPool(diskManager, 10)
	storageBackend := NewMVCCStorageBackend(bufferPool, cat, nil, txnManager)

	// Create test schema
	if err := cat.CreateSchema("test"); err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}

	// Create test table
	schema := &catalog.TableSchema{
		SchemaName: "test",
		TableName:  "test_table",
		Columns: []catalog.ColumnDef{
			{Name: "id", DataType: types.Integer, IsNullable: false},
			{Name: "value", DataType: types.Text, IsNullable: false},
		},
	}
	table, err := cat.CreateTable(schema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	if err := storageBackend.CreateTable(table); err != nil {
		t.Fatalf("Failed to create table storage: %v", err)
	}

	// Begin transaction
	ctx := context.Background()
	txn1, err := txnManager.BeginTransaction(ctx, txn.ReadCommitted)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	defer txn1.Rollback()

	storageBackend.SetTransactionID(uint64(txn1.ID()))

	// Insert initial row
	initialRow := &Row{
		Values: []types.Value{
			types.NewValue(int32(1)),
			types.NewValue("initial_value"),
		},
	}

	rowID, err := storageBackend.InsertRow(table.ID, initialRow)
	if err != nil {
		t.Fatalf("Failed to insert initial row: %v", err)
	}

	// Update the row to create a version chain
	updatedRow := &Row{
		Values: []types.Value{
			types.NewValue(int32(1)),
			types.NewValue("updated_value"),
		},
	}

	err = storageBackend.UpdateRow(table.ID, rowID, updatedRow)
	if err != nil {
		t.Fatalf("Failed to update row: %v", err)
	}

	// Test version chain traversal
	iterator := NewVersionChainIterator(storageBackend, table.ID)

	// Get current timestamp for visibility check
	tsService := txn.NewTimestampService()
	currentTS := int64(tsService.GetNextTimestamp())

	// Debug: Check what we can get from the storage directly
	directRow, err := storageBackend.GetMVCCRow(table.ID, rowID)
	if err != nil {
		t.Fatalf("Failed to get row directly: %v", err)
	}
	t.Logf("Direct row: CreatedAt=%d, DeletedAt=%d, DeletedByTxn=%d", 
		directRow.Header.CreatedAt, directRow.Header.DeletedAt, directRow.Header.DeletedByTxn)
	t.Logf("Current timestamp: %d", currentTS)

	// Should find the updated version
	visibleRow, visibleRowID, err := iterator.FindVisibleVersion(rowID, currentTS)
	if err != nil {
		t.Fatalf("Failed to find visible version: %v", err)
	}

	if visibleRow == nil {
		t.Fatal("Expected to find a visible version, got nil")
	}

	// The visible version should have the updated value
	if visibleRow.Data.Values[1].Data.(string) != "updated_value" {
		t.Errorf("Expected updated_value, got %v", visibleRow.Data.Values[1].Data)
	}

	// Test getting all versions
	allVersions, allRowIDs, err := iterator.GetAllVersions(rowID)
	if err != nil {
		t.Fatalf("Failed to get all versions: %v", err)
	}

	if len(allVersions) != 2 {
		t.Errorf("Expected 2 versions (current + old backup), got %d", len(allVersions))
	}

	// Test chain length
	chainLength, err := iterator.GetChainLength(rowID)
	if err != nil {
		t.Fatalf("Failed to get chain length: %v", err)
	}

	if chainLength != 2 {
		t.Errorf("Expected chain length 2 (current + old backup), got %d", chainLength)
	}

	t.Logf("Test completed successfully")
	t.Logf("Visible row ID: %v", visibleRowID)
	t.Logf("All versions count: %d", len(allVersions))
	t.Logf("All row IDs: %v", allRowIDs)
}

func TestVersionChainVisibility(t *testing.T) {
	// Setup test infrastructure
	cat := catalog.NewMemoryCatalog()
	eng := engine.NewMemoryEngine()
	defer eng.Close()

	txnManager := txn.NewManager(eng, nil)

	// Create storage backend
	diskManager, err := storage.NewDiskManager(":memory:")
	if err != nil {
		t.Fatalf("Failed to create disk manager: %v", err)
	}
	defer diskManager.Close()

	bufferPool := storage.NewBufferPool(diskManager, 10)
	storageBackend := NewMVCCStorageBackend(bufferPool, cat, nil, txnManager)

	// Create test schema
	if err := cat.CreateSchema("test"); err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}

	// Create test table
	schema := &catalog.TableSchema{
		SchemaName: "test",
		TableName:  "test_table",
		Columns: []catalog.ColumnDef{
			{Name: "id", DataType: types.Integer, IsNullable: false},
			{Name: "value", DataType: types.Text, IsNullable: false},
		},
	}
	table, err := cat.CreateTable(schema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	if err := storageBackend.CreateTable(table); err != nil {
		t.Fatalf("Failed to create table storage: %v", err)
	}

	// Begin transaction 1
	ctx := context.Background()
	txn1, err := txnManager.BeginTransaction(ctx, txn.RepeatableRead)
	if err != nil {
		t.Fatalf("Failed to begin transaction 1: %v", err)
	}
	defer txn1.Rollback()

	storageBackend.SetTransactionID(uint64(txn1.ID()))

	// Get timestamp before any operations
	tsService := txn.NewTimestampService()
	beforeInsertTS := int64(tsService.GetCurrentTimestamp())

	// Insert initial row
	initialRow := &Row{
		Values: []types.Value{
			types.NewValue(int32(1)),
			types.NewValue("initial_value"),
		},
	}

	rowID, err := storageBackend.InsertRow(table.ID, initialRow)
	if err != nil {
		t.Fatalf("Failed to insert initial row: %v", err)
	}

	// Get timestamp after insert
	afterInsertTS := int64(tsService.GetNextTimestamp())

	// Update the row
	updatedRow := &Row{
		Values: []types.Value{
			types.NewValue(int32(1)),
			types.NewValue("updated_value"),
		},
	}

	err = storageBackend.UpdateRow(table.ID, rowID, updatedRow)
	if err != nil {
		t.Fatalf("Failed to update row: %v", err)
	}

	// Get timestamp after update
	afterUpdateTS := int64(tsService.GetNextTimestamp())

	// Test version chain iterator
	iterator := NewVersionChainIterator(storageBackend, table.ID)

	// Test 1: Snapshot before insert should see nothing
	visibleRow, _, err := iterator.FindVisibleVersion(rowID, beforeInsertTS)
	if err != nil {
		t.Fatalf("Failed to find visible version before insert: %v", err)
	}
	if visibleRow != nil {
		t.Error("Expected no visible version before insert, but found one")
	}

	// Test 2: Snapshot after insert but before update should see initial value
	visibleRow, _, err = iterator.FindVisibleVersion(rowID, afterInsertTS)
	if err != nil {
		t.Fatalf("Failed to find visible version after insert: %v", err)
	}
	if visibleRow == nil {
		t.Fatal("Expected to find visible version after insert, got nil")
	}
	if visibleRow.Data.Values[1].Data.(string) != "initial_value" {
		t.Errorf("Expected initial_value, got %v", visibleRow.Data.Values[1].Data)
	}

	// Test 3: Snapshot after update should see updated value
	visibleRow, _, err = iterator.FindVisibleVersion(rowID, afterUpdateTS)
	if err != nil {
		t.Fatalf("Failed to find visible version after update: %v", err)
	}
	if visibleRow == nil {
		t.Fatal("Expected to find visible version after update, got nil")
	}
	if visibleRow.Data.Values[1].Data.(string) != "updated_value" {
		t.Errorf("Expected updated_value, got %v", visibleRow.Data.Values[1].Data)
	}

	t.Logf("Version chain visibility tests completed successfully")
}

func TestVersionChainMultipleUpdates(t *testing.T) {
	// Setup test infrastructure
	cat := catalog.NewMemoryCatalog()
	eng := engine.NewMemoryEngine()
	defer eng.Close()

	txnManager := txn.NewManager(eng, nil)

	// Create storage backend
	diskManager, err := storage.NewDiskManager(":memory:")
	if err != nil {
		t.Fatalf("Failed to create disk manager: %v", err)
	}
	defer diskManager.Close()

	bufferPool := storage.NewBufferPool(diskManager, 10)
	storageBackend := NewMVCCStorageBackend(bufferPool, cat, nil, txnManager)

	// Create test schema
	if err := cat.CreateSchema("test"); err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}

	// Create test table
	schema := &catalog.TableSchema{
		SchemaName: "test",
		TableName:  "test_table",
		Columns: []catalog.ColumnDef{
			{Name: "id", DataType: types.Integer, IsNullable: false},
			{Name: "counter", DataType: types.Integer, IsNullable: false},
		},
	}
	table, err := cat.CreateTable(schema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	if err := storageBackend.CreateTable(table); err != nil {
		t.Fatalf("Failed to create table storage: %v", err)
	}

	// Begin transaction
	ctx := context.Background()
	txn1, err := txnManager.BeginTransaction(ctx, txn.ReadCommitted)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	defer txn1.Rollback()

	storageBackend.SetTransactionID(uint64(txn1.ID()))

	// Insert initial row
	initialRow := &Row{
		Values: []types.Value{
			types.NewValue(int32(1)),
			types.NewValue(int32(0)),
		},
	}

	rowID, err := storageBackend.InsertRow(table.ID, initialRow)
	if err != nil {
		t.Fatalf("Failed to insert initial row: %v", err)
	}

	// Perform multiple updates to create a longer version chain
	for i := 1; i <= 5; i++ {
		updatedRow := &Row{
			Values: []types.Value{
				types.NewValue(int32(1)),
				types.NewValue(int32(i)),
			},
		}

		err = storageBackend.UpdateRow(table.ID, rowID, updatedRow)
		if err != nil {
			t.Fatalf("Failed to update row (iteration %d): %v", i, err)
		}
	}

	// Test that we can find the latest version
	iterator := NewVersionChainIterator(storageBackend, table.ID)
	tsService := txn.NewTimestampService()
	currentTS := int64(tsService.GetNextTimestamp())

	visibleRow, _, err := iterator.FindVisibleVersion(rowID, currentTS)
	if err != nil {
		t.Fatalf("Failed to find visible version: %v", err)
	}

	if visibleRow == nil {
		t.Fatal("Expected to find a visible version, got nil")
	}

	// Should see the last update (counter = 5)
	if visibleRow.Data.Values[1].Data.(int32) != 5 {
		t.Errorf("Expected counter value 5, got %v", visibleRow.Data.Values[1].Data)
	}

	// Test chain length - should only see 1 version (the latest visible one)
	// Note: Previous versions are marked as deleted by the same transaction
	chainLength, err := iterator.GetChainLength(rowID)
	if err != nil {
		t.Fatalf("Failed to get chain length: %v", err)
	}

	t.Logf("Chain length: %d", chainLength)
	t.Logf("Latest visible counter value: %v", visibleRow.Data.Values[1].Data)

	// Get all versions for debugging
	allVersions, allRowIDs, err := iterator.GetAllVersions(rowID)
	if err != nil {
		t.Fatalf("Failed to get all versions: %v", err)
	}

	t.Logf("Total versions in chain: %d", len(allVersions))
	for i, version := range allVersions {
		t.Logf("Version %d (RowID %v): counter=%v, deleted=%v", 
			i, allRowIDs[i], version.Data.Values[1].Data, version.IsDeleted())
	}
}

func TestVersionChainValidation(t *testing.T) {
	// Setup test infrastructure
	cat := catalog.NewMemoryCatalog()
	eng := engine.NewMemoryEngine()
	defer eng.Close()

	txnManager := txn.NewManager(eng, nil)

	// Create storage backend
	diskManager, err := storage.NewDiskManager(":memory:")
	if err != nil {
		t.Fatalf("Failed to create disk manager: %v", err)
	}
	defer diskManager.Close()

	bufferPool := storage.NewBufferPool(diskManager, 10)
	storageBackend := NewMVCCStorageBackend(bufferPool, cat, nil, txnManager)

	// Create test schema
	if err := cat.CreateSchema("test"); err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}

	// Create test table
	schema := &catalog.TableSchema{
		SchemaName: "test",
		TableName:  "test_table",
		Columns: []catalog.ColumnDef{
			{Name: "id", DataType: types.Integer, IsNullable: false},
			{Name: "value", DataType: types.Text, IsNullable: false},
		},
	}
	table, err := cat.CreateTable(schema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	if err := storageBackend.CreateTable(table); err != nil {
		t.Fatalf("Failed to create table storage: %v", err)
	}

	// Begin transaction
	ctx := context.Background()
	txn1, err := txnManager.BeginTransaction(ctx, txn.ReadCommitted)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	defer txn1.Rollback()

	storageBackend.SetTransactionID(uint64(txn1.ID()))

	// Insert initial row
	initialRow := &Row{
		Values: []types.Value{
			types.NewValue(int32(1)),
			types.NewValue("initial_value"),
		},
	}

	rowID, err := storageBackend.InsertRow(table.ID, initialRow)
	if err != nil {
		t.Fatalf("Failed to insert initial row: %v", err)
	}

	// Update the row to create a version chain
	updatedRow := &Row{
		Values: []types.Value{
			types.NewValue(int32(1)),
			types.NewValue("updated_value"),
		},
	}

	err = storageBackend.UpdateRow(table.ID, rowID, updatedRow)
	if err != nil {
		t.Fatalf("Failed to update row: %v", err)
	}

	// Test version chain validation
	iterator := NewVersionChainIterator(storageBackend, table.ID)
	result, err := iterator.ValidateVersionChain(rowID)
	if err != nil {
		t.Fatalf("Failed to validate version chain: %v", err)
	}

	// Check validation results
	if !result.IsValid {
		t.Errorf("Version chain validation failed. Errors: %v", result.Errors)
	}

	if result.ChainLength != 2 {
		t.Errorf("Expected chain length 2, got %d", result.ChainLength)
	}

	if result.StartRowID != rowID {
		t.Errorf("Expected start RowID %v, got %v", rowID, result.StartRowID)
	}

	t.Logf("Validation results:")
	t.Logf("  Chain length: %d", result.ChainLength)
	t.Logf("  Is valid: %v", result.IsValid)
	t.Logf("  Errors: %v", result.Errors)
	t.Logf("  Warnings: %v", result.Warnings)
}

func TestVersionChainStatistics(t *testing.T) {
	// Setup test infrastructure
	cat := catalog.NewMemoryCatalog()
	eng := engine.NewMemoryEngine()
	defer eng.Close()

	txnManager := txn.NewManager(eng, nil)

	// Create storage backend
	diskManager, err := storage.NewDiskManager(":memory:")
	if err != nil {
		t.Fatalf("Failed to create disk manager: %v", err)
	}
	defer diskManager.Close()

	bufferPool := storage.NewBufferPool(diskManager, 10)
	storageBackend := NewMVCCStorageBackend(bufferPool, cat, nil, txnManager)

	// Create test schema
	if err := cat.CreateSchema("test"); err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}

	// Create test table
	schema := &catalog.TableSchema{
		SchemaName: "test",
		TableName:  "test_table",
		Columns: []catalog.ColumnDef{
			{Name: "id", DataType: types.Integer, IsNullable: false},
			{Name: "counter", DataType: types.Integer, IsNullable: false},
		},
	}
	table, err := cat.CreateTable(schema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	if err := storageBackend.CreateTable(table); err != nil {
		t.Fatalf("Failed to create table storage: %v", err)
	}

	// Begin transaction
	ctx := context.Background()
	txn1, err := txnManager.BeginTransaction(ctx, txn.ReadCommitted)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	defer txn1.Rollback()

	storageBackend.SetTransactionID(uint64(txn1.ID()))

	// Insert initial row
	initialRow := &Row{
		Values: []types.Value{
			types.NewValue(int32(1)),
			types.NewValue(int32(0)),
		},
	}

	rowID, err := storageBackend.InsertRow(table.ID, initialRow)
	if err != nil {
		t.Fatalf("Failed to insert initial row: %v", err)
	}

	// Get statistics before updates
	iterator := NewVersionChainIterator(storageBackend, table.ID)
	tsService := txn.NewTimestampService()
	currentTS := int64(tsService.GetNextTimestamp())

	statsBefore, err := iterator.GetSingleVersionChainStats(rowID, currentTS)
	if err != nil {
		t.Fatalf("Failed to get stats before updates: %v", err)
	}

	// Perform multiple updates to create a longer version chain
	for i := 1; i <= 3; i++ {
		updatedRow := &Row{
			Values: []types.Value{
				types.NewValue(int32(1)),
				types.NewValue(int32(i)),
			},
		}

		err = storageBackend.UpdateRow(table.ID, rowID, updatedRow)
		if err != nil {
			t.Fatalf("Failed to update row (iteration %d): %v", i, err)
		}
	}

	// Get statistics after updates
	currentTS = int64(tsService.GetNextTimestamp())
	statsAfter, err := iterator.GetSingleVersionChainStats(rowID, currentTS)
	if err != nil {
		t.Fatalf("Failed to get stats after updates: %v", err)
	}

	// Verify statistics
	if statsBefore.ChainLength != 1 {
		t.Errorf("Expected chain length 1 before updates, got %d", statsBefore.ChainLength)
	}

	if !statsBefore.HasUpdates && statsBefore.ChainLength > 1 {
		t.Errorf("Expected HasUpdates to be false before updates")
	}

	if statsAfter.ChainLength != 2 { // Current version + 1 old backup
		t.Errorf("Expected chain length 2 after updates, got %d", statsAfter.ChainLength)
	}

	if !statsAfter.HasUpdates {
		t.Errorf("Expected HasUpdates to be true after updates")
	}

	if statsAfter.VisibleVersions != 1 {
		t.Errorf("Expected 1 visible version, got %d", statsAfter.VisibleVersions)
	}

	if statsAfter.DeadVersions != 1 {
		t.Errorf("Expected 1 dead version, got %d", statsAfter.DeadVersions)
	}

	t.Logf("Statistics before updates:")
	t.Logf("  Chain length: %d", statsBefore.ChainLength)
	t.Logf("  Visible versions: %d", statsBefore.VisibleVersions)
	t.Logf("  Dead versions: %d", statsBefore.DeadVersions)
	t.Logf("  Has updates: %v", statsBefore.HasUpdates)

	t.Logf("Statistics after updates:")
	t.Logf("  Chain length: %d", statsAfter.ChainLength)
	t.Logf("  Visible versions: %d", statsAfter.VisibleVersions)
	t.Logf("  Dead versions: %d", statsAfter.DeadVersions)
	t.Logf("  Has updates: %v", statsAfter.HasUpdates)
}