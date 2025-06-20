package executor

import (
	"testing"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/types"
	"github.com/dshills/QuantaDB/internal/storage"
	"github.com/dshills/QuantaDB/internal/txn"
)

// TestVacuumVersionChain tests vacuum with version chains from updates
func TestVacuumVersionChain(t *testing.T) {
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

	// Create a test table
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

	err = mvccStorage.CreateTable(table)
	if err != nil {
		t.Fatalf("failed to create table in storage: %v", err)
	}

	tableID := table.ID

	// Insert a row
	row := &Row{
		Values: []types.Value{
			types.NewValue(int32(1)),
			types.NewValue(int32(100)),
		},
	}
	rowID, err := mvccStorage.InsertRow(tableID, row)
	if err != nil {
		t.Fatalf("failed to insert row: %v", err)
	}

	// Update the row multiple times to create a version chain
	for i := 0; i < 3; i++ {
		// Simulate different transactions
		mvccStorage.SetCurrentTransaction(txn.TransactionID(1000+i), int64(1000+i))

		updatedRow := &Row{
			Values: []types.Value{
				types.NewValue(int32(1)),
				types.NewValue(int32(200 + i*100)), // 200, 300, 400
			},
		}
		err = mvccStorage.UpdateRow(tableID, rowID, updatedRow)
		if err != nil {
			t.Fatalf("failed to update row %d: %v", i, err)
		}
	}

	// Create version chain iterator to verify chain
	iterator := NewVersionChainIterator(mvccStorage, tableID)
	versions, versionRowIDs, err := iterator.GetAllVersions(rowID)
	if err != nil {
		t.Fatalf("failed to get version chain: %v", err)
	}

	t.Logf("Version chain has %d versions", len(versions))

	// The chain should have the current version + 1 old version
	// (due to our in-place update strategy)
	if len(versions) < 2 {
		t.Errorf("expected at least 2 versions in chain, got %d", len(versions))
	}

	// Get the old version's RowID
	var oldVersionRowID RowID
	if len(versionRowIDs) >= 2 {
		oldVersionRowID = versionRowIDs[1]
		t.Logf("Old version RowID: %v", oldVersionRowID)
	}

	// Create vacuum scanner and executor
	scanner := NewVacuumScanner(mvccStorage, txnMgr.GetHorizonTracker())
	scanner.SetSafetyConfig(txn.VacuumSafetyConfig{
		MinAgeBeforeVacuum: 0,
		SafetyMargin:       0,
	})

	executor := NewVacuumExecutor(mvccStorage, txnMgr.GetHorizonTracker())
	executor.SetBatchConfig(10, 0)
	executor.scanner.SetSafetyConfig(txn.VacuumSafetyConfig{
		MinAgeBeforeVacuum: 0,
		SafetyMargin:       0,
	})

	// Find dead versions in chain
	deadVersions, err := scanner.FindDeadVersionsInChain(tableID, rowID)
	if err != nil {
		t.Fatalf("failed to find dead versions in chain: %v", err)
	}

	t.Logf("Found %d dead versions in chain", len(deadVersions))

	// Should find 1 dead version (the old backup created on first update)
	if len(deadVersions) != 1 {
		t.Errorf("expected 1 dead version in chain, got %d", len(deadVersions))
	}

	// Run vacuum
	err = executor.VacuumTable(tableID)
	if err != nil {
		t.Fatalf("failed to vacuum table: %v", err)
	}

	// Get statistics
	stats := executor.GetStats()
	t.Logf("Vacuum stats: Tables=%d, Scanned=%d, Removed=%d, Space=%d, ChainRelinks=%d",
		stats.TablesProcessed, stats.VersionsScanned, stats.VersionsRemoved, stats.SpaceReclaimed, stats.ChainRelinks)

	// Verify old version is gone if we found it
	if len(versionRowIDs) >= 2 && stats.VersionsRemoved > 0 {
		_, err = mvccStorage.GetMVCCRow(tableID, oldVersionRowID)
		if err == nil {
			t.Error("old version should have been removed by vacuum")
		} else {
			t.Logf("Old version correctly removed: %v", err)
		}
	}

	// Verify current version is still readable
	currentRow, err := mvccStorage.GetRow(tableID, rowID, int64(txn.NextTimestamp()))
	if err != nil {
		t.Fatalf("failed to read current version after vacuum: %v", err)
	}

	// Should have the last update value (400)
	val, ok := currentRow.Values[1].Data.(int32)
	if !ok || val != 400 {
		t.Errorf("expected current value to be 400, got %v", currentRow.Values[1].Data)
	}
}

// TestVacuumWithConcurrentReads tests vacuum doesn't interfere with active reads
func TestVacuumWithConcurrentReads(t *testing.T) {
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

	// Create a test table
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

	err = mvccStorage.CreateTable(table)
	if err != nil {
		t.Fatalf("failed to create table in storage: %v", err)
	}

	tableID := table.ID

	// Insert and delete some rows to create dead versions
	var deletedRowIDs []RowID
	for i := 1; i <= 5; i++ {
		row := &Row{
			Values: []types.Value{
				types.NewValue(int32(i)),
				types.NewValue(int32(i * 100)),
			},
		}
		rowID, err := mvccStorage.InsertRow(tableID, row)
		if err != nil {
			t.Fatalf("failed to insert row %d: %v", i, err)
		}

		// Delete even numbered rows
		if i%2 == 0 {
			mvccStorage.SetCurrentTransaction(txn.TransactionID(1000+i), int64(10+i))
			err = mvccStorage.DeleteRow(tableID, rowID)
			if err != nil {
				t.Fatalf("failed to delete row %d: %v", i, err)
			}
			deletedRowIDs = append(deletedRowIDs, rowID)
		}
	}

	// Start a read transaction with a snapshot before deletions
	readSnapshot := int64(5) // Before any deletions

	// Create vacuum executor
	executor := NewVacuumExecutor(mvccStorage, txnMgr.GetHorizonTracker())
	executor.SetBatchConfig(10, 0)

	// With default safety config, vacuum should be conservative
	err = executor.VacuumTable(tableID)
	if err != nil {
		t.Fatalf("failed to vacuum table: %v", err)
	}

	stats := executor.GetStats()
	t.Logf("Conservative vacuum stats: Removed=%d (with active transaction)",
		stats.VersionsRemoved)

	// Now set aggressive config for testing
	executor.scanner.SetSafetyConfig(txn.VacuumSafetyConfig{
		MinAgeBeforeVacuum: 0,
		SafetyMargin:       0,
	})

	// Run vacuum again
	executor = NewVacuumExecutor(mvccStorage, txnMgr.GetHorizonTracker())
	executor.scanner.SetSafetyConfig(txn.VacuumSafetyConfig{
		MinAgeBeforeVacuum: 0,
		SafetyMargin:       0,
	})

	err = executor.VacuumTable(tableID)
	if err != nil {
		t.Fatalf("failed to vacuum table: %v", err)
	}

	stats = executor.GetStats()
	t.Logf("Aggressive vacuum stats: Removed=%d", stats.VersionsRemoved)

	// Verify we can still read with old snapshot
	iterator, err := mvccStorage.ScanTable(tableID, readSnapshot)
	if err != nil {
		t.Fatalf("failed to create scan iterator: %v", err)
	}
	defer iterator.Close()

	count := 0
	for {
		row, _, err := iterator.Next()
		if err != nil {
			break
		}
		if row != nil {
			count++
		}
	}

	// With snapshot before deletions, should see all 5 rows
	t.Logf("Rows visible with old snapshot: %d", count)
	if count != 5 {
		t.Errorf("expected 5 rows visible with old snapshot, got %d", count)
	}
}

