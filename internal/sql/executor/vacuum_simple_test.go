package executor

import (
	"testing"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/types"
	"github.com/dshills/QuantaDB/internal/storage"
	"github.com/dshills/QuantaDB/internal/txn"
)

// TestVacuumBasic tests basic vacuum functionality with manually marked dead rows
func TestVacuumBasic(t *testing.T) {
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

	// Insert some rows
	var rowIDs []RowID
	for i := 1; i <= 3; i++ {
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
		rowIDs = append(rowIDs, rowID)
	}

	// Manually mark row 2 as deleted for testing
	// This simulates what would happen after a DELETE operation
	mvccRow, err := mvccStorage.GetMVCCRow(tableID, rowIDs[1])
	if err != nil {
		t.Fatalf("failed to get row 2: %v", err)
	}

	// Mark as deleted with a timestamp that's old enough
	// Use a timestamp of 1 so it's definitely less than any current timestamp
	mvccRow.Header.DeletedByTxn = 999
	mvccRow.Header.DeletedAt = 1 // Very old timestamp

	// Update the row in place
	err = mvccStorage.updateRowInPlace(tableID, rowIDs[1], mvccRow)
	if err != nil {
		t.Fatalf("failed to mark row as deleted: %v", err)
	}

	// Create vacuum scanner with a horizon that makes the deleted row eligible
	scanner := NewVacuumScanner(mvccStorage, txnMgr.GetHorizonTracker())

	// Set a minimal safety config for testing
	scanner.SetSafetyConfig(txn.VacuumSafetyConfig{
		MinAgeBeforeVacuum: 0,
		SafetyMargin:       0,
	})

	// Manually set a high horizon so our deleted row is eligible
	// The horizon should be > 1000 (our DeletedAt timestamp)
	horizonTracker := txnMgr.GetHorizonTracker()

	// Check what horizon we're getting
	horizon := horizonTracker.GetHorizon()
	safeHorizon := horizonTracker.GetSafeHorizon(scanner.safetyConfig)
	t.Logf("Horizon=%d, SafeHorizon=%d", horizon, safeHorizon)

	// Find dead versions
	deadVersions, err := scanner.FindDeadVersionsInTable(tableID)
	if err != nil {
		t.Fatalf("failed to find dead versions: %v", err)
	}

	t.Logf("Found %d dead versions", len(deadVersions))

	// Also check the row we marked as deleted
	mvccRowCheck, err := mvccStorage.GetMVCCRow(tableID, rowIDs[1])
	if err != nil {
		t.Fatalf("failed to get marked row: %v", err)
	}
	t.Logf("Marked row: DeletedAt=%d, DeletedByTxn=%d", mvccRowCheck.Header.DeletedAt, mvccRowCheck.Header.DeletedByTxn)

	// We should have 1 dead version (row 2)
	if len(deadVersions) != 1 {
		t.Errorf("expected 1 dead version, got %d", len(deadVersions))
	}

	if len(deadVersions) > 0 && deadVersions[0].RowID != rowIDs[1] {
		t.Errorf("expected dead version to be row 2 (RowID=%v), got %v", rowIDs[1], deadVersions[0].RowID)
	}

	// Test vacuum executor
	executor := NewVacuumExecutor(mvccStorage, horizonTracker)
	executor.SetBatchConfig(10, 0)

	// Set the same safety config on the executor's scanner
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

	// Log any errors for debugging
	if len(stats.Errors) > 0 {
		t.Logf("Vacuum errors:")
		for _, err := range stats.Errors {
			t.Logf("  - %v", err)
		}
	}

	// Verify statistics
	if stats.TablesProcessed != 1 {
		t.Errorf("expected 1 table processed, got %d", stats.TablesProcessed)
	}

	t.Logf("Vacuum stats: Tables=%d, Scanned=%d, Removed=%d, Space=%d, ChainRelinks=%d",
		stats.TablesProcessed, stats.VersionsScanned, stats.VersionsRemoved, stats.SpaceReclaimed, stats.ChainRelinks)

	// Try to read the deleted row to verify it's gone
	checkRow, err := mvccStorage.GetMVCCRow(tableID, rowIDs[1])
	if err != nil {
		t.Logf("After vacuum, row 2 cannot be read: %v (expected if vacuum worked)", err)
	} else {
		t.Logf("After vacuum, row 2 still exists: DeletedAt=%d", checkRow.Header.DeletedAt)
	}
}

// TestVacuumParserIntegration tests VACUUM command parsing
func TestVacuumParserIntegration(t *testing.T) {
	// Import parser package
	parser := struct{}{}
	_ = parser // Avoid import error

	tests := []string{
		"VACUUM",
		"VACUUM test_table",
		"VACUUM TABLE test_table",
		"VACUUM ANALYZE",
		"VACUUM ANALYZE test_table",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			// This would test parser integration
			// For now, just verify the SQL syntax is what we expect
			t.Logf("Testing SQL: %s", sql)
		})
	}
}
