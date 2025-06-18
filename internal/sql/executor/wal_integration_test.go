package executor

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/types"
	"github.com/dshills/QuantaDB/internal/storage"
	"github.com/dshills/QuantaDB/internal/wal"
)

func TestStorageWithWAL(t *testing.T) {
	// Create temporary directory for test
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	walDir := filepath.Join(tmpDir, "wal")
	
	// Create disk manager and buffer pool
	dm, err := storage.NewDiskManager(dbPath)
	if err != nil {
		t.Fatalf("failed to create disk manager: %v", err)
	}
	defer dm.Close()
	
	bufferPool := storage.NewBufferPool(dm, 10)
	cat := catalog.NewMemoryCatalog()
	
	// Create WAL manager
	walConfig := &wal.Config{
		Directory:    walDir,
		BufferSize:   1024 * 1024, // 1MB
		SegmentSize:  16 * 1024 * 1024, // 16MB
		SyncOnCommit: true,
	}
	
	walManager, err := wal.NewManager(walConfig)
	if err != nil {
		t.Fatalf("failed to create WAL manager: %v", err)
	}
	defer walManager.Close()
	
	// Create storage backend with WAL
	storageBackend := NewDiskStorageBackendWithWAL(bufferPool, cat, walManager)
	
	// Create a test table
	table := &catalog.Table{
		ID:         1,
		SchemaName: "public",
		TableName:  "users",
		Columns: []*catalog.Column{
			{ID: 1, Name: "id", DataType: types.Integer, IsNullable: false},
			{ID: 2, Name: "name", DataType: types.Text, IsNullable: false},
			{ID: 3, Name: "age", DataType: types.Integer, IsNullable: true},
		},
	}
	
	// Create table in storage
	err = storageBackend.CreateTable(table)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	
	// Start a transaction
	txnID := uint64(100)
	storageBackend.SetTransactionID(txnID)
	
	// Log transaction begin
	beginLSN, err := walManager.LogBeginTxn(txnID)
	if err != nil {
		t.Fatalf("failed to log begin transaction: %v", err)
	}
	t.Logf("Transaction %d started at LSN %d", txnID, beginLSN)
	
	// Insert test data
	testRows := []struct {
		id   int32
		name string
		age  int32
	}{
		{1, "Alice", 25},
		{2, "Bob", 30},
		{3, "Charlie", 35},
	}
	
	var rowIDs []RowID
	for _, tr := range testRows {
		row := &Row{
			Values: []types.Value{
				types.NewValue(tr.id),
				types.NewValue(tr.name),
				types.NewValue(tr.age),
			},
		}
		
		rowID, err := storageBackend.InsertRow(table.ID, row)
		if err != nil {
			t.Fatalf("failed to insert row: %v", err)
		}
		rowIDs = append(rowIDs, rowID)
		t.Logf("Inserted row %d at PageID=%d, SlotID=%d", tr.id, rowID.PageID, rowID.SlotID)
	}
	
	// Update a row
	updatedRow := &Row{
		Values: []types.Value{
			types.NewValue(int32(2)),
			types.NewValue("Bob Updated"),
			types.NewValue(int32(31)),
		},
	}
	
	err = storageBackend.UpdateRow(table.ID, rowIDs[1], updatedRow)
	if err != nil {
		t.Fatalf("failed to update row: %v", err)
	}
	t.Log("Updated Bob's record")
	
	// Delete a row
	err = storageBackend.DeleteRow(table.ID, rowIDs[2])
	if err != nil {
		t.Fatalf("failed to delete row: %v", err)
	}
	t.Log("Deleted Charlie's record")
	
	// Commit transaction
	commitLSN, err := walManager.LogCommitTxn(txnID)
	if err != nil {
		t.Fatalf("failed to log commit transaction: %v", err)
	}
	t.Logf("Transaction %d committed at LSN %d", txnID, commitLSN)
	
	// Force WAL flush
	err = walManager.Flush()
	if err != nil {
		t.Fatalf("failed to flush WAL: %v", err)
	}
	
	// Verify WAL records were written
	verifyWALRecords(t, walDir)
	
	// Scan table to verify final state
	iter, err := storageBackend.ScanTable(table.ID)
	if err != nil {
		t.Fatalf("failed to scan table: %v", err)
	}
	defer iter.Close()
	
	rowCount := 0
	for {
		row, _, err := iter.Next()
		if err != nil {
			break
		}
		if row == nil {
			break
		}
		rowCount++
		
		id := row.Values[0].Data.(int32)
		name := row.Values[1].Data.(string)
		
		switch id {
		case 1:
			if name != "Alice" {
				t.Errorf("expected Alice, got %s", name)
			}
		case 2:
			if name != "Bob Updated" {
				t.Errorf("expected Bob Updated, got %s", name)
			}
		case 3:
			t.Error("Charlie should have been deleted")
		}
	}
	
	// Should have 2 rows after update and delete
	if rowCount != 2 {
		t.Errorf("expected 2 rows, got %d", rowCount)
	}
}

func verifyWALRecords(t *testing.T, walDir string) {
	// Find WAL files
	pattern := filepath.Join(walDir, "*.wal")
	files, err := filepath.Glob(pattern)
	if err != nil {
		t.Fatalf("failed to list WAL files: %v", err)
	}
	
	if len(files) == 0 {
		t.Fatal("no WAL files found")
	}
	
	// Read and verify records from the first WAL file
	file, err := os.Open(files[0])
	if err != nil {
		t.Fatalf("failed to open WAL file: %v", err)
	}
	defer file.Close()
	
	expectedTypes := []wal.RecordType{
		wal.RecordTypeBeginTxn,
		wal.RecordTypeInsert,
		wal.RecordTypeInsert,
		wal.RecordTypeInsert,
		wal.RecordTypeInsert, // New version for update
		wal.RecordTypeDelete, // Old version marked deleted
		wal.RecordTypeDelete, // Charlie deleted
		wal.RecordTypeCommitTxn,
	}
	
	recordCount := 0
	for {
		record, err := wal.DeserializeRecord(file)
		if err == io.EOF {
			break
		}
		if err != nil {
			// It's okay to have incomplete records at the end of file
			t.Logf("Stopped reading at record %d (expected behavior at end of file)", recordCount)
			break
		}
		
		if recordCount < len(expectedTypes) {
			if record.Type != expectedTypes[recordCount] {
				t.Errorf("record %d: expected type %v, got %v", 
					recordCount, expectedTypes[recordCount], record.Type)
			}
		}
		
		t.Logf("WAL Record %d: Type=%v, LSN=%d, TxnID=%d", 
			recordCount, record.Type, record.LSN, record.TxnID)
		
		recordCount++
	}
	
	if recordCount < len(expectedTypes) {
		t.Errorf("expected at least %d WAL records, got %d", len(expectedTypes), recordCount)
	}
}

func TestWALRecoveryAfterCrash(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	walDir := filepath.Join(tmpDir, "wal")
	
	// Phase 1: Create data and "crash"
	var expectedRowCount int
	{
		dm, err := storage.NewDiskManager(dbPath)
		if err != nil {
			t.Fatalf("failed to create disk manager: %v", err)
		}
		
		bufferPool := storage.NewBufferPool(dm, 10)
		cat := catalog.NewMemoryCatalog()
		
		walConfig := &wal.Config{
			Directory:    walDir,
			BufferSize:   1024,
			SegmentSize:  4096,
			SyncOnCommit: true,
		}
		
		walManager, err := wal.NewManager(walConfig)
		if err != nil {
			t.Fatalf("failed to create WAL manager: %v", err)
		}
		
		storageBackend := NewDiskStorageBackendWithWAL(bufferPool, cat, walManager)
		
		// Create table
		table := &catalog.Table{
			ID:         1,
			SchemaName: "public",
			TableName:  "test",
			Columns: []*catalog.Column{
				{ID: 1, Name: "id", DataType: types.Integer, IsNullable: false},
				{ID: 2, Name: "data", DataType: types.Text, IsNullable: false},
			},
		}
		
		storageBackend.CreateTable(table)
		
		// Insert data in transaction
		txnID := uint64(200)
		storageBackend.SetTransactionID(txnID)
		walManager.LogBeginTxn(txnID)
		
		expectedRowCount = 5
		for i := 0; i < expectedRowCount; i++ {
			row := &Row{
				Values: []types.Value{
					types.NewValue(int32(i)),
					types.NewValue("data"),
				},
			}
			storageBackend.InsertRow(table.ID, row)
		}
		
		walManager.LogCommitTxn(txnID)
		walManager.Flush()
		
		// Force buffer pool to flush pages
		bufferPool.FlushAllPages()
		
		// Simulate crash by closing everything
		dm.Close()
		walManager.Close()
	}
	
	// Phase 2: Recovery and Verification
	{
		// Create fresh disk manager and buffer pool
		dm, err := storage.NewDiskManager(dbPath)
		if err != nil {
			t.Fatalf("failed to create disk manager for recovery: %v", err)
		}
		defer dm.Close()
		
		bufferPool := storage.NewBufferPool(dm, 10)
		cat := catalog.NewMemoryCatalog()
		
		// Re-create table metadata in catalog
		table := &catalog.Table{
			ID:         1,
			SchemaName: "public",
			TableName:  "test",
			Columns: []*catalog.Column{
				{ID: 1, Name: "id", DataType: types.Integer, IsNullable: false},
				{ID: 2, Name: "data", DataType: types.Text, IsNullable: false},
			},
		}
		
		// Create new WAL manager for recovery
		walConfig := &wal.Config{
			Directory:    walDir,
			BufferSize:   1024,
			SegmentSize:  4096,
			SyncOnCommit: true,
		}
		
		walManager, err := wal.NewManager(walConfig)
		if err != nil {
			t.Fatalf("failed to create WAL manager for recovery: %v", err)
		}
		defer walManager.Close()
		
		// Perform recovery
		err = RecoverDatabase(walDir, bufferPool, cat)
		if err != nil {
			t.Fatalf("recovery failed: %v", err)
		}
		
		// Create storage backend to verify data
		storageBackend := NewDiskStorageBackendWithWAL(bufferPool, cat, walManager)
		
		// Re-create table metadata in storage backend
		// In a real system, this would be recovered from system catalogs
		storageBackend.mu.Lock()
		storageBackend.tablePages[table.ID] = 1 // Assuming table data starts at page 1
		storageBackend.tableMeta[table.ID] = &TableMetadata{
			TableID:     table.ID,
			FirstPageID: 1,
			LastPageID:  1,
			RowCount:    uint64(expectedRowCount),
			RowFormat:   NewRowFormat(&Schema{
				Columns: []Column{
					{Name: "id", Type: types.Integer, Nullable: false},
					{Name: "data", Type: types.Text, Nullable: false},
				},
			}),
		}
		storageBackend.mu.Unlock()
		
		// Scan table to verify data was recovered
		iter, err := storageBackend.ScanTable(table.ID)
		if err != nil {
			t.Fatalf("failed to scan table after recovery: %v", err)
		}
		defer iter.Close()
		
		actualRowCount := 0
		for {
			row, _, err := iter.Next()
			if err != nil || row == nil {
				break
			}
			actualRowCount++
			
			// Verify row data
			id := row.Values[0].Data.(int32)
			data := row.Values[1].Data.(string)
			
			if id < 0 || id >= int32(expectedRowCount) {
				t.Errorf("unexpected id value: %d", id)
			}
			if data != "data" {
				t.Errorf("unexpected data value: %s", data)
			}
		}
		
		// Verify all rows were recovered
		if actualRowCount != expectedRowCount {
			t.Errorf("expected %d rows after recovery, got %d", expectedRowCount, actualRowCount)
		}
		
		// Verify LSN was recovered correctly
		currentLSN := walManager.GetCurrentLSN()
		if currentLSN < 7 { // At least 7 records (begin + 5 inserts + commit)
			t.Errorf("expected LSN >= 7 after recovery, got %d", currentLSN)
		}
	}
}

// TestTransactionBoundaries tests proper WAL record chaining within transactions
func TestTransactionBoundaries(t *testing.T) {
	tmpDir := t.TempDir()
	walDir := filepath.Join(tmpDir, "wal")
	
	walConfig := &wal.Config{
		Directory:    walDir,
		BufferSize:   4096,
		SegmentSize:  1024 * 1024,
		SyncOnCommit: false,
	}
	
	walManager, err := wal.NewManager(walConfig)
	if err != nil {
		t.Fatalf("failed to create WAL manager: %v", err)
	}
	defer walManager.Close()
	
	// Test multiple concurrent transactions
	txn1 := uint64(300)
	txn2 := uint64(301)
	
	// Begin both transactions
	lsn1, _ := walManager.LogBeginTxn(txn1)
	lsn2, _ := walManager.LogBeginTxn(txn2)
	
	// Interleave operations
	walManager.LogInsert(txn1, 1, 1, 1, []byte("txn1-data1"))
	walManager.LogInsert(txn2, 2, 2, 2, []byte("txn2-data1"))
	walManager.LogInsert(txn1, 1, 1, 2, []byte("txn1-data2"))
	walManager.LogDelete(txn2, 2, 2, 1)
	
	// Commit in different order
	commitLSN2, _ := walManager.LogCommitTxn(txn2)
	commitLSN1, _ := walManager.LogCommitTxn(txn1)
	
	// Flush and read back
	walManager.Flush()
	
	// Verify LSN ordering
	if lsn2 <= lsn1 {
		t.Errorf("txn2 begin LSN %d should be > txn1 begin LSN %d", lsn2, lsn1)
	}
	if commitLSN1 <= commitLSN2 {
		t.Errorf("txn1 commit LSN %d should be > txn2 commit LSN %d", commitLSN1, commitLSN2)
	}
	
	// Read WAL and verify transaction consistency
	files, _ := filepath.Glob(filepath.Join(walDir, "*.wal"))
	file, _ := os.Open(files[0])
	defer file.Close()
	
	txnRecords := make(map[uint64][]*wal.LogRecord)
	
	for {
		record, err := wal.DeserializeRecord(file)
		if err == io.EOF {
			break
		}
		if err != nil {
			// It's okay to have incomplete records at the end of file
			break
		}
		
		if record.TxnID != 0 {
			txnRecords[record.TxnID] = append(txnRecords[record.TxnID], record)
		}
	}
	
	// Verify each transaction's records are properly chained
	for txnID, records := range txnRecords {
		t.Logf("Transaction %d has %d records", txnID, len(records))
		
		// First record should have InvalidLSN as PrevLSN
		if records[0].PrevLSN != wal.InvalidLSN {
			t.Errorf("txn %d: first record should have InvalidLSN as PrevLSN", txnID)
		}
		
		// Subsequent records should chain properly
		for i := 1; i < len(records); i++ {
			expectedPrevLSN := records[i-1].LSN
			if records[i].PrevLSN != expectedPrevLSN {
				t.Errorf("txn %d: record %d has PrevLSN %d, expected %d",
					txnID, i, records[i].PrevLSN, expectedPrevLSN)
			}
		}
	}
}