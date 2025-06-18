package wal

import (
	"bytes"
	"path/filepath"
	"testing"
	"time"
)

func TestRecordSerialization(t *testing.T) {
	// Test each record type
	tests := []struct {
		name   string
		record *LogRecord
	}{
		{
			name: "BeginTxn",
			record: &LogRecord{
				LSN:       1,
				Type:      RecordTypeBeginTxn,
				TxnID:     100,
				PrevLSN:   InvalidLSN,
				Timestamp: time.Now(),
				Data: (&TransactionRecord{
					TxnID:     100,
					Timestamp: time.Now(),
				}).Marshal(),
			},
		},
		{
			name: "Insert",
			record: &LogRecord{
				LSN:       2,
				Type:      RecordTypeInsert,
				TxnID:     100,
				PrevLSN:   1,
				Timestamp: time.Now(),
				Data: (&InsertRecord{
					TableID: 1,
					PageID:  10,
					SlotID:  5,
					RowData: []byte("test row data"),
				}).Marshal(),
			},
		},
		{
			name: "Update",
			record: &LogRecord{
				LSN:       3,
				Type:      RecordTypeUpdate,
				TxnID:     100,
				PrevLSN:   2,
				Timestamp: time.Now(),
				Data: (&UpdateRecord{
					TableID:    1,
					PageID:     10,
					SlotID:     5,
					OldRowData: []byte("old data"),
					NewRowData: []byte("new data"),
				}).Marshal(),
			},
		},
		{
			name: "Delete",
			record: &LogRecord{
				LSN:       4,
				Type:      RecordTypeDelete,
				TxnID:     100,
				PrevLSN:   3,
				Timestamp: time.Now(),
				Data: (&DeleteRecord{
					TableID: 1,
					PageID:  10,
					SlotID:  5,
				}).Marshal(),
			},
		},
		{
			name: "CommitTxn",
			record: &LogRecord{
				LSN:       5,
				Type:      RecordTypeCommitTxn,
				TxnID:     100,
				PrevLSN:   4,
				Timestamp: time.Now(),
				Data: (&TransactionRecord{
					TxnID:     100,
					Timestamp: time.Now(),
				}).Marshal(),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Serialize
			buf := &bytes.Buffer{}
			err := SerializeRecord(buf, tt.record)
			if err != nil {
				t.Fatalf("failed to serialize record: %v", err)
			}

			// Deserialize
			deserialized, err := DeserializeRecord(bytes.NewReader(buf.Bytes()))
			if err != nil {
				t.Fatalf("failed to deserialize record: %v", err)
			}

			// Compare
			if deserialized.LSN != tt.record.LSN {
				t.Errorf("LSN mismatch: got %d, want %d", deserialized.LSN, tt.record.LSN)
			}
			if deserialized.Type != tt.record.Type {
				t.Errorf("Type mismatch: got %v, want %v", deserialized.Type, tt.record.Type)
			}
			if deserialized.TxnID != tt.record.TxnID {
				t.Errorf("TxnID mismatch: got %d, want %d", deserialized.TxnID, tt.record.TxnID)
			}
			if deserialized.PrevLSN != tt.record.PrevLSN {
				t.Errorf("PrevLSN mismatch: got %d, want %d", deserialized.PrevLSN, tt.record.PrevLSN)
			}
			if !bytes.Equal(deserialized.Data, tt.record.Data) {
				t.Errorf("Data mismatch")
			}
		})
	}
}

func TestBuffer(t *testing.T) {
	buffer := NewBuffer(1024) // 1KB buffer

	// Test empty buffer
	if !buffer.IsEmpty() {
		t.Error("new buffer should be empty")
	}
	if buffer.Size() != 0 {
		t.Errorf("empty buffer size should be 0, got %d", buffer.Size())
	}

	// Add records
	record1 := NewBeginTxnRecord(1, 100)
	err := buffer.Append(record1)
	if err != nil {
		t.Fatalf("failed to append first record: %v", err)
	}

	if buffer.IsEmpty() {
		t.Error("buffer should not be empty after append")
	}
	if buffer.RecordCount() != 1 {
		t.Errorf("expected 1 record, got %d", buffer.RecordCount())
	}

	// Add more records
	record2 := NewInsertRecord(2, 100, 1, 1, 10, 5, []byte("test data"))
	err = buffer.Append(record2)
	if err != nil {
		t.Fatalf("failed to append second record: %v", err)
	}

	if buffer.RecordCount() != 2 {
		t.Errorf("expected 2 records, got %d", buffer.RecordCount())
	}
	if buffer.LastLSN() != 2 {
		t.Errorf("expected last LSN 2, got %d", buffer.LastLSN())
	}

	// Get data
	data, records := buffer.GetData()
	if len(records) != 2 {
		t.Errorf("expected 2 records in data, got %d", len(records))
	}
	if len(data) == 0 {
		t.Error("expected non-empty data")
	}

	// Reset
	buffer.Reset()
	if !buffer.IsEmpty() {
		t.Error("buffer should be empty after reset")
	}
}

func TestWALManager(t *testing.T) {
	// Create temporary directory
	tmpDir := t.TempDir()
	
	config := &Config{
		Directory:    tmpDir,
		BufferSize:   1024,
		SegmentSize:  4096,
		SyncOnCommit: true,
	}

	// Create manager
	manager, err := NewManager(config)
	if err != nil {
		t.Fatalf("failed to create WAL manager: %v", err)
	}
	defer manager.Close()

	// Test transaction logging
	txnID := uint64(100)

	// Begin transaction
	beginLSN, err := manager.LogBeginTxn(txnID)
	if err != nil {
		t.Fatalf("failed to log begin txn: %v", err)
	}
	if beginLSN == InvalidLSN {
		t.Error("got invalid LSN for begin txn")
	}

	// Insert
	insertLSN, err := manager.LogInsert(txnID, 1, 10, 5, []byte("row data"))
	if err != nil {
		t.Fatalf("failed to log insert: %v", err)
	}
	if insertLSN <= beginLSN {
		t.Errorf("insert LSN %d should be > begin LSN %d", insertLSN, beginLSN)
	}

	// Update
	updateLSN, err := manager.LogUpdate(txnID, 1, 10, 5, []byte("old"), []byte("new"))
	if err != nil {
		t.Fatalf("failed to log update: %v", err)
	}
	if updateLSN <= insertLSN {
		t.Errorf("update LSN %d should be > insert LSN %d", updateLSN, insertLSN)
	}

	// Delete
	deleteLSN, err := manager.LogDelete(txnID, 1, 10, 5)
	if err != nil {
		t.Fatalf("failed to log delete: %v", err)
	}
	if deleteLSN <= updateLSN {
		t.Errorf("delete LSN %d should be > update LSN %d", deleteLSN, updateLSN)
	}

	// Commit
	commitLSN, err := manager.LogCommitTxn(txnID)
	if err != nil {
		t.Fatalf("failed to log commit: %v", err)
	}
	if commitLSN <= deleteLSN {
		t.Errorf("commit LSN %d should be > delete LSN %d", commitLSN, deleteLSN)
	}

	// Force flush
	err = manager.Flush()
	if err != nil {
		t.Fatalf("failed to flush: %v", err)
	}

	// Verify WAL file exists
	files, err := filepath.Glob(filepath.Join(tmpDir, "*.wal"))
	if err != nil {
		t.Fatalf("failed to list WAL files: %v", err)
	}
	if len(files) == 0 {
		t.Error("no WAL files created")
	}
}

func TestWALRecovery(t *testing.T) {
	tmpDir := t.TempDir()
	
	config := &Config{
		Directory:    tmpDir,
		BufferSize:   1024,
		SegmentSize:  4096,
		SyncOnCommit: true,
	}

	// Create initial manager and write some records
	var lastLSN LSN
	{
		manager, err := NewManager(config)
		if err != nil {
			t.Fatalf("failed to create WAL manager: %v", err)
		}

		// Log some transactions
		for i := 0; i < 3; i++ {
			txnID := uint64(100 + i)
			
			manager.LogBeginTxn(txnID)
			manager.LogInsert(txnID, 1, uint32(i), uint16(i), []byte("data"))
			lastLSN, _ = manager.LogCommitTxn(txnID)
		}

		manager.Close()
	}

	// Create new manager and verify it recovers the state
	{
		manager, err := NewManager(config)
		if err != nil {
			t.Fatalf("failed to create second WAL manager: %v", err)
		}
		defer manager.Close()

		currentLSN := manager.GetCurrentLSN()
		if currentLSN != lastLSN {
			t.Errorf("recovered LSN %d doesn't match last LSN %d", currentLSN, lastLSN)
		}

		// Should be able to continue logging
		nextLSN := manager.GetNextLSN()
		if nextLSN != lastLSN+1 {
			t.Errorf("next LSN %d should be %d", nextLSN, lastLSN+1)
		}
	}
}

func TestSegmentRotation(t *testing.T) {
	tmpDir := t.TempDir()
	
	config := &Config{
		Directory:    tmpDir,
		BufferSize:   512,
		SegmentSize:  1024, // Small segment for testing
		SyncOnCommit: false,
	}

	manager, err := NewManager(config)
	if err != nil {
		t.Fatalf("failed to create WAL manager: %v", err)
	}
	defer manager.Close()

	// Write enough data to force segment rotation
	for i := 0; i < 20; i++ {
		txnID := uint64(100 + i)
		manager.LogBeginTxn(txnID)
		// Large row data to fill segment quickly
		manager.LogInsert(txnID, 1, uint32(i), uint16(i), make([]byte, 100))
		manager.LogCommitTxn(txnID)
		manager.Flush()
	}

	// Check that multiple segments were created
	files, err := filepath.Glob(filepath.Join(tmpDir, "*.wal"))
	if err != nil {
		t.Fatalf("failed to list WAL files: %v", err)
	}
	if len(files) < 2 {
		t.Errorf("expected multiple segments, got %d", len(files))
	}
}

func TestChecksumValidation(t *testing.T) {
	// Create a valid record
	record := NewBeginTxnRecord(1, 100)
	
	// Serialize it
	buf := &bytes.Buffer{}
	err := SerializeRecord(buf, record)
	if err != nil {
		t.Fatalf("failed to serialize record: %v", err)
	}
	
	// Corrupt the data
	data := buf.Bytes()
	data[10] ^= 0xFF // Flip some bits
	
	// Try to deserialize
	_, err = DeserializeRecord(bytes.NewReader(data))
	if err == nil {
		t.Error("expected checksum error, got nil")
	}
}

func TestConcurrentWrites(t *testing.T) {
	tmpDir := t.TempDir()
	
	config := &Config{
		Directory:    tmpDir,
		BufferSize:   4096,
		SegmentSize:  1024 * 1024,
		SyncOnCommit: false,
	}

	manager, err := NewManager(config)
	if err != nil {
		t.Fatalf("failed to create WAL manager: %v", err)
	}
	defer manager.Close()

	// Run concurrent transactions
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(id int) {
			txnID := uint64(100 + id)
			
			manager.LogBeginTxn(txnID)
			for j := 0; j < 10; j++ {
				manager.LogInsert(txnID, int64(id), uint32(j), uint16(j), []byte("data"))
			}
			manager.LogCommitTxn(txnID)
			
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify LSNs are sequential
	currentLSN := manager.GetCurrentLSN()
	if currentLSN < 100 { // At least 10 txns * 10 inserts
		t.Errorf("expected at least 100 records, got LSN %d", currentLSN)
	}
}