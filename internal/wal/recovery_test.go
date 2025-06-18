package wal

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/dshills/QuantaDB/internal/storage"
)

// MockRecoveryCallbacks tracks which records were replayed
type MockRecoveryCallbacks struct {
	InsertCount  int
	DeleteCount  int
	UpdateCount  int
	BeginCount   int
	CommitCount  int
	AbortCount   int
	
	// Track specific operations for verification
	Operations []string
}

func (m *MockRecoveryCallbacks) OnInsert(txnID uint64, tableID int64, pageID uint32, slotID uint16, rowData []byte) error {
	m.InsertCount++
	m.Operations = append(m.Operations, fmt.Sprintf("INSERT: txn=%d, table=%d, page=%d, slot=%d", 
		txnID, tableID, pageID, slotID))
	return nil
}

func (m *MockRecoveryCallbacks) OnDelete(txnID uint64, tableID int64, pageID uint32, slotID uint16) error {
	m.DeleteCount++
	m.Operations = append(m.Operations, fmt.Sprintf("DELETE: txn=%d, table=%d, page=%d, slot=%d",
		txnID, tableID, pageID, slotID))
	return nil
}

func (m *MockRecoveryCallbacks) OnUpdate(txnID uint64, tableID int64, pageID uint32, slotID uint16, oldData, newData []byte) error {
	m.UpdateCount++
	m.Operations = append(m.Operations, fmt.Sprintf("UPDATE: txn=%d, table=%d, page=%d, slot=%d",
		txnID, tableID, pageID, slotID))
	return nil
}

func (m *MockRecoveryCallbacks) OnBeginTxn(txnID uint64) error {
	m.BeginCount++
	m.Operations = append(m.Operations, fmt.Sprintf("BEGIN: txn=%d", txnID))
	return nil
}

func (m *MockRecoveryCallbacks) OnCommitTxn(txnID uint64) error {
	m.CommitCount++
	m.Operations = append(m.Operations, fmt.Sprintf("COMMIT: txn=%d", txnID))
	return nil
}

func (m *MockRecoveryCallbacks) OnAbortTxn(txnID uint64) error {
	m.AbortCount++
	m.Operations = append(m.Operations, fmt.Sprintf("ABORT: txn=%d", txnID))
	return nil
}

func TestRecoveryManager(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	
	// Create disk manager and buffer pool
	dm, err := storage.NewDiskManager(dbPath)
	if err != nil {
		t.Fatalf("failed to create disk manager: %v", err)
	}
	defer dm.Close()
	
	bufferPool := storage.NewBufferPool(dm, 10)
	
	// Phase 1: Create some WAL records
	{
		config := &Config{
			Directory:    tmpDir,
			BufferSize:   1024,
			SegmentSize:  4096,
			SyncOnCommit: true,
		}
		
		manager, err := NewManager(config)
		if err != nil {
			t.Fatalf("failed to create WAL manager: %v", err)
		}
		
		// Committed transaction
		txn1 := uint64(100)
		manager.LogBeginTxn(txn1)
		manager.LogInsert(txn1, 1, 10, 1, []byte("row1"))
		manager.LogInsert(txn1, 1, 10, 2, []byte("row2"))
		manager.LogCommitTxn(txn1)
		
		// Uncommitted transaction
		txn2 := uint64(200)
		manager.LogBeginTxn(txn2)
		manager.LogInsert(txn2, 2, 20, 1, []byte("row3"))
		// No commit - simulating crash
		
		manager.Close()
	}
	
	// Phase 2: Recovery
	{
		recoveryMgr := NewRecoveryManager(tmpDir, bufferPool)
		
		mockCallbacks := &MockRecoveryCallbacks{
			Operations: make([]string, 0),
		}
		
		recoveryMgr.SetCallbacks(RecoveryCallbacks{
			OnInsert: mockCallbacks.OnInsert,
			OnDelete: mockCallbacks.OnDelete,
			OnUpdate: mockCallbacks.OnUpdate,
			OnBeginTxn: mockCallbacks.OnBeginTxn,
			OnCommitTxn: mockCallbacks.OnCommitTxn,
			OnAbortTxn: mockCallbacks.OnAbortTxn,
		})
		
		// Perform recovery
		err := recoveryMgr.Recover()
		if err != nil {
			t.Fatalf("recovery failed: %v", err)
		}
		
		// Verify recovery stats
		stats := recoveryMgr.GetRecoveryStats()
		if stats.ActiveTxnCount != 1 {
			t.Errorf("expected 1 active transaction, got %d", stats.ActiveTxnCount)
		}
		
		// Verify operations were replayed
		if mockCallbacks.BeginCount != 2 {
			t.Errorf("expected 2 begin operations, got %d", mockCallbacks.BeginCount)
		}
		if mockCallbacks.InsertCount != 3 {
			t.Errorf("expected 3 insert operations, got %d", mockCallbacks.InsertCount)
		}
		if mockCallbacks.CommitCount != 1 {
			t.Errorf("expected 1 commit operation, got %d", mockCallbacks.CommitCount)
		}
		
		// Verify operation order
		expectedOps := []string{
			"BEGIN: txn=100",
			"INSERT: txn=100, table=1, page=10, slot=1",
			"INSERT: txn=100, table=1, page=10, slot=2",
			"COMMIT: txn=100",
			"BEGIN: txn=200",
			"INSERT: txn=200, table=2, page=20, slot=1",
		}
		
		if len(mockCallbacks.Operations) != len(expectedOps) {
			t.Errorf("expected %d operations, got %d", len(expectedOps), len(mockCallbacks.Operations))
		}
		
		for i, op := range expectedOps {
			if i < len(mockCallbacks.Operations) && mockCallbacks.Operations[i] != op {
				t.Errorf("operation %d: expected %s, got %s", i, op, mockCallbacks.Operations[i])
			}
		}
	}
}

func TestRecoveryWithPageLSN(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	
	// Create disk manager and buffer pool
	dm, err := storage.NewDiskManager(dbPath)
	if err != nil {
		t.Fatalf("failed to create disk manager: %v", err)
	}
	defer dm.Close()
	
	bufferPool := storage.NewBufferPool(dm, 10)
	
	// Create a page with LSN
	page, err := bufferPool.NewPage()
	if err != nil {
		t.Fatalf("failed to create page: %v", err)
	}
	pageID := page.Header.PageID
	page.Header.LSN = 3 // Simulate that LSN 1-3 have been applied
	bufferPool.UnpinPage(pageID, true)
	
	// Create WAL with records both before and after page LSN
	{
		config := &Config{
			Directory:    tmpDir,
			BufferSize:   1024,
			SegmentSize:  4096,
			SyncOnCommit: true,
		}
		
		manager, err := NewManager(config)
		if err != nil {
			t.Fatalf("failed to create WAL manager: %v", err)
		}
		
		txn := uint64(100)
		
		// These should be skipped (LSN <= page LSN)
		manager.currentLSN = 0
		manager.LogBeginTxn(txn)
		manager.LogInsert(txn, 1, uint32(pageID), 1, []byte("old1"))
		manager.LogInsert(txn, 1, uint32(pageID), 2, []byte("old2"))
		
		// These should be replayed (LSN > page LSN)
		manager.LogInsert(txn, 1, uint32(pageID), 3, []byte("new1"))
		manager.LogInsert(txn, 1, uint32(pageID), 4, []byte("new2"))
		manager.LogCommitTxn(txn)
		
		manager.Close()
	}
	
	// Recovery
	{
		recoveryMgr := NewRecoveryManager(tmpDir, bufferPool)
		
		mockCallbacks := &MockRecoveryCallbacks{
			Operations: make([]string, 0),
		}
		
		recoveryMgr.SetCallbacks(RecoveryCallbacks{
			OnInsert: mockCallbacks.OnInsert,
			OnBeginTxn: mockCallbacks.OnBeginTxn,
			OnCommitTxn: mockCallbacks.OnCommitTxn,
		})
		
		err := recoveryMgr.Recover()
		if err != nil {
			t.Fatalf("recovery failed: %v", err)
		}
		
		// Should replay begin, 2 inserts (LSN 4 and 5), and commit
		// The first 2 inserts (LSN 2 and 3) should be skipped
		if mockCallbacks.InsertCount != 2 {
			t.Errorf("expected 2 insert operations to be replayed, got %d", mockCallbacks.InsertCount)
		}
		
		// Verify the correct inserts were replayed
		expectedInserts := []string{
			fmt.Sprintf("INSERT: txn=100, table=1, page=%d, slot=3", pageID),
			fmt.Sprintf("INSERT: txn=100, table=1, page=%d, slot=4", pageID),
		}
		
		insertOps := []string{}
		for _, op := range mockCallbacks.Operations {
			if len(op) > 6 && op[:6] == "INSERT" {
				insertOps = append(insertOps, op)
			}
		}
		
		for i, expected := range expectedInserts {
			if i < len(insertOps) && insertOps[i] != expected {
				t.Errorf("insert %d: expected %s, got %s", i, expected, insertOps[i])
			}
		}
	}
}