package wal

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/dshills/QuantaDB/internal/storage"
)

func TestCheckpointManager(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// Create disk manager and buffer pool
	dm, err := storage.NewDiskManager(dbPath)
	if err != nil {
		t.Fatalf("failed to create disk manager: %v", err)
	}
	defer dm.Close()

	bufferPool := storage.NewBufferPool(dm, 10)

	// Create WAL manager
	walConfig := &Config{
		Directory:    tmpDir,
		BufferSize:   1024,
		SegmentSize:  4096,
		SyncOnCommit: true,
	}

	walManager, err := NewManager(walConfig)
	if err != nil {
		t.Fatalf("failed to create WAL manager: %v", err)
	}
	defer walManager.Close()

	// Create checkpoint manager
	checkpointConfig := &CheckpointConfig{
		Interval:   100 * time.Millisecond, // Short interval for testing
		MinRecords: 5,
	}

	checkpointMgr := NewCheckpointManager(walManager, bufferPool, checkpointConfig)

	// Log some transactions
	txn1 := uint64(100)
	walManager.LogBeginTxn(txn1)
	walManager.LogInsert(txn1, 1, 10, 1, []byte("row1"))
	walManager.LogCommitTxn(txn1)

	txn2 := uint64(200)
	walManager.LogBeginTxn(txn2)
	walManager.LogInsert(txn2, 1, 10, 2, []byte("row2"))
	// Leave txn2 uncommitted

	// Manual checkpoint
	err = checkpointMgr.Checkpoint()
	if err != nil {
		t.Fatalf("checkpoint failed: %v", err)
	}

	// Verify checkpoint was logged
	lastCheckpointLSN := checkpointMgr.GetLastCheckpointLSN()
	if lastCheckpointLSN == InvalidLSN {
		t.Error("expected valid checkpoint LSN")
	}

	// Verify stats
	stats := checkpointMgr.GetStats()
	if stats.LastCheckpointLSN != lastCheckpointLSN {
		t.Errorf("stats LSN mismatch: got %d, want %d", stats.LastCheckpointLSN, lastCheckpointLSN)
	}
	if stats.RecordsSinceCheckpoint != 0 {
		t.Errorf("expected 0 records since checkpoint, got %d", stats.RecordsSinceCheckpoint)
	}
}

func TestCheckpointRecovery(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// Create disk manager and buffer pool
	dm, err := storage.NewDiskManager(dbPath)
	if err != nil {
		t.Fatalf("failed to create disk manager: %v", err)
	}
	defer dm.Close()

	bufferPool := storage.NewBufferPool(dm, 10)

	var checkpointLSN LSN

	// Phase 1: Create WAL with checkpoint
	{
		walConfig := &Config{
			Directory:    tmpDir,
			BufferSize:   1024,
			SegmentSize:  4096,
			SyncOnCommit: true,
		}

		walManager, err := NewManager(walConfig)
		if err != nil {
			t.Fatalf("failed to create WAL manager: %v", err)
		}

		// Records before checkpoint
		walManager.LogBeginTxn(100)
		walManager.LogInsert(100, 1, 10, 1, []byte("before-checkpoint"))
		walManager.LogCommitTxn(100)

		// Create checkpoint
		checkpoint := &CheckpointRecord{
			Timestamp:  time.Now(),
			LastLSN:    walManager.GetCurrentLSN(),
			ActiveTxns: []uint64{200}, // One active transaction
			DirtyPages: []DirtyPageInfo{
				{PageID: 10, TableID: 1, RecLSN: 2},
			},
		}

		checkpointLSN, err = walManager.LogCheckpoint(checkpoint)
		if err != nil {
			t.Fatalf("failed to log checkpoint: %v", err)
		}

		// Records after checkpoint
		walManager.LogBeginTxn(300)
		walManager.LogInsert(300, 1, 10, 2, []byte("after-checkpoint"))
		walManager.LogCommitTxn(300)

		walManager.Close()
	}

	// Phase 2: Recovery should find checkpoint
	{
		recoveryMgr := NewRecoveryManager(tmpDir, bufferPool)

		checkpointFound := false
		recoveryMgr.SetCallbacks(RecoveryCallbacks{
			OnCheckpoint: func(lsn LSN) error {
				checkpointFound = true
				if lsn != checkpointLSN {
					t.Errorf("checkpoint LSN mismatch: got %d, want %d", lsn, checkpointLSN)
				}
				return nil
			},
			OnInsert: func(txnID uint64, tableID int64, pageID uint32, slotID uint16, rowData []byte) error {
				// Track inserts
				return nil
			},
			OnBeginTxn: func(txnID uint64) error {
				return nil
			},
			OnCommitTxn: func(txnID uint64) error {
				return nil
			},
		})

		err := recoveryMgr.Recover()
		if err != nil {
			t.Fatalf("recovery failed: %v", err)
		}

		if !checkpointFound {
			t.Error("checkpoint not found during recovery")
		}

		// In a real implementation, recovery could start from checkpoint
		// instead of replaying all records
		stats := recoveryMgr.GetRecoveryStats()
		if stats.CheckpointLSN != checkpointLSN {
			t.Errorf("recovery checkpoint LSN mismatch: got %d, want %d",
				stats.CheckpointLSN, checkpointLSN)
		}
	}
}

func TestPeriodicCheckpoints(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// Create disk manager and buffer pool
	dm, err := storage.NewDiskManager(dbPath)
	if err != nil {
		t.Fatalf("failed to create disk manager: %v", err)
	}
	defer dm.Close()

	bufferPool := storage.NewBufferPool(dm, 10)

	// Create WAL manager
	walConfig := &Config{
		Directory:    tmpDir,
		BufferSize:   1024,
		SegmentSize:  8192,
		SyncOnCommit: false,
	}

	walManager, err := NewManager(walConfig)
	if err != nil {
		t.Fatalf("failed to create WAL manager: %v", err)
	}
	defer walManager.Close()

	// Create checkpoint manager with short interval
	checkpointConfig := &CheckpointConfig{
		Interval:   200 * time.Millisecond,
		MinRecords: 3,
	}

	checkpointMgr := NewCheckpointManager(walManager, bufferPool, checkpointConfig)

	// Start periodic checkpoints
	checkpointMgr.Start()
	defer checkpointMgr.Stop()

	// Generate enough records to trigger checkpoint
	for i := 0; i < 10; i++ {
		txnID := uint64(100 + i)
		walManager.LogBeginTxn(txnID)
		walManager.LogInsert(txnID, 1, uint32(i), uint16(i), []byte("data"))
		walManager.LogCommitTxn(txnID)
		checkpointMgr.IncrementRecordCount()
		checkpointMgr.IncrementRecordCount()
		checkpointMgr.IncrementRecordCount() // 3 records per transaction
	}

	// Wait for periodic checkpoint
	time.Sleep(300 * time.Millisecond)

	// Check that checkpoint occurred
	stats := checkpointMgr.GetStats()
	if stats.LastCheckpointLSN == InvalidLSN {
		t.Error("expected checkpoint to have occurred")
	}

	// Records since checkpoint should be reset
	if stats.RecordsSinceCheckpoint >= checkpointMgr.minRecords {
		t.Errorf("expected records since checkpoint < %d, got %d",
			checkpointMgr.minRecords, stats.RecordsSinceCheckpoint)
	}
}
