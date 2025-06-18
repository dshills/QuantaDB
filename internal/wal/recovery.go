package wal

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/dshills/QuantaDB/internal/storage"
)

// RecoveryManager handles crash recovery by replaying WAL records
type RecoveryManager struct {
	walDir     string
	bufferPool *storage.BufferPool
	
	// Recovery state
	redoLSN    LSN // Minimum LSN that needs to be replayed
	checkpointLSN LSN // LSN of last checkpoint (if any)
	
	// Transaction tracking during recovery
	activeTxns map[uint64]bool // Transactions that were active at crash
	
	// Callbacks for applying different record types
	callbacks RecoveryCallbacks
}

// RecoveryCallbacks defines functions to apply different WAL record types
type RecoveryCallbacks struct {
	OnInsert func(txnID uint64, tableID int64, pageID uint32, slotID uint16, rowData []byte) error
	OnDelete func(txnID uint64, tableID int64, pageID uint32, slotID uint16) error
	OnUpdate func(txnID uint64, tableID int64, pageID uint32, slotID uint16, oldData, newData []byte) error
	OnBeginTxn func(txnID uint64) error
	OnCommitTxn func(txnID uint64) error
	OnAbortTxn func(txnID uint64) error
	OnCheckpoint func(lsn LSN) error
}

// NewRecoveryManager creates a new recovery manager
func NewRecoveryManager(walDir string, bufferPool *storage.BufferPool) *RecoveryManager {
	return &RecoveryManager{
		walDir:     walDir,
		bufferPool: bufferPool,
		activeTxns: make(map[uint64]bool),
		redoLSN:    InvalidLSN,
	}
}

// SetCallbacks sets the recovery callbacks
func (rm *RecoveryManager) SetCallbacks(callbacks RecoveryCallbacks) {
	rm.callbacks = callbacks
}

// Recover performs crash recovery by replaying WAL records
func (rm *RecoveryManager) Recover() error {
	// Phase 1: Analysis - Scan WAL to determine what needs to be recovered
	if err := rm.analysisPhase(); err != nil {
		return fmt.Errorf("analysis phase failed: %w", err)
	}
	
	// Phase 2: Redo - Replay all records from redoLSN
	if err := rm.redoPhase(); err != nil {
		return fmt.Errorf("redo phase failed: %w", err)
	}
	
	// Phase 3: Undo - Rollback uncommitted transactions
	// For now, we're using a simple approach where uncommitted changes
	// are just marked as invalid (no explicit undo)
	if err := rm.undoPhase(); err != nil {
		return fmt.Errorf("undo phase failed: %w", err)
	}
	
	return nil
}

// analysisPhase scans the WAL to determine recovery parameters
func (rm *RecoveryManager) analysisPhase() error {
	segments, err := rm.findWALSegments()
	if err != nil {
		return fmt.Errorf("failed to find WAL segments: %w", err)
	}
	
	if len(segments) == 0 {
		// No WAL files, nothing to recover
		return nil
	}
	
	// Start from the beginning of the oldest segment
	rm.redoLSN = 1
	
	// Scan all segments to find:
	// 1. Active transactions at time of crash
	// 2. Last checkpoint (if any)
	for _, segment := range segments {
		if err := rm.scanSegment(segment); err != nil {
			return fmt.Errorf("failed to scan segment %s: %w", segment, err)
		}
	}
	
	return nil
}

// redoPhase replays all records starting from redoLSN
func (rm *RecoveryManager) redoPhase() error {
	segments, err := rm.findWALSegments()
	if err != nil {
		return fmt.Errorf("failed to find WAL segments: %w", err)
	}
	
	for _, segment := range segments {
		if err := rm.replaySegment(segment); err != nil {
			return fmt.Errorf("failed to replay segment %s: %w", segment, err)
		}
	}
	
	return nil
}

// undoPhase handles uncommitted transactions
func (rm *RecoveryManager) undoPhase() error {
	// In a full MVCC implementation, we would:
	// 1. Identify all uncommitted transactions
	// 2. Roll back their changes in reverse order
	// 
	// For now, we just log which transactions were uncommitted
	if len(rm.activeTxns) > 0 {
		fmt.Printf("Found %d uncommitted transactions during recovery\n", len(rm.activeTxns))
		for txnID := range rm.activeTxns {
			fmt.Printf("  Transaction %d was not committed\n", txnID)
		}
	}
	
	return nil
}

// scanSegment scans a WAL segment during analysis phase
func (rm *RecoveryManager) scanSegment(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open segment: %w", err)
	}
	defer file.Close()
	
	for {
		record, err := DeserializeRecord(file)
		if err == io.EOF {
			break
		}
		if err != nil {
			// Incomplete record at end is expected
			break
		}
		
		// Track transaction state
		switch record.Type {
		case RecordTypeBeginTxn:
			rm.activeTxns[record.TxnID] = true
		case RecordTypeCommitTxn, RecordTypeAbortTxn:
			delete(rm.activeTxns, record.TxnID)
		case RecordTypeCheckpoint:
			rm.checkpointLSN = record.LSN
			// Could optimize redoLSN based on checkpoint
		}
	}
	
	return nil
}

// replaySegment replays records from a WAL segment
func (rm *RecoveryManager) replaySegment(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open segment: %w", err)
	}
	defer file.Close()
	
	for {
		record, err := DeserializeRecord(file)
		if err == io.EOF {
			break
		}
		if err != nil {
			// Incomplete record at end is expected
			break
		}
		
		// Skip records before redoLSN
		if record.LSN < rm.redoLSN {
			continue
		}
		
		// Check if page needs this update
		if !rm.shouldReplayRecord(record) {
			continue
		}
		
		// Apply the record
		if err := rm.applyRecord(record); err != nil {
			return fmt.Errorf("failed to apply record LSN %d: %w", record.LSN, err)
		}
	}
	
	return nil
}

// shouldReplayRecord determines if a record needs to be replayed
func (rm *RecoveryManager) shouldReplayRecord(record *LogRecord) bool {
	// For data modification records, check page LSN
	switch record.Type {
	case RecordTypeInsert, RecordTypeDelete, RecordTypeUpdate:
		// Parse the record to get page information
		var pageID uint32
		var needsReplay bool
		
		switch record.Type {
		case RecordTypeInsert:
			var insertRec InsertRecord
			if err := insertRec.Unmarshal(record.Data); err == nil {
				pageID = insertRec.PageID
			}
		case RecordTypeDelete:
			var deleteRec DeleteRecord
			if err := deleteRec.Unmarshal(record.Data); err == nil {
				pageID = deleteRec.PageID
			}
		case RecordTypeUpdate:
			var updateRec UpdateRecord
			if err := updateRec.Unmarshal(record.Data); err == nil {
				pageID = updateRec.PageID
			}
		}
		
		// Fetch page and check LSN
		if pageID != 0 {
			page, err := rm.bufferPool.FetchPage(storage.PageID(pageID))
			if err == nil {
				pageLSN := LSN(page.Header.LSN)
				needsReplay = record.LSN > pageLSN
				rm.bufferPool.UnpinPage(storage.PageID(pageID), false)
			} else {
				// If page doesn't exist or can't be read, replay the record
				needsReplay = true
			}
		}
		
		return needsReplay
	
	default:
		// Non-data records are always replayed
		return true
	}
}

// applyRecord applies a WAL record during recovery
func (rm *RecoveryManager) applyRecord(record *LogRecord) error {
	switch record.Type {
	case RecordTypeBeginTxn:
		if rm.callbacks.OnBeginTxn != nil {
			var txnRec TransactionRecord
			if err := txnRec.Unmarshal(record.Data); err != nil {
				return fmt.Errorf("failed to unmarshal begin txn record: %w", err)
			}
			return rm.callbacks.OnBeginTxn(txnRec.TxnID)
		}
		
	case RecordTypeCommitTxn:
		if rm.callbacks.OnCommitTxn != nil {
			var txnRec TransactionRecord
			if err := txnRec.Unmarshal(record.Data); err != nil {
				return fmt.Errorf("failed to unmarshal commit txn record: %w", err)
			}
			return rm.callbacks.OnCommitTxn(txnRec.TxnID)
		}
		
	case RecordTypeAbortTxn:
		if rm.callbacks.OnAbortTxn != nil {
			var txnRec TransactionRecord
			if err := txnRec.Unmarshal(record.Data); err != nil {
				return fmt.Errorf("failed to unmarshal abort txn record: %w", err)
			}
			return rm.callbacks.OnAbortTxn(txnRec.TxnID)
		}
		
	case RecordTypeInsert:
		if rm.callbacks.OnInsert != nil {
			var insertRec InsertRecord
			if err := insertRec.Unmarshal(record.Data); err != nil {
				return fmt.Errorf("failed to unmarshal insert record: %w", err)
			}
			return rm.callbacks.OnInsert(record.TxnID, insertRec.TableID, 
				insertRec.PageID, insertRec.SlotID, insertRec.RowData)
		}
		
	case RecordTypeDelete:
		if rm.callbacks.OnDelete != nil {
			var deleteRec DeleteRecord
			if err := deleteRec.Unmarshal(record.Data); err != nil {
				return fmt.Errorf("failed to unmarshal delete record: %w", err)
			}
			return rm.callbacks.OnDelete(record.TxnID, deleteRec.TableID,
				deleteRec.PageID, deleteRec.SlotID)
		}
		
	case RecordTypeUpdate:
		if rm.callbacks.OnUpdate != nil {
			var updateRec UpdateRecord
			if err := updateRec.Unmarshal(record.Data); err != nil {
				return fmt.Errorf("failed to unmarshal update record: %w", err)
			}
			return rm.callbacks.OnUpdate(record.TxnID, updateRec.TableID,
				updateRec.PageID, updateRec.SlotID, updateRec.OldRowData, updateRec.NewRowData)
		}
		
	case RecordTypeCheckpoint:
		if rm.callbacks.OnCheckpoint != nil {
			return rm.callbacks.OnCheckpoint(record.LSN)
		}
	}
	
	return nil
}

// findWALSegments returns all WAL segment files in order
func (rm *RecoveryManager) findWALSegments() ([]string, error) {
	pattern := filepath.Join(rm.walDir, "*.wal")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("failed to list WAL files: %w", err)
	}
	
	// Sort files by name (which includes segment number)
	sort.Strings(files)
	
	return files, nil
}

// GetRecoveryStats returns statistics about the recovery process
func (rm *RecoveryManager) GetRecoveryStats() RecoveryStats {
	return RecoveryStats{
		RedoLSN:          rm.redoLSN,
		CheckpointLSN:    rm.checkpointLSN,
		ActiveTxnCount:   len(rm.activeTxns),
	}
}

// RecoveryStats contains statistics about recovery
type RecoveryStats struct {
	RedoLSN        LSN
	CheckpointLSN  LSN
	ActiveTxnCount int
}