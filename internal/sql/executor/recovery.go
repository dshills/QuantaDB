package executor

import (
	"fmt"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/storage"
	"github.com/dshills/QuantaDB/internal/wal"
)

// RecoverDatabase performs crash recovery using WAL
func RecoverDatabase(walDir string, bufferPool *storage.BufferPool, cat catalog.Catalog) error {
	recoveryMgr := wal.NewRecoveryManager(walDir, bufferPool)
	
	// Create a temporary storage backend for recovery operations
	// storageBackend := NewDiskStorageBackend(bufferPool, cat)
	// Note: In a full implementation, we'd use the storage backend to apply recovery operations
	
	// Set up recovery callbacks
	recoveryMgr.SetCallbacks(wal.RecoveryCallbacks{
		OnInsert: func(txnID uint64, tableID int64, pageID uint32, slotID uint16, rowData []byte) error {
			// During recovery, we need to reapply the insert
			page, err := bufferPool.FetchPage(storage.PageID(pageID))
			if err != nil {
				// Page doesn't exist, might need to allocate it
				return fmt.Errorf("page %d not found during recovery: %w", pageID, err)
			}
			defer bufferPool.UnpinPage(storage.PageID(pageID), true)
			
			// Note: In a complete implementation, we would need the record's LSN
			// to properly check if this change has already been applied.
			// For now, we'll apply all changes during recovery.
			
			// Reinsert the data at the specific slot
			// This is simplified - in reality we'd need to handle the page format properly
			slotOffset := storage.PageHeaderSize + slotID*4
			
			// Ensure slot offset is within bounds
			if int(slotOffset)+4 > len(page.Data)+storage.PageHeaderSize {
				return fmt.Errorf("slot offset out of bounds during recovery")
			}
			
			// Get the slot data correctly
			slotData := page.Data[slotOffset : slotOffset+4]
			
			// Calculate data position
			dataSize := uint16(len(rowData))
			// FreeSpacePtr is relative to page start, we need offset in Data array
			dataOffset := page.Header.FreeSpacePtr - dataSize - storage.PageHeaderSize
			pageDataOffset := dataOffset + storage.PageHeaderSize
			
			// Write slot entry
			slotData[0] = byte(pageDataOffset >> 8)
			slotData[1] = byte(pageDataOffset)
			slotData[2] = byte(dataSize >> 8)
			slotData[3] = byte(dataSize)
			
			// Write data
			if int(dataOffset) < 0 || int(dataOffset)+int(dataSize) > len(page.Data) {
				return fmt.Errorf("invalid data offset during recovery")
			}
			copy(page.Data[dataOffset:], rowData)
			
			// Update page header if this is a new slot
			if slotID >= page.Header.ItemCount {
				page.Header.ItemCount = slotID + 1
			}
			page.Header.FreeSpacePtr = pageDataOffset
			page.Header.FreeSpace -= dataSize + 4
			
			return nil
		},
		
		OnDelete: func(txnID uint64, tableID int64, pageID uint32, slotID uint16) error {
			// During recovery, reapply the delete
			page, err := bufferPool.FetchPage(storage.PageID(pageID))
			if err != nil {
				return fmt.Errorf("page %d not found during recovery: %w", pageID, err)
			}
			defer bufferPool.UnpinPage(storage.PageID(pageID), true)
			
			// Mark slot as deleted
			slotOffset := storage.PageHeaderSize + slotID*4
			
			// Ensure slot offset is within bounds
			if int(slotOffset)+4 > len(page.Data)+storage.PageHeaderSize {
				return fmt.Errorf("slot offset out of bounds during recovery")
			}
			
			// Get the slot data correctly
			slotData := page.Data[slotOffset : slotOffset+4]
			slotData[2] = 0
			slotData[3] = 0
			
			return nil
		},
		
		OnUpdate: func(txnID uint64, tableID int64, pageID uint32, slotID uint16, oldData, newData []byte) error {
			// Updates in our MVCC model are insert + delete
			// The update record type isn't used in our current implementation
			return nil
		},
		
		OnBeginTxn: func(txnID uint64) error {
			// Track transaction state during recovery
			fmt.Printf("Recovery: Transaction %d began\n", txnID)
			return nil
		},
		
		OnCommitTxn: func(txnID uint64) error {
			// Mark transaction as committed
			fmt.Printf("Recovery: Transaction %d committed\n", txnID)
			return nil
		},
		
		OnAbortTxn: func(txnID uint64) error {
			// Handle aborted transaction
			fmt.Printf("Recovery: Transaction %d aborted\n", txnID)
			return nil
		},
		
		OnCheckpoint: func(lsn wal.LSN) error {
			// Handle checkpoint record
			fmt.Printf("Recovery: Checkpoint at LSN %d\n", lsn)
			return nil
		},
	})
	
	// Perform recovery
	fmt.Println("Starting database recovery...")
	if err := recoveryMgr.Recover(); err != nil {
		return fmt.Errorf("recovery failed: %w", err)
	}
	
	stats := recoveryMgr.GetRecoveryStats()
	fmt.Printf("Recovery completed. RedoLSN: %d, Active transactions: %d\n", 
		stats.RedoLSN, stats.ActiveTxnCount)
	
	return nil
}