package executor

import (
	"fmt"

	"github.com/dshills/QuantaDB/internal/storage"
)

// RawRowIterator iterates over raw row data without deserialization
type RawRowIterator interface {
	// Next advances to the next row
	Next() bool
	// Value returns the current row's raw data and ID
	Value() ([]byte, RowID, error)
	// Close releases resources
	Close() error
}

// DiskRawRowIterator implements RawRowIterator for disk-based storage
type DiskRawRowIterator struct {
	bufferPool  *storage.BufferPool
	currentPage *storage.Page
	pageID      storage.PageID
	slotID      uint16
}

// NewDiskRawRowIterator creates a new raw row iterator
func NewDiskRawRowIterator(bufferPool *storage.BufferPool, firstPageID storage.PageID) *DiskRawRowIterator {
	return &DiskRawRowIterator{
		bufferPool: bufferPool,
		pageID:     firstPageID,
		slotID:     0,
	}
}

// Next advances to the next row
func (it *DiskRawRowIterator) Next() bool {
	for {
		// Fetch current page if needed
		if it.currentPage == nil {
			if it.pageID == storage.InvalidPageID {
				return false // EOF
			}

			page, err := it.bufferPool.FetchPage(it.pageID)
			if err != nil {
				return false
			}
			it.currentPage = page
			it.slotID = 0
		}

		// Check if we have more slots in current page
		if it.slotID >= it.currentPage.Header.ItemCount {
			// Move to next page
			nextPageID := it.currentPage.Header.NextPageID
			it.bufferPool.UnpinPage(it.pageID, false)
			it.currentPage = nil
			it.pageID = nextPageID
			continue
		}

		// Check if slot is valid (not deleted)
		slotOffset := int(it.slotID) * 4 // Offset within Data array

		// Bounds check for slot access
		if slotOffset+4 > len(it.currentPage.Data) {
			// Page is corrupted, move to next page
			it.slotID = it.currentPage.Header.ItemCount
			continue
		}

		slotData := it.currentPage.Data[slotOffset : slotOffset+4]
		dataSize := uint16(slotData[2])<<8 | uint16(slotData[3])

		if dataSize > 0 {
			// Don't increment slotID here - Value() will use current slotID
			return true // Found a valid row
		}

		it.slotID++
	}
}

// Value returns the current row's raw data and ID
func (it *DiskRawRowIterator) Value() ([]byte, RowID, error) {
	if it.currentPage == nil {
		return nil, RowID{}, fmt.Errorf("no current page")
	}

	// Read slot entry
	slotOffset := int(it.slotID) * 4 // Offset within Data array

	// Bounds check for slot access
	if slotOffset+4 > len(it.currentPage.Data) {
		return nil, RowID{}, fmt.Errorf("slot offset out of bounds: %d+4 > %d", slotOffset, len(it.currentPage.Data))
	}

	slotData := it.currentPage.Data[slotOffset : slotOffset+4]
	pageDataOffset := uint16(slotData[0])<<8 | uint16(slotData[1])
	dataSize := uint16(slotData[2])<<8 | uint16(slotData[3])

	if dataSize == 0 {
		return nil, RowID{}, fmt.Errorf("row is deleted")
	}

	// Read row data
	dataOffset := pageDataOffset - storage.PageHeaderSize

	// Bounds check for data access
	if int(dataOffset) >= len(it.currentPage.Data) {
		return nil, RowID{}, fmt.Errorf("data offset out of bounds: %d not in [0, %d) - pageDataOffset=%d, slot %d on page %d (bytes: %02x %02x %02x %02x)", 
			dataOffset, len(it.currentPage.Data), pageDataOffset, it.slotID, it.pageID, slotData[0], slotData[1], slotData[2], slotData[3])
	}
	if int(dataOffset)+int(dataSize) > len(it.currentPage.Data) {
		return nil, RowID{}, fmt.Errorf("data read would exceed page bounds: %d+%d > %d", dataOffset, dataSize, len(it.currentPage.Data))
	}

	rowData := make([]byte, dataSize)
	copy(rowData, it.currentPage.Data[dataOffset:dataOffset+dataSize])

	rowID := RowID{
		PageID: it.pageID,
		SlotID: it.slotID,
	}

	// Increment slotID for next iteration
	it.slotID++

	return rowData, rowID, nil
}

// Close releases resources
func (it *DiskRawRowIterator) Close() error {
	if it.currentPage != nil {
		it.bufferPool.UnpinPage(it.pageID, false)
		it.currentPage = nil
	}
	return nil
}

// MVCCRawRowIterator wraps a raw iterator with MVCC visibility checks
type MVCCRawRowIterator struct {
	baseIterator RawRowIterator
	txnID        int64
	timestamp    int64
	schema       *Schema
}

// NewMVCCRawRowIterator creates a new MVCC-aware raw row iterator
func NewMVCCRawRowIterator(base RawRowIterator, txnID, timestamp int64, schema *Schema) *MVCCRawRowIterator {
	return &MVCCRawRowIterator{
		baseIterator: base,
		txnID:        txnID,
		timestamp:    timestamp,
		schema:       schema,
	}
}

// Next advances to the next visible row
func (it *MVCCRawRowIterator) Next() (*Row, RowID, error) {
	mvccFormat := NewMVCCRowFormat(it.schema)
	legacyFormat := NewRowFormat(it.schema)

	errorCount := 0
	maxErrors := 10 // Prevent infinite loops on persistent errors

	for it.baseIterator.Next() {
		rowData, rowID, err := it.baseIterator.Value()
		if err != nil {
			errorCount++
			if errorCount >= maxErrors {
				// Too many consecutive errors, likely data corruption
				return nil, RowID{}, err
			}
			continue
		}

		// Reset error count on successful read
		errorCount = 0

		// Check if this is MVCC format by looking at version byte
		
		if len(rowData) > 0 && rowData[0] == 1 {
			// MVCC format
			mvccRow, err := mvccFormat.Deserialize(rowData)
			if err != nil {
				continue
			}

			// Check visibility
			if it.isVisible(mvccRow) {
				return mvccRow.Data, rowID, nil
			}
		} else {
			// Legacy format - always visible
			row, err := legacyFormat.Deserialize(rowData)
			if err != nil {
				continue
			}
			return row, rowID, nil
		}
	}

	return nil, RowID{}, nil // EOF
}

// Close closes the iterator
func (it *MVCCRawRowIterator) Close() error {
	return it.baseIterator.Close()
}

// isVisible checks if a row version is visible to the current transaction
func (it *MVCCRawRowIterator) isVisible(row *MVCCRow) bool {
	// Row is visible if:
	// 1. It was created by our transaction (always visible)
	// 2. It was created before our snapshot timestamp
	// 3. It hasn't been deleted, or was deleted after our snapshot

	// Always see our own writes
	if row.Header.CreatedByTxn == it.txnID {
		// Check if we deleted it ourselves
		if row.Header.DeletedByTxn == it.txnID {
			return false
		}
		return true
	}

	// Use timestamp-based visibility for other transactions
	if row.Header.CreatedAt > it.timestamp {
		return false // Created after our snapshot
	}

	if row.Header.DeletedAt > 0 && row.Header.DeletedAt <= it.timestamp {
		return false // Deleted before or at our snapshot
	}
	return true
}
