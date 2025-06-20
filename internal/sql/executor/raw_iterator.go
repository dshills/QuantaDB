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
		slotOffset := storage.PageHeaderSize + it.slotID*4
		slotData := it.currentPage.Data[slotOffset : slotOffset+4]
		dataSize := uint16(slotData[2])<<8 | uint16(slotData[3])

		if dataSize > 0 {
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
	slotOffset := storage.PageHeaderSize + it.slotID*4
	slotData := it.currentPage.Data[slotOffset : slotOffset+4]
	pageDataOffset := uint16(slotData[0])<<8 | uint16(slotData[1])
	dataSize := uint16(slotData[2])<<8 | uint16(slotData[3])

	if dataSize == 0 {
		return nil, RowID{}, fmt.Errorf("row is deleted")
	}

	// Read row data
	dataOffset := pageDataOffset - storage.PageHeaderSize
	rowData := make([]byte, dataSize)
	copy(rowData, it.currentPage.Data[dataOffset:dataOffset+dataSize])

	rowID := RowID{
		PageID: it.pageID,
		SlotID: it.slotID,
	}

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

	for it.baseIterator.Next() {
		rowData, rowID, err := it.baseIterator.Value()
		if err != nil {
			continue
		}

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
	// 1. It was created before our snapshot timestamp
	// 2. It hasn't been deleted, or was deleted after our snapshot

	// Use timestamp-based visibility for MVCC correctness
	if row.Header.CreatedAt > it.timestamp {
		return false // Created after our snapshot
	}

	if row.Header.DeletedAt > 0 && row.Header.DeletedAt <= it.timestamp {
		return false // Deleted before or at our snapshot
	}

	return true
}
