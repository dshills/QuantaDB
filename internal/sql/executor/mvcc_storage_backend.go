package executor

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/storage"
	"github.com/dshills/QuantaDB/internal/txn"
	"github.com/dshills/QuantaDB/internal/wal"
)

// MVCCStorageBackend extends DiskStorageBackend with MVCC support
type MVCCStorageBackend struct {
	*DiskStorageBackend
	txnManager *txn.Manager
	pageMutex  sync.RWMutex // Protects against concurrent page modifications
}

// SetTransactionID atomically sets the current transaction ID
func (m *MVCCStorageBackend) SetTransactionID(txnID uint64) {
	atomic.StoreUint64(&m.currentTxnID, txnID)
}

// NewMVCCStorageBackend creates a new MVCC-aware storage backend
func NewMVCCStorageBackend(bufferPool *storage.BufferPool, catalog catalog.Catalog, walManager *wal.Manager, txnManager *txn.Manager) *MVCCStorageBackend {
	return &MVCCStorageBackend{
		DiskStorageBackend: NewDiskStorageBackendWithWAL(bufferPool, catalog, walManager),
		txnManager:         txnManager,
	}
}

// CreateTable creates storage for a new table with MVCC support
func (m *MVCCStorageBackend) CreateTable(table *catalog.Table) error {
	// Use the parent's CreateTable implementation
	// The only difference would be using MVCC format in operations,
	// which we handle in InsertRow/UpdateRow/etc.
	return m.DiskStorageBackend.CreateTable(table)
}

// InsertRow inserts a row with MVCC metadata
func (m *MVCCStorageBackend) InsertRow(tableID int64, row *Row) (RowID, error) {
	m.mu.RLock()
	meta, exists := m.tableMeta[tableID]
	if !exists {
		m.mu.RUnlock()
		return RowID{}, NewTableNotFoundError(tableID)
	}
	schema := meta.RowFormat.Schema
	m.mu.RUnlock()

	// Get the next logical timestamp
	// This ensures consistency with the transaction system's timestamps
	currentTimestamp := int64(txn.NextTimestamp()) //nolint:gosec // txn.NextTimestamp() returns sequential safe values

	// Create MVCC row with transaction metadata
	currentTxnID := atomic.LoadUint64(&m.currentTxnID)
	mvccRow := &MVCCRow{
		Header: MVCCRowHeader{
			CreatedByTxn: int64(currentTxnID), //nolint:gosec // Transaction IDs are controlled internally
			CreatedAt:    currentTimestamp,
			DeletedByTxn: 0,
			DeletedAt:    0,
			NextVersion:  0,
		},
		Data: row,
	}

	// Serialize with MVCC format
	mvccFormat := NewMVCCRowFormat(schema)
	rowData, err := mvccFormat.Serialize(mvccRow)
	if err != nil {
		return RowID{}, fmt.Errorf("failed to serialize row: %w", err)
	}

	// Protect the entire page selection and modification process
	m.pageMutex.Lock()
	defer m.pageMutex.Unlock()

	// Re-read metadata inside the critical section to ensure consistency
	m.mu.RLock()
	currentMeta, exists := m.tableMeta[tableID]
	if !exists {
		m.mu.RUnlock()
		return RowID{}, NewTableNotFoundError(tableID)
	}
	currentLastPageID := currentMeta.LastPageID
	m.mu.RUnlock()

	// Find a page with enough space
	pageID := currentLastPageID
	page, err := m.bufferPool.FetchPage(pageID)
	if err != nil {
		return RowID{}, fmt.Errorf("failed to fetch page: %w", err)
	}

	// Check if we have enough space
	dataLen := len(rowData)
	if dataLen > 65531 { // max uint16 - 4 for slot entry
		return RowID{}, fmt.Errorf("row data too large: %d bytes", dataLen)
	}
	requiredSpace := uint16(dataLen) + 4 // data + slot entry
	if page.Header.FreeSpace < requiredSpace {
		// Need to allocate a new page - use helper method
		newPageID, newPage, err := m.allocateNewDataPage(pageID, page)
		if err != nil {
			return RowID{}, err
		}

		// Update metadata safely
		m.mu.Lock()
		if currentMeta, exists := m.tableMeta[tableID]; exists {
			currentMeta.LastPageID = newPageID
		}
		m.mu.Unlock()

		pageID = newPageID
		page = newPage
	}

	// Insert row into page
	slotID := m.insertIntoPage(page, rowData)

	// Log the insert if WAL is enabled
	if m.walManager != nil {
		lsn, err := m.walManager.LogInsert(currentTxnID, tableID, uint32(pageID), slotID, rowData)
		if err != nil {
			m.bufferPool.UnpinPage(pageID, false)
			return RowID{}, fmt.Errorf("failed to log insert: %w", err)
		}
		page.Header.LSN = uint64(lsn)
	}

	m.bufferPool.UnpinPage(pageID, true)

	// Update row count
	m.mu.Lock()
	if currentMeta, exists := m.tableMeta[tableID]; exists {
		currentMeta.RowCount++
	}
	m.mu.Unlock()

	// Set RowID in header for future reference
	rowID := RowID{PageID: pageID, SlotID: slotID}
	mvccRow.Header.RowID = EncodeVersionPointer(uint32(pageID), slotID)

	return rowID, nil
}

// UpdateRow creates a new version of the row with proper version chain management
// The update is done in-place by replacing the old row data and creating a version chain link
func (m *MVCCStorageBackend) UpdateRow(tableID int64, rowID RowID, row *Row) error {
	// Get the current row (will become the old version)
	currentRow, err := m.GetMVCCRow(tableID, rowID)
	if err != nil {
		return fmt.Errorf("failed to get current row: %w", err)
	}

	// Get current timestamp and transaction ID
	currentTimestamp := int64(txn.NextTimestamp()) //nolint:gosec // txn.NextTimestamp() returns sequential safe values
	currentTxnID := atomic.LoadUint64(&m.currentTxnID)

	// First, create a backup of the old version if there isn't one already
	if currentRow.Header.NextVersion == 0 {
		// This is the first update, so create a backup of the original
		oldVersionBackup := &MVCCRow{
			Header: MVCCRowHeader{
				CreatedByTxn: currentRow.Header.CreatedByTxn,
				CreatedAt:    currentRow.Header.CreatedAt,
				DeletedByTxn: int64(currentTxnID), //nolint:gosec // Mark as deleted by this transaction
				DeletedAt:    currentTimestamp,
				NextVersion:  0, // End of chain
			},
			Data: currentRow.Data, // Copy the old data
		}

		// Insert the old version backup
		oldVersionRowID, err := m.insertMVCCRow(tableID, oldVersionBackup)
		if err != nil {
			return fmt.Errorf("failed to create old version backup: %w", err)
		}

		// Update the current row with new data and link to the old version
		currentRow.Header.CreatedByTxn = int64(currentTxnID) //nolint:gosec
		currentRow.Header.CreatedAt = currentTimestamp
		currentRow.Header.DeletedByTxn = 0 // New version is not deleted
		currentRow.Header.DeletedAt = 0
		currentRow.Header.NextVersion = EncodeVersionPointer(uint32(oldVersionRowID.PageID), oldVersionRowID.SlotID)
		currentRow.Data = row // Replace with new data

		// Update the row in place
		if err := m.updateRowInPlace(tableID, rowID, currentRow); err != nil {
			// Rollback: try to delete the old version backup
			_ = m.physicalDeleteRow(tableID, oldVersionRowID)
			return fmt.Errorf("failed to update row in place: %w", err)
		}
	} else {
		// There's already a version chain, just update the current version
		currentRow.Header.CreatedByTxn = int64(currentTxnID) //nolint:gosec
		currentRow.Header.CreatedAt = currentTimestamp
		currentRow.Data = row // Replace with new data

		// Update the row in place
		if err := m.updateRowInPlace(tableID, rowID, currentRow); err != nil {
			return fmt.Errorf("failed to update row in place: %w", err)
		}
	}

	return nil
}

// DeleteRow marks a row as deleted with MVCC metadata
func (m *MVCCStorageBackend) DeleteRow(tableID int64, rowID RowID) error {
	// Get the row
	mvccRow, err := m.GetMVCCRow(tableID, rowID)
	if err != nil {
		return fmt.Errorf("failed to get row: %w", err)
	}

	// Mark as deleted
	currentTxnID := atomic.LoadUint64(&m.currentTxnID)
	mvccRow.Header.DeletedByTxn = int64(currentTxnID)     //nolint:gosec // Transaction IDs are controlled internally
	mvccRow.Header.DeletedAt = int64(txn.NextTimestamp()) //nolint:gosec // txn.NextTimestamp() returns sequential safe values

	// Update the row in place
	return m.updateRowInPlace(tableID, rowID, mvccRow)
}

// ScanTable returns an MVCC-aware iterator
func (m *MVCCStorageBackend) ScanTable(tableID int64, snapshotTS int64) (RowIterator, error) {
	// Get table metadata - copy needed data while holding lock
	m.mu.RLock()
	meta, exists := m.tableMeta[tableID]
	if !exists {
		m.mu.RUnlock()
		return nil, NewTableNotFoundError(tableID)
	}
	// Copy the data we need before releasing the lock
	firstPageID := meta.FirstPageID
	// Deep copy schema to avoid data races
	schemaCopy := &Schema{
		Columns: make([]Column, len(meta.RowFormat.Schema.Columns)),
	}
	copy(schemaCopy.Columns, meta.RowFormat.Schema.Columns)
	m.mu.RUnlock()

	// Create raw iterator
	rawIterator := NewDiskRawRowIterator(m.bufferPool, firstPageID)

	// Use atomic read for transaction ID
	currentTxnID := atomic.LoadUint64(&m.currentTxnID)

	// Use the provided snapshot timestamp for visibility checks
	// This is the key fix for MVCC isolation levels
	// Wrap with MVCC iterator
	return NewMVCCRawRowIterator(rawIterator, int64(currentTxnID), snapshotTS, schemaCopy), nil //nolint:gosec // Transaction IDs are controlled internally
}

// GetRow retrieves a specific row with MVCC visibility check using version chain traversal
func (m *MVCCStorageBackend) GetRow(tableID int64, rowID RowID, snapshotTS int64) (*Row, error) {
	// Create version chain iterator
	iterator := NewVersionChainIterator(m, tableID)

	// Find the visible version in the chain
	visibleRow, _, err := iterator.FindVisibleVersion(rowID, snapshotTS)
	if err != nil {
		return nil, fmt.Errorf("failed to traverse version chain: %w", err)
	}

	// If no visible version found, row doesn't exist for this snapshot
	if visibleRow == nil {
		return nil, ErrRowNotVisible
	}

	return visibleRow.Data, nil
}

// GetMVCCRow retrieves the MVCC row without visibility check
func (m *MVCCStorageBackend) GetMVCCRow(tableID int64, rowID RowID) (*MVCCRow, error) {
	m.mu.RLock()
	meta, exists := m.tableMeta[tableID]
	if !exists {
		m.mu.RUnlock()
		return nil, NewTableNotFoundError(tableID)
	}
	schema := meta.RowFormat.Schema
	m.mu.RUnlock()

	// Fetch the page
	page, err := m.bufferPool.FetchPage(rowID.PageID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch page: %w", err)
	}
	defer m.bufferPool.UnpinPage(rowID.PageID, false)

	// Read slot entry
	slotOffset := int(rowID.SlotID) * 4 // Offset within Data array

	// Bounds check for slot access
	if slotOffset+4 > len(page.Data) {
		return nil, fmt.Errorf("slot offset out of bounds: %d+4 > %d", slotOffset, len(page.Data))
	}

	slotData := page.Data[slotOffset : slotOffset+4]
	pageDataOffset := uint16(slotData[0])<<8 | uint16(slotData[1])
	dataSize := uint16(slotData[2])<<8 | uint16(slotData[3])

	if dataSize == 0 {
		return nil, ErrRowDeleted
	}

	// Read row data - check for underflow first
	if pageDataOffset < storage.PageHeaderSize {
		return nil, fmt.Errorf("invalid page data offset: %d < header size %d", pageDataOffset, storage.PageHeaderSize)
	}
	dataOffset := pageDataOffset - storage.PageHeaderSize

	// Bounds check for data access
	if int(dataOffset) >= len(page.Data) {
		return nil, fmt.Errorf("data offset out of bounds: %d not in [0, %d)", dataOffset, len(page.Data))
	}
	if int(dataOffset)+int(dataSize) > len(page.Data) {
		return nil, fmt.Errorf("data read would exceed page bounds: %d+%d > %d", dataOffset, dataSize, len(page.Data))
	}

	rowData := page.Data[dataOffset : dataOffset+dataSize]

	// Deserialize as MVCC row
	mvccFormat := NewMVCCRowFormat(schema)
	return mvccFormat.Deserialize(rowData)
}

// updateRowInPlace updates an MVCC row in its current location
func (m *MVCCStorageBackend) updateRowInPlace(tableID int64, rowID RowID, mvccRow *MVCCRow) error {
	m.mu.RLock()
	meta, exists := m.tableMeta[tableID]
	if !exists {
		m.mu.RUnlock()
		return fmt.Errorf("table %d not found", tableID)
	}
	schema := meta.RowFormat.Schema
	m.mu.RUnlock()

	// Serialize the updated row
	mvccFormat := NewMVCCRowFormat(schema)
	rowData, err := mvccFormat.Serialize(mvccRow)
	if err != nil {
		return fmt.Errorf("failed to serialize row: %w", err)
	}

	// Fetch the page
	page, err := m.bufferPool.FetchPage(rowID.PageID)
	if err != nil {
		return fmt.Errorf("failed to fetch page: %w", err)
	}
	defer m.bufferPool.UnpinPage(rowID.PageID, true)

	// Acquire page lock before modifying
	m.bufferPool.AcquirePageLock(rowID.PageID)
	defer m.bufferPool.ReleasePageLock(rowID.PageID)

	// Read current slot to get size
	slotOffset := int(rowID.SlotID) * 4 // Offset within Data array
	slotData := page.Data[slotOffset : slotOffset+4]
	pageDataOffset := uint16(slotData[0])<<8 | uint16(slotData[1])
	currentSize := uint16(slotData[2])<<8 | uint16(slotData[3])

	dataLen := len(rowData)
	if dataLen > int(currentSize) {
		return ErrSizeMismatch
	}
	// Allow new data to be smaller or equal (MVCC headers can change size)
	// We'll pad with zeros if needed, but only update the slot size if smaller

	// Update the data
	dataOffset := pageDataOffset - storage.PageHeaderSize
	if dataLen < 0 || dataLen > 65535 {
		return fmt.Errorf("invalid data length: %d", dataLen)
	}
	dataLenU16 := uint16(dataLen)
	copy(page.Data[dataOffset:dataOffset+dataLenU16], rowData)

	// Update slot size if new data is smaller
	if dataLenU16 < currentSize {
		// Update the slot entry with new size
		slotData[2] = byte(dataLenU16 >> 8)
		slotData[3] = byte(dataLenU16 & 0xFF)
	}

	// Log the update if WAL is enabled
	if m.walManager != nil {
		// For in-place updates, old and new data are the same (just metadata changed)
		currentTxnID := atomic.LoadUint64(&m.currentTxnID)
		lsn, err := m.walManager.LogUpdate(currentTxnID, tableID, uint32(rowID.PageID), rowID.SlotID, rowData, rowData)
		if err != nil {
			return fmt.Errorf("failed to log update: %w", err)
		}
		page.Header.LSN = uint64(lsn)
	}

	return nil
}

// SetCurrentTransaction sets the current transaction ID and timestamp for operations
// This is mainly used for testing
func (m *MVCCStorageBackend) SetCurrentTransaction(txnID txn.TransactionID, timestamp int64) {
	atomic.StoreUint64(&m.currentTxnID, uint64(txnID))
	atomic.StoreInt64(&m.currentTimestamp, timestamp)
}

// physicalDeleteRow physically removes a row (used for cleanup)
func (m *MVCCStorageBackend) physicalDeleteRow(tableID int64, rowID RowID) error {
	return m.DiskStorageBackend.DeleteRow(tableID, rowID)
}

// allocateNewDataPage allocates a new data page and links it to the current page
func (m *MVCCStorageBackend) allocateNewDataPage(currentPageID storage.PageID, currentPage *storage.Page) (storage.PageID, *storage.Page, error) {
	// Allocate new page first
	newPage, err := m.bufferPool.NewPage()
	if err != nil {
		return 0, nil, fmt.Errorf("failed to allocate new page: %w", err)
	}
	newPageID := newPage.Header.PageID

	// Initialize new page BEFORE linking to prevent race conditions
	newPage.Header.Type = storage.PageTypeData
	// NewPage already sets FreeSpacePtr = PageSize and FreeSpace correctly
	newPage.Header.NextPageID = storage.InvalidPageID

	// Acquire lock before modifying current page
	m.bufferPool.AcquirePageLock(currentPageID)
	// Link pages and mark current page as dirty
	currentPage.Header.NextPageID = newPageID
	m.bufferPool.ReleasePageLock(currentPageID)

	// Unpin current page after all modifications are complete
	m.bufferPool.UnpinPage(currentPageID, true)

	return newPageID, newPage, nil
}

// insertMVCCRow inserts an already-constructed MVCC row and returns its RowID
func (m *MVCCStorageBackend) insertMVCCRow(tableID int64, mvccRow *MVCCRow) (RowID, error) {
	// Check table exists and serialize row first
	m.mu.RLock()
	meta, exists := m.tableMeta[tableID]
	if !exists {
		m.mu.RUnlock()
		return RowID{}, NewTableNotFoundError(tableID)
	}
	schema := meta.RowFormat.Schema
	m.mu.RUnlock()

	// Serialize the MVCC row
	mvccFormat := NewMVCCRowFormat(schema)
	rowData, err := mvccFormat.Serialize(mvccRow)
	if err != nil {
		return RowID{}, fmt.Errorf("failed to serialize MVCC row: %w", err)
	}

	// Check size first
	dataLen := len(rowData)
	if dataLen > 65531 { // max uint16 - 4 for slot entry
		return RowID{}, fmt.Errorf("row data too large: %d bytes", dataLen)
	}

	// Now lock table metadata to get current last page
	m.mu.Lock()
	meta, exists = m.tableMeta[tableID]
	if !exists {
		m.mu.Unlock()
		return RowID{}, NewTableNotFoundError(tableID)
	}
	pageID := meta.LastPageID
	m.mu.Unlock()

	// Try to insert into current last page
	page, err := m.bufferPool.FetchPage(pageID)
	if err != nil {
		return RowID{}, fmt.Errorf("failed to fetch page: %w", err)
	}

	// Acquire page lock before checking space and modifying
	m.bufferPool.AcquirePageLock(pageID)

	// Re-check if page has space after acquiring lock (another thread might have filled it)
	if !page.HasSpaceFor(uint16(dataLen)) {
		// Need to allocate a new page
		// First release current page lock to avoid deadlock
		m.bufferPool.ReleasePageLock(pageID)

		// Allocate new page
		newPage, err := m.bufferPool.NewPage()
		if err != nil {
			m.bufferPool.UnpinPage(pageID, false)
			return RowID{}, fmt.Errorf("failed to allocate new page: %w", err)
		}
		newPageID := newPage.Header.PageID

		// Initialize new page
		newPage.Header.Type = storage.PageTypeData
		newPage.Header.NextPageID = storage.InvalidPageID

		// Re-acquire lock on old page to update link
		m.bufferPool.AcquirePageLock(pageID)
		page.Header.NextPageID = newPageID
		m.bufferPool.ReleasePageLock(pageID)
		m.bufferPool.UnpinPage(pageID, true)

		// Update metadata atomically - re-check if another thread already allocated
		m.mu.Lock()
		if currentMeta, exists := m.tableMeta[tableID]; exists {
			// Check if another thread already updated LastPageID
			if currentMeta.LastPageID != pageID {
				// Another thread already allocated a new page, use that instead
				m.mu.Unlock()
				m.bufferPool.UnpinPage(newPageID, false)
				// Retry with the new last page
				return m.insertMVCCRow(tableID, mvccRow)
			}
			// We're the first to allocate, update metadata
			currentMeta.LastPageID = newPageID
		}
		m.mu.Unlock()

		// Switch to new page
		pageID = newPageID
		page = newPage

		// Acquire lock for new page
		m.bufferPool.AcquirePageLock(pageID)
	}

	// Insert row into page - lock is already held
	slotID := m.insertIntoPage(page, rowData)

	// Log the insert if WAL is enabled
	if m.walManager != nil {
		currentTxnID := atomic.LoadUint64(&m.currentTxnID)
		lsn, err := m.walManager.LogInsert(currentTxnID, tableID, uint32(pageID), slotID, rowData)
		if err != nil {
			m.bufferPool.ReleasePageLock(pageID)
			m.bufferPool.UnpinPage(pageID, false)
			return RowID{}, fmt.Errorf("failed to log insert: %w", err)
		}
		page.Header.LSN = uint64(lsn)
	}

	// Release page lock before unpinning
	m.bufferPool.ReleasePageLock(pageID)
	m.bufferPool.UnpinPage(pageID, true)

	// Update row count
	m.mu.Lock()
	if currentMeta, exists := m.tableMeta[tableID]; exists {
		currentMeta.RowCount++
	}
	m.mu.Unlock()

	return RowID{PageID: pageID, SlotID: slotID}, nil
}
