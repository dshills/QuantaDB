package executor

import (
	"fmt"
	"math"
	"sync"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/storage"
	"github.com/dshills/QuantaDB/internal/wal"
)

// StorageBackend provides an interface for table storage operations
type StorageBackend interface {
	// CreateTable creates storage for a new table
	CreateTable(table *catalog.Table) error

	// DropTable removes storage for a table
	DropTable(tableID int64) error

	// InsertRow inserts a row into a table
	InsertRow(tableID int64, row *Row) (RowID, error)

	// UpdateRow updates an existing row
	UpdateRow(tableID int64, rowID RowID, row *Row) error

	// DeleteRow deletes a row
	DeleteRow(tableID int64, rowID RowID) error

	// ScanTable returns an iterator for scanning a table
	ScanTable(tableID int64, snapshotTS int64) (RowIterator, error)

	// GetRow retrieves a specific row by ID
	GetRow(tableID int64, rowID RowID, snapshotTS int64) (*Row, error)

	// SetTransactionID sets the current transaction ID for operations
	SetTransactionID(txnID uint64)
}

// RowID uniquely identifies a row in storage
type RowID struct {
	PageID storage.PageID
	SlotID uint16
}

// RowIterator iterates over rows in storage
type RowIterator interface {
	// Next returns the next row and its ID
	Next() (*Row, RowID, error)
	// Close releases resources
	Close() error
}

// DiskStorageBackend implements StorageBackend using the page-based storage engine
type DiskStorageBackend struct {
	bufferPool *storage.BufferPool
	catalog    catalog.Catalog
	walManager *wal.Manager

	// Table metadata mapping
	mu         sync.RWMutex
	tablePages map[int64]storage.PageID // Table ID -> First page ID
	tableMeta  map[int64]*TableMetadata

	// Current transaction ID
	// TODO: In a production system, this should be passed as a parameter to each operation
	// or managed via context.Context to ensure thread safety for concurrent transactions
	currentTxnID uint64

	// Current timestamp for MVCC operations
	currentTimestamp int64
}

// TableMetadata stores metadata about a table's storage
type TableMetadata struct {
	TableID     int64
	FirstPageID storage.PageID
	LastPageID  storage.PageID
	RowCount    uint64
	RowFormat   *RowFormat
}

// NewDiskStorageBackend creates a new disk-based storage backend
func NewDiskStorageBackend(bufferPool *storage.BufferPool, catalog catalog.Catalog) *DiskStorageBackend {
	return &DiskStorageBackend{
		bufferPool: bufferPool,
		catalog:    catalog,
		tablePages: make(map[int64]storage.PageID),
		tableMeta:  make(map[int64]*TableMetadata),
	}
}

// NewDiskStorageBackendWithWAL creates a new disk-based storage backend with WAL support
func NewDiskStorageBackendWithWAL(bufferPool *storage.BufferPool, catalog catalog.Catalog, walManager *wal.Manager) *DiskStorageBackend {
	return &DiskStorageBackend{
		bufferPool:   bufferPool,
		catalog:      catalog,
		walManager:   walManager,
		tablePages:   make(map[int64]storage.PageID),
		tableMeta:    make(map[int64]*TableMetadata),
		currentTxnID: 1, // Start with transaction ID 1
	}
}

// SetTransactionID sets the current transaction ID for WAL logging
func (d *DiskStorageBackend) SetTransactionID(txnID uint64) {
	d.currentTxnID = txnID
}

// CreateTable creates storage for a new table
func (d *DiskStorageBackend) CreateTable(table *catalog.Table) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Allocate first page for the table
	page, err := d.bufferPool.NewPage()
	if err != nil {
		return fmt.Errorf("failed to allocate page for table: %w", err)
	}
	pageID := page.Header.PageID
	defer d.bufferPool.UnpinPage(pageID, true)

	// Set page type and initialize
	page.Header.Type = storage.PageTypeData
	// Note: NewPage already set FreeSpacePtr = PageSize and FreeSpace correctly
	// We don't need to set them again

	// Create table metadata
	schema := &Schema{
		Columns: make([]Column, len(table.Columns)),
	}
	for i, col := range table.Columns {
		schema.Columns[i] = Column{
			Name:     col.Name,
			Type:     col.DataType,
			Nullable: col.IsNullable,
		}
	}

	metadata := &TableMetadata{
		TableID:     table.ID,
		FirstPageID: pageID,
		LastPageID:  pageID,
		RowCount:    0,
		RowFormat:   NewRowFormat(schema),
	}

	d.tablePages[table.ID] = pageID
	d.tableMeta[table.ID] = metadata

	return nil
}

// DropTable removes storage for a table
func (d *DiskStorageBackend) DropTable(tableID int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Get table metadata
	meta, exists := d.tableMeta[tableID]
	if !exists {
		return fmt.Errorf("table %d not found", tableID)
	}

	// TODO: Free all pages belonging to the table
	// For now, just remove from metadata
	delete(d.tablePages, tableID)
	delete(d.tableMeta, meta.TableID)

	return nil
}

// InsertRow inserts a row into a table
func (d *DiskStorageBackend) InsertRow(tableID int64, row *Row) (RowID, error) {
	d.mu.RLock()
	meta, exists := d.tableMeta[tableID]
	if !exists {
		d.mu.RUnlock()
		return RowID{}, fmt.Errorf("table %d not found", tableID)
	}
	d.mu.RUnlock()

	// Serialize the row
	rowData, err := meta.RowFormat.Serialize(row)
	if err != nil {
		return RowID{}, fmt.Errorf("failed to serialize row: %w", err)
	}

	// Find a page with enough space
	pageID := meta.LastPageID
	page, err := d.bufferPool.FetchPage(pageID)
	if err != nil {
		return RowID{}, fmt.Errorf("failed to fetch page: %w", err)
	}

	// Check if page has space
	if len(rowData) > math.MaxUint16 {
		d.bufferPool.UnpinPage(pageID, false)
		return RowID{}, fmt.Errorf("row data too large: %d exceeds max uint16", len(rowData))
	}
	rowSize := uint16(len(rowData)) //nolint:gosec // Bounds checked above

	// Acquire page lock before checking space and modifying
	d.bufferPool.AcquirePageLock(pageID)

	// Re-check if page has space after acquiring lock
	if !page.HasSpaceFor(rowSize) {
		// Need to allocate a new page
		// First release current page lock to avoid deadlock
		d.bufferPool.ReleasePageLock(pageID)

		// Allocate new page
		newPage, err := d.bufferPool.NewPage()
		if err != nil {
			d.bufferPool.UnpinPage(pageID, false)
			return RowID{}, fmt.Errorf("failed to allocate new page: %w", err)
		}
		newPageID := newPage.Header.PageID

		// Initialize new page
		newPage.Header.Type = storage.PageTypeData
		newPage.Header.NextPageID = storage.InvalidPageID

		// Re-acquire lock on old page to update link
		d.bufferPool.AcquirePageLock(pageID)
		page.Header.NextPageID = newPageID
		d.bufferPool.ReleasePageLock(pageID)
		d.bufferPool.UnpinPage(pageID, true)

		// Update metadata atomically - re-check if another thread already allocated
		d.mu.Lock()
		if meta.LastPageID != pageID {
			// Another thread already allocated a new page, use that instead
			d.mu.Unlock()
			d.bufferPool.UnpinPage(newPageID, false)
			// Retry with the new last page
			return d.InsertRow(tableID, row)
		}
		// We're the first to allocate, update metadata
		meta.LastPageID = newPageID
		d.mu.Unlock()

		// Use new page
		pageID = newPageID
		page = newPage

		// Acquire lock for new page
		d.bufferPool.AcquirePageLock(pageID)
	}

	// Insert row into page using slotted page format - lock is already held
	slotID := d.insertIntoPage(page, rowData)

	// Log the insert if WAL is enabled
	if d.walManager != nil {
		lsn, err := d.walManager.LogInsert(d.currentTxnID, tableID, uint32(pageID), slotID, rowData)
		if err != nil {
			// Rollback the insert on WAL failure
			d.bufferPool.ReleasePageLock(pageID)
			d.bufferPool.UnpinPage(pageID, false)
			return RowID{}, fmt.Errorf("failed to log insert: %w", err)
		}
		// Set page LSN
		page.Header.LSN = uint64(lsn)
	}

	// Release page lock before unpinning
	d.bufferPool.ReleasePageLock(pageID)
	d.bufferPool.UnpinPage(pageID, true)

	// Update row count
	d.mu.Lock()
	meta.RowCount++
	d.mu.Unlock()

	return RowID{
		PageID: pageID,
		SlotID: slotID,
	}, nil
}

// insertIntoPage inserts data into a page and returns the slot ID
func (d *DiskStorageBackend) insertIntoPage(page *storage.Page, data []byte) uint16 {
	// Simple slotted page implementation
	// Slots grow from the beginning, data grows from the end

	slotID := page.Header.ItemCount
	// Calculate slot offset within the Data array (excluding header)
	slotOffset := int(slotID) * 4 // Each slot is 4 bytes

	// Validate page state before insert
	if page.Header.FreeSpacePtr < storage.PageHeaderSize || page.Header.FreeSpacePtr > storage.PageSize {
		panic(fmt.Sprintf("invalid FreeSpacePtr before insert: %d (must be in [%d, %d])",
			page.Header.FreeSpacePtr, storage.PageHeaderSize, storage.PageSize))
	}

	// Calculate data position (grows from end)
	if len(data) > math.MaxUint16 {
		panic(fmt.Sprintf("data too large for page: %d exceeds max uint16", len(data)))
	}
	dataSize := uint16(len(data)) //nolint:gosec // Bounds checked above

	// Bounds check for slot offset
	if slotOffset+4 > len(page.Data) {
		panic(fmt.Sprintf("slot offset out of bounds: %d+4 > %d", slotOffset, len(page.Data)))
	}

	// FreeSpacePtr points to start of free space (where next data will be written)
	// Data grows from the end backwards, so new data position is FreeSpacePtr - dataSize
	pageDataOffset := page.Header.FreeSpacePtr - dataSize

	// Validate we're not going below the header
	if pageDataOffset < storage.PageHeaderSize {
		panic(fmt.Sprintf("data would overlap header: pageDataOffset=%d < PageHeaderSize=%d (FreeSpacePtr=%d, dataSize=%d, slotID=%d)",
			pageDataOffset, storage.PageHeaderSize, page.Header.FreeSpacePtr, dataSize, slotID))
	}

	// Calculate offset within Data array
	dataOffset := pageDataOffset - storage.PageHeaderSize

	// Bounds check for data offset
	if int(dataOffset) >= len(page.Data) {
		panic(fmt.Sprintf("data offset out of bounds: %d not in [0, %d)", dataOffset, len(page.Data)))
	}
	if int(dataOffset)+len(data) > len(page.Data) {
		panic(fmt.Sprintf("data write would exceed page bounds: %d+%d > %d", dataOffset, len(data), len(page.Data)))
	}

	// Write slot entry (offset and size)
	// Store the offset as relative to page start (including header)
	if slotOffset+4 > len(page.Data) {
		panic(fmt.Sprintf("Invalid slot offset calculation: slotOffset=%d, page.Data len=%d", slotOffset, len(page.Data)))
	}
	slotData := page.Data[slotOffset : slotOffset+4]

	// Validate before writing
	if pageDataOffset < storage.PageHeaderSize {
		panic(fmt.Sprintf("invalid pageDataOffset: %d < %d", pageDataOffset, storage.PageHeaderSize))
	}
	if pageDataOffset > storage.PageSize {
		panic(fmt.Sprintf("pageDataOffset out of bounds: %d > %d", pageDataOffset, storage.PageSize))
	}

	slotData[0] = byte(pageDataOffset >> 8)
	slotData[1] = byte(pageDataOffset)
	slotData[2] = byte(dataSize >> 8)
	slotData[3] = byte(dataSize)

	// Verify slot was written correctly
	writtenOffset := uint16(slotData[0])<<8 | uint16(slotData[1])
	writtenSize := uint16(slotData[2])<<8 | uint16(slotData[3])
	if writtenOffset != pageDataOffset {
		panic(fmt.Sprintf("slot write verification failed: wrote offset %d, read back %d (slot %d, page %d)",
			pageDataOffset, writtenOffset, slotID, page.Header.PageID))
	}
	if writtenSize != dataSize {
		panic(fmt.Sprintf("slot write verification failed: wrote size %d, read back %d (slot %d, page %d)",
			dataSize, writtenSize, slotID, page.Header.PageID))
	}
	if pageDataOffset == 0 {
		panic(fmt.Sprintf("invalid pageDataOffset=0 written to slot %d on page %d", slotID, page.Header.PageID))
	}

	// Write data
	copy(page.Data[dataOffset:dataOffset+dataSize], data)

	// Update page header
	page.Header.ItemCount++
	page.Header.FreeSpacePtr = pageDataOffset
	page.Header.FreeSpace -= dataSize + 4

	return slotID
}

// ScanTable returns an iterator for scanning a table
func (d *DiskStorageBackend) ScanTable(tableID int64, snapshotTS int64) (RowIterator, error) {
	d.mu.RLock()
	meta, exists := d.tableMeta[tableID]
	if !exists {
		d.mu.RUnlock()
		return nil, fmt.Errorf("table %d not found", tableID)
	}
	firstPageID := meta.FirstPageID
	rowFormat := meta.RowFormat
	d.mu.RUnlock()

	return &diskRowIterator{
		bufferPool:  d.bufferPool,
		currentPage: nil,
		pageID:      firstPageID,
		slotID:      0,
		rowFormat:   rowFormat,
	}, nil
}

// diskRowIterator implements RowIterator for disk-based storage
type diskRowIterator struct {
	bufferPool  *storage.BufferPool
	currentPage *storage.Page
	pageID      storage.PageID
	slotID      uint16
	rowFormat   *RowFormat
}

// Next returns the next row
func (it *diskRowIterator) Next() (*Row, RowID, error) {
	for {
		// Fetch current page if needed
		if it.currentPage == nil {
			if it.pageID == storage.InvalidPageID {
				return nil, RowID{}, nil // EOF
			}

			page, err := it.bufferPool.FetchPage(it.pageID)
			if err != nil {
				return nil, RowID{}, fmt.Errorf("failed to fetch page: %w", err)
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

		// Read slot entry
		slotOffset := int(it.slotID) * 4 // Offset within Data array
		if slotOffset+4 > len(it.currentPage.Data) {
			// Invalid slot offset, skip
			it.slotID++
			continue
		}
		slotData := it.currentPage.Data[slotOffset : slotOffset+4]
		pageDataOffset := uint16(slotData[0])<<8 | uint16(slotData[1])
		dataSize := uint16(slotData[2])<<8 | uint16(slotData[3])

		// Skip deleted slots (size = 0)
		if dataSize == 0 {
			it.slotID++
			continue
		}

		// Read row data - pageDataOffset includes header, so subtract it
		dataOffset := pageDataOffset - storage.PageHeaderSize
		if int(dataOffset) >= len(it.currentPage.Data) || int(dataOffset)+int(dataSize) > len(it.currentPage.Data) {
			// Invalid data bounds, skip
			it.slotID++
			continue
		}
		rowData := it.currentPage.Data[dataOffset : dataOffset+dataSize]

		// Deserialize row
		row, err := it.rowFormat.Deserialize(rowData)
		if err != nil {
			return nil, RowID{}, fmt.Errorf("failed to deserialize row: %w", err)
		}

		rowID := RowID{
			PageID: it.pageID,
			SlotID: it.slotID,
		}

		it.slotID++

		return row, rowID, nil
	}
}

// Close releases resources
func (it *diskRowIterator) Close() error {
	if it.currentPage != nil {
		it.bufferPool.UnpinPage(it.pageID, false)
		it.currentPage = nil
	}
	return nil
}

// UpdateRow updates an existing row
func (d *DiskStorageBackend) UpdateRow(tableID int64, rowID RowID, row *Row) error {
	// Check table exists first with read lock
	d.mu.RLock()
	_, exists := d.tableMeta[tableID]
	d.mu.RUnlock()

	if !exists {
		return fmt.Errorf("table %d not found", tableID)
	}

	// For MVCC, we don't update in place. Instead:
	// 1. Insert a new row version
	// 2. Mark the old row as deleted (but keep it for older transactions)

	// Insert the new row version first (atomic operation)
	newRowID, err := d.InsertRow(tableID, row)
	if err != nil {
		return fmt.Errorf("failed to insert new row version: %w", err)
	}

	// Now mark the old row as deleted using the proper DeleteRow method
	if err := d.DeleteRow(tableID, rowID); err != nil {
		// Delete failed, we need to rollback the new row to maintain consistency
		if rollbackErr := d.DeleteRow(tableID, newRowID); rollbackErr != nil {
			// Rollback also failed - we're in an inconsistent state
			return fmt.Errorf("failed to mark old row as deleted: %v; rollback of new row also failed: %w", err, rollbackErr)
		}
		return fmt.Errorf("failed to mark old row as deleted, rolled back new version: %w", err)
	}

	// In a full MVCC implementation, we would:
	// - Store transaction IDs with each row version
	// - Keep track of which versions are visible to which transactions
	// - Clean up old versions during vacuum

	// For now, we just inserted a new version and marked the old one as deleted
	_ = newRowID // The new row ID could be returned or stored for reference

	return nil
}

// DeleteRow deletes a row
func (d *DiskStorageBackend) DeleteRow(tableID int64, rowID RowID) error {
	// Fetch the page
	page, err := d.bufferPool.FetchPage(rowID.PageID)
	if err != nil {
		return fmt.Errorf("failed to fetch page: %w", err)
	}

	// Acquire page lock before modifying
	d.bufferPool.AcquirePageLock(rowID.PageID)
	defer d.bufferPool.ReleasePageLock(rowID.PageID)

	// Log the delete if WAL is enabled
	if d.walManager != nil {
		lsn, err := d.walManager.LogDelete(d.currentTxnID, tableID, uint32(rowID.PageID), rowID.SlotID)
		if err != nil {
			d.bufferPool.UnpinPage(rowID.PageID, false)
			return fmt.Errorf("failed to log delete: %w", err)
		}
		// Set page LSN
		page.Header.LSN = uint64(lsn)
	}

	// Mark slot as deleted by setting size to 0
	slotOffset := int(rowID.SlotID) * 4 // Offset within Data array
	// Correct slice access: directly index into page.Data
	if slotOffset+4 > len(page.Data) {
		d.bufferPool.UnpinPage(rowID.PageID, false)
		return fmt.Errorf("slot offset out of bounds")
	}
	slotData := page.Data[slotOffset : slotOffset+4]
	slotData[2] = 0
	slotData[3] = 0

	d.bufferPool.UnpinPage(rowID.PageID, true)

	return nil
}

// GetRow retrieves a specific row by ID
func (d *DiskStorageBackend) GetRow(tableID int64, rowID RowID, snapshotTS int64) (*Row, error) {
	d.mu.RLock()
	meta, exists := d.tableMeta[tableID]
	if !exists {
		d.mu.RUnlock()
		return nil, fmt.Errorf("table %d not found", tableID)
	}
	rowFormat := meta.RowFormat
	d.mu.RUnlock()

	// Fetch the page
	page, err := d.bufferPool.FetchPage(rowID.PageID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch page: %w", err)
	}
	defer d.bufferPool.UnpinPage(rowID.PageID, false)

	// Read slot entry
	slotOffset := int(rowID.SlotID) * 4 // Offset within Data array
	slotData := page.Data[slotOffset : slotOffset+4]
	pageDataOffset := uint16(slotData[0])<<8 | uint16(slotData[1])
	dataSize := uint16(slotData[2])<<8 | uint16(slotData[3])

	if dataSize == 0 {
		return nil, fmt.Errorf("row has been deleted")
	}

	// Read row data - pageDataOffset includes header, so subtract it
	dataOffset := pageDataOffset - storage.PageHeaderSize
	rowData := page.Data[dataOffset : dataOffset+dataSize]

	// Deserialize row
	return rowFormat.Deserialize(rowData)
}
