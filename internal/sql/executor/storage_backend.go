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
	page.Header.FreeSpacePtr = storage.PageSize // Start from end of page
	page.Header.FreeSpace = storage.PageSize - storage.PageHeaderSize
	page.Header.ItemCount = 0

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
		return RowID{}, fmt.Errorf("row data too large: %d exceeds max uint16", len(rowData))
	}
	rowSize := uint16(len(rowData)) //nolint:gosec // Bounds checked above
	if !page.HasSpaceFor(rowSize) {
		d.bufferPool.UnpinPage(pageID, false)

		// Allocate new page
		newPage, err := d.bufferPool.NewPage()
		if err != nil {
			return RowID{}, fmt.Errorf("failed to allocate new page: %w", err)
		}
		newPageID := newPage.Header.PageID

		// Link pages
		page.Header.NextPageID = newPageID
		d.bufferPool.UnpinPage(pageID, true)

		// Update metadata
		d.mu.Lock()
		meta.LastPageID = newPageID
		d.mu.Unlock()

		// Use new page
		pageID = newPageID
		page = newPage
		page.Header.Type = storage.PageTypeData
		page.Header.FreeSpacePtr = storage.PageSize
		page.Header.FreeSpace = storage.PageSize - storage.PageHeaderSize
		page.Header.ItemCount = 0
	}

	// Insert row into page using slotted page format
	slotID := d.insertIntoPage(page, rowData)

	// Log the insert if WAL is enabled
	if d.walManager != nil {
		lsn, err := d.walManager.LogInsert(d.currentTxnID, tableID, uint32(pageID), slotID, rowData)
		if err != nil {
			// Rollback the insert on WAL failure
			d.bufferPool.UnpinPage(pageID, false)
			return RowID{}, fmt.Errorf("failed to log insert: %w", err)
		}
		// Set page LSN
		page.Header.LSN = uint64(lsn)
	}

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
	slotOffset := storage.PageHeaderSize + slotID*4 // Each slot is 4 bytes

	// Calculate data position (grows from end)
	if len(data) > math.MaxUint16 {
		panic(fmt.Sprintf("data too large for page: %d exceeds max uint16", len(data)))
	}
	dataSize := uint16(len(data)) //nolint:gosec // Bounds checked above
	// FreeSpacePtr is relative to page start, we need offset in Data array
	dataOffset := page.Header.FreeSpacePtr - dataSize - storage.PageHeaderSize

	// Write slot entry (offset and size)
	// Store the offset as relative to page start (including header)
	slotData := page.Data[slotOffset : slotOffset+4]
	pageDataOffset := dataOffset + storage.PageHeaderSize
	slotData[0] = byte(pageDataOffset >> 8)
	slotData[1] = byte(pageDataOffset)
	slotData[2] = byte(dataSize >> 8)
	slotData[3] = byte(dataSize)

	// Write data
	copy(page.Data[dataOffset:], data)

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
		slotOffset := storage.PageHeaderSize + it.slotID*4
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
	slotOffset := storage.PageHeaderSize + rowID.SlotID*4
	// Correct slice access: directly index into page.Data
	if int(slotOffset)+4 > len(page.Data)+storage.PageHeaderSize {
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
	slotOffset := storage.PageHeaderSize + rowID.SlotID*4
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
