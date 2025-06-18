package executor

import (
	"fmt"
	"sync"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/storage"
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
	ScanTable(tableID int64) (RowIterator, error)
	
	// GetRow retrieves a specific row by ID
	GetRow(tableID int64, rowID RowID) (*Row, error)
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
	
	// Table metadata mapping
	mu          sync.RWMutex
	tablePages  map[int64]storage.PageID // Table ID -> First page ID
	tableMeta   map[int64]*TableMetadata
}

// TableMetadata stores metadata about a table's storage
type TableMetadata struct {
	TableID      int64
	FirstPageID  storage.PageID
	LastPageID   storage.PageID
	RowCount     uint64
	RowFormat    *RowFormat
}

// NewDiskStorageBackend creates a new disk-based storage backend
func NewDiskStorageBackend(bufferPool *storage.BufferPool, catalog catalog.Catalog) *DiskStorageBackend {
	return &DiskStorageBackend{
		bufferPool:  bufferPool,
		catalog:     catalog,
		tablePages:  make(map[int64]storage.PageID),
		tableMeta:   make(map[int64]*TableMetadata),
	}
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
	
	// Set page type
	page.Header.Type = storage.PageTypeData
	
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
	rowSize := uint16(len(rowData))
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
	}
	
	// Insert row into page using slotted page format
	slotID := d.insertIntoPage(page, rowData)
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
	dataSize := uint16(len(data))
	dataOffset := page.Header.FreeSpacePtr - dataSize
	
	// Write slot entry (offset and size)
	slotData := page.Data[slotOffset-storage.PageHeaderSize:]
	slotData[0] = byte(dataOffset >> 8)
	slotData[1] = byte(dataOffset)
	slotData[2] = byte(dataSize >> 8)
	slotData[3] = byte(dataSize)
	
	// Write data
	copy(page.Data[dataOffset-storage.PageHeaderSize:], data)
	
	// Update page header
	page.Header.ItemCount++
	page.Header.FreeSpacePtr = dataOffset
	page.Header.FreeSpace -= dataSize + 4
	
	return slotID
}

// ScanTable returns an iterator for scanning a table
func (d *DiskStorageBackend) ScanTable(tableID int64) (RowIterator, error) {
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
		slotData := it.currentPage.Data[slotOffset-storage.PageHeaderSize:]
		dataOffset := uint16(slotData[0])<<8 | uint16(slotData[1])
		dataSize := uint16(slotData[2])<<8 | uint16(slotData[3])
		
		// Skip deleted slots (size = 0)
		if dataSize == 0 {
			it.slotID++
			continue
		}
		
		// Read row data
		rowData := it.currentPage.Data[dataOffset-storage.PageHeaderSize : dataOffset-storage.PageHeaderSize+dataSize]
		
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
	// TODO: Implement update with MVCC versioning
	return fmt.Errorf("update not yet implemented")
}

// DeleteRow deletes a row
func (d *DiskStorageBackend) DeleteRow(tableID int64, rowID RowID) error {
	// Fetch the page
	page, err := d.bufferPool.FetchPage(rowID.PageID)
	if err != nil {
		return fmt.Errorf("failed to fetch page: %w", err)
	}
	defer d.bufferPool.UnpinPage(rowID.PageID, true)
	
	// Mark slot as deleted by setting size to 0
	slotOffset := storage.PageHeaderSize + rowID.SlotID*4
	slotData := page.Data[slotOffset-storage.PageHeaderSize:]
	slotData[2] = 0
	slotData[3] = 0
	
	return nil
}

// GetRow retrieves a specific row by ID
func (d *DiskStorageBackend) GetRow(tableID int64, rowID RowID) (*Row, error) {
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
	slotData := page.Data[slotOffset-storage.PageHeaderSize:]
	dataOffset := uint16(slotData[0])<<8 | uint16(slotData[1])
	dataSize := uint16(slotData[2])<<8 | uint16(slotData[3])
	
	if dataSize == 0 {
		return nil, fmt.Errorf("row has been deleted")
	}
	
	// Read row data
	rowData := page.Data[dataOffset-storage.PageHeaderSize : dataOffset-storage.PageHeaderSize+dataSize]
	
	// Deserialize row
	return rowFormat.Deserialize(rowData)
}