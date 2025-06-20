package storage

import (
	"fmt"
	"math"
	"os"
	"sync"
)

// DiskManager handles disk I/O operations for database pages
type DiskManager struct {
	file       *os.File
	filePath   string
	nextPageID PageID
	mu         sync.RWMutex
}

// NewDiskManager creates a new disk manager
func NewDiskManager(filePath string) (*DiskManager, error) {
	// Open or create the database file
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open database file: %w", err)
	}

	// Get file info to determine next page ID
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat database file: %w", err)
	}

	// Calculate next page ID based on file size
	numPages := info.Size() / PageSize
	if numPages > math.MaxUint32 {
		return nil, fmt.Errorf("file too large: %d pages exceeds max uint32", numPages)
	}
	nextPageID := PageID(uint32(numPages)) //nolint:gosec // Bounds checked above
	if nextPageID == 0 {
		nextPageID = 1 // Page 0 is reserved for metadata
	}

	dm := &DiskManager{
		file:       file,
		filePath:   filePath,
		nextPageID: nextPageID,
	}

	// Initialize metadata page if new database
	if numPages == 0 {
		if err := dm.initializeMetadataPage(); err != nil {
			file.Close()
			return nil, err
		}
	}

	return dm, nil
}

// initializeMetadataPage creates the initial metadata page
func (dm *DiskManager) initializeMetadataPage() error {
	metaPage := NewPage(0, PageTypeFree)
	// TODO: Add database metadata (version, page count, etc.)

	if err := dm.WritePage(metaPage); err != nil {
		return fmt.Errorf("failed to write metadata page: %w", err)
	}

	dm.nextPageID = 1
	return nil
}

// AllocatePage allocates a new page and returns its ID
func (dm *DiskManager) AllocatePage() (PageID, error) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	pageID := dm.nextPageID
	dm.nextPageID++

	// Extend the file to accommodate the new page
	newSize := int64(dm.nextPageID) * PageSize
	if err := dm.file.Truncate(newSize); err != nil {
		return InvalidPageID, fmt.Errorf("failed to extend file: %w", err)
	}
	
	// Write zeros to the new page to ensure it's properly initialized
	// This is important because truncate behavior is OS-dependent
	zeroPage := make([]byte, PageSize)
	offset := int64(pageID) * PageSize
	if _, err := dm.file.WriteAt(zeroPage, offset); err != nil {
		return InvalidPageID, fmt.Errorf("failed to initialize page %d: %w", pageID, err)
	}

	return pageID, nil
}

// ReadPage reads a page from disk
func (dm *DiskManager) ReadPage(pageID PageID) (*Page, error) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	// Validate page ID
	if pageID >= dm.nextPageID {
		return nil, fmt.Errorf("invalid page ID %d (max: %d)", pageID, dm.nextPageID-1)
	}

	// Seek to page location
	offset := int64(pageID) * PageSize
	if _, err := dm.file.Seek(offset, 0); err != nil {
		return nil, fmt.Errorf("failed to seek to page %d: %w", pageID, err)
	}

	// Read page data
	buf := make([]byte, PageSize)
	if _, err := dm.file.Read(buf); err != nil {
		return nil, fmt.Errorf("failed to read page %d: %w", pageID, err)
	}

	// Deserialize page
	page := &Page{}
	if err := page.Deserialize(buf); err != nil {
		return nil, fmt.Errorf("failed to deserialize page %d: %w", pageID, err)
	}

	return page, nil
}

// WritePage writes a page to disk
func (dm *DiskManager) WritePage(page *Page) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	// Validate page ID
	if page.Header.PageID >= dm.nextPageID {
		return fmt.Errorf("invalid page ID %d (max: %d)", page.Header.PageID, dm.nextPageID-1)
	}

	// Seek to page location
	offset := int64(page.Header.PageID) * PageSize
	if _, err := dm.file.Seek(offset, 0); err != nil {
		return fmt.Errorf("failed to seek to page %d: %w", page.Header.PageID, err)
	}

	// Serialize and write page
	buf := page.Serialize()
	if _, err := dm.file.Write(buf); err != nil {
		return fmt.Errorf("failed to write page %d: %w", page.Header.PageID, err)
	}

	return nil
}

// Sync ensures all changes are flushed to disk
func (dm *DiskManager) Sync() error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	return dm.file.Sync()
}

// Close closes the disk manager and underlying file
func (dm *DiskManager) Close() error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	if err := dm.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync before close: %w", err)
	}

	return dm.file.Close()
}

// GetPageCount returns the total number of pages
func (dm *DiskManager) GetPageCount() PageID {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	return dm.nextPageID
}
