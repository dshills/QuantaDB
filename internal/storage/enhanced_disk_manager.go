package storage

import (
	"fmt"
	"sync"
	"time"
)

// EnhancedDiskManager extends DiskManager with compression and parallel I/O
type EnhancedDiskManager struct {
	*DiskManager
	compressionManager *CompressionManager
	parallelIO         *ParallelIOManager
	compressionEnabled bool
	parallelIOEnabled  bool
	readAheadEnabled   bool
	readAheadSize      int
	cache              map[PageID]*Page // Simple read-ahead cache
	cacheMu            sync.RWMutex
	cacheMaxSize       int
}

// EnhancedDiskManagerConfig configures the enhanced disk manager
type EnhancedDiskManagerConfig struct {
	CompressionEnabled bool
	ParallelIOEnabled  bool
	ReadAheadEnabled   bool
	ReadAheadSize      int
	WorkerCount        int
	CacheSize          int
}

// NewEnhancedDiskManager creates a new enhanced disk manager
func NewEnhancedDiskManager(filePath string, config EnhancedDiskManagerConfig) (*EnhancedDiskManager, error) {
	// Create base disk manager
	baseDM, err := NewDiskManager(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create base disk manager: %w", err)
	}

	edm := &EnhancedDiskManager{
		DiskManager:        baseDM,
		compressionEnabled: config.CompressionEnabled,
		parallelIOEnabled:  config.ParallelIOEnabled,
		readAheadEnabled:   config.ReadAheadEnabled,
		readAheadSize:      config.ReadAheadSize,
		cache:              make(map[PageID]*Page),
		cacheMaxSize:       config.CacheSize,
	}

	// Initialize compression manager if enabled
	if config.CompressionEnabled {
		edm.compressionManager = NewCompressionManager()
	}

	// Initialize parallel I/O manager if enabled
	if config.ParallelIOEnabled {
		workerCount := config.WorkerCount
		if workerCount <= 0 {
			workerCount = 4 // Default worker count
		}

		edm.parallelIO = NewParallelIOManager(baseDM, workerCount)
		if err := edm.parallelIO.Start(); err != nil {
			return nil, fmt.Errorf("failed to start parallel I/O manager: %w", err)
		}
	}

	// Set default read-ahead size if not specified
	if edm.readAheadSize <= 0 {
		edm.readAheadSize = 4 // Read ahead 4 pages by default
	}

	// Set default cache size
	if edm.cacheMaxSize <= 0 {
		edm.cacheMaxSize = 100 // Cache up to 100 pages
	}

	return edm, nil
}

// ReadPage reads a page with enhanced features
func (edm *EnhancedDiskManager) ReadPage(pageID PageID) (*Page, error) {
	// Check cache first
	if page := edm.getCachedPage(pageID); page != nil {
		return page, nil
	}

	// Use parallel I/O if enabled
	if edm.parallelIOEnabled {
		return edm.readPageAsync(pageID)
	}

	// Fallback to synchronous read
	page, err := edm.DiskManager.ReadPage(pageID)
	if err != nil {
		return nil, err
	}

	// Trigger read-ahead if enabled
	if edm.readAheadEnabled {
		go edm.performReadAhead(pageID)
	}

	// Cache the page
	edm.cachePage(pageID, page)

	return page, nil
}

// readPageAsync reads a page using parallel I/O
func (edm *EnhancedDiskManager) readPageAsync(pageID PageID) (*Page, error) {
	resultChan := edm.parallelIO.ReadPageAsync(pageID, IOPriorityNormal)

	select {
	case result := <-resultChan:
		if result.Error != nil {
			return nil, result.Error
		}

		// Cache the page
		edm.cachePage(pageID, result.Page)

		// Trigger read-ahead if enabled
		if edm.readAheadEnabled {
			go edm.performReadAhead(pageID)
		}

		return result.Page, nil

	case <-time.After(30 * time.Second): // Timeout
		return nil, fmt.Errorf("read page %d timed out", pageID)
	}
}

// WritePage writes a page with enhanced features
func (edm *EnhancedDiskManager) WritePage(page *Page) error {
	// Try compression if enabled
	if edm.compressionEnabled && edm.compressionManager.ShouldCompress(page) {
		return edm.writePageCompressed(page)
	}

	// Use parallel I/O if enabled
	if edm.parallelIOEnabled {
		return edm.writePageAsync(page)
	}

	// Fallback to synchronous write
	err := edm.DiskManager.WritePage(page)
	if err == nil {
		// Update cache
		edm.cachePage(page.Header.PageID, page)
	}

	return err
}

// writePageCompressed writes a page with compression
func (edm *EnhancedDiskManager) writePageCompressed(page *Page) error {
	compressedPage, compressed, err := edm.compressionManager.CompressPage(page, CompressionLZ4)
	if err != nil {
		// Fall back to uncompressed write
		return edm.WritePage(page)
	}

	if !compressed {
		// Compression not beneficial, write normally
		return edm.WritePage(page)
	}

	// For now, we'll store compression metadata separately
	// In a full implementation, we'd need to modify the page format
	// to include compression metadata

	// Create a new page with compressed data
	compressedPageData := &Page{
		Header: page.Header,
	}
	// Only copy up to the size of the page data array
	copySize := len(compressedPage.CompressedData)
	if copySize > len(compressedPageData.Data) {
		copySize = len(compressedPageData.Data)
	}
	copy(compressedPageData.Data[:copySize], compressedPage.CompressedData[:copySize])

	// Mark page as compressed in header (we'd need to add this field)
	// For now, just write the compressed page
	if edm.parallelIOEnabled {
		return edm.writePageAsync(compressedPageData)
	}

	return edm.DiskManager.WritePage(compressedPageData)
}

// writePageAsync writes a page using parallel I/O
func (edm *EnhancedDiskManager) writePageAsync(page *Page) error {
	resultChan := edm.parallelIO.WritePageAsync(page, IOPriorityNormal)

	select {
	case result := <-resultChan:
		if result.Error == nil {
			// Update cache
			edm.cachePage(page.Header.PageID, page)
		}
		return result.Error

	case <-time.After(30 * time.Second): // Timeout
		return fmt.Errorf("write page %d timed out", page.Header.PageID)
	}
}

// ReadPagesBatch reads multiple pages in parallel
func (edm *EnhancedDiskManager) ReadPagesBatch(pageIDs []PageID) (map[PageID]*Page, error) {
	results := make(map[PageID]*Page)

	if !edm.parallelIOEnabled {
		// Fallback to sequential reads
		for _, pageID := range pageIDs {
			page, err := edm.ReadPage(pageID)
			if err != nil {
				return nil, fmt.Errorf("failed to read page %d: %w", pageID, err)
			}
			results[pageID] = page
		}
		return results, nil
	}

	// Use parallel I/O for batch reads
	resultChans := edm.parallelIO.ReadPagesBatch(pageIDs, IOPriorityNormal)

	// Collect results
	for pageID, resultChan := range resultChans {
		select {
		case result := <-resultChan:
			if result.Error != nil {
				return nil, fmt.Errorf("failed to read page %d: %w", pageID, result.Error)
			}
			results[pageID] = result.Page

			// Cache the page
			edm.cachePage(pageID, result.Page)

		case <-time.After(30 * time.Second):
			return nil, fmt.Errorf("batch read timed out for page %d", pageID)
		}
	}

	return results, nil
}

// performReadAhead performs read-ahead caching for sequential access patterns
func (edm *EnhancedDiskManager) performReadAhead(startPageID PageID) {
	// Read ahead the next few pages
	for i := 1; i <= edm.readAheadSize; i++ {
		nextPageID := startPageID + PageID(uint32(i)) //nolint:gosec // Bounds are controlled by readAheadSize

		// Check if page is already cached
		if edm.getCachedPage(nextPageID) != nil {
			continue
		}

		// Async read with low priority to not interfere with user requests
		if edm.parallelIOEnabled {
			go func(pageID PageID) {
				resultChan := edm.parallelIO.ReadPageAsync(pageID, IOPriorityLow)
				select {
				case result := <-resultChan:
					if result.Error == nil {
						edm.cachePage(pageID, result.Page)
					}
				case <-time.After(5 * time.Second):
					// Timeout for read-ahead, ignore
				}
			}(nextPageID)
		}
	}
}

// getCachedPage retrieves a page from cache
func (edm *EnhancedDiskManager) getCachedPage(pageID PageID) *Page {
	edm.cacheMu.RLock()
	defer edm.cacheMu.RUnlock()

	return edm.cache[pageID]
}

// cachePage stores a page in cache
func (edm *EnhancedDiskManager) cachePage(pageID PageID, page *Page) {
	edm.cacheMu.Lock()
	defer edm.cacheMu.Unlock()

	// Simple LRU eviction if cache is full
	if len(edm.cache) >= edm.cacheMaxSize {
		// Remove the first entry (simple approximation of LRU)
		for id := range edm.cache {
			delete(edm.cache, id)
			break
		}
	}

	// Clone the page to avoid shared memory issues
	cachedPage := &Page{
		Header: page.Header,
	}
	copy(cachedPage.Data[:], page.Data[:])

	edm.cache[pageID] = cachedPage
}

// InvalidateCache removes a page from cache
func (edm *EnhancedDiskManager) InvalidateCache(pageID PageID) {
	edm.cacheMu.Lock()
	defer edm.cacheMu.Unlock()

	delete(edm.cache, pageID)
}

// ClearCache clears all cached pages
func (edm *EnhancedDiskManager) ClearCache() {
	edm.cacheMu.Lock()
	defer edm.cacheMu.Unlock()

	edm.cache = make(map[PageID]*Page)
}

// GetStats returns enhanced statistics
func (edm *EnhancedDiskManager) GetStats() map[string]interface{} {
	stats := make(map[string]interface{})

	// Base disk manager stats
	stats["page_count"] = edm.GetPageCount()

	// Cache stats
	edm.cacheMu.RLock()
	stats["cache_size"] = len(edm.cache)
	stats["cache_max_size"] = edm.cacheMaxSize
	edm.cacheMu.RUnlock()

	// Compression stats if enabled
	if edm.compressionEnabled {
		compressionStats := edm.compressionManager.GetStats()
		stats["compression"] = map[string]interface{}{
			"compressed_pages":   compressionStats.CompressedPages,
			"decompressed_pages": compressionStats.DecompressedPages,
			"compression_ratio":  compressionStats.CompressionRatio,
			"total_savings":      compressionStats.TotalSavings,
		}
	}

	// Parallel I/O stats if enabled
	if edm.parallelIOEnabled {
		ioStats := edm.parallelIO.GetStats()
		stats["parallel_io"] = map[string]interface{}{
			"requests_processed": ioStats.RequestsProcessed,
			"bytes_read":         ioStats.BytesRead,
			"bytes_written":      ioStats.BytesWritten,
			"average_latency":    ioStats.AverageLatency.String(),
			"queue_depth":        ioStats.QueueDepth,
		}
	}

	return stats
}

// Sync with enhanced features
func (edm *EnhancedDiskManager) Sync() error {
	if edm.parallelIOEnabled {
		resultChan := edm.parallelIO.SyncAsync(IOPriorityHigh)
		select {
		case result := <-resultChan:
			return result.Error
		case <-time.After(60 * time.Second): // Longer timeout for sync
			return fmt.Errorf("sync operation timed out")
		}
	}

	return edm.DiskManager.Sync()
}

// Close cleans up all resources
func (edm *EnhancedDiskManager) Close() error {
	var lastError error

	// Stop parallel I/O manager
	if edm.parallelIOEnabled {
		if err := edm.parallelIO.Stop(); err != nil {
			lastError = err
		}
	}

	// Clear cache
	edm.ClearCache()

	// Close base disk manager
	if err := edm.DiskManager.Close(); err != nil {
		lastError = err
	}

	return lastError
}
