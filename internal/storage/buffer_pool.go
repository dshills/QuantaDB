package storage

import (
	"container/list"
	"fmt"
	"sync"
)

// BufferPoolStats tracks buffer pool performance metrics.
type BufferPoolStats struct {
	PagesHit     int64 // Cache hits
	PagesRead    int64 // Pages read from disk
	PagesDirtied int64 // Pages marked dirty
	PagesEvicted int64 // Pages evicted from cache
	mu           sync.RWMutex
}

// BufferPool manages pages in memory with LRU eviction.
type BufferPool struct {
	diskManager *DiskManager
	pages       map[PageID]*BufferPoolPage
	lruList     *list.List
	maxPages    int
	mu          sync.RWMutex
	pageLockMgr *PageLockManager

	// Statistics
	stats BufferPoolStats
}

// BufferPoolPage wraps a page with metadata for buffer management.
type BufferPoolPage struct {
	page     *Page
	pinCount int           // Number of threads currently using this page
	dirty    bool          // Has the page been modified?
	lruNode  *list.Element // Node in LRU list
}

// NewBufferPool creates a new buffer pool.
func NewBufferPool(diskManager *DiskManager, maxPages int) *BufferPool {
	return &BufferPool{
		diskManager: diskManager,
		pages:       make(map[PageID]*BufferPoolPage),
		lruList:     list.New(),
		maxPages:    maxPages,
		pageLockMgr: NewPageLockManager(),
	}
}

// FetchPage retrieves a page from the buffer pool or disk
func (bp *BufferPool) FetchPage(pageID PageID) (*Page, error) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	// Check if page is already in buffer
	if bpPage, exists := bp.pages[pageID]; exists {
		// Page is in buffer, pin it
		bpPage.pinCount++

		// Move to front of LRU list if not pinned by others
		if bpPage.pinCount == 1 && bpPage.lruNode != nil {
			bp.lruList.MoveToFront(bpPage.lruNode)
		}

		// Track cache hit
		bp.stats.mu.Lock()
		bp.stats.PagesHit++
		bp.stats.mu.Unlock()

		return bpPage.page, nil
	}

	// Page not in buffer, need to fetch from disk
	// First check if we need to evict
	if len(bp.pages) >= bp.maxPages {
		if !bp.evictPage() {
			return nil, fmt.Errorf("buffer pool is full and no pages can be evicted")
		}
	}

	// Read page from disk
	page, err := bp.diskManager.ReadPage(pageID)
	if err != nil {
		return nil, fmt.Errorf("failed to read page from disk: %w", err)
	}

	// Track page read from disk
	bp.stats.mu.Lock()
	bp.stats.PagesRead++
	bp.stats.mu.Unlock()

	// Add to buffer pool
	bpPage := &BufferPoolPage{
		page:     page,
		pinCount: 1,
		dirty:    false,
	}

	// Add to LRU list (at front since it's just accessed)
	bpPage.lruNode = bp.lruList.PushFront(pageID)
	bp.pages[pageID] = bpPage

	return page, nil
}

// NewPage allocates a new page in the buffer pool
func (bp *BufferPool) NewPage() (*Page, error) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	// Allocate new page ID
	pageID, err := bp.diskManager.AllocatePage()
	if err != nil {
		return nil, fmt.Errorf("failed to allocate new page: %w", err)
	}

	// Check if we need to evict
	if len(bp.pages) >= bp.maxPages {
		if !bp.evictPage() {
			return nil, fmt.Errorf("buffer pool is full and no pages can be evicted")
		}
	}

	// Create new page
	page := NewPage(pageID, PageTypeData)

	// Add to buffer pool
	bpPage := &BufferPoolPage{
		page:     page,
		pinCount: 1,
		dirty:    true, // New pages are dirty by default
	}

	// Add to LRU list
	bpPage.lruNode = bp.lruList.PushFront(pageID)
	bp.pages[pageID] = bpPage

	return page, nil
}

// UnpinPage decrements the pin count of a page
func (bp *BufferPool) UnpinPage(pageID PageID, isDirty bool) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	bpPage, exists := bp.pages[pageID]
	if !exists {
		return fmt.Errorf("page %d not in buffer pool", pageID)
	}

	if bpPage.pinCount <= 0 {
		return fmt.Errorf("page %d already has pin count 0", pageID)
	}

	bpPage.pinCount--

	// Mark as dirty if requested
	if isDirty {
		wasClean := !bpPage.dirty
		bpPage.dirty = true

		// Track pages dirtied (only count when transitioning from clean to dirty)
		if wasClean {
			bp.stats.mu.Lock()
			bp.stats.PagesDirtied++
			bp.stats.mu.Unlock()
		}
	}

	// If no longer pinned, ensure it's in LRU list
	if bpPage.pinCount == 0 && bpPage.lruNode == nil {
		bpPage.lruNode = bp.lruList.PushFront(pageID)
	}

	return nil
}

// FlushPage writes a page to disk if it's dirty
func (bp *BufferPool) FlushPage(pageID PageID) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	bpPage, exists := bp.pages[pageID]
	if !exists {
		return nil // Page not in buffer, nothing to flush
	}

	if !bpPage.dirty {
		return nil // Page not dirty, nothing to flush
	}

	// Write to disk
	if err := bp.diskManager.WritePage(bpPage.page); err != nil {
		return fmt.Errorf("failed to write page to disk: %w", err)
	}

	bpPage.dirty = false
	return nil
}

// FlushAllPages writes all dirty pages to disk
func (bp *BufferPool) FlushAllPages() error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	for pageID, bpPage := range bp.pages {
		if bpPage.dirty {
			if err := bp.diskManager.WritePage(bpPage.page); err != nil {
				return fmt.Errorf("failed to write page %d: %w", pageID, err)
			}
			bpPage.dirty = false
		}
	}

	return bp.diskManager.Sync()
}

// evictPage evicts the least recently used unpinned page
// Must be called with lock held
func (bp *BufferPool) evictPage() bool {
	// Iterate from back of LRU list (least recently used)
	for elem := bp.lruList.Back(); elem != nil; elem = elem.Prev() {
		pageID := elem.Value.(PageID)
		bpPage := bp.pages[pageID]

		// Can only evict unpinned pages
		if bpPage.pinCount == 0 {
			// Write to disk if dirty
			if bpPage.dirty {
				if err := bp.diskManager.WritePage(bpPage.page); err != nil {
					// Log error but continue trying to evict
					continue
				}
			}

			// Remove from LRU list and map
			bp.lruList.Remove(elem)
			delete(bp.pages, pageID)

			// Clean up page lock
			bp.pageLockMgr.CleanupPageLock(pageID)

			// Track page eviction
			bp.stats.mu.Lock()
			bp.stats.PagesEvicted++
			bp.stats.mu.Unlock()

			return true
		}
	}

	return false // No pages could be evicted
}

// GetPoolSize returns the current number of pages in the buffer pool
func (bp *BufferPool) GetPoolSize() int {
	bp.mu.RLock()
	defer bp.mu.RUnlock()

	return len(bp.pages)
}

// GetPinnedCount returns the number of currently pinned pages
func (bp *BufferPool) GetPinnedCount() int {
	bp.mu.RLock()
	defer bp.mu.RUnlock()

	count := 0
	for _, bpPage := range bp.pages {
		if bpPage.pinCount > 0 {
			count++
		}
	}

	return count
}

// GetMaxPages returns the maximum number of pages the buffer pool can hold
func (bp *BufferPool) GetMaxPages() int {
	return bp.maxPages
}

// GetMemoryUsageBytes estimates current memory usage in bytes
func (bp *BufferPool) GetMemoryUsageBytes() int64 {
	bp.mu.RLock()
	defer bp.mu.RUnlock()

	pageSize := int64(4096) // Assuming 4KB pages
	return int64(len(bp.pages)) * pageSize
}

// GetMaxMemoryBytes returns the maximum memory the buffer pool can use
func (bp *BufferPool) GetMaxMemoryBytes() int64 {
	pageSize := int64(4096) // Assuming 4KB pages
	return int64(bp.maxPages) * pageSize
}

// IsMemoryPressure checks if buffer pool is under memory pressure
func (bp *BufferPool) IsMemoryPressure(threshold float64) bool {
	bp.mu.RLock()
	defer bp.mu.RUnlock()

	usage := float64(len(bp.pages)) / float64(bp.maxPages)
	return usage >= threshold
}

// GetAvailableMemoryBytes returns available memory in bytes
func (bp *BufferPool) GetAvailableMemoryBytes() int64 {
	bp.mu.RLock()
	defer bp.mu.RUnlock()

	pageSize := int64(4096) // Assuming 4KB pages
	availablePages := bp.maxPages - len(bp.pages)
	return int64(availablePages) * pageSize
}

// AcquirePageLock acquires an exclusive lock on a page for modifications
func (bp *BufferPool) AcquirePageLock(pageID PageID) {
	bp.pageLockMgr.AcquirePageLock(pageID)
}

// ReleasePageLock releases the exclusive lock on a page
func (bp *BufferPool) ReleasePageLock(pageID PageID) {
	bp.pageLockMgr.ReleasePageLock(pageID)
}

// GetStats returns a copy of the current buffer pool statistics
func (bp *BufferPool) GetStats() BufferPoolStats {
	bp.stats.mu.RLock()
	defer bp.stats.mu.RUnlock()

	return BufferPoolStats{
		PagesHit:     bp.stats.PagesHit,
		PagesRead:    bp.stats.PagesRead,
		PagesDirtied: bp.stats.PagesDirtied,
		PagesEvicted: bp.stats.PagesEvicted,
	}
}

// ResetStats resets all buffer pool statistics to zero
func (bp *BufferPool) ResetStats() {
	bp.stats.mu.Lock()
	defer bp.stats.mu.Unlock()

	bp.stats.PagesHit = 0
	bp.stats.PagesRead = 0
	bp.stats.PagesDirtied = 0
	bp.stats.PagesEvicted = 0
}
