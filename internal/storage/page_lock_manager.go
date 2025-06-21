package storage

import (
	"sync"
)

// PageLockManager manages locks for pages to prevent concurrent modifications
type PageLockManager struct {
	mu    sync.Mutex
	locks map[PageID]*sync.Mutex
}

// NewPageLockManager creates a new page lock manager
func NewPageLockManager() *PageLockManager {
	return &PageLockManager{
		locks: make(map[PageID]*sync.Mutex),
	}
}

// AcquirePageLock gets or creates a lock for a page and locks it
func (plm *PageLockManager) AcquirePageLock(pageID PageID) {
	plm.mu.Lock()
	lock, exists := plm.locks[pageID]
	if !exists {
		lock = &sync.Mutex{}
		plm.locks[pageID] = lock
	}
	plm.mu.Unlock()

	lock.Lock()
}

// ReleasePageLock unlocks the page
func (plm *PageLockManager) ReleasePageLock(pageID PageID) {
	plm.mu.Lock()
	lock, exists := plm.locks[pageID]
	plm.mu.Unlock()

	if exists {
		lock.Unlock()
	}
}

// CleanupPageLock removes the lock for a page (call when page is evicted)
func (plm *PageLockManager) CleanupPageLock(pageID PageID) {
	plm.mu.Lock()
	delete(plm.locks, pageID)
	plm.mu.Unlock()
}
