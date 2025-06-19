package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestBufferPool(t *testing.T) {
	// Create temporary directory for test files
	tmpDir, err := os.MkdirTemp("", "quantadb_bufpool_test_")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")

	t.Run("BasicOperations", func(t *testing.T) {
		dm, err := NewDiskManager(dbPath)
		if err != nil {
			t.Fatalf("failed to create disk manager: %v", err)
		}
		defer dm.Close()

		// Create buffer pool with capacity for 3 pages
		bp := NewBufferPool(dm, 3)

		// Create a new page
		page1, err := bp.NewPage()
		if err != nil {
			t.Fatalf("failed to create new page: %v", err)
		}

		pageID1 := page1.Header.PageID

		// Write some data to the page
		testData := []byte("Hello, Buffer Pool!")
		copy(page1.Data[:], testData)

		// Unpin the page (mark as dirty)
		if err := bp.UnpinPage(pageID1, true); err != nil {
			t.Fatalf("failed to unpin page: %v", err)
		}

		// Fetch the page again
		page1Again, err := bp.FetchPage(pageID1)
		if err != nil {
			t.Fatalf("failed to fetch page: %v", err)
		}

		// Verify data is still there
		readData := make([]byte, len(testData))
		copy(readData, page1Again.Data[:len(testData)])
		if string(readData) != string(testData) {
			t.Errorf("data mismatch: expected %s, got %s", testData, readData)
		}

		// Unpin again
		if err := bp.UnpinPage(pageID1, false); err != nil {
			t.Fatalf("failed to unpin page again: %v", err)
		}
	})

	t.Run("LRUEviction", func(t *testing.T) {
		dm, err := NewDiskManager(dbPath)
		if err != nil {
			t.Fatalf("failed to create disk manager: %v", err)
		}
		defer dm.Close()

		// Create buffer pool with capacity for 2 pages
		bp := NewBufferPool(dm, 2)

		// Create 3 pages (will trigger eviction)
		pages := make([]*Page, 3)
		pageIDs := make([]PageID, 3)

		for i := 0; i < 3; i++ {
			page, err := bp.NewPage()
			if err != nil {
				t.Fatalf("failed to create page %d: %v", i, err)
			}

			pages[i] = page
			pageIDs[i] = page.Header.PageID

			// Write unique data to each page
			data := []byte{byte(i), byte(i), byte(i)}
			copy(page.Data[:], data)

			// Unpin the page
			if err := bp.UnpinPage(pageIDs[i], true); err != nil {
				t.Fatalf("failed to unpin page %d: %v", i, err)
			}
		}

		// Buffer pool should have 2 pages (page 0 was evicted)
		if bp.GetPoolSize() != 2 {
			t.Errorf("expected pool size 2, got %d", bp.GetPoolSize())
		}

		// Fetch page 0 again (should trigger eviction of page 1)
		page0, err := bp.FetchPage(pageIDs[0])
		if err != nil {
			t.Fatalf("failed to fetch page 0: %v", err)
		}

		// Verify page 0 data was persisted
		if page0.Data[0] != 0 || page0.Data[1] != 0 || page0.Data[2] != 0 {
			t.Error("page 0 data was not persisted correctly")
		}

		bp.UnpinPage(pageIDs[0], false)

		// Pool should still have 2 pages
		if bp.GetPoolSize() != 2 {
			t.Errorf("expected pool size 2, got %d", bp.GetPoolSize())
		}
	})

	t.Run("PinnedPagesNotEvicted", func(t *testing.T) {
		dm, err := NewDiskManager(dbPath)
		if err != nil {
			t.Fatalf("failed to create disk manager: %v", err)
		}
		defer dm.Close()

		// Create buffer pool with capacity for 2 pages
		bp := NewBufferPool(dm, 2)

		// Create and pin 2 pages
		page1, _ := bp.NewPage()
		page2, _ := bp.NewPage()
		pageID1 := page1.Header.PageID
		pageID2 := page2.Header.PageID

		// Keep both pages pinned

		// Try to create a third page - should fail because no pages can be evicted
		_, err = bp.NewPage()
		if err == nil {
			t.Error("expected error when trying to add page to full buffer with all pages pinned")
		}

		// Unpin one page
		bp.UnpinPage(pageID1, false)

		// Now we should be able to create a new page
		page3, err := bp.NewPage()
		if err != nil {
			t.Fatalf("failed to create page after unpinning: %v", err)
		}

		// Verify we have the expected pages
		if bp.GetPoolSize() != 2 {
			t.Errorf("expected pool size 2, got %d", bp.GetPoolSize())
		}

		// Clean up
		bp.UnpinPage(pageID2, false)
		bp.UnpinPage(page3.Header.PageID, false)
	})

	t.Run("FlushDirtyPages", func(t *testing.T) {
		dm, err := NewDiskManager(dbPath)
		if err != nil {
			t.Fatalf("failed to create disk manager: %v", err)
		}
		defer dm.Close()

		bp := NewBufferPool(dm, 5)

		// Create a page and modify it
		page, _ := bp.NewPage()
		pageID := page.Header.PageID

		testData := []byte("Dirty page data")
		copy(page.Data[:], testData)

		// Mark as dirty when unpinning
		bp.UnpinPage(pageID, true)

		// Flush all pages
		if err := bp.FlushAllPages(); err != nil {
			t.Fatalf("failed to flush pages: %v", err)
		}

		// Create a new buffer pool and disk manager to verify persistence
		dm2, err := NewDiskManager(dbPath)
		if err != nil {
			t.Fatalf("failed to reopen disk manager: %v", err)
		}
		defer dm2.Close()

		bp2 := NewBufferPool(dm2, 5)

		// Fetch the page
		page2, err := bp2.FetchPage(pageID)
		if err != nil {
			t.Fatalf("failed to fetch page in new buffer pool: %v", err)
		}

		// Verify data was persisted
		readData := make([]byte, len(testData))
		copy(readData, page2.Data[:len(testData)])
		if string(readData) != string(testData) {
			t.Errorf("persisted data mismatch: expected %s, got %s", testData, readData)
		}

		bp2.UnpinPage(pageID, false)
	})

	t.Run("ConcurrentAccess", func(t *testing.T) {
		dm, err := NewDiskManager(dbPath)
		if err != nil {
			t.Fatalf("failed to create disk manager: %v", err)
		}
		defer dm.Close()

		bp := NewBufferPool(dm, 10)

		// Create a page
		page, _ := bp.NewPage()
		pageID := page.Header.PageID
		bp.UnpinPage(pageID, false)

		// Simulate concurrent access
		done := make(chan bool, 10)

		for i := 0; i < 10; i++ {
			go func(id int) {
				defer func() { done <- true }()

				// Fetch page
				p, err := bp.FetchPage(pageID)
				if err != nil {
					t.Errorf("goroutine %d: failed to fetch page: %v", id, err)
					return
				}

				// Do some work
				p.Data[id] = byte(id)

				// Unpin
				if err := bp.UnpinPage(pageID, true); err != nil {
					t.Errorf("goroutine %d: failed to unpin: %v", id, err)
				}
			}(i)
		}

		// Wait for all goroutines
		for i := 0; i < 10; i++ {
			<-done
		}

		// Verify all writes succeeded
		finalPage, _ := bp.FetchPage(pageID)
		for i := 0; i < 10; i++ {
			if finalPage.Data[i] != byte(i) {
				t.Errorf("expected data[%d] = %d, got %d", i, i, finalPage.Data[i])
			}
		}
		bp.UnpinPage(pageID, false)
	})
}
