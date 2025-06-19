package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDiskManager(t *testing.T) {
	// Create temporary directory for test files
	tmpDir, err := os.MkdirTemp("", "quantadb_test_")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")

	t.Run("NewDiskManager", func(t *testing.T) {
		dm, err := NewDiskManager(dbPath)
		if err != nil {
			t.Fatalf("failed to create disk manager: %v", err)
		}
		defer dm.Close()

		// Should have metadata page
		if dm.GetPageCount() != 1 {
			t.Errorf("expected page count 1, got %d", dm.GetPageCount())
		}

		// Verify metadata page exists
		page, err := dm.ReadPage(0)
		if err != nil {
			t.Fatalf("failed to read metadata page: %v", err)
		}

		if page.Header.PageID != 0 {
			t.Errorf("expected page ID 0, got %d", page.Header.PageID)
		}
	})

	t.Run("AllocatePage", func(t *testing.T) {
		dm, err := NewDiskManager(dbPath)
		if err != nil {
			t.Fatalf("failed to create disk manager: %v", err)
		}
		defer dm.Close()

		// Allocate first page
		pageID1, err := dm.AllocatePage()
		if err != nil {
			t.Fatalf("failed to allocate page: %v", err)
		}

		if pageID1 != 1 {
			t.Errorf("expected first allocated page ID 1, got %d", pageID1)
		}

		// Allocate second page
		pageID2, err := dm.AllocatePage()
		if err != nil {
			t.Fatalf("failed to allocate second page: %v", err)
		}

		if pageID2 != 2 {
			t.Errorf("expected second allocated page ID 2, got %d", pageID2)
		}

		// Verify page count
		if dm.GetPageCount() != 3 {
			t.Errorf("expected page count 3, got %d", dm.GetPageCount())
		}
	})

	t.Run("ReadWritePage", func(t *testing.T) {
		dm, err := NewDiskManager(dbPath)
		if err != nil {
			t.Fatalf("failed to create disk manager: %v", err)
		}
		defer dm.Close()

		// Allocate a page
		pageID, err := dm.AllocatePage()
		if err != nil {
			t.Fatalf("failed to allocate page: %v", err)
		}

		// Create a page with data
		page := NewPage(pageID, PageTypeData)
		page.Header.LSN = 12345
		page.Header.ItemCount = 5
		testData := []byte("Test data for disk persistence")
		copy(page.Data[:], testData)

		// Write page
		if err := dm.WritePage(page); err != nil {
			t.Fatalf("failed to write page: %v", err)
		}

		// Sync to ensure it's on disk
		if err := dm.Sync(); err != nil {
			t.Fatalf("failed to sync: %v", err)
		}

		// Read page back
		readPage, err := dm.ReadPage(pageID)
		if err != nil {
			t.Fatalf("failed to read page: %v", err)
		}

		// Verify page contents
		if readPage.Header.PageID != pageID {
			t.Errorf("page ID mismatch: expected %d, got %d", pageID, readPage.Header.PageID)
		}

		if readPage.Header.LSN != 12345 {
			t.Errorf("LSN mismatch: expected 12345, got %d", readPage.Header.LSN)
		}

		if readPage.Header.ItemCount != 5 {
			t.Errorf("item count mismatch: expected 5, got %d", readPage.Header.ItemCount)
		}

		// Verify data
		readData := make([]byte, len(testData))
		copy(readData, readPage.Data[:len(testData)])
		if string(readData) != string(testData) {
			t.Errorf("data mismatch: expected %s, got %s", testData, readData)
		}
	})

	t.Run("InvalidPageOperations", func(t *testing.T) {
		dm, err := NewDiskManager(dbPath)
		if err != nil {
			t.Fatalf("failed to create disk manager: %v", err)
		}
		defer dm.Close()

		// Try to read non-existent page
		_, err = dm.ReadPage(999)
		if err == nil {
			t.Error("expected error reading non-existent page")
		}

		// Try to write page with invalid ID
		page := NewPage(999, PageTypeData)
		err = dm.WritePage(page)
		if err == nil {
			t.Error("expected error writing page with invalid ID")
		}
	})

	t.Run("Persistence", func(t *testing.T) {
		// Create disk manager and write some pages
		dm1, err := NewDiskManager(dbPath)
		if err != nil {
			t.Fatalf("failed to create disk manager: %v", err)
		}

		// Allocate and write pages
		pageIDs := make([]PageID, 3)
		for i := 0; i < 3; i++ {
			pageIDs[i], _ = dm1.AllocatePage()
			page := NewPage(pageIDs[i], PageTypeData)
			page.Header.ItemCount = uint16(i + 1)
			dm1.WritePage(page)
		}

		pageCount := dm1.GetPageCount()
		dm1.Close()

		// Open again and verify persistence
		dm2, err := NewDiskManager(dbPath)
		if err != nil {
			t.Fatalf("failed to reopen disk manager: %v", err)
		}
		defer dm2.Close()

		// Verify page count
		if dm2.GetPageCount() != pageCount {
			t.Errorf("page count mismatch: expected %d, got %d", pageCount, dm2.GetPageCount())
		}

		// Verify pages
		for i, pageID := range pageIDs {
			page, err := dm2.ReadPage(pageID)
			if err != nil {
				t.Fatalf("failed to read page %d: %v", pageID, err)
			}

			if page.Header.ItemCount != uint16(i+1) {
				t.Errorf("item count mismatch for page %d: expected %d, got %d",
					pageID, i+1, page.Header.ItemCount)
			}
		}
	})
}
