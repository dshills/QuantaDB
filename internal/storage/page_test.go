package storage

import (
	"bytes"
	"testing"
)

func TestPage(t *testing.T) {
	t.Run("NewPage", func(t *testing.T) {
		page := NewPage(42, PageTypeData)

		if page.Header.PageID != 42 {
			t.Errorf("expected page ID 42, got %d", page.Header.PageID)
		}

		if page.Header.Type != PageTypeData {
			t.Errorf("expected page type %d, got %d", PageTypeData, page.Header.Type)
		}

		if page.Header.FreeSpace != MaxPayloadSize {
			t.Errorf("expected free space %d, got %d", MaxPayloadSize, page.Header.FreeSpace)
		}
	})

	t.Run("Serialize/Deserialize", func(t *testing.T) {
		// Create a page with some data
		page := NewPage(123, PageTypeIndex)
		page.Header.LSN = 456789
		page.Header.ItemCount = 10
		page.Header.FreeSpace = 1000

		// Write some test data
		testData := []byte("Hello, World!")
		copy(page.Data[:], testData)

		// Serialize
		buf := page.Serialize()
		if len(buf) != PageSize {
			t.Errorf("expected serialized size %d, got %d", PageSize, len(buf))
		}

		// Deserialize into new page
		page2 := &Page{}
		if err := page2.Deserialize(buf); err != nil {
			t.Fatalf("failed to deserialize: %v", err)
		}

		// Verify header fields
		if page2.Header.PageID != page.Header.PageID {
			t.Errorf("page ID mismatch: expected %d, got %d", page.Header.PageID, page2.Header.PageID)
		}

		if page2.Header.Type != page.Header.Type {
			t.Errorf("page type mismatch: expected %d, got %d", page.Header.Type, page2.Header.Type)
		}

		if page2.Header.LSN != page.Header.LSN {
			t.Errorf("LSN mismatch: expected %d, got %d", page.Header.LSN, page2.Header.LSN)
		}

		// Verify data
		if !bytes.Equal(page2.Data[:len(testData)], testData) {
			t.Errorf("data mismatch: expected %v, got %v", testData, page2.Data[:len(testData)])
		}
	})

	t.Run("HasSpaceFor", func(t *testing.T) {
		page := NewPage(1, PageTypeData)

		// Should have space for small records
		if !page.HasSpaceFor(100) {
			t.Error("expected to have space for 100 bytes")
		}

		// Should not have space for more than max payload
		if page.HasSpaceFor(MaxPayloadSize + 1) {
			t.Error("should not have space for more than max payload")
		}

		// Simulate using some space
		page.Header.FreeSpace = 50

		// Should have space for 46 bytes (50 - 4 for slot)
		if !page.HasSpaceFor(46) {
			t.Error("expected to have space for 46 bytes")
		}

		// Should not have space for 47 bytes
		if page.HasSpaceFor(47) {
			t.Error("should not have space for 47 bytes")
		}
	})
}

func TestSlottedPage(t *testing.T) {
	t.Run("AddRecord", func(t *testing.T) {
		sp := NewSlottedPage(1)

		// Add first record
		data1 := []byte("First record")
		slot1, err := sp.AddRecord(data1)
		if err != nil {
			t.Fatalf("failed to add first record: %v", err)
		}

		if slot1 != 0 {
			t.Errorf("expected first slot to be 0, got %d", slot1)
		}

		// Add second record
		data2 := []byte("Second record with more data")
		slot2, err := sp.AddRecord(data2)
		if err != nil {
			t.Fatalf("failed to add second record: %v", err)
		}

		if slot2 != 1 {
			t.Errorf("expected second slot to be 1, got %d", slot2)
		}

		// Verify item count
		if sp.Header.ItemCount != 2 {
			t.Errorf("expected item count 2, got %d", sp.Header.ItemCount)
		}

		// Verify free space decreased
		expectedFree := MaxPayloadSize - uint16(len(data1)) - uint16(len(data2)) - 2*SlotSize
		if sp.Header.FreeSpace != expectedFree {
			t.Errorf("expected free space %d, got %d", expectedFree, sp.Header.FreeSpace)
		}
	})

	t.Run("GetRecord", func(t *testing.T) {
		sp := NewSlottedPage(1)

		// Add records
		data1 := []byte("Record 1")
		data2 := []byte("Record 2 with more content")

		slot1, _ := sp.AddRecord(data1)
		slot2, _ := sp.AddRecord(data2)

		// Get first record
		retrieved1, err := sp.GetRecord(slot1)
		if err != nil {
			t.Fatalf("failed to get record 1: %v", err)
		}

		if !bytes.Equal(retrieved1, data1) {
			t.Errorf("record 1 mismatch: expected %v, got %v", data1, retrieved1)
		}

		// Get second record
		retrieved2, err := sp.GetRecord(slot2)
		if err != nil {
			t.Fatalf("failed to get record 2: %v", err)
		}

		if !bytes.Equal(retrieved2, data2) {
			t.Errorf("record 2 mismatch: expected %v, got %v", data2, retrieved2)
		}

		// Try to get invalid slot
		_, err = sp.GetRecord(10)
		if err == nil {
			t.Error("expected error for invalid slot")
		}
	})

	t.Run("UpdateRecord", func(t *testing.T) {
		sp := NewSlottedPage(1)

		// Add record
		original := []byte("Original data")
		slot, _ := sp.AddRecord(original)

		// Update with smaller data
		smaller := []byte("Small")
		if err := sp.UpdateRecord(slot, smaller); err != nil {
			t.Fatalf("failed to update with smaller data: %v", err)
		}

		// Verify update
		retrieved, _ := sp.GetRecord(slot)
		if !bytes.Equal(retrieved, smaller) {
			t.Errorf("expected %v, got %v", smaller, retrieved)
		}

		// Try to update with larger data (should fail)
		larger := []byte("This is much larger than the original data")
		if err := sp.UpdateRecord(slot, larger); err == nil {
			t.Error("expected error when updating with larger data")
		}
	})

	t.Run("DeleteRecord", func(t *testing.T) {
		sp := NewSlottedPage(1)

		// Add record
		data := []byte("To be deleted")
		slot, _ := sp.AddRecord(data)
		freeBeforeDelete := sp.Header.FreeSpace

		// Delete record
		if err := sp.DeleteRecord(slot); err != nil {
			t.Fatalf("failed to delete record: %v", err)
		}

		// Verify free space increased
		if sp.Header.FreeSpace != freeBeforeDelete+uint16(len(data)) {
			t.Error("free space not properly updated after delete")
		}

		// Try to get deleted record
		_, err := sp.GetRecord(slot)
		if err == nil {
			t.Error("expected error when getting deleted record")
		}
	})

	t.Run("PageOverflow", func(t *testing.T) {
		sp := NewSlottedPage(1)

		// Strategy: Fill the page with large records first, then progressively smaller ones
		// to ensure we truly exhaust all space

		// First, add large records
		largeSize := 1000
		largeData := make([]byte, largeSize)
		for i := range largeData {
			largeData[i] = byte(i % 256)
		}

		largeCount := 0
		for {
			_, err := sp.AddRecord(largeData)
			if err != nil {
				break
			}
			largeCount++
			if largeCount > 100 {
				t.Fatal("too many large records")
			}
		}

		// Then add medium records
		mediumSize := 100
		mediumData := make([]byte, mediumSize)
		mediumCount := 0
		for {
			_, err := sp.AddRecord(mediumData)
			if err != nil {
				break
			}
			mediumCount++
			if mediumCount > 100 {
				t.Fatal("too many medium records")
			}
		}

		// Then add small records to fill remaining space
		smallSize := 10
		smallData := make([]byte, smallSize)
		smallCount := 0
		for {
			_, err := sp.AddRecord(smallData)
			if err != nil {
				break
			}
			smallCount++
			if smallCount > 1000 {
				t.Fatal("too many small records")
			}
		}

		// Finally try tiny records
		tinySize := 1
		tinyData := make([]byte, tinySize)
		tinyCount := 0
		for {
			_, err := sp.AddRecord(tinyData)
			if err != nil {
				break
			}
			tinyCount++
			if tinyCount > 1000 {
				t.Fatal("too many tiny records")
			}
		}

		t.Logf("Added %d large + %d medium + %d small + %d tiny records, free space: %d",
			largeCount, mediumCount, smallCount, tinyCount, sp.Header.FreeSpace)

		// Now the page should truly be full - we can't even add a 1-byte record
		finalTest := make([]byte, 1)
		_, err := sp.AddRecord(finalTest)
		if err == nil {
			t.Errorf("expected error when page is truly full, but successfully added 1 byte (free space: %d)",
				sp.Header.FreeSpace)
		}
	})
}
