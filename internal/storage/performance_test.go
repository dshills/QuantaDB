package storage

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"
)

// TestVacuumPerformance tests the enhanced vacuum process
func TestVacuumPerformance(t *testing.T) {
	// Create temporary file for testing
	tempFile, err := os.CreateTemp("", "test_vacuum_*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())
	defer tempFile.Close()

	// Create disk manager
	dm, err := NewDiskManager(tempFile.Name())
	if err != nil {
		t.Fatalf("Failed to create disk manager: %v", err)
	}
	defer dm.Close()

	// Create test pages with some deleted records
	pageCount := 10
	recordsPerPage := 20
	deletionRate := 0.3 // 30% of records will be deleted

	var pages []*Page
	for i := 0; i < pageCount; i++ {
		// Allocate page first
		pageID, err := dm.AllocatePage()
		if err != nil {
			t.Fatalf("Failed to allocate page: %v", err)
		}

		page := NewPage(pageID, PageTypeData)
		slottedPage := &SlottedPage{Page: page}

		// Add records
		for j := 0; j < recordsPerPage; j++ {
			data := []byte(fmt.Sprintf("Record %d-%d: %s", i, j, generateRandomData(100)))
			_, err := slottedPage.AddRecord(data)
			if err != nil {
				t.Errorf("Failed to add record: %v", err)
			}
		}

		// Delete some records to create fragmentation
		for j := 0; j < int(float64(recordsPerPage)*deletionRate); j++ {
			if err := slottedPage.DeleteRecord(uint16(j)); err != nil {
				t.Errorf("Failed to delete record: %v", err)
			}
		}

		pages = append(pages, page)
		if err := dm.WritePage(page); err != nil {
			t.Fatalf("Failed to write page: %v", err)
		}
	}

	// Test page compaction
	t.Run("PageCompaction", func(t *testing.T) {
		for _, page := range pages {
			slottedPage := &SlottedPage{Page: page}

			// Measure free space before compaction
			freeSpaceBefore := page.Header.FreeSpace

			// Perform compaction
			slottedPage.Compact()

			// Measure free space after compaction
			freeSpaceAfter := page.Header.FreeSpace

			// Compaction should increase or maintain free space
			if freeSpaceAfter < freeSpaceBefore {
				t.Errorf("Compaction decreased free space: before=%d, after=%d",
					freeSpaceBefore, freeSpaceAfter)
			}

			t.Logf("Page %d: Free space before=%d, after=%d, reclaimed=%d",
				page.Header.PageID, freeSpaceBefore, freeSpaceAfter,
				freeSpaceAfter-freeSpaceBefore)
		}
	})
}

// BenchmarkCompressionPerformance benchmarks page compression
func BenchmarkCompressionPerformance(b *testing.B) {
	cm := NewCompressionManager()

	// Create test pages with different data patterns
	testCases := []struct {
		name string
		page *Page
	}{
		{
			name: "SparseData",
			page: createTestPageWithPattern("sparse"),
		},
		{
			name: "DenseData",
			page: createTestPageWithPattern("dense"),
		},
		{
			name: "RandomData",
			page: createTestPageWithPattern("random"),
		},
		{
			name: "RepeatedData",
			page: createTestPageWithPattern("repeated"),
		},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				compressedPage, compressed, err := cm.CompressPage(tc.page, CompressionLZ4)
				if err != nil {
					b.Fatalf("Compression failed: %v", err)
				}

				if compressed {
					// Test decompression
					_, err := cm.DecompressPage(compressedPage)
					if err != nil {
						b.Fatalf("Decompression failed: %v", err)
					}
				}
			}
		})
	}
}

// BenchmarkParallelIOPerformance benchmarks parallel I/O operations
func BenchmarkParallelIOPerformance(b *testing.B) {
	// Create temporary file for testing
	tempFile, err := os.CreateTemp("", "test_parallel_io_*.db")
	if err != nil {
		b.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())
	defer tempFile.Close()

	// Create enhanced disk manager
	config := EnhancedDiskManagerConfig{
		CompressionEnabled: true,
		ParallelIOEnabled:  true,
		ReadAheadEnabled:   true,
		ReadAheadSize:      4,
		WorkerCount:        4,
		CacheSize:          100,
	}

	edm, err := NewEnhancedDiskManager(tempFile.Name(), config)
	if err != nil {
		b.Fatalf("Failed to create enhanced disk manager: %v", err)
	}
	defer edm.Close()

	// Create test pages
	pageCount := 100
	var pageIDs []PageID

	for i := 0; i < pageCount; i++ {
		pageID, err := edm.AllocatePage()
		if err != nil {
			b.Fatalf("Failed to allocate page: %v", err)
		}
		pageIDs = append(pageIDs, pageID)

		page := NewPage(pageID, PageTypeData)
		// Fill page with test data
		for j := 0; j < len(page.Data); j++ {
			page.Data[j] = byte(rand.Intn(256))
		}

		if err := edm.WritePage(page); err != nil {
			b.Fatalf("Failed to write page: %v", err)
		}
	}

	b.Run("SequentialRead", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			pageID := pageIDs[i%len(pageIDs)]
			_, err := edm.ReadPage(pageID)
			if err != nil {
				b.Fatalf("Failed to read page: %v", err)
			}
		}
	})

	b.Run("BatchRead", func(b *testing.B) {
		batchSize := 10
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			start := (i * batchSize) % (len(pageIDs) - batchSize)
			batchPageIDs := pageIDs[start : start+batchSize]

			_, err := edm.ReadPagesBatch(batchPageIDs)
			if err != nil {
				b.Fatalf("Failed to read page batch: %v", err)
			}
		}
	})

	b.Run("RandomWrite", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			pageID := pageIDs[rand.Intn(len(pageIDs))]
			page := NewPage(pageID, PageTypeData)

			// Fill with random data
			for j := 0; j < len(page.Data); j++ {
				page.Data[j] = byte(rand.Intn(256))
			}

			if err := edm.WritePage(page); err != nil {
				b.Fatalf("Failed to write page: %v", err)
			}
		}
	})
}

// BenchmarkVacuumScaling tests vacuum performance with different dataset sizes
func BenchmarkVacuumScaling(b *testing.B) {
	sizes := []struct {
		name      string
		pageCount int
		records   int
	}{
		{"Small", 10, 100},
		{"Medium", 50, 500},
		{"Large", 100, 1000},
	}

	for _, size := range sizes {
		b.Run(size.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				// Create temporary file
				tempFile, err := os.CreateTemp("", "benchmark_vacuum_*.db")
				if err != nil {
					b.Fatalf("Failed to create temp file: %v", err)
				}

				// Setup test data
				setupVacuumBenchmarkData(b, tempFile.Name(), size.pageCount, size.records)

				// Measure vacuum performance
				start := time.Now()
				performVacuumBenchmark(b, tempFile.Name())
				elapsed := time.Since(start)

				b.ReportMetric(float64(elapsed.Nanoseconds()), "ns/vacuum")

				// Cleanup
				os.Remove(tempFile.Name())
			}
		})
	}
}

// Helper functions

func generateRandomData(size int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	result := make([]byte, size)
	for i := range result {
		result[i] = charset[rand.Intn(len(charset))]
	}
	return string(result)
}

func createTestPageWithPattern(pattern string) *Page {
	page := NewPage(1, PageTypeData)
	slottedPage := &SlottedPage{Page: page}

	switch pattern {
	case "sparse":
		// Add few records with lots of space
		for i := 0; i < 5; i++ {
			data := []byte(fmt.Sprintf("Record %d", i))
			slottedPage.AddRecord(data)
		}

	case "dense":
		// Fill page with many small records
		for i := 0; page.Header.FreeSpace > 50; i++ {
			data := []byte(fmt.Sprintf("R%d", i))
			if _, err := slottedPage.AddRecord(data); err != nil {
				break
			}
		}

	case "random":
		// Add records with random data
		for i := 0; i < 10; i++ {
			data := []byte(generateRandomData(100 + rand.Intn(200)))
			if _, err := slottedPage.AddRecord(data); err != nil {
				break
			}
		}

	case "repeated":
		// Add records with repeated patterns
		pattern := "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
		for i := 0; i < 20; i++ {
			data := []byte(pattern + pattern) // Repeated pattern for better compression
			if _, err := slottedPage.AddRecord(data); err != nil {
				break
			}
		}
	}

	return page
}

func setupVacuumBenchmarkData(b *testing.B, filename string, pageCount, recordCount int) {
	dm, err := NewDiskManager(filename)
	if err != nil {
		b.Fatalf("Failed to create disk manager: %v", err)
	}
	defer dm.Close()

	// Create pages with fragmented data
	for i := 0; i < pageCount; i++ {
		// Allocate page first
		pageID, err := dm.AllocatePage()
		if err != nil {
			b.Fatalf("Failed to allocate page: %v", err)
		}

		page := NewPage(pageID, PageTypeData)
		slottedPage := &SlottedPage{Page: page}

		// Add records
		for j := 0; j < recordCount/pageCount; j++ {
			data := []byte(fmt.Sprintf("Record %d-%d: %s", i, j, generateRandomData(50)))
			slottedPage.AddRecord(data)
		}

		// Delete every third record to create fragmentation
		for j := 0; j < recordCount/pageCount; j += 3 {
			slottedPage.DeleteRecord(uint16(j))
		}

		dm.WritePage(page)
	}
}

func performVacuumBenchmark(b *testing.B, filename string) {
	dm, err := NewDiskManager(filename)
	if err != nil {
		b.Fatalf("Failed to create disk manager: %v", err)
	}
	defer dm.Close()

	// Simulate vacuum operation by compacting all pages
	for pageID := PageID(1); pageID < dm.GetPageCount(); pageID++ {
		page, err := dm.ReadPage(pageID)
		if err != nil {
			continue
		}

		if page.Header.Type == PageTypeData {
			slottedPage := &SlottedPage{Page: page}
			slottedPage.Compact()
			dm.WritePage(page)
		}
	}
}
