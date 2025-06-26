package storage

import (
	"fmt"
	"sync"

	"github.com/pierrec/lz4/v4"
)

// CompressionType represents different compression algorithms
type CompressionType uint8

const (
	CompressionNone CompressionType = iota
	CompressionLZ4
	CompressionSnappy // Future enhancement
)

// CompressionStats tracks compression performance metrics
type CompressionStats struct {
	CompressedPages   int64
	DecompressedPages int64
	BytesCompressed   int64
	BytesDecompressed int64
	CompressionRatio  float64
	TotalSavings      int64
	mu                sync.RWMutex
}

// CompressedPage represents a page that has been compressed
type CompressedPage struct {
	OriginalSize    uint16
	CompressedSize  uint16
	CompressionType CompressionType
	CompressedData  []byte
	Header          PageHeader // Uncompressed header for fast access
}

// Compressor interface for different compression algorithms
type Compressor interface {
	Compress(data []byte) ([]byte, error)
	Decompress(data []byte, originalSize int) ([]byte, error)
	Type() CompressionType
}

// LZ4Compressor implements LZ4 compression
type LZ4Compressor struct{}

// NewLZ4Compressor creates a new LZ4 compressor
func NewLZ4Compressor() *LZ4Compressor {
	return &LZ4Compressor{}
}

// Compress compresses data using LZ4
func (c *LZ4Compressor) Compress(data []byte) ([]byte, error) {
	dst := make([]byte, lz4.CompressBlockBound(len(data)))

	n, err := lz4.CompressBlock(data, dst, nil)
	if err != nil {
		return nil, fmt.Errorf("LZ4 compression failed: %w", err)
	}

	return dst[:n], nil
}

// Decompress decompresses LZ4 data
func (c *LZ4Compressor) Decompress(data []byte, originalSize int) ([]byte, error) {
	dst := make([]byte, originalSize)

	n, err := lz4.UncompressBlock(data, dst)
	if err != nil {
		return nil, fmt.Errorf("LZ4 decompression failed: %w", err)
	}

	if n != originalSize {
		return nil, fmt.Errorf("LZ4 decompression size mismatch: expected %d, got %d", originalSize, n)
	}

	return dst, nil
}

// Type returns the compression type
func (c *LZ4Compressor) Type() CompressionType {
	return CompressionLZ4
}

// CompressionManager manages page compression and decompression
type CompressionManager struct {
	compressors map[CompressionType]Compressor
	stats       *CompressionStats
	threshold   float64 // Minimum compression ratio to keep compressed (e.g., 0.8 = 20% reduction)
	mu          sync.RWMutex
}

// NewCompressionManager creates a new compression manager
func NewCompressionManager() *CompressionManager {
	cm := &CompressionManager{
		compressors: make(map[CompressionType]Compressor),
		stats:       &CompressionStats{},
		threshold:   0.8, // Only keep pages that compress to 80% or less
	}

	// Register default compressors
	cm.compressors[CompressionLZ4] = NewLZ4Compressor()

	return cm
}

// SetCompressionThreshold sets the minimum compression ratio to keep compressed
func (cm *CompressionManager) SetCompressionThreshold(threshold float64) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.threshold = threshold
}

// CompressPage compresses a page if beneficial
func (cm *CompressionManager) CompressPage(page *Page, compressionType CompressionType) (*CompressedPage, bool, error) {
	cm.mu.RLock()
	compressor, exists := cm.compressors[compressionType]
	cm.mu.RUnlock()

	if !exists {
		return nil, false, fmt.Errorf("unsupported compression type: %v", compressionType)
	}

	// Only compress the data portion, keep header uncompressed for fast access
	dataToCompress := page.Data[:]
	originalSize := len(dataToCompress)

	compressedData, err := compressor.Compress(dataToCompress)
	if err != nil {
		return nil, false, fmt.Errorf("compression failed: %w", err)
	}

	compressedSize := len(compressedData)
	compressionRatio := float64(compressedSize) / float64(originalSize)

	// Only keep compressed if we achieved sufficient compression
	if compressionRatio >= cm.threshold {
		return nil, false, nil // Compression not beneficial
	}

	// Check for overflow before converting
	if originalSize > 65535 || compressedSize > 65535 {
		return nil, false, fmt.Errorf("data size too large for compression: original=%d, compressed=%d", originalSize, compressedSize)
	}

	compressedPage := &CompressedPage{
		OriginalSize:    uint16(originalSize),   //nolint:gosec // Overflow checked above
		CompressedSize:  uint16(compressedSize), //nolint:gosec // Overflow checked above
		CompressionType: compressionType,
		CompressedData:  compressedData,
		Header:          page.Header, // Keep header uncompressed
	}

	// Update statistics
	cm.updateCompressionStats(int64(originalSize), int64(compressedSize))

	return compressedPage, true, nil
}

// DecompressPage decompresses a compressed page
func (cm *CompressionManager) DecompressPage(compressedPage *CompressedPage) (*Page, error) {
	cm.mu.RLock()
	compressor, exists := cm.compressors[compressedPage.CompressionType]
	cm.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("unsupported compression type: %v", compressedPage.CompressionType)
	}

	decompressedData, err := compressor.Decompress(
		compressedPage.CompressedData,
		int(compressedPage.OriginalSize),
	)
	if err != nil {
		return nil, fmt.Errorf("decompression failed: %w", err)
	}

	page := &Page{
		Header: compressedPage.Header,
	}
	copy(page.Data[:], decompressedData)

	// Update statistics
	cm.updateDecompressionStats(int64(compressedPage.OriginalSize))

	return page, nil
}

// updateCompressionStats updates compression statistics
func (cm *CompressionManager) updateCompressionStats(originalSize, compressedSize int64) {
	cm.stats.mu.Lock()
	defer cm.stats.mu.Unlock()

	cm.stats.CompressedPages++
	cm.stats.BytesCompressed += originalSize
	cm.stats.TotalSavings += originalSize - compressedSize

	// Recalculate compression ratio
	if cm.stats.BytesCompressed > 0 {
		cm.stats.CompressionRatio = float64(cm.stats.BytesCompressed-cm.stats.TotalSavings) / float64(cm.stats.BytesCompressed)
	}
}

// updateDecompressionStats updates decompression statistics
func (cm *CompressionManager) updateDecompressionStats(originalSize int64) {
	cm.stats.mu.Lock()
	defer cm.stats.mu.Unlock()

	cm.stats.DecompressedPages++
	cm.stats.BytesDecompressed += originalSize
}

// GetStats returns a copy of compression statistics
func (cm *CompressionManager) GetStats() CompressionStats {
	cm.stats.mu.RLock()
	defer cm.stats.mu.RUnlock()

	// Return a proper copy to avoid lock copying
	return CompressionStats{
		CompressedPages:   cm.stats.CompressedPages,
		DecompressedPages: cm.stats.DecompressedPages,
		BytesCompressed:   cm.stats.BytesCompressed,
		BytesDecompressed: cm.stats.BytesDecompressed,
		CompressionRatio:  cm.stats.CompressionRatio,
		TotalSavings:      cm.stats.TotalSavings,
	}
}

// ShouldCompress determines if a page should be compressed based on its characteristics
func (cm *CompressionManager) ShouldCompress(page *Page) bool {
	// Don't compress very small pages (little benefit)
	if page.Header.FreeSpace > PageSize/2 {
		return false
	}

	// Don't compress index pages (accessed frequently)
	if page.Header.Type == PageTypeIndex {
		return false
	}

	// Don't compress pages that are already mostly empty
	usedSpace := PageSize - PageHeaderSize - page.Header.FreeSpace
	if float64(usedSpace)/float64(PageSize) < 0.3 {
		return false
	}

	// Compress data pages that are reasonably full
	return page.Header.Type == PageTypeData
}

// EstimateCompressionBenefit estimates potential compression savings without actually compressing
func (cm *CompressionManager) EstimateCompressionBenefit(page *Page) float64 {
	// Simple heuristic based on data patterns
	usedSpace := PageSize - PageHeaderSize - page.Header.FreeSpace

	// Estimate compression ratio based on typical patterns
	// This is a rough estimate - actual compression may vary
	if usedSpace < PageSize/4 {
		return 0.9 // Low compression for sparse data
	} else if usedSpace < PageSize/2 {
		return 0.7 // Moderate compression
	}
	return 0.6 // Better compression for denser data
}
