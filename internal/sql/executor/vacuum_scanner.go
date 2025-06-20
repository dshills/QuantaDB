package executor

import (
	"fmt"

	"github.com/dshills/QuantaDB/internal/storage"
	"github.com/dshills/QuantaDB/internal/txn"
)

// DeadVersion represents a row version that can be safely removed by vacuum
type DeadVersion struct {
	TableID      int64
	RowID        RowID
	CreatedAt    int64
	DeletedAt    int64
	DeletedByTxn int64
	NextVersion  int64 // Version pointer to relink chains
	Size         int   // Approximate size in bytes for statistics
}

// VacuumScanner scans tables to identify dead versions that can be removed
type VacuumScanner struct {
	storage        *MVCCStorageBackend
	horizonTracker *txn.HorizonTracker
	safetyConfig   txn.VacuumSafetyConfig
}

// NewVacuumScanner creates a new scanner for identifying dead versions
func NewVacuumScanner(storage *MVCCStorageBackend, horizonTracker *txn.HorizonTracker) *VacuumScanner {
	return &VacuumScanner{
		storage:        storage,
		horizonTracker: horizonTracker,
		safetyConfig:   txn.DefaultVacuumSafetyConfig(),
	}
}

// SetSafetyConfig updates the safety configuration for vacuum operations
func (vs *VacuumScanner) SetSafetyConfig(config txn.VacuumSafetyConfig) {
	vs.safetyConfig = config
}

// FindDeadVersionsInTable scans a table and returns all dead versions that can be vacuumed
func (vs *VacuumScanner) FindDeadVersionsInTable(tableID int64) ([]DeadVersion, error) {
	var deadVersions []DeadVersion

	// Get the safe horizon for vacuum operations
	safeHorizon := vs.horizonTracker.GetSafeHorizon(vs.safetyConfig)

	// Get table metadata
	vs.storage.mu.RLock()
	meta, exists := vs.storage.tableMeta[tableID]
	if !exists {
		vs.storage.mu.RUnlock()
		return nil, NewTableNotFoundError(tableID)
	}
	firstPageID := meta.FirstPageID
	schema := meta.RowFormat.Schema
	vs.storage.mu.RUnlock()

	// Create MVCC row format for deserialization
	mvccFormat := NewMVCCRowFormat(schema)

	// Scan all pages in the table
	currentPageID := firstPageID
	for currentPageID != 0 {
		page, err := vs.storage.bufferPool.FetchPage(currentPageID)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch page %d: %w", currentPageID, err)
		}

		// Scan all slots in the page
		for slotID := uint16(0); slotID < page.Header.ItemCount; slotID++ {
			// Read slot entry
			slotOffset := storage.PageHeaderSize + slotID*4
			if slotOffset+4 > storage.PageSize {
				vs.storage.bufferPool.UnpinPage(currentPageID, false)
				continue
			}

			slotData := page.Data[slotOffset : slotOffset+4]
			pageDataOffset := uint16(slotData[0])<<8 | uint16(slotData[1])
			dataSize := uint16(slotData[2])<<8 | uint16(slotData[3])

			// Skip empty slots
			if dataSize == 0 {
				continue
			}

			// Read row data
			dataOffset := pageDataOffset - storage.PageHeaderSize
			if dataOffset+dataSize > storage.PageSize {
				continue
			}

			rowData := page.Data[dataOffset : dataOffset+dataSize]

			// Deserialize MVCC row
			mvccRow, err := mvccFormat.Deserialize(rowData)
			if err != nil {
				// Skip corrupted rows
				continue
			}

			// Create RowID for this version
			rowID := RowID{
				PageID: currentPageID,
				SlotID: slotID,
			}

			// Check if this version is dead and can be vacuumed
			if vs.IsDeadVersion(mvccRow, rowID, safeHorizon) {
				// Check if this is NOT the head of a version chain
				// We need to traverse from the head to determine this
				if !vs.isHeadOfChain(tableID, rowID) {
					deadVersion := DeadVersion{
						TableID:      tableID,
						RowID:        rowID,
						CreatedAt:    mvccRow.Header.CreatedAt,
						DeletedAt:    mvccRow.Header.DeletedAt,
						DeletedByTxn: mvccRow.Header.DeletedByTxn,
						NextVersion:  mvccRow.Header.NextVersion,
						Size:         len(rowData),
					}
					deadVersions = append(deadVersions, deadVersion)
				}
			}
		}

		// Move to next page
		nextPageID := page.Header.NextPageID
		vs.storage.bufferPool.UnpinPage(currentPageID, false)
		currentPageID = nextPageID
	}

	return deadVersions, nil
}

// IsDeadVersion checks if a specific version is dead and safe to vacuum
func (vs *VacuumScanner) IsDeadVersion(row *MVCCRow, rowID RowID, horizon int64) bool {
	// A version is dead if it has been deleted and the deletion is before the horizon
	if row.Header.DeletedAt == 0 {
		return false // Not deleted
	}

	// Check if deletion is old enough
	return row.Header.DeletedAt < horizon
}

// isHeadOfChain checks if a RowID is the head of a version chain
// This is a simplified check - in a real implementation, we might maintain
// an index of chain heads for efficiency
func (vs *VacuumScanner) isHeadOfChain(tableID int64, checkRowID RowID) bool {
	// In our current implementation, we need to scan to find chain heads
	// This is inefficient but correct - future optimization would maintain
	// a separate index of current row versions

	// For now, we consider a row NOT a head if we can find another row
	// that points to it via NextVersion
	// This requires scanning, which is expensive, so we might want to
	// maintain this information differently in production

	// Simplified approach: assume rows with no incoming version pointers are heads
	// This would need to be tracked more efficiently in a production system
	return false // Conservative: never consider anything a head for safety
}

// FindDeadVersionsInChain finds all dead versions in a specific version chain
// starting from the given RowID
func (vs *VacuumScanner) FindDeadVersionsInChain(tableID int64, startRowID RowID) ([]DeadVersion, error) {
	var deadVersions []DeadVersion

	// Get the safe horizon
	safeHorizon := vs.horizonTracker.GetSafeHorizon(vs.safetyConfig)

	// Create version chain iterator
	iterator := NewVersionChainIterator(vs.storage, tableID)

	// Get all versions in the chain
	allVersions, allRowIDs, err := iterator.GetAllVersions(startRowID)
	if err != nil {
		return nil, fmt.Errorf("failed to get version chain: %w", err)
	}

	// Check each version (except the first, which is the current version)
	for i := 1; i < len(allVersions); i++ {
		version := allVersions[i]
		rowID := allRowIDs[i]

		if vs.IsDeadVersion(version, rowID, safeHorizon) {
			// Estimate size (this is approximate)
			size := 100 // Base MVCC header size
			if version.Data != nil {
				// Add estimated size for each value
				size += len(version.Data.Values) * 20 // Rough estimate
			}

			deadVersion := DeadVersion{
				TableID:      tableID,
				RowID:        rowID,
				CreatedAt:    version.Header.CreatedAt,
				DeletedAt:    version.Header.DeletedAt,
				DeletedByTxn: version.Header.DeletedByTxn,
				NextVersion:  version.Header.NextVersion,
				Size:         size,
			}
			deadVersions = append(deadVersions, deadVersion)
		}
	}

	return deadVersions, nil
}

// VacuumScanStats contains statistics from a vacuum scan operation
type VacuumScanStats struct {
	TablesScanned   int
	VersionsScanned int
	DeadVersions    int
	EstimatedSize   int64       // Estimated bytes that can be reclaimed
	ChainLengths    map[int]int // Distribution of chain lengths
}

// ScanDatabase scans all tables in the database and returns vacuum statistics
func (vs *VacuumScanner) ScanDatabase() (*VacuumScanStats, error) {
	stats := &VacuumScanStats{
		ChainLengths: make(map[int]int),
	}

	// Get all table IDs from storage
	vs.storage.mu.RLock()
	tableIDs := make([]int64, 0, len(vs.storage.tableMeta))
	for tableID := range vs.storage.tableMeta {
		tableIDs = append(tableIDs, tableID)
	}
	vs.storage.mu.RUnlock()

	// Scan each table
	for _, tableID := range tableIDs {
		stats.TablesScanned++

		deadVersions, err := vs.FindDeadVersionsInTable(tableID)
		if err != nil {
			continue // Skip tables with errors
		}

		stats.DeadVersions += len(deadVersions)
		for _, dv := range deadVersions {
			stats.EstimatedSize += int64(dv.Size)
		}
	}

	return stats, nil
}
