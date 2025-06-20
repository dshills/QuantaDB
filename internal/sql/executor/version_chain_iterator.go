package executor

import (
	"fmt"

	"github.com/dshills/QuantaDB/internal/storage"
)

// VersionChainIterator provides methods to traverse MVCC version chains
type VersionChainIterator struct {
	storage MVCCStorageBackend
	tableID int64
}

// NewVersionChainIterator creates a new version chain iterator
func NewVersionChainIterator(storage *MVCCStorageBackend, tableID int64) *VersionChainIterator {
	return &VersionChainIterator{
		storage: *storage,
		tableID: tableID,
	}
}

// FindVisibleVersion traverses the version chain starting from the given RowID
// and returns the first version that is visible at the given snapshot timestamp.
// Returns the visible row and its RowID, or nil if no visible version exists.
func (vci *VersionChainIterator) FindVisibleVersion(startRowID RowID, snapshotTS int64) (*MVCCRow, RowID, error) {
	currentRowID := startRowID
	visitedRows := make(map[RowID]bool) // Cycle detection
	maxChainLength := 100               // Prevent infinite loops

	for i := 0; i < maxChainLength; i++ {
		// Check for cycles
		if visitedRows[currentRowID] {
			return nil, RowID{}, fmt.Errorf("version chain cycle detected at RowID %v", currentRowID)
		}
		visitedRows[currentRowID] = true

		// Get the current version
		currentRow, err := vci.storage.GetMVCCRow(vci.tableID, currentRowID)
		if err != nil {
			return nil, RowID{}, fmt.Errorf("failed to get row at RowID %v: %w", currentRowID, err)
		}

		// Check if this version is visible at the snapshot timestamp
		if vci.isVisible(currentRow, snapshotTS) {
			return currentRow, currentRowID, nil
		}

		// If not visible, follow the chain to the previous version
		if currentRow.Header.NextVersion == 0 {
			// End of chain, no visible version found
			return nil, RowID{}, nil
		}

		// Decode the next version pointer
		pageID, slotID := DecodeVersionPointer(currentRow.Header.NextVersion)
		currentRowID = RowID{
			PageID: storage.PageID(pageID),
			SlotID: slotID,
		}
	}

	return nil, RowID{}, fmt.Errorf("version chain too long (>%d versions)", maxChainLength)
}

// GetAllVersions returns all versions in a version chain starting from the given RowID.
// This is primarily useful for debugging and administrative purposes.
func (vci *VersionChainIterator) GetAllVersions(startRowID RowID) ([]*MVCCRow, []RowID, error) {
	var versions []*MVCCRow
	var rowIDs []RowID

	currentRowID := startRowID
	visitedRows := make(map[RowID]bool) // Cycle detection
	maxChainLength := 100               // Prevent infinite loops

	for i := 0; i < maxChainLength; i++ {
		// Check for cycles
		if visitedRows[currentRowID] {
			return nil, nil, fmt.Errorf("version chain cycle detected at RowID %v", currentRowID)
		}
		visitedRows[currentRowID] = true

		// Get the current version
		currentRow, err := vci.storage.GetMVCCRow(vci.tableID, currentRowID)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get row at RowID %v: %w", currentRowID, err)
		}

		// Add to results
		versions = append(versions, currentRow)
		rowIDs = append(rowIDs, currentRowID)

		// Follow the chain to the previous version
		if currentRow.Header.NextVersion == 0 {
			// End of chain
			break
		}

		// Decode the next version pointer
		pageID, slotID := DecodeVersionPointer(currentRow.Header.NextVersion)
		currentRowID = RowID{
			PageID: storage.PageID(pageID),
			SlotID: slotID,
		}
	}

	if len(versions) >= maxChainLength {
		return nil, nil, fmt.Errorf("version chain too long (>%d versions)", maxChainLength)
	}

	return versions, rowIDs, nil
}

// isVisible checks if a row version is visible at the given snapshot timestamp
// This uses the same logic as the MVCCStorageBackend.isVisible method
func (vci *VersionChainIterator) isVisible(row *MVCCRow, snapshotTS int64) bool {
	// Row is visible if:
	// 1. It was created before or at our snapshot timestamp
	// 2. It hasn't been deleted, or was deleted after our snapshot

	// Check creation timestamp
	if row.Header.CreatedAt > snapshotTS {
		return false // Created after our snapshot
	}

	// Check deletion timestamp
	if row.Header.DeletedAt > 0 && row.Header.DeletedAt <= snapshotTS {
		return false // Deleted before or at our snapshot
	}

	return true
}

// GetChainLength returns the length of the version chain starting from the given RowID
func (vci *VersionChainIterator) GetChainLength(startRowID RowID) (int, error) {
	currentRowID := startRowID
	visitedRows := make(map[RowID]bool)
	maxChainLength := 100
	length := 0

	for i := 0; i < maxChainLength; i++ {
		// Check for cycles
		if visitedRows[currentRowID] {
			return 0, fmt.Errorf("version chain cycle detected at RowID %v", currentRowID)
		}
		visitedRows[currentRowID] = true
		length++

		// Get the current version
		currentRow, err := vci.storage.GetMVCCRow(vci.tableID, currentRowID)
		if err != nil {
			return 0, fmt.Errorf("failed to get row at RowID %v: %w", currentRowID, err)
		}

		// Follow the chain to the previous version
		if currentRow.Header.NextVersion == 0 {
			// End of chain
			break
		}

		// Decode the next version pointer
		pageID, slotID := DecodeVersionPointer(currentRow.Header.NextVersion)
		currentRowID = RowID{
			PageID: storage.PageID(pageID),
			SlotID: slotID,
		}
	}

	if length >= maxChainLength {
		return 0, fmt.Errorf("version chain too long (>%d versions)", maxChainLength)
	}

	return length, nil
}

// VersionChainStats contains statistics about version chains in a table
type VersionChainStats struct {
	TotalChains       int     // Number of distinct version chains
	AverageLength     float64 // Average length of version chains
	MaxLength         int     // Length of the longest chain
	TotalVersions     int     // Total number of row versions
	DeadVersions      int     // Number of versions that are not visible to any current transaction
	ChainsWithUpdates int     // Number of chains that have more than one version
}

// ValidateVersionChain verifies the integrity of a version chain starting from the given RowID
// This function checks for:
// - Cycle detection (no infinite loops)
// - Chain length limits
// - Timestamp ordering (newer versions have newer timestamps)
// - Proper transaction metadata
func (vci *VersionChainIterator) ValidateVersionChain(startRowID RowID) (*VersionChainValidationResult, error) {
	result := &VersionChainValidationResult{
		StartRowID:  startRowID,
		ChainLength: 0,
		Errors:      []string{},
		Warnings:    []string{},
		IsValid:     true,
	}

	currentRowID := startRowID
	visitedRows := make(map[RowID]bool)
	maxChainLength := 100
	var previousTimestamp int64 = -1

	for i := 0; i < maxChainLength; i++ {
		// Check for cycles
		if visitedRows[currentRowID] {
			result.Errors = append(result.Errors, fmt.Sprintf("cycle detected at RowID %v", currentRowID))
			result.IsValid = false
			break
		}
		visitedRows[currentRowID] = true
		result.ChainLength++

		// Get the current version
		currentRow, err := vci.storage.GetMVCCRow(vci.tableID, currentRowID)
		if err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("failed to get row at RowID %v: %v", currentRowID, err))
			result.IsValid = false
			break
		}

		// Validate timestamp ordering (newer versions should have newer or equal timestamps)
		if previousTimestamp != -1 && currentRow.Header.CreatedAt > previousTimestamp {
			result.Warnings = append(result.Warnings, fmt.Sprintf("timestamp ordering violation at RowID %v: timestamp %d > previous %d",
				currentRowID, currentRow.Header.CreatedAt, previousTimestamp))
		}
		previousTimestamp = currentRow.Header.CreatedAt

		// Validate transaction metadata
		if currentRow.Header.CreatedByTxn <= 0 {
			result.Errors = append(result.Errors, fmt.Sprintf("invalid created by transaction ID %d at RowID %v",
				currentRow.Header.CreatedByTxn, currentRowID))
			result.IsValid = false
		}

		if currentRow.Header.CreatedAt <= 0 {
			result.Errors = append(result.Errors, fmt.Sprintf("invalid created timestamp %d at RowID %v",
				currentRow.Header.CreatedAt, currentRowID))
			result.IsValid = false
		}

		// Validate deletion metadata consistency
		if currentRow.Header.DeletedAt > 0 {
			if currentRow.Header.DeletedByTxn <= 0 {
				result.Errors = append(result.Errors, fmt.Sprintf("row marked as deleted at %d but no deleting transaction at RowID %v",
					currentRow.Header.DeletedAt, currentRowID))
				result.IsValid = false
			}
			if currentRow.Header.DeletedAt < currentRow.Header.CreatedAt {
				result.Errors = append(result.Errors, fmt.Sprintf("deletion timestamp %d < creation timestamp %d at RowID %v",
					currentRow.Header.DeletedAt, currentRow.Header.CreatedAt, currentRowID))
				result.IsValid = false
			}
		}

		// Follow the chain to the previous version
		if currentRow.Header.NextVersion == 0 {
			// End of chain
			break
		}

		// Decode the next version pointer
		pageID, slotID := DecodeVersionPointer(currentRow.Header.NextVersion)
		currentRowID = RowID{
			PageID: storage.PageID(pageID),
			SlotID: slotID,
		}
	}

	if result.ChainLength >= maxChainLength {
		result.Errors = append(result.Errors, fmt.Sprintf("version chain too long (>%d versions)", maxChainLength))
		result.IsValid = false
	}

	// Add warnings for excessively long chains
	if result.ChainLength > 10 {
		result.Warnings = append(result.Warnings, fmt.Sprintf("version chain is long (%d versions) - consider vacuum", result.ChainLength))
	}

	return result, nil
}

// VersionChainValidationResult contains the results of version chain validation
type VersionChainValidationResult struct {
	StartRowID  RowID    // The starting RowID of the chain
	ChainLength int      // Length of the version chain
	Errors      []string // Validation errors that make the chain invalid
	Warnings    []string // Warnings about potential issues
	IsValid     bool     // Whether the chain passed validation
}

// GetSingleVersionChainStats returns statistics for a single version chain starting from the given RowID
func (vci *VersionChainIterator) GetSingleVersionChainStats(startRowID RowID, snapshotTS int64) (*SingleVersionChainStats, error) {
	stats := &SingleVersionChainStats{
		StartRowID:      startRowID,
		ChainLength:     0,
		VisibleVersions: 0,
		DeadVersions:    0,
		HasUpdates:      false,
	}

	// Get all versions in the chain
	allVersions, _, err := vci.GetAllVersions(startRowID)
	if err != nil {
		return nil, fmt.Errorf("failed to get all versions: %w", err)
	}

	stats.ChainLength = len(allVersions)
	if stats.ChainLength > 1 {
		stats.HasUpdates = true
	}

	// Count visible and dead versions
	for _, version := range allVersions {
		if vci.isVisible(version, snapshotTS) {
			stats.VisibleVersions++
		} else {
			stats.DeadVersions++
		}
	}

	return stats, nil
}

// SingleVersionChainStats contains statistics for a single version chain
type SingleVersionChainStats struct {
	StartRowID      RowID // The starting RowID of the chain
	ChainLength     int   // Total number of versions in the chain
	VisibleVersions int   // Number of versions visible at snapshot timestamp
	DeadVersions    int   // Number of versions not visible (deleted or too new)
	HasUpdates      bool  // Whether this chain has more than one version
}

// GetTableVersionChainStats returns statistics about all version chains in a table
// This is a placeholder for future implementation when we have table scanning capabilities
func (vci *VersionChainIterator) GetTableVersionChainStats() (*VersionChainStats, error) {
	// TODO: Implement when we have efficient table scanning
	// This would require scanning all rows in a table and analyzing their version chains
	return &VersionChainStats{}, fmt.Errorf("table-wide version chain statistics not yet implemented")
}
