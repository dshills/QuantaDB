package executor

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/dshills/QuantaDB/internal/storage"
	"github.com/dshills/QuantaDB/internal/txn"
)

var ErrNoPredecessorFound = errors.New("no predecessor found")

// VacuumStats contains statistics from a vacuum operation
type VacuumStats struct {
	StartTime       time.Time
	EndTime         time.Time
	TablesProcessed int
	VersionsScanned int
	VersionsRemoved int
	SpaceReclaimed  int64
	PagesCompacted  int
	Errors          []error
	ChainRelinks    int
	Duration        time.Duration
}

// VacuumExecutor executes vacuum operations to remove dead versions and reclaim space
type VacuumExecutor struct {
	storage    *MVCCStorageBackend
	scanner    *VacuumScanner
	stats      *VacuumStats
	mu         sync.Mutex    // Protects stats during concurrent operations
	batchSize  int           // Number of dead versions to process at once
	batchDelay time.Duration // Delay between batches for throttling
}

// NewVacuumExecutor creates a new vacuum executor
func NewVacuumExecutor(storage *MVCCStorageBackend, horizonTracker *txn.HorizonTracker) *VacuumExecutor {
	return &VacuumExecutor{
		storage:    storage,
		scanner:    NewVacuumScanner(storage, horizonTracker),
		stats:      &VacuumStats{Errors: make([]error, 0)},
		batchSize:  100,                   // Process 100 dead versions at a time
		batchDelay: 10 * time.Millisecond, // Small delay between batches
	}
}

// SetBatchConfig configures batch processing parameters
func (ve *VacuumExecutor) SetBatchConfig(batchSize int, batchDelay time.Duration) {
	ve.batchSize = batchSize
	ve.batchDelay = batchDelay
}

// VacuumTable removes dead versions from a specific table
func (ve *VacuumExecutor) VacuumTable(tableID int64) error {
	ve.mu.Lock()
	ve.stats.TablesProcessed++
	ve.mu.Unlock()

	// Find dead versions in the table
	deadVersions, err := ve.scanner.FindDeadVersionsInTable(tableID)
	if err != nil {
		ve.addError(fmt.Errorf("failed to scan table %d: %w", tableID, err))
		return err
	}

	ve.mu.Lock()
	ve.stats.VersionsScanned += len(deadVersions)
	ve.mu.Unlock()

	// Debug: log found dead versions
	if len(deadVersions) == 0 {
		// No dead versions found - this might be OK if nothing is eligible
		return nil
	}

	// Process dead versions in batches
	for i := 0; i < len(deadVersions); i += ve.batchSize {
		end := i + ve.batchSize
		if end > len(deadVersions) {
			end = len(deadVersions)
		}

		batch := deadVersions[i:end]
		if err := ve.processBatch(batch); err != nil {
			ve.addError(fmt.Errorf("failed to process batch %d-%d: %w", i, end, err))
			// Continue with next batch even if this one fails
		}

		// Throttle between batches
		if ve.batchDelay > 0 && end < len(deadVersions) {
			time.Sleep(ve.batchDelay)
		}
	}

	return nil
}

// processBatch processes a batch of dead versions
func (ve *VacuumExecutor) processBatch(deadVersions []DeadVersion) error {
	// Group dead versions by their predecessor in the chain for efficient relinking
	chainUpdates := make(map[RowID][]DeadVersion)
	standaloneDeadVersions := []DeadVersion{} // Dead versions with no predecessors
	var lastError error

	for _, dv := range deadVersions {
		// Find the version that points to this dead version
		predecessor, err := ve.findPredecessor(dv.TableID, dv.RowID)
		if err != nil {
			lastError = fmt.Errorf("failed to find predecessor for %v: %w", dv.RowID, err)
			ve.addError(lastError)
			continue
		}

		if predecessor != nil {
			chainUpdates[*predecessor] = append(chainUpdates[*predecessor], dv)
		} else {
			// No predecessor means this is either:
			// 1. The head of a chain that's been deleted
			// 2. A standalone version
			// We can safely remove it without relinking
			standaloneDeadVersions = append(standaloneDeadVersions, dv)
		}
	}

	// Process chain updates (relink chains where needed)
	for predRowID, deadVersions := range chainUpdates {
		if err := ve.relinkChain(deadVersions[0].TableID, predRowID, deadVersions); err != nil {
			lastError = fmt.Errorf("failed to relink chain at %v: %w", predRowID, err)
			ve.addError(lastError)
			continue
		}

		ve.mu.Lock()
		ve.stats.ChainRelinks++
		ve.mu.Unlock()
	}

	// Remove all dead versions (both chained and standalone)
	allDeadVersions := append([]DeadVersion{}, deadVersions...)
	// Log standalone dead versions for debugging
	if len(standaloneDeadVersions) > 0 {
		ve.mu.Lock()
		ve.stats.VersionsRemoved += len(standaloneDeadVersions)
		ve.mu.Unlock()
	}

	for _, dv := range allDeadVersions {
		if err := ve.removeDeadVersion(dv); err != nil {
			lastError = fmt.Errorf("failed to remove dead version %v: %w", dv.RowID, err)
			ve.addError(lastError)
			continue
		}

		ve.mu.Lock()
		ve.stats.VersionsRemoved++
		ve.stats.SpaceReclaimed += int64(dv.Size)
		ve.mu.Unlock()
	}

	return lastError
}

// removeDeadVersion physically removes a dead version from storage
func (ve *VacuumExecutor) removeDeadVersion(dv DeadVersion) error {
	// Use the physical delete method from the storage backend
	return ve.storage.physicalDeleteRow(dv.TableID, dv.RowID)
}

// relinkChain updates version chain pointers to skip removed versions
func (ve *VacuumExecutor) relinkChain(tableID int64, predecessorRowID RowID, deadVersions []DeadVersion) error {
	// Get the predecessor version
	predRow, err := ve.storage.GetMVCCRow(tableID, predecessorRowID)
	if err != nil {
		return fmt.Errorf("failed to get predecessor row: %w", err)
	}

	// Find the furthest dead version in the chain
	var furthestNext int64
	for _, dv := range deadVersions {
		if dv.NextVersion != 0 {
			furthestNext = dv.NextVersion
		}
	}

	// Update predecessor to point to the next live version
	predRow.Header.NextVersion = furthestNext

	// Update the row in place
	if err := ve.storage.updateRowInPlace(tableID, predecessorRowID, predRow); err != nil {
		return fmt.Errorf("failed to update predecessor: %w", err)
	}

	return nil
}

// findPredecessor finds the row that points to the given row via NextVersion
func (ve *VacuumExecutor) findPredecessor(tableID int64, targetRowID RowID) (*RowID, error) {
	// This is a simplified implementation - in production, we might maintain
	// a reverse index for efficiency

	// Get table metadata
	ve.storage.mu.RLock()
	meta, exists := ve.storage.tableMeta[tableID]
	if !exists {
		ve.storage.mu.RUnlock()
		return nil, NewTableNotFoundError(tableID)
	}
	firstPageID := meta.FirstPageID
	schema := meta.RowFormat.Schema
	ve.storage.mu.RUnlock()

	// Create MVCC row format
	mvccFormat := NewMVCCRowFormat(schema)

	// Encode target as version pointer for comparison
	targetPointer := EncodeVersionPointer(uint32(targetRowID.PageID), targetRowID.SlotID)

	// Scan all pages to find predecessor
	currentPageID := firstPageID
	for currentPageID != 0 {
		page, err := ve.storage.bufferPool.FetchPage(currentPageID)
		if err != nil {
			return nil, err
		}

		// Check all slots
		for slotID := uint16(0); slotID < page.Header.ItemCount; slotID++ {
			// Read slot entry
			slotOffset := storage.PageHeaderSize + slotID*4
			if slotOffset+4 > storage.PageSize {
				continue
			}

			slotData := page.Data[slotOffset : slotOffset+4]
			pageDataOffset := uint16(slotData[0])<<8 | uint16(slotData[1])
			dataSize := uint16(slotData[2])<<8 | uint16(slotData[3])

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
				continue
			}

			// Check if this row points to our target
			if mvccRow.Header.NextVersion == targetPointer {
				rowID := RowID{
					PageID: currentPageID,
					SlotID: slotID,
				}
				ve.storage.bufferPool.UnpinPage(currentPageID, false)
				return &rowID, nil
			}
		}

		// Move to next page
		nextPageID := page.Header.NextPageID
		ve.storage.bufferPool.UnpinPage(currentPageID, false)
		currentPageID = nextPageID
	}

	return nil, ErrNoPredecessorFound
}

// VacuumDatabase vacuums all tables in the database
func (ve *VacuumExecutor) VacuumDatabase() error {
	ve.stats = &VacuumStats{
		StartTime: time.Now(),
		Errors:    make([]error, 0),
	}

	// Get all table IDs
	ve.storage.mu.RLock()
	tableIDs := make([]int64, 0, len(ve.storage.tableMeta))
	for tableID := range ve.storage.tableMeta {
		tableIDs = append(tableIDs, tableID)
	}
	ve.storage.mu.RUnlock()

	// Vacuum each table
	for _, tableID := range tableIDs {
		if err := ve.VacuumTable(tableID); err != nil {
			// Continue with other tables even if one fails
			ve.addError(fmt.Errorf("vacuum table %d failed: %w", tableID, err))
		}
	}

	ve.stats.EndTime = time.Now()
	ve.stats.Duration = ve.stats.EndTime.Sub(ve.stats.StartTime)

	return nil
}

// GetStats returns the current vacuum statistics
func (ve *VacuumExecutor) GetStats() VacuumStats {
	ve.mu.Lock()
	defer ve.mu.Unlock()

	// Return a copy to prevent external modification
	statsCopy := *ve.stats
	statsCopy.Errors = make([]error, len(ve.stats.Errors))
	copy(statsCopy.Errors, ve.stats.Errors)

	return statsCopy
}

// addError adds an error to the statistics
func (ve *VacuumExecutor) addError(err error) {
	ve.mu.Lock()
	defer ve.mu.Unlock()
	ve.stats.Errors = append(ve.stats.Errors, err)
}

// CompactPage attempts to compact a page by moving rows to fill gaps
// This is optional and can be implemented later for better space efficiency
func (ve *VacuumExecutor) CompactPage(pageID storage.PageID) error {
	// TODO: Implement page compaction
	// This would involve:
	// 1. Reading all live rows from the page
	// 2. Calculating optimal layout
	// 3. Moving rows to eliminate gaps
	// 4. Updating slot array
	// 5. Updating free space pointers
	return nil
}
