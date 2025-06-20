package catalog

import (
	"time"
)

// StatsMaintenance provides hooks for maintaining statistics when data changes.
type StatsMaintenance interface {
	// InvalidateTableStats marks table statistics as potentially outdated.
	InvalidateTableStats(tableID int64, changeType ChangeType, changeCount int64) error

	// ShouldAutoAnalyze determines if a table should be automatically analyzed.
	ShouldAutoAnalyze(tableID int64) bool

	// TriggerAutoAnalyze triggers automatic analysis of a table if needed.
	TriggerAutoAnalyze(tableID int64) error
}

// ChangeType represents the type of data change that occurred.
type ChangeType int

const (
	ChangeInsert ChangeType = iota
	ChangeUpdate
	ChangeDelete
	ChangeTruncate
)

// StatsInvalidation tracks when table statistics become invalid.
type StatsInvalidation struct {
	TableID      int64
	LastModified time.Time
	ChangesSince int64
	ChangeType   ChangeType
}

// DefaultStatsMaintenance provides a simple statistics maintenance implementation.
type DefaultStatsMaintenance struct {
	catalog     Catalog
	invalidated map[int64]*StatsInvalidation

	// Configuration thresholds
	AutoAnalyzeThreshold int64         // Number of changes before auto-analyze
	AutoAnalyzeRatio     float64       // Fraction of table size changed before auto-analyze
	StaleStatsThreshold  time.Duration // Time after which stats are considered stale
}

// NewDefaultStatsMaintenance creates a new default statistics maintenance manager.
func NewDefaultStatsMaintenance(cat Catalog) *DefaultStatsMaintenance {
	return &DefaultStatsMaintenance{
		catalog:              cat,
		invalidated:          make(map[int64]*StatsInvalidation),
		AutoAnalyzeThreshold: 1000,           // Auto-analyze after 1000 changes
		AutoAnalyzeRatio:     0.1,            // Auto-analyze after 10% of table changed
		StaleStatsThreshold:  24 * time.Hour, // Stats are stale after 24 hours
	}
}

// InvalidateTableStats marks table statistics as potentially outdated.
func (sm *DefaultStatsMaintenance) InvalidateTableStats(tableID int64, changeType ChangeType, changeCount int64) error {
	now := time.Now()

	if invalidation, exists := sm.invalidated[tableID]; exists {
		// Update existing invalidation
		invalidation.LastModified = now
		invalidation.ChangesSince += changeCount

		// Update change type to most severe (DELETE > UPDATE > INSERT)
		if changeType > invalidation.ChangeType {
			invalidation.ChangeType = changeType
		}
	} else {
		// Create new invalidation record
		sm.invalidated[tableID] = &StatsInvalidation{
			TableID:      tableID,
			LastModified: now,
			ChangesSince: changeCount,
			ChangeType:   changeType,
		}
	}

	return nil
}

// ShouldAutoAnalyze determines if a table should be automatically analyzed.
func (sm *DefaultStatsMaintenance) ShouldAutoAnalyze(tableID int64) bool {
	invalidation, exists := sm.invalidated[tableID]
	if !exists {
		return false
	}

	// Check if enough changes have accumulated
	if invalidation.ChangesSince >= sm.AutoAnalyzeThreshold {
		return true
	}

	// Check if stats are stale
	if time.Since(invalidation.LastModified) >= sm.StaleStatsThreshold {
		return true
	}

	// For ratio-based check, we would need table statistics and a mapping from ID to name
	// This is simplified - in a real implementation, we'd have a proper mapping
	// and would check if the change ratio exceeds the threshold
	// For now, just use the threshold-based approach
	return false
}

// TriggerAutoAnalyze triggers automatic analysis of a table if needed.
func (sm *DefaultStatsMaintenance) TriggerAutoAnalyze(tableID int64) error {
	if !sm.ShouldAutoAnalyze(tableID) {
		return nil
	}

	// Clear the invalidation record since we're about to re-analyze
	delete(sm.invalidated, tableID)

	// In a real implementation, this would queue an ANALYZE job
	// For now, we just clear the invalidation
	return nil
}

// GetInvalidatedTables returns a list of table IDs that have invalidated statistics.
func (sm *DefaultStatsMaintenance) GetInvalidatedTables() []int64 {
	var tableIDs []int64
	for tableID := range sm.invalidated {
		if sm.ShouldAutoAnalyze(tableID) {
			tableIDs = append(tableIDs, tableID)
		}
	}
	return tableIDs
}

// StatsMaintenanceHook provides a simple way to add statistics maintenance to operators.
func StatsMaintenanceHook(maintenance StatsMaintenance, tableID int64, changeType ChangeType, changeCount int64) {
	if maintenance != nil {
		// Best effort - don't fail the operation if stats maintenance fails
		_ = maintenance.InvalidateTableStats(tableID, changeType, changeCount)
	}
}
