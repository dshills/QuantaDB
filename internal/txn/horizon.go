package txn

import (
	"sync"
	"time"
)

// Transaction is a lightweight struct used for horizon tracking
type Transaction struct {
	id            TransactionID
	readTimestamp Timestamp
	isolation     IsolationLevel
}

// HorizonTracker tracks the oldest active transaction snapshot to determine
// which row versions can be safely removed by vacuum operations.
type HorizonTracker struct {
	mu              sync.RWMutex
	activeSnapshots map[TransactionID]int64 // Maps transaction ID to snapshot timestamp
	minSnapshot     int64                   // Cached minimum snapshot for performance
}

// NewHorizonTracker creates a new transaction horizon tracker
func NewHorizonTracker() *HorizonTracker {
	return &HorizonTracker{
		activeSnapshots: make(map[TransactionID]int64),
		minSnapshot:     0,
	}
}

// OnTransactionStart registers a new transaction with its snapshot timestamp
func (ht *HorizonTracker) OnTransactionStart(txn *Transaction) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	// Only track read transactions (they have snapshots)
	if txn.readTimestamp > 0 {
		ts := txn.readTimestamp
		if ts > Timestamp(1<<63-1) {
			ts = Timestamp(1<<63 - 1) // Cap at max int64
		}
		ht.activeSnapshots[txn.id] = int64(ts) //nolint:gosec // Safe conversion after bounds check
		ht.updateMinSnapshot()
	}
}

// OnTransactionEnd removes a transaction from tracking when it commits or rolls back
func (ht *HorizonTracker) OnTransactionEnd(txn *Transaction) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	delete(ht.activeSnapshots, txn.id)
	ht.updateMinSnapshot()
}

// GetHorizon returns the oldest snapshot that might be needed by any active transaction.
// Versions deleted before this horizon are safe to remove.
func (ht *HorizonTracker) GetHorizon() int64 {
	ht.mu.RLock()
	defer ht.mu.RUnlock()

	// If no active transactions, use current time as horizon
	if ht.minSnapshot == 0 {
		// Use the timestamp service to get current logical timestamp
		ts := NewTimestampService()
		currentTS := ts.GetCurrentTimestamp()
		if currentTS > Timestamp(1<<63-1) {
			return 1<<63 - 1 // Max int64 value
		}
		return int64(currentTS) //nolint:gosec // Safe conversion after bounds check
	}

	return ht.minSnapshot
}

// GetMinActiveSnapshot returns the minimum snapshot timestamp of all active transactions
func (ht *HorizonTracker) GetMinActiveSnapshot() int64 {
	ht.mu.RLock()
	defer ht.mu.RUnlock()

	return ht.minSnapshot
}

// GetActiveTransactionCount returns the number of active transactions being tracked
func (ht *HorizonTracker) GetActiveTransactionCount() int {
	ht.mu.RLock()
	defer ht.mu.RUnlock()

	return len(ht.activeSnapshots)
}

// GetActiveSnapshots returns a copy of all active snapshot timestamps for debugging
func (ht *HorizonTracker) GetActiveSnapshots() map[TransactionID]int64 {
	ht.mu.RLock()
	defer ht.mu.RUnlock()

	// Return a copy to prevent external modification
	result := make(map[TransactionID]int64, len(ht.activeSnapshots))
	for id, ts := range ht.activeSnapshots {
		result[id] = ts
	}
	return result
}

// updateMinSnapshot recalculates the minimum snapshot timestamp
// Must be called with mu.Lock held
func (ht *HorizonTracker) updateMinSnapshot() {
	if len(ht.activeSnapshots) == 0 {
		ht.minSnapshot = 0
		return
	}

	// Find the minimum snapshot
	var minSnap int64
	first := true
	for _, snapshot := range ht.activeSnapshots {
		if first || snapshot < minSnap {
			minSnap = snapshot
			first = false
		}
	}
	ht.minSnapshot = minSnap
}

// VacuumSafetyConfig adds a safety margin to the horizon to prevent
// removing versions that might be needed by transactions starting soon
type VacuumSafetyConfig struct {
	// Minimum age before a deleted version can be vacuumed (default: 5 minutes)
	MinAgeBeforeVacuum time.Duration
	// Safety margin in logical timestamp units (default: 100)
	SafetyMargin int64
}

// DefaultVacuumSafetyConfig returns the default safety configuration
func DefaultVacuumSafetyConfig() VacuumSafetyConfig {
	return VacuumSafetyConfig{
		MinAgeBeforeVacuum: 5 * time.Minute,
		SafetyMargin:       100,
	}
}

// GetSafeHorizon returns a horizon with safety margin applied.
// This ensures we don't remove versions that might be needed by
// transactions that are about to start.
func (ht *HorizonTracker) GetSafeHorizon(config VacuumSafetyConfig) int64 {
	horizon := ht.GetHorizon()

	// Apply timestamp margin (go back in time for safety)
	safeHorizon := horizon - config.SafetyMargin
	if safeHorizon < 0 {
		safeHorizon = 0
	}

	return safeHorizon
}

// CanVacuumVersion checks if a specific version is safe to vacuum
func (ht *HorizonTracker) CanVacuumVersion(deletedAt int64, config VacuumSafetyConfig) bool {
	if deletedAt == 0 {
		return false // Not deleted, cannot vacuum
	}

	safeHorizon := ht.GetSafeHorizon(config)
	return deletedAt < safeHorizon
}
