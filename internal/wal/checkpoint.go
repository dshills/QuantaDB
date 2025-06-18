package wal

import (
	"fmt"
	"sync"
	"time"

	"github.com/dshills/QuantaDB/internal/storage"
)

// CheckpointManager handles periodic checkpoints to limit recovery time
type CheckpointManager struct {
	walManager *Manager
	bufferPool *storage.BufferPool
	
	// Checkpoint configuration
	checkpointInterval time.Duration // Time between checkpoints
	minRecords        int            // Minimum records before checkpoint
	
	// State
	mu               sync.Mutex
	lastCheckpointLSN LSN
	lastCheckpointTime time.Time
	recordsSinceCheckpoint int
	
	// Control
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// CheckpointConfig holds checkpoint configuration
type CheckpointConfig struct {
	Interval   time.Duration // Time between checkpoints
	MinRecords int          // Minimum records before checkpoint
}

// DefaultCheckpointConfig returns default checkpoint configuration
func DefaultCheckpointConfig() *CheckpointConfig {
	return &CheckpointConfig{
		Interval:   5 * time.Minute,
		MinRecords: 1000,
	}
}

// NewCheckpointManager creates a new checkpoint manager
func NewCheckpointManager(walManager *Manager, bufferPool *storage.BufferPool, config *CheckpointConfig) *CheckpointManager {
	if config == nil {
		config = DefaultCheckpointConfig()
	}
	
	return &CheckpointManager{
		walManager:         walManager,
		bufferPool:         bufferPool,
		checkpointInterval: config.Interval,
		minRecords:        config.MinRecords,
		stopCh:            make(chan struct{}),
	}
}

// Start begins periodic checkpointing
func (cm *CheckpointManager) Start() {
	cm.wg.Add(1)
	go cm.checkpointLoop()
}

// Stop stops the checkpoint manager
func (cm *CheckpointManager) Stop() {
	close(cm.stopCh)
	cm.wg.Wait()
}

// checkpointLoop runs periodic checkpoints
func (cm *CheckpointManager) checkpointLoop() {
	defer cm.wg.Done()
	
	ticker := time.NewTicker(cm.checkpointInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			if err := cm.maybeCheckpoint(); err != nil {
				fmt.Printf("Checkpoint failed: %v\n", err)
			}
		case <-cm.stopCh:
			return
		}
	}
}

// maybeCheckpoint performs a checkpoint if needed
func (cm *CheckpointManager) maybeCheckpoint() error {
	cm.mu.Lock()
	recordCount := cm.recordsSinceCheckpoint
	cm.mu.Unlock()
	
	// Check if checkpoint is needed
	if recordCount < cm.minRecords {
		return nil
	}
	
	return cm.Checkpoint()
}

// Checkpoint performs a checkpoint operation
func (cm *CheckpointManager) Checkpoint() error {
	fmt.Println("Starting checkpoint...")
	startTime := time.Now()
	
	// Get current LSN
	currentLSN := cm.walManager.GetCurrentLSN()
	
	// Get active transactions
	cm.walManager.txnMu.Lock()
	activeTxns := make([]uint64, 0, len(cm.walManager.txnLastLSN))
	for txnID := range cm.walManager.txnLastLSN {
		activeTxns = append(activeTxns, txnID)
	}
	cm.walManager.txnMu.Unlock()
	
	// Get dirty pages from buffer pool
	dirtyPages := cm.getDirtyPages()
	
	// Create checkpoint record
	checkpointRec := &CheckpointRecord{
		Timestamp:  time.Now(),
		LastLSN:    currentLSN,
		ActiveTxns: activeTxns,
		DirtyPages: dirtyPages,
	}
	
	// Log checkpoint start
	checkpointLSN, err := cm.walManager.LogCheckpoint(checkpointRec)
	if err != nil {
		return fmt.Errorf("failed to log checkpoint: %w", err)
	}
	
	// Flush dirty pages
	if err := cm.flushDirtyPages(dirtyPages); err != nil {
		return fmt.Errorf("failed to flush dirty pages: %w", err)
	}
	
	// Force WAL flush
	if err := cm.walManager.Flush(); err != nil {
		return fmt.Errorf("failed to flush WAL: %w", err)
	}
	
	// Update state
	cm.mu.Lock()
	cm.lastCheckpointLSN = checkpointLSN
	cm.lastCheckpointTime = time.Now()
	cm.recordsSinceCheckpoint = 0
	cm.mu.Unlock()
	
	duration := time.Since(startTime)
	fmt.Printf("Checkpoint completed at LSN %d in %v\n", checkpointLSN, duration)
	fmt.Printf("  Active transactions: %d\n", len(activeTxns))
	fmt.Printf("  Dirty pages flushed: %d\n", len(dirtyPages))
	
	return nil
}

// getDirtyPages returns information about dirty pages in the buffer pool
func (cm *CheckpointManager) getDirtyPages() []DirtyPageInfo {
	// In a real implementation, the buffer pool would track dirty pages
	// and their recovery LSNs. For now, we'll return an empty list.
	// This would need to be implemented in the buffer pool.
	return []DirtyPageInfo{}
}

// flushDirtyPages flushes dirty pages to disk
func (cm *CheckpointManager) flushDirtyPages(dirtyPages []DirtyPageInfo) error {
	// In a real implementation, this would:
	// 1. Sort pages by table/page ID for sequential I/O
	// 2. Flush each dirty page to disk
	// 3. Update page headers with current LSN
	
	for _, dp := range dirtyPages {
		// The buffer pool would handle the actual flush
		// For now, we just simulate the operation
		_ = dp
	}
	
	return nil
}

// IncrementRecordCount increments the record count since last checkpoint
func (cm *CheckpointManager) IncrementRecordCount() {
	cm.mu.Lock()
	cm.recordsSinceCheckpoint++
	cm.mu.Unlock()
}

// GetLastCheckpointLSN returns the LSN of the last checkpoint
func (cm *CheckpointManager) GetLastCheckpointLSN() LSN {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.lastCheckpointLSN
}

// GetStats returns checkpoint statistics
func (cm *CheckpointManager) GetStats() CheckpointStats {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	
	return CheckpointStats{
		LastCheckpointLSN:      cm.lastCheckpointLSN,
		LastCheckpointTime:     cm.lastCheckpointTime,
		RecordsSinceCheckpoint: cm.recordsSinceCheckpoint,
	}
}

// CheckpointStats contains checkpoint statistics
type CheckpointStats struct {
	LastCheckpointLSN      LSN
	LastCheckpointTime     time.Time
	RecordsSinceCheckpoint int
}