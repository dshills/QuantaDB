package replication

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dshills/QuantaDB/internal/log"
	"github.com/dshills/QuantaDB/internal/wal"
)

// SynchronousReplicationConfig holds configuration for synchronous replication
type SynchronousReplicationConfig struct {
	// Mode specifies the synchronous replication level
	Mode SynchronousReplicationMode
	// Timeout for waiting for replica acknowledgments
	SyncTimeout time.Duration
	// MinReplicas is the minimum number of replicas required for sync mode
	MinReplicas int
	// MaxConcurrentSyncs limits concurrent synchronous operations
	MaxConcurrentSyncs int
	// RetryAttempts for failed synchronous operations
	RetryAttempts int
	// RetryDelay between retry attempts
	RetryDelay time.Duration
}

// DefaultSynchronousReplicationConfig returns sensible defaults
func DefaultSynchronousReplicationConfig() *SynchronousReplicationConfig {
	return &SynchronousReplicationConfig{
		Mode:               AsyncMode,
		SyncTimeout:        10 * time.Second,
		MinReplicas:        1,
		MaxConcurrentSyncs: 10,
		RetryAttempts:      3,
		RetryDelay:         100 * time.Millisecond,
	}
}

// SynchronousReplicationManager handles synchronous replication coordination
type SynchronousReplicationManager struct {
	config    *SynchronousReplicationConfig
	manager   *ReplicationManagerImpl
	semaphore chan struct{} // Limits concurrent operations
	logger    log.Logger

	// Tracking for pending synchronous operations
	mu              sync.RWMutex
	pendingOps      map[string]*SyncOperation
	replicaStatuses map[string]*ReplicaStatus
}

// SyncOperation tracks a synchronous replication operation
type SyncOperation struct {
	ID           string
	LSN          wal.LSN
	RequiredAcks int
	ReceivedAcks int
	StartTime    time.Time
	Done         chan error
	Context      context.Context
	Cancel       context.CancelFunc
}

// ReplicaStatus tracks the status of a replica for synchronous operations
type ReplicaStatus struct {
	ReplicaID    string
	LastAckLSN   wal.LSN
	LastAckTime  time.Time
	IsHealthy    bool
	ResponseTime time.Duration
}

// NewSynchronousReplicationManager creates a new synchronous replication manager
func NewSynchronousReplicationManager(manager *ReplicationManagerImpl, config *SynchronousReplicationConfig) *SynchronousReplicationManager {
	if config == nil {
		config = DefaultSynchronousReplicationConfig()
	}

	return &SynchronousReplicationManager{
		config:          config,
		manager:         manager,
		semaphore:       make(chan struct{}, config.MaxConcurrentSyncs),
		logger:          manager.logger,
		pendingOps:      make(map[string]*SyncOperation),
		replicaStatuses: make(map[string]*ReplicaStatus),
	}
}

// WaitForReplication waits for WAL replication according to the configured mode
func (srm *SynchronousReplicationManager) WaitForReplication(ctx context.Context, lsn wal.LSN) error {
	if srm.config.Mode == AsyncMode {
		return nil // No synchronous waiting required
	}

	// Acquire semaphore to limit concurrent operations
	select {
	case srm.semaphore <- struct{}{}:
		defer func() { <-srm.semaphore }()
	case <-ctx.Done():
		return ctx.Err()
	}

	// Create sync operation
	opID := fmt.Sprintf("sync_%d_%d", time.Now().UnixNano(), lsn)
	requiredAcks := srm.calculateRequiredAcks()

	if requiredAcks == 0 {
		srm.logger.Warn("No replicas available for synchronous replication",
			"mode", srm.config.Mode, "lsn", lsn)
		return nil // Proceed without synchronous replication
	}

	// Create operation context with timeout
	opCtx, cancel := context.WithTimeout(ctx, srm.config.SyncTimeout)
	defer cancel()

	op := &SyncOperation{
		ID:           opID,
		LSN:          lsn,
		RequiredAcks: requiredAcks,
		ReceivedAcks: 0,
		StartTime:    time.Now(),
		Done:         make(chan error, 1),
		Context:      opCtx,
		Cancel:       cancel,
	}

	// Register operation
	srm.mu.Lock()
	srm.pendingOps[opID] = op
	srm.mu.Unlock()

	// Clean up operation when done
	defer func() {
		srm.mu.Lock()
		delete(srm.pendingOps, opID)
		srm.mu.Unlock()
	}()

	// Wait for required acknowledgments or timeout
	select {
	case err := <-op.Done:
		if err != nil {
			srm.logger.Error("Synchronous replication failed",
				"op_id", opID, "lsn", lsn, "error", err)
			return fmt.Errorf("synchronous replication failed: %w", err)
		}

		duration := time.Since(op.StartTime)
		srm.logger.Info("Synchronous replication completed",
			"op_id", opID, "lsn", lsn, "acks", op.ReceivedAcks, "duration", duration)
		return nil

	case <-opCtx.Done():
		srm.logger.Warn("Synchronous replication timeout",
			"op_id", opID, "lsn", lsn, "received_acks", op.ReceivedAcks, "required_acks", requiredAcks)
		return fmt.Errorf("synchronous replication timeout after %v", srm.config.SyncTimeout)
	}
}

// ProcessReplicaAcknowledgment processes an acknowledgment from a replica
func (srm *SynchronousReplicationManager) ProcessReplicaAcknowledgment(replicaID string, lsn wal.LSN) {
	now := time.Now()

	// Update replica status
	srm.mu.Lock()
	status, exists := srm.replicaStatuses[replicaID]
	if !exists {
		status = &ReplicaStatus{
			ReplicaID: replicaID,
			IsHealthy: true,
		}
		srm.replicaStatuses[replicaID] = status
	}

	status.LastAckLSN = lsn
	status.LastAckTime = now
	status.IsHealthy = true

	// Check pending operations that might be satisfied by this acknowledgment
	for opID, op := range srm.pendingOps {
		if lsn >= op.LSN && op.ReceivedAcks < op.RequiredAcks {
			op.ReceivedAcks++

			srm.logger.Debug("Received replica acknowledgment",
				"replica_id", replicaID, "op_id", opID, "lsn", lsn,
				"received_acks", op.ReceivedAcks, "required_acks", op.RequiredAcks)

			// Check if operation is now complete
			if op.ReceivedAcks >= op.RequiredAcks {
				select {
				case op.Done <- nil:
					srm.logger.Info("Synchronous operation completed",
						"op_id", opID, "lsn", op.LSN, "acks", op.ReceivedAcks)
				default:
					// Channel already closed or operation timed out
				}
			}
		}
	}
	srm.mu.Unlock()
}

// calculateRequiredAcks determines how many acknowledgments are needed
func (srm *SynchronousReplicationManager) calculateRequiredAcks() int {
	srm.mu.RLock()
	healthyReplicas := 0
	for _, status := range srm.replicaStatuses {
		if status.IsHealthy {
			healthyReplicas++
		}
	}
	srm.mu.RUnlock()

	switch srm.config.Mode {
	case AsyncMode:
		return 0
	case SyncMode:
		if healthyReplicas >= srm.config.MinReplicas {
			return 1
		}
		return 0
	case QuorumMode:
		// Require majority of available replicas
		if healthyReplicas > 0 {
			return (healthyReplicas + 1) / 2
		}
		return 0
	case AllMode:
		return healthyReplicas
	default:
		return 0
	}
}

// UpdateReplicaHealth updates the health status of a replica
func (srm *SynchronousReplicationManager) UpdateReplicaHealth(replicaID string, isHealthy bool) {
	srm.mu.Lock()
	defer srm.mu.Unlock()

	status, exists := srm.replicaStatuses[replicaID]
	if !exists {
		status = &ReplicaStatus{
			ReplicaID: replicaID,
		}
		srm.replicaStatuses[replicaID] = status
	}

	oldHealth := status.IsHealthy
	status.IsHealthy = isHealthy

	if oldHealth != isHealthy {
		srm.logger.Info("Replica health changed",
			"replica_id", replicaID, "healthy", isHealthy)

		// If replica became unhealthy, check if any pending operations need to be failed
		if !isHealthy {
			srm.checkPendingOperationsForFailure()
		}
	}
}

// checkPendingOperationsForFailure checks if any pending operations should fail due to insufficient replicas
func (srm *SynchronousReplicationManager) checkPendingOperationsForFailure() {
	availableReplicas := srm.calculateRequiredAcks()

	for opID, op := range srm.pendingOps {
		if op.RequiredAcks > availableReplicas {
			srm.logger.Warn("Insufficient replicas for synchronous operation",
				"op_id", opID, "required", op.RequiredAcks, "available", availableReplicas)

			select {
			case op.Done <- fmt.Errorf("insufficient healthy replicas: required %d, available %d",
				op.RequiredAcks, availableReplicas):
			default:
				// Channel already closed
			}
		}
	}
}

// GetReplicationMode returns the current replication mode
func (srm *SynchronousReplicationManager) GetReplicationMode() SynchronousReplicationMode {
	return srm.config.Mode
}

// SetReplicationMode dynamically changes the replication mode
func (srm *SynchronousReplicationManager) SetReplicationMode(mode SynchronousReplicationMode) {
	srm.mu.Lock()
	defer srm.mu.Unlock()

	oldMode := srm.config.Mode
	srm.config.Mode = mode

	srm.logger.Info("Replication mode changed",
		"old_mode", oldMode, "new_mode", mode)

	// If switching to async mode, complete all pending operations
	if mode == AsyncMode {
		for opID, op := range srm.pendingOps {
			select {
			case op.Done <- nil:
				srm.logger.Info("Completing pending operation due to async mode switch",
					"op_id", opID)
			default:
				// Channel already closed
			}
		}
	}
}

// GetStatus returns the current status of synchronous replication
func (srm *SynchronousReplicationManager) GetStatus() map[string]interface{} {
	srm.mu.RLock()
	defer srm.mu.RUnlock()

	replicas := make(map[string]interface{})
	for id, status := range srm.replicaStatuses {
		replicas[id] = map[string]interface{}{
			"last_ack_lsn":  status.LastAckLSN,
			"last_ack_time": status.LastAckTime,
			"is_healthy":    status.IsHealthy,
			"response_time": status.ResponseTime,
		}
	}

	return map[string]interface{}{
		"mode":             srm.config.Mode,
		"pending_ops":      len(srm.pendingOps),
		"healthy_replicas": srm.getHealthyReplicaCount(),
		"replicas":         replicas,
		"config": map[string]interface{}{
			"sync_timeout":         srm.config.SyncTimeout,
			"min_replicas":         srm.config.MinReplicas,
			"max_concurrent_syncs": srm.config.MaxConcurrentSyncs,
			"retry_attempts":       srm.config.RetryAttempts,
		},
	}
}

// getHealthyReplicaCount returns the number of healthy replicas
func (srm *SynchronousReplicationManager) getHealthyReplicaCount() int {
	count := 0
	for _, status := range srm.replicaStatuses {
		if status.IsHealthy {
			count++
		}
	}
	return count
}

// Shutdown gracefully shuts down the synchronous replication manager
func (srm *SynchronousReplicationManager) Shutdown(ctx context.Context) error {
	srm.logger.Info("Shutting down synchronous replication manager")

	srm.mu.Lock()
	defer srm.mu.Unlock()

	// Cancel all pending operations
	for opID, op := range srm.pendingOps {
		op.Cancel()
		select {
		case op.Done <- fmt.Errorf("shutdown in progress"):
			srm.logger.Debug("Cancelled pending operation due to shutdown", "op_id", opID)
		default:
			// Channel already closed
		}
	}

	// Clear state
	srm.pendingOps = make(map[string]*SyncOperation)
	srm.replicaStatuses = make(map[string]*ReplicaStatus)

	srm.logger.Info("Synchronous replication manager shutdown complete")
	return nil
}
