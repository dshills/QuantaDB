package replication

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dshills/QuantaDB/internal/log"
	"github.com/dshills/QuantaDB/internal/wal"
)

// ReplicationManagerImpl implements the ReplicationManager interface
type ReplicationManagerImpl struct {
	config *ReplicationConfig
	walMgr *wal.Manager
	logger log.Logger

	// Replication components
	streamer WALStreamer
	receiver WALReceiver

	// Synchronous replication
	syncManager *SynchronousReplicationManager

	// State management
	mu       sync.RWMutex
	mode     ReplicationMode
	replicas map[NodeID]ReplicaInfo
	status   ReplicationStatus

	// Control channels
	closeCh   chan struct{}
	closeOnce sync.Once
}

// NewReplicationManager creates a new replication manager
func NewReplicationManager(config *ReplicationConfig, walMgr *wal.Manager, logger log.Logger) *ReplicationManagerImpl {
	rm := &ReplicationManagerImpl{
		config:   config,
		walMgr:   walMgr,
		logger:   logger,
		mode:     config.Mode,
		replicas: make(map[NodeID]ReplicaInfo),
		status: ReplicationStatus{
			Mode:          config.Mode,
			NodeID:        config.NodeID,
			Address:       config.Address,
			State:         "INITIALIZED",
			ReplicaStates: make(map[NodeID]ReplicaState),
		},
		closeCh: make(chan struct{}),
	}

	// Initialize synchronous replication manager
	rm.syncManager = NewSynchronousReplicationManager(rm, DefaultSynchronousReplicationConfig())

	return rm
}

// StartPrimary starts the node as a primary (read/write)
func (rm *ReplicationManagerImpl) StartPrimary() error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if rm.mode != ReplicationModePrimary {
		return fmt.Errorf("node is not configured as primary")
	}

	// Initialize WAL streamer
	rm.streamer = NewWALStreamer(rm.config, rm.walMgr, rm.logger)

	// Start streamer service
	if err := rm.streamer.(*WALStreamerImpl).Start(); err != nil {
		return fmt.Errorf("failed to start WAL streamer: %w", err)
	}

	// Update status
	rm.status.State = "PRIMARY_ACTIVE"
	rm.status.LastLSN = rm.walMgr.GetCurrentLSN()

	// Start monitoring goroutine
	go rm.monitorReplicas()

	// Hook into WAL manager to stream new records
	go rm.walStreamingLoop()

	rm.logger.Info("Started as primary node", "nodeID", rm.config.NodeID)
	return nil
}

// AddReplica adds a new replica to stream WAL to
func (rm *ReplicationManagerImpl) AddReplica(address string) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if rm.mode != ReplicationModePrimary {
		return fmt.Errorf("only primary can add replicas")
	}

	if rm.streamer == nil {
		return fmt.Errorf("primary not started - call StartPrimary() first")
	}

	// Generate replica ID (in production, this would come from handshake)
	nodeID := NodeID(fmt.Sprintf("replica-%d", time.Now().Unix()))

	replica := ReplicaInfo{
		NodeID:        nodeID,
		Address:       address,
		State:         ReplicaStateConnecting,
		ConnectedAt:   time.Now(),
		LastHeartbeat: time.Now(),
	}

	// Start streaming to replica
	startLSN := rm.walMgr.GetCurrentLSN()
	if err := rm.streamer.StartStream(replica, startLSN); err != nil {
		return fmt.Errorf("failed to start stream to replica: %w", err)
	}

	// Add to replica list
	rm.replicas[nodeID] = replica
	rm.status.ReplicaCount++
	rm.status.ReplicaStates[nodeID] = ReplicaStateStreaming

	rm.logger.Info("Added replica", "nodeID", nodeID, "address", address)
	return nil
}

// RemoveReplica removes a replica from streaming
func (rm *ReplicationManagerImpl) RemoveReplica(nodeID NodeID) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if rm.mode != ReplicationModePrimary {
		return fmt.Errorf("only primary can remove replicas")
	}

	// Check if replica exists
	if _, exists := rm.replicas[nodeID]; !exists {
		return fmt.Errorf("replica %s not found", nodeID)
	}

	// Stop streaming
	if err := rm.streamer.StopStream(nodeID); err != nil {
		rm.logger.Warn("Error stopping stream", "nodeID", nodeID, "error", err)
	}

	// Remove from lists
	delete(rm.replicas, nodeID)
	delete(rm.status.ReplicaStates, nodeID)
	rm.status.ReplicaCount--

	rm.logger.Info("Removed replica", "nodeID", nodeID)
	return nil
}

// GetReplicas returns information about all replicas
func (rm *ReplicationManagerImpl) GetReplicas() []ReplicaInfo {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	replicas := make([]ReplicaInfo, 0, len(rm.replicas))
	for _, replica := range rm.replicas {
		replicas = append(replicas, replica)
	}

	return replicas
}

// StartReplica starts the node as a replica (read-only)
func (rm *ReplicationManagerImpl) StartReplica(primaryAddr string) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if rm.mode != ReplicationModeReplica {
		return fmt.Errorf("node is not configured as replica")
	}

	// Initialize WAL receiver
	rm.receiver = NewWALReceiver(rm.config, rm.walMgr, rm.logger)

	// Start receiving from primary
	startLSN := rm.walMgr.GetCurrentLSN()
	if err := rm.receiver.StartReceiving(primaryAddr, startLSN); err != nil {
		return fmt.Errorf("failed to start receiving: %w", err)
	}

	// Update status
	rm.status.State = "REPLICA_ACTIVE"
	rm.status.LastLSN = startLSN

	// Start monitoring goroutine
	go rm.monitorReplication()

	rm.logger.Info("Started as replica node", "nodeID", rm.config.NodeID, "primary", primaryAddr)
	return nil
}

// StopReplica stops the replica
func (rm *ReplicationManagerImpl) StopReplica() error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if rm.mode != ReplicationModeReplica || rm.receiver == nil {
		return fmt.Errorf("not running as replica")
	}

	// Stop receiving
	if err := rm.receiver.StopReceiving(); err != nil {
		return fmt.Errorf("failed to stop receiving: %w", err)
	}

	rm.status.State = "STOPPED"
	rm.logger.Info("Stopped replica")
	return nil
}

// GetStatus returns the current replication status
func (rm *ReplicationManagerImpl) GetStatus() ReplicationStatus {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	// Update current LSN
	rm.status.LastLSN = rm.walMgr.GetCurrentLSN()

	// Create a copy to avoid race conditions
	status := rm.status
	status.ReplicaStates = make(map[NodeID]ReplicaState)
	for nodeID, state := range rm.status.ReplicaStates {
		status.ReplicaStates[nodeID] = state
	}

	return status
}

// Close stops the replication manager and cleans up resources
func (rm *ReplicationManagerImpl) Close() error {
	rm.closeOnce.Do(func() {
		close(rm.closeCh)

		rm.mu.Lock()
		defer rm.mu.Unlock()

		// Stop streamer
		if rm.streamer != nil {
			rm.streamer.(*WALStreamerImpl).Close() //nolint:errcheck // Best effort cleanup
		}

		// Stop receiver
		if rm.receiver != nil {
			rm.receiver.Close() //nolint:errcheck // Best effort cleanup
		}

		rm.status.State = "CLOSED"
	})

	return nil
}

// monitorReplicas monitors replica health and lag (primary only)
func (rm *ReplicationManagerImpl) monitorReplicas() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rm.checkReplicaHealth()
		case <-rm.closeCh:
			return
		}
	}
}

// checkReplicaHealth checks the health of all replicas
func (rm *ReplicationManagerImpl) checkReplicaHealth() {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if rm.streamer == nil {
		return
	}

	// Get stream status for all replicas
	streamStatus := rm.streamer.GetStreamStatus()
	currentTime := time.Now()

	for nodeID, replica := range rm.replicas {
		status, exists := streamStatus[nodeID]
		if !exists {
			// Replica stream not found - mark as failed
			replica.State = ReplicaStateFailed
			rm.replicas[nodeID] = replica
			rm.status.ReplicaStates[nodeID] = ReplicaStateFailed
			continue
		}

		// Check lag
		currentLSN := rm.walMgr.GetCurrentLSN()
		lagLSN := int64(currentLSN) - int64(status.LastSentLSN)
		lagTime := currentTime.Sub(status.LastActivity)

		// Update replica info
		replica.LastReceivedLSN = status.LastSentLSN
		replica.LagBytes = lagLSN * 100 // Approximate bytes per LSN
		replica.LagTime = lagTime
		replica.LastHeartbeat = status.LastActivity

		// Determine state based on lag
		if lagTime > rm.config.MaxLagTime || replica.LagBytes > rm.config.MaxLagBytes {
			replica.State = ReplicaStateLagging
		} else {
			replica.State = ReplicaStateStreaming
		}

		rm.replicas[nodeID] = replica
		rm.status.ReplicaStates[nodeID] = replica.State
	}
}

// monitorReplication monitors replication status (replica only)
func (rm *ReplicationManagerImpl) monitorReplication() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rm.checkReplicationHealth()
		case <-rm.closeCh:
			return
		}
	}
}

// checkReplicationHealth checks replication health for replica
func (rm *ReplicationManagerImpl) checkReplicationHealth() {
	rm.mu.RLock()
	receiver := rm.receiver
	rm.mu.RUnlock()

	if receiver == nil {
		return
	}

	// Get receiver status
	status := receiver.GetStatus()

	// Update replication status based on receiver state
	rm.mu.Lock()
	rm.status.LastLSN = status.LastAppliedLSN

	switch status.State {
	case "STREAMING":
		rm.status.State = "REPLICA_STREAMING"
	case "DISCONNECTED":
		rm.status.State = "REPLICA_DISCONNECTED"
	case "STOPPED":
		rm.status.State = "REPLICA_STOPPED"
	}
	rm.mu.Unlock()

	// Log status periodically
	rm.logger.Debug("Replication status",
		"state", status.State,
		"lastAppliedLSN", status.LastAppliedLSN,
		"recordsReceived", status.RecordsReceived,
		"recordsApplied", status.RecordsApplied)
}

// walStreamingLoop streams new WAL records to replicas (primary only)
func (rm *ReplicationManagerImpl) walStreamingLoop() {
	rm.logger.Info("Started WAL streaming loop")

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	lastStreamedLSN := rm.walMgr.GetCurrentLSN()

	for {
		select {
		case <-ticker.C:
			currentLSN := rm.walMgr.GetCurrentLSN()
			if currentLSN > lastStreamedLSN {
				// Stream new WAL records and notify sync manager
				rm.streamWALRecords(lastStreamedLSN, currentLSN)
				lastStreamedLSN = currentLSN
			}
		case <-rm.closeCh:
			return
		}
	}
}

// streamWALRecords handles streaming WAL records and synchronous replication
func (rm *ReplicationManagerImpl) streamWALRecords(fromLSN, toLSN wal.LSN) {
	if rm.streamer == nil {
		return
	}

	// TODO: Read actual WAL records from fromLSN to toLSN
	// For now, we'll simulate the streaming process

	// Update replica acknowledgment tracking for sync manager
	rm.mu.RLock()
	replicas := make([]NodeID, 0, len(rm.replicas))
	for nodeID := range rm.replicas {
		replicas = append(replicas, nodeID)
	}
	rm.mu.RUnlock()

	// Notify sync manager about healthy replicas
	for _, nodeID := range replicas {
		rm.syncManager.UpdateReplicaHealth(string(nodeID), true)
	}
}

// WaitForSynchronousReplication waits for WAL replication according to configured mode
func (rm *ReplicationManagerImpl) WaitForSynchronousReplication(ctx context.Context, lsn wal.LSN) error {
	if rm.syncManager == nil {
		return nil // No synchronous replication configured
	}
	return rm.syncManager.WaitForReplication(ctx, lsn)
}

// ProcessReplicaAcknowledgment processes an acknowledgment from a replica
func (rm *ReplicationManagerImpl) ProcessReplicaAcknowledgment(replicaID string, lsn wal.LSN) {
	if rm.syncManager != nil {
		rm.syncManager.ProcessReplicaAcknowledgment(replicaID, lsn)
	}
}

// SetSynchronousReplicationMode changes the synchronous replication mode
func (rm *ReplicationManagerImpl) SetSynchronousReplicationMode(mode SynchronousReplicationMode) {
	if rm.syncManager != nil {
		rm.syncManager.SetReplicationMode(mode)
	}
}

// GetSynchronousReplicationStatus returns the current synchronous replication status
func (rm *ReplicationManagerImpl) GetSynchronousReplicationStatus() map[string]interface{} {
	if rm.syncManager != nil {
		return rm.syncManager.GetStatus()
	}
	return map[string]interface{}{
		"mode":             "disabled",
		"pending_ops":      0,
		"healthy_replicas": 0,
		"replicas":         map[string]interface{}{},
	}
}
