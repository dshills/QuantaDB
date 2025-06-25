package raft

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dshills/QuantaDB/internal/cluster/replication"
	"github.com/dshills/QuantaDB/internal/log"
)

// RaftReplicationIntegration integrates Raft consensus with database replication
type RaftReplicationIntegration struct {
	raftNode           RaftNode
	replicationManager replication.ReplicationManager
	logger             log.Logger
	
	// State management
	mu           sync.RWMutex
	isLeader     bool
	currentTerm  Term
	
	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewRaftReplicationIntegration creates a new integration
func NewRaftReplicationIntegration(
	raftNode RaftNode,
	replicationManager replication.ReplicationManager,
	logger log.Logger,
) *RaftReplicationIntegration {
	ctx, cancel := context.WithCancel(context.Background())
	
	return &RaftReplicationIntegration{
		raftNode:           raftNode,
		replicationManager: replicationManager,
		logger:             logger,
		ctx:                ctx,
		cancel:             cancel,
	}
}

// Start starts the integration
func (ri *RaftReplicationIntegration) Start() error {
	ri.logger.Info("Starting Raft-Replication integration")
	
	// Start monitoring Raft state changes
	ri.wg.Add(1)
	go ri.monitorRaftState()
	
	return nil
}

// Stop stops the integration
func (ri *RaftReplicationIntegration) Stop() error {
	ri.logger.Info("Stopping Raft-Replication integration")
	
	ri.cancel()
	ri.wg.Wait()
	
	return nil
}

// monitorRaftState monitors Raft state changes and updates replication accordingly
func (ri *RaftReplicationIntegration) monitorRaftState() {
	defer ri.wg.Done()
	
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			ri.checkRaftState()
			
		case <-ri.ctx.Done():
			return
		}
	}
}

// checkRaftState checks current Raft state and updates replication mode
func (ri *RaftReplicationIntegration) checkRaftState() {
	term, state, leader := ri.raftNode.GetState()
	
	ri.mu.Lock()
	defer ri.mu.Unlock()
	
	// Check if leadership changed
	wasLeader := ri.isLeader
	ri.isLeader = (state == StateLeader)
	ri.currentTerm = term
	
	if ri.isLeader && !wasLeader {
		// Became leader
		ri.logger.Info("Became Raft leader, switching to primary mode", "term", term)
		if err := ri.replicationManager.StartPrimary(); err != nil {
			ri.logger.Error("Failed to start as primary", "error", err)
		}
		
	} else if !ri.isLeader && wasLeader {
		// Lost leadership
		ri.logger.Info("Lost Raft leadership, switching to replica mode", "term", term)
		if err := ri.replicationManager.StopReplica(); err != nil {
			ri.logger.Error("Failed to stop replica mode", "error", err)
		}
		
		// If we know the leader, connect as replica
		if leader != nil {
			leaderAddr := ri.resolveNodeAddress(*leader)
			if leaderAddr != "" {
				if err := ri.replicationManager.StartReplica(leaderAddr); err != nil {
					ri.logger.Error("Failed to start as replica", "leader", *leader, "error", err)
				}
			}
		}
	}
}

// resolveNodeAddress resolves a node ID to an address for replication
func (ri *RaftReplicationIntegration) resolveNodeAddress(nodeID NodeID) string {
	// TODO: In a real implementation, this would use service discovery
	// For now, use a simple mapping
	switch nodeID {
	case "node-1":
		return "127.0.0.1:7001"
	case "node-2":
		return "127.0.0.1:7002"
	case "node-3":
		return "127.0.0.1:7003"
	default:
		return ""
	}
}

// IsLeader returns true if this node is currently the Raft leader
func (ri *RaftReplicationIntegration) IsLeader() bool {
	ri.mu.RLock()
	defer ri.mu.RUnlock()
	return ri.isLeader
}

// GetCurrentTerm returns the current Raft term
func (ri *RaftReplicationIntegration) GetCurrentTerm() Term {
	ri.mu.RLock()
	defer ri.mu.RUnlock()
	return ri.currentTerm
}

// DatabaseStateMachine implements StateMachine for database operations
type DatabaseStateMachine struct {
	logger log.Logger
	mu     sync.RWMutex
	
	// In a real implementation, this would interface with the database storage engine
	appliedCommands []DatabaseCommand
}

// DatabaseCommand represents a database command that can be replicated
type DatabaseCommand struct {
	Type      string      `json:"type"`
	TableID   int64       `json:"table_id,omitempty"`
	Operation string      `json:"operation"`
	Data      interface{} `json:"data,omitempty"`
	Timestamp time.Time   `json:"timestamp"`
}

// NewDatabaseStateMachine creates a new database state machine
func NewDatabaseStateMachine(logger log.Logger) *DatabaseStateMachine {
	return &DatabaseStateMachine{
		logger:          logger,
		appliedCommands: make([]DatabaseCommand, 0),
	}
}

// Apply applies a committed log entry to the database state machine
func (dsm *DatabaseStateMachine) Apply(entry *LogEntry) error {
	dsm.mu.Lock()
	defer dsm.mu.Unlock()
	
	dsm.logger.Debug("Applying log entry to database", 
		"index", entry.Index, 
		"term", entry.Term,
		"type", entry.Type)
	
	switch entry.Type {
	case EntryTypeCommand:
		// Parse database command from entry data
		// In a real implementation, this would deserialize the command
		// and execute it against the database
		
		cmd := DatabaseCommand{
			Type:      "database_operation",
			Operation: "parsed_from_data",
			Data:      entry.Data,
			Timestamp: entry.Timestamp,
		}
		
		dsm.appliedCommands = append(dsm.appliedCommands, cmd)
		
		dsm.logger.Debug("Applied database command", 
			"index", entry.Index,
			"operation", cmd.Operation,
			"dataSize", len(entry.Data))
		
	case EntryTypeConfiguration:
		// Handle cluster configuration changes
		dsm.logger.Info("Applied configuration change", "index", entry.Index)
		
	case EntryTypeNoOp:
		// No-op entries don't need processing
		dsm.logger.Debug("Applied no-op entry", "index", entry.Index)
		
	default:
		return fmt.Errorf("unknown log entry type: %v", entry.Type)
	}
	
	return nil
}

// Snapshot creates a snapshot of the current database state
func (dsm *DatabaseStateMachine) Snapshot() ([]byte, error) {
	dsm.mu.RLock()
	defer dsm.mu.RUnlock()
	
	// In a real implementation, this would create a snapshot of the database
	// For now, we'll just serialize the applied commands as a simple example
	
	snapshot := DatabaseSnapshot{
		AppliedCommands: dsm.appliedCommands,
		Timestamp:       time.Now(),
	}
	
	// In a real implementation, you'd use a more efficient serialization format
	data := fmt.Sprintf("snapshot with %d commands", len(dsm.appliedCommands))
	
	dsm.logger.Debug("Created database snapshot", 
		"commandCount", len(dsm.appliedCommands),
		"dataSize", len(data))
	
	// Use the snapshot variable to avoid compiler warning
	_ = snapshot
	
	return []byte(data), nil
}

// Restore restores the database state from a snapshot
func (dsm *DatabaseStateMachine) Restore(snapshot []byte) error {
	dsm.mu.Lock()
	defer dsm.mu.Unlock()
	
	// In a real implementation, this would restore the database from the snapshot
	// For now, we'll just reset the applied commands
	
	dsm.appliedCommands = make([]DatabaseCommand, 0)
	
	dsm.logger.Info("Restored database from snapshot", "dataSize", len(snapshot))
	return nil
}

// GetAppliedCommandCount returns the number of applied commands (for testing)
func (dsm *DatabaseStateMachine) GetAppliedCommandCount() int {
	dsm.mu.RLock()
	defer dsm.mu.RUnlock()
	return len(dsm.appliedCommands)
}

// DatabaseSnapshot represents a snapshot of database state
type DatabaseSnapshot struct {
	AppliedCommands []DatabaseCommand `json:"applied_commands"`
	Timestamp       time.Time         `json:"timestamp"`
}

// ClusterManager manages the overall cluster including Raft and replication
type ClusterManager struct {
	config             *ClusterConfig
	logger             log.Logger
	
	// Core components
	raftNode           RaftNode
	replicationManager replication.ReplicationManager
	integration        *RaftReplicationIntegration
	
	// Lifecycle
	mu      sync.RWMutex
	started bool
}

// ClusterConfig holds configuration for the cluster manager
type ClusterConfig struct {
	NodeID      NodeID
	Peers       []NodeID
	DataDir     string
	ReplicationConfig *replication.ReplicationConfig
	RaftConfig  *RaftConfig
}

// NewClusterManager creates a new cluster manager
func NewClusterManager(config *ClusterConfig, logger log.Logger) (*ClusterManager, error) {
	// Create storage
	storage, err := NewFileStorage(config.DataDir, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage: %w", err)
	}
	
	// Create state machine
	stateMachine := NewDatabaseStateMachine(logger)
	
	// Create transport
	transport := NewTCPTransport(config.RaftConfig, logger)
	
	// Create Raft node
	raftNode, err := NewRaftNode(config.RaftConfig, transport, storage, stateMachine, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create Raft node: %w", err)
	}
	
	// Create replication manager (this would be injected from outside)
	var replicationManager replication.ReplicationManager
	// replicationManager = replication.NewReplicationManager(config.ReplicationConfig, walManager, logger)
	
	// Create integration
	integration := NewRaftReplicationIntegration(raftNode, replicationManager, logger)
	
	return &ClusterManager{
		config:             config,
		logger:             logger,
		raftNode:           raftNode,
		replicationManager: replicationManager,
		integration:        integration,
	}, nil
}

// Start starts the cluster manager
func (cm *ClusterManager) Start() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	
	if cm.started {
		return fmt.Errorf("cluster manager already started")
	}
	
	cm.logger.Info("Starting cluster manager", "nodeID", cm.config.NodeID)
	
	// Start Raft node
	if err := cm.raftNode.Start(); err != nil {
		return fmt.Errorf("failed to start Raft node: %w", err)
	}
	
	// Start integration
	if err := cm.integration.Start(); err != nil {
		cm.raftNode.Stop()
		return fmt.Errorf("failed to start integration: %w", err)
	}
	
	cm.started = true
	cm.logger.Info("Cluster manager started successfully")
	
	return nil
}

// Stop stops the cluster manager
func (cm *ClusterManager) Stop() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	
	if !cm.started {
		return nil
	}
	
	cm.logger.Info("Stopping cluster manager")
	
	// Stop integration
	if err := cm.integration.Stop(); err != nil {
		cm.logger.Error("Failed to stop integration", "error", err)
	}
	
	// Stop Raft node
	if err := cm.raftNode.Stop(); err != nil {
		cm.logger.Error("Failed to stop Raft node", "error", err)
	}
	
	// Stop replication manager
	if cm.replicationManager != nil {
		if err := cm.replicationManager.Close(); err != nil {
			cm.logger.Error("Failed to stop replication manager", "error", err)
		}
	}
	
	cm.started = false
	cm.logger.Info("Cluster manager stopped")
	
	return nil
}

// IsLeader returns true if this node is the current leader
func (cm *ClusterManager) IsLeader() bool {
	return cm.integration.IsLeader()
}

// GetRaftNode returns the underlying Raft node
func (cm *ClusterManager) GetRaftNode() RaftNode {
	return cm.raftNode
}

// GetReplicationManager returns the replication manager
func (cm *ClusterManager) GetReplicationManager() replication.ReplicationManager {
	return cm.replicationManager
}