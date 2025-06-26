package failover

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dshills/QuantaDB/internal/cluster/raft"
	"github.com/dshills/QuantaDB/internal/cluster/replication"
	"github.com/dshills/QuantaDB/internal/log"
	"github.com/dshills/QuantaDB/internal/wal"
)

// FailoverManager manages automatic failover using Raft consensus
type FailoverManager struct {
	config    *FailoverConfig
	logger    log.Logger
	
	// Core components
	raftNode           raft.RaftNode
	replicationManager replication.ReplicationManager
	walManager         *wal.Manager
	
	// State tracking
	mu              sync.RWMutex
	currentRole     NodeRole
	isHealthy       bool
	lastHealthCheck time.Time
	
	// Failover state
	failoverInProgress bool
	lastFailoverTime   time.Time
	
	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// FailoverConfig holds configuration for failover manager
type FailoverConfig struct {
	NodeID              raft.NodeID
	ClusterNodes        []ClusterNode
	HealthCheckInterval time.Duration
	FailoverTimeout     time.Duration
	MinFailoverInterval time.Duration // Prevent failover thrashing
}

// ClusterNode represents a node in the cluster
type ClusterNode struct {
	NodeID           raft.NodeID
	ReplicationAddr  string // Address for replication (PostgreSQL protocol)
	RaftAddr         string // Address for Raft consensus
}

// NodeRole represents the current role of the node
type NodeRole int

const (
	RoleFollower NodeRole = iota
	RolePrimary
	RoleStandby
	RoleReplica // Alias for RoleStandby
)

// String returns string representation of node role
func (r NodeRole) String() string {
	switch r {
	case RoleFollower:
		return "FOLLOWER"
	case RolePrimary:
		return "PRIMARY"
	case RoleStandby:
		return "STANDBY"
	case RoleReplica:
		return "REPLICA"
	default:
		return "UNKNOWN"
	}
}

// NewFailoverManager creates a new failover manager
func NewFailoverManager(
	config *FailoverConfig,
	raftNode raft.RaftNode,
	replicationManager replication.ReplicationManager,
	walManager *wal.Manager,
	logger log.Logger,
) *FailoverManager {
	ctx, cancel := context.WithCancel(context.Background())
	
	return &FailoverManager{
		config:             config,
		logger:             logger,
		raftNode:           raftNode,
		replicationManager: replicationManager,
		walManager:         walManager,
		currentRole:        RoleFollower,
		isHealthy:          true,
		lastHealthCheck:    time.Now(),
		ctx:                ctx,
		cancel:             cancel,
	}
}

// Start starts the failover manager
func (fm *FailoverManager) Start() error {
	fm.logger.Info("Starting failover manager", "nodeID", fm.config.NodeID)
	
	// Start monitoring Raft state
	fm.wg.Add(1)
	go fm.monitorRaftState()
	
	// Start health checking
	fm.wg.Add(1)
	go fm.performHealthChecks()
	
	// Start failover coordinator
	fm.wg.Add(1)
	go fm.failoverCoordinator()
	
	return nil
}

// Stop stops the failover manager
func (fm *FailoverManager) Stop() error {
	fm.logger.Info("Stopping failover manager")
	
	fm.cancel()
	fm.wg.Wait()
	
	return nil
}

// monitorRaftState monitors Raft leadership changes
func (fm *FailoverManager) monitorRaftState() {
	defer fm.wg.Done()
	
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			fm.checkLeadershipChange()
			
		case <-fm.ctx.Done():
			return
		}
	}
}

// checkLeadershipChange checks if leadership has changed
func (fm *FailoverManager) checkLeadershipChange() {
	term, state, leader := fm.raftNode.GetState()
	
	fm.mu.Lock()
	defer fm.mu.Unlock()
	
	oldRole := fm.currentRole
	newRole := fm.determineRole(state, leader)
	
	if oldRole != newRole {
		fm.logger.Info("Node role changed", 
			"oldRole", oldRole, 
			"newRole", newRole,
			"term", term,
			"leader", leader)
		
		fm.currentRole = newRole
		
		// Trigger role transition
		go fm.handleRoleTransition(oldRole, newRole)
	}
}

// determineRole determines the node's role based on Raft state
func (fm *FailoverManager) determineRole(state raft.NodeState, leader *raft.NodeID) NodeRole {
	if state == raft.StateLeader {
		return RolePrimary
	}
	
	if leader != nil {
		return RoleStandby
	}
	
	return RoleFollower
}

// handleRoleTransition handles transition between roles
func (fm *FailoverManager) handleRoleTransition(oldRole, newRole NodeRole) {
	fm.logger.Info("Handling role transition", "from", oldRole, "to", newRole)
	
	switch newRole {
	case RolePrimary:
		if err := fm.transitionToPrimary(); err != nil {
			fm.logger.Error("Failed to transition to primary", "error", err)
			// Trigger re-election or recovery
			fm.triggerRecovery()
		}
		
	case RoleStandby:
		if err := fm.transitionToStandby(); err != nil {
			fm.logger.Error("Failed to transition to standby", "error", err)
		}
		
	case RoleFollower:
		if err := fm.transitionToFollower(); err != nil {
			fm.logger.Error("Failed to transition to follower", "error", err)
		}
	}
}

// transitionToPrimary transitions the node to primary role
func (fm *FailoverManager) transitionToPrimary() error {
	fm.mu.Lock()
	if fm.failoverInProgress {
		fm.mu.Unlock()
		return fmt.Errorf("failover already in progress")
	}
	
	// Check minimum failover interval
	if time.Since(fm.lastFailoverTime) < fm.config.MinFailoverInterval {
		fm.mu.Unlock()
		return fmt.Errorf("minimum failover interval not met")
	}
	
	fm.failoverInProgress = true
	fm.mu.Unlock()
	
	defer func() {
		fm.mu.Lock()
		fm.failoverInProgress = false
		fm.lastFailoverTime = time.Now()
		fm.mu.Unlock()
	}()
	
	fm.logger.Info("Starting transition to primary")
	
	// Step 1: Stop any existing replication
	if err := fm.replicationManager.StopReplica(); err != nil {
		fm.logger.Warn("Error stopping replica mode", "error", err)
	}
	
	// Step 2: Ensure WAL is flushed and consistent
	if err := fm.walManager.Flush(); err != nil {
		return fmt.Errorf("failed to flush WAL: %w", err)
	}
	
	// Step 3: Start as primary
	if err := fm.replicationManager.StartPrimary(); err != nil {
		return fmt.Errorf("failed to start as primary: %w", err)
	}
	
	// Step 4: Add known replicas
	for _, node := range fm.config.ClusterNodes {
		if node.NodeID != fm.config.NodeID {
			if err := fm.replicationManager.AddReplica(node.ReplicationAddr); err != nil {
				fm.logger.Warn("Failed to add replica", 
					"nodeID", node.NodeID, 
					"address", node.ReplicationAddr,
					"error", err)
			}
		}
	}
	
	fm.logger.Info("Successfully transitioned to primary")
	return nil
}

// transitionToStandby transitions the node to standby role
func (fm *FailoverManager) transitionToStandby() error {
	fm.logger.Info("Starting transition to standby")
	
	// Get current leader
	leader := fm.raftNode.GetLeader()
	if leader == nil {
		return fmt.Errorf("no leader available")
	}
	
	// Find leader's replication address
	var leaderAddr string
	for _, node := range fm.config.ClusterNodes {
		if node.NodeID == *leader {
			leaderAddr = node.ReplicationAddr
			break
		}
	}
	
	if leaderAddr == "" {
		return fmt.Errorf("leader address not found for %s", *leader)
	}
	
	// Stop primary mode if active
	fm.replicationManager.StopReplica()
	
	// Connect to leader as replica
	if err := fm.replicationManager.StartReplica(leaderAddr); err != nil {
		return fmt.Errorf("failed to start as replica: %w", err)
	}
	
	fm.logger.Info("Successfully transitioned to standby", "leader", *leader)
	return nil
}

// transitionToFollower transitions the node to follower role (no leader)
func (fm *FailoverManager) transitionToFollower() error {
	fm.logger.Info("Starting transition to follower")
	
	// Stop any active replication
	fm.replicationManager.StopReplica()
	
	fm.logger.Info("Successfully transitioned to follower")
	return nil
}

// performHealthChecks performs periodic health checks
func (fm *FailoverManager) performHealthChecks() {
	defer fm.wg.Done()
	
	ticker := time.NewTicker(fm.config.HealthCheckInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			fm.checkHealth()
			
		case <-fm.ctx.Done():
			return
		}
	}
}

// checkHealth checks the health of the current node
func (fm *FailoverManager) checkHealth() {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	
	healthy := true
	
	// Check replication status
	replStatus := fm.replicationManager.GetStatus()
	if replStatus.State == "ERROR" {
		healthy = false
		fm.logger.Warn("Replication unhealthy", "state", replStatus.State)
	}
	
	// Check WAL status (WAL manager doesn't have IsHealthy, so we'll skip this for now)
	// In a real implementation, we'd check WAL health metrics
	
	// Update health state
	fm.isHealthy = healthy
	fm.lastHealthCheck = time.Now()
	
	// If unhealthy and we're primary, consider stepping down
	if !healthy && fm.currentRole == RolePrimary {
		fm.logger.Warn("Primary node unhealthy, considering step down")
		go fm.considerStepDown()
	}
}

// considerStepDown considers stepping down from primary role
func (fm *FailoverManager) considerStepDown() {
	// In a real implementation, this would:
	// 1. Check if there are healthy standbys
	// 2. Trigger a new election if appropriate
	// 3. Ensure data consistency before stepping down
	
	fm.logger.Info("Step down evaluation completed")
}

// failoverCoordinator coordinates failover operations
func (fm *FailoverManager) failoverCoordinator() {
	defer fm.wg.Done()
	
	for {
		select {
		case <-fm.ctx.Done():
			return
		}
	}
}

// triggerRecovery triggers recovery procedures
func (fm *FailoverManager) triggerRecovery() {
	fm.logger.Info("Triggering recovery procedures")
	
	// In a real implementation, this would:
	// 1. Attempt to restore service
	// 2. Trigger alerts
	// 3. Potentially restart services
}

// GetStatus returns the current failover status
func (fm *FailoverManager) GetStatus() *FailoverStatus {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	
	term, raftState, leader := fm.raftNode.GetState()
	
	return &FailoverStatus{
		NodeID:             fm.config.NodeID,
		CurrentRole:        fm.currentRole,
		RaftState:          raftState,
		RaftTerm:           term,
		Leader:             leader,
		IsHealthy:          fm.isHealthy,
		LastHealthCheck:    fm.lastHealthCheck,
		FailoverInProgress: fm.failoverInProgress,
		LastFailoverTime:   fm.lastFailoverTime,
	}
}

// FailoverStatus represents the current failover status
type FailoverStatus struct {
	NodeID             raft.NodeID
	CurrentRole        NodeRole
	RaftState          raft.NodeState
	RaftTerm           raft.Term
	Leader             *raft.NodeID
	IsHealthy          bool
	LastHealthCheck    time.Time
	FailoverInProgress bool
	LastFailoverTime   time.Time
}

// PromoteToPrimary manually promotes the node to primary (for testing/admin)
func (fm *FailoverManager) PromoteToPrimary() error {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	
	if fm.currentRole == RolePrimary {
		return fmt.Errorf("already primary")
	}
	
	// This would trigger a Raft election
	// In a real implementation, we'd expose a method on RaftNode
	fm.logger.Info("Manual promotion to primary requested")
	
	return fmt.Errorf("manual promotion not yet implemented")
}

// DefaultFailoverConfig returns default failover configuration
func DefaultFailoverConfig() *FailoverConfig {
	return &FailoverConfig{
		HealthCheckInterval: 5 * time.Second,
		FailoverTimeout:     30 * time.Second,
		MinFailoverInterval: 60 * time.Second,
	}
}