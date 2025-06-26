package cluster

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dshills/QuantaDB/internal/cluster/failover"
	"github.com/dshills/QuantaDB/internal/cluster/raft"
	"github.com/dshills/QuantaDB/internal/cluster/replication"
	"github.com/dshills/QuantaDB/internal/log"
	"github.com/dshills/QuantaDB/internal/wal"
)

// Coordinator manages all cluster operations including Raft, replication, and failover
type Coordinator struct {
	config *Config
	logger log.Logger

	// Core components
	raftNode           raft.RaftNode
	raftTransport      raft.RaftTransport
	raftStorage        raft.Storage
	replicationManager replication.ReplicationManager
	failoverManager    *failover.FailoverManager
	walManager         *wal.Manager

	// State
	mu       sync.RWMutex
	started  bool
	nodeInfo *NodeInfo

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
}

// NodeInfo contains information about the current node
type NodeInfo struct {
	NodeID          string
	RaftAddress     string
	ReplicationPort int
	Role            failover.NodeRole
	StartTime       time.Time
}

// NewCoordinator creates a new cluster coordinator
func NewCoordinator(
	clusterConfig *Config,
	walManager *wal.Manager,
	logger log.Logger,
) (*Coordinator, error) {
	ctx, cancel := context.WithCancel(context.Background())

	// Create Raft configuration
	raftConfig := clusterConfig.ToRaftConfig()

	// Create Raft storage
	raftStorage, err := raft.NewFileStorage(raftConfig.DataDir, logger)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create Raft storage: %w", err)
	}

	// Create state machine
	stateMachine := raft.NewDatabaseStateMachine(logger)

	// Create Raft transport
	raftTransport := raft.NewTCPTransport(raftConfig, logger)

	// Create Raft node
	raftNode, err := raft.NewRaftNode(raftConfig, raftTransport, raftStorage, stateMachine, logger)
	if err != nil {
		cancel()
		raftStorage.Close()
		return nil, fmt.Errorf("failed to create Raft node: %w", err)
	}

	// Create replication configuration
	replConfig := &replication.ReplicationConfig{
		NodeID:            replication.NodeID(clusterConfig.NodeID),
		Mode:              replication.ReplicationModeReplica, // Start as replica
		Address:           fmt.Sprintf(":%d", clusterConfig.ReplicationPort),
		HeartbeatInterval: 5 * time.Second,
		ReconnectInterval: 10 * time.Second,
		BatchSize:         100,
		FlushInterval:     100 * time.Millisecond,
		MaxLagTime:        5 * time.Minute,
	}

	// Create replication manager
	replManager := replication.NewReplicationManager(replConfig, walManager, logger)

	// Create failover configuration
	failoverConfig := &failover.FailoverConfig{
		NodeID:              raft.NodeID(clusterConfig.NodeID),
		ClusterNodes:        convertClusterNodes(clusterConfig),
		HealthCheckInterval: 5 * time.Second,
		FailoverTimeout:     30 * time.Second,
		MinFailoverInterval: 60 * time.Second,
	}

	// Create failover manager
	failoverMgr := failover.NewFailoverManager(
		failoverConfig,
		raftNode,
		replManager,
		walManager,
		logger,
	)

	return &Coordinator{
		config:             clusterConfig,
		logger:             logger,
		raftNode:           raftNode,
		raftTransport:      raftTransport,
		raftStorage:        raftStorage,
		replicationManager: replManager,
		failoverManager:    failoverMgr,
		walManager:         walManager,
		nodeInfo: &NodeInfo{
			NodeID:          clusterConfig.NodeID,
			RaftAddress:     clusterConfig.RaftAddress,
			ReplicationPort: clusterConfig.ReplicationPort,
			Role:            failover.RoleFollower,
			StartTime:       time.Now(),
		},
		ctx:    ctx,
		cancel: cancel,
	}, nil
}

// Start starts the cluster coordinator
func (c *Coordinator) Start() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.started {
		return fmt.Errorf("coordinator already started")
	}

	c.logger.Info("Starting cluster coordinator",
		"nodeID", c.config.NodeID,
		"raftAddress", c.config.RaftAddress,
		"replicationPort", c.config.ReplicationPort)

	// Start Raft node
	if err := c.raftNode.Start(); err != nil {
		return fmt.Errorf("failed to start Raft node: %w", err)
	}

	// Start failover manager
	if err := c.failoverManager.Start(); err != nil {
		c.raftNode.Stop()
		return fmt.Errorf("failed to start failover manager: %w", err)
	}

	c.started = true
	c.logger.Info("Cluster coordinator started successfully")

	return nil
}

// Stop stops the cluster coordinator
func (c *Coordinator) Stop() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.started {
		return nil
	}

	c.logger.Info("Stopping cluster coordinator")

	// Stop failover manager
	if err := c.failoverManager.Stop(); err != nil {
		c.logger.Error("Failed to stop failover manager", "error", err)
	}

	// Stop replication manager
	if err := c.replicationManager.Close(); err != nil {
		c.logger.Error("Failed to stop replication manager", "error", err)
	}

	// Stop Raft node
	if err := c.raftNode.Stop(); err != nil {
		c.logger.Error("Failed to stop Raft node", "error", err)
	}

	// Close storage
	if err := c.raftStorage.Close(); err != nil {
		c.logger.Error("Failed to close Raft storage", "error", err)
	}

	c.cancel()
	c.started = false

	c.logger.Info("Cluster coordinator stopped")
	return nil
}

// GetStatus returns the current cluster status
func (c *Coordinator) GetStatus() *ClusterStatus {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Get Raft status
	raftTerm, raftState, leader := c.raftNode.GetState()
	raftConfig := c.raftNode.GetClusterConfiguration()

	// Get replication status
	replStatus := c.replicationManager.GetStatus()

	// Get failover status
	failoverStatus := c.failoverManager.GetStatus()

	return &ClusterStatus{
		NodeInfo:          c.nodeInfo,
		RaftTerm:          raftTerm,
		RaftState:         raftState,
		RaftLeader:        leader,
		RaftConfiguration: raftConfig,
		ReplicationStatus: replStatus,
		FailoverStatus:    failoverStatus,
		IsHealthy:         c.isHealthy(),
		Uptime:            time.Since(c.nodeInfo.StartTime),
	}
}

// ClusterStatus represents the overall cluster status
type ClusterStatus struct {
	NodeInfo          *NodeInfo
	RaftTerm          raft.Term
	RaftState         raft.NodeState
	RaftLeader        *raft.NodeID
	RaftConfiguration *raft.ClusterConfiguration
	ReplicationStatus replication.ReplicationStatus
	FailoverStatus    *failover.FailoverStatus
	IsHealthy         bool
	Uptime            time.Duration
}

// IsLeader returns true if this node is the current leader
func (c *Coordinator) IsLeader() bool {
	return c.raftNode.IsLeader()
}

// GetRole returns the current role of the node
func (c *Coordinator) GetRole() failover.NodeRole {
	status := c.failoverManager.GetStatus()
	return status.CurrentRole
}

// AddNode adds a new node to the cluster (leader only)
func (c *Coordinator) AddNode(nodeID string, raftAddr string, replAddr string) error {
	if !c.IsLeader() {
		return fmt.Errorf("only leader can add nodes")
	}

	// Add to Raft cluster
	if err := c.raftNode.AddNode(raft.NodeID(nodeID), raftAddr); err != nil {
		return fmt.Errorf("failed to add node to Raft: %w", err)
	}

	// Add to replication if we're primary
	if c.GetRole() == failover.RolePrimary {
		if err := c.replicationManager.AddReplica(replAddr); err != nil {
			c.logger.Warn("Failed to add replica", "nodeID", nodeID, "error", err)
		}
	}

	c.logger.Info("Added node to cluster", "nodeID", nodeID)
	return nil
}

// RemoveNode removes a node from the cluster (leader only)
func (c *Coordinator) RemoveNode(nodeID string) error {
	if !c.IsLeader() {
		return fmt.Errorf("only leader can remove nodes")
	}

	// Remove from Raft cluster
	if err := c.raftNode.RemoveNode(raft.NodeID(nodeID)); err != nil {
		return fmt.Errorf("failed to remove node from Raft: %w", err)
	}

	// Remove from replication
	if err := c.replicationManager.RemoveReplica(replication.NodeID(nodeID)); err != nil {
		c.logger.Warn("Failed to remove replica", "nodeID", nodeID, "error", err)
	}

	c.logger.Info("Removed node from cluster", "nodeID", nodeID)
	return nil
}

// isHealthy checks if the coordinator is healthy
func (c *Coordinator) isHealthy() bool {
	// Check various health indicators
	failoverStatus := c.failoverManager.GetStatus()
	return failoverStatus.IsHealthy && !failoverStatus.FailoverInProgress
}

// Helper function

func convertClusterNodes(cfg *Config) []failover.ClusterNode {
	nodes := make([]failover.ClusterNode, 0, len(cfg.Peers)+1)

	// Add self
	nodes = append(nodes, failover.ClusterNode{
		NodeID:          raft.NodeID(cfg.NodeID),
		ReplicationAddr: fmt.Sprintf("localhost:%d", cfg.ReplicationPort),
		RaftAddr:        cfg.RaftAddress,
	})

	// Add peers
	for _, peer := range cfg.Peers {
		nodes = append(nodes, failover.ClusterNode{
			NodeID:          raft.NodeID(peer.NodeID),
			ReplicationAddr: peer.ReplicationAddress,
			RaftAddr:        peer.RaftAddress,
		})
	}

	return nodes
}
