package failover

import (
	"testing"
	"time"

	"github.com/dshills/QuantaDB/internal/cluster/raft"
	"github.com/dshills/QuantaDB/internal/cluster/replication"
	"github.com/dshills/QuantaDB/internal/log"
	"github.com/dshills/QuantaDB/internal/wal"
)

// MockRaftNode implements a mock Raft node for testing
type MockRaftNode struct {
	state    raft.NodeState
	term     raft.Term
	leader   *raft.NodeID
	isLeader bool
}

func (m *MockRaftNode) Start() error                                           { return nil }
func (m *MockRaftNode) Stop() error                                            { return nil }
func (m *MockRaftNode) GetState() (raft.Term, raft.NodeState, *raft.NodeID)   { return m.term, m.state, m.leader }
func (m *MockRaftNode) IsLeader() bool                                         { return m.isLeader }
func (m *MockRaftNode) GetLeader() *raft.NodeID                               { return m.leader }
func (m *MockRaftNode) GetTerm() raft.Term                                     { return m.term }
func (m *MockRaftNode) AppendCommand(data []byte) (raft.LogIndex, error)      { return 0, nil }
func (m *MockRaftNode) GetCommittedIndex() raft.LogIndex                      { return 0 }
func (m *MockRaftNode) GetClusterConfiguration() *raft.ClusterConfiguration   { return nil }
func (m *MockRaftNode) AddNode(nodeID raft.NodeID, address string) error      { return nil }
func (m *MockRaftNode) RemoveNode(nodeID raft.NodeID) error                   { return nil }
func (m *MockRaftNode) RequestVote(args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	return &raft.RequestVoteReply{}, nil
}
func (m *MockRaftNode) AppendEntries(args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	return &raft.AppendEntriesReply{}, nil
}

// MockReplicationManager implements a mock replication manager for testing
type MockReplicationManager struct {
	mode    replication.ReplicationMode
	state   string
	started bool
	primary bool
}

func (m *MockReplicationManager) StartPrimary() error {
	m.primary = true
	m.started = true
	m.mode = replication.ReplicationModePrimary
	m.state = "RUNNING"
	return nil
}

func (m *MockReplicationManager) AddReplica(address string) error {
	return nil
}

func (m *MockReplicationManager) RemoveReplica(nodeID replication.NodeID) error {
	return nil
}

func (m *MockReplicationManager) GetReplicas() []replication.ReplicaInfo {
	return []replication.ReplicaInfo{}
}

func (m *MockReplicationManager) StartReplica(primaryAddr string) error {
	m.primary = false
	m.started = true
	m.mode = replication.ReplicationModeReplica
	m.state = "RUNNING"
	return nil
}

func (m *MockReplicationManager) StopReplica() error {
	m.started = false
	m.state = "STOPPED"
	return nil
}

func (m *MockReplicationManager) GetStatus() replication.ReplicationStatus {
	return replication.ReplicationStatus{
		NodeID: "test-node",
		Mode:   m.mode,
		State:  m.state,
	}
}

func (m *MockReplicationManager) Close() error {
	m.started = false
	return nil
}

// TestFailoverManagerCreation tests creating a failover manager
func TestFailoverManagerCreation(t *testing.T) {
	logger := log.Default()
	
	config := &FailoverConfig{
		NodeID: "test-node",
		ClusterNodes: []ClusterNode{
			{NodeID: "test-node", ReplicationAddr: "localhost:5432", RaftAddr: "localhost:7000"},
		},
		HealthCheckInterval: 1 * time.Second,
		FailoverTimeout:     5 * time.Second,
		MinFailoverInterval: 10 * time.Second,
	}
	
	raftNode := &MockRaftNode{
		state: raft.StateFollower,
		term:  0,
	}
	
	replManager := &MockReplicationManager{}
	
	// Create a mock WAL manager
	walConfig := wal.DefaultConfig()
	walConfig.Directory = t.TempDir()
	walManager, err := wal.NewManager(walConfig)
	if err != nil {
		t.Fatalf("Failed to create WAL manager: %v", err)
	}
	defer walManager.Close()
	
	fm := NewFailoverManager(config, raftNode, replManager, walManager, logger)
	defer fm.Stop()
	
	// Verify initial state
	status := fm.GetStatus()
	if status.NodeID != "test-node" {
		t.Errorf("Expected nodeID test-node, got %s", status.NodeID)
	}
	if status.CurrentRole != RoleFollower {
		t.Errorf("Expected initial role FOLLOWER, got %s", status.CurrentRole)
	}
	if !status.IsHealthy {
		t.Error("Expected node to be initially healthy")
	}
}

// TestRoleTransitions tests role transitions based on Raft state
func TestRoleTransitions(t *testing.T) {
	logger := log.Default()
	
	config := &FailoverConfig{
		NodeID: "test-node",
		ClusterNodes: []ClusterNode{
			{NodeID: "test-node", ReplicationAddr: "localhost:5432", RaftAddr: "localhost:7000"},
			{NodeID: "node-2", ReplicationAddr: "localhost:5433", RaftAddr: "localhost:7001"},
		},
		HealthCheckInterval: 100 * time.Millisecond,
		FailoverTimeout:     5 * time.Second,
		MinFailoverInterval: 10 * time.Second,
	}
	
	raftNode := &MockRaftNode{
		state: raft.StateFollower,
		term:  0,
	}
	
	replManager := &MockReplicationManager{}
	
	walConfig := wal.DefaultConfig()
	walConfig.Directory = t.TempDir()
	walManager, err := wal.NewManager(walConfig)
	if err != nil {
		t.Fatalf("Failed to create WAL manager: %v", err)
	}
	defer walManager.Close()
	
	fm := NewFailoverManager(config, raftNode, replManager, walManager, logger)
	
	// Start the failover manager
	if err := fm.Start(); err != nil {
		t.Fatalf("Failed to start failover manager: %v", err)
	}
	defer fm.Stop()
	
	// Test 1: Transition to Primary
	t.Log("Testing transition to primary")
	raftNode.state = raft.StateLeader
	raftNode.isLeader = true
	raftNode.term = 1
	leader := raft.NodeID("test-node")
	raftNode.leader = &leader
	
	// Wait for role transition
	time.Sleep(200 * time.Millisecond)
	
	status := fm.GetStatus()
	if status.CurrentRole != RolePrimary {
		t.Errorf("Expected role PRIMARY after becoming leader, got %s", status.CurrentRole)
	}
	
	// Verify replication manager was started as primary
	if !replManager.primary {
		t.Error("Expected replication manager to be started as primary")
	}
	
	// Test 2: Transition to Standby
	t.Log("Testing transition to standby")
	raftNode.state = raft.StateFollower
	raftNode.isLeader = false
	raftNode.term = 2
	leader = raft.NodeID("node-2")
	raftNode.leader = &leader
	
	// Wait for role transition
	time.Sleep(200 * time.Millisecond)
	
	status = fm.GetStatus()
	if status.CurrentRole != RoleStandby {
		t.Errorf("Expected role STANDBY after losing leadership, got %s", status.CurrentRole)
	}
	
	// Test 3: Transition to Follower (no leader)
	t.Log("Testing transition to follower")
	raftNode.leader = nil
	
	// Wait for role transition
	time.Sleep(200 * time.Millisecond)
	
	status = fm.GetStatus()
	if status.CurrentRole != RoleFollower {
		t.Errorf("Expected role FOLLOWER when no leader exists, got %s", status.CurrentRole)
	}
}

// TestHealthChecking tests health check functionality
func TestHealthChecking(t *testing.T) {
	logger := log.Default()
	
	config := &FailoverConfig{
		NodeID: "test-node",
		ClusterNodes: []ClusterNode{
			{NodeID: "test-node", ReplicationAddr: "localhost:5432", RaftAddr: "localhost:7000"},
		},
		HealthCheckInterval: 100 * time.Millisecond,
		FailoverTimeout:     5 * time.Second,
		MinFailoverInterval: 10 * time.Second,
	}
	
	raftNode := &MockRaftNode{
		state:    raft.StateLeader,
		term:     1,
		isLeader: true,
	}
	leader := raft.NodeID("test-node")
	raftNode.leader = &leader
	
	replManager := &MockReplicationManager{
		mode:  replication.ReplicationModePrimary,
		state: "RUNNING",
	}
	
	walConfig := wal.DefaultConfig()
	walConfig.Directory = t.TempDir()
	walManager, err := wal.NewManager(walConfig)
	if err != nil {
		t.Fatalf("Failed to create WAL manager: %v", err)
	}
	defer walManager.Close()
	
	fm := NewFailoverManager(config, raftNode, replManager, walManager, logger)
	
	// Start the failover manager
	if err := fm.Start(); err != nil {
		t.Fatalf("Failed to start failover manager: %v", err)
	}
	defer fm.Stop()
	
	// Wait for initial health check
	time.Sleep(200 * time.Millisecond)
	
	// Verify healthy state
	status := fm.GetStatus()
	if !status.IsHealthy {
		t.Error("Expected node to be healthy")
	}
	
	// Simulate unhealthy replication
	replManager.state = "ERROR"
	
	// Wait for health check to detect
	time.Sleep(200 * time.Millisecond)
	
	// Check health status
	fm.checkHealth()
	status = fm.GetStatus()
	if status.IsHealthy {
		t.Error("Expected node to be unhealthy after replication error")
	}
}

// TestFailoverInterval tests minimum failover interval enforcement
func TestFailoverInterval(t *testing.T) {
	logger := log.Default()
	
	config := &FailoverConfig{
		NodeID: "test-node",
		ClusterNodes: []ClusterNode{
			{NodeID: "test-node", ReplicationAddr: "localhost:5432", RaftAddr: "localhost:7000"},
		},
		HealthCheckInterval: 100 * time.Millisecond,
		FailoverTimeout:     5 * time.Second,
		MinFailoverInterval: 1 * time.Second, // Short interval for testing
	}
	
	raftNode := &MockRaftNode{
		state: raft.StateFollower,
		term:  0,
	}
	
	replManager := &MockReplicationManager{}
	
	walConfig := wal.DefaultConfig()
	walConfig.Directory = t.TempDir()
	walManager, err := wal.NewManager(walConfig)
	if err != nil {
		t.Fatalf("Failed to create WAL manager: %v", err)
	}
	defer walManager.Close()
	
	fm := NewFailoverManager(config, raftNode, replManager, walManager, logger)
	defer fm.Stop()
	
	// First transition to primary
	err = fm.transitionToPrimary()
	if err != nil {
		t.Fatalf("First transition to primary failed: %v", err)
	}
	
	// Immediate second transition should fail
	err = fm.transitionToPrimary()
	if err == nil {
		t.Error("Expected second immediate transition to fail due to minimum interval")
	}
	
	// Wait for minimum interval
	time.Sleep(1100 * time.Millisecond)
	
	// Now transition should succeed
	err = fm.transitionToPrimary()
	if err != nil {
		t.Errorf("Transition after interval should succeed: %v", err)
	}
}

// TestDefaultConfig tests default configuration
func TestDefaultConfig(t *testing.T) {
	config := DefaultFailoverConfig()
	
	if config.HealthCheckInterval != 5*time.Second {
		t.Errorf("Expected health check interval 5s, got %v", config.HealthCheckInterval)
	}
	
	if config.FailoverTimeout != 30*time.Second {
		t.Errorf("Expected failover timeout 30s, got %v", config.FailoverTimeout)
	}
	
	if config.MinFailoverInterval != 60*time.Second {
		t.Errorf("Expected min failover interval 60s, got %v", config.MinFailoverInterval)
	}
}