package cluster

import (
	"fmt"
	"testing"
	"time"

	"github.com/dshills/QuantaDB/internal/cluster/failover"
	"github.com/dshills/QuantaDB/internal/log"
	"github.com/dshills/QuantaDB/internal/wal"
)

// TestClusterIntegration tests the integration of all cluster components
func TestClusterIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	logger := log.Default()

	// Create three nodes for testing
	nodes := []struct {
		nodeID   string
		raftPort int
		replPort int
	}{
		{"node-1", 7001, 5432},
		{"node-2", 7002, 5433},
		{"node-3", 7003, 5434},
	}

	coordinators := make([]*Coordinator, 0, len(nodes))

	// Create and start all nodes
	for i, node := range nodes {
		// Create WAL manager for each node
		walConfig := wal.DefaultConfig()
		walConfig.Directory = t.TempDir() + "/" + node.nodeID
		walManager, err := wal.NewManager(walConfig)
		if err != nil {
			t.Fatalf("Failed to create WAL manager for %s: %v", node.nodeID, err)
		}
		defer walManager.Close()

		// Create cluster configuration with defaults
		clusterConfig := DefaultConfig()
		clusterConfig.NodeID = node.nodeID
		clusterConfig.RaftAddress = fmt.Sprintf("127.0.0.1:%d", node.raftPort)
		clusterConfig.ReplicationPort = node.replPort
		clusterConfig.DataDir = t.TempDir() + "/" + node.nodeID
		clusterConfig.Peers = []Peer{}

		// Add other nodes as peers
		for _, peer := range nodes {
			if peer.nodeID != node.nodeID {
				clusterConfig.Peers = append(clusterConfig.Peers, Peer{
					NodeID:             peer.nodeID,
					RaftAddress:        fmt.Sprintf("127.0.0.1:%d", peer.raftPort),
					ReplicationAddress: fmt.Sprintf("127.0.0.1:%d", peer.replPort),
				})
			}
		}

		// Create coordinator
		coordinator, err := NewCoordinator(clusterConfig, walManager, logger)
		if err != nil {
			t.Fatalf("Failed to create coordinator for %s: %v", node.nodeID, err)
		}
		coordinators = append(coordinators, coordinator)

		// Start coordinator
		if err := coordinator.Start(); err != nil {
			t.Fatalf("Failed to start coordinator for %s: %v", node.nodeID, err)
		}

		// Stagger node startup to avoid election conflicts
		if i < len(nodes)-1 {
			time.Sleep(100 * time.Millisecond)
		}
	}

	// Clean up all coordinators when done
	defer func() {
		for _, c := range coordinators {
			c.Stop()
		}
	}()

	t.Log("All nodes started, waiting for leader election...")

	// Wait for leader election
	time.Sleep(2 * time.Second)

	// Find the leader
	var leaderIdx int = -1
	for i, c := range coordinators {
		if c.IsLeader() {
			leaderIdx = i
			t.Logf("Node %s is the leader", nodes[i].nodeID)
			break
		}
	}

	if leaderIdx == -1 {
		t.Fatal("No leader elected")
	}

	// Verify all nodes agree on the leader
	leaderStatus := coordinators[leaderIdx].GetStatus()
	for i, c := range coordinators {
		status := c.GetStatus()
		if status.RaftLeader == nil {
			t.Errorf("Node %s has no leader", nodes[i].nodeID)
			continue
		}
		if *status.RaftLeader != *leaderStatus.RaftLeader {
			t.Errorf("Node %s disagrees on leader: expected %v, got %v",
				nodes[i].nodeID, *leaderStatus.RaftLeader, *status.RaftLeader)
		}
	}

	// Verify roles
	primaryCount := 0
	standbyCount := 0
	for i, c := range coordinators {
		role := c.GetRole()
		t.Logf("Node %s has role %s", nodes[i].nodeID, role)

		switch role {
		case failover.RolePrimary:
			primaryCount++
		case failover.RoleStandby:
			standbyCount++
		case failover.RoleFollower:
			// Expected for nodes that aren't primary or standby
		}
	}

	if primaryCount != 1 {
		t.Errorf("Expected exactly 1 primary, got %d", primaryCount)
	}

	if standbyCount != len(nodes)-1 {
		t.Errorf("Expected %d standbys, got %d", len(nodes)-1, standbyCount)
	}

	t.Log("Cluster integration test completed successfully")
}

// TestFailoverScenario tests a failover scenario
func TestFailoverScenario(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping failover scenario test in short mode")
	}

	logger := log.Default()

	// Create two nodes for simple failover test
	nodes := []struct {
		nodeID   string
		raftPort int
		replPort int
	}{
		{"primary", 7001, 5432},
		{"standby", 7002, 5433},
	}

	coordinators := make([]*Coordinator, 0, len(nodes))

	// Create and start both nodes
	for i, node := range nodes {
		walConfig := wal.DefaultConfig()
		walConfig.Directory = t.TempDir() + "/" + node.nodeID
		walManager, err := wal.NewManager(walConfig)
		if err != nil {
			t.Fatalf("Failed to create WAL manager for %s: %v", node.nodeID, err)
		}
		defer walManager.Close()

		clusterConfig := DefaultConfig()
		clusterConfig.NodeID = node.nodeID
		clusterConfig.RaftAddress = fmt.Sprintf("127.0.0.1:%d", node.raftPort)
		clusterConfig.ReplicationPort = node.replPort
		clusterConfig.DataDir = t.TempDir() + "/" + node.nodeID
		clusterConfig.Peers = []Peer{}

		// Add peer
		peerIdx := (i + 1) % len(nodes)
		peer := nodes[peerIdx]
		clusterConfig.Peers = append(clusterConfig.Peers, Peer{
			NodeID:             peer.nodeID,
			RaftAddress:        fmt.Sprintf("127.0.0.1:%d", peer.raftPort),
			ReplicationAddress: fmt.Sprintf("127.0.0.1:%d", peer.replPort),
		})

		coordinator, err := NewCoordinator(clusterConfig, walManager, logger)
		if err != nil {
			t.Fatalf("Failed to create coordinator for %s: %v", node.nodeID, err)
		}
		coordinators = append(coordinators, coordinator)

		if err := coordinator.Start(); err != nil {
			t.Fatalf("Failed to start coordinator for %s: %v", node.nodeID, err)
		}

		time.Sleep(100 * time.Millisecond)
	}

	defer func() {
		for _, c := range coordinators {
			c.Stop()
		}
	}()

	// Wait for initial setup
	time.Sleep(2 * time.Second)

	// Find primary
	var primaryIdx int = -1
	for i, c := range coordinators {
		if c.GetRole() == failover.RolePrimary {
			primaryIdx = i
			t.Logf("Node %s is the primary", nodes[i].nodeID)
			break
		}
	}

	if primaryIdx == -1 {
		t.Fatal("No primary found")
	}

	// Simulate primary failure by stopping it
	t.Log("Simulating primary failure...")
	coordinators[primaryIdx].Stop()

	// Wait for failover
	time.Sleep(3 * time.Second)

	// Check that standby became primary
	standbyIdx := (primaryIdx + 1) % len(nodes)
	newRole := coordinators[standbyIdx].GetRole()

	if newRole != failover.RolePrimary {
		t.Errorf("Expected standby to become primary after failover, got role %s", newRole)
	}

	t.Log("Failover scenario test completed")
}

// TestClusterMembership tests adding and removing nodes
func TestClusterMembership(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping cluster membership test in short mode")
	}

	logger := log.Default()

	// Start with a single node
	walConfig := wal.DefaultConfig()
	walConfig.Directory = t.TempDir() + "/node-1"
	walManager, err := wal.NewManager(walConfig)
	if err != nil {
		t.Fatalf("Failed to create WAL manager: %v", err)
	}
	defer walManager.Close()

	clusterConfig := DefaultConfig()
	clusterConfig.NodeID = "node-1"
	clusterConfig.RaftAddress = "127.0.0.1:7001"
	clusterConfig.ReplicationPort = 5432
	clusterConfig.DataDir = t.TempDir() + "/node-1"
	clusterConfig.Peers = []Peer{}

	coordinator, err := NewCoordinator(clusterConfig, walManager, logger)
	if err != nil {
		t.Fatalf("Failed to create coordinator: %v", err)
	}
	defer coordinator.Stop()

	if err := coordinator.Start(); err != nil {
		t.Fatalf("Failed to start coordinator: %v", err)
	}

	// Wait for single-node leadership
	time.Sleep(1 * time.Second)

	if !coordinator.IsLeader() {
		t.Fatal("Single node should be leader")
	}

	// Test adding a node
	err = coordinator.AddNode("node-2", "127.0.0.1:7002", "127.0.0.1:5433")
	if err != nil {
		t.Fatalf("Failed to add node: %v", err)
	}

	// Verify node was added to Raft configuration
	status := coordinator.GetStatus()
	if status.RaftConfiguration == nil {
		t.Fatal("Raft configuration is nil")
	}

	if len(status.RaftConfiguration.Members) != 2 {
		t.Errorf("Expected 2 members after adding node, got %d", len(status.RaftConfiguration.Members))
	}

	// Test removing a node
	err = coordinator.RemoveNode("node-2")
	if err != nil {
		t.Fatalf("Failed to remove node: %v", err)
	}

	// Verify node was removed
	status = coordinator.GetStatus()
	if len(status.RaftConfiguration.Members) != 1 {
		t.Errorf("Expected 1 member after removing node, got %d", len(status.RaftConfiguration.Members))
	}

	t.Log("Cluster membership test completed")
}

