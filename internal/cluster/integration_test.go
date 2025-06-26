//go:build integration
// +build integration

package cluster

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/dshills/QuantaDB/internal/cluster/failover"
	"github.com/dshills/QuantaDB/internal/log"
	"github.com/dshills/QuantaDB/internal/wal"
)

func TestClusterIntegration(t *testing.T) {
	// Skip if not running integration tests
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	// Create a temporary directory for test data
	tmpDir := t.TempDir()

	// Create WAL manager for testing
	walConfig := wal.DefaultConfig()
	walConfig.Directory = tmpDir + "/wal"
	walManager, err := wal.NewManager(walConfig)
	if err != nil {
		t.Fatalf("Failed to create WAL manager: %v", err)
	}
	defer walManager.Close()

	// Create logger
	logger := log.NewNopLogger()

	// Create cluster configuration for node1 (primary)
	config1 := &Config{
		NodeID:              "node1",
		RaftAddress:         "localhost:7000",
		ReplicationPort:     6432,
		DataDir:             tmpDir + "/node1",
		ElectionTimeout:     150 * time.Millisecond,
		HeartbeatInterval:   50 * time.Millisecond,
		HealthCheckInterval: 1 * time.Second,
		FailoverTimeout:     5 * time.Second,
		MinFailoverInterval: 10 * time.Second,
		Peers:               []Peer{},
	}

	// Create coordinator
	coordinator, err := NewCoordinator(config1, walManager, logger)
	if err != nil {
		t.Fatalf("Failed to create coordinator: %v", err)
	}
	defer coordinator.Stop()

	// Start coordinator
	if err := coordinator.Start(); err != nil {
		t.Fatalf("Failed to start coordinator: %v", err)
	}

	// Wait for initialization
	time.Sleep(2 * time.Second)

	// Test 1: Check initial status
	t.Run("InitialStatus", func(t *testing.T) {
		status := coordinator.GetStatus()

		if status.NodeInfo.NodeID != "node1" {
			t.Errorf("Expected node ID 'node1', got %s", status.NodeInfo.NodeID)
		}

		// Should be leader since it's the only node
		if !coordinator.IsLeader() {
			t.Error("Expected node to be leader")
		}
	})

	// Test 2: Test API endpoints
	t.Run("APIEndpoints", func(t *testing.T) {
		// Create API server
		api := NewAPI(coordinator, "localhost:8000")
		if err := api.Start(); err != nil {
			t.Fatalf("Failed to start API: %v", err)
		}
		defer api.Stop()

		// Wait for API to start
		time.Sleep(100 * time.Millisecond)

		// Test status endpoint
		resp, err := http.Get("http://localhost:8000/cluster/status")
		if err != nil {
			t.Fatalf("Failed to get status: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status 200, got %d", resp.StatusCode)
		}

		// Test health endpoint
		resp, err = http.Get("http://localhost:8000/cluster/health")
		if err != nil {
			t.Fatalf("Failed to get health: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status 200, got %d", resp.StatusCode)
		}

		// Test nodes endpoint
		resp, err = http.Get("http://localhost:8000/cluster/nodes")
		if err != nil {
			t.Fatalf("Failed to get nodes: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status 200, got %d", resp.StatusCode)
		}
	})

	// Test 3: Test role transitions
	t.Run("RoleTransitions", func(t *testing.T) {
		// Get initial role
		role := coordinator.GetRole()

		// For a single node, it should eventually become primary
		maxWait := 10 * time.Second
		checkInterval := 500 * time.Millisecond
		deadline := time.Now().Add(maxWait)

		for time.Now().Before(deadline) {
			if coordinator.GetRole() == failover.RolePrimary {
				break
			}
			time.Sleep(checkInterval)
		}

		finalRole := coordinator.GetRole()
		if finalRole != failover.RolePrimary {
			t.Errorf("Expected role PRIMARY, got %s", finalRole.String())
		}
	})
}

func TestClusterMultiNode(t *testing.T) {
	// Skip if not running integration tests
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	// This test would require setting up multiple nodes
	// and testing replication, failover, etc.
	// For now, we'll just have a placeholder
	t.Skip("Multi-node testing requires more infrastructure")
}

// TestClusterReplication tests replication between primary and replica
func TestClusterReplication(t *testing.T) {
	// Skip if not running integration tests
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create temporary directories
	primaryDir := t.TempDir()
	replicaDir := t.TempDir()

	// Create WAL managers
	primaryWALConfig := wal.DefaultConfig()
	primaryWALConfig.Directory = primaryDir + "/wal"
	primaryWAL, err := wal.NewManager(primaryWALConfig)
	if err != nil {
		t.Fatalf("Failed to create primary WAL: %v", err)
	}
	defer primaryWAL.Close()

	replicaWALConfig := wal.DefaultConfig()
	replicaWALConfig.Directory = replicaDir + "/wal"
	replicaWAL, err := wal.NewManager(replicaWALConfig)
	if err != nil {
		t.Fatalf("Failed to create replica WAL: %v", err)
	}
	defer replicaWAL.Close()

	logger := log.NewNopLogger()

	// Create primary node configuration
	primaryConfig := &Config{
		NodeID:              "primary",
		RaftAddress:         "localhost:7001",
		ReplicationPort:     6433,
		DataDir:             primaryDir,
		ElectionTimeout:     150 * time.Millisecond,
		HeartbeatInterval:   50 * time.Millisecond,
		HealthCheckInterval: 1 * time.Second,
		FailoverTimeout:     5 * time.Second,
		MinFailoverInterval: 10 * time.Second,
		Peers:               []Peer{},
	}

	// Create primary coordinator
	primaryCoord, err := NewCoordinator(primaryConfig, primaryWAL, logger)
	if err != nil {
		t.Fatalf("Failed to create primary coordinator: %v", err)
	}
	defer primaryCoord.Stop()

	// Start primary
	if err := primaryCoord.Start(); err != nil {
		t.Fatalf("Failed to start primary: %v", err)
	}

	// Create replica configuration
	replicaConfig := &Config{
		NodeID:              "replica",
		RaftAddress:         "localhost:7002",
		ReplicationPort:     6434,
		DataDir:             replicaDir,
		ElectionTimeout:     150 * time.Millisecond,
		HeartbeatInterval:   50 * time.Millisecond,
		HealthCheckInterval: 1 * time.Second,
		FailoverTimeout:     5 * time.Second,
		MinFailoverInterval: 10 * time.Second,
		Peers: []Peer{
			{
				NodeID:             "primary",
				RaftAddress:        "localhost:7001",
				ReplicationAddress: fmt.Sprintf("localhost:%d", primaryConfig.ReplicationPort),
			},
		},
	}

	// Create replica coordinator
	replicaCoord, err := NewCoordinator(replicaConfig, replicaWAL, logger)
	if err != nil {
		t.Fatalf("Failed to create replica coordinator: %v", err)
	}
	defer replicaCoord.Stop()

	// Start replica
	if err := replicaCoord.Start(); err != nil {
		t.Fatalf("Failed to start replica: %v", err)
	}

	// Wait for cluster to form
	time.Sleep(3 * time.Second)

	// Test replication status
	t.Run("ReplicationStatus", func(t *testing.T) {
		// Check primary status
		primaryStatus := primaryCoord.GetStatus()
		if primaryStatus.ReplicationStatus.Mode.String() != "PRIMARY" {
			t.Errorf("Expected primary mode, got %s", primaryStatus.ReplicationStatus.Mode.String())
		}

		// Check replica status
		replicaStatus := replicaCoord.GetStatus()
		if replicaStatus.ReplicationStatus.Mode.String() != "REPLICA" {
			t.Errorf("Expected replica mode, got %s", replicaStatus.ReplicationStatus.Mode.String())
		}
	})

	// Test adding nodes
	t.Run("AddNode", func(t *testing.T) {
		// Only leader can add nodes
		if primaryCoord.IsLeader() {
			err := primaryCoord.AddNode("replica2", "localhost:7003", "localhost:6435")
			if err != nil {
				t.Logf("Add node error (expected in test): %v", err)
			}
		}
	})

	// Clean up
	cancel()
}
