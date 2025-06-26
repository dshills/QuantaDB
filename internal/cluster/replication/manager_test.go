package replication

import (
	"testing"

	"github.com/dshills/QuantaDB/internal/log"
	"github.com/dshills/QuantaDB/internal/wal"
)

func TestNewReplicationManager(t *testing.T) {
	config := DefaultReplicationConfig()
	config.NodeID = "test-node"
	config.Mode = ReplicationModePrimary

	// Create a mock WAL manager
	walConfig := wal.DefaultConfig()
	walConfig.Directory = t.TempDir()
	walMgr, err := wal.NewManager(walConfig)
	if err != nil {
		t.Fatalf("Failed to create WAL manager: %v", err)
	}
	defer walMgr.Close()

	logger := log.Default()

	rm := NewReplicationManager(config, walMgr, logger)

	if rm == nil {
		t.Fatal("NewReplicationManager returned nil")
	}

	if rm.config != config {
		t.Error("Config not set correctly")
	}

	if rm.walMgr != walMgr {
		t.Error("WAL manager not set correctly")
	}

	if rm.mode != ReplicationModePrimary {
		t.Errorf("Mode = %v, want %v", rm.mode, ReplicationModePrimary)
	}

	status := rm.GetStatus()
	if status.Mode != ReplicationModePrimary {
		t.Errorf("Status mode = %v, want %v", status.Mode, ReplicationModePrimary)
	}

	if status.NodeID != "test-node" {
		t.Errorf("Status NodeID = %v, want %v", status.NodeID, "test-node")
	}

	if status.State != "INITIALIZED" {
		t.Errorf("Status state = %v, want %v", status.State, "INITIALIZED")
	}
}

func TestReplicationManager_PrimaryMode(t *testing.T) {
	config := DefaultReplicationConfig()
	config.NodeID = "primary-node"
	config.Mode = ReplicationModePrimary
	config.Address = "localhost:6543"

	// Create a mock WAL manager
	walConfig := wal.DefaultConfig()
	walConfig.Directory = t.TempDir()
	walMgr, err := wal.NewManager(walConfig)
	if err != nil {
		t.Fatalf("Failed to create WAL manager: %v", err)
	}
	defer walMgr.Close()

	logger := log.Default()
	rm := NewReplicationManager(config, walMgr, logger)
	defer rm.Close()

	// Test that only primary can add replicas
	err = rm.AddReplica("localhost:6544")
	if err == nil {
		t.Error("Expected error when adding replica before starting primary")
	}

	// Note: We can't actually start the primary in tests without a real network setup
	// This would require integration tests with actual network connections
}

func TestReplicationManager_ReplicaMode(t *testing.T) {
	config := DefaultReplicationConfig()
	config.NodeID = "replica-node"
	config.Mode = ReplicationModeReplica

	// Create a mock WAL manager
	walConfig := wal.DefaultConfig()
	walConfig.Directory = t.TempDir()
	walMgr, err := wal.NewManager(walConfig)
	if err != nil {
		t.Fatalf("Failed to create WAL manager: %v", err)
	}
	defer walMgr.Close()

	logger := log.Default()
	rm := NewReplicationManager(config, walMgr, logger)
	defer rm.Close()

	// Test that replica cannot add replicas
	err = rm.AddReplica("localhost:6544")
	if err == nil {
		t.Error("Expected error when replica tries to add replica")
	}

	// Test that replica cannot remove replicas
	err = rm.RemoveReplica("test")
	if err == nil {
		t.Error("Expected error when replica tries to remove replica")
	}

	// Note: We can't actually start the replica in tests without a real primary
	// This would require integration tests with actual network connections
}

func TestReplicationManager_GetStatus(t *testing.T) {
	config := DefaultReplicationConfig()
	config.NodeID = "status-test-node"
	config.Mode = ReplicationModePrimary
	config.Address = "localhost:6545"

	// Create a mock WAL manager
	walConfig := wal.DefaultConfig()
	walConfig.Directory = t.TempDir()
	walMgr, err := wal.NewManager(walConfig)
	if err != nil {
		t.Fatalf("Failed to create WAL manager: %v", err)
	}
	defer walMgr.Close()

	logger := log.Default()
	rm := NewReplicationManager(config, walMgr, logger)
	defer rm.Close()

	status := rm.GetStatus()

	if status.Mode != ReplicationModePrimary {
		t.Errorf("Status mode = %v, want %v", status.Mode, ReplicationModePrimary)
	}

	if status.NodeID != "status-test-node" {
		t.Errorf("Status NodeID = %v, want %v", status.NodeID, "status-test-node")
	}

	if status.Address != "localhost:6545" {
		t.Errorf("Status Address = %v, want %v", status.Address, "localhost:6545")
	}

	if status.ReplicaCount != 0 {
		t.Errorf("Status ReplicaCount = %v, want %v", status.ReplicaCount, 0)
	}

	if status.ReplicaStates == nil {
		t.Error("Status ReplicaStates should not be nil")
	}
}

func TestReplicationManager_Close(t *testing.T) {
	config := DefaultReplicationConfig()
	config.NodeID = "close-test-node"
	config.Mode = ReplicationModePrimary

	// Create a mock WAL manager
	walConfig := wal.DefaultConfig()
	walConfig.Directory = t.TempDir()
	walMgr, err := wal.NewManager(walConfig)
	if err != nil {
		t.Fatalf("Failed to create WAL manager: %v", err)
	}
	defer walMgr.Close()

	logger := log.Default()
	rm := NewReplicationManager(config, walMgr, logger)

	// Close should not return error
	err = rm.Close()
	if err != nil {
		t.Errorf("Close() returned error: %v", err)
	}

	// Multiple calls to Close should be safe
	err = rm.Close()
	if err != nil {
		t.Errorf("Second Close() returned error: %v", err)
	}

	// Status should show closed state
	status := rm.GetStatus()
	if status.State != "CLOSED" {
		t.Errorf("Status state after close = %v, want %v", status.State, "CLOSED")
	}
}

func TestReplicationManager_GetReplicas(t *testing.T) {
	config := DefaultReplicationConfig()
	config.NodeID = "replicas-test-node"
	config.Mode = ReplicationModePrimary

	// Create a mock WAL manager
	walConfig := wal.DefaultConfig()
	walConfig.Directory = t.TempDir()
	walMgr, err := wal.NewManager(walConfig)
	if err != nil {
		t.Fatalf("Failed to create WAL manager: %v", err)
	}
	defer walMgr.Close()

	logger := log.Default()
	rm := NewReplicationManager(config, walMgr, logger)
	defer rm.Close()

	// Initially should have no replicas
	replicas := rm.GetReplicas()
	if len(replicas) != 0 {
		t.Errorf("GetReplicas() returned %d replicas, want 0", len(replicas))
	}

	// The slice should not be nil
	if replicas == nil {
		t.Error("GetReplicas() returned nil")
	}
}
