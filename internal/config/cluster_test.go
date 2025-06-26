package config

import (
	"testing"
	"time"

	"github.com/dshills/QuantaDB/internal/cluster/replication"
)

func TestConfig_IsClusterEnabled(t *testing.T) {
	tests := []struct {
		name     string
		mode     string
		expected bool
	}{
		{"none mode", "none", false},
		{"empty mode", "", false},
		{"primary mode", "primary", true},
		{"replica mode", "replica", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := DefaultConfig()
			config.Cluster.Mode = tt.mode

			if got := config.IsClusterEnabled(); got != tt.expected {
				t.Errorf("IsClusterEnabled() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestConfig_IsPrimary(t *testing.T) {
	tests := []struct {
		name     string
		mode     string
		expected bool
	}{
		{"primary mode", "primary", true},
		{"replica mode", "replica", false},
		{"none mode", "none", false},
		{"empty mode", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := DefaultConfig()
			config.Cluster.Mode = tt.mode

			if got := config.IsPrimary(); got != tt.expected {
				t.Errorf("IsPrimary() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestConfig_IsReplica(t *testing.T) {
	tests := []struct {
		name     string
		mode     string
		expected bool
	}{
		{"replica mode", "replica", true},
		{"primary mode", "primary", false},
		{"none mode", "none", false},
		{"empty mode", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := DefaultConfig()
			config.Cluster.Mode = tt.mode

			if got := config.IsReplica(); got != tt.expected {
				t.Errorf("IsReplica() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestConfig_GetReplicationAddress(t *testing.T) {
	config := DefaultConfig()
	config.Host = "localhost"
	config.Port = 5432

	expected := "localhost:6432" // Port + 1000
	if got := config.GetReplicationAddress(); got != expected {
		t.Errorf("GetReplicationAddress() = %v, want %v", got, expected)
	}
}

func TestConfig_GetClusterDataDir(t *testing.T) {
	tests := []struct {
		name       string
		dataDir    string
		clusterDir string
		expected   string
	}{
		{
			name:       "default cluster dir",
			dataDir:    "./data",
			clusterDir: "",
			expected:   "data/cluster",
		},
		{
			name:       "custom cluster dir",
			dataDir:    "./data",
			clusterDir: "/custom/cluster",
			expected:   "/custom/cluster",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := DefaultConfig()
			config.DataDir = tt.dataDir
			config.Cluster.DataDir = tt.clusterDir

			if got := config.GetClusterDataDir(); got != tt.expected {
				t.Errorf("GetClusterDataDir() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestConfig_ToReplicationConfig(t *testing.T) {
	config := DefaultConfig()
	config.Host = "localhost"
	config.Port = 5432
	config.Cluster.NodeID = "test-node"
	config.Cluster.Mode = "primary"
	config.Cluster.Replication.PrimaryAddress = "primary:6432"
	config.Cluster.Replication.StreamBufferSize = 2048
	config.Cluster.Replication.BatchSize = 50
	config.Cluster.Replication.FlushInterval = 200
	config.Cluster.Replication.HeartbeatInterval = 15
	config.Cluster.Replication.HeartbeatTimeout = 45

	repConfig := config.ToReplicationConfig()

	if repConfig.NodeID != "test-node" {
		t.Errorf("NodeID = %v, want %v", repConfig.NodeID, "test-node")
	}

	if repConfig.Mode != replication.ReplicationModePrimary {
		t.Errorf("Mode = %v, want %v", repConfig.Mode, replication.ReplicationModePrimary)
	}

	if repConfig.Address != "localhost:6432" {
		t.Errorf("Address = %v, want %v", repConfig.Address, "localhost:6432")
	}

	if repConfig.PrimaryAddress != "primary:6432" {
		t.Errorf("PrimaryAddress = %v, want %v", repConfig.PrimaryAddress, "primary:6432")
	}

	if repConfig.StreamBufferSize != 2048 {
		t.Errorf("StreamBufferSize = %v, want %v", repConfig.StreamBufferSize, 2048)
	}

	if repConfig.BatchSize != 50 {
		t.Errorf("BatchSize = %v, want %v", repConfig.BatchSize, 50)
	}

	expectedFlushInterval := 200 * time.Millisecond
	if repConfig.FlushInterval != expectedFlushInterval {
		t.Errorf("FlushInterval = %v, want %v", repConfig.FlushInterval, expectedFlushInterval)
	}

	expectedHeartbeatInterval := 15 * time.Second
	if repConfig.HeartbeatInterval != expectedHeartbeatInterval {
		t.Errorf("HeartbeatInterval = %v, want %v", repConfig.HeartbeatInterval, expectedHeartbeatInterval)
	}

	expectedHeartbeatTimeout := 45 * time.Second
	if repConfig.HeartbeatTimeout != expectedHeartbeatTimeout {
		t.Errorf("HeartbeatTimeout = %v, want %v", repConfig.HeartbeatTimeout, expectedHeartbeatTimeout)
	}
}

func TestConfig_ToReplicationConfig_ReplicaMode(t *testing.T) {
	config := DefaultConfig()
	config.Cluster.Mode = "replica"

	repConfig := config.ToReplicationConfig()

	if repConfig.Mode != replication.ReplicationModeReplica {
		t.Errorf("Mode = %v, want %v", repConfig.Mode, replication.ReplicationModeReplica)
	}
}

func TestConfig_ToReplicationConfig_NoneMode(t *testing.T) {
	config := DefaultConfig()
	config.Cluster.Mode = "none"

	repConfig := config.ToReplicationConfig()

	if repConfig.Mode != replication.ReplicationModeNone {
		t.Errorf("Mode = %v, want %v", repConfig.Mode, replication.ReplicationModeNone)
	}
}

func TestConfig_GenerateNodeID(t *testing.T) {
	tests := []struct {
		name         string
		mode         string
		initialID    string
		shouldChange bool
	}{
		{
			name:         "generate for primary with empty ID",
			mode:         "primary",
			initialID:    "",
			shouldChange: true,
		},
		{
			name:         "generate for replica with empty ID",
			mode:         "replica",
			initialID:    "",
			shouldChange: true,
		},
		{
			name:         "keep existing ID",
			mode:         "primary",
			initialID:    "existing-id",
			shouldChange: false,
		},
		{
			name:         "no generation for none mode",
			mode:         "none",
			initialID:    "",
			shouldChange: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := DefaultConfig()
			config.Host = "localhost"
			config.Port = 5432
			config.Cluster.Mode = tt.mode
			config.Cluster.NodeID = tt.initialID

			config.GenerateNodeID()

			if tt.shouldChange {
				if config.Cluster.NodeID == tt.initialID {
					t.Error("NodeID should have been generated but wasn't")
				}
				if config.Cluster.NodeID == "" {
					t.Error("NodeID should not be empty after generation")
				}
			} else {
				if config.Cluster.NodeID != tt.initialID {
					t.Errorf("NodeID should not have changed: got %v, want %v",
						config.Cluster.NodeID, tt.initialID)
				}
			}
		})
	}
}
