package config

import (
	"time"

	"github.com/dshills/QuantaDB/internal/cluster/replication"
)

// ToReplicationConfig converts the config to a replication config
func (c *Config) ToReplicationConfig() *replication.ReplicationConfig {
	var mode replication.ReplicationMode
	switch c.Cluster.Mode {
	case "primary":
		mode = replication.ReplicationModePrimary
	case "replica":
		mode = replication.ReplicationModeReplica
	default:
		mode = replication.ReplicationModeNone
	}

	return &replication.ReplicationConfig{
		NodeID:            replication.NodeID(c.Cluster.NodeID),
		Address:           c.GetReplicationAddress(),
		DataDir:           c.GetClusterDataDir(),
		Mode:              mode,
		PrimaryAddress:    c.Cluster.Replication.PrimaryAddress,
		StreamBufferSize:  c.Cluster.Replication.StreamBufferSize,
		BatchSize:         c.Cluster.Replication.BatchSize,
		FlushInterval:     time.Duration(c.Cluster.Replication.FlushInterval) * time.Millisecond,
		HeartbeatInterval: time.Duration(c.Cluster.Replication.HeartbeatInterval) * time.Second,
		HeartbeatTimeout:  time.Duration(c.Cluster.Replication.HeartbeatTimeout) * time.Second,
		MaxLagBytes:       int64(c.Cluster.Replication.MaxLagBytes),
		MaxLagTime:        time.Duration(c.Cluster.Replication.MaxLagTime) * time.Second,
		ConnectTimeout:    time.Duration(c.Cluster.Replication.ConnectTimeout) * time.Second,
		ReconnectInterval: time.Duration(c.Cluster.Replication.ReconnectInterval) * time.Second,
		MaxReconnectTries: c.Cluster.Replication.MaxReconnectTries,
	}
}

// GenerateNodeID generates a node ID if not set
func (c *Config) GenerateNodeID() {
	if c.Cluster.NodeID == "" && c.IsClusterEnabled() {
		// Generate a simple node ID based on address and timestamp
		c.Cluster.NodeID = c.Host + ":" + string(rune(c.Port))
	}
}
