package cluster

import (
	"time"

	"github.com/dshills/QuantaDB/internal/cluster/raft"
)

// Config represents comprehensive cluster configuration
type Config struct {
	// Node identity
	NodeID          string
	RaftAddress     string // Address for Raft consensus
	ReplicationPort int    // Port for database replication
	DataDir         string

	// Cluster peers
	Peers []Peer

	// Timing configuration
	ElectionTimeout     time.Duration
	HeartbeatInterval   time.Duration
	HealthCheckInterval time.Duration
	FailoverTimeout     time.Duration
	MinFailoverInterval time.Duration
}

// Peer represents a cluster peer
type Peer struct {
	NodeID             string
	RaftAddress        string
	ReplicationAddress string
}

// DefaultConfig returns default cluster configuration
func DefaultConfig() *Config {
	return &Config{
		ElectionTimeout:     150 * time.Millisecond,
		HeartbeatInterval:   50 * time.Millisecond,
		HealthCheckInterval: 5 * time.Second,
		FailoverTimeout:     30 * time.Second,
		MinFailoverInterval: 60 * time.Second,
		ReplicationPort:     5432,
		DataDir:             "./data/cluster",
		Peers:               []Peer{},
	}
}

// ToRaftConfig converts to Raft configuration
func (c *Config) ToRaftConfig() *raft.RaftConfig {
	peers := make([]raft.NodeID, len(c.Peers))
	peerAddresses := make(map[raft.NodeID]string)

	for i, p := range c.Peers {
		peers[i] = raft.NodeID(p.NodeID)
		peerAddresses[raft.NodeID(p.NodeID)] = p.RaftAddress
	}

	return &raft.RaftConfig{
		NodeID:            raft.NodeID(c.NodeID),
		Address:           c.RaftAddress,
		Peers:             peers,
		PeerAddresses:     peerAddresses,
		ElectionTimeout:   c.ElectionTimeout,
		HeartbeatInterval: c.HeartbeatInterval,
		RequestTimeout:    100 * time.Millisecond,
		MaxLogEntries:     100,
		DataDir:           c.DataDir + "/raft",
	}
}
