package replication

import (
	"time"

	"github.com/dshills/QuantaDB/internal/wal"
)

// NodeID represents a unique identifier for a cluster node
type NodeID string

// ReplicaState represents the state of a replica node
type ReplicaState int

const (
	ReplicaStateUnknown ReplicaState = iota
	ReplicaStateConnecting
	ReplicaStateStreaming
	ReplicaStateLagging
	ReplicaStateDisconnected
	ReplicaStateFailed
)

// String returns string representation of replica state
func (rs ReplicaState) String() string {
	switch rs {
	case ReplicaStateConnecting:
		return "CONNECTING"
	case ReplicaStateStreaming:
		return "STREAMING"
	case ReplicaStateLagging:
		return "LAGGING"
	case ReplicaStateDisconnected:
		return "DISCONNECTED"
	case ReplicaStateFailed:
		return "FAILED"
	default:
		return "UNKNOWN"
	}
}

// ReplicaInfo contains information about a replica node
type ReplicaInfo struct {
	NodeID          NodeID
	Address         string
	State           ReplicaState
	LastReceivedLSN wal.LSN
	LastAppliedLSN  wal.LSN
	LagBytes        int64
	LagTime         time.Duration
	ConnectedAt     time.Time
	LastHeartbeat   time.Time
}

// WALStreamMessage represents a message in the WAL streaming protocol
type WALStreamMessage struct {
	Type      WALStreamMessageType
	StartLSN  wal.LSN
	EndLSN    wal.LSN
	Data      []byte
	Timestamp time.Time
}

// WALStreamMessageType represents the type of WAL stream message
type WALStreamMessageType int

const (
	WALStreamMessageTypeUnknown WALStreamMessageType = iota
	WALStreamMessageTypeStart
	WALStreamMessageTypeData
	WALStreamMessageTypeHeartbeat
	WALStreamMessageTypeStop
	WALStreamMessageTypeStatus
)

// String returns string representation of message type
func (mt WALStreamMessageType) String() string {
	switch mt {
	case WALStreamMessageTypeStart:
		return "START"
	case WALStreamMessageTypeData:
		return "DATA"
	case WALStreamMessageTypeHeartbeat:
		return "HEARTBEAT"
	case WALStreamMessageTypeStop:
		return "STOP"
	case WALStreamMessageTypeStatus:
		return "STATUS"
	default:
		return "UNKNOWN"
	}
}

// ReplicationConfig holds configuration for replication
type ReplicationConfig struct {
	// Node configuration
	NodeID   NodeID
	Address  string
	DataDir  string

	// Replication mode
	Mode ReplicationMode

	// Primary node address (for replicas)
	PrimaryAddress string

	// Stream configuration
	StreamBufferSize int
	BatchSize        int
	FlushInterval    time.Duration

	// Heartbeat configuration
	HeartbeatInterval time.Duration
	HeartbeatTimeout  time.Duration

	// Lag thresholds
	MaxLagBytes int64
	MaxLagTime  time.Duration

	// Connection settings
	ConnectTimeout    time.Duration
	ReconnectInterval time.Duration
	MaxReconnectTries int
}

// ReplicationMode represents the replication mode
type ReplicationMode int

const (
	ReplicationModeNone ReplicationMode = iota
	ReplicationModePrimary
	ReplicationModeReplica
)

// String returns string representation of replication mode
func (rm ReplicationMode) String() string {
	switch rm {
	case ReplicationModePrimary:
		return "PRIMARY"
	case ReplicationModeReplica:
		return "REPLICA"
	default:
		return "NONE"
	}
}

// DefaultReplicationConfig returns default replication configuration
func DefaultReplicationConfig() *ReplicationConfig {
	return &ReplicationConfig{
		StreamBufferSize:  1024 * 1024, // 1MB
		BatchSize:         100,
		FlushInterval:     100 * time.Millisecond,
		HeartbeatInterval: 10 * time.Second,
		HeartbeatTimeout:  30 * time.Second,
		MaxLagBytes:       16 * 1024 * 1024, // 16MB
		MaxLagTime:        5 * time.Minute,
		ConnectTimeout:    30 * time.Second,
		ReconnectInterval: 5 * time.Second,
		MaxReconnectTries: 10,
	}
}

// ReplicationManager interface defines the contract for replication management
type ReplicationManager interface {
	// Primary node operations
	StartPrimary() error
	AddReplica(address string) error
	RemoveReplica(nodeID NodeID) error
	GetReplicas() []ReplicaInfo

	// Replica node operations
	StartReplica(primaryAddr string) error
	StopReplica() error

	// Common operations
	GetStatus() ReplicationStatus
	Close() error
}

// ReplicationStatus represents the overall replication status
type ReplicationStatus struct {
	Mode          ReplicationMode
	NodeID        NodeID
	Address       string
	State         string
	LastLSN       wal.LSN
	ReplicaCount  int
	ReplicaStates map[NodeID]ReplicaState
}

// WALStreamer interface defines the contract for WAL streaming
type WALStreamer interface {
	// Start streaming WAL from the specified LSN to the replica
	StartStream(replica ReplicaInfo, startLSN wal.LSN) error
	
	// Stop streaming to the specified replica
	StopStream(nodeID NodeID) error
	
	// Send WAL data to replicas
	StreamWALRecord(record *wal.LogRecord) error
	
	// Get streaming status for all replicas
	GetStreamStatus() map[NodeID]*StreamStatus
}

// WALReceiver interface defines the contract for WAL receiving
type WALReceiver interface {
	// Start receiving WAL stream from primary
	StartReceiving(primaryAddr string, startLSN wal.LSN) error
	
	// Stop receiving WAL stream
	StopReceiving() error
	
	// Get the last applied LSN
	GetLastAppliedLSN() wal.LSN
	
	// Get receiver status
	GetStatus() *ReceiverStatus
	
	// Close stops the receiver and cleans up resources
	Close() error
}

// StreamStatus represents the status of a WAL stream to a replica
type StreamStatus struct {
	NodeID        NodeID
	Address       string
	State         ReplicaState
	LastSentLSN   wal.LSN
	BytesSent     int64
	RecordsSent   int64
	StartTime     time.Time
	LastActivity  time.Time
}

// ReceiverStatus represents the status of WAL receiving
type ReceiverStatus struct {
	PrimaryAddress   string
	State            string
	LastReceivedLSN  wal.LSN
	LastAppliedLSN   wal.LSN
	BytesReceived    int64
	RecordsReceived  int64
	RecordsApplied   int64
	StartTime        time.Time
	LastActivity     time.Time
	ConnectionErrors int64
}