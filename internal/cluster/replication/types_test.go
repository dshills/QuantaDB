package replication

import (
	"testing"
	"time"

	"github.com/dshills/QuantaDB/internal/wal"
)

func TestReplicaState_String(t *testing.T) {
	tests := []struct {
		state    ReplicaState
		expected string
	}{
		{ReplicaStateConnecting, "CONNECTING"},
		{ReplicaStateStreaming, "STREAMING"},
		{ReplicaStateLagging, "LAGGING"},
		{ReplicaStateDisconnected, "DISCONNECTED"},
		{ReplicaStateFailed, "FAILED"},
		{ReplicaStateUnknown, "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if got := tt.state.String(); got != tt.expected {
				t.Errorf("ReplicaState.String() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestWALStreamMessageType_String(t *testing.T) {
	tests := []struct {
		msgType  WALStreamMessageType
		expected string
	}{
		{WALStreamMessageTypeStart, "START"},
		{WALStreamMessageTypeData, "DATA"},
		{WALStreamMessageTypeHeartbeat, "HEARTBEAT"},
		{WALStreamMessageTypeStop, "STOP"},
		{WALStreamMessageTypeStatus, "STATUS"},
		{WALStreamMessageTypeUnknown, "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if got := tt.msgType.String(); got != tt.expected {
				t.Errorf("WALStreamMessageType.String() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestReplicationMode_String(t *testing.T) {
	tests := []struct {
		mode     ReplicationMode
		expected string
	}{
		{ReplicationModePrimary, "PRIMARY"},
		{ReplicationModeReplica, "REPLICA"},
		{ReplicationModeNone, "NONE"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if got := tt.mode.String(); got != tt.expected {
				t.Errorf("ReplicationMode.String() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestDefaultReplicationConfig(t *testing.T) {
	config := DefaultReplicationConfig()

	// Test non-zero values
	if config.StreamBufferSize == 0 {
		t.Error("StreamBufferSize should not be zero")
	}
	if config.BatchSize == 0 {
		t.Error("BatchSize should not be zero")
	}
	if config.FlushInterval == 0 {
		t.Error("FlushInterval should not be zero")
	}
	if config.HeartbeatInterval == 0 {
		t.Error("HeartbeatInterval should not be zero")
	}
	if config.HeartbeatTimeout == 0 {
		t.Error("HeartbeatTimeout should not be zero")
	}

	// Test reasonable defaults
	expectedBufferSize := 1024 * 1024 // 1MB
	if config.StreamBufferSize != expectedBufferSize {
		t.Errorf("StreamBufferSize = %d, want %d", config.StreamBufferSize, expectedBufferSize)
	}

	if config.BatchSize != 100 {
		t.Errorf("BatchSize = %d, want %d", config.BatchSize, 100)
	}

	expectedInterval := 100 * time.Millisecond
	if config.FlushInterval != expectedInterval {
		t.Errorf("FlushInterval = %v, want %v", config.FlushInterval, expectedInterval)
	}
}

func TestReplicaInfo(t *testing.T) {
	nodeID := NodeID("test-replica")
	address := "localhost:6432"
	now := time.Now()

	replica := ReplicaInfo{
		NodeID:          nodeID,
		Address:         address,
		State:           ReplicaStateStreaming,
		LastReceivedLSN: wal.LSN(100),
		LastAppliedLSN:  wal.LSN(95),
		LagBytes:        1024,
		LagTime:         5 * time.Second,
		ConnectedAt:     now,
		LastHeartbeat:   now,
	}

	if replica.NodeID != nodeID {
		t.Errorf("NodeID = %v, want %v", replica.NodeID, nodeID)
	}
	if replica.Address != address {
		t.Errorf("Address = %v, want %v", replica.Address, address)
	}
	if replica.State != ReplicaStateStreaming {
		t.Errorf("State = %v, want %v", replica.State, ReplicaStateStreaming)
	}
	if replica.LastReceivedLSN != wal.LSN(100) {
		t.Errorf("LastReceivedLSN = %v, want %v", replica.LastReceivedLSN, wal.LSN(100))
	}
}

func TestWALStreamMessage(t *testing.T) {
	msg := WALStreamMessage{
		Type:      WALStreamMessageTypeData,
		StartLSN:  wal.LSN(100),
		EndLSN:    wal.LSN(150),
		Data:      []byte("test data"),
		Timestamp: time.Now(),
	}

	if msg.Type != WALStreamMessageTypeData {
		t.Errorf("Type = %v, want %v", msg.Type, WALStreamMessageTypeData)
	}
	if msg.StartLSN != wal.LSN(100) {
		t.Errorf("StartLSN = %v, want %v", msg.StartLSN, wal.LSN(100))
	}
	if msg.EndLSN != wal.LSN(150) {
		t.Errorf("EndLSN = %v, want %v", msg.EndLSN, wal.LSN(150))
	}
	if string(msg.Data) != "test data" {
		t.Errorf("Data = %v, want %v", string(msg.Data), "test data")
	}
}
