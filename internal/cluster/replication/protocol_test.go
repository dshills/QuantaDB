package replication

import (
	"bytes"
	"testing"
	"time"

	"github.com/dshills/QuantaDB/internal/wal"
)

func TestProtocolMessageType_String(t *testing.T) {
	tests := []struct {
		msgType  ProtocolMessageType
		expected string
	}{
		{MsgTypeHandshake, "HANDSHAKE"},
		{MsgTypeHandshakeAck, "HANDSHAKE_ACK"},
		{MsgTypeStartStreaming, "START_STREAMING"},
		{MsgTypeWALData, "WAL_DATA"},
		{MsgTypeHeartbeat, "HEARTBEAT"},
		{MsgTypeStatus, "STATUS"},
		{MsgTypeStop, "STOP"},
		{MsgTypeError, "ERROR"},
		{ProtocolMessageType(99), "UNKNOWN(99)"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if got := tt.msgType.String(); got != tt.expected {
				t.Errorf("ProtocolMessageType.String() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestHandshakeMessage_MarshalUnmarshal(t *testing.T) {
	original := &HandshakeMessage{
		NodeID:      "test-replica-1",
		StartLSN:    wal.LSN(12345),
		DatabaseID:  "quantadb",
		ClusterName: "test-cluster",
	}

	// Marshal
	data := original.Marshal()
	if len(data) == 0 {
		t.Fatal("Marshal returned empty data")
	}

	// Unmarshal
	restored := &HandshakeMessage{}
	err := restored.Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	// Compare
	if restored.NodeID != original.NodeID {
		t.Errorf("NodeID = %v, want %v", restored.NodeID, original.NodeID)
	}
	if restored.StartLSN != original.StartLSN {
		t.Errorf("StartLSN = %v, want %v", restored.StartLSN, original.StartLSN)
	}
	if restored.DatabaseID != original.DatabaseID {
		t.Errorf("DatabaseID = %v, want %v", restored.DatabaseID, original.DatabaseID)
	}
	if restored.ClusterName != original.ClusterName {
		t.Errorf("ClusterName = %v, want %v", restored.ClusterName, original.ClusterName)
	}
}

func TestHandshakeMessage_UnmarshalErrors(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"too short", []byte{1, 2, 3}},
		{"missing NodeID length", make([]byte, 12)},
		{"invalid NodeID length", func() []byte {
			buf := make([]byte, 16)
			// Set a large NodeID length that exceeds buffer
			buf[8] = 0xFF
			buf[9] = 0xFF
			buf[10] = 0xFF
			buf[11] = 0xFF
			return buf
		}()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &HandshakeMessage{}
			err := msg.Unmarshal(tt.data)
			if err == nil {
				t.Error("Expected error but got none")
			}
		})
	}
}

func TestHandshakeAckMessage_MarshalUnmarshal(t *testing.T) {
	timestamp := time.Now()
	original := &HandshakeAckMessage{
		Accepted:        true,
		CurrentLSN:      wal.LSN(54321),
		ServerTimeStamp: timestamp,
		ErrorMessage:    "test error",
	}

	// Marshal
	data := original.Marshal()
	if len(data) == 0 {
		t.Fatal("Marshal returned empty data")
	}

	// Unmarshal
	restored := &HandshakeAckMessage{}
	err := restored.Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	// Compare
	if restored.Accepted != original.Accepted {
		t.Errorf("Accepted = %v, want %v", restored.Accepted, original.Accepted)
	}
	if restored.CurrentLSN != original.CurrentLSN {
		t.Errorf("CurrentLSN = %v, want %v", restored.CurrentLSN, original.CurrentLSN)
	}
	if restored.ErrorMessage != original.ErrorMessage {
		t.Errorf("ErrorMessage = %v, want %v", restored.ErrorMessage, original.ErrorMessage)
	}

	// Timestamp comparison (within reasonable tolerance)
	if restored.ServerTimeStamp.Sub(original.ServerTimeStamp).Abs() > time.Millisecond {
		t.Errorf("ServerTimeStamp = %v, want %v", restored.ServerTimeStamp, original.ServerTimeStamp)
	}
}

func TestWALDataMessage_MarshalUnmarshal(t *testing.T) {
	timestamp := time.Now()
	records := []*wal.LogRecord{
		{
			LSN:       wal.LSN(100),
			Type:      wal.RecordTypeInsert,
			TxnID:     1,
			PrevLSN:   wal.LSN(99),
			Timestamp: timestamp,
			Data:      []byte("test data 1"),
		},
		{
			LSN:       wal.LSN(101),
			Type:      wal.RecordTypeCommitTxn,
			TxnID:     1,
			PrevLSN:   wal.LSN(100),
			Timestamp: timestamp,
			Data:      []byte("test data 2"),
		},
	}

	original := &WALDataMessage{
		StartLSN:  wal.LSN(100),
		EndLSN:    wal.LSN(101),
		Records:   records,
		Timestamp: timestamp,
	}

	// Marshal
	data := original.Marshal()
	if len(data) == 0 {
		t.Fatal("Marshal returned empty data")
	}

	// Unmarshal
	restored := &WALDataMessage{}
	err := restored.Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	// Compare
	if restored.StartLSN != original.StartLSN {
		t.Errorf("StartLSN = %v, want %v", restored.StartLSN, original.StartLSN)
	}
	if restored.EndLSN != original.EndLSN {
		t.Errorf("EndLSN = %v, want %v", restored.EndLSN, original.EndLSN)
	}
	if len(restored.Records) != len(original.Records) {
		t.Errorf("Records length = %v, want %v", len(restored.Records), len(original.Records))
	}

	// Compare records
	for i, originalRec := range original.Records {
		restoredRec := restored.Records[i]
		if restoredRec.LSN != originalRec.LSN {
			t.Errorf("Record %d LSN = %v, want %v", i, restoredRec.LSN, originalRec.LSN)
		}
		if restoredRec.Type != originalRec.Type {
			t.Errorf("Record %d Type = %v, want %v", i, restoredRec.Type, originalRec.Type)
		}
		if restoredRec.TxnID != originalRec.TxnID {
			t.Errorf("Record %d TxnID = %v, want %v", i, restoredRec.TxnID, originalRec.TxnID)
		}
		if string(restoredRec.Data) != string(originalRec.Data) {
			t.Errorf("Record %d Data = %v, want %v", i, string(restoredRec.Data), string(originalRec.Data))
		}
	}
}

func TestHeartbeatMessage_MarshalUnmarshal(t *testing.T) {
	timestamp := time.Now()
	original := &HeartbeatMessage{
		Timestamp: timestamp,
		LastLSN:   wal.LSN(9999),
	}

	// Marshal
	data := original.Marshal()
	if len(data) != 16 {
		t.Errorf("Marshal returned %d bytes, want 16", len(data))
	}

	// Unmarshal
	restored := &HeartbeatMessage{}
	err := restored.Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	// Compare
	if restored.LastLSN != original.LastLSN {
		t.Errorf("LastLSN = %v, want %v", restored.LastLSN, original.LastLSN)
	}

	// Timestamp comparison (within reasonable tolerance)
	if restored.Timestamp.Sub(original.Timestamp).Abs() > time.Millisecond {
		t.Errorf("Timestamp = %v, want %v", restored.Timestamp, original.Timestamp)
	}
}

func TestWriteReadMessage(t *testing.T) {
	tests := []struct {
		name string
		msg  *ProtocolMessage
	}{
		{
			name: "handshake message",
			msg: &ProtocolMessage{
				Type: MsgTypeHandshake,
				Payload: (&HandshakeMessage{
					NodeID:      "test-node",
					StartLSN:    wal.LSN(123),
					DatabaseID:  "test-db",
					ClusterName: "test-cluster",
				}).Marshal(),
			},
		},
		{
			name: "heartbeat message",
			msg: &ProtocolMessage{
				Type: MsgTypeHeartbeat,
				Payload: (&HeartbeatMessage{
					Timestamp: time.Now(),
					LastLSN:   wal.LSN(456),
				}).Marshal(),
			},
		},
		{
			name: "empty payload",
			msg: &ProtocolMessage{
				Type:    MsgTypeStop,
				Payload: nil,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Write message
			buf := &bytes.Buffer{}
			err := WriteMessage(buf, tt.msg)
			if err != nil {
				t.Fatalf("WriteMessage failed: %v", err)
			}

			// Read message
			restored, err := ReadMessage(buf)
			if err != nil {
				t.Fatalf("ReadMessage failed: %v", err)
			}

			// Compare
			if restored.Type != tt.msg.Type {
				t.Errorf("Type = %v, want %v", restored.Type, tt.msg.Type)
			}
			if !bytes.Equal(restored.Payload, tt.msg.Payload) {
				t.Errorf("Payload = %v, want %v", restored.Payload, tt.msg.Payload)
			}
		})
	}
}

func TestReadMessage_Errors(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"empty data", []byte{}},
		{"short header", []byte{1, 2, 3}},
		{"wrong version", []byte{99, 1, 0, 0, 0, 0}},
		{"payload too large", func() []byte {
			header := make([]byte, 6)
			header[0] = ProtocolVersion
			header[1] = byte(MsgTypeHeartbeat)
			// Set payload length to max + 1
			header[2] = 0xFF
			header[3] = 0xFF
			header[4] = 0xFF
			header[5] = 0xFF
			return header
		}()},
		{"incomplete payload", []byte{ProtocolVersion, byte(MsgTypeHeartbeat), 0, 0, 0, 10}}, // Claims 10 bytes but provides none
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewReader(tt.data)
			_, err := ReadMessage(buf)
			if err == nil {
				t.Error("Expected error but got none")
			}
		})
	}
}

func TestMessageSizeLimit(t *testing.T) {
	// Create a message that exceeds the size limit
	largePayload := make([]byte, MaxMessageSize+1)
	msg := &ProtocolMessage{
		Type:    MsgTypeWALData,
		Payload: largePayload,
	}

	buf := &bytes.Buffer{}
	err := WriteMessage(buf, msg)
	if err == nil {
		t.Error("Expected error for oversized message but got none")
	}
}
