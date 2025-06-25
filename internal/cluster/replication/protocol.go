package replication

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/dshills/QuantaDB/internal/wal"
)

// Protocol constants
const (
	ProtocolVersion = 1
	
	// Message header size: version(1) + type(1) + length(4) = 6 bytes
	MessageHeaderSize = 6
	
	// Maximum message size (16MB)
	MaxMessageSize = 16 * 1024 * 1024
)

// ProtocolMessageType represents different message types in the replication protocol
type ProtocolMessageType byte

const (
	MsgTypeHandshake ProtocolMessageType = iota + 1
	MsgTypeHandshakeAck
	MsgTypeStartStreaming
	MsgTypeWALData
	MsgTypeHeartbeat
	MsgTypeStatus
	MsgTypeStop
	MsgTypeError
)

// String returns string representation of message type
func (mt ProtocolMessageType) String() string {
	switch mt {
	case MsgTypeHandshake:
		return "HANDSHAKE"
	case MsgTypeHandshakeAck:
		return "HANDSHAKE_ACK"
	case MsgTypeStartStreaming:
		return "START_STREAMING"
	case MsgTypeWALData:
		return "WAL_DATA"
	case MsgTypeHeartbeat:
		return "HEARTBEAT"
	case MsgTypeStatus:
		return "STATUS"
	case MsgTypeStop:
		return "STOP"
	case MsgTypeError:
		return "ERROR"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", mt)
	}
}

// ProtocolMessage represents a message in the replication protocol
type ProtocolMessage struct {
	Type    ProtocolMessageType
	Payload []byte
}

// HandshakeMessage represents the initial handshake from replica to primary
type HandshakeMessage struct {
	NodeID      NodeID
	StartLSN    wal.LSN
	DatabaseID  string
	ClusterName string
}

// Marshal serializes the handshake message
func (h *HandshakeMessage) Marshal() []byte {
	nodeIDBytes := []byte(h.NodeID)
	dbIDBytes := []byte(h.DatabaseID)
	clusterBytes := []byte(h.ClusterName)
	
	// Calculate total size
	size := 8 + 4 + len(nodeIDBytes) + 4 + len(dbIDBytes) + 4 + len(clusterBytes)
	buf := make([]byte, size)
	
	pos := 0
	// StartLSN
	binary.BigEndian.PutUint64(buf[pos:pos+8], uint64(h.StartLSN))
	pos += 8
	
	// NodeID with length prefix
	binary.BigEndian.PutUint32(buf[pos:pos+4], uint32(len(nodeIDBytes)))
	pos += 4
	copy(buf[pos:pos+len(nodeIDBytes)], nodeIDBytes)
	pos += len(nodeIDBytes)
	
	// DatabaseID with length prefix
	binary.BigEndian.PutUint32(buf[pos:pos+4], uint32(len(dbIDBytes)))
	pos += 4
	copy(buf[pos:pos+len(dbIDBytes)], dbIDBytes)
	pos += len(dbIDBytes)
	
	// ClusterName with length prefix
	binary.BigEndian.PutUint32(buf[pos:pos+4], uint32(len(clusterBytes)))
	pos += 4
	copy(buf[pos:pos+len(clusterBytes)], clusterBytes)
	
	return buf
}

// Unmarshal deserializes the handshake message
func (h *HandshakeMessage) Unmarshal(data []byte) error {
	if len(data) < 16 { // Minimum size
		return fmt.Errorf("handshake message too short: %d bytes", len(data))
	}
	
	pos := 0
	// StartLSN
	h.StartLSN = wal.LSN(binary.BigEndian.Uint64(data[pos : pos+8]))
	pos += 8
	
	// NodeID
	nodeIDLen := binary.BigEndian.Uint32(data[pos : pos+4])
	pos += 4
	if pos+int(nodeIDLen) > len(data) {
		return fmt.Errorf("invalid NodeID length")
	}
	h.NodeID = NodeID(data[pos : pos+int(nodeIDLen)])
	pos += int(nodeIDLen)
	
	// DatabaseID
	if pos+4 > len(data) {
		return fmt.Errorf("missing DatabaseID length")
	}
	dbIDLen := binary.BigEndian.Uint32(data[pos : pos+4])
	pos += 4
	if pos+int(dbIDLen) > len(data) {
		return fmt.Errorf("invalid DatabaseID length")
	}
	h.DatabaseID = string(data[pos : pos+int(dbIDLen)])
	pos += int(dbIDLen)
	
	// ClusterName
	if pos+4 > len(data) {
		return fmt.Errorf("missing ClusterName length")
	}
	clusterLen := binary.BigEndian.Uint32(data[pos : pos+4])
	pos += 4
	if pos+int(clusterLen) > len(data) {
		return fmt.Errorf("invalid ClusterName length")
	}
	h.ClusterName = string(data[pos : pos+int(clusterLen)])
	
	return nil
}

// HandshakeAckMessage represents the acknowledgment from primary to replica
type HandshakeAckMessage struct {
	Accepted        bool
	CurrentLSN      wal.LSN
	ServerTimeStamp time.Time
	ErrorMessage    string
}

// Marshal serializes the handshake ack message
func (h *HandshakeAckMessage) Marshal() []byte {
	errorBytes := []byte(h.ErrorMessage)
	size := 1 + 8 + 8 + 4 + len(errorBytes)
	buf := make([]byte, size)
	
	pos := 0
	// Accepted flag
	if h.Accepted {
		buf[pos] = 1
	}
	pos++
	
	// CurrentLSN
	binary.BigEndian.PutUint64(buf[pos:pos+8], uint64(h.CurrentLSN))
	pos += 8
	
	// Timestamp
	binary.BigEndian.PutUint64(buf[pos:pos+8], uint64(h.ServerTimeStamp.UnixNano()))
	pos += 8
	
	// Error message with length prefix
	binary.BigEndian.PutUint32(buf[pos:pos+4], uint32(len(errorBytes)))
	pos += 4
	copy(buf[pos:], errorBytes)
	
	return buf
}

// Unmarshal deserializes the handshake ack message
func (h *HandshakeAckMessage) Unmarshal(data []byte) error {
	if len(data) < 21 { // Minimum size
		return fmt.Errorf("handshake ack message too short: %d bytes", len(data))
	}
	
	pos := 0
	// Accepted flag
	h.Accepted = data[pos] == 1
	pos++
	
	// CurrentLSN
	h.CurrentLSN = wal.LSN(binary.BigEndian.Uint64(data[pos : pos+8]))
	pos += 8
	
	// Timestamp
	h.ServerTimeStamp = time.Unix(0, int64(binary.BigEndian.Uint64(data[pos:pos+8])))
	pos += 8
	
	// Error message
	errorLen := binary.BigEndian.Uint32(data[pos : pos+4])
	pos += 4
	if pos+int(errorLen) > len(data) {
		return fmt.Errorf("invalid error message length")
	}
	h.ErrorMessage = string(data[pos : pos+int(errorLen)])
	
	return nil
}

// WALDataMessage represents a batch of WAL records
type WALDataMessage struct {
	StartLSN  wal.LSN
	EndLSN    wal.LSN
	Records   []*wal.LogRecord
	Timestamp time.Time
}

// Marshal serializes the WAL data message
func (w *WALDataMessage) Marshal() []byte {
	// Calculate total size
	totalSize := 8 + 8 + 8 + 4 // StartLSN + EndLSN + Timestamp + RecordCount
	recordData := make([][]byte, len(w.Records))
	
	for i, record := range w.Records {
		recordData[i] = w.serializeRecord(record)
		totalSize += 4 + len(recordData[i]) // Length prefix + data
	}
	
	buf := make([]byte, totalSize)
	pos := 0
	
	// StartLSN
	binary.BigEndian.PutUint64(buf[pos:pos+8], uint64(w.StartLSN))
	pos += 8
	
	// EndLSN
	binary.BigEndian.PutUint64(buf[pos:pos+8], uint64(w.EndLSN))
	pos += 8
	
	// Timestamp
	binary.BigEndian.PutUint64(buf[pos:pos+8], uint64(w.Timestamp.UnixNano()))
	pos += 8
	
	// Record count
	binary.BigEndian.PutUint32(buf[pos:pos+4], uint32(len(w.Records)))
	pos += 4
	
	// Records with length prefixes
	for _, data := range recordData {
		binary.BigEndian.PutUint32(buf[pos:pos+4], uint32(len(data)))
		pos += 4
		copy(buf[pos:pos+len(data)], data)
		pos += len(data)
	}
	
	return buf
}

// Unmarshal deserializes the WAL data message
func (w *WALDataMessage) Unmarshal(data []byte) error {
	if len(data) < 28 { // Minimum size
		return fmt.Errorf("WAL data message too short: %d bytes", len(data))
	}
	
	pos := 0
	// StartLSN
	w.StartLSN = wal.LSN(binary.BigEndian.Uint64(data[pos : pos+8]))
	pos += 8
	
	// EndLSN
	w.EndLSN = wal.LSN(binary.BigEndian.Uint64(data[pos : pos+8]))
	pos += 8
	
	// Timestamp
	w.Timestamp = time.Unix(0, int64(binary.BigEndian.Uint64(data[pos:pos+8])))
	pos += 8
	
	// Record count
	recordCount := binary.BigEndian.Uint32(data[pos : pos+4])
	pos += 4
	
	// Records
	w.Records = make([]*wal.LogRecord, recordCount)
	for i := uint32(0); i < recordCount; i++ {
		if pos+4 > len(data) {
			return fmt.Errorf("missing record length at index %d", i)
		}
		recordLen := binary.BigEndian.Uint32(data[pos : pos+4])
		pos += 4
		
		if pos+int(recordLen) > len(data) {
			return fmt.Errorf("record data exceeds message at index %d", i)
		}
		
		record, err := w.deserializeRecord(data[pos : pos+int(recordLen)])
		if err != nil {
			return fmt.Errorf("failed to deserialize record %d: %w", i, err)
		}
		w.Records[i] = record
		pos += int(recordLen)
	}
	
	return nil
}

// serializeRecord serializes a WAL record for transmission
func (w *WALDataMessage) serializeRecord(record *wal.LogRecord) []byte {
	// Use the existing WAL record serialization
	// This would normally use the wal package's SerializeRecord function
	// For now, create a simple format
	
	dataLen := len(record.Data)
	size := 8 + 2 + 8 + 8 + 8 + 4 + dataLen // LSN + Type + TxnID + PrevLSN + Timestamp + DataLen + Data
	buf := make([]byte, size)
	
	pos := 0
	binary.BigEndian.PutUint64(buf[pos:pos+8], uint64(record.LSN))
	pos += 8
	binary.BigEndian.PutUint16(buf[pos:pos+2], uint16(record.Type))
	pos += 2
	binary.BigEndian.PutUint64(buf[pos:pos+8], record.TxnID)
	pos += 8
	binary.BigEndian.PutUint64(buf[pos:pos+8], uint64(record.PrevLSN))
	pos += 8
	binary.BigEndian.PutUint64(buf[pos:pos+8], uint64(record.Timestamp.UnixNano()))
	pos += 8
	binary.BigEndian.PutUint32(buf[pos:pos+4], uint32(dataLen))
	pos += 4
	copy(buf[pos:], record.Data)
	
	return buf
}

// deserializeRecord deserializes a WAL record from transmission
func (w *WALDataMessage) deserializeRecord(data []byte) (*wal.LogRecord, error) {
	if len(data) < 38 { // Minimum size without data
		return nil, fmt.Errorf("record data too short: %d bytes", len(data))
	}
	
	pos := 0
	lsn := wal.LSN(binary.BigEndian.Uint64(data[pos : pos+8]))
	pos += 8
	recordType := wal.RecordType(binary.BigEndian.Uint16(data[pos : pos+2]))
	pos += 2
	txnID := binary.BigEndian.Uint64(data[pos : pos+8])
	pos += 8
	prevLSN := wal.LSN(binary.BigEndian.Uint64(data[pos : pos+8]))
	pos += 8
	timestamp := time.Unix(0, int64(binary.BigEndian.Uint64(data[pos:pos+8])))
	pos += 8
	dataLen := binary.BigEndian.Uint32(data[pos : pos+4])
	pos += 4
	
	if pos+int(dataLen) > len(data) {
		return nil, fmt.Errorf("record data length exceeds available data")
	}
	
	recordData := make([]byte, dataLen)
	copy(recordData, data[pos:pos+int(dataLen)])
	
	return &wal.LogRecord{
		LSN:       lsn,
		Type:      recordType,
		TxnID:     txnID,
		PrevLSN:   prevLSN,
		Timestamp: timestamp,
		Data:      recordData,
	}, nil
}

// HeartbeatMessage represents a heartbeat message
type HeartbeatMessage struct {
	Timestamp time.Time
	LastLSN   wal.LSN
}

// Marshal serializes the heartbeat message
func (h *HeartbeatMessage) Marshal() []byte {
	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf[0:8], uint64(h.Timestamp.UnixNano()))
	binary.BigEndian.PutUint64(buf[8:16], uint64(h.LastLSN))
	return buf
}

// Unmarshal deserializes the heartbeat message
func (h *HeartbeatMessage) Unmarshal(data []byte) error {
	if len(data) < 16 {
		return fmt.Errorf("heartbeat message too short: %d bytes", len(data))
	}
	
	h.Timestamp = time.Unix(0, int64(binary.BigEndian.Uint64(data[0:8])))
	h.LastLSN = wal.LSN(binary.BigEndian.Uint64(data[8:16]))
	return nil
}

// WriteMessage writes a protocol message to the writer
func WriteMessage(w io.Writer, msg *ProtocolMessage) error {
	if len(msg.Payload) > MaxMessageSize {
		return fmt.Errorf("message too large: %d bytes", len(msg.Payload))
	}
	
	// Write header: version(1) + type(1) + length(4)
	header := make([]byte, MessageHeaderSize)
	header[0] = ProtocolVersion
	header[1] = byte(msg.Type)
	binary.BigEndian.PutUint32(header[2:6], uint32(len(msg.Payload)))
	
	if _, err := w.Write(header); err != nil {
		return fmt.Errorf("failed to write message header: %w", err)
	}
	
	// Write payload
	if len(msg.Payload) > 0 {
		if _, err := w.Write(msg.Payload); err != nil {
			return fmt.Errorf("failed to write message payload: %w", err)
		}
	}
	
	return nil
}

// ReadMessage reads a protocol message from the reader
func ReadMessage(r io.Reader) (*ProtocolMessage, error) {
	// Read header
	header := make([]byte, MessageHeaderSize)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, fmt.Errorf("failed to read message header: %w", err)
	}
	
	// Parse header
	version := header[0]
	if version != ProtocolVersion {
		return nil, fmt.Errorf("unsupported protocol version: %d", version)
	}
	
	msgType := ProtocolMessageType(header[1])
	payloadLen := binary.BigEndian.Uint32(header[2:6])
	
	if payloadLen > MaxMessageSize {
		return nil, fmt.Errorf("message too large: %d bytes", payloadLen)
	}
	
	// Read payload
	var payload []byte
	if payloadLen > 0 {
		payload = make([]byte, payloadLen)
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil, fmt.Errorf("failed to read message payload: %w", err)
		}
	}
	
	return &ProtocolMessage{
		Type:    msgType,
		Payload: payload,
	}, nil
}