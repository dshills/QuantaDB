package replication

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dshills/QuantaDB/internal/log"
	"github.com/dshills/QuantaDB/internal/wal"
)

// WALReceiverImpl implements the WALReceiver interface
type WALReceiverImpl struct {
	config *ReplicationConfig
	walMgr *wal.Manager
	logger log.Logger

	// Connection state
	mu             sync.RWMutex
	conn           net.Conn
	primaryAddr    string
	isReceiving    bool
	lastAppliedLSN int64 // Use atomic operations

	// Statistics
	stats *ReceiverStatus

	// Control channels
	ctx       context.Context
	cancel    context.CancelFunc
	closeCh   chan struct{}
	closeOnce sync.Once
}

// NewWALReceiver creates a new WAL receiver
func NewWALReceiver(config *ReplicationConfig, walMgr *wal.Manager, logger log.Logger) *WALReceiverImpl {
	ctx, cancel := context.WithCancel(context.Background())
	
	return &WALReceiverImpl{
		config: config,
		walMgr: walMgr,
		logger: logger,
		stats: &ReceiverStatus{
			StartTime: time.Now(),
		},
		ctx:     ctx,
		cancel:  cancel,
		closeCh: make(chan struct{}),
	}
}

// StartReceiving starts receiving WAL stream from primary
func (r *WALReceiverImpl) StartReceiving(primaryAddr string, startLSN wal.LSN) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.isReceiving {
		return fmt.Errorf("already receiving from primary")
	}

	r.primaryAddr = primaryAddr
	r.stats.PrimaryAddress = primaryAddr
	r.stats.State = "CONNECTING"

	// Start receiving goroutine
	go r.receiveLoop(startLSN)

	r.isReceiving = true
	r.logger.Info("Started WAL receiver", "primary", primaryAddr, "startLSN", startLSN)

	return nil
}

// StopReceiving stops receiving WAL stream
func (r *WALReceiverImpl) StopReceiving() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.isReceiving {
		return nil
	}

	r.isReceiving = false
	r.stats.State = "STOPPED"

	// Close connection if active
	if r.conn != nil {
		r.conn.Close() //nolint:errcheck // Best effort cleanup
		r.conn = nil
	}

	r.logger.Info("Stopped WAL receiver")
	return nil
}

// GetLastAppliedLSN returns the last applied LSN
func (r *WALReceiverImpl) GetLastAppliedLSN() wal.LSN {
	return wal.LSN(atomic.LoadInt64(&r.lastAppliedLSN))
}

// GetStatus returns the receiver status
func (r *WALReceiverImpl) GetStatus() *ReceiverStatus {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Create a copy to avoid race conditions
	status := *r.stats
	status.LastAppliedLSN = r.GetLastAppliedLSN()
	return &status
}

// Close stops the receiver and cleans up resources
func (r *WALReceiverImpl) Close() error {
	r.closeOnce.Do(func() {
		close(r.closeCh)
		r.cancel()
		r.StopReceiving() //nolint:errcheck // Best effort cleanup
	})

	return nil
}

// receiveLoop is the main receiving loop
func (r *WALReceiverImpl) receiveLoop(startLSN wal.LSN) {
	defer func() {
		r.mu.Lock()
		r.isReceiving = false
		r.stats.State = "DISCONNECTED"
		if r.conn != nil {
			r.conn.Close() //nolint:errcheck // Best effort cleanup
			r.conn = nil
		}
		r.mu.Unlock()
	}()

	reconnectCount := 0
	for {
		select {
		case <-r.closeCh:
			return
		default:
		}

		// Connect to primary
		if err := r.connectToPrimary(); err != nil {
			r.logger.Error("Failed to connect to primary", "error", err)
			reconnectCount++
			
			if reconnectCount >= r.config.MaxReconnectTries {
				r.logger.Error("Max reconnection attempts reached")
				return
			}

			// Wait before retrying
			select {
			case <-time.After(r.config.ReconnectInterval):
			case <-r.closeCh:
				return
			}
			continue
		}

		reconnectCount = 0 // Reset on successful connection
		r.stats.State = "STREAMING"

		// Send start streaming request
		if err := r.requestWALStream(startLSN); err != nil {
			r.logger.Error("Failed to request WAL stream", "error", err)
			r.disconnectFromPrimary()
			continue
		}

		// Receive and apply WAL records
		if err := r.receiveWALStream(); err != nil {
			r.logger.Error("WAL stream error", "error", err)
			r.stats.ConnectionErrors++
			r.disconnectFromPrimary()
			continue
		}
	}
}

// connectToPrimary establishes connection to the primary
func (r *WALReceiverImpl) connectToPrimary() error {
	r.logger.Info("Connecting to primary", "address", r.primaryAddr)

	conn, err := net.DialTimeout("tcp", r.primaryAddr, r.config.ConnectTimeout)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	r.mu.Lock()
	r.conn = conn
	r.mu.Unlock()

	r.logger.Info("Connected to primary", "address", r.primaryAddr)
	return nil
}

// disconnectFromPrimary closes connection to primary
func (r *WALReceiverImpl) disconnectFromPrimary() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.conn != nil {
		r.conn.Close() //nolint:errcheck // Best effort cleanup
		r.conn = nil
	}
	
	r.stats.State = "DISCONNECTED"
}

// requestWALStream sends a request to start WAL streaming
func (r *WALReceiverImpl) requestWALStream(startLSN wal.LSN) error {
	r.logger.Info("Requesting WAL stream", "startLSN", startLSN)

	// Send handshake message
	handshake := &HandshakeMessage{
		NodeID:      NodeID(r.config.NodeID),
		StartLSN:    startLSN,
		DatabaseID:  "quantadb",
		ClusterName: "default",
	}

	protocolMsg := &ProtocolMessage{
		Type:    MsgTypeHandshake,
		Payload: handshake.Marshal(),
	}

	if err := WriteMessage(r.conn, protocolMsg); err != nil {
		return fmt.Errorf("failed to send handshake: %w", err)
	}

	// Wait for handshake acknowledgment
	if err := r.conn.SetReadDeadline(time.Now().Add(r.config.ConnectTimeout)); err != nil {
		return fmt.Errorf("failed to set read deadline: %w", err)
	}

	ackMsg, err := ReadMessage(r.conn)
	if err != nil {
		return fmt.Errorf("failed to read handshake ack: %w", err)
	}

	if ackMsg.Type != MsgTypeHandshakeAck {
		return fmt.Errorf("expected handshake ack, got %s", ackMsg.Type)
	}

	// Parse handshake acknowledgment
	ack := &HandshakeAckMessage{}
	if err := ack.Unmarshal(ackMsg.Payload); err != nil {
		return fmt.Errorf("failed to parse handshake ack: %w", err)
	}

	if !ack.Accepted {
		return fmt.Errorf("handshake rejected: %s", ack.ErrorMessage)
	}

	// Clear read deadline
	if err := r.conn.SetReadDeadline(time.Time{}); err != nil {
		return fmt.Errorf("failed to clear read deadline: %w", err)
	}

	r.logger.Info("Handshake successful", "currentLSN", ack.CurrentLSN)
	return nil
}

// receiveWALStream receives and applies WAL records
func (r *WALReceiverImpl) receiveWALStream() error {
	r.mu.RLock()
	conn := r.conn
	r.mu.RUnlock()

	if conn == nil {
		return fmt.Errorf("no connection to primary")
	}

	reader := bufio.NewReader(conn)
	
	// Set read timeout
	heartbeatDeadline := time.Now().Add(r.config.HeartbeatTimeout)

	for {
		select {
		case <-r.closeCh:
			return nil
		default:
		}

		// Set read deadline based on heartbeat timeout
		if err := conn.SetReadDeadline(heartbeatDeadline); err != nil {
			return fmt.Errorf("failed to set read deadline: %w", err)
		}

		// Read protocol message from stream
		protocolMsg, err := ReadMessage(reader)
		if err != nil {
			// Check if it's a timeout (expected for heartbeats)
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				r.logger.Warn("Read timeout - checking for heartbeat")
				continue
			}
			return fmt.Errorf("failed to read protocol message: %w", err)
		}

		// Update activity timestamp
		r.stats.LastActivity = time.Now()
		heartbeatDeadline = time.Now().Add(r.config.HeartbeatTimeout)

		// Process message based on type
		switch protocolMsg.Type {
		case MsgTypeWALData:
			if err := r.processWALData(protocolMsg); err != nil {
				return fmt.Errorf("failed to process WAL data: %w", err)
			}

		case MsgTypeHeartbeat:
			if err := r.processHeartbeat(protocolMsg); err != nil {
				r.logger.Warn("Failed to process heartbeat", "error", err)
			} else {
				r.logger.Debug("Received heartbeat from primary")
			}

		case MsgTypeStop:
			r.logger.Info("Received stop message from primary")
			return nil

		default:
			r.logger.Warn("Received unknown message type", "type", protocolMsg.Type)
		}
	}
}

// processWALData processes received WAL data
func (r *WALReceiverImpl) processWALData(protocolMsg *ProtocolMessage) error {
	// Parse WAL data message
	walData := &WALDataMessage{}
	if err := walData.Unmarshal(protocolMsg.Payload); err != nil {
		return fmt.Errorf("failed to unmarshal WAL data: %w", err)
	}

	r.logger.Debug("Processing WAL data", 
		"startLSN", walData.StartLSN, 
		"endLSN", walData.EndLSN,
		"recordCount", len(walData.Records),
		"dataSize", len(protocolMsg.Payload))

	// Apply each WAL record
	for _, record := range walData.Records {
		if err := r.applyWALRecord(record); err != nil {
			return fmt.Errorf("failed to apply WAL record %d: %w", record.LSN, err)
		}
	}

	// Update statistics
	r.stats.LastReceivedLSN = walData.EndLSN
	r.stats.BytesReceived += int64(len(protocolMsg.Payload))
	r.stats.RecordsReceived += int64(len(walData.Records))
	r.stats.RecordsApplied += int64(len(walData.Records)) // Assume all applied successfully

	// Update last applied LSN
	atomic.StoreInt64(&r.lastAppliedLSN, int64(walData.EndLSN))

	return nil
}

// processHeartbeat processes received heartbeat message
func (r *WALReceiverImpl) processHeartbeat(protocolMsg *ProtocolMessage) error {
	// Parse heartbeat message
	heartbeat := &HeartbeatMessage{}
	if err := heartbeat.Unmarshal(protocolMsg.Payload); err != nil {
		return fmt.Errorf("failed to unmarshal heartbeat: %w", err)
	}

	// Update last received LSN from heartbeat
	if heartbeat.LastLSN > r.stats.LastReceivedLSN {
		r.stats.LastReceivedLSN = heartbeat.LastLSN
	}

	return nil
}

// applyWALRecord applies a single WAL record to the local storage
func (r *WALReceiverImpl) applyWALRecord(record *wal.LogRecord) error {
	// TODO: Integrate with actual WAL manager and storage engine
	// This would involve:
	// 1. Appending the record to the local WAL
	// 2. Applying the record to the storage engine
	// 3. Updating any indexes
	// 4. Handling transaction state changes

	// For now, just append to WAL manager
	if err := r.walMgr.AppendRecord(record); err != nil {
		return fmt.Errorf("failed to append WAL record: %w", err)
	}

	return nil
}

// sendStatusUpdate sends status update to primary (if needed)
func (r *WALReceiverImpl) sendStatusUpdate() error {
	r.mu.RLock()
	conn := r.conn
	r.mu.RUnlock()

	if conn == nil {
		return fmt.Errorf("no connection to primary")
	}

	// TODO: Implement status message protocol
	// Send information about:
	// - Last applied LSN
	// - Current lag
	// - Any errors

	r.logger.Debug("Sent status update to primary")
	return nil
}