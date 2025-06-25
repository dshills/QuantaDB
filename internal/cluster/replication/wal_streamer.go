package replication

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/dshills/QuantaDB/internal/log"
	"github.com/dshills/QuantaDB/internal/wal"
)

// WALStreamerImpl implements the WALStreamer interface
type WALStreamerImpl struct {
	config   *ReplicationConfig
	walMgr   *wal.Manager
	logger   log.Logger

	// Streaming state
	mu       sync.RWMutex
	replicas map[NodeID]*replicaStream
	listener net.Listener

	// Control channels
	ctx       context.Context
	cancel    context.CancelFunc
	closeCh   chan struct{}
	closeOnce sync.Once
}

// replicaStream represents an active WAL stream to a replica
type replicaStream struct {
	info     ReplicaInfo
	conn     net.Conn
	sendCh   chan *wal.LogRecord
	statusCh chan StreamStatus
	stopCh   chan struct{}
	started  time.Time

	// Stream statistics
	mu          sync.RWMutex
	lastSentLSN wal.LSN
	bytesSent   int64
	recordsSent int64
}

// NewWALStreamer creates a new WAL streamer
func NewWALStreamer(config *ReplicationConfig, walMgr *wal.Manager, logger log.Logger) *WALStreamerImpl {
	ctx, cancel := context.WithCancel(context.Background())
	
	return &WALStreamerImpl{
		config:   config,
		walMgr:   walMgr,
		logger:   logger,
		replicas: make(map[NodeID]*replicaStream),
		ctx:      ctx,
		cancel:   cancel,
		closeCh:  make(chan struct{}),
	}
}

// Start starts the WAL streamer service
func (s *WALStreamerImpl) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Listen for replica connections
	listener, err := net.Listen("tcp", s.config.Address)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.config.Address, err)
	}
	s.listener = listener

	s.logger.Info("WAL streamer started", "address", s.config.Address)

	// Start accepting connections
	go s.acceptConnections()

	// Start heartbeat routine
	go s.heartbeatRoutine()

	return nil
}

// StartStream starts streaming WAL to a replica
func (s *WALStreamerImpl) StartStream(replica ReplicaInfo, startLSN wal.LSN) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if already streaming to this replica
	if _, exists := s.replicas[replica.NodeID]; exists {
		return fmt.Errorf("already streaming to replica %s", replica.NodeID)
	}

	// Connect to replica
	conn, err := net.DialTimeout("tcp", replica.Address, s.config.ConnectTimeout)
	if err != nil {
		return fmt.Errorf("failed to connect to replica %s: %w", replica.Address, err)
	}

	// Create replica stream
	stream := &replicaStream{
		info:     replica,
		conn:     conn,
		sendCh:   make(chan *wal.LogRecord, s.config.BatchSize),
		statusCh: make(chan StreamStatus, 1),
		stopCh:   make(chan struct{}),
		started:  time.Now(),
	}

	s.replicas[replica.NodeID] = stream

	// Start streaming goroutine
	go s.streamToReplica(stream, startLSN)

	s.logger.Info("Started WAL stream", "replica", replica.NodeID, "address", replica.Address, "startLSN", startLSN)

	return nil
}

// StopStream stops streaming to a replica
func (s *WALStreamerImpl) StopStream(nodeID NodeID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	stream, exists := s.replicas[nodeID]
	if !exists {
		return fmt.Errorf("no active stream to replica %s", nodeID)
	}

	// Signal stop
	close(stream.stopCh)

	// Close connection
	if err := stream.conn.Close(); err != nil {
		s.logger.Warn("Error closing connection to replica", "replica", nodeID, "error", err)
	}

	// Remove from active replicas
	delete(s.replicas, nodeID)

	s.logger.Info("Stopped WAL stream", "replica", nodeID)

	return nil
}

// StreamWALRecord streams a WAL record to all active replicas
func (s *WALStreamerImpl) StreamWALRecord(record *wal.LogRecord) error {
	s.mu.RLock()
	replicas := make([]*replicaStream, 0, len(s.replicas))
	for _, stream := range s.replicas {
		replicas = append(replicas, stream)
	}
	s.mu.RUnlock()

	// Send to all replicas
	for _, stream := range replicas {
		select {
		case stream.sendCh <- record:
			// Record queued for sending
		case <-stream.stopCh:
			// Stream is stopped
		default:
			// Channel is full - replica might be lagging
			s.logger.Warn("WAL stream buffer full", "replica", stream.info.NodeID)
			
			// Try to send with timeout
			select {
			case stream.sendCh <- record:
			case <-time.After(100 * time.Millisecond):
				s.logger.Error("Failed to queue WAL record - replica lagging", "replica", stream.info.NodeID)
			case <-stream.stopCh:
				// Stream stopped while waiting
			}
		}
	}

	return nil
}

// GetStreamStatus returns status for all active streams
func (s *WALStreamerImpl) GetStreamStatus() map[NodeID]*StreamStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()

	status := make(map[NodeID]*StreamStatus)
	for nodeID, stream := range s.replicas {
		stream.mu.RLock()
		status[nodeID] = &StreamStatus{
			NodeID:       nodeID,
			Address:      stream.info.Address,
			State:        stream.info.State,
			LastSentLSN:  stream.lastSentLSN,
			BytesSent:    stream.bytesSent,
			RecordsSent:  stream.recordsSent,
			StartTime:    stream.started,
			LastActivity: time.Now(), // Approximate
		}
		stream.mu.RUnlock()
	}

	return status
}

// Close stops the WAL streamer and closes all connections
func (s *WALStreamerImpl) Close() error {
	s.closeOnce.Do(func() {
		close(s.closeCh)
		s.cancel()

		// Stop all streams
		s.mu.Lock()
		for nodeID := range s.replicas {
			s.StopStream(nodeID) //nolint:errcheck // Best effort cleanup
		}
		s.mu.Unlock()

		// Close listener
		if s.listener != nil {
			s.listener.Close() //nolint:errcheck // Best effort cleanup
		}
	})

	return nil
}

// acceptConnections accepts incoming replica connections
func (s *WALStreamerImpl) acceptConnections() {
	for {
		select {
		case <-s.closeCh:
			return
		default:
		}

		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.closeCh:
				return
			default:
				s.logger.Error("Failed to accept connection", "error", err)
				time.Sleep(100 * time.Millisecond)
				continue
			}
		}

		// Handle connection in goroutine
		go s.handleReplicaConnection(conn)
	}
}

// handleReplicaConnection handles a new replica connection
func (s *WALStreamerImpl) handleReplicaConnection(conn net.Conn) {
	defer conn.Close()

	s.logger.Info("New replica connection", "remoteAddr", conn.RemoteAddr())

	// Perform handshake
	replica, err := s.performHandshake(conn)
	if err != nil {
		s.logger.Error("Handshake failed", "remoteAddr", conn.RemoteAddr(), "error", err)
		return
	}

	s.logger.Info("Handshake successful", "replica", replica.NodeID, "startLSN", replica.LastReceivedLSN)

	// Create and start replica stream
	stream := &replicaStream{
		info:     *replica,
		conn:     conn,
		sendCh:   make(chan *wal.LogRecord, s.config.BatchSize),
		statusCh: make(chan StreamStatus, 1),
		stopCh:   make(chan struct{}),
		started:  time.Now(),
	}

	// Add to active replicas
	s.mu.Lock()
	s.replicas[replica.NodeID] = stream
	s.mu.Unlock()

	// Start streaming to this replica
	s.streamToReplica(stream, replica.LastReceivedLSN)
}

// streamToReplica streams WAL records to a specific replica
func (s *WALStreamerImpl) streamToReplica(stream *replicaStream, startLSN wal.LSN) {
	defer func() {
		// Cleanup on exit
		s.mu.Lock()
		delete(s.replicas, stream.info.NodeID)
		s.mu.Unlock()
	}()

	// Send initial records from startLSN if needed
	if startLSN != wal.InvalidLSN {
		if err := s.sendHistoricalRecords(stream, startLSN); err != nil {
			s.logger.Error("Failed to send historical records", "replica", stream.info.NodeID, "error", err)
			return
		}
	}

	// Stream new records
	ticker := time.NewTicker(s.config.FlushInterval)
	defer ticker.Stop()

	batch := make([]*wal.LogRecord, 0, s.config.BatchSize)

	for {
		select {
		case record := <-stream.sendCh:
			batch = append(batch, record)
			
			// Send batch if full or after flush interval
			if len(batch) >= s.config.BatchSize {
				if err := s.sendBatch(stream, batch); err != nil {
					s.logger.Error("Failed to send batch", "replica", stream.info.NodeID, "error", err)
					return
				}
				batch = batch[:0] // Reset batch
			}

		case <-ticker.C:
			// Send partial batch on timer
			if len(batch) > 0 {
				if err := s.sendBatch(stream, batch); err != nil {
					s.logger.Error("Failed to send batch", "replica", stream.info.NodeID, "error", err)
					return
				}
				batch = batch[:0] // Reset batch
			}

		case <-stream.stopCh:
			// Send final batch
			if len(batch) > 0 {
				s.sendBatch(stream, batch) //nolint:errcheck // Best effort on shutdown
			}
			return

		case <-s.closeCh:
			return
		}
	}
}

// sendHistoricalRecords sends WAL records from disk starting at startLSN
func (s *WALStreamerImpl) sendHistoricalRecords(stream *replicaStream, startLSN wal.LSN) error {
	// TODO: Implement reading historical WAL records from disk
	// This would involve:
	// 1. Reading WAL segments from disk starting at startLSN
	// 2. Deserializing records
	// 3. Sending them to the replica
	
	s.logger.Info("Sending historical records", "replica", stream.info.NodeID, "startLSN", startLSN)
	return nil
}

// sendBatch sends a batch of WAL records to a replica
func (s *WALStreamerImpl) sendBatch(stream *replicaStream, batch []*wal.LogRecord) error {
	if len(batch) == 0 {
		return nil
	}

	// Create WAL data message
	walData := &WALDataMessage{
		StartLSN:  batch[0].LSN,
		EndLSN:    batch[len(batch)-1].LSN,
		Records:   batch,
		Timestamp: time.Now(),
	}

	// Create protocol message
	protocolMsg := &ProtocolMessage{
		Type:    MsgTypeWALData,
		Payload: walData.Marshal(),
	}

	// Send message over network connection
	if err := WriteMessage(stream.conn, protocolMsg); err != nil {
		return fmt.Errorf("failed to send WAL batch: %w", err)
	}

	// Update statistics
	stream.mu.Lock()
	stream.lastSentLSN = batch[len(batch)-1].LSN
	stream.recordsSent += int64(len(batch))
	for _, record := range batch {
		stream.bytesSent += int64(record.Size())
	}
	stream.mu.Unlock()

	s.logger.Debug("Sent WAL batch", 
		"replica", stream.info.NodeID, 
		"records", len(batch),
		"startLSN", walData.StartLSN,
		"endLSN", walData.EndLSN,
		"bytes", len(protocolMsg.Payload))

	return nil
}

// heartbeatRoutine sends periodic heartbeats to replicas
func (s *WALStreamerImpl) heartbeatRoutine() {
	ticker := time.NewTicker(s.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.sendHeartbeats()
		case <-s.closeCh:
			return
		}
	}
}

// sendHeartbeats sends heartbeat messages to all replicas
func (s *WALStreamerImpl) sendHeartbeats() {
	s.mu.RLock()
	replicas := make([]*replicaStream, 0, len(s.replicas))
	for _, stream := range s.replicas {
		replicas = append(replicas, stream)
	}
	s.mu.RUnlock()

	for _, stream := range replicas {
		// Send heartbeat message
		heartbeat := &HeartbeatMessage{
			Timestamp: time.Now(),
			LastLSN:   s.walMgr.GetCurrentLSN(),
		}

		protocolMsg := &ProtocolMessage{
			Type:    MsgTypeHeartbeat,
			Payload: heartbeat.Marshal(),
		}

		if err := WriteMessage(stream.conn, protocolMsg); err != nil {
			s.logger.Warn("Failed to send heartbeat", "replica", stream.info.NodeID, "error", err)
		} else {
			s.logger.Debug("Sent heartbeat", "replica", stream.info.NodeID)
		}
	}
}

// performHandshake performs the handshake with a connecting replica
func (s *WALStreamerImpl) performHandshake(conn net.Conn) (*ReplicaInfo, error) {
	// Set handshake timeout
	if err := conn.SetReadDeadline(time.Now().Add(s.config.ConnectTimeout)); err != nil {
		return nil, fmt.Errorf("failed to set read deadline: %w", err)
	}

	// Read handshake message
	protocolMsg, err := ReadMessage(conn)
	if err != nil {
		return nil, fmt.Errorf("failed to read handshake message: %w", err)
	}

	if protocolMsg.Type != MsgTypeHandshake {
		return nil, fmt.Errorf("expected handshake message, got %s", protocolMsg.Type)
	}

	// Parse handshake message
	handshake := &HandshakeMessage{}
	if err := handshake.Unmarshal(protocolMsg.Payload); err != nil {
		return nil, fmt.Errorf("failed to parse handshake: %w", err)
	}

	// Validate handshake (simplified validation)
	var ackMsg *HandshakeAckMessage
	if handshake.DatabaseID == "quantadb" { // Simple validation
		ackMsg = &HandshakeAckMessage{
			Accepted:        true,
			CurrentLSN:      s.walMgr.GetCurrentLSN(),
			ServerTimeStamp: time.Now(),
			ErrorMessage:    "",
		}
	} else {
		ackMsg = &HandshakeAckMessage{
			Accepted:        false,
			CurrentLSN:      0,
			ServerTimeStamp: time.Now(),
			ErrorMessage:    "invalid database ID",
		}
	}

	// Send handshake acknowledgment
	ackProtocolMsg := &ProtocolMessage{
		Type:    MsgTypeHandshakeAck,
		Payload: ackMsg.Marshal(),
	}

	if err := WriteMessage(conn, ackProtocolMsg); err != nil {
		return nil, fmt.Errorf("failed to send handshake ack: %w", err)
	}

	if !ackMsg.Accepted {
		return nil, fmt.Errorf("handshake rejected: %s", ackMsg.ErrorMessage)
	}

	// Clear read deadline
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		return nil, fmt.Errorf("failed to clear read deadline: %w", err)
	}

	// Create replica info
	replica := &ReplicaInfo{
		NodeID:          handshake.NodeID,
		Address:         conn.RemoteAddr().String(),
		State:           ReplicaStateConnecting,
		LastReceivedLSN: handshake.StartLSN,
		LastAppliedLSN:  handshake.StartLSN,
		ConnectedAt:     time.Now(),
		LastHeartbeat:   time.Now(),
	}

	return replica, nil
}