package raft

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/dshills/QuantaDB/internal/log"
)

// TCPTransport implements RaftTransport using TCP connections
type TCPTransport struct {
	config   *RaftConfig
	logger   log.Logger
	raftNode RaftNode
	
	// Network state
	listener net.Listener
	address  string
	
	// Connection management
	mu          sync.RWMutex
	connections map[NodeID]net.Conn
	
	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewTCPTransport creates a new TCP transport
func NewTCPTransport(config *RaftConfig, logger log.Logger) *TCPTransport {
	ctx, cancel := context.WithCancel(context.Background())
	
	return &TCPTransport{
		config:      config,
		logger:      logger,
		connections: make(map[NodeID]net.Conn),
		ctx:         ctx,
		cancel:      cancel,
	}
}

// Start starts the transport on the given address
func (t *TCPTransport) Start(address string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	
	if address == "" {
		address = "127.0.0.1:0" // Let OS assign port
	}
	
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", address, err)
	}
	
	t.listener = listener
	t.address = listener.Addr().String()
	
	t.logger.Info("Raft transport started", "address", t.address)
	
	// Start accepting connections
	t.wg.Add(1)
	go t.acceptConnections()
	
	return nil
}

// Stop stops the transport
func (t *TCPTransport) Stop() error {
	t.cancel()
	
	t.mu.Lock()
	if t.listener != nil {
		t.listener.Close()
	}
	
	// Close all connections
	for nodeID, conn := range t.connections {
		conn.Close()
		delete(t.connections, nodeID)
	}
	t.mu.Unlock()
	
	t.wg.Wait()
	
	t.logger.Info("Raft transport stopped")
	return nil
}

// GetAddress returns the actual listening address
func (t *TCPTransport) GetAddress() string {
	return t.address
}

// SetRaftNode sets the Raft node that will handle incoming RPCs
func (t *TCPTransport) SetRaftNode(node RaftNode) {
	t.raftNode = node
}

// SendRequestVote sends a RequestVote RPC to the specified node
func (t *TCPTransport) SendRequestVote(nodeID NodeID, args *RequestVoteArgs) (*RequestVoteReply, error) {
	conn, err := t.getConnection(nodeID)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection to %s: %w", nodeID, err)
	}
	
	// Send request
	req := &RPCRequest{
		Type: RPCTypeRequestVote,
		Data: args,
	}
	
	if err := t.sendRPC(conn, req); err != nil {
		t.closeConnection(nodeID)
		return nil, err
	}
	
	// Receive response
	resp, err := t.receiveRPCResponse(conn)
	if err != nil {
		t.closeConnection(nodeID)
		return nil, err
	}
	
	reply, ok := resp.Data.(*RequestVoteReply)
	if !ok {
		return nil, fmt.Errorf("invalid response type for RequestVote")
	}
	
	return reply, nil
}

// SendAppendEntries sends an AppendEntries RPC to the specified node
func (t *TCPTransport) SendAppendEntries(nodeID NodeID, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	conn, err := t.getConnection(nodeID)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection to %s: %w", nodeID, err)
	}
	
	// Send request
	req := &RPCRequest{
		Type: RPCTypeAppendEntries,
		Data: args,
	}
	
	if err := t.sendRPC(conn, req); err != nil {
		t.closeConnection(nodeID)
		return nil, err
	}
	
	// Receive response
	resp, err := t.receiveRPCResponse(conn)
	if err != nil {
		t.closeConnection(nodeID)
		return nil, err
	}
	
	reply, ok := resp.Data.(*AppendEntriesReply)
	if !ok {
		return nil, fmt.Errorf("invalid response type for AppendEntries")
	}
	
	return reply, nil
}

// getConnection gets or creates a connection to the specified node
func (t *TCPTransport) getConnection(nodeID NodeID) (net.Conn, error) {
	t.mu.RLock()
	if conn, exists := t.connections[nodeID]; exists {
		t.mu.RUnlock()
		return conn, nil
	}
	t.mu.RUnlock()
	
	// Need to create new connection
	t.mu.Lock()
	defer t.mu.Unlock()
	
	// Check again in case another goroutine created it
	if conn, exists := t.connections[nodeID]; exists {
		return conn, nil
	}
	
	// Resolve node address (in real implementation, this would use service discovery)
	address := t.resolveNodeAddress(nodeID)
	if address == "" {
		return nil, fmt.Errorf("cannot resolve address for node %s", nodeID)
	}
	
	// Create connection with timeout
	conn, err := net.DialTimeout("tcp", address, t.config.RequestTimeout)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s at %s: %w", nodeID, address, err)
	}
	
	t.connections[nodeID] = conn
	
	t.logger.Debug("Created connection", "nodeID", nodeID, "address", address)
	return conn, nil
}

// closeConnection closes and removes a connection
func (t *TCPTransport) closeConnection(nodeID NodeID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	
	if conn, exists := t.connections[nodeID]; exists {
		conn.Close()
		delete(t.connections, nodeID)
		t.logger.Debug("Closed connection", "nodeID", nodeID)
	}
}

// resolveNodeAddress resolves a node ID to an address
func (t *TCPTransport) resolveNodeAddress(nodeID NodeID) string {
	// Use addresses from configuration
	if address, exists := t.config.PeerAddresses[nodeID]; exists {
		return address
	}
	
	// For backward compatibility, check hardcoded addresses
	switch nodeID {
	case "node-1":
		return "127.0.0.1:7001"
	case "node-2":
		return "127.0.0.1:7002"
	case "node-3":
		return "127.0.0.1:7003"
	default:
		return ""
	}
}

// acceptConnections accepts incoming connections
func (t *TCPTransport) acceptConnections() {
	defer t.wg.Done()
	
	for {
		select {
		case <-t.ctx.Done():
			return
		default:
		}
		
		// Set accept timeout
		if tcpListener, ok := t.listener.(*net.TCPListener); ok {
			tcpListener.SetDeadline(time.Now().Add(100 * time.Millisecond))
		}
		
		conn, err := t.listener.Accept()
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue // Timeout is expected
			}
			
			select {
			case <-t.ctx.Done():
				return // Shutting down
			default:
				t.logger.Warn("Failed to accept connection", "error", err)
				continue
			}
		}
		
		// Handle connection in background
		t.wg.Add(1)
		go t.handleConnection(conn)
	}
}

// handleConnection handles an incoming connection
func (t *TCPTransport) handleConnection(conn net.Conn) {
	defer t.wg.Done()
	defer conn.Close()
	
	t.logger.Debug("Handling incoming connection", "remoteAddr", conn.RemoteAddr())
	
	for {
		select {
		case <-t.ctx.Done():
			return
		default:
		}
		
		// Set read timeout
		conn.SetReadDeadline(time.Now().Add(t.config.RequestTimeout))
		
		// Receive RPC request
		req, err := t.receiveRPCRequest(conn)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue // Timeout is expected
			}
			t.logger.Debug("Failed to receive RPC request", "error", err)
			return
		}
		
		// Process request
		resp := t.processRPC(req)
		
		// Send response
		conn.SetWriteDeadline(time.Now().Add(t.config.RequestTimeout))
		if err := t.sendRPCResponse(conn, resp); err != nil {
			t.logger.Debug("Failed to send RPC response", "error", err)
			return
		}
	}
}

// processRPC processes an incoming RPC request
func (t *TCPTransport) processRPC(req *RPCRequest) *RPCResponse {
	if t.raftNode == nil {
		return &RPCResponse{
			Error: "Raft node not set",
		}
	}
	
	switch req.Type {
	case RPCTypeRequestVote:
		args, ok := req.Data.(*RequestVoteArgs)
		if !ok {
			return &RPCResponse{Error: "Invalid RequestVote args"}
		}
		
		reply, err := t.raftNode.RequestVote(args)
		if err != nil {
			return &RPCResponse{Error: err.Error()}
		}
		
		return &RPCResponse{Data: reply}
		
	case RPCTypeAppendEntries:
		args, ok := req.Data.(*AppendEntriesArgs)
		if !ok {
			return &RPCResponse{Error: "Invalid AppendEntries args"}
		}
		
		reply, err := t.raftNode.AppendEntries(args)
		if err != nil {
			return &RPCResponse{Error: err.Error()}
		}
		
		return &RPCResponse{Data: reply}
		
	default:
		return &RPCResponse{Error: fmt.Sprintf("Unknown RPC type: %v", req.Type)}
	}
}

// RPC message types
type RPCType int

const (
	RPCTypeRequestVote RPCType = iota
	RPCTypeAppendEntries
)

// RPCRequest represents an RPC request
type RPCRequest struct {
	Type RPCType
	Data interface{}
}

// RPCResponse represents an RPC response
type RPCResponse struct {
	Data  interface{}
	Error string
}

// sendRPC sends an RPC request
func (t *TCPTransport) sendRPC(conn net.Conn, req *RPCRequest) error {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	
	if err := encoder.Encode(req); err != nil {
		return fmt.Errorf("failed to encode RPC request: %w", err)
	}
	
	// Send length first
	length := uint32(buf.Len())
	if err := gob.NewEncoder(conn).Encode(length); err != nil {
		return fmt.Errorf("failed to send message length: %w", err)
	}
	
	// Send data
	if _, err := conn.Write(buf.Bytes()); err != nil {
		return fmt.Errorf("failed to send RPC request: %w", err)
	}
	
	return nil
}

// sendRPCResponse sends an RPC response
func (t *TCPTransport) sendRPCResponse(conn net.Conn, resp *RPCResponse) error {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	
	if err := encoder.Encode(resp); err != nil {
		return fmt.Errorf("failed to encode RPC response: %w", err)
	}
	
	// Send length first
	length := uint32(buf.Len())
	if err := gob.NewEncoder(conn).Encode(length); err != nil {
		return fmt.Errorf("failed to send message length: %w", err)
	}
	
	// Send data
	if _, err := conn.Write(buf.Bytes()); err != nil {
		return fmt.Errorf("failed to send RPC response: %w", err)
	}
	
	return nil
}

// receiveRPCRequest receives an RPC request
func (t *TCPTransport) receiveRPCRequest(conn net.Conn) (*RPCRequest, error) {
	// Receive length first
	var length uint32
	if err := gob.NewDecoder(conn).Decode(&length); err != nil {
		return nil, fmt.Errorf("failed to receive message length: %w", err)
	}
	
	// Receive data
	buf := make([]byte, length)
	if _, err := conn.Read(buf); err != nil {
		return nil, fmt.Errorf("failed to receive RPC request: %w", err)
	}
	
	// Decode request
	var req RPCRequest
	decoder := gob.NewDecoder(bytes.NewReader(buf))
	if err := decoder.Decode(&req); err != nil {
		return nil, fmt.Errorf("failed to decode RPC request: %w", err)
	}
	
	return &req, nil
}

// receiveRPCResponse receives an RPC response
func (t *TCPTransport) receiveRPCResponse(conn net.Conn) (*RPCResponse, error) {
	// Receive length first
	var length uint32
	if err := gob.NewDecoder(conn).Decode(&length); err != nil {
		return nil, fmt.Errorf("failed to receive message length: %w", err)
	}
	
	// Receive data
	buf := make([]byte, length)
	if _, err := conn.Read(buf); err != nil {
		return nil, fmt.Errorf("failed to receive RPC response: %w", err)
	}
	
	// Decode response
	var resp RPCResponse
	decoder := gob.NewDecoder(bytes.NewReader(buf))
	if err := decoder.Decode(&resp); err != nil {
		return nil, fmt.Errorf("failed to decode RPC response: %w", err)
	}
	
	if resp.Error != "" {
		return nil, fmt.Errorf("RPC error: %s", resp.Error)
	}
	
	return &resp, nil
}

func init() {
	// Register types for gob encoding
	gob.Register(&RequestVoteArgs{})
	gob.Register(&RequestVoteReply{})
	gob.Register(&AppendEntriesArgs{})
	gob.Register(&AppendEntriesReply{})
}