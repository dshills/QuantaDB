package network

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/engine"
	"github.com/dshills/QuantaDB/internal/log"
	"github.com/dshills/QuantaDB/internal/sql/executor"
	"github.com/dshills/QuantaDB/internal/txn"
)

// Server represents the PostgreSQL protocol server
type Server struct {
	config     Config
	listener   net.Listener
	catalog    catalog.Catalog
	engine     engine.Engine
	storage    executor.StorageBackend
	txnManager *txn.Manager
	logger     log.Logger

	// Connection management
	mu          sync.RWMutex
	connections map[uint32]*Connection
	nextConnID  uint32
	shutdown    chan struct{}
	wg          sync.WaitGroup
}

// Config holds server configuration
type Config struct {
	Host           string
	Port           int
	MaxConnections int
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration
}

// DefaultConfig returns default server configuration
func DefaultConfig() Config {
	return Config{
		Host:           "localhost",
		Port:           5432,
		MaxConnections: 100,
		ReadTimeout:    30 * time.Second,
		WriteTimeout:   30 * time.Second,
	}
}

// NewServer creates a new server instance
func NewServer(config Config, cat catalog.Catalog, eng engine.Engine, logger log.Logger) *Server {
	return &Server{
		config:      config,
		catalog:     cat,
		engine:      eng,
		storage:     nil, // Set with SetStorageBackend
		txnManager:  txn.NewManager(eng, nil),
		logger:      logger,
		connections: make(map[uint32]*Connection),
		shutdown:    make(chan struct{}),
	}
}

// SetStorageBackend sets the storage backend for the server
func (s *Server) SetStorageBackend(storage executor.StorageBackend) {
	s.storage = storage
}

// Start starts the server
func (s *Server) Start(ctx context.Context) error {
	addr := fmt.Sprintf("%s:%d", s.config.Host, s.config.Port)

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	s.listener = listener
	s.logger.Info("Server listening", "address", addr)

	// Start accepting connections
	go s.acceptLoop(ctx)

	return nil
}

// Stop gracefully stops the server
func (s *Server) Stop() error {
	s.logger.Info("Stopping server")

	// Signal shutdown
	close(s.shutdown)

	// Close listener
	if s.listener != nil {
		s.listener.Close()
	}

	// Close all connections
	s.mu.Lock()
	for _, conn := range s.connections {
		conn.Close()
	}
	s.mu.Unlock()

	// Wait for all goroutines
	s.wg.Wait()

	s.logger.Info("Server stopped")
	return nil
}

// acceptLoop accepts new connections
func (s *Server) acceptLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.shutdown:
			return
		default:
			// Set accept deadline
			if deadline, ok := ctx.Deadline(); ok {
				s.listener.(*net.TCPListener).SetDeadline(deadline)
			}

			conn, err := s.listener.Accept()
			if err != nil {
				select {
				case <-s.shutdown:
					return
				default:
					s.logger.Error("Failed to accept connection", "error", err)
					continue
				}
			}

			// Check connection limit
			s.mu.RLock()
			connCount := len(s.connections)
			s.mu.RUnlock()

			if connCount >= s.config.MaxConnections {
				s.logger.Warn("Connection limit reached", "limit", s.config.MaxConnections)
				conn.Close()
				continue
			}

			// Handle connection
			s.wg.Add(1)
			go s.handleConnection(ctx, conn)
		}
	}
}

// handleConnection handles a single client connection
func (s *Server) handleConnection(ctx context.Context, netConn net.Conn) {
	defer s.wg.Done()

	// Create connection ID
	connID := atomic.AddUint32(&s.nextConnID, 1)

	// Create connection wrapper
	conn := &Connection{
		id:         connID,
		conn:       netConn,
		server:     s,
		catalog:    s.catalog,
		engine:     s.engine,
		storage:    s.storage,
		txnManager: s.txnManager,
		logger:     s.logger.With("conn_id", connID),
		state:      StateStartup,
		params:     make(map[string]string),
	}

	// Register connection
	s.mu.Lock()
	s.connections[connID] = conn
	s.mu.Unlock()

	// Remove connection on exit
	defer func() {
		s.mu.Lock()
		delete(s.connections, connID)
		s.mu.Unlock()
	}()

	// Set timeouts
	conn.SetReadTimeout(s.config.ReadTimeout)
	conn.SetWriteTimeout(s.config.WriteTimeout)

	// Handle connection
	if err := conn.Handle(ctx); err != nil {
		conn.logger.Error("Connection error", "error", err)
	}

	// Close connection
	conn.Close()
}

// GetConnectionCount returns the current number of connections
func (s *Server) GetConnectionCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.connections)
}
