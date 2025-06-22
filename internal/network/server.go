package network

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/engine"
	"github.com/dshills/QuantaDB/internal/log"
	"github.com/dshills/QuantaDB/internal/network/protocol"
	"github.com/dshills/QuantaDB/internal/sql/executor"
	"github.com/dshills/QuantaDB/internal/txn"
)

// UserStore defines the interface for user authentication
type UserStore interface {
	// Authenticate validates user credentials
	Authenticate(username, password string) bool
	// GetUserMD5 returns the MD5 hash for a user (if using MD5 auth)
	GetUserMD5(username, salt string) (string, bool)
	// UserExists checks if a user exists
	UserExists(username string) bool
}

// Server represents the PostgreSQL protocol server
type Server struct {
	config     Config
	listener   net.Listener
	catalog    catalog.Catalog
	engine     engine.Engine
	storage    executor.StorageBackend
	txnManager *txn.Manager
	tsService  *txn.TimestampService
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
	
	// SSL/TLS configuration
	EnableSSL    bool
	TLSConfig    *tls.Config
	CertFile     string
	KeyFile      string
	RequireSSL   bool  // If true, reject non-SSL connections
	
	// Authentication configuration  
	AuthMethod   string  // "none", "password", "md5"
	UserStore    UserStore // User credential store
}

// DefaultConfig returns default server configuration
func DefaultConfig() Config {
	return Config{
		Host:           "localhost",
		Port:           5432,
		MaxConnections: 100,
		ReadTimeout:    30 * time.Second,
		WriteTimeout:   30 * time.Second,
		EnableSSL:      false,
		RequireSSL:     false,
		AuthMethod:     "none", // Default to no authentication
		UserStore:      nil,    // Will be set by user
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
		tsService:   txn.NewTimestampService(),
		logger:      logger,
		connections: make(map[uint32]*Connection),
		shutdown:    make(chan struct{}),
	}
}

// NewServerWithTxnManager creates a new server instance with a custom transaction manager
func NewServerWithTxnManager(config Config, cat catalog.Catalog, eng engine.Engine, txnMgr *txn.Manager, logger log.Logger) *Server {
	return &Server{
		config:      config,
		catalog:     cat,
		engine:      eng,
		storage:     nil, // Set with SetStorageBackend
		txnManager:  txnMgr,
		tsService:   txn.NewTimestampService(),
		logger:      logger,
		connections: make(map[uint32]*Connection),
		shutdown:    make(chan struct{}),
	}
}

// ConfigureSSL configures SSL/TLS for the server using certificate files
func (s *Server) ConfigureSSL(certFile, keyFile string) error {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return fmt.Errorf("failed to load SSL certificate: %w", err)
	}
	
	s.config.TLSConfig = &tls.Config{
		Certificates: []tls.Certificate{cert},
		ServerName:   s.config.Host,
	}
	s.config.EnableSSL = true
	s.config.CertFile = certFile
	s.config.KeyFile = keyFile
	
	s.logger.Info("SSL/TLS configured", "cert_file", certFile, "key_file", keyFile)
	return nil
}

// ConfigureSSLWithConfig configures SSL/TLS for the server using a custom TLS config
func (s *Server) ConfigureSSLWithConfig(tlsConfig *tls.Config) {
	s.config.TLSConfig = tlsConfig
	s.config.EnableSSL = true
	s.logger.Info("SSL/TLS configured with custom config")
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
		tsService:  s.tsService,
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
	s.logger.Debug("Setting connection timeouts", "read_timeout", s.config.ReadTimeout, "write_timeout", s.config.WriteTimeout)
	conn.SetReadTimeout(s.config.ReadTimeout)
	conn.SetWriteTimeout(s.config.WriteTimeout)

	// Handle SSL upgrade if configured
	actualConn := netConn
	if s.config.EnableSSL {
		sslConn, err := s.handleSSLUpgrade(netConn)
		if err != nil {
			s.logger.Error("SSL upgrade failed", "error", err)
			return
		}
		if sslConn != nil {
			actualConn = sslConn
			conn.conn = actualConn  // Update connection with SSL-wrapped connection
			s.logger.Debug("SSL connection established")
		}
	}

	// Handle connection
	if err := conn.Handle(ctx); err != nil {
		conn.logger.Error("Connection error", "error", err)
	}

	// Close connection
	conn.Close()
}

// handleSSLUpgrade handles the SSL request and potentially upgrades the connection to TLS
func (s *Server) handleSSLUpgrade(conn net.Conn) (net.Conn, error) {
	// Set a read deadline for the SSL request
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	defer conn.SetReadDeadline(time.Time{}) // Clear deadline
	
	// Read the first message to check if it's an SSL request
	lengthBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, lengthBuf); err != nil {
		return nil, fmt.Errorf("failed to read message length: %w", err)
	}
	
	length := int(binary.BigEndian.Uint32(lengthBuf))
	if length < 4 {
		return nil, fmt.Errorf("invalid message length: %d", length)
	}
	
	// Read the message body
	msgBuf := make([]byte, length-4)
	if _, err := io.ReadFull(conn, msgBuf); err != nil {
		return nil, fmt.Errorf("failed to read message body: %w", err)
	}
	
	// Check if this is an SSL request
	if length == 8 && len(msgBuf) >= 4 {
		version := binary.BigEndian.Uint32(msgBuf[:4])
		if version == protocol.SSLRequestCode {
			s.logger.Debug("SSL request received")
			
			// Send SSL support response
			response := byte('S') // 'S' for SSL supported
			if s.config.RequireSSL {
				response = 'S' // Always 'S' if we require SSL
			}
			
			if _, err := conn.Write([]byte{response}); err != nil {
				return nil, fmt.Errorf("failed to send SSL response: %w", err)
			}
			
			s.logger.Debug("SSL response sent", "response", string(response))
			
			if response == 'S' {
				// Upgrade to TLS
				tlsConn := tls.Server(conn, s.config.TLSConfig)
				
				// Perform TLS handshake
				if err := tlsConn.Handshake(); err != nil {
					return nil, fmt.Errorf("TLS handshake failed: %w", err)
				}
				
				s.logger.Debug("TLS handshake completed successfully")
				return tlsConn, nil
			}
			
			// Client will reconnect without SSL
			return conn, nil
		}
	}
	
	// Not an SSL request - put the data back by creating a buffered connection
	// We need to prepend the data we read back to the connection
	combinedData := make([]byte, 0, len(lengthBuf)+len(msgBuf))
	combinedData = append(combinedData, lengthBuf...)
	combinedData = append(combinedData, msgBuf...)
	
	// Create a connection that has the data prepended
	bufferedConn := &prependedConn{
		conn:   conn,
		prefix: combinedData,
	}
	
	return bufferedConn, nil
}

// prependedConn wraps a connection and prepends some data to reads
type prependedConn struct {
	conn   net.Conn
	prefix []byte
	offset int
}

func (pc *prependedConn) Read(b []byte) (int, error) {
	if pc.offset < len(pc.prefix) {
		// Still reading from prefix
		n := copy(b, pc.prefix[pc.offset:])
		pc.offset += n
		return n, nil
	}
	// Reading from underlying connection
	return pc.conn.Read(b)
}

func (pc *prependedConn) Write(b []byte) (int, error) {
	return pc.conn.Write(b)
}

func (pc *prependedConn) Close() error {
	return pc.conn.Close()
}

func (pc *prependedConn) LocalAddr() net.Addr {
	return pc.conn.LocalAddr()
}

func (pc *prependedConn) RemoteAddr() net.Addr {
	return pc.conn.RemoteAddr()
}

func (pc *prependedConn) SetDeadline(t time.Time) error {
	return pc.conn.SetDeadline(t)
}

func (pc *prependedConn) SetReadDeadline(t time.Time) error {
	return pc.conn.SetReadDeadline(t)
}

func (pc *prependedConn) SetWriteDeadline(t time.Time) error {
	return pc.conn.SetWriteDeadline(t)
}

// GetConnectionCount returns the current number of connections
func (s *Server) GetConnectionCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.connections)
}
