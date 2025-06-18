package network

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/engine"
	"github.com/dshills/QuantaDB/internal/log"
	"github.com/dshills/QuantaDB/internal/network/protocol"
	"github.com/dshills/QuantaDB/internal/txn"
)

// mockConn implements net.Conn for testing
type mockConn struct {
	readBuf  *bytes.Buffer
	writeBuf *bytes.Buffer
	closed   bool
}

func newMockConn() *mockConn {
	return &mockConn{
		readBuf:  &bytes.Buffer{},
		writeBuf: &bytes.Buffer{},
	}
}

func (m *mockConn) Read(b []byte) (n int, err error) {
	if m.closed {
		return 0, io.EOF
	}
	return m.readBuf.Read(b)
}

func (m *mockConn) Write(b []byte) (n int, err error) {
	if m.closed {
		return 0, io.ErrClosedPipe
	}
	return m.writeBuf.Write(b)
}

func (m *mockConn) Close() error {
	m.closed = true
	return nil
}

func (m *mockConn) LocalAddr() net.Addr                { return nil }
func (m *mockConn) RemoteAddr() net.Addr               { return nil }
func (m *mockConn) SetDeadline(t time.Time) error      { return nil }
func (m *mockConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *mockConn) SetWriteDeadline(t time.Time) error { return nil }

// TestSSLRequestHandling tests the SSL request handling during startup
func TestSSLRequestHandling(t *testing.T) {
	mockLogger := log.Default()
	mockCatalog := catalog.NewMemoryCatalog()
	mockEngine := engine.NewMemoryEngine()

	config := DefaultConfig()
	server := &Server{
		config:     config,
		catalog:    mockCatalog,
		engine:     mockEngine,
		txnManager: txn.NewManager(mockEngine, nil),
		logger:     mockLogger,
	}

	conn := &Connection{
		id:         1,
		conn:       newMockConn(),
		server:     server,
		catalog:    mockCatalog,
		engine:     mockEngine,
		txnManager: server.txnManager,
		logger:     mockLogger,
	}

	// Prepare SSL request message
	sslRequest := make([]byte, 8)
	binary.BigEndian.PutUint32(sslRequest[0:4], 8)                       // Length
	binary.BigEndian.PutUint32(sslRequest[4:8], protocol.SSLRequestCode) // SSL request code

	// Prepare startup message after SSL negotiation
	startupMsg := buildStartupMessage(map[string]string{
		"user":     "testuser",
		"database": "testdb",
	})

	// Write both messages to the mock connection
	mockConn := conn.conn.(*mockConn)
	mockConn.readBuf.Write(sslRequest)
	mockConn.readBuf.Write(startupMsg)

	// Create buffered reader
	conn.reader = bufio.NewReader(conn.conn)
	conn.writer = bufio.NewWriter(conn.conn)

	// Test handleStartup
	err := conn.handleStartup()
	if err != nil {
		t.Fatalf("handleStartup failed: %v", err)
	}

	// Check SSL response
	response := mockConn.writeBuf.Bytes()
	if len(response) < 1 || response[0] != 'N' {
		t.Errorf("Expected SSL rejection response 'N', got %v", response)
	}

	// Verify connection parameters
	if conn.params["user"] != "testuser" {
		t.Errorf("Expected user 'testuser', got '%s'", conn.params["user"])
	}
	if conn.params["database"] != "testdb" {
		t.Errorf("Expected database 'testdb', got '%s'", conn.params["database"])
	}
}

// TestNonSSLStartup tests direct startup without SSL request
func TestNonSSLStartup(t *testing.T) {
	mockLogger := log.Default()
	mockCatalog := catalog.NewMemoryCatalog()
	mockEngine := engine.NewMemoryEngine()

	config := DefaultConfig()
	server := &Server{
		config:     config,
		catalog:    mockCatalog,
		engine:     mockEngine,
		txnManager: txn.NewManager(mockEngine, nil),
		logger:     mockLogger,
	}

	conn := &Connection{
		id:         1,
		conn:       newMockConn(),
		server:     server,
		catalog:    mockCatalog,
		engine:     mockEngine,
		txnManager: server.txnManager,
		logger:     mockLogger,
	}

	// Prepare startup message without SSL request
	startupMsg := buildStartupMessage(map[string]string{
		"user":             "testuser",
		"database":         "testdb",
		"application_name": "psql",
	})

	// Write message to the mock connection
	mockConn := conn.conn.(*mockConn)
	mockConn.readBuf.Write(startupMsg)

	// Create buffered reader
	conn.reader = bufio.NewReader(conn.conn)
	conn.writer = bufio.NewWriter(conn.conn)

	// Test handleStartup
	err := conn.handleStartup()
	if err != nil {
		t.Fatalf("handleStartup failed: %v", err)
	}

	// Verify connection parameters
	if conn.params["user"] != "testuser" {
		t.Errorf("Expected user 'testuser', got '%s'", conn.params["user"])
	}
	if conn.params["database"] != "testdb" {
		t.Errorf("Expected database 'testdb', got '%s'", conn.params["database"])
	}
	if conn.params["application_name"] != "psql" {
		t.Errorf("Expected application_name 'psql', got '%s'", conn.params["application_name"])
	}
}

// TestConnectionStateTransitions tests state tracking during connection lifecycle
func TestConnectionStateTransitions(t *testing.T) {
	mockLogger := log.Default()
	mockCatalog := catalog.NewMemoryCatalog()
	mockEngine := engine.NewMemoryEngine()

	config := DefaultConfig()
	server := &Server{
		config:     config,
		catalog:    mockCatalog,
		engine:     mockEngine,
		txnManager: txn.NewManager(mockEngine, nil),
		logger:     mockLogger,
	}

	conn := &Connection{
		id:         1,
		conn:       newMockConn(),
		server:     server,
		catalog:    mockCatalog,
		engine:     mockEngine,
		txnManager: server.txnManager,
		logger:     mockLogger,
		state:      StateStartup,
	}

	// Test state transitions
	conn.setState(StateAuthentication)
	if conn.state != StateAuthentication {
		t.Errorf("Expected state Authentication, got %d", conn.state)
	}

	conn.setState(StateReady)
	if conn.state != StateReady {
		t.Errorf("Expected state Ready, got %d", conn.state)
	}

	conn.setState(StateBusy)
	if conn.state != StateBusy {
		t.Errorf("Expected state Busy, got %d", conn.state)
	}

	conn.setState(StateClosed)
	if conn.state != StateClosed {
		t.Errorf("Expected state Closed, got %d", conn.state)
	}
}

// buildStartupMessage builds a PostgreSQL startup message
func buildStartupMessage(params map[string]string) []byte {
	var buf bytes.Buffer

	// Reserve space for length
	lengthPos := buf.Len()
	buf.Write(make([]byte, 4))

	// Write protocol version
	versionBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(versionBytes, protocol.ProtocolVersion)
	buf.Write(versionBytes)

	// Write parameters
	for key, value := range params {
		buf.WriteString(key)
		buf.WriteByte(0)
		buf.WriteString(value)
		buf.WriteByte(0)
	}

	// Write terminating null
	buf.WriteByte(0)

	// Update length at the beginning
	data := buf.Bytes()
	binary.BigEndian.PutUint32(data[lengthPos:lengthPos+4], uint32(len(data)-lengthPos))

	return data
}
