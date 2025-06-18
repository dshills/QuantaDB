package network

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/engine"
	"github.com/dshills/QuantaDB/internal/log"
	"github.com/dshills/QuantaDB/internal/network/protocol"
	"github.com/dshills/QuantaDB/internal/sql/executor"
	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/sql/types"
	"github.com/dshills/QuantaDB/internal/txn"
)

// Connection states
const (
	StateStartup = iota
	StateAuthentication
	StateReady
	StateBusy
	StateClosed
)

// stateNames maps connection states to readable names
var stateNames = map[int]string{
	StateStartup:        "Startup",
	StateAuthentication: "Authentication",
	StateReady:          "Ready",
	StateBusy:           "Busy",
	StateClosed:         "Closed",
}

// Connection represents a client connection
type Connection struct {
	id         uint32
	conn       net.Conn
	reader     *bufio.Reader
	writer     *bufio.Writer
	server     *Server
	catalog    catalog.Catalog
	engine     engine.Engine
	storage    executor.StorageBackend
	txnManager *txn.Manager
	logger     log.Logger

	// Connection state
	state  int
	params map[string]string

	// Security
	secretKey uint32

	// Current transaction
	currentTxn *txn.MvccTransaction

	// Prepared statements
	preparedStmts map[string]*PreparedStatement
}

// PreparedStatement represents a prepared statement
type PreparedStatement struct {
	Name  string
	Query string
	Plan  planner.Plan
}

// setState sets the connection state and logs the transition
func (c *Connection) setState(newState int) {
	oldStateName := stateNames[c.state]
	newStateName := stateNames[newState]

	if c.state != newState {
		c.logger.Debug("Connection state change",
			"conn_id", c.id,
			"old_state", oldStateName,
			"new_state", newStateName)
		c.state = newState
	}
}

// Handle handles the connection lifecycle
func (c *Connection) Handle(ctx context.Context) error {
	c.reader = bufio.NewReader(c.conn)
	c.writer = bufio.NewWriter(c.conn)
	c.preparedStmts = make(map[string]*PreparedStatement)

	// Set initial state
	c.setState(StateStartup)

	// Reset timeout for startup phase
	if c.server.config.ReadTimeout > 0 {
		c.conn.SetReadDeadline(time.Now().Add(c.server.config.ReadTimeout))
	}

	// Handle startup
	if err := c.handleStartup(); err != nil {
		// Set write deadline before sending error
		if c.server.config.WriteTimeout > 0 {
			c.conn.SetWriteDeadline(time.Now().Add(c.server.config.WriteTimeout))
		}
		c.sendError(err)
		c.writer.Flush() // Ensure error is sent
		return fmt.Errorf("startup failed: %w", err)
	}

	// Handle authentication
	if err := c.handleAuthentication(); err != nil {
		// Set write deadline before sending error
		if c.server.config.WriteTimeout > 0 {
			c.conn.SetWriteDeadline(time.Now().Add(c.server.config.WriteTimeout))
		}
		c.sendError(err)
		c.writer.Flush() // Ensure error is sent
		return fmt.Errorf("authentication failed: %w", err)
	}

	// Send ready for query
	if err := c.sendReadyForQuery(); err != nil {
		return err
	}

	// Main message loop
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Reset read deadline for each message
			if c.server.config.ReadTimeout > 0 {
				c.conn.SetReadDeadline(time.Now().Add(c.server.config.ReadTimeout))
			}

			msg, err := protocol.ReadMessage(c.reader)
			if err != nil {
				if err == io.EOF {
					c.logger.Info("Client disconnected")
					return nil
				}
				return fmt.Errorf("failed to read message: %w", err)
			}

			if err := c.handleMessage(ctx, msg); err != nil {
				// Set write deadline before sending error
				if c.server.config.WriteTimeout > 0 {
					c.conn.SetWriteDeadline(time.Now().Add(c.server.config.WriteTimeout))
				}
				c.sendError(err)
				if c.state == StateClosed {
					return err
				}
			}
		}
	}
}

// handleStartup handles the startup phase
func (c *Connection) handleStartup() error {
	// Peek at the first 8 bytes to check for SSL request
	peekBuf, err := c.reader.Peek(8)
	if err != nil {
		return fmt.Errorf("failed to peek startup message: %w", err)
	}

	// Check if this is an SSL request
	length := int(binary.BigEndian.Uint32(peekBuf[:4]))
	version := binary.BigEndian.Uint32(peekBuf[4:])

	c.logger.Debug("Startup message received", "length", length, "version", version)

	if length == 8 && version == protocol.SSLRequestCode {
		c.logger.Debug("SSL request detected")

		// Consume the 8 bytes we peeked
		_, err := c.reader.Discard(8)
		if err != nil {
			return fmt.Errorf("failed to discard SSL request bytes: %w", err)
		}

		// SSL request - respond with 'N' (no SSL support)
		// Set write deadline
		if c.server.config.WriteTimeout > 0 {
			c.conn.SetWriteDeadline(time.Now().Add(c.server.config.WriteTimeout))
		}
		if _, err := c.conn.Write([]byte{'N'}); err != nil {
			return fmt.Errorf("failed to send SSL response: %w", err)
		}

		c.logger.Debug("SSL response sent (no SSL support)")

		// Now read the actual startup message
		params, err := protocol.ReadStartupMessage(c.reader)
		if err != nil {
			return fmt.Errorf("failed to read startup message after SSL: %w", err)
		}
		c.params = params
	} else {
		// Not an SSL request, read as normal startup message
		c.logger.Debug("Regular startup message detected")

		params, err := protocol.ReadStartupMessage(c.reader)
		if err != nil {
			return fmt.Errorf("failed to read startup message: %w", err)
		}
		c.params = params
	}

	c.logger.Info("Client connected",
		"user", c.params["user"],
		"database", c.params["database"],
		"application", c.params["application_name"])

	// Send authentication request
	// Set write deadline for all subsequent writes
	if c.server.config.WriteTimeout > 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.server.config.WriteTimeout))
	}
	auth := &protocol.Authentication{
		Type: protocol.AuthOK, // No authentication for now
	}
	if err := protocol.WriteMessage(c.writer, auth.ToMessage()); err != nil {
		return err
	}

	// Send parameter status messages
	// Set write deadline for parameter status messages
	if c.server.config.WriteTimeout > 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.server.config.WriteTimeout))
	}
	parameters := map[string]string{
		"server_version":              "15.0 (QuantaDB 0.1.0)",
		"server_encoding":             "UTF8",
		"client_encoding":             "UTF8",
		"DateStyle":                   "ISO, MDY",
		"integer_datetimes":           "on",
		"TimeZone":                    "UTC",
		"standard_conforming_strings": "on",
	}

	for name, value := range parameters {
		param := &protocol.ParameterStatus{
			Name:  name,
			Value: value,
		}
		if err := protocol.WriteMessage(c.writer, param.ToMessage()); err != nil {
			return err
		}
	}

	// Generate cryptographically secure secret key
	if err := binary.Read(rand.Reader, binary.BigEndian, &c.secretKey); err != nil {
		return fmt.Errorf("failed to generate secret key: %w", err)
	}

	// Send backend key data
	// Set write deadline for backend key data
	if c.server.config.WriteTimeout > 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.server.config.WriteTimeout))
	}
	keyData := &protocol.BackendKeyData{
		ProcessID: c.id,
		SecretKey: c.secretKey,
	}
	if err := protocol.WriteMessage(c.writer, keyData.ToMessage()); err != nil {
		return err
	}

	// Set write deadline before final flush
	if c.server.config.WriteTimeout > 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.server.config.WriteTimeout))
	}
	return c.writer.Flush()
}

// handleAuthentication handles authentication
func (c *Connection) handleAuthentication() error {
	c.setState(StateAuthentication)
	// For now, we accept all connections
	c.setState(StateReady)
	return nil
}

// handleMessage handles a single message
func (c *Connection) handleMessage(ctx context.Context, msg *protocol.Message) error {
	switch msg.Type {
	case protocol.MsgQuery:
		return c.handleQuery(ctx, msg)
	case protocol.MsgParse:
		return c.handleParse(ctx, msg)
	case protocol.MsgBind:
		return c.handleBind(ctx, msg)
	case protocol.MsgExecute:
		return c.handleExecute(ctx, msg)
	case protocol.MsgSync:
		return c.handleSync(ctx)
	case protocol.MsgTerminate:
		c.setState(StateClosed)
		return nil
	default:
		return fmt.Errorf("unsupported message type: %c", msg.Type)
	}
}

// handleQuery handles a simple query
func (c *Connection) handleQuery(ctx context.Context, msg *protocol.Message) error {
	c.setState(StateBusy)

	// Parse query
	q := &protocol.Query{}
	if err := q.Parse(msg.Data); err != nil {
		return err
	}

	c.logger.Debug("Executing query", "query", q.Query)

	// Handle special commands
	query := strings.TrimSpace(q.Query)
	upperQuery := strings.ToUpper(query)

	// Transaction control
	switch {
	case upperQuery == "BEGIN":
		return c.handleBegin(ctx)
	case upperQuery == "COMMIT":
		return c.handleCommit(ctx)
	case upperQuery == "ROLLBACK":
		return c.handleRollback(ctx)
	}

	// Parse SQL
	p := parser.NewParser(query)
	stmt, err := p.Parse()
	if err != nil {
		return fmt.Errorf("parse error: %w", err)
	}

	// Plan query
	planr := planner.NewBasicPlannerWithCatalog(c.catalog)
	plan, err := planr.Plan(stmt)
	if err != nil {
		return fmt.Errorf("planning error: %w", err)
	}

	// Execute query
	exec := executor.NewBasicExecutor(c.catalog, c.engine)
	if c.storage != nil {
		exec.SetStorageBackend(c.storage)
	}
	execCtx := &executor.ExecContext{
		Catalog:    c.catalog,
		Engine:     c.engine,
		TxnManager: c.txnManager,
		Txn:        c.currentTxn,
		Stats:      &executor.ExecStats{},
	}

	result, err := exec.Execute(plan, execCtx)
	if err != nil {
		return fmt.Errorf("execution error: %w", err)
	}
	defer result.Close()

	// Send results
	if err := c.sendResults(result, stmt); err != nil {
		return err
	}

	// Send ready for query
	return c.sendReadyForQuery()
}

// handleBegin starts a new transaction
func (c *Connection) handleBegin(ctx context.Context) error {
	if c.currentTxn != nil {
		return fmt.Errorf("already in a transaction")
	}

	txn, err := c.txnManager.BeginTransaction(ctx, txn.ReadCommitted)
	if err != nil {
		return err
	}

	c.currentTxn = txn
	c.logger.Debug("Transaction started", "txn_id", txn.ID())

	// Set write deadline before sending command complete
	if c.server.config.WriteTimeout > 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.server.config.WriteTimeout))
	}
	// Send command complete
	complete := &protocol.CommandComplete{Tag: "BEGIN"}
	if err := protocol.WriteMessage(c.writer, complete.ToMessage()); err != nil {
		return err
	}

	return c.sendReadyForQuery()
}

// handleCommit commits the current transaction
func (c *Connection) handleCommit(ctx context.Context) error {
	if c.currentTxn == nil {
		return fmt.Errorf("not in a transaction")
	}

	err := c.currentTxn.Commit()
	c.currentTxn = nil

	if err != nil {
		return err
	}

	// Set write deadline before sending command complete
	if c.server.config.WriteTimeout > 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.server.config.WriteTimeout))
	}
	// Send command complete
	complete := &protocol.CommandComplete{Tag: "COMMIT"}
	if err := protocol.WriteMessage(c.writer, complete.ToMessage()); err != nil {
		return err
	}

	return c.sendReadyForQuery()
}

// handleRollback rolls back the current transaction
func (c *Connection) handleRollback(ctx context.Context) error {
	if c.currentTxn == nil {
		return fmt.Errorf("not in a transaction")
	}

	err := c.currentTxn.Rollback()
	c.currentTxn = nil

	if err != nil {
		return err
	}

	// Set write deadline before sending command complete
	if c.server.config.WriteTimeout > 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.server.config.WriteTimeout))
	}
	// Send command complete
	complete := &protocol.CommandComplete{Tag: "ROLLBACK"}
	if err := protocol.WriteMessage(c.writer, complete.ToMessage()); err != nil {
		return err
	}

	return c.sendReadyForQuery()
}

// sendResults sends query results to the client
func (c *Connection) sendResults(result executor.Result, stmt parser.Statement) error {
	// Set write deadline for all result writes
	if c.server.config.WriteTimeout > 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.server.config.WriteTimeout))
	}
	schema := result.Schema()

	// Send row description
	rowDesc := &protocol.RowDescription{
		Fields: make([]protocol.FieldDescription, len(schema.Columns)),
	}

	for i, col := range schema.Columns {
		rowDesc.Fields[i] = protocol.FieldDescription{
			Name:         col.Name,
			TableOID:     0, // TODO: Add table OID
			ColumnNumber: int16(i + 1),
			DataTypeOID:  getTypeOID(col.Type),
			DataTypeSize: getTypeSize(col.Type),
			TypeModifier: -1,
			Format:       protocol.FormatText,
		}
	}

	if err := protocol.WriteMessage(c.writer, rowDesc.ToMessage()); err != nil {
		return err
	}

	// Send data rows
	rowCount := 0
	for {
		row, err := result.Next()
		if err != nil {
			return err
		}
		if row == nil {
			break
		}

		dataRow := &protocol.DataRow{
			Values: make([][]byte, len(row.Values)),
		}

		for i, val := range row.Values {
			if val.IsNull() {
				dataRow.Values[i] = nil
			} else {
				dataRow.Values[i] = []byte(val.String())
			}
		}

		if err := protocol.WriteMessage(c.writer, dataRow.ToMessage()); err != nil {
			return err
		}

		rowCount++
	}

	// Send command complete
	tag := getCommandTag(stmt, rowCount)
	complete := &protocol.CommandComplete{Tag: tag}
	if err := protocol.WriteMessage(c.writer, complete.ToMessage()); err != nil {
		return err
	}

	return c.writer.Flush()
}

// sendReadyForQuery sends ready for query message
func (c *Connection) sendReadyForQuery() error {
	// Set write deadline
	if c.server.config.WriteTimeout > 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.server.config.WriteTimeout))
	}
	status := byte(protocol.TxnStatusIdle)
	if c.currentTxn != nil {
		status = byte(protocol.TxnStatusInBlock)
	}

	ready := &protocol.ReadyForQuery{Status: status}
	if err := protocol.WriteMessage(c.writer, ready.ToMessage()); err != nil {
		return err
	}

	c.setState(StateReady)
	return c.writer.Flush()
}

// sendError sends an error response
func (c *Connection) sendError(err error) error {
	// Set write deadline
	if c.server.config.WriteTimeout > 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.server.config.WriteTimeout))
	}
	errResp := &protocol.ErrorResponse{
		Severity: protocol.SeverityError,
		Code:     "42000", // Syntax error
		Message:  err.Error(),
	}

	if err := protocol.WriteMessage(c.writer, errResp.ToMessage()); err != nil {
		return err
	}

	return c.writer.Flush()
}

// SetReadTimeout sets the read timeout
func (c *Connection) SetReadTimeout(timeout time.Duration) {
	c.conn.SetReadDeadline(time.Now().Add(timeout))
}

// SetWriteTimeout sets the write timeout
func (c *Connection) SetWriteTimeout(timeout time.Duration) {
	c.conn.SetWriteDeadline(time.Now().Add(timeout))
}

// Close closes the connection
func (c *Connection) Close() error {
	c.setState(StateClosed)

	// Rollback any active transaction
	if c.currentTxn != nil {
		c.currentTxn.Rollback()
		c.currentTxn = nil
	}

	return c.conn.Close()
}

// Extended query protocol stubs
func (c *Connection) handleParse(ctx context.Context, msg *protocol.Message) error {
	// TODO: Implement
	return fmt.Errorf("parse not implemented")
}

func (c *Connection) handleBind(ctx context.Context, msg *protocol.Message) error {
	// TODO: Implement
	return fmt.Errorf("bind not implemented")
}

func (c *Connection) handleExecute(ctx context.Context, msg *protocol.Message) error {
	// TODO: Implement
	return fmt.Errorf("execute not implemented")
}

func (c *Connection) handleSync(ctx context.Context) error {
	return c.sendReadyForQuery()
}

// Helper functions

// getTypeOID returns PostgreSQL type OID for a data type
func getTypeOID(dt types.DataType) uint32 {
	switch dt.Name() {
	case "INTEGER":
		return 23 // int4
	case "BIGINT":
		return 20 // int8
	case "SMALLINT":
		return 21 // int2
	case "TEXT", "VARCHAR", "CHAR":
		return 25 // text
	case "BOOLEAN":
		return 16 // bool
	case "TIMESTAMP":
		return 1114 // timestamp
	case "DATE":
		return 1082 // date
	case "DECIMAL":
		return 1700 // numeric
	default:
		return 25 // text as fallback
	}
}

// getTypeSize returns the size of a data type
func getTypeSize(dt types.DataType) int16 {
	size := dt.Size()
	if size < 0 {
		return -1 // Variable size
	}
	return int16(size)
}

// getCommandTag returns the command tag for a statement
func getCommandTag(stmt parser.Statement, rowCount int) string {
	switch stmt.(type) {
	case *parser.SelectStmt:
		return fmt.Sprintf("SELECT %d", rowCount)
	case *parser.InsertStmt:
		return fmt.Sprintf("INSERT 0 %d", rowCount)
	case *parser.UpdateStmt:
		return fmt.Sprintf("UPDATE %d", rowCount)
	case *parser.DeleteStmt:
		return fmt.Sprintf("DELETE %d", rowCount)
	case *parser.CreateTableStmt:
		return "CREATE TABLE"
	default:
		return "OK"
	}
}
