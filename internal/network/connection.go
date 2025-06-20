package network

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net"
	"strings"
	"time"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/engine"
	"github.com/dshills/QuantaDB/internal/errors"
	"github.com/dshills/QuantaDB/internal/log"
	"github.com/dshills/QuantaDB/internal/network/protocol"
	"github.com/dshills/QuantaDB/internal/sql"
	"github.com/dshills/QuantaDB/internal/sql/executor"
	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/sql/types"
	"github.com/dshills/QuantaDB/internal/txn"
)

// Connection states.
const (
	StateStartup = iota
	StateAuthentication
	StateReady
	StateBusy
	StateClosed
)

// stateNames maps connection states to readable names.
var stateNames = map[int]string{
	StateStartup:        "Startup",
	StateAuthentication: "Authentication",
	StateReady:          "Ready",
	StateBusy:           "Busy",
	StateClosed:         "Closed",
}

// Connection represents a client connection.
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
	tsService  *txn.TimestampService
	logger     log.Logger

	// Connection state
	state  int
	params map[string]string

	// Security
	secretKey uint32

	// Current transaction
	currentTxn *txn.MvccTransaction

	// Extended query protocol session
	extQuerySession *ExtendedQuerySession
}

// setState sets the connection state and logs the transition.
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

// Handle handles the connection lifecycle.
func (c *Connection) Handle(ctx context.Context) error {
	c.reader = bufio.NewReader(c.conn)
	c.writer = bufio.NewWriter(c.conn)
	c.extQuerySession = NewExtendedQuerySession()

	// Set initial state
	c.setState(StateStartup)

	// Log connection details
	c.logger.Debug("New connection established",
		"local_addr", c.conn.LocalAddr(),
		"remote_addr", c.conn.RemoteAddr())

	// Set a reasonable timeout for the startup phase
	if c.server.config.ReadTimeout > 0 {
		c.conn.SetReadDeadline(time.Now().Add(c.server.config.ReadTimeout))
	}

	// Handle startup
	if err := c.handleStartup(); err != nil {
		c.logger.Error("Startup phase failed", "error", err)
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
				// For simple queries, we need to send ReadyForQuery after an error
				// to indicate the server is ready for the next command
				if msg.Type == protocol.MsgQuery {
					c.sendReadyForQuery()
				}
			}
		}
	}
}

// handleStartup handles the startup phase.
func (c *Connection) handleStartup() error {
	c.logger.Debug("Entering handleStartup")

	// First, read the message length (4 bytes)
	lengthBuf := make([]byte, 4)
	c.logger.Debug("About to read startup message length")
	if _, err := io.ReadFull(c.reader, lengthBuf); err != nil {
		c.logger.Debug("Failed to read startup message length", "error", err)
		return fmt.Errorf("failed to read startup message length: %w", err)
	}

	length := int(binary.BigEndian.Uint32(lengthBuf))
	if length < 8 {
		return fmt.Errorf("invalid startup message length: %d", length)
	}

	// Read the rest of the message
	msgBuf := make([]byte, length-4)
	if _, err := io.ReadFull(c.reader, msgBuf); err != nil {
		return fmt.Errorf("failed to read startup message: %w", err)
	}

	// Check the version/request code
	version := binary.BigEndian.Uint32(msgBuf[:4])

	c.logger.Debug("Startup message received", "length", length, "version", version)

	if length == 8 && version == protocol.SSLRequestCode {
		c.logger.Debug("SSL request detected")

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
		// Not an SSL request, parse as normal startup message
		c.logger.Debug("Regular startup message detected")

		// Create a reader with the full message (including length)
		fullMsg := make([]byte, length)
		copy(fullMsg[:4], lengthBuf)
		copy(fullMsg[4:], msgBuf)
		msgReader := bytes.NewReader(fullMsg)

		// Use the standard protocol.ReadStartupMessage
		params, err := protocol.ReadStartupMessage(msgReader)
		if err != nil {
			return fmt.Errorf("failed to parse startup message: %w", err)
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
// Currently accepts all connections without validation.
// TODO: In production, this should validate credentials against a user database.
//
//nolint:unparam // Will return errors when authentication is implemented
func (c *Connection) handleAuthentication() error {
	c.setState(StateAuthentication)

	// TODO: Implement actual authentication
	// Example of what production code might look like:
	// if !c.validateCredentials() {
	//     return fmt.Errorf("authentication failed")
	// }
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
	case protocol.MsgDescribe:
		return c.handleDescribe(ctx, msg)
	case protocol.MsgClose:
		return c.handleClose(ctx, msg)
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
	if strings.HasPrefix(upperQuery, "BEGIN") {
		return c.handleBegin(ctx, query)
	}
	switch upperQuery {
	case "COMMIT":
		return c.handleCommit(ctx)
	case "ROLLBACK":
		return c.handleRollback(ctx)
	}

	// Parse SQL
	p := parser.NewParser(query)
	stmt, err := p.Parse()
	if err != nil {
		// Parser errors should already have position info
		// but wrap them in case they don't
		if pErr, ok := err.(*parser.ParseError); ok && pErr.Line > 0 {
			return errors.ParseError(pErr.Msg, pErr.Line, pErr.Column)
		}
		return errors.SyntaxErrorf(0, "parse error: %v", err)
	}

	// Plan query
	planr := planner.NewBasicPlannerWithCatalog(c.catalog)
	plan, err := planr.Plan(stmt)
	if err != nil {
		// Convert planner errors to appropriate PostgreSQL errors
		// Check for common error patterns
		errStr := err.Error()
		if strings.Contains(errStr, "table") && strings.Contains(errStr, "not found") {
			// Extract table name if possible
			parts := strings.Split(errStr, "\"")
			if len(parts) >= 2 {
				return errors.TableNotFoundError(parts[1])
			}
			return errors.UndefinedTableError("unknown")
		}
		if strings.Contains(errStr, "column") && strings.Contains(errStr, "not found") {
			// Extract column name if possible
			parts := strings.Split(errStr, "\"")
			if len(parts) >= 2 {
				return errors.ColumnNotFoundError(parts[1], "")
			}
			return errors.UndefinedColumnError("unknown", "")
		}
		if strings.Contains(errStr, "ambiguous") {
			parts := strings.Split(errStr, "\"")
			if len(parts) >= 2 {
				return errors.AmbiguousColumnError(parts[1])
			}
			return errors.New(errors.AmbiguousColumn, errStr)
		}
		return errors.Newf(errors.SyntaxErrorOrAccessRuleViolation, "planning error: %v", err)
	}

	// Execute query
	exec := executor.NewBasicExecutor(c.catalog, c.engine)
	if c.storage != nil {
		exec.SetStorageBackend(c.storage)
	}
	execCtx := &executor.ExecContext{
		Catalog:        c.catalog,
		Engine:         c.engine,
		TxnManager:     c.txnManager,
		Txn:            c.currentTxn,
		SnapshotTS:     c.getSnapshotTimestamp(),
		IsolationLevel: c.getIsolationLevel(),
		Stats:          &executor.ExecStats{},
	}

	result, err := exec.Execute(plan, execCtx)
	if err != nil {
		// Convert executor errors to appropriate PostgreSQL errors
		errStr := err.Error()
		if strings.Contains(errStr, "division by zero") {
			return errors.DivisionByZeroError()
		}
		if strings.Contains(errStr, "type mismatch") {
			return errors.DataTypeMismatchError("unknown", "unknown")
		}
		if strings.Contains(errStr, "not null constraint") {
			return errors.NotNullViolationError("unknown", "unknown")
		}
		if strings.Contains(errStr, "unique constraint") {
			return errors.UniqueViolationError("unknown", "unknown")
		}
		if strings.Contains(errStr, "foreign key constraint") {
			return errors.ForeignKeyViolationError("unknown", "unknown")
		}
		return errors.InternalErrorf("execution error: %v", err)
	}
	defer result.Close()

	// Send results
	c.logger.Debug("Sending results")
	if err := c.sendResults(result, stmt); err != nil {
		c.logger.Debug("Failed to send results", "error", err)
		return err
	}
	c.logger.Debug("Results sent")

	// Send ready for query
	c.logger.Debug("Sending ready for query")
	if err := c.sendReadyForQuery(); err != nil {
		c.logger.Debug("Failed to send ready for query", "error", err)
		return err
	}
	c.logger.Debug("Ready for query sent")
	return nil
}

// handleBegin starts a new transaction
func (c *Connection) handleBegin(ctx context.Context, query string) error {
	if c.currentTxn != nil {
		return errors.TransactionAlreadyActiveError()
	}

	// Parse isolation level from query
	isolationLevel := c.parseIsolationLevel(query)

	txn, err := c.txnManager.BeginTransaction(ctx, isolationLevel)
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
	_ = ctx // Context might be used for cancellation in the future
	return c.endTransaction(true, "COMMIT")
}

// handleRollback rolls back the current transaction
func (c *Connection) handleRollback(ctx context.Context) error {
	_ = ctx // Context might be used for cancellation in the future
	return c.endTransaction(false, "ROLLBACK")
}

// endTransaction handles transaction commit or rollback
func (c *Connection) endTransaction(commit bool, tag string) error {
	if c.currentTxn == nil {
		return errors.NoActiveTransactionError()
	}

	var err error
	if commit {
		err = c.currentTxn.Commit()
	} else {
		err = c.currentTxn.Rollback()
	}
	c.currentTxn = nil

	if err != nil {
		return err
	}

	// Set write deadline before sending command complete
	if c.server.config.WriteTimeout > 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.server.config.WriteTimeout))
	}
	// Send command complete
	complete := &protocol.CommandComplete{Tag: tag}
	if err := protocol.WriteMessage(c.writer, complete.ToMessage()); err != nil {
		return err
	}

	return c.sendReadyForQuery()
}

// sendResults sends query results to the client
func (c *Connection) sendResults(result executor.Result, stmt parser.Statement) error {
	c.logger.Debug("sendResults called")
	// Set write deadline for all result writes
	if c.server.config.WriteTimeout > 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.server.config.WriteTimeout))
	}
	schema := result.Schema()
	c.logger.Debug("Got schema", "columns", len(schema.Columns))

	// Send row description
	rowDesc := &protocol.RowDescription{
		Fields: make([]protocol.FieldDescription, len(schema.Columns)),
	}

	for i, col := range schema.Columns {
		if i >= math.MaxInt16 {
			return fmt.Errorf("too many columns: %d exceeds max int16", i+1)
		}
		rowDesc.Fields[i] = protocol.FieldDescription{
			Name:         col.Name,
			TableOID:     0,            // TODO: Add table OID
			ColumnNumber: int16(i + 1), //nolint:gosec // Bounds checked above
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

	// Convert to QuantaDB error if it isn't already
	qErr := errors.GetError(err)

	errResp := &protocol.ErrorResponse{
		Severity:       protocol.SeverityError,
		Code:           qErr.Code,
		Message:        qErr.Message,
		Detail:         qErr.Detail,
		Hint:           qErr.Hint,
		Where:          qErr.Where,
		SchemaName:     qErr.Schema,
		TableName:      qErr.Table,
		ColumnName:     qErr.Column,
		DataType:       qErr.DataType,
		ConstraintName: qErr.Constraint,
		File:           qErr.File,
		Routine:        qErr.Routine,
	}

	// Convert integer fields to strings
	if qErr.Position > 0 {
		errResp.Position = fmt.Sprintf("%d", qErr.Position)
	}
	if qErr.InternalPosition > 0 {
		errResp.InternalPosition = fmt.Sprintf("%d", qErr.InternalPosition)
	}
	if qErr.Line > 0 {
		errResp.Line = fmt.Sprintf("%d", qErr.Line)
	}
	if qErr.InternalQuery != "" {
		errResp.InternalQuery = qErr.InternalQuery
	}

	if err := protocol.WriteMessage(c.writer, errResp.ToMessage()); err != nil {
		return err
	}

	return c.writer.Flush()
}

// SetReadTimeout sets the read timeout
func (c *Connection) SetReadTimeout(timeout time.Duration) {
	if timeout > 0 {
		c.conn.SetReadDeadline(time.Now().Add(timeout))
	}
}

// SetWriteTimeout sets the write timeout
func (c *Connection) SetWriteTimeout(timeout time.Duration) {
	if timeout > 0 {
		c.conn.SetWriteDeadline(time.Now().Add(timeout))
	}
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

// getSnapshotTimestamp returns the snapshot timestamp for reads
func (c *Connection) getSnapshotTimestamp() int64 {
	// For Read Committed isolation, get a new snapshot for each statement
	if c.currentTxn != nil && c.currentTxn.IsolationLevel() == txn.ReadCommitted {
		return int64(c.tsService.GetNextTimestamp())
	}
	// For other isolation levels, use the transaction's snapshot
	return int64(c.tsService.GetSnapshotTimestamp(c.currentTxn))
}

// getIsolationLevel returns the current isolation level
func (c *Connection) getIsolationLevel() txn.IsolationLevel {
	if c.currentTxn != nil {
		return c.currentTxn.IsolationLevel()
	}
	// Default to Read Committed for non-transactional queries
	return txn.ReadCommitted
}

// parseIsolationLevel parses the isolation level from a BEGIN statement
func (c *Connection) parseIsolationLevel(query string) txn.IsolationLevel {
	upperQuery := strings.ToUpper(query)

	// Check for isolation level in the query
	if strings.Contains(upperQuery, "SERIALIZABLE") {
		return txn.Serializable
	}
	if strings.Contains(upperQuery, "REPEATABLE READ") {
		return txn.RepeatableRead
	}
	if strings.Contains(upperQuery, "READ COMMITTED") {
		return txn.ReadCommitted
	}
	if strings.Contains(upperQuery, "READ UNCOMMITTED") {
		return txn.ReadUncommitted
	}

	// Default to Read Committed
	return txn.ReadCommitted
}

// handleParse handles a Parse message
func (c *Connection) handleParse(ctx context.Context, msg *protocol.Message) error {
	_ = ctx // Context might be used for cancellation in the future
	c.setState(StateBusy)

	// Parse the message
	parseMsg, err := protocol.ParseMessage(msg.Data)
	if err != nil {
		return fmt.Errorf("failed to parse Parse message: %w", err)
	}

	c.logger.Debug("Parse message received",
		"name", parseMsg.Name,
		"query", parseMsg.Query,
		"param_count", len(parseMsg.ParameterOIDs))

	// Parse the SQL query
	p := parser.NewParser(parseMsg.Query)
	stmt, err := p.Parse()
	if err != nil {
		return fmt.Errorf("parse error: %w", err)
	}

	// Infer parameter types from the query
	paramTypes, err := sql.InferParameterTypes(stmt)
	if err != nil {
		return fmt.Errorf("failed to infer parameter types: %w", err)
	}

	// Override with explicitly provided types if any
	for i, oid := range parseMsg.ParameterOIDs {
		if oid != 0 && i < len(paramTypes) {
			// TODO: Convert OID to DataType and override inferred types
			// Currently keeping inferred types until OID mapping is implemented
			_ = oid // Placeholder to avoid empty branch warning
		}
	}

	// Get field descriptions for the result
	var fieldDescs []sql.FieldDescription
	// TODO: Determine field descriptions from the statement
	// For now, we'll populate these during Bind or Execute

	// Create prepared statement
	prepStmt := &sql.PreparedStatement{
		Name:         parseMsg.Name,
		SQL:          parseMsg.Query,
		ParseTree:    stmt,
		ParamTypes:   paramTypes,
		ResultFields: fieldDescs,
		IsCacheable:  true,
	}

	// Store the prepared statement
	if err := c.extQuerySession.StorePreparedStatement(prepStmt); err != nil {
		return fmt.Errorf("failed to store prepared statement: %w", err)
	}

	// Send ParseComplete
	if c.server.config.WriteTimeout > 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.server.config.WriteTimeout))
	}
	parseComplete := &protocol.Message{
		Type: protocol.MsgParseComplete,
		Data: []byte{},
	}
	if err := protocol.WriteMessage(c.writer, parseComplete); err != nil {
		return fmt.Errorf("failed to send ParseComplete: %w", err)
	}

	return c.writer.Flush()
}

// handleBind handles a Bind message
func (c *Connection) handleBind(ctx context.Context, msg *protocol.Message) error {
	_ = ctx // Context might be used for cancellation in the future
	c.setState(StateBusy)

	// Parse the message
	bindMsg, err := protocol.ParseBind(msg.Data)
	if err != nil {
		return fmt.Errorf("failed to parse Bind message: %w", err)
	}

	c.logger.Debug("Bind message received",
		"portal", bindMsg.Portal,
		"statement", bindMsg.Statement,
		"param_count", len(bindMsg.ParameterValues))

	// Retrieve the prepared statement
	prepStmt, err := c.extQuerySession.GetPreparedStatement(bindMsg.Statement)
	if err != nil {
		return fmt.Errorf("prepared statement not found: %w", err)
	}

	// Validate parameter count
	if len(bindMsg.ParameterValues) != len(prepStmt.ParamTypes) {
		return fmt.Errorf("parameter count mismatch: expected %d, got %d",
			len(prepStmt.ParamTypes), len(bindMsg.ParameterValues))
	}

	// Parse parameter values
	paramValues := make([]types.Value, len(bindMsg.ParameterValues))
	for i, paramData := range bindMsg.ParameterValues {
		// Determine format (text or binary)
		format := int16(0) // Default to text
		if i < len(bindMsg.ParameterFormats) {
			format = bindMsg.ParameterFormats[i]
		} else if len(bindMsg.ParameterFormats) == 1 {
			// If single format specified, it applies to all parameters
			format = bindMsg.ParameterFormats[0]
		}

		// Parse the parameter value
		value, err := sql.ParseParameterValue(paramData, prepStmt.ParamTypes[i], format)
		if err != nil {
			return fmt.Errorf("failed to parse parameter %d: %w", i+1, err)
		}
		paramValues[i] = value
	}

	// Create portal
	portal := &sql.Portal{
		Name:          bindMsg.Portal,
		Statement:     prepStmt,
		ParamValues:   paramValues,
		ParamFormats:  bindMsg.ParameterFormats,
		ResultFormats: bindMsg.ResultFormats,
		CurrentRow:    0,
		IsSuspended:   false,
	}

	// Store the portal
	if err := c.extQuerySession.CreatePortal(portal); err != nil {
		return fmt.Errorf("failed to create portal: %w", err)
	}

	// Send BindComplete
	if c.server.config.WriteTimeout > 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.server.config.WriteTimeout))
	}
	bindComplete := &protocol.Message{
		Type: protocol.MsgBindComplete,
		Data: []byte{},
	}
	if err := protocol.WriteMessage(c.writer, bindComplete); err != nil {
		return fmt.Errorf("failed to send BindComplete: %w", err)
	}

	return c.writer.Flush()
}

// handleExecute handles an Execute message
func (c *Connection) handleExecute(ctx context.Context, msg *protocol.Message) error {
	_ = ctx // Context might be used for query execution in the future
	c.setState(StateBusy)

	// Parse the message
	execMsg, err := protocol.ParseExecute(msg.Data)
	if err != nil {
		return fmt.Errorf("failed to parse Execute message: %w", err)
	}

	c.logger.Debug("Execute message received",
		"portal", execMsg.Portal,
		"max_rows", execMsg.MaxRows)

	// Retrieve the portal
	portal, err := c.extQuerySession.GetPortal(execMsg.Portal)
	if err != nil {
		return fmt.Errorf("portal not found: %w", err)
	}

	// Get the prepared statement
	prepStmt := portal.Statement

	// Create a plan
	// For now, we'll re-plan the query
	// In a more optimized implementation, we could cache the plan
	planr := planner.NewBasicPlannerWithCatalog(c.catalog)
	plan, err := planr.Plan(prepStmt.ParseTree)
	if err != nil {
		return fmt.Errorf("planning error: %w", err)
	}

	// Apply parameter substitution to the plan
	if len(portal.ParamValues) > 0 {
		// Check if plan is a LogicalPlan (it should be from the planner)
		if logicalPlan, ok := plan.(planner.LogicalPlan); ok {
			substitutor := sql.NewParameterSubstitutor(portal.ParamValues)
			substitutedPlan, err := substitutor.SubstituteInPlan(logicalPlan)
			if err != nil {
				return fmt.Errorf("parameter substitution error: %w", err)
			}
			plan = substitutedPlan
		}
	}

	// Execute the query
	exec := executor.NewBasicExecutor(c.catalog, c.engine)
	if c.storage != nil {
		exec.SetStorageBackend(c.storage)
	}
	execCtx := &executor.ExecContext{
		Catalog:        c.catalog,
		Engine:         c.engine,
		TxnManager:     c.txnManager,
		Txn:            c.currentTxn,
		SnapshotTS:     c.getSnapshotTimestamp(),
		IsolationLevel: c.getIsolationLevel(),
		Params:         portal.ParamValues,
		Stats:          &executor.ExecStats{},
	}

	result, err := exec.Execute(plan, execCtx)
	if err != nil {
		// Convert executor errors to appropriate PostgreSQL errors
		errStr := err.Error()
		if strings.Contains(errStr, "division by zero") {
			return errors.DivisionByZeroError()
		}
		if strings.Contains(errStr, "type mismatch") {
			return errors.DataTypeMismatchError("unknown", "unknown")
		}
		if strings.Contains(errStr, "not null constraint") {
			return errors.NotNullViolationError("unknown", "unknown")
		}
		if strings.Contains(errStr, "unique constraint") {
			return errors.UniqueViolationError("unknown", "unknown")
		}
		if strings.Contains(errStr, "foreign key constraint") {
			return errors.ForeignKeyViolationError("unknown", "unknown")
		}
		return errors.InternalErrorf("execution error: %v", err)
	}
	defer result.Close()

	// Send row description if this is the first execute or if portal was just bound
	if portal.CurrentRow == 0 {
		schema := result.Schema()
		rowDesc := &protocol.RowDescription{
			Fields: make([]protocol.FieldDescription, len(schema.Columns)),
		}

		for i, col := range schema.Columns {
			if i >= math.MaxInt16 {
				break // Column count already validated during statement creation
			}
			// Determine format for this column
			format := int16(protocol.FormatText) // Default to text
			if i < len(portal.ResultFormats) && portal.ResultFormats[i] != 0 {
				format = portal.ResultFormats[i]
			} else if len(portal.ResultFormats) == 1 && portal.ResultFormats[0] != 0 {
				// Single format applies to all columns
				format = portal.ResultFormats[0]
			}

			rowDesc.Fields[i] = protocol.FieldDescription{
				Name:         col.Name,
				TableOID:     0,            // TODO: Add table OID
				ColumnNumber: int16(i + 1), //nolint:gosec // Bounds checked above
				DataTypeOID:  getTypeOID(col.Type),
				DataTypeSize: getTypeSize(col.Type),
				TypeModifier: -1,
				Format:       format,
			}
		}

		if c.server.config.WriteTimeout > 0 {
			c.conn.SetWriteDeadline(time.Now().Add(c.server.config.WriteTimeout))
		}
		if err := protocol.WriteMessage(c.writer, rowDesc.ToMessage()); err != nil {
			return fmt.Errorf("failed to send RowDescription: %w", err)
		}
	}

	// Send data rows
	rowCount := int32(0)
	for {
		// Check if we've reached the max rows limit
		if execMsg.MaxRows > 0 && rowCount >= execMsg.MaxRows {
			portal.IsSuspended = true
			break
		}

		row, err := result.Next()
		if err != nil {
			return err
		}
		if row == nil {
			// No more rows
			portal.IsSuspended = false
			break
		}

		dataRow := &protocol.DataRow{
			Values: make([][]byte, len(row.Values)),
		}

		for i, val := range row.Values {
			if val.IsNull() {
				dataRow.Values[i] = nil
			} else {
				// Format value based on result format
				format := int16(protocol.FormatText)
				if i < len(portal.ResultFormats) && portal.ResultFormats[i] != 0 {
					format = portal.ResultFormats[i]
				} else if len(portal.ResultFormats) == 1 && portal.ResultFormats[0] != 0 {
					format = portal.ResultFormats[0]
				}

				if format == protocol.FormatBinary {
					// TODO: Implement binary formatting
					return errors.FeatureNotSupportedError("binary result format")
				}

				dataRow.Values[i] = []byte(val.String())
			}
		}

		if c.server.config.WriteTimeout > 0 {
			c.conn.SetWriteDeadline(time.Now().Add(c.server.config.WriteTimeout))
		}
		if err := protocol.WriteMessage(c.writer, dataRow.ToMessage()); err != nil {
			return fmt.Errorf("failed to send DataRow: %w", err)
		}

		portal.CurrentRow++
		rowCount++
	}

	// Send appropriate completion message
	if portal.IsSuspended {
		// Portal suspended
		if c.server.config.WriteTimeout > 0 {
			c.conn.SetWriteDeadline(time.Now().Add(c.server.config.WriteTimeout))
		}
		suspended := &protocol.Message{
			Type: protocol.MsgPortalSuspended,
			Data: []byte{},
		}
		if err := protocol.WriteMessage(c.writer, suspended); err != nil {
			return fmt.Errorf("failed to send PortalSuspended: %w", err)
		}
	} else {
		// Command complete
		tag := getCommandTag(prepStmt.ParseTree, int(rowCount))
		complete := &protocol.CommandComplete{Tag: tag}
		if c.server.config.WriteTimeout > 0 {
			c.conn.SetWriteDeadline(time.Now().Add(c.server.config.WriteTimeout))
		}
		if err := protocol.WriteMessage(c.writer, complete.ToMessage()); err != nil {
			return fmt.Errorf("failed to send CommandComplete: %w", err)
		}

		// If portal is unnamed and execution is complete, destroy it
		if execMsg.Portal == "" {
			c.extQuerySession.ClosePortal("")
		}
	}

	return c.writer.Flush()
}

// handleDescribe handles Describe message (D)
func (c *Connection) handleDescribe(ctx context.Context, msg *protocol.Message) error {
	_ = ctx // Context might be used for cancellation in the future
	if len(msg.Data) < 1 {
		return fmt.Errorf("invalid Describe message")
	}

	// Read describe type: 'S' for statement, 'P' for portal
	describeType := msg.Data[0]
	nameBytes := msg.Data[1:]

	// Find null terminator
	nameEnd := 0
	for i, b := range nameBytes {
		if b == 0 {
			nameEnd = i
			break
		}
	}
	name := string(nameBytes[:nameEnd])

	switch describeType {
	case 'S': // Describe statement
		stmt, err := c.extQuerySession.GetPreparedStatement(name)
		if err != nil {
			// Statement not found
			return errors.InvalidStatementNameError(name)
		}

		// Send ParameterDescription
		paramOIDs := make([]uint32, len(stmt.ParamTypes))
		for i, dt := range stmt.ParamTypes {
			paramOIDs[i] = getTypeOID(dt)
		}
		paramDesc := &protocol.ParameterDescription{
			TypeOIDs: paramOIDs,
		}
		if err := protocol.WriteMessage(c.writer, paramDesc.ToMessage()); err != nil {
			return err
		}

		// Send RowDescription if statement produces results
		if statementReturnsData(stmt.ParseTree) {
			// Convert ResultFields to protocol.FieldDescription
			fields := make([]protocol.FieldDescription, len(stmt.ResultFields))
			for i, rf := range stmt.ResultFields {
				fields[i] = protocol.FieldDescription{
					Name:         rf.Name,
					TableOID:     rf.TableOID,
					ColumnNumber: rf.ColumnAttribute,
					DataTypeOID:  rf.TypeOID,
					DataTypeSize: rf.TypeSize,
					TypeModifier: rf.TypeModifier,
					Format:       rf.Format,
				}
			}
			rowDesc := &protocol.RowDescription{
				Fields: fields,
			}
			if err := protocol.WriteMessage(c.writer, rowDesc.ToMessage()); err != nil {
				return err
			}
		} else {
			// Send NoData for statements that don't return data
			noData := &protocol.NoData{}
			if err := protocol.WriteMessage(c.writer, noData.ToMessage()); err != nil {
				return err
			}
		}

	case 'P': // Describe portal
		portal, err := c.extQuerySession.GetPortal(name)
		if err != nil {
			// Portal not found
			return errors.InvalidPortalNameError(name)
		}

		// Send RowDescription if portal produces results
		if statementReturnsData(portal.Statement.ParseTree) {
			// Convert ResultFields to protocol.FieldDescription
			fields := make([]protocol.FieldDescription, len(portal.Statement.ResultFields))
			for i, rf := range portal.Statement.ResultFields {
				fields[i] = protocol.FieldDescription{
					Name:         rf.Name,
					TableOID:     rf.TableOID,
					ColumnNumber: rf.ColumnAttribute,
					DataTypeOID:  rf.TypeOID,
					DataTypeSize: rf.TypeSize,
					TypeModifier: rf.TypeModifier,
					Format:       rf.Format,
				}
			}
			rowDesc := &protocol.RowDescription{
				Fields: fields,
			}
			if err := protocol.WriteMessage(c.writer, rowDesc.ToMessage()); err != nil {
				return err
			}
		} else {
			// Send NoData for statements that don't return data
			noData := &protocol.NoData{}
			if err := protocol.WriteMessage(c.writer, noData.ToMessage()); err != nil {
				return err
			}
		}

	default:
		return fmt.Errorf("invalid describe type: %c", describeType)
	}

	return c.writer.Flush()
}

// handleClose handles Close message (C)
func (c *Connection) handleClose(ctx context.Context, msg *protocol.Message) error {
	_ = ctx // Context might be used for cancellation in the future
	if len(msg.Data) < 1 {
		return fmt.Errorf("invalid Close message")
	}

	// Read close type: 'S' for statement, 'P' for portal
	closeType := msg.Data[0]
	nameBytes := msg.Data[1:]

	// Find null terminator
	nameEnd := 0
	for i, b := range nameBytes {
		if b == 0 {
			nameEnd = i
			break
		}
	}
	name := string(nameBytes[:nameEnd])

	switch closeType {
	case 'S': // Close statement
		c.extQuerySession.CloseStatement(name)
	case 'P': // Close portal
		c.extQuerySession.ClosePortal(name)
	default:
		return fmt.Errorf("invalid close type: %c", closeType)
	}

	// Send CloseComplete
	closeComplete := &protocol.CloseComplete{}
	if err := protocol.WriteMessage(c.writer, closeComplete.ToMessage()); err != nil {
		return err
	}

	return c.writer.Flush()
}

func (c *Connection) handleSync(ctx context.Context) error {
	_ = ctx // Context might be used for cancellation in the future
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
	if size > math.MaxInt16 {
		return math.MaxInt16 // Cap at max int16
	}
	return int16(size) //nolint:gosec // Bounds checked above
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

// statementReturnsData returns true if the statement returns result rows
func statementReturnsData(stmt parser.Statement) bool {
	switch stmt.(type) {
	case *parser.SelectStmt:
		return true
	default:
		return false
	}
}

