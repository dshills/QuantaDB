package network

import (
	"bufio"
	"context"
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

// Connection represents a client connection
type Connection struct {
	id         uint32
	conn       net.Conn
	reader     *bufio.Reader
	writer     *bufio.Writer
	server     *Server
	catalog    catalog.Catalog
	engine     engine.Engine
	txnManager *txn.Manager
	logger     log.Logger

	// Connection state
	state  int
	params map[string]string

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

// Handle handles the connection lifecycle
func (c *Connection) Handle(ctx context.Context) error {
	c.reader = bufio.NewReader(c.conn)
	c.writer = bufio.NewWriter(c.conn)
	c.preparedStmts = make(map[string]*PreparedStatement)

	// Handle startup
	if err := c.handleStartup(); err != nil {
		return fmt.Errorf("startup failed: %w", err)
	}

	// Handle authentication
	if err := c.handleAuthentication(); err != nil {
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
			msg, err := protocol.ReadMessage(c.reader)
			if err != nil {
				if err == io.EOF {
					c.logger.Info("Client disconnected")
					return nil
				}
				return fmt.Errorf("failed to read message: %w", err)
			}

			if err := c.handleMessage(ctx, msg); err != nil {
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
	// Read startup message
	params, err := protocol.ReadStartupMessage(c.reader)
	if err != nil {
		return err
	}

	c.params = params
	c.logger.Info("Client connected", 
		"user", params["user"],
		"database", params["database"],
		"application", params["application_name"])

	// Send authentication request
	auth := &protocol.Authentication{
		Type: protocol.AuthOK, // No authentication for now
	}
	if err := protocol.WriteMessage(c.writer, auth.ToMessage()); err != nil {
		return err
	}

	// Send parameter status messages
	parameters := map[string]string{
		"server_version":    "15.0 (QuantaDB 0.1.0)",
		"server_encoding":   "UTF8",
		"client_encoding":   "UTF8",
		"DateStyle":         "ISO, MDY",
		"integer_datetimes": "on",
		"TimeZone":          "UTC",
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

	// Send backend key data
	keyData := &protocol.BackendKeyData{
		ProcessID: c.id,
		SecretKey: c.id * 12345, // Simple secret key generation
	}
	if err := protocol.WriteMessage(c.writer, keyData.ToMessage()); err != nil {
		return err
	}

	return c.writer.Flush()
}

// handleAuthentication handles authentication
func (c *Connection) handleAuthentication() error {
	// For now, we accept all connections
	c.state = StateReady
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
		c.state = StateClosed
		return nil
	default:
		return fmt.Errorf("unsupported message type: %c", msg.Type)
	}
}

// handleQuery handles a simple query
func (c *Connection) handleQuery(ctx context.Context, msg *protocol.Message) error {
	c.state = StateBusy

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

	// Send command complete
	complete := &protocol.CommandComplete{Tag: "ROLLBACK"}
	if err := protocol.WriteMessage(c.writer, complete.ToMessage()); err != nil {
		return err
	}

	return c.sendReadyForQuery()
}

// sendResults sends query results to the client
func (c *Connection) sendResults(result executor.Result, stmt parser.Statement) error {
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
	status := byte(protocol.TxnStatusIdle)
	if c.currentTxn != nil {
		status = byte(protocol.TxnStatusInBlock)
	}

	ready := &protocol.ReadyForQuery{Status: status}
	if err := protocol.WriteMessage(c.writer, ready.ToMessage()); err != nil {
		return err
	}

	c.state = StateReady
	return c.writer.Flush()
}

// sendError sends an error response
func (c *Connection) sendError(err error) error {
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
	c.state = StateClosed
	
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