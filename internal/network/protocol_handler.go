package network

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/dshills/QuantaDB/internal/log"
	"github.com/dshills/QuantaDB/internal/network/protocol"
)

// BasicProtocolHandler implements ProtocolHandler interface.
type BasicProtocolHandler struct {
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer
	logger log.Logger
	server *Server

	// Component handlers
	authHandler       AuthenticationHandler
	queryExecutor     QueryExecutor
	transactionMgr    TransactionManager
	extQueryHandler   ExtendedQueryHandler
	resultFormatter   ResultFormatter
}

// NewBasicProtocolHandler creates a new protocol handler.
func NewBasicProtocolHandler(
	conn net.Conn,
	logger log.Logger,
	server *Server,
) *BasicProtocolHandler {
	return &BasicProtocolHandler{
		conn:   conn,
		reader: bufio.NewReader(conn),
		writer: bufio.NewWriter(conn),
		logger: logger,
		server: server,
	}
}

// SetHandlers sets the component handlers for delegation.
func (ph *BasicProtocolHandler) SetHandlers(
	authHandler AuthenticationHandler,
	queryExecutor QueryExecutor,
	transactionMgr TransactionManager,
	extQueryHandler ExtendedQueryHandler,
	resultFormatter ResultFormatter,
) {
	ph.authHandler = authHandler
	ph.queryExecutor = queryExecutor
	ph.transactionMgr = transactionMgr
	ph.extQueryHandler = extQueryHandler
	ph.resultFormatter = resultFormatter
}

// HandleMessage routes messages to appropriate handlers.
func (ph *BasicProtocolHandler) HandleMessage(ctx context.Context, msg *protocol.Message, connCtx *ConnectionContext) error {
	switch msg.Type {
	case protocol.MsgQuery:
		return ph.queryExecutor.HandleQuery(ctx, msg, connCtx)

	case protocol.MsgParse:
		return ph.extQueryHandler.HandleParse(ctx, msg, connCtx)

	case protocol.MsgBind:
		return ph.extQueryHandler.HandleBind(ctx, msg, connCtx)

	case protocol.MsgExecute:
		return ph.extQueryHandler.HandleExecute(ctx, msg, connCtx)

	case protocol.MsgDescribe:
		return ph.extQueryHandler.HandleDescribe(ctx, msg, connCtx)

	case protocol.MsgClose:
		return ph.extQueryHandler.HandleClose(ctx, msg, connCtx)

	case protocol.MsgSync:
		return ph.extQueryHandler.HandleSync(ctx, connCtx)

	case protocol.MsgTerminate:
		ph.logger.Info("Client requested termination")
		return io.EOF

	default:
		return fmt.Errorf("unsupported message type: %c", msg.Type)
	}
}

// SendError sends an error response to the client.
func (ph *BasicProtocolHandler) SendError(err error) error {
	// Convert internal error to PostgreSQL error response
	var sqlState string
	var severity string

	// For now, use generic error mapping
	// TODO: Implement proper error type checking
	sqlState = "42000" // syntax_error_or_access_rule_violation
	severity = "ERROR"

	errorResp := &protocol.ErrorResponse{
		Severity: severity,
		Code:     sqlState,
		Message:  err.Error(),
	}

	// Set write deadline if configured
	if ph.server.config.WriteTimeout > 0 {
		ph.conn.SetWriteDeadline(time.Now().Add(ph.server.config.WriteTimeout))
	}

	if err := protocol.WriteMessage(ph.writer, errorResp.ToMessage()); err != nil {
		return fmt.Errorf("failed to send error response: %w", err)
	}

	return ph.writer.Flush()
}

// SendReadyForQuery sends a ReadyForQuery message with transaction status.
func (ph *BasicProtocolHandler) SendReadyForQuery(txnStatus byte) error {
	// Set write deadline if configured
	if ph.server.config.WriteTimeout > 0 {
		ph.conn.SetWriteDeadline(time.Now().Add(ph.server.config.WriteTimeout))
	}

	ready := &protocol.ReadyForQuery{
		Status: txnStatus,
	}

	if err := protocol.WriteMessage(ph.writer, ready.ToMessage()); err != nil {
		return fmt.Errorf("failed to send ready for query: %w", err)
	}

	return ph.writer.Flush()
}

// SetReadTimeout sets the read timeout for the connection.
func (ph *BasicProtocolHandler) SetReadTimeout(timeout time.Duration) {
	if timeout > 0 {
		ph.conn.SetReadDeadline(time.Now().Add(timeout))
	}
}

// SetWriteTimeout sets the write timeout for the connection.
func (ph *BasicProtocolHandler) SetWriteTimeout(timeout time.Duration) {
	if timeout > 0 {
		ph.conn.SetWriteDeadline(time.Now().Add(timeout))
	}
}

// Close closes the connection and cleans up resources.
func (ph *BasicProtocolHandler) Close() error {
	// Flush any pending writes
	if ph.writer != nil {
		ph.writer.Flush()
	}

	// Close the network connection
	return ph.conn.Close()
}

// GetReader returns the buffered reader for reading protocol messages.
func (ph *BasicProtocolHandler) GetReader() *bufio.Reader {
	return ph.reader
}

// GetWriter returns the buffered writer for writing protocol responses.
func (ph *BasicProtocolHandler) GetWriter() *bufio.Writer {
	return ph.writer
}