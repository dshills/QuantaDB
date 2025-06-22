package network

import (
	"context"
	"io"
	"net"
	"time"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/engine"
	"github.com/dshills/QuantaDB/internal/log"
	"github.com/dshills/QuantaDB/internal/network/protocol"
	"github.com/dshills/QuantaDB/internal/sql/executor"
	"github.com/dshills/QuantaDB/internal/txn"
)

// RefactoredConnection represents a client connection using the new component-based architecture.
type RefactoredConnection struct {
	id      uint32
	conn    net.Conn
	server  *Server
	logger  log.Logger
	context *ConnectionContext

	// Component handlers
	protocolHandler   ProtocolHandler
	authHandler       AuthenticationHandler
	queryExecutor     QueryExecutor
	transactionMgr    TransactionManager
	extQueryHandler   ExtendedQueryHandler
	resultFormatter   ResultFormatter
}

// NewRefactoredConnection creates a new refactored connection.
func NewRefactoredConnection(
	id uint32,
	conn net.Conn,
	server *Server,
	catalog catalog.Catalog,
	engine engine.Engine,
	storage executor.StorageBackend,
	indexMgr interface{},
	txnManager *txn.Manager,
	tsService *txn.TimestampService,
	logger log.Logger,
) *RefactoredConnection {
	// Create connection context
	connCtx := &ConnectionContext{
		ID:               id,
		Catalog:          catalog,
		Engine:           engine,
		Storage:          storage,
		IndexMgr:         indexMgr,
		TxnManager:       txnManager,
		TimestampService: tsService,
		Params:           make(map[string]string),
		SecretKey:        0, // Will be generated during startup
		CurrentTxn:       nil,
		ExtQuerySession:  NewExtendedQuerySession(),
	}

	// Create protocol handler
	protocolHandler := NewBasicProtocolHandler(conn, logger, server)

	// Create component handlers
	authHandler := NewBasicAuthenticationHandler(protocolHandler, logger, server)
	resultFormatter := NewBasicResultFormatter(protocolHandler)
	transactionMgr := NewBasicTransactionManager(protocolHandler)
	queryExecutor := NewBasicQueryExecutor(protocolHandler, transactionMgr, resultFormatter)
	extQueryHandler := NewBasicExtendedQueryHandler(protocolHandler, queryExecutor, resultFormatter)

	// Wire up the protocol handler with all components
	protocolHandler.SetHandlers(
		authHandler,
		queryExecutor,
		transactionMgr,
		extQueryHandler,
		resultFormatter,
	)

	return &RefactoredConnection{
		id:                id,
		conn:              conn,
		server:            server,
		logger:            logger,
		context:           connCtx,
		protocolHandler:   protocolHandler,
		authHandler:       authHandler,
		queryExecutor:     queryExecutor,
		transactionMgr:    transactionMgr,
		extQueryHandler:   extQueryHandler,
		resultFormatter:   resultFormatter,
	}
}

// Handle handles the connection lifecycle using the new component architecture.
func (rc *RefactoredConnection) Handle(ctx context.Context) error {
	rc.logger.Debug("New refactored connection established",
		"local_addr", rc.conn.LocalAddr(),
		"remote_addr", rc.conn.RemoteAddr())

	// Set initial read timeout
	if rc.server.config.ReadTimeout > 0 {
		rc.protocolHandler.SetReadTimeout(rc.server.config.ReadTimeout)
	}

	// Handle startup phase
	if err := rc.authHandler.HandleStartup(rc.context); err != nil {
		rc.logger.Error("Startup phase failed", "error", err)
		rc.protocolHandler.SendError(err)
		return err
	}

	// Handle authentication phase
	if err := rc.authHandler.HandleAuthentication(rc.context); err != nil {
		rc.logger.Error("Authentication phase failed", "error", err)
		rc.protocolHandler.SendError(err)
		return err
	}

	// Main message loop
	reader := rc.protocolHandler.(*BasicProtocolHandler).GetReader()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Reset read deadline for each message
			if rc.server.config.ReadTimeout > 0 {
				rc.protocolHandler.SetReadTimeout(rc.server.config.ReadTimeout)
			}

			// Read next message
			msg, err := protocol.ReadMessage(reader)
			if err != nil {
				if err == io.EOF {
					rc.logger.Info("Client disconnected")
					return nil
				}
				rc.logger.Error("Failed to read message", "error", err)
				return err
			}

			// Update state to busy
			rc.authHandler.SetState(StateBusy)

			// Handle the message
			if err := rc.protocolHandler.HandleMessage(ctx, msg, rc.context); err != nil {
				rc.logger.Error("Failed to handle message", "error", err, "type", string(msg.Type))
				rc.protocolHandler.SendError(err)
				// Continue processing other messages unless it's a fatal error
				if err == io.EOF {
					return err
				}
			}

			// Update state back to ready
			rc.authHandler.SetState(StateReady)
		}
	}
}

// Close closes the connection and cleans up resources.
func (rc *RefactoredConnection) Close() error {
	// Rollback any active transaction
	if rc.context.CurrentTxn != nil {
		rc.transactionMgr.HandleRollback(context.Background(), rc.context)
	}

	// Close the protocol handler (which closes the network connection)
	return rc.protocolHandler.Close()
}

// SetReadTimeout sets the read timeout.
func (rc *RefactoredConnection) SetReadTimeout(timeout time.Duration) {
	rc.protocolHandler.SetReadTimeout(timeout)
}

// SetWriteTimeout sets the write timeout.
func (rc *RefactoredConnection) SetWriteTimeout(timeout time.Duration) {
	rc.protocolHandler.SetWriteTimeout(timeout)
}