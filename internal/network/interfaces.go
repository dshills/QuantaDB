package network

import (
	"context"
	"time"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/engine"
	"github.com/dshills/QuantaDB/internal/network/protocol"
	"github.com/dshills/QuantaDB/internal/sql/executor"
	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/txn"
)

// ConnectionContext provides shared state and dependencies for connection components.
type ConnectionContext struct {
	ID               uint32
	Catalog          catalog.Catalog
	Engine           engine.Engine
	Storage          executor.StorageBackend
	IndexMgr         interface{} // Index manager interface
	TxnManager       *txn.Manager
	TimestampService *txn.TimestampService
	Params           map[string]string
	SecretKey        uint32
	CurrentTxn       *txn.MvccTransaction
	ExtQuerySession  *ExtendedQuerySession
}

// ProtocolHandler handles low-level protocol message reading/writing.
type ProtocolHandler interface {
	// Message routing and dispatching
	HandleMessage(ctx context.Context, msg *protocol.Message, connCtx *ConnectionContext) error

	// Error and status messages
	SendError(err error) error
	SendReadyForQuery(txnStatus byte) error

	// Network configuration
	SetReadTimeout(timeout time.Duration)
	SetWriteTimeout(timeout time.Duration)
	Close() error
}

// AuthenticationHandler handles connection startup and authentication.
type AuthenticationHandler interface {
	// Connection lifecycle
	HandleStartup(connCtx *ConnectionContext) error
	HandleAuthentication(connCtx *ConnectionContext) error

	// State management
	SetState(state int)
	GetState() int
}

// QueryExecutor handles SQL query processing and execution.
type QueryExecutor interface {
	// Simple query protocol
	HandleQuery(ctx context.Context, msg *protocol.Message, connCtx *ConnectionContext) error

	// Transaction helpers
	GetSnapshotTimestamp(connCtx *ConnectionContext) int64
	GetIsolationLevel(connCtx *ConnectionContext) txn.IsolationLevel
	ParseIsolationLevel(query string) txn.IsolationLevel
}

// TransactionManager handles transaction lifecycle management.
type TransactionManager interface {
	// Transaction commands
	HandleBegin(ctx context.Context, query string, connCtx *ConnectionContext) error
	HandleCommit(ctx context.Context, connCtx *ConnectionContext) error
	HandleRollback(ctx context.Context, connCtx *ConnectionContext) error

	// Transaction completion
	EndTransaction(commit bool, tag string, connCtx *ConnectionContext) error
}

// ExtendedQueryHandler handles Parse/Bind/Execute protocol flow.
type ExtendedQueryHandler interface {
	// Extended query protocol messages
	HandleParse(ctx context.Context, msg *protocol.Message, connCtx *ConnectionContext) error
	HandleBind(ctx context.Context, msg *protocol.Message, connCtx *ConnectionContext) error
	HandleExecute(ctx context.Context, msg *protocol.Message, connCtx *ConnectionContext) error
	HandleDescribe(ctx context.Context, msg *protocol.Message, connCtx *ConnectionContext) error
	HandleClose(ctx context.Context, msg *protocol.Message, connCtx *ConnectionContext) error
	HandleSync(ctx context.Context, connCtx *ConnectionContext) error
}

// ResultFormatter handles converting executor results to protocol format.
type ResultFormatter interface {
	// Result formatting and transmission
	SendResults(result executor.Result, stmt parser.Statement, connCtx *ConnectionContext) error

	// Helper methods for result formatting
	GetTypeOID(dataType string) uint32
	GetTypeSize(dataType string) int16
	GetCommandTag(stmt parser.Statement, rowsAffected int64) string
	StatementReturnsData(stmt parser.Statement) bool
}
