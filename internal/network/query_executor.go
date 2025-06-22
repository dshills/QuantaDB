package network

import (
	"context"
	"fmt"
	"strings"

	"github.com/dshills/QuantaDB/internal/network/protocol"
	"github.com/dshills/QuantaDB/internal/sql/executor"
	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/txn"
)

// BasicQueryExecutor implements QueryExecutor interface.
type BasicQueryExecutor struct {
	protocolHandler ProtocolHandler
	transactionMgr  TransactionManager
	resultFormatter ResultFormatter
}

// NewBasicQueryExecutor creates a new query executor.
func NewBasicQueryExecutor(
	protocolHandler ProtocolHandler,
	transactionMgr TransactionManager,
	resultFormatter ResultFormatter,
) *BasicQueryExecutor {
	return &BasicQueryExecutor{
		protocolHandler: protocolHandler,
		transactionMgr:  transactionMgr,
		resultFormatter: resultFormatter,
	}
}

// HandleQuery processes a simple query message.
func (qe *BasicQueryExecutor) HandleQuery(ctx context.Context, msg *protocol.Message, connCtx *ConnectionContext) error {
	// Parse query from message
	q := &protocol.Query{}
	if err := q.Parse(msg.Data); err != nil {
		return fmt.Errorf("failed to parse query: %w", err)
	}

	query := q.Query

	// Handle special transaction commands
	upperQuery := strings.ToUpper(strings.TrimSpace(query))
	switch {
	case strings.HasPrefix(upperQuery, "BEGIN"):
		return qe.transactionMgr.HandleBegin(ctx, query, connCtx)
	case upperQuery == "COMMIT":
		return qe.transactionMgr.HandleCommit(ctx, connCtx)
	case upperQuery == "ROLLBACK":
		return qe.transactionMgr.HandleRollback(ctx, connCtx)
	}

	// Handle empty query (PostgreSQL compatibility)
	if strings.TrimSpace(query) == "" || query == ";" {
		// Send empty query response
		return qe.protocolHandler.SendReadyForQuery(qe.getTransactionStatus(connCtx))
	}

	// Parse SQL statement
	p := parser.NewParser(query)
	stmt, err := p.Parse()
	if err != nil {
		return fmt.Errorf("failed to parse SQL: %w", err)
	}

	// Create planner
	plnr := planner.NewBasicPlannerWithCatalog(connCtx.Catalog)

	// Plan the statement
	plan, err := plnr.Plan(stmt)
	if err != nil {
		return fmt.Errorf("failed to plan query: %w", err)
	}

	// Create executor
	exec := executor.NewBasicExecutor(connCtx.Catalog, connCtx.Engine)
	exec.SetStorageBackend(connCtx.Storage)
	if connCtx.IndexMgr != nil {
		exec.SetIndexManager(connCtx.IndexMgr)
	}

	// Create execution context
	execCtx := &executor.ExecContext{
		Catalog:        connCtx.Catalog,
		Engine:         connCtx.Engine,
		TxnManager:     connCtx.TxnManager,
		Txn:            connCtx.CurrentTxn,
		SnapshotTS:     qe.GetSnapshotTimestamp(connCtx),
		IsolationLevel: qe.GetIsolationLevel(connCtx),
		Stats:          &executor.ExecStats{},
	}

	// Execute the plan
	result, err := exec.Execute(plan, execCtx)
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}
	defer result.Close()

	// Send results to client
	if err := qe.resultFormatter.SendResults(result, stmt, connCtx); err != nil {
		return fmt.Errorf("failed to send results: %w", err)
	}

	// Send ready for query
	return qe.protocolHandler.SendReadyForQuery(qe.getTransactionStatus(connCtx))
}

// GetSnapshotTimestamp returns the snapshot timestamp for MVCC reads.
func (qe *BasicQueryExecutor) GetSnapshotTimestamp(connCtx *ConnectionContext) int64 {
	if connCtx.CurrentTxn != nil {
		return int64(connCtx.CurrentTxn.ReadTimestamp())
	}
	// For autocommit queries, get next timestamp
	return int64(connCtx.TimestampService.GetNextTimestamp())
}

// GetIsolationLevel returns the current isolation level.
func (qe *BasicQueryExecutor) GetIsolationLevel(connCtx *ConnectionContext) txn.IsolationLevel {
	if connCtx.CurrentTxn != nil {
		return connCtx.CurrentTxn.IsolationLevel()
	}
	// Default isolation level for autocommit
	return txn.ReadCommitted
}

// ParseIsolationLevel extracts isolation level from SQL string.
func (qe *BasicQueryExecutor) ParseIsolationLevel(query string) txn.IsolationLevel {
	upperQuery := strings.ToUpper(strings.TrimSpace(query))

	if strings.Contains(upperQuery, "READ UNCOMMITTED") {
		return txn.ReadUncommitted
	} else if strings.Contains(upperQuery, "READ COMMITTED") {
		return txn.ReadCommitted
	} else if strings.Contains(upperQuery, "REPEATABLE READ") {
		return txn.RepeatableRead
	} else if strings.Contains(upperQuery, "SERIALIZABLE") {
		return txn.Serializable
	}

	// Default isolation level
	return txn.ReadCommitted
}

// getTransactionStatus returns the current transaction status byte for ReadyForQuery.
func (qe *BasicQueryExecutor) getTransactionStatus(connCtx *ConnectionContext) byte {
	if connCtx.CurrentTxn != nil {
		// Check if transaction is in error state
		// For now, assume no error state tracking
		return 'T' // T = in transaction
	}
	return 'I' // I = idle (no transaction)
}
