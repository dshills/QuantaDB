package network

import (
	"context"
	"fmt"
	"strings"

	"github.com/dshills/QuantaDB/internal/txn"
)

// BasicTransactionManager implements TransactionManager interface.
type BasicTransactionManager struct {
	protocolHandler ProtocolHandler
}

// NewBasicTransactionManager creates a new transaction manager.
func NewBasicTransactionManager(protocolHandler ProtocolHandler) *BasicTransactionManager {
	return &BasicTransactionManager{
		protocolHandler: protocolHandler,
	}
}

// HandleBegin starts a new transaction.
func (tm *BasicTransactionManager) HandleBegin(ctx context.Context, query string, connCtx *ConnectionContext) error {
	// Parse isolation level if specified
	isolationLevel := tm.parseIsolationLevel(query)

	// Begin new transaction
	txn, err := connCtx.TxnManager.BeginTransaction(context.Background(), isolationLevel)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Set current transaction
	connCtx.CurrentTxn = txn

	// Send command completion
	return tm.protocolHandler.SendReadyForQuery('T') // T = in transaction
}

// HandleCommit commits the current transaction.
func (tm *BasicTransactionManager) HandleCommit(ctx context.Context, connCtx *ConnectionContext) error {
	return tm.EndTransaction(true, "COMMIT", connCtx)
}

// HandleRollback rolls back the current transaction.
func (tm *BasicTransactionManager) HandleRollback(ctx context.Context, connCtx *ConnectionContext) error {
	return tm.EndTransaction(false, "ROLLBACK", connCtx)
}

// EndTransaction completes the current transaction.
func (tm *BasicTransactionManager) EndTransaction(commit bool, tag string, connCtx *ConnectionContext) error {
	if connCtx.CurrentTxn == nil {
		// No transaction active - send warning but continue
		return tm.protocolHandler.SendReadyForQuery('I') // I = idle (no transaction)
	}

	var err error
	if commit {
		err = connCtx.TxnManager.CommitTransaction(connCtx.CurrentTxn)
		if err != nil {
			// If commit fails, try to rollback
			rollbackErr := connCtx.TxnManager.AbortTransaction(connCtx.CurrentTxn)
			if rollbackErr != nil {
				// Log rollback error but return original commit error
				return fmt.Errorf("commit failed: %w (rollback also failed: %v)", err, rollbackErr)
			}
			return fmt.Errorf("commit failed: %w", err)
		}
	} else {
		err = connCtx.TxnManager.AbortTransaction(connCtx.CurrentTxn)
		if err != nil {
			return fmt.Errorf("rollback failed: %w", err)
		}
	}

	// Clear current transaction
	connCtx.CurrentTxn = nil

	// Send ready for query with idle status
	return tm.protocolHandler.SendReadyForQuery('I') // I = idle (no transaction)
}

// parseIsolationLevel extracts isolation level from BEGIN statement.
func (tm *BasicTransactionManager) parseIsolationLevel(query string) txn.IsolationLevel {
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
