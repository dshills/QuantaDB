package txn

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dshills/QuantaDB/internal/engine"
)

// Manager manages transactions with MVCC support.
type Manager struct {
	mu            sync.RWMutex
	activeTxns    map[TransactionID]*MvccTransaction
	committedTxns map[TransactionID]*TransactionInfo
	engine        engine.Engine
	gcThreshold   time.Duration
	maxActiveTime time.Duration
}

// ManagerOptions contains configuration for the transaction manager.
type ManagerOptions struct {
	// GCThreshold is how long to keep committed transaction info for garbage collection.
	GCThreshold time.Duration
	// MaxActiveTime is the maximum time a transaction can be active.
	MaxActiveTime time.Duration
}

// NewManager creates a new transaction manager.
func NewManager(eng engine.Engine, opts *ManagerOptions) *Manager {
	if opts == nil {
		opts = &ManagerOptions{
			GCThreshold:   time.Hour,
			MaxActiveTime: time.Minute * 30,
		}
	}

	return &Manager{
		activeTxns:    make(map[TransactionID]*MvccTransaction),
		committedTxns: make(map[TransactionID]*TransactionInfo),
		engine:        eng,
		gcThreshold:   opts.GCThreshold,
		maxActiveTime: opts.MaxActiveTime,
	}
}

// BeginTransaction starts a new transaction with the specified isolation level.
func (m *Manager) BeginTransaction(ctx context.Context, isolation IsolationLevel) (*MvccTransaction, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	txnID := NextTransactionID()
	readTS := NextTimestamp()

	// For serializable isolation, use the same timestamp for reads and writes
	writeTS := readTS
	if isolation != Serializable {
		writeTS = NextTimestamp()
	}

	info := &TransactionInfo{
		ID:             txnID,
		StartTime:      time.Now(),
		Status:         Active,
		IsolationLevel: isolation,
		ReadTimestamp:  readTS,
		WriteTimestamp: writeTS,
	}

	// Start underlying engine transaction
	engineTxn, err := m.engine.BeginTransaction(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to start engine transaction: %w", err)
	}

	txn := &MvccTransaction{
		manager:   m,
		info:      info,
		engineTxn: engineTxn,
		readSet:   make(map[string]Timestamp),
		writeSet:  make(map[string]*VersionedValue),
		closed:    false,
	}

	m.activeTxns[txnID] = txn
	return txn, nil
}

// GetActiveTxns returns a map of currently active transaction IDs.
func (m *Manager) GetActiveTxns() map[TransactionID]bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	active := make(map[TransactionID]bool)
	for txnID := range m.activeTxns {
		active[txnID] = true
	}
	return active
}

// CommitTransaction commits a transaction.
func (m *Manager) CommitTransaction(txn *MvccTransaction) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Validate transaction can commit
	if err := m.validateCommit(txn); err != nil {
		return err
	}

	// Commit the underlying engine transaction
	if err := txn.engineTxn.Commit(); err != nil {
		return fmt.Errorf("engine commit failed: %w", err)
	}

	// Update transaction info
	txn.info.Status = Committed
	txn.info.CommitTime = time.Now()
	txn.closed = true

	// Move to committed transactions
	m.committedTxns[txn.info.ID] = txn.info
	delete(m.activeTxns, txn.info.ID)

	return nil
}

// AbortTransaction aborts a transaction.
func (m *Manager) AbortTransaction(txn *MvccTransaction) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Rollback the underlying engine transaction
	if err := txn.engineTxn.Rollback(); err != nil {
		return fmt.Errorf("engine rollback failed: %w", err)
	}

	// Update transaction info
	txn.info.Status = Aborted
	txn.closed = true

	// Remove from active transactions
	delete(m.activeTxns, txn.info.ID)

	return nil
}

// validateCommit performs validation checks before committing.
func (m *Manager) validateCommit(txn *MvccTransaction) error {
	// Check for write-write conflicts for serializable isolation
	if txn.info.IsolationLevel == Serializable {
		for key := range txn.writeSet {
			// Check if any other active transaction has written to this key
			// with a timestamp between our read and write timestamps
			for _, otherTxn := range m.activeTxns {
				if otherTxn.info.ID == txn.info.ID {
					continue
				}
				if otherTxn.hasWritten(key) {
					return engine.ErrTransactionConflict
				}
			}
		}
	}

	// Check transaction timeout
	if time.Since(txn.info.StartTime) > m.maxActiveTime {
		return fmt.Errorf("transaction timeout")
	}

	return nil
}

// GarbageCollect removes old committed transaction info and unreachable versions.
func (m *Manager) GarbageCollect() {
	m.mu.Lock()
	defer m.mu.Unlock()

	cutoff := time.Now().Add(-m.gcThreshold)

	// Remove old committed transactions
	for txnID, info := range m.committedTxns {
		if info.CommitTime.Before(cutoff) {
			delete(m.committedTxns, txnID)
		}
	}
}

// GetTransactionInfo returns information about a transaction.
func (m *Manager) GetTransactionInfo(txnID TransactionID) (*TransactionInfo, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if txn, exists := m.activeTxns[txnID]; exists {
		return txn.info, true
	}

	if info, exists := m.committedTxns[txnID]; exists {
		return info, true
	}

	return nil, false
}

// Stats returns transaction manager statistics.
func (m *Manager) Stats() ManagerStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return ManagerStats{
		ActiveTransactions:    len(m.activeTxns),
		CommittedTransactions: len(m.committedTxns),
	}
}

// ManagerStats contains transaction manager statistics.
type ManagerStats struct {
	ActiveTransactions    int
	CommittedTransactions int
}
