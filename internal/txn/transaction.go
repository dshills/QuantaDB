package txn

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/dshills/QuantaDB/internal/engine"
)

// MvccTransaction represents a transaction with MVCC support.
type MvccTransaction struct {
	manager   *Manager
	info      *TransactionInfo
	engineTxn engine.Transaction
	mu        sync.RWMutex
	readSet   map[string]Timestamp       // key -> timestamp when read
	writeSet  map[string]*VersionedValue // key -> versioned value written
	closed    bool
}

// ID returns the transaction ID.
func (txn *MvccTransaction) ID() TransactionID {
	return txn.info.ID
}

// IsolationLevel returns the transaction's isolation level.
func (txn *MvccTransaction) IsolationLevel() IsolationLevel {
	return txn.info.IsolationLevel
}

// ReadTimestamp returns the transaction's read timestamp.
func (txn *MvccTransaction) ReadTimestamp() Timestamp {
	return txn.info.ReadTimestamp
}

// WriteTimestamp returns the transaction's write timestamp.
func (txn *MvccTransaction) WriteTimestamp() Timestamp {
	return txn.info.WriteTimestamp
}

// Get retrieves a value with MVCC visibility rules.
func (txn *MvccTransaction) Get(key []byte) ([]byte, error) {
	txn.mu.RLock()
	defer txn.mu.RUnlock()

	if txn.closed {
		return nil, engine.ErrTransactionClosed
	}

	keyStr := string(key)

	// Check write set first for read-your-writes consistency
	if value, exists := txn.writeSet[keyStr]; exists {
		if value.IsDeleted() {
			return nil, engine.ErrKeyNotFound
		}
		result := make([]byte, len(value.Value))
		copy(result, value.Value)
		return result, nil
	}

	// Read from storage and apply MVCC visibility
	value, err := txn.readWithMVCC(key)
	if err != nil {
		return nil, err
	}

	// Track read for isolation level enforcement
	txn.trackRead(keyStr)

	return value, nil
}

// Put stores a key-value pair with versioning.
func (txn *MvccTransaction) Put(key, value []byte) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.closed {
		return engine.ErrTransactionClosed
	}

	keyStr := string(key)

	// Create versioned value
	versionedValue := &VersionedValue{
		Value:        make([]byte, len(value)),
		CreatedByTxn: txn.info.ID,
		CreatedAt:    txn.info.WriteTimestamp,
	}
	copy(versionedValue.Value, value)

	// Store in write set
	txn.writeSet[keyStr] = versionedValue

	// Serialize and store in engine transaction
	serialized, err := txn.serializeVersionedValue(versionedValue)
	if err != nil {
		return fmt.Errorf("failed to serialize versioned value: %w", err)
	}

	return txn.engineTxn.Put(key, serialized)
}

// Delete marks a key as deleted with versioning.
func (txn *MvccTransaction) Delete(key []byte) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.closed {
		return engine.ErrTransactionClosed
	}

	keyStr := string(key)

	// Create a deletion marker
	versionedValue := &VersionedValue{
		CreatedByTxn: txn.info.ID,
		CreatedAt:    txn.info.WriteTimestamp,
		DeletedByTxn: txn.info.ID,
		DeletedAt:    txn.info.WriteTimestamp,
	}

	// Store in write set
	txn.writeSet[keyStr] = versionedValue

	// Mark as deleted in engine transaction
	return txn.engineTxn.Delete(key)
}

// Commit commits the transaction.
func (txn *MvccTransaction) Commit() error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.closed {
		return engine.ErrTransactionClosed
	}

	return txn.manager.CommitTransaction(txn)
}

// Rollback rolls back the transaction.
func (txn *MvccTransaction) Rollback() error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.closed {
		return engine.ErrTransactionClosed
	}

	return txn.manager.AbortTransaction(txn)
}

// readWithMVCC reads a value and applies MVCC visibility rules.
func (txn *MvccTransaction) readWithMVCC(key []byte) ([]byte, error) {
	// Read raw data from engine
	rawData, err := txn.engineTxn.Get(key)
	if err != nil {
		return nil, err
	}

	// Deserialize versioned value
	versionedValue, err := txn.deserializeVersionedValue(rawData)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize versioned value: %w", err)
	}

	// Get active transactions for visibility check
	activeTxns := txn.manager.GetActiveTxns()

	// Remove ourselves from active list for visibility check
	delete(activeTxns, txn.info.ID)

	// Find visible version
	for current := versionedValue; current != nil; current = current.NextVersion {
		if current.IsVisible(txn.info.ReadTimestamp, activeTxns) {
			if current.IsDeleted() {
				return nil, engine.ErrKeyNotFound
			}
			result := make([]byte, len(current.Value))
			copy(result, current.Value)
			return result, nil
		}
	}

	return nil, engine.ErrKeyNotFound
}

// trackRead records that this transaction read a key at a specific timestamp.
func (txn *MvccTransaction) trackRead(key string) {
	if txn.info.IsolationLevel >= RepeatableRead {
		txn.readSet[key] = txn.info.ReadTimestamp
	}
}

// hasWritten checks if this transaction has written to a key.
func (txn *MvccTransaction) hasWritten(key string) bool {
	_, exists := txn.writeSet[key]
	return exists
}

// serializeVersionedValue serializes a versioned value for storage.
func (txn *MvccTransaction) serializeVersionedValue(vv *VersionedValue) ([]byte, error) {
	// Simple JSON serialization for now
	// In production, would use a more efficient binary format
	type SerializedVersion struct {
		Value        []byte        `json:"value"`
		CreatedByTxn TransactionID `json:"created_by_txn"`
		CreatedAt    Timestamp     `json:"created_at"`
		DeletedByTxn TransactionID `json:"deleted_by_txn,omitempty"`
		DeletedAt    Timestamp     `json:"deleted_at,omitempty"`
	}

	sv := SerializedVersion{
		Value:        vv.Value,
		CreatedByTxn: vv.CreatedByTxn,
		CreatedAt:    vv.CreatedAt,
		DeletedByTxn: vv.DeletedByTxn,
		DeletedAt:    vv.DeletedAt,
	}

	return json.Marshal(sv)
}

// deserializeVersionedValue deserializes a versioned value from storage.
func (txn *MvccTransaction) deserializeVersionedValue(data []byte) (*VersionedValue, error) {
	type SerializedVersion struct {
		Value        []byte        `json:"value"`
		CreatedByTxn TransactionID `json:"created_by_txn"`
		CreatedAt    Timestamp     `json:"created_at"`
		DeletedByTxn TransactionID `json:"deleted_by_txn,omitempty"`
		DeletedAt    Timestamp     `json:"deleted_at,omitempty"`
	}

	var sv SerializedVersion
	if err := json.Unmarshal(data, &sv); err != nil {
		return nil, err
	}

	return &VersionedValue{
		Value:        sv.Value,
		CreatedByTxn: sv.CreatedByTxn,
		CreatedAt:    sv.CreatedAt,
		DeletedByTxn: sv.DeletedByTxn,
		DeletedAt:    sv.DeletedAt,
	}, nil
}

// String returns a string representation of the transaction.
func (txn *MvccTransaction) String() string {
	return fmt.Sprintf("Transaction{ID: %d, Status: %d, Isolation: %d, ReadTS: %d, WriteTS: %d}",
		txn.info.ID, txn.info.Status, txn.info.IsolationLevel,
		txn.info.ReadTimestamp, txn.info.WriteTimestamp)
}

// GetEngineTxn returns the underlying engine transaction (for testing).
func (txn *MvccTransaction) GetEngineTxn() engine.Transaction {
	txn.mu.RLock()
	defer txn.mu.RUnlock()
	return txn.engineTxn
}

// SetEngineTxn sets the underlying engine transaction (for testing).
func (txn *MvccTransaction) SetEngineTxn(engineTxn engine.Transaction) {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	txn.engineTxn = engineTxn
}
