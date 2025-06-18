package txn

import (
	"sync/atomic"
	"time"
)

// TransactionID represents a unique transaction identifier.
type TransactionID uint64

// Timestamp represents a logical timestamp for versioning.
type Timestamp uint64

// IsolationLevel represents the transaction isolation level.
type IsolationLevel int

const (
	// ReadUncommitted allows dirty reads.
	ReadUncommitted IsolationLevel = iota
	// ReadCommitted prevents dirty reads but allows non-repeatable reads.
	ReadCommitted
	// RepeatableRead prevents dirty and non-repeatable reads but allows phantom reads.
	RepeatableRead
	// Serializable provides full isolation.
	Serializable
)

// Status represents the transaction status.
type Status int

const (
	// Active means the transaction is running.
	Active Status = iota
	// Committed means the transaction has committed successfully.
	Committed
	// Aborted means the transaction has been rolled back.
	Aborted
)

// TransactionInfo contains metadata about a transaction.
type TransactionInfo struct {
	ID             TransactionID
	StartTime      time.Time
	CommitTime     time.Time
	Status         Status
	IsolationLevel IsolationLevel
	ReadTimestamp  Timestamp
	WriteTimestamp Timestamp
}

// VersionedValue represents a versioned value in the MVCC system.
type VersionedValue struct {
	Value        []byte
	CreatedByTxn TransactionID
	CreatedAt    Timestamp
	DeletedByTxn TransactionID
	DeletedAt    Timestamp
	NextVersion  *VersionedValue
}

// IsVisible checks if this version is visible to a transaction at the given timestamp.
func (v *VersionedValue) IsVisible(readTimestamp Timestamp, activeTxns map[TransactionID]bool) bool {
	// Version must be created before the read timestamp
	if v.CreatedAt > readTimestamp {
		return false
	}

	// If created by an active transaction (other than reader), not visible
	if activeTxns[v.CreatedByTxn] {
		return false
	}

	// If deleted before read timestamp, not visible
	if v.DeletedAt != 0 && v.DeletedAt <= readTimestamp {
		// Unless deleted by an active transaction
		if activeTxns[v.DeletedByTxn] {
			return true
		}
		return false
	}

	return true
}

// IsDeleted checks if this version is deleted.
func (v *VersionedValue) IsDeleted() bool {
	return v.DeletedAt != 0
}

// Global timestamp generator for MVCC.
var globalTimestamp uint64

// NextTimestamp returns the next logical timestamp.
func NextTimestamp() Timestamp {
	return Timestamp(atomic.AddUint64(&globalTimestamp, 1))
}

// Global transaction ID generator.
var globalTxnID uint64

// NextTransactionID returns the next transaction ID.
func NextTransactionID() TransactionID {
	return TransactionID(atomic.AddUint64(&globalTxnID, 1))
}
