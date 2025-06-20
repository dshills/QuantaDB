package txn

import "sync/atomic"

// TimestampService provides thread-safe timestamp generation for MVCC operations.
// This service should be used instead of calling NextTimestamp() directly.
type TimestampService struct {
	// No fields needed - we use the global atomic counter
}

// NewTimestampService creates a new timestamp service.
func NewTimestampService() *TimestampService {
	return &TimestampService{}
}

// GetCurrentTimestamp returns the current timestamp without advancing it.
// This is useful for read-only operations that need a consistent snapshot.
func (ts *TimestampService) GetCurrentTimestamp() Timestamp {
	// Read the current value atomically without incrementing
	return Timestamp(atomic.LoadUint64(&globalTimestamp))
}

// GetNextTimestamp returns the next timestamp, advancing the global counter.
// This should be used when creating new transactions or versions.
func (ts *TimestampService) GetNextTimestamp() Timestamp {
	return NextTimestamp()
}

// GetSnapshotTimestamp returns a timestamp suitable for snapshot reads.
// For non-transactional reads, this returns the next timestamp.
// For transactional reads, the transaction's read timestamp should be used instead.
func (ts *TimestampService) GetSnapshotTimestamp(txn *MvccTransaction) Timestamp {
	if txn != nil {
		return txn.ReadTimestamp()
	}
	// For non-transactional reads, use current timestamp
	// Note: This doesn't advance the counter, avoiding unnecessary timestamp advancement
	return ts.GetCurrentTimestamp()
}
