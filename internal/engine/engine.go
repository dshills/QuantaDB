package engine

import (
	"context"
	"errors"
)

var (
	// ErrKeyNotFound is returned when a key doesn't exist.
	ErrKeyNotFound = errors.New("key not found")

	// ErrTransactionClosed is returned when operating on a closed transaction.
	ErrTransactionClosed = errors.New("transaction closed")

	// ErrTransactionConflict is returned when a transaction conflicts.
	ErrTransactionConflict = errors.New("transaction conflict")
)

// Engine defines the interface for storage backends
type Engine interface {
	// Get retrieves a value by key
	Get(ctx context.Context, key []byte) ([]byte, error)

	// Put stores a key-value pair
	Put(ctx context.Context, key, value []byte) error

	// Delete removes a key
	Delete(ctx context.Context, key []byte) error

	// Scan returns an iterator for range queries
	Scan(ctx context.Context, start, end []byte) (Iterator, error)

	// BeginTransaction starts a new transaction
	BeginTransaction(ctx context.Context) (Transaction, error)

	// Close closes the engine
	Close() error
}

// Transaction represents a database transaction
type Transaction interface {
	// Get retrieves a value by key within the transaction
	Get(key []byte) ([]byte, error)

	// Put stores a key-value pair within the transaction
	Put(key, value []byte) error

	// Delete removes a key within the transaction
	Delete(key []byte) error

	// Commit commits the transaction
	Commit() error

	// Rollback rolls back the transaction
	Rollback() error
}

// Iterator provides iteration over key-value pairs
type Iterator interface {
	// Next moves to the next key-value pair
	Next() bool

	// Key returns the current key
	Key() []byte

	// Value returns the current value
	Value() []byte

	// Error returns any error that occurred during iteration
	Error() error

	// Close closes the iterator
	Close() error
}

// Options represents engine configuration options
type Options struct {
	// CacheSize is the size of the in-memory cache in bytes
	CacheSize int64

	// WriteBufferSize is the size of the write buffer
	WriteBufferSize int

	// MaxOpenFiles is the maximum number of open file handles
	MaxOpenFiles int

	// CompactionInterval is how often to run compaction
	CompactionInterval int
}
