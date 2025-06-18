package engine

import (
	"bytes"
	"context"
	"sort"
	"sync"
)

// memoryEngine is an in-memory implementation of the Engine interface
type memoryEngine struct {
	mu   sync.RWMutex
	data map[string][]byte
}

// NewMemoryEngine creates a new in-memory storage engine
func NewMemoryEngine() Engine {
	return &memoryEngine{
		data: make(map[string][]byte),
	}
}

func (m *memoryEngine) Get(ctx context.Context, key []byte) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	value, exists := m.data[string(key)]
	if !exists {
		return nil, ErrKeyNotFound
	}

	// Return a copy to prevent external modifications
	result := make([]byte, len(value))
	copy(result, value)
	return result, nil
}

func (m *memoryEngine) Put(ctx context.Context, key, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Store copies to prevent external modifications
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)

	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)

	m.data[string(keyCopy)] = valueCopy
	return nil
}

func (m *memoryEngine) Delete(ctx context.Context, key []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.data, string(key))
	return nil
}

func (m *memoryEngine) Scan(ctx context.Context, start, end []byte) (Iterator, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Collect all keys within range
	var keys []string
	for k := range m.data {
		keyBytes := []byte(k)
		if (start == nil || bytes.Compare(keyBytes, start) >= 0) &&
			(end == nil || bytes.Compare(keyBytes, end) < 0) {
			keys = append(keys, k)
		}
	}

	// Sort keys
	sort.Strings(keys)

	// Create snapshot of values
	values := make(map[string][]byte)
	for _, k := range keys {
		v := m.data[k]
		valueCopy := make([]byte, len(v))
		copy(valueCopy, v)
		values[k] = valueCopy
	}

	return &memoryIterator{
		keys:   keys,
		values: values,
		pos:    -1,
	}, nil
}

func (m *memoryEngine) BeginTransaction(ctx context.Context) (Transaction, error) {
	return &memoryTransaction{
		engine:  m,
		reads:   make(map[string][]byte),
		writes:  make(map[string][]byte),
		deletes: make(map[string]bool),
		closed:  false,
	}, nil
}

func (m *memoryEngine) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Clear data
	m.data = nil
	return nil
}

// memoryIterator implements the Iterator interface
type memoryIterator struct {
	keys   []string
	values map[string][]byte
	pos    int
	err    error
}

func (it *memoryIterator) Next() bool {
	it.pos++
	return it.pos < len(it.keys)
}

func (it *memoryIterator) Key() []byte {
	if it.pos < 0 || it.pos >= len(it.keys) {
		return nil
	}
	return []byte(it.keys[it.pos])
}

func (it *memoryIterator) Value() []byte {
	if it.pos < 0 || it.pos >= len(it.keys) {
		return nil
	}
	return it.values[it.keys[it.pos]]
}

func (it *memoryIterator) Error() error {
	return it.err
}

func (it *memoryIterator) Close() error {
	it.keys = nil
	it.values = nil
	return nil
}

// memoryTransaction implements the Transaction interface
type memoryTransaction struct {
	engine  *memoryEngine
	mu      sync.Mutex
	reads   map[string][]byte
	writes  map[string][]byte
	deletes map[string]bool
	closed  bool
}

func (tx *memoryTransaction) Get(key []byte) ([]byte, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.closed {
		return nil, ErrTransactionClosed
	}

	keyStr := string(key)

	// Check if deleted in this transaction
	if tx.deletes[keyStr] {
		return nil, ErrKeyNotFound
	}

	// Check writes first
	if value, exists := tx.writes[keyStr]; exists {
		result := make([]byte, len(value))
		copy(result, value)
		return result, nil
	}

	// Check reads cache
	if value, exists := tx.reads[keyStr]; exists {
		result := make([]byte, len(value))
		copy(result, value)
		return result, nil
	}

	// Read from engine
	value, err := tx.engine.Get(context.Background(), key)
	if err != nil {
		return nil, err
	}

	// Cache the read
	tx.reads[keyStr] = value

	result := make([]byte, len(value))
	copy(result, value)
	return result, nil
}

func (tx *memoryTransaction) Put(key, value []byte) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.closed {
		return ErrTransactionClosed
	}

	keyStr := string(key)
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)

	tx.writes[keyStr] = valueCopy
	delete(tx.deletes, keyStr)

	return nil
}

func (tx *memoryTransaction) Delete(key []byte) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.closed {
		return ErrTransactionClosed
	}

	keyStr := string(key)
	tx.deletes[keyStr] = true
	delete(tx.writes, keyStr)

	return nil
}

func (tx *memoryTransaction) Commit() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.closed {
		return ErrTransactionClosed
	}

	// Apply all changes atomically
	tx.engine.mu.Lock()
	defer tx.engine.mu.Unlock()

	// Apply writes
	for k, v := range tx.writes {
		tx.engine.data[k] = v
	}

	// Apply deletes
	for k := range tx.deletes {
		delete(tx.engine.data, k)
	}

	tx.closed = true
	return nil
}

func (tx *memoryTransaction) Rollback() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.closed {
		return ErrTransactionClosed
	}

	// Clear transaction state
	tx.reads = nil
	tx.writes = nil
	tx.deletes = nil
	tx.closed = true

	return nil
}
