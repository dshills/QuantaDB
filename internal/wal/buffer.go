package wal

import (
	"bytes"
	"fmt"
	"sync"
)

const (
	// DefaultBufferSize is the default size of the WAL buffer (1MB).
	DefaultBufferSize = 1024 * 1024

	// MinBufferSize is the minimum allowed buffer size (64KB).
	MinBufferSize = 64 * 1024
)

// Buffer represents an in-memory buffer for WAL records.
type Buffer struct {
	mu       sync.Mutex
	data     *bytes.Buffer
	size     int
	capacity int
	records  []*LogRecord
}

// NewBuffer creates a new WAL buffer
func NewBuffer(capacity int) *Buffer {
	if capacity < MinBufferSize {
		capacity = MinBufferSize
	}

	return &Buffer{
		data:     bytes.NewBuffer(make([]byte, 0, capacity)),
		size:     0,
		capacity: capacity,
		records:  make([]*LogRecord, 0),
	}
}

// Append adds a log record to the buffer
func (b *Buffer) Append(record *LogRecord) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Serialize the record to temporary buffer to get size
	tempBuf := &bytes.Buffer{}
	if err := SerializeRecord(tempBuf, record); err != nil {
		return fmt.Errorf("failed to serialize record: %w", err)
	}

	recordSize := tempBuf.Len()

	// Check if record fits in buffer
	if b.size+recordSize > b.capacity {
		return fmt.Errorf("buffer full: need %d bytes, have %d available", recordSize, b.capacity-b.size)
	}

	// Write to actual buffer
	if err := SerializeRecord(b.data, record); err != nil {
		return fmt.Errorf("failed to write record to buffer: %w", err)
	}

	b.size += recordSize
	b.records = append(b.records, record)

	return nil
}

// Size returns the current size of data in the buffer
func (b *Buffer) Size() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.size
}

// IsFull returns true if the buffer cannot accept a record of the given size
func (b *Buffer) IsFull(recordSize int) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.size+recordSize > b.capacity
}

// IsEmpty returns true if the buffer contains no records
func (b *Buffer) IsEmpty() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.size == 0
}

// GetData returns the serialized data and record list
func (b *Buffer) GetData() ([]byte, []*LogRecord) {
	b.mu.Lock()
	defer b.mu.Unlock()

	data := make([]byte, b.data.Len())
	copy(data, b.data.Bytes())

	records := make([]*LogRecord, len(b.records))
	copy(records, b.records)

	return data, records
}

// Reset clears the buffer
func (b *Buffer) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.data.Reset()
	b.size = 0
	b.records = b.records[:0]
}

// Capacity returns the total capacity of the buffer
func (b *Buffer) Capacity() int {
	return b.capacity
}

// AvailableSpace returns the available space in the buffer
func (b *Buffer) AvailableSpace() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.capacity - b.size
}

// RecordCount returns the number of records in the buffer
func (b *Buffer) RecordCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.records)
}

// LastLSN returns the LSN of the last record in the buffer
func (b *Buffer) LastLSN() LSN {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.records) == 0 {
		return InvalidLSN
	}

	return b.records[len(b.records)-1].LSN
}
