package wal

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// Config holds configuration for the WAL manager
type Config struct {
	// Directory where WAL files are stored
	Directory string
	
	// Size of the in-memory buffer
	BufferSize int
	
	// Maximum size of a single WAL segment file
	SegmentSize int64
	
	// Whether to sync on every commit
	SyncOnCommit bool
}

// DefaultConfig returns a default WAL configuration
func DefaultConfig() *Config {
	return &Config{
		Directory:    "wal",
		BufferSize:   DefaultBufferSize,
		SegmentSize:  16 * 1024 * 1024, // 16MB segments
		SyncOnCommit: true,
	}
}

// Manager manages the write-ahead log
type Manager struct {
	config *Config
	
	// Current LSN - use atomic operations
	currentLSN uint64
	
	// In-memory buffer
	buffer *Buffer
	
	// Current segment file
	currentSegment     *os.File
	currentSegmentNum  uint64
	currentSegmentSize int64
	
	// Synchronization
	mu          sync.Mutex
	flushMu     sync.Mutex // Separate mutex for flushing
	closeCh     chan struct{}
	closeWg     sync.WaitGroup
	
	// Transaction tracking
	txnMu      sync.Mutex
	txnLastLSN map[uint64]LSN // Maps transaction ID to its last LSN
}

// NewManager creates a new WAL manager
func NewManager(config *Config) (*Manager, error) {
	// Create WAL directory if it doesn't exist
	if err := os.MkdirAll(config.Directory, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}
	
	m := &Manager{
		config:     config,
		currentLSN: 0,
		buffer:     NewBuffer(config.BufferSize),
		closeCh:    make(chan struct{}),
		txnLastLSN: make(map[uint64]LSN),
	}
	
	// Find the latest segment and LSN
	if err := m.recoverState(); err != nil {
		return nil, fmt.Errorf("failed to recover WAL state: %w", err)
	}
	
	// Open or create current segment
	if err := m.openSegment(); err != nil {
		return nil, fmt.Errorf("failed to open WAL segment: %w", err)
	}
	
	return m, nil
}

// GetNextLSN returns the next available LSN
func (m *Manager) GetNextLSN() LSN {
	return LSN(atomic.AddUint64(&m.currentLSN, 1))
}

// GetCurrentLSN returns the current LSN without incrementing
func (m *Manager) GetCurrentLSN() LSN {
	return LSN(atomic.LoadUint64(&m.currentLSN))
}

// AppendRecord appends a log record to the WAL
func (m *Manager) AppendRecord(record *LogRecord) error {
	// Track transaction's last LSN
	if record.TxnID != 0 {
		m.txnMu.Lock()
		m.txnLastLSN[record.TxnID] = record.LSN
		m.txnMu.Unlock()
	}
	
	// Try to append to buffer
	m.mu.Lock()
	err := m.buffer.Append(record)
	needsFlush := err != nil || m.shouldFlush(record)
	m.mu.Unlock()
	
	if err != nil {
		// Buffer is full, flush and retry
		if err := m.Flush(); err != nil {
			return fmt.Errorf("failed to flush buffer: %w", err)
		}
		
		// Retry append
		m.mu.Lock()
		err = m.buffer.Append(record)
		m.mu.Unlock()
		
		if err != nil {
			return fmt.Errorf("failed to append record after flush: %w", err)
		}
	}
	
	// Flush if needed (e.g., on commit)
	if needsFlush {
		if err := m.Flush(); err != nil {
			return fmt.Errorf("failed to flush on commit: %w", err)
		}
	}
	
	return nil
}

// LogBeginTxn logs the start of a transaction
func (m *Manager) LogBeginTxn(txnID uint64) (LSN, error) {
	lsn := m.GetNextLSN()
	record := NewBeginTxnRecord(lsn, txnID)
	
	if err := m.AppendRecord(record); err != nil {
		return InvalidLSN, err
	}
	
	return lsn, nil
}

// LogCommitTxn logs the commit of a transaction
func (m *Manager) LogCommitTxn(txnID uint64) (LSN, error) {
	lsn := m.GetNextLSN()
	
	// Get previous LSN for this transaction
	m.txnMu.Lock()
	prevLSN := m.txnLastLSN[txnID]
	delete(m.txnLastLSN, txnID) // Clean up
	m.txnMu.Unlock()
	
	record := NewCommitTxnRecord(lsn, txnID, prevLSN)
	
	if err := m.AppendRecord(record); err != nil {
		return InvalidLSN, err
	}
	
	return lsn, nil
}

// LogAbortTxn logs the abort of a transaction
func (m *Manager) LogAbortTxn(txnID uint64) (LSN, error) {
	lsn := m.GetNextLSN()
	
	// Get previous LSN for this transaction
	m.txnMu.Lock()
	prevLSN := m.txnLastLSN[txnID]
	delete(m.txnLastLSN, txnID) // Clean up
	m.txnMu.Unlock()
	
	record := NewAbortTxnRecord(lsn, txnID, prevLSN)
	
	if err := m.AppendRecord(record); err != nil {
		return InvalidLSN, err
	}
	
	return lsn, nil
}

// LogInsert logs an insert operation
func (m *Manager) LogInsert(txnID uint64, tableID int64, pageID uint32, slotID uint16, rowData []byte) (LSN, error) {
	lsn := m.GetNextLSN()
	
	// Get previous LSN for this transaction
	m.txnMu.Lock()
	prevLSN := m.txnLastLSN[txnID]
	m.txnMu.Unlock()
	
	record := NewInsertRecord(lsn, txnID, prevLSN, tableID, pageID, slotID, rowData)
	
	if err := m.AppendRecord(record); err != nil {
		return InvalidLSN, err
	}
	
	return lsn, nil
}

// LogDelete logs a delete operation
func (m *Manager) LogDelete(txnID uint64, tableID int64, pageID uint32, slotID uint16) (LSN, error) {
	lsn := m.GetNextLSN()
	
	// Get previous LSN for this transaction
	m.txnMu.Lock()
	prevLSN := m.txnLastLSN[txnID]
	m.txnMu.Unlock()
	
	record := NewDeleteRecord(lsn, txnID, prevLSN, tableID, pageID, slotID)
	
	if err := m.AppendRecord(record); err != nil {
		return InvalidLSN, err
	}
	
	return lsn, nil
}

// LogUpdate logs an update operation
func (m *Manager) LogUpdate(txnID uint64, tableID int64, pageID uint32, slotID uint16, oldData, newData []byte) (LSN, error) {
	lsn := m.GetNextLSN()
	
	// Get previous LSN for this transaction
	m.txnMu.Lock()
	prevLSN := m.txnLastLSN[txnID]
	m.txnMu.Unlock()
	
	record := NewUpdateRecord(lsn, txnID, prevLSN, tableID, pageID, slotID, oldData, newData)
	
	if err := m.AppendRecord(record); err != nil {
		return InvalidLSN, err
	}
	
	return lsn, nil
}

// LogCheckpoint logs a checkpoint record
func (m *Manager) LogCheckpoint(checkpoint *CheckpointRecord) (LSN, error) {
	lsn := m.GetNextLSN()
	
	record := &LogRecord{
		LSN:       lsn,
		Type:      RecordTypeCheckpoint,
		TxnID:     0, // Checkpoints are not transaction-specific
		PrevLSN:   InvalidLSN,
		Timestamp: time.Now(),
		Data:      checkpoint.Marshal(),
	}
	
	if err := m.AppendRecord(record); err != nil {
		return InvalidLSN, err
	}
	
	// Force flush for checkpoint
	if err := m.Flush(); err != nil {
		return InvalidLSN, err
	}
	
	return lsn, nil
}

// Flush writes buffered records to disk
func (m *Manager) Flush() error {
	m.flushMu.Lock()
	defer m.flushMu.Unlock()
	
	// Get data from buffer
	m.mu.Lock()
	if m.buffer.IsEmpty() {
		m.mu.Unlock()
		return nil
	}
	
	data, _ := m.buffer.GetData()
	m.buffer.Reset()
	m.mu.Unlock()
	
	// Write to segment file
	if err := m.writeToSegment(data); err != nil {
		return fmt.Errorf("failed to write to segment: %w", err)
	}
	
	// Sync if configured
	if m.config.SyncOnCommit {
		if err := m.currentSegment.Sync(); err != nil {
			return fmt.Errorf("failed to sync segment: %w", err)
		}
	}
	
	return nil
}

// Close closes the WAL manager
func (m *Manager) Close() error {
	close(m.closeCh)
	m.closeWg.Wait()
	
	// Final flush
	if err := m.Flush(); err != nil {
		return fmt.Errorf("failed to flush on close: %w", err)
	}
	
	// Close current segment
	if m.currentSegment != nil {
		if err := m.currentSegment.Close(); err != nil {
			return fmt.Errorf("failed to close segment: %w", err)
		}
	}
	
	return nil
}

// shouldFlush determines if the buffer should be flushed
func (m *Manager) shouldFlush(record *LogRecord) bool {
	// Always flush on commit if configured
	if m.config.SyncOnCommit && record.Type == RecordTypeCommitTxn {
		return true
	}
	
	// Flush if buffer is getting full (>80%)
	if float64(m.buffer.Size()) > float64(m.buffer.Capacity())*0.8 {
		return true
	}
	
	return false
}

// recoverState recovers the WAL state from existing files
func (m *Manager) recoverState() error {
	// Find all segment files
	pattern := filepath.Join(m.config.Directory, "*.wal")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return fmt.Errorf("failed to list WAL files: %w", err)
	}
	
	if len(files) == 0 {
		// No existing WAL files
		m.currentSegmentNum = 1
		atomic.StoreUint64(&m.currentLSN, 0)
		return nil
	}
	
	// Find the latest segment
	var maxSegNum uint64
	for _, file := range files {
		base := filepath.Base(file)
		var segNum uint64
		if _, err := fmt.Sscanf(base, "%016x.wal", &segNum); err == nil {
			if segNum > maxSegNum {
				maxSegNum = segNum
			}
		}
	}
	
	m.currentSegmentNum = maxSegNum
	
	// Scan the latest segment to find the last LSN
	segmentPath := m.segmentPath(maxSegNum)
	if err := m.scanSegmentForLastLSN(segmentPath); err != nil {
		return fmt.Errorf("failed to scan segment %d: %w", maxSegNum, err)
	}
	
	return nil
}

// scanSegmentForLastLSN scans a segment file to find the last valid LSN
func (m *Manager) scanSegmentForLastLSN(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open segment: %w", err)
	}
	defer file.Close()
	
	var lastLSN LSN
	for {
		record, err := DeserializeRecord(file)
		if err == io.EOF {
			break
		}
		if err != nil {
			// Partial record at end of file is expected
			break
		}
		
		if record.LSN > lastLSN {
			lastLSN = record.LSN
		}
	}
	
	atomic.StoreUint64(&m.currentLSN, uint64(lastLSN))
	
	// Get file size
	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat segment: %w", err)
	}
	m.currentSegmentSize = stat.Size()
	
	return nil
}

// openSegment opens or creates the current segment file
func (m *Manager) openSegment() error {
	segmentPath := m.segmentPath(m.currentSegmentNum)
	
	// Open with append mode
	file, err := os.OpenFile(segmentPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open segment %d: %w", m.currentSegmentNum, err)
	}
	
	m.currentSegment = file
	
	// Get current size
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return fmt.Errorf("failed to stat segment: %w", err)
	}
	m.currentSegmentSize = stat.Size()
	
	return nil
}

// writeToSegment writes data to the current segment
func (m *Manager) writeToSegment(data []byte) error {
	// Check if we need to rotate segment
	if m.currentSegmentSize+int64(len(data)) > m.config.SegmentSize {
		if err := m.rotateSegment(); err != nil {
			return fmt.Errorf("failed to rotate segment: %w", err)
		}
	}
	
	// Write data
	n, err := m.currentSegment.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write to segment: %w", err)
	}
	
	m.currentSegmentSize += int64(n)
	
	return nil
}

// rotateSegment creates a new segment file
func (m *Manager) rotateSegment() error {
	// Close current segment
	if m.currentSegment != nil {
		if err := m.currentSegment.Close(); err != nil {
			return fmt.Errorf("failed to close current segment: %w", err)
		}
	}
	
	// Increment segment number
	m.currentSegmentNum++
	m.currentSegmentSize = 0
	
	// Open new segment
	return m.openSegment()
}

// segmentPath returns the path for a segment file
func (m *Manager) segmentPath(segNum uint64) string {
	return filepath.Join(m.config.Directory, fmt.Sprintf("%016x.wal", segNum))
}