package raft

import (
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/dshills/QuantaDB/internal/log"
)

// FileStorage implements Storage using file-based persistence
type FileStorage struct {
	dataDir string
	logger  log.Logger
	mu      sync.RWMutex
	
	// Cached state
	state *PersistentState
}

// NewFileStorage creates a new file-based storage
func NewFileStorage(dataDir string, logger log.Logger) (*FileStorage, error) {
	// Create data directory if it doesn't exist
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}
	
	storage := &FileStorage{
		dataDir: dataDir,
		logger:  logger,
	}
	
	// Load existing state if available
	if err := storage.loadStateFromDisk(); err != nil {
		logger.Warn("Failed to load existing state, starting fresh", "error", err)
		storage.state = &PersistentState{
			CurrentTerm: 0,
			VotedFor:    nil,
			Log:         []LogEntry{},
		}
	}
	
	return storage, nil
}

// SaveState saves the persistent state to disk
func (s *FileStorage) SaveState(state *PersistentState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	// Write to temporary file first
	tempPath := filepath.Join(s.dataDir, "state.tmp")
	file, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("failed to create temp state file: %w", err)
	}
	defer file.Close()
	
	// Encode state
	encoder := gob.NewEncoder(file)
	if err := encoder.Encode(state); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("failed to encode state: %w", err)
	}
	
	// Sync to ensure durability
	if err := file.Sync(); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("failed to sync state file: %w", err)
	}
	
	file.Close()
	
	// Atomically replace the state file
	statePath := filepath.Join(s.dataDir, "state.dat")
	if err := os.Rename(tempPath, statePath); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("failed to replace state file: %w", err)
	}
	
	// Update cached state
	s.state = &PersistentState{
		CurrentTerm: state.CurrentTerm,
		VotedFor:    state.VotedFor,
		Log:         make([]LogEntry, len(state.Log)),
	}
	copy(s.state.Log, state.Log)
	
	s.logger.Debug("Saved persistent state", 
		"term", state.CurrentTerm, 
		"votedFor", state.VotedFor,
		"logEntries", len(state.Log))
	
	return nil
}

// LoadState loads the persistent state from disk
func (s *FileStorage) LoadState() (*PersistentState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	if s.state == nil {
		return nil, fmt.Errorf("no state available")
	}
	
	// Return a copy to avoid external mutations
	state := &PersistentState{
		CurrentTerm: s.state.CurrentTerm,
		VotedFor:    s.state.VotedFor,
		Log:         make([]LogEntry, len(s.state.Log)),
	}
	copy(state.Log, s.state.Log)
	
	return state, nil
}

// AppendEntries appends new log entries to storage
func (s *FileStorage) AppendEntries(entries []LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	// Append to in-memory log
	s.state.Log = append(s.state.Log, entries...)
	
	// Persist the updated state
	return s.saveStateWithoutLock(s.state)
}

// GetEntry retrieves a specific log entry by index
func (s *FileStorage) GetEntry(index LogIndex) (*LogEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	if index == 0 || index > LogIndex(len(s.state.Log)) {
		return nil, fmt.Errorf("log entry not found at index %d", index)
	}
	
	entry := s.state.Log[index-1] // Convert to 0-based indexing
	return &entry, nil
}

// GetEntries retrieves a range of log entries
func (s *FileStorage) GetEntries(start, end LogIndex) ([]LogEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	if start == 0 || start > LogIndex(len(s.state.Log)) {
		return nil, fmt.Errorf("invalid start index %d", start)
	}
	
	if end > LogIndex(len(s.state.Log)) {
		end = LogIndex(len(s.state.Log))
	}
	
	if start > end {
		return []LogEntry{}, nil
	}
	
	// Convert to 0-based indexing and extract range [start, end)
	startIdx := start - 1
	endIdx := end - 1
	
	entries := make([]LogEntry, endIdx-startIdx+1)
	copy(entries, s.state.Log[startIdx:endIdx+1])
	
	return entries, nil
}

// GetLastEntry retrieves the last log entry
func (s *FileStorage) GetLastEntry() (*LogEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	if len(s.state.Log) == 0 {
		return nil, fmt.Errorf("no log entries")
	}
	
	entry := s.state.Log[len(s.state.Log)-1]
	return &entry, nil
}

// TruncateAfter truncates the log after the specified index
func (s *FileStorage) TruncateAfter(index LogIndex) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if index >= LogIndex(len(s.state.Log)) {
		// Nothing to truncate
		return nil
	}
	
	// Truncate the log
	s.state.Log = s.state.Log[:index]
	
	// Persist the updated state
	return s.saveStateWithoutLock(s.state)
}

// SaveSnapshot saves a snapshot to disk
func (s *FileStorage) SaveSnapshot(index LogIndex, term Term, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	snapshot := &Snapshot{
		Index: index,
		Term:  term,
		Data:  data,
	}
	
	// Write to temporary file first
	tempPath := filepath.Join(s.dataDir, "snapshot.tmp")
	file, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("failed to create temp snapshot file: %w", err)
	}
	defer file.Close()
	
	// Encode snapshot
	encoder := gob.NewEncoder(file)
	if err := encoder.Encode(snapshot); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("failed to encode snapshot: %w", err)
	}
	
	// Sync to ensure durability
	if err := file.Sync(); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("failed to sync snapshot file: %w", err)
	}
	
	file.Close()
	
	// Atomically replace the snapshot file
	snapshotPath := filepath.Join(s.dataDir, "snapshot.dat")
	if err := os.Rename(tempPath, snapshotPath); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("failed to replace snapshot file: %w", err)
	}
	
	s.logger.Debug("Saved snapshot", "index", index, "term", term, "dataSize", len(data))
	return nil
}

// LoadSnapshot loads a snapshot from disk
func (s *FileStorage) LoadSnapshot() (LogIndex, Term, []byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	snapshotPath := filepath.Join(s.dataDir, "snapshot.dat")
	file, err := os.Open(snapshotPath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, 0, nil, fmt.Errorf("no snapshot available")
		}
		return 0, 0, nil, fmt.Errorf("failed to open snapshot file: %w", err)
	}
	defer file.Close()
	
	// Decode snapshot
	var snapshot Snapshot
	decoder := gob.NewDecoder(file)
	if err := decoder.Decode(&snapshot); err != nil {
		return 0, 0, nil, fmt.Errorf("failed to decode snapshot: %w", err)
	}
	
	s.logger.Debug("Loaded snapshot", 
		"index", snapshot.Index, 
		"term", snapshot.Term, 
		"dataSize", len(snapshot.Data))
	
	return snapshot.Index, snapshot.Term, snapshot.Data, nil
}

// CompactLog compacts the log by removing entries up to the specified index
func (s *FileStorage) CompactLog(index LogIndex) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if index >= LogIndex(len(s.state.Log)) {
		// Nothing to compact
		return nil
	}
	
	// Remove entries up to index
	newLog := make([]LogEntry, len(s.state.Log)-int(index))
	copy(newLog, s.state.Log[index:])
	s.state.Log = newLog
	
	// Persist the updated state
	err := s.saveStateWithoutLock(s.state)
	if err != nil {
		return err
	}
	
	s.logger.Debug("Compacted log", "removedEntries", index, "remainingEntries", len(s.state.Log))
	return nil
}

// Close closes the storage
func (s *FileStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	// Final save to ensure all data is persisted
	if s.state != nil {
		s.saveStateWithoutLock(s.state)
	}
	
	s.logger.Debug("File storage closed")
	return nil
}

// loadStateFromDisk loads state from disk without holding lock
func (s *FileStorage) loadStateFromDisk() error {
	statePath := filepath.Join(s.dataDir, "state.dat")
	file, err := os.Open(statePath)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("state file does not exist")
		}
		return fmt.Errorf("failed to open state file: %w", err)
	}
	defer file.Close()
	
	// Decode state
	var state PersistentState
	decoder := gob.NewDecoder(file)
	if err := decoder.Decode(&state); err != nil {
		return fmt.Errorf("failed to decode state: %w", err)
	}
	
	s.state = &state
	
	s.logger.Debug("Loaded persistent state", 
		"term", state.CurrentTerm, 
		"votedFor", state.VotedFor,
		"logEntries", len(state.Log))
	
	return nil
}

// saveStateWithoutLock saves state without acquiring lock (assumes caller holds lock)
func (s *FileStorage) saveStateWithoutLock(state *PersistentState) error {
	// Write to temporary file first
	tempPath := filepath.Join(s.dataDir, "state.tmp")
	file, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("failed to create temp state file: %w", err)
	}
	defer file.Close()
	
	// Encode state
	encoder := gob.NewEncoder(file)
	if err := encoder.Encode(state); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("failed to encode state: %w", err)
	}
	
	// Sync to ensure durability
	if err := file.Sync(); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("failed to sync state file: %w", err)
	}
	
	file.Close()
	
	// Atomically replace the state file
	statePath := filepath.Join(s.dataDir, "state.dat")
	if err := os.Rename(tempPath, statePath); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("failed to replace state file: %w", err)
	}
	
	return nil
}

// Snapshot represents a point-in-time snapshot of the state machine
type Snapshot struct {
	Index LogIndex
	Term  Term
	Data  []byte
}

// MemoryStorage is an in-memory implementation for testing
type MemoryStorage struct {
	mu       sync.RWMutex
	state    *PersistentState
	snapshot *Snapshot
}

// NewMemoryStorage creates a new in-memory storage
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		state: &PersistentState{
			CurrentTerm: 0,
			VotedFor:    nil,
			Log:         []LogEntry{},
		},
	}
}

// SaveState saves the persistent state
func (m *MemoryStorage) SaveState(state *PersistentState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	m.state = &PersistentState{
		CurrentTerm: state.CurrentTerm,
		VotedFor:    state.VotedFor,
		Log:         make([]LogEntry, len(state.Log)),
	}
	copy(m.state.Log, state.Log)
	
	return nil
}

// LoadState loads the persistent state
func (m *MemoryStorage) LoadState() (*PersistentState, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	state := &PersistentState{
		CurrentTerm: m.state.CurrentTerm,
		VotedFor:    m.state.VotedFor,
		Log:         make([]LogEntry, len(m.state.Log)),
	}
	copy(state.Log, m.state.Log)
	
	return state, nil
}

// AppendEntries appends new log entries
func (m *MemoryStorage) AppendEntries(entries []LogEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	m.state.Log = append(m.state.Log, entries...)
	return nil
}

// GetEntry retrieves a specific log entry
func (m *MemoryStorage) GetEntry(index LogIndex) (*LogEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	if index == 0 || index > LogIndex(len(m.state.Log)) {
		return nil, fmt.Errorf("log entry not found at index %d", index)
	}
	
	entry := m.state.Log[index-1]
	return &entry, nil
}

// GetEntries retrieves a range of log entries
func (m *MemoryStorage) GetEntries(start, end LogIndex) ([]LogEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	if start == 0 || start > LogIndex(len(m.state.Log)) {
		return nil, fmt.Errorf("invalid start index %d", start)
	}
	
	if end > LogIndex(len(m.state.Log)) {
		end = LogIndex(len(m.state.Log))
	}
	
	if start > end {
		return []LogEntry{}, nil
	}
	
	startIdx := start - 1
	endIdx := end - 1
	
	entries := make([]LogEntry, endIdx-startIdx+1)
	copy(entries, m.state.Log[startIdx:endIdx+1])
	
	return entries, nil
}

// GetLastEntry retrieves the last log entry
func (m *MemoryStorage) GetLastEntry() (*LogEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	if len(m.state.Log) == 0 {
		return nil, fmt.Errorf("no log entries")
	}
	
	entry := m.state.Log[len(m.state.Log)-1]
	return &entry, nil
}

// TruncateAfter truncates the log after the specified index
func (m *MemoryStorage) TruncateAfter(index LogIndex) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if index >= LogIndex(len(m.state.Log)) {
		return nil
	}
	
	m.state.Log = m.state.Log[:index]
	return nil
}

// SaveSnapshot saves a snapshot
func (m *MemoryStorage) SaveSnapshot(index LogIndex, term Term, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	m.snapshot = &Snapshot{
		Index: index,
		Term:  term,
		Data:  make([]byte, len(data)),
	}
	copy(m.snapshot.Data, data)
	
	return nil
}

// LoadSnapshot loads a snapshot
func (m *MemoryStorage) LoadSnapshot() (LogIndex, Term, []byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	if m.snapshot == nil {
		return 0, 0, nil, fmt.Errorf("no snapshot available")
	}
	
	data := make([]byte, len(m.snapshot.Data))
	copy(data, m.snapshot.Data)
	
	return m.snapshot.Index, m.snapshot.Term, data, nil
}

// CompactLog compacts the log
func (m *MemoryStorage) CompactLog(index LogIndex) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if index >= LogIndex(len(m.state.Log)) {
		return nil
	}
	
	newLog := make([]LogEntry, len(m.state.Log)-int(index))
	copy(newLog, m.state.Log[index:])
	m.state.Log = newLog
	
	return nil
}

// Close closes the storage
func (m *MemoryStorage) Close() error {
	return nil
}