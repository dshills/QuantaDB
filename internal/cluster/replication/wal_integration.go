package replication

import (
	"sync"
	"time"

	"github.com/dshills/QuantaDB/internal/log"
	"github.com/dshills/QuantaDB/internal/wal"
)

// WALReplicationHook integrates with the WAL manager to stream records in real-time
type WALReplicationHook struct {
	logger   log.Logger
	streamer WALStreamer
	mu       sync.RWMutex
	enabled  bool
}

// NewWALReplicationHook creates a new WAL replication hook
func NewWALReplicationHook(logger log.Logger) *WALReplicationHook {
	return &WALReplicationHook{
		logger:  logger,
		enabled: false,
	}
}

// SetStreamer sets the WAL streamer for this hook
func (h *WALReplicationHook) SetStreamer(streamer WALStreamer) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.streamer = streamer
	h.enabled = streamer != nil
}

// Enable enables WAL streaming
func (h *WALReplicationHook) Enable() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.enabled = h.streamer != nil
}

// Disable disables WAL streaming
func (h *WALReplicationHook) Disable() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.enabled = false
}

// OnRecordAppended is called whenever a new WAL record is appended
func (h *WALReplicationHook) OnRecordAppended(record *wal.LogRecord) error {
	h.mu.RLock()
	enabled := h.enabled
	streamer := h.streamer
	h.mu.RUnlock()

	if !enabled || streamer == nil {
		return nil
	}

	// Stream the record to all replicas
	if err := streamer.StreamWALRecord(record); err != nil {
		h.logger.Warn("Failed to stream WAL record", 
			"lsn", record.LSN, 
			"type", record.Type, 
			"error", err)
		// Don't return error to avoid breaking the main WAL append operation
	}

	return nil
}

// ReplicationAwareWALManager wraps the existing WAL manager with replication capabilities
type ReplicationAwareWALManager struct {
	*wal.Manager
	hook *WALReplicationHook
}

// NewReplicationAwareWALManager creates a new replication-aware WAL manager
func NewReplicationAwareWALManager(config *wal.Config, logger log.Logger) (*ReplicationAwareWALManager, error) {
	// Create the underlying WAL manager
	walMgr, err := wal.NewManager(config)
	if err != nil {
		return nil, err
	}

	// Create the replication hook
	hook := NewWALReplicationHook(logger)

	return &ReplicationAwareWALManager{
		Manager: walMgr,
		hook:    hook,
	}, nil
}

// AppendRecord overrides the base AppendRecord to add replication hooks
func (rm *ReplicationAwareWALManager) AppendRecord(record *wal.LogRecord) error {
	// Call the original append method
	if err := rm.Manager.AppendRecord(record); err != nil {
		return err
	}

	// Trigger replication hook
	rm.hook.OnRecordAppended(record) // Ignore error to not block primary operations

	return nil
}

// LogBeginTxn logs the start of a transaction with replication
func (rm *ReplicationAwareWALManager) LogBeginTxn(txnID uint64) (wal.LSN, error) {
	lsn, err := rm.Manager.LogBeginTxn(txnID)
	if err == nil {
		// Create a record for replication
		record := &wal.LogRecord{
			LSN:       lsn,
			Type:      wal.RecordTypeBeginTxn,
			TxnID:     txnID,
			PrevLSN:   wal.InvalidLSN,
			Timestamp: time.Now(),
		}
		rm.hook.OnRecordAppended(record) // Trigger replication
	}
	return lsn, err
}

// LogCommitTxn logs the commit of a transaction with replication
func (rm *ReplicationAwareWALManager) LogCommitTxn(txnID uint64) (wal.LSN, error) {
	lsn, err := rm.Manager.LogCommitTxn(txnID)
	if err == nil {
		// Create a record for replication
		record := &wal.LogRecord{
			LSN:       lsn,
			Type:      wal.RecordTypeCommitTxn,
			TxnID:     txnID,
			Timestamp: time.Now(),
		}
		rm.hook.OnRecordAppended(record) // Trigger replication
	}
	return lsn, err
}

// LogInsert logs an insert operation with replication
func (rm *ReplicationAwareWALManager) LogInsert(txnID uint64, tableID int64, pageID uint32, slotID uint16, rowData []byte) (wal.LSN, error) {
	lsn, err := rm.Manager.LogInsert(txnID, tableID, pageID, slotID, rowData)
	if err == nil {
		// Create a record for replication
		record := &wal.LogRecord{
			LSN:       lsn,
			Type:      wal.RecordTypeInsert,
			TxnID:     txnID,
			Timestamp: time.Now(),
			Data:      rowData,
		}
		rm.hook.OnRecordAppended(record) // Trigger replication
	}
	return lsn, err
}

// SetStreamer sets the WAL streamer for replication
func (rm *ReplicationAwareWALManager) SetStreamer(streamer WALStreamer) {
	rm.hook.SetStreamer(streamer)
}

// EnableReplication enables WAL replication
func (rm *ReplicationAwareWALManager) EnableReplication() {
	rm.hook.Enable()
}

// DisableReplication disables WAL replication
func (rm *ReplicationAwareWALManager) DisableReplication() {
	rm.hook.Disable()
}

// RealtimeWALReader reads WAL records from disk for historical streaming
type RealtimeWALReader struct {
	walMgr  *wal.Manager
	logger  log.Logger
	segDir  string
}

// NewRealtimeWALReader creates a new realtime WAL reader
func NewRealtimeWALReader(walMgr *wal.Manager, logger log.Logger, segmentDir string) *RealtimeWALReader {
	return &RealtimeWALReader{
		walMgr: walMgr,
		logger: logger,
		segDir: segmentDir,
	}
}

// ReadRecordsFromLSN reads WAL records starting from the specified LSN
func (r *RealtimeWALReader) ReadRecordsFromLSN(startLSN wal.LSN, maxRecords int) ([]*wal.LogRecord, error) {
	// TODO: Implement reading WAL records from disk starting at startLSN
	// This would involve:
	// 1. Finding the correct WAL segment file for startLSN
	// 2. Seeking to the correct position within the file
	// 3. Reading and deserializing records
	// 4. Handling segment boundaries
	// 5. Returning up to maxRecords

	r.logger.Debug("Reading WAL records from disk", 
		"startLSN", startLSN, 
		"maxRecords", maxRecords)

	// For now, return empty slice
	// In a real implementation, this would read from WAL segment files
	return []*wal.LogRecord{}, nil
}

// StreamHistoricalRecords streams historical WAL records to a replica
func (r *RealtimeWALReader) StreamHistoricalRecords(
	streamer WALStreamer, 
	replica ReplicaInfo, 
	startLSN wal.LSN,
	currentLSN wal.LSN,
) error {
	const batchSize = 100
	
	r.logger.Info("Streaming historical records", 
		"replica", replica.NodeID,
		"startLSN", startLSN,
		"currentLSN", currentLSN)

	// Stream records in batches
	readLSN := startLSN
	for readLSN < currentLSN {
		// Read batch of records
		records, err := r.ReadRecordsFromLSN(readLSN, batchSize)
		if err != nil {
			return err
		}

		if len(records) == 0 {
			break // No more records
		}

		// Stream each record
		for _, record := range records {
			if record.LSN >= currentLSN {
				break // Caught up
			}

			if err := streamer.StreamWALRecord(record); err != nil {
				return err
			}

			readLSN = record.LSN + 1
		}

		// Small delay to avoid overwhelming the replica
		time.Sleep(10 * time.Millisecond)
	}

	r.logger.Info("Finished streaming historical records", 
		"replica", replica.NodeID,
		"endLSN", readLSN)

	return nil
}

// WALStreamingCoordinator coordinates between WAL manager and streaming
type WALStreamingCoordinator struct {
	walMgr   *ReplicationAwareWALManager
	reader   *RealtimeWALReader
	streamer WALStreamer
	logger   log.Logger
}

// NewWALStreamingCoordinator creates a new WAL streaming coordinator
func NewWALStreamingCoordinator(
	walMgr *ReplicationAwareWALManager,
	streamer WALStreamer,
	logger log.Logger,
) *WALStreamingCoordinator {
	reader := NewRealtimeWALReader(walMgr.Manager, logger, "")

	return &WALStreamingCoordinator{
		walMgr:   walMgr,
		reader:   reader,
		streamer: streamer,
		logger:   logger,
	}
}

// Start starts the WAL streaming coordination
func (c *WALStreamingCoordinator) Start() error {
	// Set the streamer in the WAL manager
	c.walMgr.SetStreamer(c.streamer)
	
	// Enable replication
	c.walMgr.EnableReplication()
	
	c.logger.Info("WAL streaming coordination started")
	return nil
}

// Stop stops the WAL streaming coordination
func (c *WALStreamingCoordinator) Stop() error {
	// Disable replication
	c.walMgr.DisableReplication()
	
	c.logger.Info("WAL streaming coordination stopped")
	return nil
}

// HandleNewReplica handles a new replica that needs historical data
func (c *WALStreamingCoordinator) HandleNewReplica(replica ReplicaInfo, startLSN wal.LSN) error {
	currentLSN := c.walMgr.GetCurrentLSN()
	
	// If the replica is behind, stream historical records
	if startLSN < currentLSN {
		go func() {
			if err := c.reader.StreamHistoricalRecords(c.streamer, replica, startLSN, currentLSN); err != nil {
				c.logger.Error("Failed to stream historical records", 
					"replica", replica.NodeID, 
					"error", err)
			}
		}()
	}

	return nil
}