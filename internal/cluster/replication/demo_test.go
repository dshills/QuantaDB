package replication

import (
	"fmt"
	"testing"
	"time"

	"github.com/dshills/QuantaDB/internal/log"
	"github.com/dshills/QuantaDB/internal/wal"
)

// TestReplicationDemo demonstrates a basic primary/replica setup
func TestReplicationDemo(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping replication demo in short mode")
	}

	logger := log.Default()

	// Create temporary directories for WAL
	primaryWALDir := t.TempDir()
	replicaWALDir := t.TempDir()

	// Setup primary WAL manager
	primaryWALConfig := wal.DefaultConfig()
	primaryWALConfig.Directory = primaryWALDir
	primaryWALMgr, err := NewReplicationAwareWALManager(primaryWALConfig, logger)
	if err != nil {
		t.Fatalf("Failed to create primary WAL manager: %v", err)
	}
	defer primaryWALMgr.Close()

	// Setup replica WAL manager
	replicaWALConfig := wal.DefaultConfig()
	replicaWALConfig.Directory = replicaWALDir
	replicaWALMgr, err := wal.NewManager(replicaWALConfig)
	if err != nil {
		t.Fatalf("Failed to create replica WAL manager: %v", err)
	}
	defer replicaWALMgr.Close()

	// Create replication configurations
	primaryConfig := DefaultReplicationConfig()
	primaryConfig.NodeID = "primary-1"
	primaryConfig.Mode = ReplicationModePrimary
	primaryConfig.Address = "localhost:7001"

	replicaConfig := DefaultReplicationConfig()
	replicaConfig.NodeID = "replica-1"
	replicaConfig.Mode = ReplicationModeReplica
	replicaConfig.PrimaryAddress = "localhost:7001"

	// Create replication managers
	primaryRM := NewReplicationManager(primaryConfig, primaryWALMgr.Manager, logger)
	replicaRM := NewReplicationManager(replicaConfig, replicaWALMgr, logger)

	defer primaryRM.Close()
	defer replicaRM.Close()

	// Create WAL streamer for primary
	streamerImpl := NewWALStreamer(primaryConfig, primaryWALMgr.Manager, logger)
	defer streamerImpl.Close()

	var streamer WALStreamer = streamerImpl

	// Create streaming coordinator
	coordinator := NewWALStreamingCoordinator(primaryWALMgr, streamer, logger)

	// Start coordination
	if err := coordinator.Start(); err != nil {
		t.Fatalf("Failed to start coordination: %v", err)
	}
	defer coordinator.Stop()

	t.Log("=== Replication Demo ===")
	t.Logf("Primary WAL directory: %s", primaryWALDir)
	t.Logf("Replica WAL directory: %s", replicaWALDir)

	// Log some test transactions to the primary
	t.Log("Writing test transactions to primary...")

	// Begin transaction
	txnID := uint64(1)
	beginLSN, err := primaryWALMgr.LogBeginTxn(txnID)
	if err != nil {
		t.Fatalf("Failed to log begin transaction: %v", err)
	}
	t.Logf("Logged BEGIN transaction at LSN %d", beginLSN)

	// Log some operations
	for i := 0; i < 5; i++ {
		insertLSN, err := primaryWALMgr.LogInsert(
			txnID,
			1,         // tableID
			uint32(i), // pageID
			uint16(i), // slotID
			[]byte(fmt.Sprintf("test data %d", i)),
		)
		if err != nil {
			t.Fatalf("Failed to log insert: %v", err)
		}
		t.Logf("Logged INSERT at LSN %d", insertLSN)

		// Small delay to see streaming in action
		time.Sleep(10 * time.Millisecond)
	}

	// Commit transaction
	commitLSN, err := primaryWALMgr.LogCommitTxn(txnID)
	if err != nil {
		t.Fatalf("Failed to log commit transaction: %v", err)
	}
	t.Logf("Logged COMMIT transaction at LSN %d", commitLSN)

	// Check WAL state
	primaryCurrentLSN := primaryWALMgr.GetCurrentLSN()
	replicaCurrentLSN := replicaWALMgr.GetCurrentLSN()

	t.Logf("Primary current LSN: %d", primaryCurrentLSN)
	t.Logf("Replica current LSN: %d", replicaCurrentLSN)

	// Get replication status
	primaryStatus := primaryRM.GetStatus()
	replicaStatus := replicaRM.GetStatus()

	t.Logf("Primary status: Mode=%s, State=%s, LastLSN=%d",
		primaryStatus.Mode, primaryStatus.State, primaryStatus.LastLSN)
	t.Logf("Replica status: Mode=%s, State=%s, LastLSN=%d",
		replicaStatus.Mode, replicaStatus.State, replicaStatus.LastLSN)

	// Verify that primary has processed more WAL records
	if primaryCurrentLSN == 0 {
		t.Error("Primary should have non-zero LSN after transactions")
	}

	t.Log("=== Demo completed successfully ===")
}

// TestWALIntegration tests the WAL integration components
func TestWALIntegration(t *testing.T) {
	logger := log.Default()

	// Create temporary directory for WAL
	walDir := t.TempDir()

	// Setup WAL manager
	walConfig := wal.DefaultConfig()
	walConfig.Directory = walDir
	replicationWALMgr, err := NewReplicationAwareWALManager(walConfig, logger)
	if err != nil {
		t.Fatalf("Failed to create replication-aware WAL manager: %v", err)
	}
	defer replicationWALMgr.Close()

	// Create a mock streamer
	mockStreamer := &MockWALStreamer{
		streamedRecords: make([]*wal.LogRecord, 0),
	}

	// Set up the streamer
	replicationWALMgr.SetStreamer(mockStreamer)
	replicationWALMgr.EnableReplication()

	// Log some records
	txnID := uint64(1)
	beginLSN, err := replicationWALMgr.LogBeginTxn(txnID)
	if err != nil {
		t.Fatalf("Failed to log begin transaction: %v", err)
	}

	insertLSN, err := replicationWALMgr.LogInsert(txnID, 1, 0, 0, []byte("test data"))
	if err != nil {
		t.Fatalf("Failed to log insert: %v", err)
	}

	commitLSN, err := replicationWALMgr.LogCommitTxn(txnID)
	if err != nil {
		t.Fatalf("Failed to log commit: %v", err)
	}

	// Verify that records were streamed
	if len(mockStreamer.streamedRecords) != 3 {
		t.Errorf("Expected 3 streamed records, got %d", len(mockStreamer.streamedRecords))
	}

	// Verify LSNs
	expectedLSNs := []wal.LSN{beginLSN, insertLSN, commitLSN}
	for i, record := range mockStreamer.streamedRecords {
		if record.LSN != expectedLSNs[i] {
			t.Errorf("Record %d: expected LSN %d, got %d", i, expectedLSNs[i], record.LSN)
		}
	}

	t.Log("WAL integration test completed successfully")
}

// MockWALStreamer is a mock implementation for testing
type MockWALStreamer struct {
	streamedRecords []*wal.LogRecord
}

func (m *MockWALStreamer) StartStream(replica ReplicaInfo, startLSN wal.LSN) error {
	return nil
}

func (m *MockWALStreamer) StopStream(nodeID NodeID) error {
	return nil
}

func (m *MockWALStreamer) StreamWALRecord(record *wal.LogRecord) error {
	// Make a copy to avoid issues with pointer sharing
	recordCopy := *record
	if record.Data != nil {
		recordCopy.Data = make([]byte, len(record.Data))
		copy(recordCopy.Data, record.Data)
	}
	m.streamedRecords = append(m.streamedRecords, &recordCopy)
	return nil
}

func (m *MockWALStreamer) GetStreamStatus() map[NodeID]*StreamStatus {
	return make(map[NodeID]*StreamStatus)
}
