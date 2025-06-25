package raft

import (
	"testing"
	"time"

	"github.com/dshills/QuantaDB/internal/log"
)

// TestRaftNodeCreation tests creating a Raft node
func TestRaftNodeCreation(t *testing.T) {
	logger := log.Default()
	
	config := DefaultRaftConfig()
	config.NodeID = "test-node"
	config.Peers = []NodeID{"test-node"}
	
	storage := NewMemoryStorage()
	transport := NewTCPTransport(config, logger)
	stateMachine := NewDatabaseStateMachine(logger)
	
	node, err := NewRaftNode(config, transport, storage, stateMachine, logger)
	if err != nil {
		t.Fatalf("Failed to create Raft node: %v", err)
	}
	defer node.Stop()
	
	// Verify initial state
	term, state, leader := node.GetState()
	if term != 0 {
		t.Errorf("Expected initial term 0, got %d", term)
	}
	if state != StateFollower {
		t.Errorf("Expected initial state FOLLOWER, got %s", state)
	}
	if leader != nil {
		t.Errorf("Expected no initial leader, got %v", leader)
	}
}

// TestRaftNodeStartStop tests starting and stopping a Raft node
func TestRaftNodeStartStop(t *testing.T) {
	logger := log.Default()
	
	config := DefaultRaftConfig()
	config.NodeID = "test-node"
	config.Peers = []NodeID{"test-node"}
	
	storage := NewMemoryStorage()
	transport := NewTCPTransport(config, logger)
	stateMachine := NewDatabaseStateMachine(logger)
	
	node, err := NewRaftNode(config, transport, storage, stateMachine, logger)
	if err != nil {
		t.Fatalf("Failed to create Raft node: %v", err)
	}
	
	// Start the node
	if err := node.Start(); err != nil {
		t.Fatalf("Failed to start Raft node: %v", err)
	}
	
	// Give it a moment to initialize
	time.Sleep(50 * time.Millisecond)
	
	// Stop the node
	if err := node.Stop(); err != nil {
		t.Fatalf("Failed to stop Raft node: %v", err)
	}
}

// TestSingleNodeElection tests that a single node becomes leader
func TestSingleNodeElection(t *testing.T) {
	logger := log.Default()
	
	config := DefaultRaftConfig()
	config.NodeID = "single-node"
	config.Peers = []NodeID{"single-node"}
	config.ElectionTimeout = 100 * time.Millisecond
	
	storage := NewMemoryStorage()
	transport := NewTCPTransport(config, logger)
	stateMachine := NewDatabaseStateMachine(logger)
	
	node, err := NewRaftNode(config, transport, storage, stateMachine, logger)
	if err != nil {
		t.Fatalf("Failed to create Raft node: %v", err)
	}
	defer node.Stop()
	
	// Start the node
	if err := node.Start(); err != nil {
		t.Fatalf("Failed to start Raft node: %v", err)
	}
	
	// Wait for election (longer timeout to account for randomization)
	time.Sleep(500 * time.Millisecond)
	
	// Should become leader since it's the only node
	if !node.IsLeader() {
		t.Error("Single node should become leader")
	}
	
	term, state, leader := node.GetState()
	if state != StateLeader {
		t.Errorf("Expected state LEADER, got %s", state)
	}
	if leader == nil || *leader != config.NodeID {
		t.Errorf("Expected leader to be %s, got %v", config.NodeID, leader)
	}
	if term == 0 {
		t.Error("Expected term > 0 after election")
	}
}

// TestLogAppending tests appending commands to the log
func TestLogAppending(t *testing.T) {
	logger := log.Default()
	
	config := DefaultRaftConfig()
	config.NodeID = "leader-node"
	config.Peers = []NodeID{} // Single node cluster
	config.ElectionTimeout = 50 * time.Millisecond
	
	storage := NewMemoryStorage()
	transport := NewTCPTransport(config, logger)
	stateMachine := NewDatabaseStateMachine(logger)
	
	node, err := NewRaftNode(config, transport, storage, stateMachine, logger)
	if err != nil {
		t.Fatalf("Failed to create Raft node: %v", err)
	}
	defer node.Stop()
	
	// Start and wait for leadership
	if err := node.Start(); err != nil {
		t.Fatalf("Failed to start Raft node: %v", err)
	}
	
	time.Sleep(300 * time.Millisecond)
	
	if !node.IsLeader() {
		t.Fatal("Node should be leader")
	}
	
	// Append some commands
	testData := [][]byte{
		[]byte("command1"),
		[]byte("command2"),
		[]byte("command3"),
	}
	
	var indices []LogIndex
	for _, data := range testData {
		index, err := node.AppendCommand(data)
		if err != nil {
			t.Fatalf("Failed to append command: %v", err)
		}
		indices = append(indices, index)
	}
	
	// Verify indices are sequential
	for i, index := range indices {
		expectedIndex := LogIndex(i + 1)
		if index != expectedIndex {
			t.Errorf("Expected index %d, got %d", expectedIndex, index)
		}
	}
	
	// Wait for commits (single node should commit immediately)
	time.Sleep(50 * time.Millisecond)
	
	// Verify commit index
	commitIndex := node.GetCommittedIndex()
	if commitIndex != LogIndex(len(testData)) {
		t.Errorf("Expected commit index %d, got %d", len(testData), commitIndex)
	}
}

// TestRequestVoteRPC tests the RequestVote RPC
func TestRequestVoteRPC(t *testing.T) {
	logger := log.Default()
	
	config := DefaultRaftConfig()
	config.NodeID = "test-node"
	config.Peers = []NodeID{"test-node"}
	
	storage := NewMemoryStorage()
	transport := NewTCPTransport(config, logger)
	stateMachine := NewDatabaseStateMachine(logger)
	
	node, err := NewRaftNode(config, transport, storage, stateMachine, logger)
	if err != nil {
		t.Fatalf("Failed to create Raft node: %v", err)
	}
	defer node.Stop()
	
	// Test RequestVote from higher term
	args := &RequestVoteArgs{
		Term:         1,
		CandidateID:  "candidate",
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	
	reply, err := node.RequestVote(args)
	if err != nil {
		t.Fatalf("RequestVote failed: %v", err)
	}
	
	if !reply.VoteGranted {
		t.Error("Vote should be granted for higher term")
	}
	if reply.Term != 1 {
		t.Errorf("Expected reply term 1, got %d", reply.Term)
	}
	
	// Test RequestVote from same term (should be rejected)
	args2 := &RequestVoteArgs{
		Term:         1,
		CandidateID:  "other-candidate",
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	
	reply2, err := node.RequestVote(args2)
	if err != nil {
		t.Fatalf("RequestVote failed: %v", err)
	}
	
	if reply2.VoteGranted {
		t.Error("Vote should not be granted - already voted")
	}
}

// TestAppendEntriesRPC tests the AppendEntries RPC
func TestAppendEntriesRPC(t *testing.T) {
	logger := log.Default()
	
	config := DefaultRaftConfig()
	config.NodeID = "test-node"
	config.Peers = []NodeID{"test-node"}
	
	storage := NewMemoryStorage()
	transport := NewTCPTransport(config, logger)
	stateMachine := NewDatabaseStateMachine(logger)
	
	node, err := NewRaftNode(config, transport, storage, stateMachine, logger)
	if err != nil {
		t.Fatalf("Failed to create Raft node: %v", err)
	}
	defer node.Stop()
	
	// Test heartbeat (empty AppendEntries)
	args := &AppendEntriesArgs{
		Term:         1,
		LeaderID:     "leader",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []LogEntry{},
		LeaderCommit: 0,
	}
	
	reply, err := node.AppendEntries(args)
	if err != nil {
		t.Fatalf("AppendEntries failed: %v", err)
	}
	
	if !reply.Success {
		t.Error("Heartbeat should succeed")
	}
	if reply.Term != 1 {
		t.Errorf("Expected reply term 1, got %d", reply.Term)
	}
	
	// Verify node became follower
	term, state, leader := node.GetState()
	if state != StateFollower {
		t.Errorf("Expected state FOLLOWER, got %s", state)
	}
	if term != 1 {
		t.Errorf("Expected term 1, got %d", term)
	}
	if leader == nil || *leader != "leader" {
		t.Errorf("Expected leader 'leader', got %v", leader)
	}
}

// TestStorage tests the storage implementations
func TestStorage(t *testing.T) {
	// Test memory storage
	t.Run("MemoryStorage", func(t *testing.T) {
		testStorageImplementation(t, NewMemoryStorage())
	})
	
	// Test file storage
	t.Run("FileStorage", func(t *testing.T) {
		tempDir := t.TempDir()
		storage, err := NewFileStorage(tempDir, log.Default())
		if err != nil {
			t.Fatalf("Failed to create file storage: %v", err)
		}
		defer storage.Close()
		
		testStorageImplementation(t, storage)
	})
}

// testStorageImplementation tests a storage implementation
func testStorageImplementation(t *testing.T, storage Storage) {
	// Test initial state
	state, err := storage.LoadState()
	if err != nil {
		t.Fatalf("Failed to load initial state: %v", err)
	}
	
	if state.CurrentTerm != 0 {
		t.Errorf("Expected initial term 0, got %d", state.CurrentTerm)
	}
	if state.VotedFor != nil {
		t.Errorf("Expected no initial vote, got %v", state.VotedFor)
	}
	if len(state.Log) != 0 {
		t.Errorf("Expected empty initial log, got %d entries", len(state.Log))
	}
	
	// Test saving state
	nodeID := NodeID("test-node")
	newState := &PersistentState{
		CurrentTerm: 5,
		VotedFor:    &nodeID,
		Log: []LogEntry{
			{Index: 1, Term: 1, Type: EntryTypeCommand, Data: []byte("test1")},
			{Index: 2, Term: 2, Type: EntryTypeCommand, Data: []byte("test2")},
		},
	}
	
	if err := storage.SaveState(newState); err != nil {
		t.Fatalf("Failed to save state: %v", err)
	}
	
	// Test loading saved state
	loadedState, err := storage.LoadState()
	if err != nil {
		t.Fatalf("Failed to load saved state: %v", err)
	}
	
	if loadedState.CurrentTerm != 5 {
		t.Errorf("Expected term 5, got %d", loadedState.CurrentTerm)
	}
	if loadedState.VotedFor == nil || *loadedState.VotedFor != nodeID {
		t.Errorf("Expected voted for %s, got %v", nodeID, loadedState.VotedFor)
	}
	if len(loadedState.Log) != 2 {
		t.Errorf("Expected 2 log entries, got %d", len(loadedState.Log))
	}
	
	// Test appending entries
	newEntries := []LogEntry{
		{Index: 3, Term: 3, Type: EntryTypeCommand, Data: []byte("test3")},
		{Index: 4, Term: 3, Type: EntryTypeCommand, Data: []byte("test4")},
	}
	
	if err := storage.AppendEntries(newEntries); err != nil {
		t.Fatalf("Failed to append entries: %v", err)
	}
	
	// Verify entries were appended
	entry, err := storage.GetEntry(3)
	if err != nil {
		t.Fatalf("Failed to get entry 3: %v", err)
	}
	if string(entry.Data) != "test3" {
		t.Errorf("Expected entry data 'test3', got '%s'", string(entry.Data))
	}
	
	// Test getting entry range (now we have 4 entries total: 2 original + 2 appended)
	entries, err := storage.GetEntries(2, 3)
	if err != nil {
		t.Fatalf("Failed to get entries 2-3: %v", err)
	}
	if len(entries) != 2 {
		t.Errorf("Expected 2 entries (indices 2-3), got %d", len(entries))
	}
	
	// Test truncation
	if err := storage.TruncateAfter(2); err != nil {
		t.Fatalf("Failed to truncate after index 2: %v", err)
	}
	
	// Verify truncation
	_, err = storage.GetEntry(3)
	if err == nil {
		t.Error("Expected error getting truncated entry")
	}
}

// TestDatabaseStateMachine tests the database state machine
func TestDatabaseStateMachine(t *testing.T) {
	logger := log.Default()
	stateMachine := NewDatabaseStateMachine(logger)
	
	// Test applying commands
	entries := []*LogEntry{
		{Index: 1, Term: 1, Type: EntryTypeCommand, Data: []byte("command1"), Timestamp: time.Now()},
		{Index: 2, Term: 1, Type: EntryTypeCommand, Data: []byte("command2"), Timestamp: time.Now()},
		{Index: 3, Term: 2, Type: EntryTypeNoOp, Data: nil, Timestamp: time.Now()},
	}
	
	for _, entry := range entries {
		if err := stateMachine.Apply(entry); err != nil {
			t.Fatalf("Failed to apply entry %d: %v", entry.Index, err)
		}
	}
	
	// Verify commands were applied
	if count := stateMachine.GetAppliedCommandCount(); count != 2 {
		t.Errorf("Expected 2 applied commands, got %d", count)
	}
	
	// Test snapshot
	snapshot, err := stateMachine.Snapshot()
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}
	
	if len(snapshot) == 0 {
		t.Error("Snapshot should not be empty")
	}
	
	// Test restore
	if err := stateMachine.Restore(snapshot); err != nil {
		t.Fatalf("Failed to restore from snapshot: %v", err)
	}
	
	// After restore, applied commands should be reset
	if count := stateMachine.GetAppliedCommandCount(); count != 0 {
		t.Errorf("Expected 0 applied commands after restore, got %d", count)
	}
}

// TestTransport tests the TCP transport (basic functionality)
func TestTransport(t *testing.T) {
	logger := log.Default()
	
	config := DefaultRaftConfig()
	config.NodeID = "test-node"
	config.RequestTimeout = 1 * time.Second
	
	transport := NewTCPTransport(config, logger)
	
	// Test start/stop
	if err := transport.Start(""); err != nil {
		t.Fatalf("Failed to start transport: %v", err)
	}
	
	address := transport.GetAddress()
	if address == "" {
		t.Error("Transport should have a listening address")
	}
	
	if err := transport.Stop(); err != nil {
		t.Fatalf("Failed to stop transport: %v", err)
	}
}