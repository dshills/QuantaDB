package raft

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dshills/QuantaDB/internal/log"
)

// RaftNodeImpl implements the RaftNode interface
type RaftNodeImpl struct {
	// Configuration
	config *RaftConfig
	logger log.Logger
	
	// Node identity and networking
	nodeID    NodeID
	transport RaftTransport
	storage   Storage
	stateMachine StateMachine
	
	// State management
	mu    sync.RWMutex
	state NodeState
	
	// Persistent state (protected by mu)
	currentTerm Term
	votedFor    *NodeID
	log         []LogEntry
	
	// Volatile state (protected by mu)
	commitIndex LogIndex
	lastApplied LogIndex
	
	// Leader state (only valid when state == StateLeader)
	nextIndex  map[NodeID]LogIndex
	matchIndex map[NodeID]LogIndex
	
	// Cluster membership
	peers map[NodeID]string // nodeID -> address
	
	// Timing and control
	electionTimer  *time.Timer
	heartbeatTicker *time.Ticker
	
	// Lifecycle
	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once
	
	// Event notification
	eventCh chan ClusterEvent
	
	// Leader tracking
	currentLeader atomic.Value // *NodeID
}

// NewRaftNode creates a new Raft node
func NewRaftNode(
	config *RaftConfig,
	transport RaftTransport,
	storage Storage,
	stateMachine StateMachine,
	logger log.Logger,
) (*RaftNodeImpl, error) {
	
	ctx, cancel := context.WithCancel(context.Background())
	
	node := &RaftNodeImpl{
		config:       config,
		logger:       logger,
		nodeID:       config.NodeID,
		transport:    transport,
		storage:      storage,
		stateMachine: stateMachine,
		state:        StateFollower,
		currentTerm:  0,
		commitIndex:  0,
		lastApplied:  0,
		peers:        make(map[NodeID]string),
		ctx:          ctx,
		cancel:       cancel,
		eventCh:      make(chan ClusterEvent, 100),
	}
	
	// Initialize peer list
	for _, peerID := range config.Peers {
		if peerID != config.NodeID {
			// Get address from config if available
			if addr, exists := config.PeerAddresses[peerID]; exists {
				node.peers[peerID] = addr
			} else {
				node.peers[peerID] = "" // Address will be resolved later
			}
		}
	}
	
	// Load persistent state
	if err := node.loadState(); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to load persistent state: %w", err)
	}
	
	// Set transport handler
	transport.SetRaftNode(node)
	
	return node, nil
}

// Start starts the Raft node
func (n *RaftNodeImpl) Start() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	
	n.logger.Info("Starting Raft node", "nodeID", n.nodeID, "term", n.currentTerm)
	
	// Start transport
	if err := n.transport.Start(n.config.Address); err != nil {
		return fmt.Errorf("failed to start transport: %w", err)
	}
	
	// Start as follower
	n.becomeFollower(n.currentTerm, nil)
	
	// Start background routines
	go n.eventProcessor()
	go n.logApplier()
	
	n.logger.Info("Raft node started", "nodeID", n.nodeID)
	return nil
}

// Stop stops the Raft node
func (n *RaftNodeImpl) Stop() error {
	n.closeOnce.Do(func() {
		n.logger.Info("Stopping Raft node", "nodeID", n.nodeID)
		
		n.cancel()
		
		n.mu.Lock()
		if n.electionTimer != nil {
			n.electionTimer.Stop()
		}
		if n.heartbeatTicker != nil {
			n.heartbeatTicker.Stop()
		}
		n.mu.Unlock()
		
		// Stop transport
		n.transport.Stop()
		
		// Close storage
		if n.storage != nil {
			n.storage.Close()
		}
		
		close(n.eventCh)
		
		n.logger.Info("Raft node stopped", "nodeID", n.nodeID)
	})
	
	return nil
}

// GetState returns current term, state, and leader
func (n *RaftNodeImpl) GetState() (Term, NodeState, *NodeID) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	
	leader := n.getCurrentLeader()
	return n.currentTerm, n.state, leader
}

// IsLeader returns true if this node is the current leader
func (n *RaftNodeImpl) IsLeader() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	
	return n.state == StateLeader
}

// GetLeader returns the current leader
func (n *RaftNodeImpl) GetLeader() *NodeID {
	return n.getCurrentLeader()
}

// GetTerm returns the current term
func (n *RaftNodeImpl) GetTerm() Term {
	n.mu.RLock()
	defer n.mu.RUnlock()
	
	return n.currentTerm
}

// AppendCommand appends a command to the log (only on leader)
func (n *RaftNodeImpl) AppendCommand(data []byte) (LogIndex, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	
	if n.state != StateLeader {
		return 0, fmt.Errorf("not the leader")
	}
	
	// Create new log entry
	entry := LogEntry{
		Index:     n.getLastLogIndex() + 1,
		Term:      n.currentTerm,
		Type:      EntryTypeCommand,
		Data:      data,
		Timestamp: time.Now(),
	}
	
	// Append to local log
	n.log = append(n.log, entry)
	
	// Persist the entry
	if err := n.storage.AppendEntries([]LogEntry{entry}); err != nil {
		// Remove from memory if persistence failed
		n.log = n.log[:len(n.log)-1]
		return 0, fmt.Errorf("failed to persist log entry: %w", err)
	}
	
	n.logger.Debug("Command appended to log", 
		"index", entry.Index, 
		"term", entry.Term,
		"dataSize", len(data))
	
	// Replicate to followers (async)
	go n.replicateToFollowers()
	
	// For single-node clusters, immediately update commit index
	if len(n.peers) == 0 {
		n.updateCommitIndex()
	}
	
	return entry.Index, nil
}

// GetCommittedIndex returns the highest committed log index
func (n *RaftNodeImpl) GetCommittedIndex() LogIndex {
	n.mu.RLock()
	defer n.mu.RUnlock()
	
	return n.commitIndex
}

// GetClusterConfiguration returns current cluster configuration
func (n *RaftNodeImpl) GetClusterConfiguration() *ClusterConfiguration {
	n.mu.RLock()
	defer n.mu.RUnlock()
	
	members := make(map[NodeID]*ClusterMember)
	
	// Add self
	members[n.nodeID] = &ClusterMember{
		NodeID:  n.nodeID,
		Address: "", // Self address
		State:   n.state,
		IsOnline: true,
		LastHeartbeat: time.Now(),
	}
	
	// Add peers
	for peerID, address := range n.peers {
		members[peerID] = &ClusterMember{
			NodeID:   peerID,
			Address:  address,
			State:    StateFollower, // Assume followers unless we know otherwise
			IsOnline: true, // Simplified - would need proper health checking
			LastHeartbeat: time.Now(), // Simplified
		}
	}
	
	leader := n.getCurrentLeader()
	
	return &ClusterConfiguration{
		Members:   members,
		Leader:    leader,
		Term:      n.currentTerm,
		Timestamp: time.Now(),
	}
}

// AddNode adds a new node to the cluster (leader only)
func (n *RaftNodeImpl) AddNode(nodeID NodeID, address string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	
	if n.state != StateLeader {
		return fmt.Errorf("only leader can add nodes")
	}
	
	// Add to peers
	n.peers[nodeID] = address
	
	// Initialize leader state for new node
	if n.nextIndex == nil {
		n.nextIndex = make(map[NodeID]LogIndex)
		n.matchIndex = make(map[NodeID]LogIndex)
	}
	n.nextIndex[nodeID] = n.getLastLogIndex() + 1
	n.matchIndex[nodeID] = 0
	
	n.logger.Info("Added node to cluster", "nodeID", nodeID, "address", address)
	
	// Notify about cluster change
	n.sendEvent(ClusterEvent{
		Type:      EventNodeJoined,
		NodeID:    nodeID,
		Term:      n.currentTerm,
		Timestamp: time.Now(),
	})
	
	return nil
}

// RemoveNode removes a node from the cluster (leader only)
func (n *RaftNodeImpl) RemoveNode(nodeID NodeID) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	
	if n.state != StateLeader {
		return fmt.Errorf("only leader can remove nodes")
	}
	
	// Remove from peers
	delete(n.peers, nodeID)
	
	// Remove from leader state
	if n.nextIndex != nil {
		delete(n.nextIndex, nodeID)
		delete(n.matchIndex, nodeID)
	}
	
	n.logger.Info("Removed node from cluster", "nodeID", nodeID)
	
	// Notify about cluster change
	n.sendEvent(ClusterEvent{
		Type:      EventNodeLeft,
		NodeID:    nodeID,
		Term:      n.currentTerm,
		Timestamp: time.Now(),
	})
	
	return nil
}

// becomeFollower transitions to follower state
func (n *RaftNodeImpl) becomeFollower(term Term, leader *NodeID) {
	n.state = StateFollower
	
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = nil
		n.saveState() // Persist state change
	}
	
	n.setCurrentLeader(leader)
	n.resetElectionTimer()
	
	if n.heartbeatTicker != nil {
		n.heartbeatTicker.Stop()
		n.heartbeatTicker = nil
	}
	
	n.logger.Info("Became follower", "term", n.currentTerm, "leader", leader)
}

// becomeCandidate transitions to candidate state and starts election
func (n *RaftNodeImpl) becomeCandidate() {
	n.state = StateCandidate
	n.currentTerm++
	n.votedFor = &n.nodeID
	n.setCurrentLeader(nil)
	
	n.saveState() // Persist state change
	n.resetElectionTimer()
	
	n.logger.Info("Became candidate", "term", n.currentTerm)
	
	// Start election
	go n.startElection()
}

// becomeLeader transitions to leader state
func (n *RaftNodeImpl) becomeLeader() {
	n.state = StateLeader
	n.setCurrentLeader(&n.nodeID)
	
	// Initialize leader state
	n.nextIndex = make(map[NodeID]LogIndex)
	n.matchIndex = make(map[NodeID]LogIndex)
	
	lastLogIndex := n.getLastLogIndex()
	for peerID := range n.peers {
		n.nextIndex[peerID] = lastLogIndex + 1
		n.matchIndex[peerID] = 0
	}
	
	// Stop election timer, start heartbeat timer
	if n.electionTimer != nil {
		n.electionTimer.Stop()
		n.electionTimer = nil
	}
	
	n.startHeartbeatTimer()
	
	n.logger.Info("Became leader", "term", n.currentTerm)
	
	// Send initial heartbeat
	go n.sendHeartbeat()
	
	// Notify about leadership
	n.sendEvent(ClusterEvent{
		Type:      EventLeaderElected,
		NodeID:    n.nodeID,
		Term:      n.currentTerm,
		Timestamp: time.Now(),
	})
}

// Helper methods

func (n *RaftNodeImpl) getCurrentLeader() *NodeID {
	if leader := n.currentLeader.Load(); leader != nil {
		leaderID := leader.(*NodeID)
		return leaderID
	}
	return nil
}

func (n *RaftNodeImpl) setCurrentLeader(leader *NodeID) {
	n.currentLeader.Store(leader)
}

func (n *RaftNodeImpl) getLastLogIndex() LogIndex {
	if len(n.log) == 0 {
		return 0
	}
	return n.log[len(n.log)-1].Index
}

func (n *RaftNodeImpl) getLastLogTerm() Term {
	if len(n.log) == 0 {
		return 0
	}
	return n.log[len(n.log)-1].Term
}

func (n *RaftNodeImpl) loadState() error {
	state, err := n.storage.LoadState()
	if err != nil {
		// If no state exists, start with defaults
		n.currentTerm = 0
		n.votedFor = nil
		n.log = []LogEntry{}
		return nil
	}
	
	n.currentTerm = state.CurrentTerm
	n.votedFor = state.VotedFor
	n.log = state.Log
	
	return nil
}

func (n *RaftNodeImpl) saveState() {
	state := &PersistentState{
		CurrentTerm: n.currentTerm,
		VotedFor:    n.votedFor,
		Log:         n.log,
	}
	
	if err := n.storage.SaveState(state); err != nil {
		n.logger.Error("Failed to save persistent state", "error", err)
	}
}

func (n *RaftNodeImpl) sendEvent(event ClusterEvent) {
	select {
	case n.eventCh <- event:
	default:
		// Event channel full, drop event
		n.logger.Warn("Event channel full, dropping event", "type", event.Type)
	}
}

func (n *RaftNodeImpl) resetElectionTimer() {
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}
	
	// Randomize election timeout to avoid split votes
	timeout := n.config.ElectionTimeout + time.Duration(rand.Intn(150))*time.Millisecond
	
	n.electionTimer = time.AfterFunc(timeout, func() {
		n.mu.Lock()
		defer n.mu.Unlock()
		
		if n.state == StateFollower || n.state == StateCandidate {
			n.becomeCandidate()
		}
	})
}

func (n *RaftNodeImpl) startHeartbeatTimer() {
	if n.heartbeatTicker != nil {
		n.heartbeatTicker.Stop()
	}
	
	n.heartbeatTicker = time.NewTicker(n.config.HeartbeatInterval)
	
	go func() {
		for {
			select {
			case <-n.heartbeatTicker.C:
				n.mu.RLock()
				isLeader := n.state == StateLeader
				n.mu.RUnlock()
				
				if isLeader {
					go n.sendHeartbeat()
				} else {
					return
				}
				
			case <-n.ctx.Done():
				return
			}
		}
	}()
}

// Background goroutines

// eventProcessor processes cluster events
func (n *RaftNodeImpl) eventProcessor() {
	for {
		select {
		case event := <-n.eventCh:
			n.logger.Debug("Processing cluster event", 
				"type", event.Type, 
				"nodeID", event.NodeID,
				"term", event.Term)
			
			// Application can listen to these events for notifications
			// For now, we just log them
			
		case <-n.ctx.Done():
			return
		}
	}
}

// logApplier applies committed log entries to the state machine
func (n *RaftNodeImpl) logApplier() {
	for {
		select {
		case <-time.After(10 * time.Millisecond):
			n.applyCommittedEntries()
			
		case <-n.ctx.Done():
			return
		}
	}
}

// applyCommittedEntries applies newly committed entries to the state machine
func (n *RaftNodeImpl) applyCommittedEntries() {
	n.mu.Lock()
	defer n.mu.Unlock()
	
	// Apply entries from lastApplied+1 to commitIndex
	for index := n.lastApplied + 1; index <= n.commitIndex; index++ {
		if index > LogIndex(len(n.log)) {
			// Should not happen, but handle gracefully
			n.logger.Warn("Commit index beyond log length", 
				"commitIndex", n.commitIndex,
				"logLength", len(n.log))
			break
		}
		
		entry := &n.log[index-1] // Convert to 0-based indexing
		
		// Apply to state machine
		if n.stateMachine != nil {
			if err := n.stateMachine.Apply(entry); err != nil {
				n.logger.Error("Failed to apply log entry to state machine", 
					"index", index, 
					"error", err)
				// Continue applying other entries
			}
		}
		
		n.lastApplied = index
		
		n.logger.Debug("Applied log entry", 
			"index", index, 
			"term", entry.Term,
			"type", entry.Type)
	}
}