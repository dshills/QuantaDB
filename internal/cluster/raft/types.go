package raft

import (
	"time"

	"github.com/dshills/QuantaDB/internal/cluster/replication"
)

// NodeID represents a unique identifier for a Raft node
type NodeID string

// Term represents a Raft term number
type Term uint64

// LogIndex represents an index in the Raft log
type LogIndex uint64

// NodeState represents the current state of a Raft node
type NodeState int

const (
	StateFollower NodeState = iota
	StateCandidate
	StateLeader
)

// String returns string representation of node state
func (s NodeState) String() string {
	switch s {
	case StateFollower:
		return "FOLLOWER"
	case StateCandidate:
		return "CANDIDATE"
	case StateLeader:
		return "LEADER"
	default:
		return "UNKNOWN"
	}
}

// RaftConfig holds configuration for the Raft consensus algorithm
type RaftConfig struct {
	// Node identification
	NodeID NodeID

	// Network configuration
	Address string // Address to listen on for Raft RPCs

	// Cluster membership
	Peers         []NodeID
	PeerAddresses map[NodeID]string // Map of peer IDs to their addresses

	// Timing configuration
	ElectionTimeout   time.Duration // Time to wait before starting election
	HeartbeatInterval time.Duration // Interval between heartbeats
	RequestTimeout    time.Duration // Timeout for RPC requests

	// Log configuration
	MaxLogEntries      int      // Maximum entries to send in one AppendEntries RPC
	LogCompactionIndex LogIndex // Index below which logs can be compacted

	// Storage configuration
	DataDir string // Directory for persistent state
}

// DefaultRaftConfig returns default Raft configuration
func DefaultRaftConfig() *RaftConfig {
	return &RaftConfig{
		ElectionTimeout:    150 * time.Millisecond,
		HeartbeatInterval:  50 * time.Millisecond,
		RequestTimeout:     100 * time.Millisecond,
		MaxLogEntries:      100,
		LogCompactionIndex: 0,
		DataDir:            "./raft",
		PeerAddresses:      make(map[NodeID]string),
	}
}

// PersistentState represents the persistent Raft state that survives restarts
type PersistentState struct {
	CurrentTerm Term       // Latest term server has seen
	VotedFor    *NodeID    // CandidateID that received vote in current term (null if none)
	Log         []LogEntry // Log entries
}

// VolatileState represents the volatile Raft state
type VolatileState struct {
	CommitIndex LogIndex // Index of highest log entry known to be committed
	LastApplied LogIndex // Index of highest log entry applied to state machine
}

// LeaderState represents volatile state specific to leaders
type LeaderState struct {
	NextIndex  map[NodeID]LogIndex // For each server, index of next log entry to send
	MatchIndex map[NodeID]LogIndex // For each server, index of highest log entry known to be replicated
}

// LogEntry represents a single entry in the Raft log
type LogEntry struct {
	Index     LogIndex  // Index of this entry
	Term      Term      // Term when entry was received by leader
	Type      EntryType // Type of log entry
	Data      []byte    // Command or data
	Timestamp time.Time // When entry was created
}

// EntryType represents the type of a log entry
type EntryType int

const (
	EntryTypeCommand       EntryType = iota // Regular command
	EntryTypeConfiguration                  // Cluster configuration change
	EntryTypeNoOp                           // No-operation (used for heartbeats)
)

// String returns string representation of entry type
func (t EntryType) String() string {
	switch t {
	case EntryTypeCommand:
		return "COMMAND"
	case EntryTypeConfiguration:
		return "CONFIGURATION"
	case EntryTypeNoOp:
		return "NO_OP"
	default:
		return "UNKNOWN"
	}
}

// RequestVoteArgs represents arguments for RequestVote RPC
type RequestVoteArgs struct {
	Term         Term     // Candidate's term
	CandidateID  NodeID   // Candidate requesting vote
	LastLogIndex LogIndex // Index of candidate's last log entry
	LastLogTerm  Term     // Term of candidate's last log entry
}

// RequestVoteReply represents response for RequestVote RPC
type RequestVoteReply struct {
	Term        Term // Current term, for candidate to update itself
	VoteGranted bool // True means candidate received vote
}

// AppendEntriesArgs represents arguments for AppendEntries RPC
type AppendEntriesArgs struct {
	Term         Term       // Leader's term
	LeaderID     NodeID     // So follower can redirect clients
	PrevLogIndex LogIndex   // Index of log entry immediately preceding new ones
	PrevLogTerm  Term       // Term of prevLogIndex entry
	Entries      []LogEntry // Log entries to store (empty for heartbeat)
	LeaderCommit LogIndex   // Leader's commitIndex
}

// AppendEntriesReply represents response for AppendEntries RPC
type AppendEntriesReply struct {
	Term    Term // Current term, for leader to update itself
	Success bool // True if follower contained entry matching prevLogIndex and prevLogTerm

	// Optimization fields for faster log backtracking
	ConflictIndex LogIndex // Index of first entry in conflicting term
	ConflictTerm  Term     // Term of conflicting entry
}

// ClusterMember represents a member of the Raft cluster
type ClusterMember struct {
	NodeID  NodeID
	Address string
	State   NodeState

	// Leader tracking
	LastHeartbeat time.Time
	IsOnline      bool

	// Replication tracking (for leaders)
	NextIndex  LogIndex
	MatchIndex LogIndex
}

// ClusterConfiguration represents the current cluster configuration
type ClusterConfiguration struct {
	Members   map[NodeID]*ClusterMember
	Leader    *NodeID
	Term      Term
	Timestamp time.Time
}

// RaftNode interface defines the contract for Raft consensus
type RaftNode interface {
	// Node lifecycle
	Start() error
	Stop() error

	// State queries
	GetState() (Term, NodeState, *NodeID)
	IsLeader() bool
	GetLeader() *NodeID
	GetTerm() Term

	// Log operations
	AppendCommand(data []byte) (LogIndex, error)
	GetCommittedIndex() LogIndex

	// Cluster membership
	GetClusterConfiguration() *ClusterConfiguration
	AddNode(nodeID NodeID, address string) error
	RemoveNode(nodeID NodeID) error

	// RPC endpoints
	RequestVote(args *RequestVoteArgs) (*RequestVoteReply, error)
	AppendEntries(args *AppendEntriesArgs) (*AppendEntriesReply, error)
}

// RaftTransport interface defines the network transport for Raft RPCs
type RaftTransport interface {
	// Send RPCs to other nodes
	SendRequestVote(nodeID NodeID, args *RequestVoteArgs) (*RequestVoteReply, error)
	SendAppendEntries(nodeID NodeID, args *AppendEntriesArgs) (*AppendEntriesReply, error)

	// Start/stop transport
	Start(address string) error
	Stop() error

	// Set the Raft node that will handle incoming RPCs
	SetRaftNode(node RaftNode)
}

// StateMachine interface defines the application state machine
type StateMachine interface {
	// Apply a committed log entry to the state machine
	Apply(entry *LogEntry) error

	// Take a snapshot of the current state
	Snapshot() ([]byte, error)

	// Restore state from a snapshot
	Restore(snapshot []byte) error
}

// Storage interface defines persistent storage for Raft state
type Storage interface {
	// Persistent state
	SaveState(state *PersistentState) error
	LoadState() (*PersistentState, error)

	// Log operations
	AppendEntries(entries []LogEntry) error
	GetEntry(index LogIndex) (*LogEntry, error)
	GetEntries(start, end LogIndex) ([]LogEntry, error)
	GetLastEntry() (*LogEntry, error)
	TruncateAfter(index LogIndex) error

	// Snapshots
	SaveSnapshot(index LogIndex, term Term, data []byte) error
	LoadSnapshot() (LogIndex, Term, []byte, error)

	// Cleanup
	CompactLog(index LogIndex) error
	Close() error
}

// ReplicationIntegration connects Raft consensus with database replication
type ReplicationIntegration struct {
	RaftNode           RaftNode
	ReplicationManager replication.ReplicationManager
}

// ClusterEvent represents events in the cluster
type ClusterEvent struct {
	Type      ClusterEventType
	NodeID    NodeID
	Term      Term
	Timestamp time.Time
	Data      interface{}
}

// ClusterEventType represents different types of cluster events
type ClusterEventType int

const (
	EventLeaderElected ClusterEventType = iota
	EventLeaderLost
	EventNodeJoined
	EventNodeLeft
	EventTermChanged
)

// String returns string representation of cluster event type
func (t ClusterEventType) String() string {
	switch t {
	case EventLeaderElected:
		return "LEADER_ELECTED"
	case EventLeaderLost:
		return "LEADER_LOST"
	case EventNodeJoined:
		return "NODE_JOINED"
	case EventNodeLeft:
		return "NODE_LEFT"
	case EventTermChanged:
		return "TERM_CHANGED"
	default:
		return "UNKNOWN"
	}
}
