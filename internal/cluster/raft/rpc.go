package raft

import (
	"fmt"
	"time"
)

// RequestVote RPC implementation
func (n *RaftNodeImpl) RequestVote(args *RequestVoteArgs) (*RequestVoteReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.logger.Debug("Received RequestVote",
		"from", args.CandidateID,
		"term", args.Term,
		"lastLogIndex", args.LastLogIndex,
		"lastLogTerm", args.LastLogTerm)

	reply := &RequestVoteReply{
		Term:        n.currentTerm,
		VoteGranted: false,
	}

	// Rule 1: Reply false if term < currentTerm
	if args.Term < n.currentTerm {
		n.logger.Debug("Rejecting vote - stale term",
			"candidateTerm", args.Term,
			"currentTerm", n.currentTerm)
		return reply, nil
	}

	// If RPC request contains term T > currentTerm: set currentTerm = T, convert to follower
	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term, nil)
		reply.Term = n.currentTerm
	}

	// Rule 2: If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote
	voteGranted := false

	if n.votedFor == nil || *n.votedFor == args.CandidateID {
		// Check if candidate's log is at least as up-to-date
		lastLogIndex := n.getLastLogIndex()
		lastLogTerm := n.getLastLogTerm()

		candidateLogUpToDate := false
		if args.LastLogTerm > lastLogTerm {
			candidateLogUpToDate = true
		} else if args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex {
			candidateLogUpToDate = true
		}

		if candidateLogUpToDate {
			voteGranted = true
			n.votedFor = &args.CandidateID
			n.saveState()          // Persist vote
			n.resetElectionTimer() // Reset election timer when granting vote

			n.logger.Info("Granted vote",
				"to", args.CandidateID,
				"term", args.Term)
		} else {
			n.logger.Debug("Rejecting vote - candidate log not up-to-date",
				"candidateLastTerm", args.LastLogTerm,
				"candidateLastIndex", args.LastLogIndex,
				"ourLastTerm", lastLogTerm,
				"ourLastIndex", lastLogIndex)
		}
	} else {
		n.logger.Debug("Rejecting vote - already voted",
			"votedFor", *n.votedFor,
			"candidate", args.CandidateID)
	}

	reply.VoteGranted = voteGranted
	return reply, nil
}

// AppendEntries RPC implementation
func (n *RaftNodeImpl) AppendEntries(args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	isHeartbeat := len(args.Entries) == 0

	if !isHeartbeat {
		n.logger.Debug("Received AppendEntries",
			"from", args.LeaderID,
			"term", args.Term,
			"prevLogIndex", args.PrevLogIndex,
			"prevLogTerm", args.PrevLogTerm,
			"entriesCount", len(args.Entries),
			"leaderCommit", args.LeaderCommit)
	}

	reply := &AppendEntriesReply{
		Term:    n.currentTerm,
		Success: false,
	}

	// Rule 1: Reply false if term < currentTerm
	if args.Term < n.currentTerm {
		if !isHeartbeat {
			n.logger.Debug("Rejecting AppendEntries - stale term",
				"leaderTerm", args.Term,
				"currentTerm", n.currentTerm)
		}
		return reply, nil
	}

	// If RPC request contains term T > currentTerm: set currentTerm = T, convert to follower
	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term, &args.LeaderID)
		reply.Term = n.currentTerm
	} else if n.state != StateFollower {
		// Convert to follower if we receive AppendEntries from current term leader
		n.becomeFollower(args.Term, &args.LeaderID)
	} else {
		// Update leader and reset election timer
		n.setCurrentLeader(&args.LeaderID)
		n.resetElectionTimer()
	}

	// Rule 2: Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	if args.PrevLogIndex > 0 {
		if args.PrevLogIndex > n.getLastLogIndex() {
			// Optimization: tell leader about our last log index
			reply.ConflictIndex = n.getLastLogIndex() + 1
			reply.ConflictTerm = 0

			if !isHeartbeat {
				n.logger.Debug("Rejecting AppendEntries - missing prev log entry",
					"prevLogIndex", args.PrevLogIndex,
					"ourLastIndex", n.getLastLogIndex())
			}
			return reply, nil
		}

		// Convert to 0-based indexing for our log slice
		prevLogSliceIndex := args.PrevLogIndex - 1
		if prevLogSliceIndex >= LogIndex(len(n.log)) || n.log[prevLogSliceIndex].Term != args.PrevLogTerm {
			// Find the first index of the conflicting term
			conflictTerm := Term(0)
			if prevLogSliceIndex < LogIndex(len(n.log)) {
				conflictTerm = n.log[prevLogSliceIndex].Term
			}

			conflictIndex := LogIndex(1)
			for i := range n.log {
				if n.log[i].Term == conflictTerm {
					conflictIndex = LogIndex(i + 1) // Convert back to 1-based
					break
				}
			}

			reply.ConflictIndex = conflictIndex
			reply.ConflictTerm = conflictTerm

			if !isHeartbeat {
				n.logger.Debug("Rejecting AppendEntries - term mismatch",
					"prevLogIndex", args.PrevLogIndex,
					"prevLogTerm", args.PrevLogTerm,
					"ourTerm", conflictTerm,
					"conflictIndex", conflictIndex)
			}
			return reply, nil
		}
	}

	// Rule 3 & 4: If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it, then append new entries
	if len(args.Entries) > 0 {
		// Find insertion point
		insertIndex := args.PrevLogIndex + 1
		newEntries := make([]LogEntry, 0, len(args.Entries))

		for i, entry := range args.Entries {
			entryIndex := insertIndex + LogIndex(i)
			sliceIndex := entryIndex - 1 // Convert to 0-based

			// If we have an entry at this index, check for conflicts
			if sliceIndex < LogIndex(len(n.log)) {
				if n.log[sliceIndex].Term != entry.Term {
					// Conflict: truncate log from this point
					n.log = n.log[:sliceIndex]

					// Persist truncation
					if err := n.storage.TruncateAfter(sliceIndex); err != nil {
						n.logger.Error("Failed to truncate log", "error", err)
						return reply, fmt.Errorf("failed to truncate log: %w", err)
					}

					n.logger.Debug("Truncated log due to conflict",
						"at", entryIndex,
						"newLength", len(n.log))
				} else {
					// No conflict, skip this entry (already have it)
					continue
				}
			}

			// Append new entry
			newEntries = append(newEntries, entry)
		}

		if len(newEntries) > 0 {
			// Append new entries to log
			n.log = append(n.log, newEntries...)

			// Persist new entries
			if err := n.storage.AppendEntries(newEntries); err != nil {
				// Rollback memory changes
				n.log = n.log[:len(n.log)-len(newEntries)]
				n.logger.Error("Failed to persist log entries", "error", err)
				return reply, fmt.Errorf("failed to persist log entries: %w", err)
			}

			n.logger.Debug("Appended entries to log",
				"count", len(newEntries),
				"startIndex", insertIndex,
				"newLogLength", len(n.log))
		}
	}

	// Rule 5: If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > n.commitIndex {
		oldCommitIndex := n.commitIndex
		n.commitIndex = args.LeaderCommit

		// Don't commit beyond our log
		if lastLogIndex := n.getLastLogIndex(); n.commitIndex > lastLogIndex {
			n.commitIndex = lastLogIndex
		}

		if n.commitIndex > oldCommitIndex {
			n.logger.Debug("Updated commit index",
				"from", oldCommitIndex,
				"to", n.commitIndex)

			// Signal log applier
			go n.applyCommittedEntries()
		}
	}

	reply.Success = true
	return reply, nil
}

// startElection starts a new election
func (n *RaftNodeImpl) startElection() {
	n.mu.RLock()
	currentTerm := n.currentTerm
	lastLogIndex := n.getLastLogIndex()
	lastLogTerm := n.getLastLogTerm()
	peers := make([]NodeID, 0, len(n.peers))
	for peerID := range n.peers {
		peers = append(peers, peerID)
	}
	n.mu.RUnlock()

	n.logger.Info("Starting election", "term", currentTerm)

	// Vote for self (already done in becomeCandidate)
	votes := 1
	votesNeeded := (len(peers) + 2) / 2 // +1 for self, +1 for majority

	if votes >= votesNeeded {
		// We have enough votes already (single node cluster)
		n.mu.Lock()
		if n.state == StateCandidate && n.currentTerm == currentTerm {
			n.becomeLeader()
		}
		n.mu.Unlock()
		return
	}

	// Send RequestVote RPCs to all peers
	voteCh := make(chan bool, len(peers))

	for _, peerID := range peers {
		go func(peer NodeID) {
			args := &RequestVoteArgs{
				Term:         currentTerm,
				CandidateID:  n.nodeID,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}

			reply, err := n.transport.SendRequestVote(peer, args)
			if err != nil {
				n.logger.Debug("RequestVote failed", "peer", peer, "error", err)
				voteCh <- false
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			// Check if we're still a candidate in the same term
			if n.state != StateCandidate || n.currentTerm != currentTerm {
				voteCh <- false
				return
			}

			// If peer has higher term, become follower
			if reply.Term > n.currentTerm {
				n.becomeFollower(reply.Term, nil)
				voteCh <- false
				return
			}

			voteCh <- reply.VoteGranted
		}(peerID)
	}

	// Collect votes with timeout
	timeout := time.After(n.config.RequestTimeout)

	for i := 0; i < len(peers); i++ {
		select {
		case voteGranted := <-voteCh:
			if voteGranted {
				votes++
				if votes >= votesNeeded {
					// Won the election
					n.mu.Lock()
					if n.state == StateCandidate && n.currentTerm == currentTerm {
						n.becomeLeader()
					}
					n.mu.Unlock()
					return
				}
			}

		case <-timeout:
			n.logger.Debug("Election timeout", "term", currentTerm, "votes", votes)
			return

		case <-n.ctx.Done():
			return
		}
	}

	n.logger.Debug("Election completed without majority", "term", currentTerm, "votes", votes, "needed", votesNeeded)
}

// sendHeartbeat sends heartbeat to all peers
func (n *RaftNodeImpl) sendHeartbeat() {
	n.mu.RLock()
	if n.state != StateLeader {
		n.mu.RUnlock()
		return
	}

	currentTerm := n.currentTerm
	commitIndex := n.commitIndex
	peers := make([]NodeID, 0, len(n.peers))
	for peerID := range n.peers {
		peers = append(peers, peerID)
	}
	n.mu.RUnlock()

	// Send heartbeat to each peer
	for _, peerID := range peers {
		go func(peer NodeID) {
			n.sendAppendEntriesToPeer(peer, currentTerm, commitIndex)
		}(peerID)
	}
}

// sendAppendEntriesToPeer sends AppendEntries RPC to a specific peer
func (n *RaftNodeImpl) sendAppendEntriesToPeer(peerID NodeID, term Term, leaderCommit LogIndex) {
	n.mu.RLock()
	if n.state != StateLeader || n.currentTerm != term {
		n.mu.RUnlock()
		return
	}

	nextIndex := n.nextIndex[peerID]
	prevLogIndex := nextIndex - 1
	prevLogTerm := Term(0)

	if prevLogIndex > 0 {
		if prevLogIndex <= LogIndex(len(n.log)) {
			prevLogTerm = n.log[prevLogIndex-1].Term
		} else {
			// Should not happen, but handle gracefully
			nextIndex = LogIndex(len(n.log)) + 1
			prevLogIndex = nextIndex - 1
			if prevLogIndex > 0 {
				prevLogTerm = n.log[prevLogIndex-1].Term
			}
		}
	}

	// Collect entries to send
	var entries []LogEntry
	if nextIndex <= LogIndex(len(n.log)) {
		maxEntries := n.config.MaxLogEntries
		endIndex := nextIndex + LogIndex(maxEntries) - 1
		if endIndex > LogIndex(len(n.log)) {
			endIndex = LogIndex(len(n.log))
		}

		entries = make([]LogEntry, endIndex-nextIndex+1)
		copy(entries, n.log[nextIndex-1:endIndex])
	}

	n.mu.RUnlock()

	args := &AppendEntriesArgs{
		Term:         term,
		LeaderID:     n.nodeID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}

	reply, err := n.transport.SendAppendEntries(peerID, args)
	if err != nil {
		n.logger.Debug("AppendEntries failed", "peer", peerID, "error", err)
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	// Check if we're still leader in the same term
	if n.state != StateLeader || n.currentTerm != term {
		return
	}

	// If peer has higher term, become follower
	if reply.Term > n.currentTerm {
		n.becomeFollower(reply.Term, nil)
		return
	}

	if reply.Success {
		// Update nextIndex and matchIndex for this peer
		if len(entries) > 0 {
			n.nextIndex[peerID] = entries[len(entries)-1].Index + 1
			n.matchIndex[peerID] = entries[len(entries)-1].Index

			// Check if we can advance commit index
			n.updateCommitIndex()
		}
	} else {
		// Log replication failed, decrement nextIndex and retry
		if reply.ConflictTerm != 0 {
			// Fast backup using conflict optimization
			conflictIndex := reply.ConflictIndex

			// Look for last entry with ConflictTerm
			for i := len(n.log) - 1; i >= 0; i-- {
				if n.log[i].Term == reply.ConflictTerm {
					conflictIndex = LogIndex(i + 2) // +1 for 1-based indexing, +1 for next
					break
				}
			}

			n.nextIndex[peerID] = conflictIndex
		} else {
			// Simple backup
			if n.nextIndex[peerID] > 1 {
				n.nextIndex[peerID]--
			}
		}

		// Retry immediately
		go n.sendAppendEntriesToPeer(peerID, term, leaderCommit)
	}
}

// updateCommitIndex updates the commit index based on majority agreement
func (n *RaftNodeImpl) updateCommitIndex() {
	if n.state != StateLeader {
		return
	}

	// Find the highest index that's replicated on a majority of servers
	lastLogIndex := n.getLastLogIndex()

	for index := n.commitIndex + 1; index <= lastLogIndex; index++ {
		// Count how many servers have this index
		count := 1 // Leader always has its own entries

		for _, matchIndex := range n.matchIndex {
			if matchIndex >= index {
				count++
			}
		}

		majority := (len(n.peers) + 2) / 2 // +1 for self

		if count >= majority {
			// Check that the entry is from current term (safety requirement)
			if n.log[index-1].Term == n.currentTerm {
				oldCommitIndex := n.commitIndex
				n.commitIndex = index

				n.logger.Debug("Advanced commit index",
					"from", oldCommitIndex,
					"to", n.commitIndex,
					"replicationCount", count)

				// Apply newly committed entries
				go n.applyCommittedEntries()
			}
		} else {
			break // No point checking higher indices
		}
	}
}

// replicateToFollowers triggers replication to all followers
func (n *RaftNodeImpl) replicateToFollowers() {
	n.mu.RLock()
	if n.state != StateLeader {
		n.mu.RUnlock()
		return
	}

	currentTerm := n.currentTerm
	commitIndex := n.commitIndex
	peers := make([]NodeID, 0, len(n.peers))
	for peerID := range n.peers {
		peers = append(peers, peerID)
	}
	n.mu.RUnlock()

	for _, peerID := range peers {
		go n.sendAppendEntriesToPeer(peerID, currentTerm, commitIndex)
	}
}
