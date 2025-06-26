package routing

import (
	"fmt"
	"sync"
	"time"
)

// PooledConnection represents a connection in the pool
type PooledConnection struct {
	id        string
	nodeID    string
	isActive  bool
	created   time.Time
	lastUsed  time.Time
	queryCount int64
}

// ConnectionPool manages a pool of connections to a database node
type ConnectionPool struct {
	nodeID      string
	address     string
	maxConns    int
	idleTimeout time.Duration
	
	mu          sync.RWMutex
	connections []*PooledConnection
	activeCount int
	totalCreated int64
	totalReused  int64
	created     time.Time
	lastCleanup time.Time
}

// NewConnectionPool creates a new connection pool
func NewConnectionPool(nodeID, address string, config *QueryRouterConfig) *ConnectionPool {
	return &ConnectionPool{
		nodeID:      nodeID,
		address:     address,
		maxConns:    config.MaxConnectionsPerNode,
		idleTimeout: config.IdleTimeout,
		connections: make([]*PooledConnection, 0, config.MaxConnectionsPerNode),
		created:     time.Now(),
		lastCleanup: time.Now(),
	}
}

// GetConnection gets a connection from the pool or creates a new one
func (cp *ConnectionPool) GetConnection() (*PooledConnection, error) {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	// Try to reuse an existing idle connection
	for i, conn := range cp.connections {
		if !conn.isActive {
			// Reuse this connection
			conn.isActive = true
			conn.lastUsed = time.Now()
			conn.queryCount++
			cp.activeCount++
			cp.totalReused++
			
			// Remove from idle pool
			cp.connections = append(cp.connections[:i], cp.connections[i+1:]...)
			
			return conn, nil
		}
	}

	// Create new connection if under limit
	if cp.activeCount < cp.maxConns {
		conn := &PooledConnection{
			id:         fmt.Sprintf("%s-conn-%d", cp.nodeID, cp.totalCreated),
			nodeID:     cp.nodeID,
			isActive:   true,
			created:    time.Now(),
			lastUsed:   time.Now(),
			queryCount: 1,
		}
		
		cp.activeCount++
		cp.totalCreated++
		
		return conn, nil
	}

	return nil, fmt.Errorf("connection pool exhausted for node %s", cp.nodeID)
}

// ReturnConnection returns a connection to the pool
func (cp *ConnectionPool) ReturnConnection(conn *PooledConnection) {
	if conn == nil {
		return
	}

	cp.mu.Lock()
	defer cp.mu.Unlock()

	if conn.isActive {
		conn.isActive = false
		conn.lastUsed = time.Now()
		cp.activeCount--
		
		// Add back to idle pool if under limit
		if len(cp.connections) < cp.maxConns {
			cp.connections = append(cp.connections, conn)
		}
		// If pool is full, let the connection be garbage collected
	}
}

// CleanupIdleConnections removes connections that have been idle too long
func (cp *ConnectionPool) CleanupIdleConnections() {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	now := time.Now()
	if now.Sub(cp.lastCleanup) < time.Minute {
		return // Don't cleanup too frequently
	}

	cleanedCount := 0
	newConnections := make([]*PooledConnection, 0, len(cp.connections))

	for _, conn := range cp.connections {
		if !conn.isActive && now.Sub(conn.lastUsed) > cp.idleTimeout {
			// Connection has been idle too long, remove it
			cleanedCount++
		} else {
			newConnections = append(newConnections, conn)
		}
	}

	cp.connections = newConnections
	cp.lastCleanup = now

	if cleanedCount > 0 {
		// Note: In a real implementation, we would properly close the actual connections here
	}
}

// Close closes the connection pool and all connections
func (cp *ConnectionPool) Close() {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	// In a real implementation, we would close all actual database connections here
	cp.connections = nil
	cp.activeCount = 0
}

// GetStats returns statistics about the connection pool
func (cp *ConnectionPool) GetStats() map[string]interface{} {
	cp.mu.RLock()
	defer cp.mu.RUnlock()

	return map[string]interface{}{
		"node_id":        cp.nodeID,
		"address":        cp.address,
		"max_connections": cp.maxConns,
		"active_connections": cp.activeCount,
		"idle_connections": len(cp.connections),
		"total_created":  cp.totalCreated,
		"total_reused":   cp.totalReused,
		"created":        cp.created,
		"last_cleanup":   cp.lastCleanup,
		"idle_timeout":   cp.idleTimeout,
	}
}