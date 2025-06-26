package routing

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/dshills/QuantaDB/internal/log"
	"github.com/dshills/QuantaDB/internal/sql/parser"
)

// QueryRouter intelligently routes queries to appropriate nodes
type QueryRouter struct {
	config          *QueryRouterConfig
	logger          log.Logger
	mu              sync.RWMutex
	
	// Node management
	primaryNode     *NodeInfo
	replicaNodes    map[string]*NodeInfo
	nodeConnections map[string]*ConnectionPool
	
	// Load balancing state
	loadBalancer    LoadBalancer
	healthMonitor   *HealthMonitor
	
	// Statistics
	stats           *RouterStatistics
	
	// Control
	stopCh          chan struct{}
	stopOnce        sync.Once
}

// QueryRouterConfig contains configuration for the query router
type QueryRouterConfig struct {
	// Load balancing strategy
	LoadBalancingStrategy LoadBalancingStrategy
	
	// Health monitoring
	HealthCheckInterval   time.Duration
	HealthCheckTimeout    time.Duration
	MaxFailureThreshold   int
	
	// Connection pooling
	MaxConnectionsPerNode int
	ConnectionTimeout     time.Duration
	IdleTimeout          time.Duration
	MaxIdleConnections   int
	
	// Query routing
	EnableReadReplicas   bool
	PreferLocalReplicas  bool
	MaxReplicationLag    time.Duration
	
	// Retry configuration
	MaxRetries           int
	RetryDelay          time.Duration
	
	// Circuit breaker
	EnableCircuitBreaker bool
	CircuitBreakerThreshold int
	CircuitBreakerTimeout   time.Duration
}

// LoadBalancingStrategy defines the load balancing approach
type LoadBalancingStrategy int

const (
	RoundRobin LoadBalancingStrategy = iota
	LeastConnections
	WeightedRoundRobin
	ResponseTimeWeighted
	HealthAware
)

// NodeInfo contains information about a database node
type NodeInfo struct {
	ID           string
	Address      string
	Role         NodeRole
	IsHealthy    bool
	LastSeen     time.Time
	
	// Performance metrics
	ResponseTime    time.Duration
	ActiveConns     int
	TotalQueries    int64
	FailedQueries   int64
	ReplicationLag  time.Duration
	
	// Load balancing weights
	Weight          int
	LoadScore       float64
	
	// Circuit breaker state
	CircuitState    CircuitState
	FailureCount    int
	LastFailure     time.Time
}

// NodeRole defines the role of a node in the cluster
type NodeRole int

const (
	Primary NodeRole = iota
	Replica
	Standby
)

// CircuitState represents the circuit breaker state
type CircuitState int

const (
	CircuitClosed CircuitState = iota
	CircuitOpen
	CircuitHalfOpen
)


// RouterStatistics tracks routing performance metrics
type RouterStatistics struct {
	mu sync.RWMutex
	
	TotalQueries       int64
	ReadQueries        int64
	WriteQueries       int64
	FailedQueries      int64
	
	RoutingDecisions   map[string]int64
	NodeUtilization    map[string]float64
	AverageResponseTime time.Duration
	
	LoadBalancerStats  map[LoadBalancingStrategy]int64
	HealthCheckStats   map[string]int64
}

// DefaultQueryRouterConfig returns sensible defaults
func DefaultQueryRouterConfig() *QueryRouterConfig {
	return &QueryRouterConfig{
		LoadBalancingStrategy:   HealthAware,
		HealthCheckInterval:     10 * time.Second,
		HealthCheckTimeout:      3 * time.Second,
		MaxFailureThreshold:     3,
		MaxConnectionsPerNode:   20,
		ConnectionTimeout:       30 * time.Second,
		IdleTimeout:            5 * time.Minute,
		MaxIdleConnections:     5,
		EnableReadReplicas:     true,
		PreferLocalReplicas:    false,
		MaxReplicationLag:      1 * time.Second,
		MaxRetries:             3,
		RetryDelay:            100 * time.Millisecond,
		EnableCircuitBreaker:   true,
		CircuitBreakerThreshold: 5,
		CircuitBreakerTimeout:  30 * time.Second,
	}
}

// NewQueryRouter creates a new query router
func NewQueryRouter(config *QueryRouterConfig) *QueryRouter {
	if config == nil {
		config = DefaultQueryRouterConfig()
	}

	qr := &QueryRouter{
		config:          config,
		logger:          log.NewTextLogger(slog.LevelInfo),
		replicaNodes:    make(map[string]*NodeInfo),
		nodeConnections: make(map[string]*ConnectionPool),
		stats:           NewRouterStatistics(),
		stopCh:          make(chan struct{}),
	}

	// Initialize load balancer
	qr.loadBalancer = NewLoadBalancer(config.LoadBalancingStrategy, qr.logger)
	
	// Initialize health monitor
	qr.healthMonitor = NewHealthMonitor(config, qr.logger)

	return qr
}

// Start begins the query router operations
func (qr *QueryRouter) Start(ctx context.Context) error {
	qr.logger.Info("Starting query router")

	// Start health monitoring
	if err := qr.healthMonitor.Start(ctx, qr.updateNodeHealth); err != nil {
		return fmt.Errorf("failed to start health monitor: %w", err)
	}

	// Start background maintenance
	go qr.maintenanceLoop(ctx)

	qr.logger.Info("Query router started successfully")
	return nil
}

// Stop gracefully stops the query router
func (qr *QueryRouter) Stop() error {
	qr.stopOnce.Do(func() {
		close(qr.stopCh)
		
		// Stop health monitor
		if qr.healthMonitor != nil {
			qr.healthMonitor.Stop()
		}
		
		// Close all connection pools
		qr.mu.Lock()
		for _, pool := range qr.nodeConnections {
			pool.Close()
		}
		qr.mu.Unlock()
		
		qr.logger.Info("Query router stopped")
	})
	return nil
}

// AddNode adds a node to the router
func (qr *QueryRouter) AddNode(nodeID, address string, role NodeRole) error {
	qr.mu.Lock()
	defer qr.mu.Unlock()

	node := &NodeInfo{
		ID:           nodeID,
		Address:      address,
		Role:         role,
		IsHealthy:    true,
		LastSeen:     time.Now(),
		Weight:       1,
		CircuitState: CircuitClosed,
	}

	if role == Primary {
		qr.primaryNode = node
	} else {
		qr.replicaNodes[nodeID] = node
	}

	// Create connection pool
	pool := NewConnectionPool(nodeID, address, qr.config)
	qr.nodeConnections[nodeID] = pool

	// Add to health monitor
	qr.healthMonitor.AddNode(nodeID, address)

	qr.logger.Info("Added node to router", "node_id", nodeID, "address", address, "role", role)
	return nil
}

// RemoveNode removes a node from the router
func (qr *QueryRouter) RemoveNode(nodeID string) error {
	qr.mu.Lock()
	defer qr.mu.Unlock()

	// Remove from primary or replicas
	if qr.primaryNode != nil && qr.primaryNode.ID == nodeID {
		qr.primaryNode = nil
	} else {
		delete(qr.replicaNodes, nodeID)
	}

	// Close and remove connection pool
	if pool, exists := qr.nodeConnections[nodeID]; exists {
		pool.Close()
		delete(qr.nodeConnections, nodeID)
	}

	// Remove from health monitor
	qr.healthMonitor.RemoveNode(nodeID)

	qr.logger.Info("Removed node from router", "node_id", nodeID)
	return nil
}

// RouteQuery routes a query to the appropriate node
func (qr *QueryRouter) RouteQuery(ctx context.Context, query string, stmt parser.Statement) (*QueryResult, error) {
	start := time.Now()
	defer func() {
		qr.updateStatistics(time.Since(start), stmt)
	}()

	// Determine query type
	isWrite := IsWriteQuery(stmt)
	
	// Route based on query type
	var targetNode *NodeInfo
	var err error

	if isWrite {
		targetNode, err = qr.routeWriteQuery(ctx)
	} else {
		targetNode, err = qr.routeReadQuery(ctx)
	}

	if err != nil {
		qr.stats.IncrementFailedQueries()
		return nil, fmt.Errorf("failed to route query: %w", err)
	}

	// Execute query with retries
	result, err := qr.executeQueryWithRetries(ctx, targetNode, query, stmt)
	if err != nil {
		qr.stats.IncrementFailedQueries()
		qr.updateNodeFailure(targetNode.ID)
		return nil, fmt.Errorf("failed to execute query on node %s: %w", targetNode.ID, err)
	}

	// Update node success metrics
	qr.updateNodeSuccess(targetNode.ID, time.Since(start))

	return result, nil
}

// routeWriteQuery routes a write query to the primary node
func (qr *QueryRouter) routeWriteQuery(ctx context.Context) (*NodeInfo, error) {
	qr.mu.RLock()
	defer qr.mu.RUnlock()

	if qr.primaryNode == nil {
		return nil, fmt.Errorf("no primary node available")
	}

	if !qr.isNodeAvailable(qr.primaryNode) {
		return nil, fmt.Errorf("primary node is not available")
	}

	return qr.primaryNode, nil
}

// routeReadQuery routes a read query using load balancing
func (qr *QueryRouter) routeReadQuery(ctx context.Context) (*NodeInfo, error) {
	qr.mu.RLock()
	defer qr.mu.RUnlock()

	// Collect available nodes for read queries
	availableNodes := make([]*NodeInfo, 0)

	// Add primary if available (can handle reads)
	if qr.primaryNode != nil && qr.isNodeAvailable(qr.primaryNode) {
		availableNodes = append(availableNodes, qr.primaryNode)
	}

	// Add healthy replicas if read replicas are enabled
	if qr.config.EnableReadReplicas {
		for _, replica := range qr.replicaNodes {
			if qr.isNodeAvailable(replica) && qr.isReplicationLagAcceptable(replica) {
				availableNodes = append(availableNodes, replica)
			}
		}
	}

	if len(availableNodes) == 0 {
		return nil, fmt.Errorf("no available nodes for read query")
	}

	// Use load balancer to select node
	selectedNode := qr.loadBalancer.SelectNode(availableNodes)
	if selectedNode == nil {
		return nil, fmt.Errorf("load balancer failed to select node")
	}

	return selectedNode, nil
}

// isNodeAvailable checks if a node is available for queries
func (qr *QueryRouter) isNodeAvailable(node *NodeInfo) bool {
	if !node.IsHealthy {
		return false
	}

	if qr.config.EnableCircuitBreaker && node.CircuitState == CircuitOpen {
		// Check if circuit breaker timeout has passed
		if time.Since(node.LastFailure) > qr.config.CircuitBreakerTimeout {
			node.CircuitState = CircuitHalfOpen
			return true
		}
		return false
	}

	return true
}

// isReplicationLagAcceptable checks if replication lag is within acceptable limits
func (qr *QueryRouter) isReplicationLagAcceptable(node *NodeInfo) bool {
	if node.Role != Replica {
		return true
	}
	return node.ReplicationLag <= qr.config.MaxReplicationLag
}

// executeQueryWithRetries executes a query with retry logic
func (qr *QueryRouter) executeQueryWithRetries(ctx context.Context, node *NodeInfo, query string, stmt parser.Statement) (*QueryResult, error) {
	var lastErr error

	for attempt := 0; attempt <= qr.config.MaxRetries; attempt++ {
		if attempt > 0 {
			// Wait before retry
			select {
			case <-time.After(qr.config.RetryDelay):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		result, err := qr.executeQuery(ctx, node, query, stmt)
		if err == nil {
			// Success - reset circuit breaker if it was half-open
			if node.CircuitState == CircuitHalfOpen {
				node.CircuitState = CircuitClosed
				node.FailureCount = 0
			}
			return result, nil
		}

		lastErr = err
		qr.logger.Warn("Query execution failed, will retry", 
			"node_id", node.ID, "attempt", attempt+1, "error", err)

		// Check if we should continue retrying
		if !qr.shouldRetry(err) {
			break
		}
	}

	return nil, fmt.Errorf("query failed after %d attempts: %w", qr.config.MaxRetries+1, lastErr)
}

// executeQuery executes a query on a specific node
func (qr *QueryRouter) executeQuery(ctx context.Context, node *NodeInfo, query string, stmt parser.Statement) (*QueryResult, error) {
	// Get connection from pool
	conn, err := qr.getConnection(node.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection: %w", err)
	}
	defer qr.returnConnection(node.ID, conn)

	// TODO: Execute actual query
	// For now, simulate query execution
	start := time.Now()
	
	// Simulate network latency and processing time
	select {
	case <-time.After(10 * time.Millisecond):
		duration := time.Since(start)
		
		// Update node metrics
		qr.mu.Lock()
		node.ResponseTime = duration
		node.TotalQueries++
		qr.mu.Unlock()

		return &QueryResult{
			NodeID:       node.ID,
			Duration:     duration,
			RowsAffected: 1,
			Success:      true,
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// shouldRetry determines if a query should be retried based on the error
func (qr *QueryRouter) shouldRetry(err error) bool {
	// TODO: Implement retry logic based on error types
	// For now, retry on any error
	return true
}

// getConnection gets a connection from the node's connection pool
func (qr *QueryRouter) getConnection(nodeID string) (*PooledConnection, error) {
	qr.mu.RLock()
	pool, exists := qr.nodeConnections[nodeID]
	qr.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("no connection pool for node %s", nodeID)
	}

	return pool.GetConnection()
}

// returnConnection returns a connection to the node's connection pool
func (qr *QueryRouter) returnConnection(nodeID string, conn *PooledConnection) {
	qr.mu.RLock()
	pool, exists := qr.nodeConnections[nodeID]
	qr.mu.RUnlock()

	if exists {
		pool.ReturnConnection(conn)
	}
}

// updateNodeHealth is called by the health monitor to update node health
func (qr *QueryRouter) updateNodeHealth(nodeID string, isHealthy bool, responseTime time.Duration) {
	qr.mu.Lock()
	defer qr.mu.Unlock()

	var node *NodeInfo
	if qr.primaryNode != nil && qr.primaryNode.ID == nodeID {
		node = qr.primaryNode
	} else if replica, exists := qr.replicaNodes[nodeID]; exists {
		node = replica
	}

	if node != nil {
		node.IsHealthy = isHealthy
		node.LastSeen = time.Now()
		if isHealthy {
			node.ResponseTime = responseTime
		}

		qr.logger.Debug("Updated node health", 
			"node_id", nodeID, "healthy", isHealthy, "response_time", responseTime)
	}
}

// updateNodeFailure updates failure metrics for a node
func (qr *QueryRouter) updateNodeFailure(nodeID string) {
	qr.mu.Lock()
	defer qr.mu.Unlock()

	var node *NodeInfo
	if qr.primaryNode != nil && qr.primaryNode.ID == nodeID {
		node = qr.primaryNode
	} else if replica, exists := qr.replicaNodes[nodeID]; exists {
		node = replica
	}

	if node != nil {
		node.FailedQueries++
		node.FailureCount++
		node.LastFailure = time.Now()

		// Update circuit breaker state
		if qr.config.EnableCircuitBreaker && node.FailureCount >= qr.config.CircuitBreakerThreshold {
			node.CircuitState = CircuitOpen
			qr.logger.Warn("Circuit breaker opened for node", "node_id", nodeID)
		}
	}
}

// updateNodeSuccess updates success metrics for a node
func (qr *QueryRouter) updateNodeSuccess(nodeID string, responseTime time.Duration) {
	qr.mu.Lock()
	defer qr.mu.Unlock()

	var node *NodeInfo
	if qr.primaryNode != nil && qr.primaryNode.ID == nodeID {
		node = qr.primaryNode
	} else if replica, exists := qr.replicaNodes[nodeID]; exists {
		node = replica
	}

	if node != nil {
		node.TotalQueries++
		node.ResponseTime = responseTime
		node.FailureCount = 0 // Reset failure count on success
	}
}

// updateStatistics updates router statistics
func (qr *QueryRouter) updateStatistics(duration time.Duration, stmt parser.Statement) {
	isWrite := IsWriteQuery(stmt)
	
	qr.stats.mu.Lock()
	defer qr.stats.mu.Unlock()
	
	qr.stats.TotalQueries++
	if isWrite {
		qr.stats.WriteQueries++
	} else {
		qr.stats.ReadQueries++
	}
	
	// Update average response time
	if qr.stats.TotalQueries == 1 {
		qr.stats.AverageResponseTime = duration
	} else {
		// Simple moving average
		qr.stats.AverageResponseTime = time.Duration(
			(int64(qr.stats.AverageResponseTime) + int64(duration)) / 2)
	}
}

// maintenanceLoop performs background maintenance tasks
func (qr *QueryRouter) maintenanceLoop(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			qr.performMaintenance()
		case <-qr.stopCh:
			return
		case <-ctx.Done():
			return
		}
	}
}

// performMaintenance performs periodic maintenance tasks
func (qr *QueryRouter) performMaintenance() {
	qr.mu.Lock()
	defer qr.mu.Unlock()

	// Clean up idle connections
	for _, pool := range qr.nodeConnections {
		pool.CleanupIdleConnections()
	}

	// Update load balancer weights based on performance
	qr.updateLoadBalancerWeights()

	// Log statistics
	qr.logStatistics()
}

// updateLoadBalancerWeights updates load balancing weights based on node performance
func (qr *QueryRouter) updateLoadBalancerWeights() {
	// Update weights for all replica nodes based on their performance
	for _, node := range qr.replicaNodes {
		if node.IsHealthy && node.TotalQueries > 0 {
			// Calculate load score based on response time and failure rate
			failureRate := float64(node.FailedQueries) / float64(node.TotalQueries)
			responseScore := 1.0 / (float64(node.ResponseTime.Milliseconds()) + 1)
			
			node.LoadScore = responseScore * (1.0 - failureRate)
			
			// Convert to weight (higher score = higher weight)
			node.Weight = int(node.LoadScore * 100)
			if node.Weight < 1 {
				node.Weight = 1
			}
		}
	}
}

// logStatistics logs current router statistics
func (qr *QueryRouter) logStatistics() {
	qr.stats.mu.RLock()
	defer qr.stats.mu.RUnlock()

	qr.logger.Info("Router statistics",
		"total_queries", qr.stats.TotalQueries,
		"read_queries", qr.stats.ReadQueries,
		"write_queries", qr.stats.WriteQueries,
		"failed_queries", qr.stats.FailedQueries,
		"avg_response_time", qr.stats.AverageResponseTime)
}

// GetStatus returns the current status of the query router
func (qr *QueryRouter) GetStatus() map[string]interface{} {
	qr.mu.RLock()
	defer qr.mu.RUnlock()

	nodes := make(map[string]interface{})
	
	if qr.primaryNode != nil {
		nodes["primary"] = qr.nodeToStatus(qr.primaryNode)
	}
	
	replicas := make(map[string]interface{})
	for id, node := range qr.replicaNodes {
		replicas[id] = qr.nodeToStatus(node)
	}
	nodes["replicas"] = replicas

	return map[string]interface{}{
		"config": map[string]interface{}{
			"load_balancing_strategy": qr.config.LoadBalancingStrategy,
			"enable_read_replicas":    qr.config.EnableReadReplicas,
			"max_replication_lag":     qr.config.MaxReplicationLag,
		},
		"nodes": nodes,
		"statistics": map[string]interface{}{
			"total_queries":        qr.stats.TotalQueries,
			"read_queries":         qr.stats.ReadQueries,
			"write_queries":        qr.stats.WriteQueries,
			"failed_queries":       qr.stats.FailedQueries,
			"average_response_time": qr.stats.AverageResponseTime,
		},
		"health_monitor": qr.healthMonitor.GetStatus(),
	}
}

// nodeToStatus converts a NodeInfo to a status map
func (qr *QueryRouter) nodeToStatus(node *NodeInfo) map[string]interface{} {
	return map[string]interface{}{
		"id":               node.ID,
		"address":          node.Address,
		"role":             node.Role,
		"is_healthy":       node.IsHealthy,
		"last_seen":        node.LastSeen,
		"response_time":    node.ResponseTime,
		"active_conns":     node.ActiveConns,
		"total_queries":    node.TotalQueries,
		"failed_queries":   node.FailedQueries,
		"replication_lag":  node.ReplicationLag,
		"weight":           node.Weight,
		"load_score":       node.LoadScore,
		"circuit_state":    node.CircuitState,
		"failure_count":    node.FailureCount,
	}
}