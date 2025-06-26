package routing

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dshills/QuantaDB/internal/log"
)

// HealthMonitor monitors the health of database nodes
type HealthMonitor struct {
	config *QueryRouterConfig
	logger log.Logger
	mu     sync.RWMutex

	// Monitored nodes
	nodes map[string]*MonitoredNode

	// Health check callback
	callback HealthCheckCallback

	// Control
	stopCh   chan struct{}
	stopOnce sync.Once
	started  bool
}

// MonitoredNode represents a node being monitored
type MonitoredNode struct {
	ID                  string
	Address             string
	IsHealthy           bool
	LastCheck           time.Time
	LastSuccess         time.Time
	ConsecutiveFailures int
	ResponseTime        time.Duration
	CheckCount          int64
	FailureCount        int64
}

// HealthCheckCallback is called when node health changes
type HealthCheckCallback func(nodeID string, isHealthy bool, responseTime time.Duration)

// HealthCheckResult represents the result of a health check
type HealthCheckResult struct {
	NodeID       string
	IsHealthy    bool
	ResponseTime time.Duration
	Error        error
	Timestamp    time.Time
}

// NewHealthMonitor creates a new health monitor
func NewHealthMonitor(config *QueryRouterConfig, logger log.Logger) *HealthMonitor {
	return &HealthMonitor{
		config: config,
		logger: logger,
		nodes:  make(map[string]*MonitoredNode),
		stopCh: make(chan struct{}),
	}
}

// Start begins health monitoring
func (hm *HealthMonitor) Start(ctx context.Context, callback HealthCheckCallback) error {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	if hm.started {
		return nil
	}

	hm.callback = callback
	hm.started = true

	// Start health check goroutine
	go hm.healthCheckLoop(ctx)

	hm.logger.Info("Health monitor started")
	return nil
}

// Stop stops health monitoring
func (hm *HealthMonitor) Stop() error {
	hm.stopOnce.Do(func() {
		close(hm.stopCh)

		hm.mu.Lock()
		hm.started = false
		hm.mu.Unlock()

		hm.logger.Info("Health monitor stopped")
	})
	return nil
}

// AddNode adds a node to health monitoring
func (hm *HealthMonitor) AddNode(nodeID, address string) {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	hm.nodes[nodeID] = &MonitoredNode{
		ID:          nodeID,
		Address:     address,
		IsHealthy:   true,
		LastCheck:   time.Now(),
		LastSuccess: time.Now(),
	}

	hm.logger.Info("Added node to health monitoring", "node_id", nodeID, "address", address)
}

// RemoveNode removes a node from health monitoring
func (hm *HealthMonitor) RemoveNode(nodeID string) {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	delete(hm.nodes, nodeID)
	hm.logger.Info("Removed node from health monitoring", "node_id", nodeID)
}

// healthCheckLoop performs periodic health checks
func (hm *HealthMonitor) healthCheckLoop(ctx context.Context) {
	ticker := time.NewTicker(hm.config.HealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			hm.performHealthChecks(ctx)
		case <-hm.stopCh:
			return
		case <-ctx.Done():
			return
		}
	}
}

// performHealthChecks checks the health of all monitored nodes
func (hm *HealthMonitor) performHealthChecks(ctx context.Context) {
	hm.mu.RLock()
	nodes := make([]*MonitoredNode, 0, len(hm.nodes))
	for _, node := range hm.nodes {
		nodes = append(nodes, node)
	}
	hm.mu.RUnlock()

	// Perform health checks concurrently
	resultCh := make(chan *HealthCheckResult, len(nodes))

	for _, node := range nodes {
		go hm.checkNodeHealth(ctx, node, resultCh)
	}

	// Collect results
	for i := 0; i < len(nodes); i++ {
		select {
		case result := <-resultCh:
			hm.processHealthCheckResult(result)
		case <-ctx.Done():
			return
		}
	}
}

// checkNodeHealth performs a health check on a single node
func (hm *HealthMonitor) checkNodeHealth(ctx context.Context, node *MonitoredNode, resultCh chan<- *HealthCheckResult) {
	start := time.Now()

	// Create timeout context for this health check
	checkCtx, cancel := context.WithTimeout(ctx, hm.config.HealthCheckTimeout)
	defer cancel()

	result := &HealthCheckResult{
		NodeID:    node.ID,
		Timestamp: start,
	}

	// Perform actual health check
	err := hm.pingNode(checkCtx, node)
	result.ResponseTime = time.Since(start)
	result.Error = err
	result.IsHealthy = err == nil

	resultCh <- result
}

// pingNode performs the actual health check ping
func (hm *HealthMonitor) pingNode(ctx context.Context, node *MonitoredNode) error {
	// TODO: Implement actual health check
	// This could be:
	// - TCP connection test
	// - HTTP health endpoint
	// - Simple SQL query (SELECT 1)
	// - PostgreSQL protocol ping

	// For now, simulate a health check
	select {
	case <-time.After(10 * time.Millisecond): // Simulate network latency
		// Simulate occasional failures for testing
		if time.Now().Unix()%17 == 0 { // ~6% failure rate
			return &HealthCheckError{
				NodeID:  node.ID,
				Address: node.Address,
				Message: "simulated health check failure",
			}
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// processHealthCheckResult processes the result of a health check
func (hm *HealthMonitor) processHealthCheckResult(result *HealthCheckResult) {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	node, exists := hm.nodes[result.NodeID]
	if !exists {
		return
	}

	// Update node statistics
	node.LastCheck = result.Timestamp
	node.CheckCount++
	node.ResponseTime = result.ResponseTime

	wasHealthy := node.IsHealthy

	if result.IsHealthy {
		// Health check succeeded
		node.IsHealthy = true
		node.LastSuccess = result.Timestamp
		node.ConsecutiveFailures = 0
	} else {
		// Health check failed
		node.FailureCount++
		node.ConsecutiveFailures++

		// Mark as unhealthy if failure threshold exceeded
		if node.ConsecutiveFailures >= hm.config.MaxFailureThreshold {
			node.IsHealthy = false
		}

		hm.logger.Warn("Health check failed",
			"node_id", result.NodeID,
			"consecutive_failures", node.ConsecutiveFailures,
			"error", result.Error)
	}

	// Notify callback if health status changed
	if wasHealthy != node.IsHealthy && hm.callback != nil {
		hm.callback(result.NodeID, node.IsHealthy, result.ResponseTime)

		hm.logger.Info("Node health status changed",
			"node_id", result.NodeID,
			"healthy", node.IsHealthy,
			"response_time", result.ResponseTime)
	}

	hm.logger.Debug("Health check completed",
		"node_id", result.NodeID,
		"healthy", result.IsHealthy,
		"response_time", result.ResponseTime,
		"consecutive_failures", node.ConsecutiveFailures)
}

// GetNodeHealth returns the health status of a specific node
func (hm *HealthMonitor) GetNodeHealth(nodeID string) (*MonitoredNode, bool) {
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	node, exists := hm.nodes[nodeID]
	if !exists {
		return nil, false
	}

	// Return a copy to avoid race conditions
	nodeCopy := *node
	return &nodeCopy, true
}

// GetAllNodeHealth returns the health status of all monitored nodes
func (hm *HealthMonitor) GetAllNodeHealth() map[string]*MonitoredNode {
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	result := make(map[string]*MonitoredNode)
	for id, node := range hm.nodes {
		nodeCopy := *node
		result[id] = &nodeCopy
	}

	return result
}

// GetHealthyNodes returns a list of currently healthy node IDs
func (hm *HealthMonitor) GetHealthyNodes() []string {
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	healthy := make([]string, 0)
	for id, node := range hm.nodes {
		if node.IsHealthy {
			healthy = append(healthy, id)
		}
	}

	return healthy
}

// GetUnhealthyNodes returns a list of currently unhealthy node IDs
func (hm *HealthMonitor) GetUnhealthyNodes() []string {
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	unhealthy := make([]string, 0)
	for id, node := range hm.nodes {
		if !node.IsHealthy {
			unhealthy = append(unhealthy, id)
		}
	}

	return unhealthy
}

// GetStatus returns the current status of the health monitor
func (hm *HealthMonitor) GetStatus() map[string]interface{} {
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	nodes := make(map[string]interface{})
	totalChecks := int64(0)
	totalFailures := int64(0)
	healthyCount := 0

	for id, node := range hm.nodes {
		nodes[id] = map[string]interface{}{
			"address":              node.Address,
			"is_healthy":           node.IsHealthy,
			"last_check":           node.LastCheck,
			"last_success":         node.LastSuccess,
			"consecutive_failures": node.ConsecutiveFailures,
			"response_time":        node.ResponseTime,
			"check_count":          node.CheckCount,
			"failure_count":        node.FailureCount,
		}

		totalChecks += node.CheckCount
		totalFailures += node.FailureCount
		if node.IsHealthy {
			healthyCount++
		}
	}

	successRate := 0.0
	if totalChecks > 0 {
		successRate = float64(totalChecks-totalFailures) / float64(totalChecks)
	}

	return map[string]interface{}{
		"started":         hm.started,
		"total_nodes":     len(hm.nodes),
		"healthy_nodes":   healthyCount,
		"unhealthy_nodes": len(hm.nodes) - healthyCount,
		"total_checks":    totalChecks,
		"total_failures":  totalFailures,
		"success_rate":    successRate,
		"config": map[string]interface{}{
			"check_interval":    hm.config.HealthCheckInterval,
			"check_timeout":     hm.config.HealthCheckTimeout,
			"failure_threshold": hm.config.MaxFailureThreshold,
		},
		"nodes": nodes,
	}
}

// HealthCheckError represents a health check error
type HealthCheckError struct {
	NodeID  string
	Address string
	Message string
}

// Error implements the error interface
func (e *HealthCheckError) Error() string {
	return fmt.Sprintf("health check failed for node %s (%s): %s", e.NodeID, e.Address, e.Message)
}
