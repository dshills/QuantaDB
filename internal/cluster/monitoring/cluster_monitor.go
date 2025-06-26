package monitoring

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/dshills/QuantaDB/internal/cluster/failover"
	"github.com/dshills/QuantaDB/internal/cluster/replication"
	"github.com/dshills/QuantaDB/internal/cluster/routing"
	"github.com/dshills/QuantaDB/internal/log"
)

// ClusterMonitor provides comprehensive cluster monitoring and status reporting
type ClusterMonitor struct {
	config *ClusterMonitorConfig
	logger log.Logger
	mu     sync.RWMutex

	// Component references
	replicationManager replication.ReplicationManager
	queryRouter        *routing.QueryRouter
	partitionDetector  *failover.NetworkPartitionDetector

	// Monitoring state
	nodes         map[string]*NodeMetrics
	clusterHealth *ClusterHealth
	alerts        []*Alert
	metrics       *ClusterMetrics

	// Control
	stopCh   chan struct{}
	stopOnce sync.Once
	started  bool
}

// ClusterMonitorConfig contains configuration for cluster monitoring
type ClusterMonitorConfig struct {
	// Collection intervals
	MetricsInterval     time.Duration
	HealthCheckInterval time.Duration
	AlertCheckInterval  time.Duration

	// Retention settings
	MetricsRetention time.Duration
	AlertRetention   time.Duration
	MaxStoredMetrics int

	// Thresholds for alerting
	CPUThreshold            float64
	MemoryThreshold         float64
	DiskThreshold           float64
	ReplicationLagThreshold time.Duration
	QueryLatencyThreshold   time.Duration

	// Export settings
	EnablePrometheusExport bool
	PrometheusPort         int
	EnableJSONExport       bool
	JSONExportPath         string
}

// NodeMetrics contains comprehensive metrics for a single node
type NodeMetrics struct {
	NodeID    string    `json:"node_id"`
	Timestamp time.Time `json:"timestamp"`

	// System metrics
	CPUUsage    float64 `json:"cpu_usage"`
	MemoryUsage float64 `json:"memory_usage"`
	DiskUsage   float64 `json:"disk_usage"`
	NetworkIn   int64   `json:"network_in"`
	NetworkOut  int64   `json:"network_out"`

	// Database metrics
	ActiveConnections int32         `json:"active_connections"`
	TotalQueries      int64         `json:"total_queries"`
	QueriesPerSecond  float64       `json:"queries_per_second"`
	AverageLatency    time.Duration `json:"average_latency"`
	ReplicationLag    time.Duration `json:"replication_lag"`

	// Storage metrics
	TableCount     int32   `json:"table_count"`
	IndexCount     int32   `json:"index_count"`
	DatabaseSize   int64   `json:"database_size"`
	WALSize        int64   `json:"wal_size"`
	BufferHitRatio float64 `json:"buffer_hit_ratio"`

	// Health status
	IsHealthy     bool      `json:"is_healthy"`
	LastSeen      time.Time `json:"last_seen"`
	UptimeSeconds int64     `json:"uptime_seconds"`
	Role          string    `json:"role"`
}

// ClusterHealth represents the overall health of the cluster
type ClusterHealth struct {
	Status          HealthStatus `json:"status"`
	TotalNodes      int          `json:"total_nodes"`
	HealthyNodes    int          `json:"healthy_nodes"`
	UnhealthyNodes  int          `json:"unhealthy_nodes"`
	PrimaryNode     string       `json:"primary_node"`
	ReplicaNodes    []string     `json:"replica_nodes"`
	PartitionStatus string       `json:"partition_status"`
	HasQuorum       bool         `json:"has_quorum"`
	LastUpdated     time.Time    `json:"last_updated"`
}

// HealthStatus represents the overall cluster health status
type HealthStatus int

const (
	HealthStatusHealthy HealthStatus = iota
	HealthStatusDegraded
	HealthStatusCritical
	HealthStatusDown
)

// ClusterMetrics contains aggregated cluster-wide metrics
type ClusterMetrics struct {
	Timestamp time.Time `json:"timestamp"`

	// Aggregate performance
	TotalQPS          float64       `json:"total_qps"`
	AverageLatency    time.Duration `json:"average_latency"`
	TotalConnections  int32         `json:"total_connections"`
	TotalDatabaseSize int64         `json:"total_database_size"`

	// Replication metrics
	MaxReplicationLag     time.Duration `json:"max_replication_lag"`
	AverageReplicationLag time.Duration `json:"average_replication_lag"`
	ReplicationHealth     float64       `json:"replication_health"`

	// Resource utilization
	AverageCPU    float64 `json:"average_cpu"`
	AverageMemory float64 `json:"average_memory"`
	AverageDisk   float64 `json:"average_disk"`

	// Failure metrics
	FailedNodes   int   `json:"failed_nodes"`
	FailoverCount int64 `json:"failover_count"`
	AlertCount    int   `json:"alert_count"`
}

// Alert represents a cluster alert
type Alert struct {
	ID         string        `json:"id"`
	Severity   AlertSeverity `json:"severity"`
	Type       AlertType     `json:"type"`
	NodeID     string        `json:"node_id,omitempty"`
	Message    string        `json:"message"`
	Details    interface{}   `json:"details,omitempty"`
	Timestamp  time.Time     `json:"timestamp"`
	Resolved   bool          `json:"resolved"`
	ResolvedAt *time.Time    `json:"resolved_at,omitempty"`
}

// AlertSeverity defines the severity level of an alert
type AlertSeverity int

const (
	AlertSeverityInfo AlertSeverity = iota
	AlertSeverityWarning
	AlertSeverityCritical
	AlertSeverityEmergency
)

// AlertType defines the type of alert
type AlertType int

const (
	AlertTypeNodeDown AlertType = iota
	AlertTypeHighCPU
	AlertTypeHighMemory
	AlertTypeHighDisk
	AlertTypeReplicationLag
	AlertTypeHighLatency
	AlertTypeNetworkPartition
	AlertTypeFailover
)

// DefaultClusterMonitorConfig returns sensible defaults
func DefaultClusterMonitorConfig() *ClusterMonitorConfig {
	return &ClusterMonitorConfig{
		MetricsInterval:         30 * time.Second,
		HealthCheckInterval:     10 * time.Second,
		AlertCheckInterval:      5 * time.Second,
		MetricsRetention:        24 * time.Hour,
		AlertRetention:          7 * 24 * time.Hour,
		MaxStoredMetrics:        1000,
		CPUThreshold:            80.0,
		MemoryThreshold:         85.0,
		DiskThreshold:           90.0,
		ReplicationLagThreshold: 5 * time.Second,
		QueryLatencyThreshold:   100 * time.Millisecond,
		EnablePrometheusExport:  true,
		PrometheusPort:          9090,
		EnableJSONExport:        true,
		JSONExportPath:          "/tmp/quantadb-metrics.json",
	}
}

// NewClusterMonitor creates a new cluster monitor
func NewClusterMonitor(
	config *ClusterMonitorConfig,
	replicationManager replication.ReplicationManager,
	queryRouter *routing.QueryRouter,
	partitionDetector *failover.NetworkPartitionDetector,
) *ClusterMonitor {
	if config == nil {
		config = DefaultClusterMonitorConfig()
	}

	return &ClusterMonitor{
		config:             config,
		logger:             log.NewTextLogger(slog.LevelInfo),
		replicationManager: replicationManager,
		queryRouter:        queryRouter,
		partitionDetector:  partitionDetector,
		nodes:              make(map[string]*NodeMetrics),
		clusterHealth:      &ClusterHealth{},
		alerts:             make([]*Alert, 0),
		metrics:            &ClusterMetrics{},
		stopCh:             make(chan struct{}),
	}
}

// Start begins cluster monitoring
func (cm *ClusterMonitor) Start(ctx context.Context) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.started {
		return nil
	}

	cm.started = true
	cm.logger.Info("Starting cluster monitor")

	// Start monitoring goroutines
	go cm.metricsCollectionLoop(ctx)
	go cm.healthCheckLoop(ctx)
	go cm.alertProcessingLoop(ctx)

	if cm.config.EnablePrometheusExport {
		go cm.prometheusExportLoop(ctx)
	}

	if cm.config.EnableJSONExport {
		go cm.jsonExportLoop(ctx)
	}

	cm.logger.Info("Cluster monitor started successfully")
	return nil
}

// Stop gracefully stops the cluster monitor
func (cm *ClusterMonitor) Stop() error {
	cm.stopOnce.Do(func() {
		close(cm.stopCh)

		cm.mu.Lock()
		cm.started = false
		cm.mu.Unlock()

		cm.logger.Info("Cluster monitor stopped")
	})
	return nil
}

// AddNode adds a node to monitoring
func (cm *ClusterMonitor) AddNode(nodeID, role string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.nodes[nodeID] = &NodeMetrics{
		NodeID:    nodeID,
		Timestamp: time.Now(),
		Role:      role,
		IsHealthy: true,
		LastSeen:  time.Now(),
	}

	cm.logger.Info("Added node to monitoring", "node_id", nodeID, "role", role)
}

// RemoveNode removes a node from monitoring
func (cm *ClusterMonitor) RemoveNode(nodeID string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	delete(cm.nodes, nodeID)
	cm.logger.Info("Removed node from monitoring", "node_id", nodeID)
}

// metricsCollectionLoop periodically collects metrics from all nodes
func (cm *ClusterMonitor) metricsCollectionLoop(ctx context.Context) {
	ticker := time.NewTicker(cm.config.MetricsInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			cm.collectMetrics()
		case <-cm.stopCh:
			return
		case <-ctx.Done():
			return
		}
	}
}

// healthCheckLoop periodically updates cluster health status
func (cm *ClusterMonitor) healthCheckLoop(ctx context.Context) {
	ticker := time.NewTicker(cm.config.HealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			cm.updateClusterHealth()
		case <-cm.stopCh:
			return
		case <-ctx.Done():
			return
		}
	}
}

// alertProcessingLoop processes and manages alerts
func (cm *ClusterMonitor) alertProcessingLoop(ctx context.Context) {
	ticker := time.NewTicker(cm.config.AlertCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			cm.processAlerts()
		case <-cm.stopCh:
			return
		case <-ctx.Done():
			return
		}
	}
}

// collectMetrics collects metrics from all monitored nodes
func (cm *ClusterMonitor) collectMetrics() {
	cm.mu.RLock()
	nodeIDs := make([]string, 0, len(cm.nodes))
	for nodeID := range cm.nodes {
		nodeIDs = append(nodeIDs, nodeID)
	}
	cm.mu.RUnlock()

	// Collect metrics from each node
	for _, nodeID := range nodeIDs {
		metrics := cm.collectNodeMetrics(nodeID)
		if metrics != nil {
			cm.mu.Lock()
			cm.nodes[nodeID] = metrics
			cm.mu.Unlock()
		}
	}

	// Update aggregate cluster metrics
	cm.updateClusterMetrics()

	cm.logger.Debug("Collected metrics from all nodes", "node_count", len(nodeIDs))
}

// collectNodeMetrics collects metrics from a specific node
func (cm *ClusterMonitor) collectNodeMetrics(nodeID string) *NodeMetrics {
	// TODO: Implement actual metrics collection
	// This would involve:
	// - Querying system metrics (CPU, memory, disk, network)
	// - Querying database metrics (connections, queries, latency)
	// - Querying storage metrics (table count, size, WAL size)

	// For now, simulate metrics collection
	now := time.Now()

	// Simulate some realistic metrics with variation
	baseTime := now.Unix()

	return &NodeMetrics{
		NodeID:    nodeID,
		Timestamp: now,

		// System metrics (simulated)
		CPUUsage:    float64((baseTime % 100) + 10), // 10-110% variation
		MemoryUsage: float64((baseTime % 50) + 30),  // 30-80% variation
		DiskUsage:   float64((baseTime % 30) + 40),  // 40-70% variation
		NetworkIn:   (baseTime % 1000) * 1024,       // Variable network usage
		NetworkOut:  (baseTime % 800) * 1024,

		// Database metrics (simulated)
		ActiveConnections: int32((baseTime % 50) + 5),     // 5-55 connections
		TotalQueries:      baseTime * 10,                  // Increasing query count
		QueriesPerSecond:  float64((baseTime % 100) + 10), // 10-110 QPS
		AverageLatency:    time.Duration((baseTime%50)+5) * time.Millisecond,
		ReplicationLag:    time.Duration((baseTime % 1000)) * time.Millisecond,

		// Storage metrics (simulated)  
		TableCount:     int32((baseTime % 20) + 10), // 10-30 tables
		IndexCount:     int32((baseTime % 50) + 20), // 20-70 indexes
		DatabaseSize:   (baseTime % 10000) * 1024 * 1024, // Variable DB size
		WALSize:        (baseTime % 1000) * 1024,         // Variable WAL size
		BufferHitRatio: 0.95 + float64((baseTime%5))/100, // 95-99% hit ratio

		// Health status
		IsHealthy:     true, // Assume healthy for simulation
		LastSeen:      now,
		UptimeSeconds: baseTime % 86400, // Simulated uptime
		Role:          cm.getNodeRole(nodeID),
	}
}

// getNodeRole determines the role of a node
func (cm *ClusterMonitor) getNodeRole(nodeID string) string {
	if cm.replicationManager != nil {
		status := cm.replicationManager.GetStatus()
		if status.NodeID == replication.NodeID(nodeID) {
			switch status.Mode {
			case replication.ReplicationModePrimary:
				return "primary"
			case replication.ReplicationModeReplica:
				return "replica"
			default:
				return "unknown"
			}
		}
	}
	return "unknown"
}

// updateClusterMetrics calculates aggregate cluster metrics
func (cm *ClusterMonitor) updateClusterMetrics() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if len(cm.nodes) == 0 {
		return
	}

	metrics := &ClusterMetrics{
		Timestamp: time.Now(),
	}

	var (
		totalQPS     float64
		totalLatency time.Duration
		totalCPU     float64
		totalMemory  float64
		totalDisk    float64
		maxLag       time.Duration
		totalLag     time.Duration
		healthyNodes int
		totalConns   int32
		totalDBSize  int64
		nodeCount    int
	)

	for _, node := range cm.nodes {
		if node.IsHealthy {
			healthyNodes++
		}

		totalQPS += node.QueriesPerSecond
		totalLatency += node.AverageLatency
		totalCPU += node.CPUUsage
		totalMemory += node.MemoryUsage
		totalDisk += node.DiskUsage
		totalConns += node.ActiveConnections
		totalDBSize += node.DatabaseSize
		totalLag += node.ReplicationLag

		if node.ReplicationLag > maxLag {
			maxLag = node.ReplicationLag
		}

		nodeCount++
	}

	if nodeCount > 0 {
		metrics.TotalQPS = totalQPS
		metrics.AverageLatency = time.Duration(int64(totalLatency) / int64(nodeCount))
		metrics.TotalConnections = totalConns
		metrics.TotalDatabaseSize = totalDBSize
		metrics.MaxReplicationLag = maxLag
		metrics.AverageReplicationLag = time.Duration(int64(totalLag) / int64(nodeCount))
		metrics.AverageCPU = totalCPU / float64(nodeCount)
		metrics.AverageMemory = totalMemory / float64(nodeCount)
		metrics.AverageDisk = totalDisk / float64(nodeCount)
		metrics.ReplicationHealth = float64(healthyNodes) / float64(nodeCount)
		metrics.FailedNodes = nodeCount - healthyNodes
		metrics.AlertCount = len(cm.alerts)
	}

	cm.metrics = metrics
}

// updateClusterHealth updates the overall cluster health status
func (cm *ClusterMonitor) updateClusterHealth() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	health := &ClusterHealth{
		LastUpdated: time.Now(),
		TotalNodes:  len(cm.nodes),
	}

	var (
		healthyCount   int
		unhealthyCount int
		primaryNode    string
		replicaNodes   []string
	)

	for _, node := range cm.nodes {
		if node.IsHealthy {
			healthyCount++
		} else {
			unhealthyCount++
		}

		switch node.Role {
		case "primary":
			primaryNode = node.NodeID
		case "replica":
			replicaNodes = append(replicaNodes, node.NodeID)
		}
	}

	health.HealthyNodes = healthyCount
	health.UnhealthyNodes = unhealthyCount
	health.PrimaryNode = primaryNode
	health.ReplicaNodes = replicaNodes

	// Determine overall health status
	if unhealthyCount == 0 {
		health.Status = HealthStatusHealthy
	} else if healthyCount > unhealthyCount {
		health.Status = HealthStatusDegraded
	} else if healthyCount > 0 {
		health.Status = HealthStatusCritical
	} else {
		health.Status = HealthStatusDown
	}

	// Check partition status and quorum
	if cm.partitionDetector != nil {
		partitionStatus := cm.partitionDetector.GetPartitionState()
		health.PartitionStatus = fmt.Sprintf("%v", partitionStatus)
		health.HasQuorum = cm.partitionDetector.HasQuorum()
	}

	cm.clusterHealth = health
}

// processAlerts checks for alert conditions and manages existing alerts
func (cm *ClusterMonitor) processAlerts() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Check for new alert conditions
	for _, node := range cm.nodes {
		cm.checkNodeAlerts(node)
	}

	// Check cluster-wide alert conditions
	cm.checkClusterAlerts()

	// Clean up old resolved alerts
	cm.cleanupOldAlerts()
}

// checkNodeAlerts checks for alert conditions on a specific node
func (cm *ClusterMonitor) checkNodeAlerts(node *NodeMetrics) {
	// Check CPU threshold
	if node.CPUUsage > cm.config.CPUThreshold {
		cm.createAlert(AlertTypeHighCPU, AlertSeverityWarning, node.NodeID,
			fmt.Sprintf("High CPU usage: %.1f%%", node.CPUUsage),
			map[string]interface{}{"cpu_usage": node.CPUUsage})
	}

	// Check memory threshold
	if node.MemoryUsage > cm.config.MemoryThreshold {
		cm.createAlert(AlertTypeHighMemory, AlertSeverityWarning, node.NodeID,
			fmt.Sprintf("High memory usage: %.1f%%", node.MemoryUsage),
			map[string]interface{}{"memory_usage": node.MemoryUsage})
	}

	// Check disk threshold
	if node.DiskUsage > cm.config.DiskThreshold {
		cm.createAlert(AlertTypeHighDisk, AlertSeverityCritical, node.NodeID,
			fmt.Sprintf("High disk usage: %.1f%%", node.DiskUsage),
			map[string]interface{}{"disk_usage": node.DiskUsage})
	}

	// Check replication lag
	if node.ReplicationLag > cm.config.ReplicationLagThreshold {
		cm.createAlert(AlertTypeReplicationLag, AlertSeverityWarning, node.NodeID,
			fmt.Sprintf("High replication lag: %v", node.ReplicationLag),
			map[string]interface{}{"replication_lag": node.ReplicationLag})
	}

	// Check query latency
	if node.AverageLatency > cm.config.QueryLatencyThreshold {
		cm.createAlert(AlertTypeHighLatency, AlertSeverityWarning, node.NodeID,
			fmt.Sprintf("High query latency: %v", node.AverageLatency),
			map[string]interface{}{"average_latency": node.AverageLatency})
	}

	// Check node health
	if !node.IsHealthy {
		cm.createAlert(AlertTypeNodeDown, AlertSeverityCritical, node.NodeID,
			"Node is unhealthy", nil)
	}
}

// checkClusterAlerts checks for cluster-wide alert conditions
func (cm *ClusterMonitor) checkClusterAlerts() {
	// Check for network partition
	if cm.partitionDetector != nil && cm.partitionDetector.IsPartitioned() {
		cm.createAlert(AlertTypeNetworkPartition, AlertSeverityEmergency, "",
			"Network partition detected",
			map[string]interface{}{
				"partition_state": cm.partitionDetector.GetPartitionState(),
				"has_quorum":      cm.partitionDetector.HasQuorum(),
			})
	}
}

// createAlert creates a new alert if it doesn't already exist
func (cm *ClusterMonitor) createAlert(alertType AlertType, severity AlertSeverity, nodeID, message string, details interface{}) {
	// Check if similar alert already exists
	alertID := fmt.Sprintf("%s-%s-%d", nodeID, message, alertType)
	for _, alert := range cm.alerts {
		if alert.ID == alertID && !alert.Resolved {
			return // Alert already exists
		}
	}

	alert := &Alert{
		ID:        alertID,
		Severity:  severity,
		Type:      alertType,
		NodeID:    nodeID,
		Message:   message,
		Details:   details,
		Timestamp: time.Now(),
		Resolved:  false,
	}

	cm.alerts = append(cm.alerts, alert)

	cm.logger.Warn("Created alert",
		"alert_id", alert.ID,
		"severity", alert.Severity,
		"type", alert.Type,
		"node_id", nodeID,
		"message", message)
}

// cleanupOldAlerts removes old resolved alerts
func (cm *ClusterMonitor) cleanupOldAlerts() {
	cutoff := time.Now().Add(-cm.config.AlertRetention)
	filteredAlerts := make([]*Alert, 0)

	for _, alert := range cm.alerts {
		if !alert.Resolved || alert.Timestamp.After(cutoff) {
			filteredAlerts = append(filteredAlerts, alert)
		}
	}

	cm.alerts = filteredAlerts
}

// prometheusExportLoop exports metrics in Prometheus format
func (cm *ClusterMonitor) prometheusExportLoop(ctx context.Context) {
	// TODO: Implement Prometheus metrics export
	// This would start an HTTP server on the configured port
	// and serve metrics in Prometheus format
	cm.logger.Info("Prometheus export would start here", "port", cm.config.PrometheusPort)
}

// jsonExportLoop exports metrics to JSON file
func (cm *ClusterMonitor) jsonExportLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			cm.exportJSON()
		case <-cm.stopCh:
			return
		case <-ctx.Done():
			return
		}
	}
}

// exportJSON exports current metrics to JSON file
func (cm *ClusterMonitor) exportJSON() {
	cm.mu.RLock()
	data := map[string]interface{}{
		"cluster_health":  cm.clusterHealth,
		"cluster_metrics": cm.metrics,
		"nodes":           cm.nodes,
		"alerts":          cm.alerts,
		"timestamp":       time.Now(),
	}
	cm.mu.RUnlock()

	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		cm.logger.Error("Failed to marshal metrics to JSON", "error", err)
		return
	}

	// TODO: Write to actual file
	// For now, just log that we would export
	cm.logger.Debug("Would export JSON metrics",
		"path", cm.config.JSONExportPath,
		"size", len(jsonData))
}

// GetClusterHealth returns the current cluster health
func (cm *ClusterMonitor) GetClusterHealth() *ClusterHealth {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	// Return a copy to avoid race conditions
	health := *cm.clusterHealth
	health.ReplicaNodes = make([]string, len(cm.clusterHealth.ReplicaNodes))
	copy(health.ReplicaNodes, cm.clusterHealth.ReplicaNodes)

	return &health
}

// GetClusterMetrics returns the current cluster metrics
func (cm *ClusterMonitor) GetClusterMetrics() *ClusterMetrics {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	// Return a copy to avoid race conditions
	metrics := *cm.metrics
	return &metrics
}

// GetNodeMetrics returns metrics for a specific node
func (cm *ClusterMonitor) GetNodeMetrics(nodeID string) (*NodeMetrics, bool) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	metrics, exists := cm.nodes[nodeID]
	if !exists {
		return nil, false
	}

	// Return a copy to avoid race conditions
	metricsCopy := *metrics
	return &metricsCopy, true
}

// GetAllNodeMetrics returns metrics for all nodes
func (cm *ClusterMonitor) GetAllNodeMetrics() map[string]*NodeMetrics {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	result := make(map[string]*NodeMetrics)
	for id, metrics := range cm.nodes {
		metricsCopy := *metrics
		result[id] = &metricsCopy
	}

	return result
}

// GetAlerts returns all active alerts
func (cm *ClusterMonitor) GetAlerts() []*Alert {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	alerts := make([]*Alert, len(cm.alerts))
	for i, alert := range cm.alerts {
		alertCopy := *alert
		alerts[i] = &alertCopy
	}

	return alerts
}

// GetActiveAlerts returns only unresolved alerts
func (cm *ClusterMonitor) GetActiveAlerts() []*Alert {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	alerts := make([]*Alert, 0)
	for _, alert := range cm.alerts {
		if !alert.Resolved {
			alertCopy := *alert
			alerts = append(alerts, &alertCopy)
		}
	}

	return alerts
}

// ResolveAlert marks an alert as resolved
func (cm *ClusterMonitor) ResolveAlert(alertID string) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	for _, alert := range cm.alerts {
		if alert.ID == alertID && !alert.Resolved {
			alert.Resolved = true
			now := time.Now()
			alert.ResolvedAt = &now

			cm.logger.Info("Resolved alert", "alert_id", alertID)
			return nil
		}
	}

	return fmt.Errorf("alert not found or already resolved: %s", alertID)
}

// GetStatus returns comprehensive monitoring status
func (cm *ClusterMonitor) GetStatus() map[string]interface{} {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	return map[string]interface{}{
		"started":         cm.started,
		"cluster_health":  cm.clusterHealth,
		"cluster_metrics": cm.metrics,
		"node_count":      len(cm.nodes),
		"alert_count":     len(cm.alerts),
		"active_alerts":   len(cm.GetActiveAlerts()),
		"config": map[string]interface{}{
			"metrics_interval":      cm.config.MetricsInterval,
			"health_check_interval": cm.config.HealthCheckInterval,
			"prometheus_enabled":    cm.config.EnablePrometheusExport,
			"json_export_enabled":   cm.config.EnableJSONExport,
		},
	}
}

// String methods for enums
func (hs HealthStatus) String() string {
	switch hs {
	case HealthStatusHealthy:
		return "HEALTHY"
	case HealthStatusDegraded:
		return "DEGRADED"
	case HealthStatusCritical:
		return "CRITICAL"
	case HealthStatusDown:
		return "DOWN"
	default:
		return "UNKNOWN"
	}
}

func (as AlertSeverity) String() string {
	switch as {
	case AlertSeverityInfo:
		return "INFO"
	case AlertSeverityWarning:
		return "WARNING"
	case AlertSeverityCritical:
		return "CRITICAL"
	case AlertSeverityEmergency:
		return "EMERGENCY"
	default:
		return "UNKNOWN"
	}
}

func (at AlertType) String() string {
	switch at {
	case AlertTypeNodeDown:
		return "NODE_DOWN"
	case AlertTypeHighCPU:
		return "HIGH_CPU"
	case AlertTypeHighMemory:
		return "HIGH_MEMORY"
	case AlertTypeHighDisk:
		return "HIGH_DISK"
	case AlertTypeReplicationLag:
		return "REPLICATION_LAG"
	case AlertTypeHighLatency:
		return "HIGH_LATENCY"
	case AlertTypeNetworkPartition:
		return "NETWORK_PARTITION"
	case AlertTypeFailover:
		return "FAILOVER"
	default:
		return "UNKNOWN"
	}
}
