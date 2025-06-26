package management

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dshills/QuantaDB/internal/cluster/monitoring"
	"github.com/dshills/QuantaDB/internal/cluster/replication"
	"github.com/dshills/QuantaDB/internal/cluster/routing"
	"github.com/dshills/QuantaDB/internal/log"
)

// ClusterAPI provides RESTful API for cluster management
type ClusterAPI struct {
	config  *ClusterAPIConfig
	logger  log.Logger
	server  *http.Server
	mu      sync.RWMutex

	// Component references
	monitor           *monitoring.ClusterMonitor
	replicationManager replication.ReplicationManager
	queryRouter       *routing.QueryRouter

	// API state
	started bool
}

// ClusterAPIConfig contains configuration for the cluster API
type ClusterAPIConfig struct {
	// Server settings
	ListenAddress string
	Port          int
	ReadTimeout   time.Duration
	WriteTimeout  time.Duration

	// Security settings
	EnableAuth     bool
	APIKey         string
	EnableTLS      bool
	CertFile       string
	KeyFile        string

	// Rate limiting
	EnableRateLimit bool
	RequestsPerMin  int

	// CORS settings
	EnableCORS      bool
	AllowedOrigins  []string
	AllowedMethods  []string
	AllowedHeaders  []string
}

// APIResponse represents a standard API response
type APIResponse struct {
	Success   bool        `json:"success"`
	Data      interface{} `json:"data,omitempty"`
	Error     string      `json:"error,omitempty"`
	Timestamp time.Time   `json:"timestamp"`
}

// NodeOperationRequest represents a request to add/remove nodes
type NodeOperationRequest struct {
	NodeID  string `json:"node_id"`
	Address string `json:"address"`
	Role    string `json:"role,omitempty"`
}

// ConfigUpdateRequest represents a configuration update request
type ConfigUpdateRequest struct {
	Component string      `json:"component"`
	Settings  interface{} `json:"settings"`
}

// MaintenanceModeRequest represents a maintenance mode request
type MaintenanceModeRequest struct {
	Enable   bool   `json:"enable"`
	Reason   string `json:"reason,omitempty"`
	Duration string `json:"duration,omitempty"`
}

// DefaultClusterAPIConfig returns sensible defaults
func DefaultClusterAPIConfig() *ClusterAPIConfig {
	return &ClusterAPIConfig{
		ListenAddress:   "0.0.0.0",
		Port:            8432,
		ReadTimeout:     30 * time.Second,
		WriteTimeout:    30 * time.Second,
		EnableAuth:      false,
		APIKey:          "",
		EnableTLS:       false,
		EnableRateLimit: true,
		RequestsPerMin:  100,
		EnableCORS:      true,
		AllowedOrigins:  []string{"*"},
		AllowedMethods:  []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:  []string{"Content-Type", "Authorization", "X-API-Key"},
	}
}

// NewClusterAPI creates a new cluster API server
func NewClusterAPI(
	config *ClusterAPIConfig,
	monitor *monitoring.ClusterMonitor,
	replicationManager replication.ReplicationManager,
	queryRouter *routing.QueryRouter,
) *ClusterAPI {
	if config == nil {
		config = DefaultClusterAPIConfig()
	}

	api := &ClusterAPI{
		config:             config,
		logger:             log.NewTextLogger(slog.LevelInfo),
		monitor:            monitor,
		replicationManager: replicationManager,
		queryRouter:        queryRouter,
	}

	// Setup HTTP server
	mux := http.NewServeMux()
	api.setupRoutes(mux)

	api.server = &http.Server{
		Addr:         fmt.Sprintf("%s:%d", config.ListenAddress, config.Port),
		Handler:      api.middlewareChain(mux),
		ReadTimeout:  config.ReadTimeout,
		WriteTimeout: config.WriteTimeout,
	}

	return api
}

// Start starts the cluster API server
func (api *ClusterAPI) Start(ctx context.Context) error {
	api.mu.Lock()
	defer api.mu.Unlock()

	if api.started {
		return nil
	}

	api.started = true
	api.logger.Info("Starting cluster API server", "address", api.server.Addr)

	go func() {
		var err error
		if api.config.EnableTLS {
			err = api.server.ListenAndServeTLS(api.config.CertFile, api.config.KeyFile)
		} else {
			err = api.server.ListenAndServe()
		}

		if err != nil && err != http.ErrServerClosed {
			api.logger.Error("Cluster API server error", "error", err)
		}
	}()

	api.logger.Info("Cluster API server started successfully")
	return nil
}

// Stop gracefully stops the cluster API server
func (api *ClusterAPI) Stop() error {
	api.mu.Lock()
	defer api.mu.Unlock()

	if !api.started {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := api.server.Shutdown(ctx)
	api.started = false

	api.logger.Info("Cluster API server stopped")
	return err
}

// setupRoutes configures all API routes
func (api *ClusterAPI) setupRoutes(mux *http.ServeMux) {
	// Health and status endpoints
	mux.HandleFunc("/health", api.handleHealth)
	mux.HandleFunc("/status", api.handleStatus)

	// Cluster information endpoints
	mux.HandleFunc("/cluster/health", api.handleClusterHealth)
	mux.HandleFunc("/cluster/metrics", api.handleClusterMetrics)
	mux.HandleFunc("/cluster/nodes", api.handleClusterNodes)

	// Node management endpoints
	mux.HandleFunc("/nodes", api.handleNodes)
	mux.HandleFunc("/nodes/", api.handleNodeOperations)

	// Replication management endpoints
	mux.HandleFunc("/replication/status", api.handleReplicationStatus)
	mux.HandleFunc("/replication/config", api.handleReplicationConfig)

	// Query routing endpoints
	mux.HandleFunc("/routing/status", api.handleRoutingStatus)
	mux.HandleFunc("/routing/config", api.handleRoutingConfig)

	// Alert management endpoints
	mux.HandleFunc("/alerts", api.handleAlerts)
	mux.HandleFunc("/alerts/", api.handleAlertOperations)

	// Maintenance and configuration endpoints
	mux.HandleFunc("/maintenance", api.handleMaintenance)
	mux.HandleFunc("/config", api.handleConfig)

	// Administrative endpoints
	mux.HandleFunc("/admin/backup", api.handleBackup)
	mux.HandleFunc("/admin/restore", api.handleRestore)
	mux.HandleFunc("/admin/failover", api.handleFailover)
}

// middlewareChain applies middleware to the HTTP handler
func (api *ClusterAPI) middlewareChain(next http.Handler) http.Handler {
	handler := next

	// Apply CORS middleware
	if api.config.EnableCORS {
		handler = api.corsMiddleware(handler)
	}

	// Apply authentication middleware
	if api.config.EnableAuth {
		handler = api.authMiddleware(handler)
	}

	// Apply rate limiting middleware
	if api.config.EnableRateLimit {
		handler = api.rateLimitMiddleware(handler)
	}

	// Apply logging middleware
	handler = api.loggingMiddleware(handler)

	return handler
}

// corsMiddleware handles CORS headers
func (api *ClusterAPI) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" {
			// Check if origin is allowed
			allowed := false
			for _, allowedOrigin := range api.config.AllowedOrigins {
				if allowedOrigin == "*" || allowedOrigin == origin {
					allowed = true
					break
				}
			}

			if allowed {
				w.Header().Set("Access-Control-Allow-Origin", origin)
				w.Header().Set("Access-Control-Allow-Methods", strings.Join(api.config.AllowedMethods, ", "))
				w.Header().Set("Access-Control-Allow-Headers", strings.Join(api.config.AllowedHeaders, ", "))
				w.Header().Set("Access-Control-Max-Age", "86400")
			}
		}

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// authMiddleware handles API authentication
func (api *ClusterAPI) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Skip auth for health check
		if r.URL.Path == "/health" {
			next.ServeHTTP(w, r)
			return
		}

		apiKey := r.Header.Get("X-API-Key")
		if apiKey == "" {
			apiKey = r.Header.Get("Authorization")
			if strings.HasPrefix(apiKey, "Bearer ") {
				apiKey = strings.TrimPrefix(apiKey, "Bearer ")
			}
		}

		if apiKey != api.config.APIKey {
			api.writeErrorResponse(w, http.StatusUnauthorized, "Invalid API key")
			return
		}

		next.ServeHTTP(w, r)
	})
}

// rateLimitMiddleware implements basic rate limiting
func (api *ClusterAPI) rateLimitMiddleware(next http.Handler) http.Handler {
	// TODO: Implement proper rate limiting with sliding window or token bucket
	// For now, just pass through
	return next
}

// loggingMiddleware logs all requests
func (api *ClusterAPI) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		
		// Wrap ResponseWriter to capture status code
		wrapped := &responseWrapper{ResponseWriter: w, statusCode: http.StatusOK}
		
		next.ServeHTTP(wrapped, r)
		
		duration := time.Since(start)
		api.logger.Info("API request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", wrapped.statusCode,
			"duration", duration,
			"remote_addr", r.RemoteAddr)
	})
}

// responseWrapper wraps http.ResponseWriter to capture status code
type responseWrapper struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWrapper) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

// Handler implementations

// handleHealth handles GET /health
func (api *ClusterAPI) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		api.writeErrorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	api.writeSuccessResponse(w, map[string]interface{}{
		"status": "healthy",
		"version": "1.0.0", // TODO: Get from build info
		"uptime": time.Since(time.Now()).String(), // TODO: Track actual uptime
	})
}

// handleStatus handles GET /status
func (api *ClusterAPI) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		api.writeErrorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	status := map[string]interface{}{
		"api_server": map[string]interface{}{
			"started": api.started,
			"address": api.server.Addr,
		},
	}

	if api.monitor != nil {
		status["monitor"] = api.monitor.GetStatus()
	}

	if api.replicationManager != nil {
		status["replication"] = api.replicationManager.GetStatus()
	}

	if api.queryRouter != nil {
		status["routing"] = api.queryRouter.GetStatus()
	}

	api.writeSuccessResponse(w, status)
}

// handleClusterHealth handles GET /cluster/health
func (api *ClusterAPI) handleClusterHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		api.writeErrorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	if api.monitor == nil {
		api.writeErrorResponse(w, http.StatusServiceUnavailable, "Cluster monitor not available")
		return
	}

	health := api.monitor.GetClusterHealth()
	api.writeSuccessResponse(w, health)
}

// handleClusterMetrics handles GET /cluster/metrics
func (api *ClusterAPI) handleClusterMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		api.writeErrorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	if api.monitor == nil {
		api.writeErrorResponse(w, http.StatusServiceUnavailable, "Cluster monitor not available")
		return
	}

	metrics := api.monitor.GetClusterMetrics()
	api.writeSuccessResponse(w, metrics)
}

// handleClusterNodes handles GET /cluster/nodes
func (api *ClusterAPI) handleClusterNodes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		api.writeErrorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	if api.monitor == nil {
		api.writeErrorResponse(w, http.StatusServiceUnavailable, "Cluster monitor not available")
		return
	}

	nodes := api.monitor.GetAllNodeMetrics()
	api.writeSuccessResponse(w, nodes)
}

// handleNodes handles node management operations
func (api *ClusterAPI) handleNodes(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		api.handleGetNodes(w, r)
	case http.MethodPost:
		api.handleAddNode(w, r)
	default:
		api.writeErrorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}

// handleGetNodes handles GET /nodes
func (api *ClusterAPI) handleGetNodes(w http.ResponseWriter, r *http.Request) {
	var nodes []interface{}

	if api.replicationManager != nil {
		replicas := api.replicationManager.GetReplicas()
		for _, replica := range replicas {
			nodes = append(nodes, map[string]interface{}{
				"node_id": replica.NodeID,
				"address": replica.Address,
				"state":   replica.State.String(),
				"role":    "replica",
			})
		}
	}

	api.writeSuccessResponse(w, nodes)
}

// handleAddNode handles POST /nodes
func (api *ClusterAPI) handleAddNode(w http.ResponseWriter, r *http.Request) {
	var req NodeOperationRequest
	if err := api.parseJSONBody(r, &req); err != nil {
		api.writeErrorResponse(w, http.StatusBadRequest, fmt.Sprintf("Invalid request body: %v", err))
		return
	}

	if req.Address == "" {
		api.writeErrorResponse(w, http.StatusBadRequest, "Address is required")
		return
	}

	// Add node to replication manager
	if api.replicationManager != nil && req.Role == "replica" {
		err := api.replicationManager.AddReplica(req.Address)
		if err != nil {
			api.writeErrorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to add replica: %v", err))
			return
		}
	}

	// Add node to monitoring
	if api.monitor != nil {
		api.monitor.AddNode(req.NodeID, req.Role)
	}

	api.writeSuccessResponse(w, map[string]interface{}{
		"message": "Node added successfully",
		"node_id": req.NodeID,
		"address": req.Address,
		"role":    req.Role,
	})
}

// handleNodeOperations handles operations on specific nodes
func (api *ClusterAPI) handleNodeOperations(w http.ResponseWriter, r *http.Request) {
	pathParts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(pathParts) < 2 {
		api.writeErrorResponse(w, http.StatusBadRequest, "Node ID required")
		return
	}

	nodeID := pathParts[1]

	switch r.Method {
	case http.MethodGet:
		api.handleGetNode(w, r, nodeID)
	case http.MethodDelete:
		api.handleRemoveNode(w, r, nodeID)
	default:
		api.writeErrorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}

// handleGetNode handles GET /nodes/{nodeID}
func (api *ClusterAPI) handleGetNode(w http.ResponseWriter, r *http.Request, nodeID string) {
	if api.monitor == nil {
		api.writeErrorResponse(w, http.StatusServiceUnavailable, "Cluster monitor not available")
		return
	}

	metrics, exists := api.monitor.GetNodeMetrics(nodeID)
	if !exists {
		api.writeErrorResponse(w, http.StatusNotFound, "Node not found")
		return
	}

	api.writeSuccessResponse(w, metrics)
}

// handleRemoveNode handles DELETE /nodes/{nodeID}
func (api *ClusterAPI) handleRemoveNode(w http.ResponseWriter, r *http.Request, nodeID string) {
	// Remove from replication manager
	if api.replicationManager != nil {
		err := api.replicationManager.RemoveReplica(replication.NodeID(nodeID))
		if err != nil {
			api.writeErrorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to remove replica: %v", err))
			return
		}
	}

	// Remove from monitoring
	if api.monitor != nil {
		api.monitor.RemoveNode(nodeID)
	}

	api.writeSuccessResponse(w, map[string]interface{}{
		"message": "Node removed successfully",
		"node_id": nodeID,
	})
}

// handleReplicationStatus handles GET /replication/status
func (api *ClusterAPI) handleReplicationStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		api.writeErrorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	if api.replicationManager == nil {
		api.writeErrorResponse(w, http.StatusServiceUnavailable, "Replication manager not available")
		return
	}

	status := api.replicationManager.GetStatus()
	syncStatus := api.replicationManager.GetSynchronousReplicationStatus()

	response := map[string]interface{}{
		"basic_replication":       status,
		"synchronous_replication": syncStatus,
	}

	api.writeSuccessResponse(w, response)
}

// handleReplicationConfig handles replication configuration
func (api *ClusterAPI) handleReplicationConfig(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		// TODO: Return current replication configuration
		api.writeSuccessResponse(w, map[string]interface{}{
			"message": "Replication configuration would be returned here",
		})
	case http.MethodPut:
		// TODO: Update replication configuration
		api.writeSuccessResponse(w, map[string]interface{}{
			"message": "Replication configuration would be updated here",
		})
	default:
		api.writeErrorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}

// handleRoutingStatus handles GET /routing/status
func (api *ClusterAPI) handleRoutingStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		api.writeErrorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	if api.queryRouter == nil {
		api.writeErrorResponse(w, http.StatusServiceUnavailable, "Query router not available")
		return
	}

	status := api.queryRouter.GetStatus()
	api.writeSuccessResponse(w, status)
}

// handleRoutingConfig handles routing configuration
func (api *ClusterAPI) handleRoutingConfig(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		// TODO: Return current routing configuration
		api.writeSuccessResponse(w, map[string]interface{}{
			"message": "Routing configuration would be returned here",
		})
	case http.MethodPut:
		// TODO: Update routing configuration
		api.writeSuccessResponse(w, map[string]interface{}{
			"message": "Routing configuration would be updated here",
		})
	default:
		api.writeErrorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}

// handleAlerts handles alert management
func (api *ClusterAPI) handleAlerts(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		api.writeErrorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	if api.monitor == nil {
		api.writeErrorResponse(w, http.StatusServiceUnavailable, "Cluster monitor not available")
		return
	}

	// Check for 'active' query parameter
	activeOnly := r.URL.Query().Get("active") == "true"

	var alerts []*monitoring.Alert
	if activeOnly {
		alerts = api.monitor.GetActiveAlerts()
	} else {
		alerts = api.monitor.GetAlerts()
	}

	api.writeSuccessResponse(w, alerts)
}

// handleAlertOperations handles operations on specific alerts
func (api *ClusterAPI) handleAlertOperations(w http.ResponseWriter, r *http.Request) {
	pathParts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(pathParts) < 2 {
		api.writeErrorResponse(w, http.StatusBadRequest, "Alert ID required")
		return
	}

	alertID := pathParts[1]

	switch r.Method {
	case http.MethodPut:
		// Resolve alert
		if api.monitor == nil {
			api.writeErrorResponse(w, http.StatusServiceUnavailable, "Cluster monitor not available")
			return
		}

		err := api.monitor.ResolveAlert(alertID)
		if err != nil {
			api.writeErrorResponse(w, http.StatusNotFound, err.Error())
			return
		}

		api.writeSuccessResponse(w, map[string]interface{}{
			"message":  "Alert resolved successfully",
			"alert_id": alertID,
		})
	default:
		api.writeErrorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}

// handleMaintenance handles maintenance mode operations
func (api *ClusterAPI) handleMaintenance(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		// TODO: Return maintenance mode status
		api.writeSuccessResponse(w, map[string]interface{}{
			"maintenance_mode": false,
			"message": "Maintenance mode status would be returned here",
		})
	case http.MethodPost:
		// TODO: Enable/disable maintenance mode
		var req MaintenanceModeRequest
		if err := api.parseJSONBody(r, &req); err != nil {
			api.writeErrorResponse(w, http.StatusBadRequest, fmt.Sprintf("Invalid request body: %v", err))
			return
		}

		api.writeSuccessResponse(w, map[string]interface{}{
			"message": "Maintenance mode would be toggled here",
			"enable":  req.Enable,
			"reason":  req.Reason,
		})
	default:
		api.writeErrorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}

// handleConfig handles configuration management
func (api *ClusterAPI) handleConfig(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		// TODO: Return current configuration
		api.writeSuccessResponse(w, map[string]interface{}{
			"message": "Configuration would be returned here",
		})
	case http.MethodPut:
		// TODO: Update configuration
		var req ConfigUpdateRequest
		if err := api.parseJSONBody(r, &req); err != nil {
			api.writeErrorResponse(w, http.StatusBadRequest, fmt.Sprintf("Invalid request body: %v", err))
			return
		}

		api.writeSuccessResponse(w, map[string]interface{}{
			"message":   "Configuration would be updated here",
			"component": req.Component,
		})
	default:
		api.writeErrorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}

// handleBackup handles backup operations
func (api *ClusterAPI) handleBackup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		api.writeErrorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	// TODO: Implement backup functionality
	api.writeSuccessResponse(w, map[string]interface{}{
		"message": "Backup would be initiated here",
		"backup_id": "backup-" + strconv.FormatInt(time.Now().Unix(), 10),
	})
}

// handleRestore handles restore operations
func (api *ClusterAPI) handleRestore(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		api.writeErrorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	// TODO: Implement restore functionality
	api.writeSuccessResponse(w, map[string]interface{}{
		"message": "Restore would be initiated here",
	})
}

// handleFailover handles manual failover operations
func (api *ClusterAPI) handleFailover(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		api.writeErrorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	// TODO: Implement failover functionality
	api.writeSuccessResponse(w, map[string]interface{}{
		"message": "Failover would be initiated here",
	})
}

// Utility methods

// parseJSONBody parses JSON request body into the provided struct
func (api *ClusterAPI) parseJSONBody(r *http.Request, v interface{}) error {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	}
	defer r.Body.Close()

	return json.Unmarshal(body, v)
}

// writeSuccessResponse writes a successful API response
func (api *ClusterAPI) writeSuccessResponse(w http.ResponseWriter, data interface{}) {
	response := APIResponse{
		Success:   true,
		Data:      data,
		Timestamp: time.Now(),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	
	if err := json.NewEncoder(w).Encode(response); err != nil {
		api.logger.Error("Failed to encode response", "error", err)
	}
}

// writeErrorResponse writes an error API response
func (api *ClusterAPI) writeErrorResponse(w http.ResponseWriter, statusCode int, message string) {
	response := APIResponse{
		Success:   false,
		Error:     message,
		Timestamp: time.Now(),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	
	if err := json.NewEncoder(w).Encode(response); err != nil {
		api.logger.Error("Failed to encode error response", "error", err)
	}
}