package cluster

import (
	"encoding/json"
	"net/http"
	"time"
)

// API provides HTTP endpoints for cluster management
type API struct {
	coordinator *Coordinator
	mux         *http.ServeMux
	server      *http.Server
}

// NewAPI creates a new cluster API server
func NewAPI(coordinator *Coordinator, address string) *API {
	api := &API{
		coordinator: coordinator,
		mux:         http.NewServeMux(),
	}

	// Register endpoints
	api.mux.HandleFunc("/cluster/status", api.handleStatus)
	api.mux.HandleFunc("/cluster/nodes", api.handleNodes)
	api.mux.HandleFunc("/cluster/health", api.handleHealth)

	// Create HTTP server
	api.server = &http.Server{
		Addr:         address,
		Handler:      api.mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	return api
}

// Start starts the API server
func (a *API) Start() error {
	go func() {
		if err := a.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			// Log error but don't crash
			return
		}
	}()
	return nil
}

// Stop stops the API server
func (a *API) Stop() error {
	if a.server != nil {
		return a.server.Close()
	}
	return nil
}

// handleStatus returns the current cluster status
func (a *API) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	status := a.coordinator.GetStatus()
	
	// Convert to JSON-friendly format
	response := map[string]interface{}{
		"node": map[string]interface{}{
			"id":               status.NodeInfo.NodeID,
			"raft_address":     status.NodeInfo.RaftAddress,
			"replication_port": status.NodeInfo.ReplicationPort,
			"role":             status.NodeInfo.Role.String(),
			"start_time":       status.NodeInfo.StartTime.Format(time.RFC3339),
			"uptime_seconds":   status.Uptime.Seconds(),
		},
		"raft": map[string]interface{}{
			"term":   uint64(status.RaftTerm),
			"state":  status.RaftState.String(),
			"leader": status.RaftLeader,
		},
		"replication": map[string]interface{}{
			"mode":          status.ReplicationStatus.Mode.String(),
			"node_id":       string(status.ReplicationStatus.NodeID),
			"address":       status.ReplicationStatus.Address,
			"state":         status.ReplicationStatus.State,
			"last_lsn":      uint64(status.ReplicationStatus.LastLSN),
			"replica_count": status.ReplicationStatus.ReplicaCount,
		},
		"failover": map[string]interface{}{
			"current_role":        status.FailoverStatus.CurrentRole.String(),
			"is_healthy":          status.FailoverStatus.IsHealthy,
			"last_failover":       status.FailoverStatus.LastFailoverTime.Format(time.RFC3339),
			"failover_in_progress": status.FailoverStatus.FailoverInProgress,
			"last_health_check":   status.FailoverStatus.LastHealthCheck.Format(time.RFC3339),
		},
		"cluster": map[string]interface{}{
			"is_healthy": status.IsHealthy,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleNodes returns information about all cluster nodes
func (a *API) handleNodes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	status := a.coordinator.GetStatus()
	
	// Build nodes list from Raft configuration
	nodes := []map[string]interface{}{}
	if status.RaftConfiguration != nil && status.RaftConfiguration.Members != nil {
		for nodeID, member := range status.RaftConfiguration.Members {
			nodeInfo := map[string]interface{}{
				"id":      string(nodeID),
				"address": member.Address,
				"state":   member.State.String(),
			}
			
			// Mark leader
			if status.RaftLeader != nil && nodeID == *status.RaftLeader {
				nodeInfo["is_leader"] = true
			}
			
			nodes = append(nodes, nodeInfo)
		}
	}

	response := map[string]interface{}{
		"nodes": nodes,
		"total": len(nodes),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleHealth returns a simple health check
func (a *API) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	status := a.coordinator.GetStatus()
	
	response := map[string]interface{}{
		"status": "ok",
		"healthy": status.IsHealthy,
		"role": status.NodeInfo.Role.String(),
		"uptime_seconds": status.Uptime.Seconds(),
	}

	if !status.IsHealthy {
		w.WriteHeader(http.StatusServiceUnavailable)
		response["status"] = "unhealthy"
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}