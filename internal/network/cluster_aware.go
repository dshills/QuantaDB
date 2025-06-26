package network

import (
	"fmt"
	"strings"

	"github.com/dshills/QuantaDB/internal/cluster"
	"github.com/dshills/QuantaDB/internal/cluster/failover"
	"github.com/dshills/QuantaDB/internal/sql/parser"
)

// ClusterAwareServer wraps a Server with cluster awareness
type ClusterAwareServer struct {
	*Server
	coordinator *cluster.Coordinator
}

// NewClusterAwareServer creates a new cluster-aware server
func NewClusterAwareServer(server *Server, coordinator *cluster.Coordinator) *ClusterAwareServer {
	return &ClusterAwareServer{
		Server:      server,
		coordinator: coordinator,
	}
}

// SetClusterCoordinator sets the cluster coordinator on the server
func (s *Server) SetClusterCoordinator(coordinator *cluster.Coordinator) {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	// Store coordinator reference for connections
	s.clusterCoordinator = coordinator
}

// isReadOnlyQuery determines if a query is read-only
func isReadOnlyQuery(query string) bool {
	// Quick check for common read-only statements
	trimmed := strings.TrimSpace(strings.ToUpper(query))
	
	// Check for read-only keywords at the start
	readOnlyPrefixes := []string{
		"SELECT",
		"SHOW",
		"DESCRIBE",
		"DESC",
		"EXPLAIN",
		"WITH", // CTEs that might be read-only
	}
	
	for _, prefix := range readOnlyPrefixes {
		if strings.HasPrefix(trimmed, prefix) {
			// Special case: WITH can have writes in the CTE
			if prefix == "WITH" {
				// Simple heuristic: if it contains INSERT/UPDATE/DELETE, it's not read-only
				if strings.Contains(trimmed, "INSERT") || 
				   strings.Contains(trimmed, "UPDATE") || 
				   strings.Contains(trimmed, "DELETE") {
					return false
				}
			}
			return true
		}
	}
	
	return false
}

// isReadOnlyStatement checks if a parsed statement is read-only
func isReadOnlyStatement(stmt parser.Statement) bool {
	switch stmt.(type) {
	case *parser.SelectStmt:
		return true
	case *parser.ExplainStmt:
		return true
	case *parser.AnalyzeStmt:
		return true
	default:
		return false
	}
}

// checkQueryPermissions checks if a query can be executed based on cluster role
func (c *Connection) checkQueryPermissions(query string, stmt parser.Statement) error {
	// If no cluster coordinator, allow all queries
	if c.clusterCoordinator == nil {
		return nil
	}
	
	// Type assert to cluster.Coordinator
	coordinator, ok := c.clusterCoordinator.(*cluster.Coordinator)
	if !ok {
		// If not the right type, allow all queries
		return nil
	}
	
	// Get current node role
	role := coordinator.GetRole()
	
	// Primary can execute all queries
	if role == failover.RolePrimary {
		return nil
	}
	
	// Replicas can only execute read-only queries
	if role == failover.RoleReplica || role == failover.RoleStandby {
		// Check if query is read-only
		isReadOnly := false
		
		if stmt != nil {
			isReadOnly = isReadOnlyStatement(stmt)
		} else {
			isReadOnly = isReadOnlyQuery(query)
		}
		
		if !isReadOnly {
			return fmt.Errorf("cannot execute write queries on replica node")
		}
	}
	
	// Followers in unknown state should not accept queries
	if role == failover.RoleFollower {
		return fmt.Errorf("node is in follower state and cannot accept queries")
	}
	
	return nil
}

// getClusterInfo returns cluster information for client notifications
func (c *Connection) getClusterInfo() map[string]string {
	info := make(map[string]string)
	
	if c.clusterCoordinator == nil {
		info["cluster_mode"] = "none"
		return info
	}
	
	// Type assert to cluster.Coordinator
	coordinator, ok := c.clusterCoordinator.(*cluster.Coordinator)
	if !ok {
		info["cluster_mode"] = "unknown"
		return info
	}
	
	status := coordinator.GetStatus()
	info["cluster_mode"] = "active"
	info["node_id"] = status.NodeInfo.NodeID
	info["node_role"] = status.NodeInfo.Role.String()
	
	// Add leader information if available
	if status.RaftLeader != nil {
		info["cluster_leader"] = string(*status.RaftLeader)
	}
	
	// Add replication info
	if status.NodeInfo.Role == failover.RoleReplica || status.NodeInfo.Role == failover.RoleStandby {
		info["replication_state"] = status.ReplicationStatus.State
		info["replica_count"] = fmt.Sprintf("%d", status.ReplicationStatus.ReplicaCount)
	}
	
	return info
}