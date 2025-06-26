package routing

import (
	"time"

	"github.com/dshills/QuantaDB/internal/sql/parser"
)

// QueryResult represents the result of executing a query
type QueryResult struct {
	NodeID       string
	Duration     time.Duration
	RowsAffected int64
	Success      bool
	Error        error
	Data         interface{} // Placeholder for actual result data
}

// RouterStatistics tracks routing performance metrics
func NewRouterStatistics() *RouterStatistics {
	return &RouterStatistics{
		RoutingDecisions:   make(map[string]int64),
		NodeUtilization:    make(map[string]float64),
		LoadBalancerStats:  make(map[LoadBalancingStrategy]int64),
		HealthCheckStats:   make(map[string]int64),
	}
}

// IncrementFailedQueries increments the failed query counter
func (rs *RouterStatistics) IncrementFailedQueries() {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.FailedQueries++
}

// IsWriteQuery determines if a SQL statement is a write operation
func IsWriteQuery(stmt parser.Statement) bool {
	if stmt == nil {
		return false
	}

	switch stmt.(type) {
	case *parser.InsertStmt:
		return true
	case *parser.UpdateStmt:
		return true
	case *parser.DeleteStmt:
		return true
	case *parser.CreateTableStmt:
		return true
	case *parser.DropTableStmt:
		return true
	case *parser.CreateIndexStmt:
		return true
	case *parser.DropIndexStmt:
		return true
	case *parser.AlterTableStmt:
		return true
	default:
		return false
	}
}

// String methods for enums
func (role NodeRole) String() string {
	switch role {
	case Primary:
		return "PRIMARY"
	case Replica:
		return "REPLICA"
	case Standby:
		return "STANDBY"
	default:
		return "UNKNOWN"
	}
}

func (state CircuitState) String() string {
	switch state {
	case CircuitClosed:
		return "CLOSED"
	case CircuitOpen:
		return "OPEN"
	case CircuitHalfOpen:
		return "HALF_OPEN"
	default:
		return "UNKNOWN"
	}
}

func (strategy LoadBalancingStrategy) String() string {
	switch strategy {
	case RoundRobin:
		return "ROUND_ROBIN"
	case LeastConnections:
		return "LEAST_CONNECTIONS"
	case WeightedRoundRobin:
		return "WEIGHTED_ROUND_ROBIN"
	case ResponseTimeWeighted:
		return "RESPONSE_TIME_WEIGHTED"
	case HealthAware:
		return "HEALTH_AWARE"
	default:
		return "UNKNOWN"
	}
}