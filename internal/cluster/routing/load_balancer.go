package routing

import (
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/dshills/QuantaDB/internal/log"
)

// LoadBalancer implements various load balancing strategies
type LoadBalancer interface {
	SelectNode(nodes []*NodeInfo) *NodeInfo
	UpdateNodeStats(nodeID string, stats *NodeStats)
	GetStrategy() LoadBalancingStrategy
	SetStrategy(strategy LoadBalancingStrategy)
}

// LoadBalancerImpl implements the LoadBalancer interface
type LoadBalancerImpl struct {
	strategy LoadBalancingStrategy
	logger   log.Logger
	mu       sync.RWMutex

	// Round robin state
	rrIndex int

	// Weighted round robin state
	wrrWeights map[string]int
	wrrCurrent map[string]int

	// Node statistics for decision making
	nodeStats map[string]*NodeStats

	// Random source
	rand *rand.Rand
}

// NodeStats contains statistics used for load balancing decisions
type NodeStats struct {
	NodeID       string
	ResponseTime time.Duration
	ActiveConns  int
	TotalQueries int64
	SuccessRate  float64
	LastUpdated  time.Time
	HealthScore  float64
}

// NewLoadBalancer creates a new load balancer
func NewLoadBalancer(strategy LoadBalancingStrategy, logger log.Logger) LoadBalancer {
	return &LoadBalancerImpl{
		strategy:   strategy,
		logger:     logger,
		wrrWeights: make(map[string]int),
		wrrCurrent: make(map[string]int),
		nodeStats:  make(map[string]*NodeStats),
		rand:       rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// SelectNode selects a node based on the configured strategy
func (lb *LoadBalancerImpl) SelectNode(nodes []*NodeInfo) *NodeInfo {
	if len(nodes) == 0 {
		return nil
	}

	lb.mu.Lock()
	defer lb.mu.Unlock()

	switch lb.strategy {
	case RoundRobin:
		return lb.selectRoundRobin(nodes)
	case LeastConnections:
		return lb.selectLeastConnections(nodes)
	case WeightedRoundRobin:
		return lb.selectWeightedRoundRobin(nodes)
	case ResponseTimeWeighted:
		return lb.selectResponseTimeWeighted(nodes)
	case HealthAware:
		return lb.selectHealthAware(nodes)
	default:
		// Fall back to round robin
		return lb.selectRoundRobin(nodes)
	}
}

// selectRoundRobin implements round robin load balancing
func (lb *LoadBalancerImpl) selectRoundRobin(nodes []*NodeInfo) *NodeInfo {
	if len(nodes) == 0 {
		return nil
	}

	selected := nodes[lb.rrIndex%len(nodes)]
	lb.rrIndex++

	lb.logger.Debug("Round robin selection",
		"selected_node", selected.ID, "index", lb.rrIndex-1)

	return selected
}

// selectLeastConnections selects the node with the fewest active connections
func (lb *LoadBalancerImpl) selectLeastConnections(nodes []*NodeInfo) *NodeInfo {
	if len(nodes) == 0 {
		return nil
	}

	var selected *NodeInfo
	minConnections := int(^uint(0) >> 1) // Max int

	for _, node := range nodes {
		if node.ActiveConns < minConnections {
			minConnections = node.ActiveConns
			selected = node
		}
	}

	lb.logger.Debug("Least connections selection",
		"selected_node", selected.ID, "connections", selected.ActiveConns)

	return selected
}

// selectWeightedRoundRobin implements weighted round robin
func (lb *LoadBalancerImpl) selectWeightedRoundRobin(nodes []*NodeInfo) *NodeInfo {
	if len(nodes) == 0 {
		return nil
	}

	// Initialize weights if not present
	for _, node := range nodes {
		if _, exists := lb.wrrWeights[node.ID]; !exists {
			lb.wrrWeights[node.ID] = node.Weight
			lb.wrrCurrent[node.ID] = 0
		}
	}

	// Find node with highest current weight
	var selected *NodeInfo
	maxCurrent := -1

	for _, node := range nodes {
		current := lb.wrrCurrent[node.ID]
		current += lb.wrrWeights[node.ID]
		lb.wrrCurrent[node.ID] = current

		if current > maxCurrent {
			maxCurrent = current
			selected = node
		}
	}

	if selected != nil {
		// Reduce current weight by total weight
		totalWeight := 0
		for _, node := range nodes {
			totalWeight += lb.wrrWeights[node.ID]
		}
		lb.wrrCurrent[selected.ID] -= totalWeight

		lb.logger.Debug("Weighted round robin selection",
			"selected_node", selected.ID, "weight", selected.Weight)
	}

	return selected
}

// selectResponseTimeWeighted selects based on response time performance
func (lb *LoadBalancerImpl) selectResponseTimeWeighted(nodes []*NodeInfo) *NodeInfo {
	if len(nodes) == 0 {
		return nil
	}

	// Calculate weights based on inverse of response time
	type nodeWeight struct {
		node   *NodeInfo
		weight float64
	}

	weights := make([]nodeWeight, 0, len(nodes))
	totalWeight := 0.0

	for _, node := range nodes {
		// Use inverse of response time as weight (faster = higher weight)
		responseMs := float64(node.ResponseTime.Milliseconds())
		if responseMs == 0 {
			responseMs = 1 // Avoid division by zero
		}

		weight := 1000.0 / responseMs // Scale up for better precision
		weights = append(weights, nodeWeight{node: node, weight: weight})
		totalWeight += weight
	}

	if totalWeight == 0 {
		return nodes[0] // Fallback
	}

	// Select based on weighted random
	target := lb.rand.Float64() * totalWeight
	current := 0.0

	for _, nw := range weights {
		current += nw.weight
		if current >= target {
			lb.logger.Debug("Response time weighted selection",
				"selected_node", nw.node.ID, "response_time", nw.node.ResponseTime)
			return nw.node
		}
	}

	// Fallback to last node
	return weights[len(weights)-1].node
}

// selectHealthAware implements health-aware load balancing
func (lb *LoadBalancerImpl) selectHealthAware(nodes []*NodeInfo) *NodeInfo {
	if len(nodes) == 0 {
		return nil
	}

	// Score nodes based on multiple factors
	type nodeScore struct {
		node  *NodeInfo
		score float64
	}

	scores := make([]nodeScore, 0, len(nodes))

	for _, node := range nodes {
		score := lb.calculateHealthScore(node)
		scores = append(scores, nodeScore{node: node, score: score})
	}

	// Sort by score (highest first)
	sort.Slice(scores, func(i, j int) bool {
		return scores[i].score > scores[j].score
	})

	// Use weighted selection from top performers
	topCount := len(scores)
	if topCount > 3 {
		topCount = 3 // Consider top 3 nodes
	}

	totalScore := 0.0
	for i := 0; i < topCount; i++ {
		totalScore += scores[i].score
	}

	if totalScore == 0 {
		return scores[0].node // Fallback to first node
	}

	// Weighted random selection from top performers
	target := lb.rand.Float64() * totalScore
	current := 0.0

	for i := 0; i < topCount; i++ {
		current += scores[i].score
		if current >= target {
			selected := scores[i].node
			lb.logger.Debug("Health-aware selection",
				"selected_node", selected.ID, "score", scores[i].score)
			return selected
		}
	}

	return scores[0].node
}

// calculateHealthScore calculates a comprehensive health score for a node
func (lb *LoadBalancerImpl) calculateHealthScore(node *NodeInfo) float64 {
	score := 0.0

	// Base health factor
	if node.IsHealthy {
		score += 100.0
	} else {
		return 0.0 // Unhealthy nodes get 0 score
	}

	// Response time factor (faster is better)
	responseMs := float64(node.ResponseTime.Milliseconds())
	if responseMs > 0 {
		responseScore := 1000.0 / responseMs // Inverse relationship
		if responseScore > 100 {
			responseScore = 100 // Cap at 100
		}
		score += responseScore * 0.3 // 30% weight
	}

	// Connection load factor (fewer connections is better)
	if node.ActiveConns < 10 {
		score += 50.0 * 0.2 // 20% weight
	} else if node.ActiveConns < 20 {
		score += 25.0 * 0.2
	}
	// Heavily loaded nodes get no bonus

	// Success rate factor
	if node.TotalQueries > 0 {
		successRate := 1.0 - (float64(node.FailedQueries) / float64(node.TotalQueries))
		score += successRate * 100.0 * 0.3 // 30% weight
	}

	// Circuit breaker penalty
	switch node.CircuitState {
	case CircuitClosed:
		score += 20.0 * 0.2 // 20% weight
	case CircuitHalfOpen:
		score += 10.0 * 0.2
	case CircuitOpen:
		score = 0.0 // Circuit open = no score
	}

	// Weight multiplier
	score *= float64(node.Weight) / 100.0

	return score
}

// UpdateNodeStats updates statistics for load balancing decisions
func (lb *LoadBalancerImpl) UpdateNodeStats(nodeID string, stats *NodeStats) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	stats.LastUpdated = time.Now()
	stats.HealthScore = lb.calculateHealthScoreFromStats(stats)
	lb.nodeStats[nodeID] = stats

	lb.logger.Debug("Updated node stats",
		"node_id", nodeID, "health_score", stats.HealthScore)
}

// calculateHealthScoreFromStats calculates health score from node statistics
func (lb *LoadBalancerImpl) calculateHealthScoreFromStats(stats *NodeStats) float64 {
	score := 0.0

	// Response time factor
	responseMs := float64(stats.ResponseTime.Milliseconds())
	if responseMs > 0 {
		responseScore := 1000.0 / responseMs
		if responseScore > 100 {
			responseScore = 100
		}
		score += responseScore * 0.4
	}

	// Connection load factor
	if stats.ActiveConns < 10 {
		score += 40.0
	} else if stats.ActiveConns < 20 {
		score += 20.0
	}

	// Success rate factor
	score += stats.SuccessRate * 60.0

	return score
}

// GetStrategy returns the current load balancing strategy
func (lb *LoadBalancerImpl) GetStrategy() LoadBalancingStrategy {
	lb.mu.RLock()
	defer lb.mu.RUnlock()
	return lb.strategy
}

// SetStrategy changes the load balancing strategy
func (lb *LoadBalancerImpl) SetStrategy(strategy LoadBalancingStrategy) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	oldStrategy := lb.strategy
	lb.strategy = strategy

	// Reset strategy-specific state
	lb.rrIndex = 0
	lb.wrrCurrent = make(map[string]int)

	lb.logger.Info("Load balancing strategy changed",
		"old_strategy", oldStrategy, "new_strategy", strategy)
}

// GetStats returns current load balancer statistics
func (lb *LoadBalancerImpl) GetStats() map[string]interface{} {
	lb.mu.RLock()
	defer lb.mu.RUnlock()

	nodeStats := make(map[string]interface{})
	for nodeID, stats := range lb.nodeStats {
		nodeStats[nodeID] = map[string]interface{}{
			"response_time": stats.ResponseTime,
			"active_conns":  stats.ActiveConns,
			"total_queries": stats.TotalQueries,
			"success_rate":  stats.SuccessRate,
			"health_score":  stats.HealthScore,
			"last_updated":  stats.LastUpdated,
		}
	}

	return map[string]interface{}{
		"strategy":          lb.strategy,
		"round_robin_index": lb.rrIndex,
		"node_stats":        nodeStats,
		"wrr_weights":       lb.wrrWeights,
	}
}
