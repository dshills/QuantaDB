package executor

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"
)

// AdaptiveParallelismManager dynamically adjusts parallelism based on runtime conditions
type AdaptiveParallelismManager struct {
	// Configuration
	minWorkers    int
	maxWorkers    int
	adaptiveCtx   *AdaptiveContext
	checkInterval time.Duration

	// Current state
	currentWorkers int
	activeWorkers  int
	workerPool     *WorkerPool

	// Performance monitoring
	systemStats   *SystemStats
	workloadStats *WorkloadStats

	// Adaptation control
	lastAdjustment  time.Time
	adjustmentCount int64
	cooldownPeriod  time.Duration

	// Synchronization
	mu             sync.RWMutex
	stopChan       chan struct{}
	monitoringDone chan struct{}
}

// SystemStats tracks system-wide resource utilization
type SystemStats struct {
	CPUUtilization    float64 // Overall CPU usage (0.0-1.0)
	MemoryUtilization float64 // Memory usage (0.0-1.0)
	IOWaitTime        float64 // Time spent waiting for I/O
	LoadAverage       float64 // System load average
	AvailableCores    int     // Number of available CPU cores

	// Contention metrics
	LockContentionTime   time.Duration
	ContextSwitchRate    float64
	ThreadContentionRate float64

	mu sync.RWMutex
}

// WorkloadStats tracks query-specific performance characteristics
type WorkloadStats struct {
	ThroughputHistory  []float64       // Recent throughput samples (rows/sec)
	LatencyHistory     []time.Duration // Recent latency samples
	DataSkewFactor     float64         // Measure of data distribution skew
	ParallelEfficiency float64         // Efficiency of current parallelism
	WorkerUtilization  []float64       // Per-worker utilization

	// Performance targets
	TargetThroughput       float64
	TargetLatency          time.Duration
	MinEfficiencyThreshold float64

	mu sync.RWMutex
}

// NewAdaptiveParallelismManager creates a new adaptive parallelism manager
func NewAdaptiveParallelismManager(adaptiveCtx *AdaptiveContext) *AdaptiveParallelismManager {
	maxCores := runtime.NumCPU()

	return &AdaptiveParallelismManager{
		minWorkers:     1,
		maxWorkers:     maxCores * 2, // Allow over-subscription for I/O bound work
		adaptiveCtx:    adaptiveCtx,
		checkInterval:  200 * time.Millisecond,
		currentWorkers: maxCores / 2, // Start conservatively
		cooldownPeriod: 1 * time.Second,
		systemStats:    NewSystemStats(),
		workloadStats:  NewWorkloadStats(),
		stopChan:       make(chan struct{}),
		monitoringDone: make(chan struct{}),
	}
}

// NewSystemStats creates a new system statistics tracker
func NewSystemStats() *SystemStats {
	return &SystemStats{
		AvailableCores: runtime.NumCPU(),
	}
}

// NewWorkloadStats creates a new workload statistics tracker
func NewWorkloadStats() *WorkloadStats {
	return &WorkloadStats{
		ThroughputHistory:      make([]float64, 0, 10),
		LatencyHistory:         make([]time.Duration, 0, 10),
		TargetThroughput:       10000, // 10K rows/sec default
		TargetLatency:          100 * time.Millisecond,
		MinEfficiencyThreshold: 0.7, // 70% efficiency threshold
	}
}

// Start begins adaptive parallelism monitoring
func (apm *AdaptiveParallelismManager) Start(ctx context.Context) error {
	apm.mu.Lock()
	defer apm.mu.Unlock()

	// Initialize worker pool with current worker count
	apm.workerPool = NewWorkerPool(apm.currentWorkers)

	// Start monitoring goroutine
	go apm.monitoringLoop(ctx)

	// Log initialization
	if apm.adaptiveCtx != nil {
		stats := NewRuntimeStats(0)
		apm.adaptiveCtx.LogAdaptiveDecision(
			"AdaptiveParallelism",
			"Initialize",
			fmt.Sprintf("InitialWorkers=%d, MaxWorkers=%d", apm.currentWorkers, apm.maxWorkers),
			stats,
		)
	}

	return nil
}

// monitoringLoop continuously monitors system and adjusts parallelism
func (apm *AdaptiveParallelismManager) monitoringLoop(ctx context.Context) {
	ticker := time.NewTicker(apm.checkInterval)
	defer ticker.Stop()
	defer close(apm.monitoringDone)

	for {
		select {
		case <-ctx.Done():
			return
		case <-apm.stopChan:
			return
		case <-ticker.C:
			apm.evaluateAndAdjust()
		}
	}
}

// evaluateAndAdjust checks current conditions and adjusts worker count if needed
func (apm *AdaptiveParallelismManager) evaluateAndAdjust() {
	// Check if we're in cooldown period
	if time.Since(apm.lastAdjustment) < apm.cooldownPeriod {
		return
	}

	// Update system statistics
	apm.updateSystemStats()

	// Update workload statistics
	apm.updateWorkloadStats()

	// Determine optimal worker count
	optimalWorkers := apm.calculateOptimalWorkers()

	// Adjust if needed
	if optimalWorkers != apm.currentWorkers {
		if err := apm.adjustWorkerCount(optimalWorkers); err != nil {
			// Log error but continue
			fmt.Printf("Failed to adjust worker count: %v\n", err)
		}
	}
}

// updateSystemStats collects current system performance metrics
func (apm *AdaptiveParallelismManager) updateSystemStats() {
	// In a real implementation, this would collect actual system metrics
	// For now, we'll use simplified estimates

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	apm.systemStats.mu.Lock()
	defer apm.systemStats.mu.Unlock()

	// Estimate CPU utilization based on active workers
	apm.systemStats.CPUUtilization = float64(apm.activeWorkers) / float64(runtime.NumCPU())
	if apm.systemStats.CPUUtilization > 1.0 {
		apm.systemStats.CPUUtilization = 1.0
	}

	// Estimate memory utilization
	totalMem := int64(8 * 1024 * 1024 * 1024) // Assume 8GB total memory
	apm.systemStats.MemoryUtilization = float64(ms.Alloc) / float64(totalMem)

	// Simple load average estimate
	apm.systemStats.LoadAverage = float64(apm.activeWorkers)

	// Update available cores
	apm.systemStats.AvailableCores = runtime.NumCPU()
}

// updateWorkloadStats collects query performance metrics
func (apm *AdaptiveParallelismManager) updateWorkloadStats() {
	apm.workloadStats.mu.Lock()
	defer apm.workloadStats.mu.Unlock()

	// Calculate current parallel efficiency
	if apm.currentWorkers > 1 {
		// Simplified efficiency calculation
		idealSpeedup := float64(apm.currentWorkers)

		// Estimate actual speedup based on worker utilization
		totalUtilization := 0.0
		for _, util := range apm.workloadStats.WorkerUtilization {
			totalUtilization += util
		}

		if len(apm.workloadStats.WorkerUtilization) > 0 {
			avgUtilization := totalUtilization / float64(len(apm.workloadStats.WorkerUtilization))
			actualSpeedup := avgUtilization * float64(apm.currentWorkers)
			apm.workloadStats.ParallelEfficiency = actualSpeedup / idealSpeedup
		}
	} else {
		apm.workloadStats.ParallelEfficiency = 1.0
	}

	// Maintain sliding window of performance metrics
	maxHistory := 10

	// Add current throughput (simplified - would come from actual measurements)
	currentThroughput := apm.estimateCurrentThroughput()
	apm.workloadStats.ThroughputHistory = append(apm.workloadStats.ThroughputHistory, currentThroughput)
	if len(apm.workloadStats.ThroughputHistory) > maxHistory {
		apm.workloadStats.ThroughputHistory = apm.workloadStats.ThroughputHistory[1:]
	}

	// Add current latency
	currentLatency := apm.estimateCurrentLatency()
	apm.workloadStats.LatencyHistory = append(apm.workloadStats.LatencyHistory, currentLatency)
	if len(apm.workloadStats.LatencyHistory) > maxHistory {
		apm.workloadStats.LatencyHistory = apm.workloadStats.LatencyHistory[1:]
	}
}

// calculateOptimalWorkers determines the best worker count for current conditions
func (apm *AdaptiveParallelismManager) calculateOptimalWorkers() int {
	apm.systemStats.mu.RLock()
	systemCPU := apm.systemStats.CPUUtilization
	systemMem := apm.systemStats.MemoryUtilization
	availableCores := apm.systemStats.AvailableCores
	apm.systemStats.mu.RUnlock()

	apm.workloadStats.mu.RLock()
	efficiency := apm.workloadStats.ParallelEfficiency
	throughput := apm.getAverageThroughput()
	targetThroughput := apm.workloadStats.TargetThroughput
	apm.workloadStats.mu.RUnlock()

	current := apm.currentWorkers

	// Rule 1: If system is overloaded, reduce workers
	if systemCPU > 0.9 || systemMem > 0.9 {
		return max(apm.minWorkers, current-1)
	}

	// Rule 2: If efficiency is poor, reduce workers
	if efficiency < apm.workloadStats.MinEfficiencyThreshold && current > apm.minWorkers {
		return current - 1
	}

	// Rule 3: If throughput is below target and system has capacity, increase workers
	if throughput < targetThroughput*0.8 && systemCPU < 0.7 && current < apm.maxWorkers {
		return min(apm.maxWorkers, current+1)
	}

	// Rule 4: Conservative scaling based on available cores
	if current > availableCores*2 {
		return availableCores * 2
	}

	// Rule 5: If everything looks good but we're not using enough cores, scale up gradually
	if efficiency > 0.8 && systemCPU < 0.6 && current < availableCores && current < apm.maxWorkers {
		return current + 1
	}

	// No change needed
	return current
}

// adjustWorkerCount changes the number of workers in the pool
func (apm *AdaptiveParallelismManager) adjustWorkerCount(newCount int) error {
	apm.mu.Lock()
	defer apm.mu.Unlock()

	oldCount := apm.currentWorkers

	// Create new worker pool with desired size
	newPool := NewWorkerPool(newCount)

	// Replace old pool
	if apm.workerPool != nil {
		apm.workerPool.Close()
	}
	apm.workerPool = newPool
	apm.currentWorkers = newCount
	apm.lastAdjustment = time.Now()
	apm.adjustmentCount++

	// Log the adjustment
	if apm.adaptiveCtx != nil {
		stats := NewRuntimeStats(0)
		reason := apm.getAdjustmentReason(oldCount, newCount)
		apm.adaptiveCtx.LogAdaptiveDecision(
			"AdaptiveParallelism",
			fmt.Sprintf("Adjust %d -> %d workers", oldCount, newCount),
			reason,
			stats,
		)
	}

	return nil
}

// getAdjustmentReason provides human-readable reason for worker count change
func (apm *AdaptiveParallelismManager) getAdjustmentReason(oldCount, newCount int) string {
	apm.systemStats.mu.RLock()
	cpu := apm.systemStats.CPUUtilization
	mem := apm.systemStats.MemoryUtilization
	apm.systemStats.mu.RUnlock()

	apm.workloadStats.mu.RLock()
	efficiency := apm.workloadStats.ParallelEfficiency
	throughput := apm.getAverageThroughput()
	apm.workloadStats.mu.RUnlock()

	if newCount < oldCount {
		if cpu > 0.9 || mem > 0.9 {
			return fmt.Sprintf("High resource usage (CPU=%.1f%%, Mem=%.1f%%)", cpu*100, mem*100)
		}
		if efficiency < apm.workloadStats.MinEfficiencyThreshold {
			return fmt.Sprintf("Poor parallel efficiency (%.1f%% < %.1f%%)", efficiency*100, apm.workloadStats.MinEfficiencyThreshold*100)
		}
		return "Resource optimization"
	}
	if throughput < apm.workloadStats.TargetThroughput*0.8 {
		return fmt.Sprintf("Below target throughput (%.0f < %.0f)", throughput, apm.workloadStats.TargetThroughput)
	}
	return "Performance scaling"
}

// getAverageThroughput calculates average throughput from recent samples
func (apm *AdaptiveParallelismManager) getAverageThroughput() float64 {
	if len(apm.workloadStats.ThroughputHistory) == 0 {
		return 0.0
	}

	sum := 0.0
	for _, tp := range apm.workloadStats.ThroughputHistory {
		sum += tp
	}
	return sum / float64(len(apm.workloadStats.ThroughputHistory))
}

// estimateCurrentThroughput provides a rough estimate of current throughput
func (apm *AdaptiveParallelismManager) estimateCurrentThroughput() float64 {
	// Simplified estimation - in reality would come from actual query metrics
	baseRate := 1000.0 // rows per second per worker
	efficiency := apm.workloadStats.ParallelEfficiency
	if efficiency == 0 {
		efficiency = 0.8 // Default assumption
	}

	return baseRate * float64(apm.currentWorkers) * efficiency
}

// estimateCurrentLatency provides a rough estimate of current latency
func (apm *AdaptiveParallelismManager) estimateCurrentLatency() time.Duration {
	// Simplified estimation
	baseLatency := 50 * time.Millisecond

	// Higher worker counts might increase latency due to coordination overhead
	overhead := float64(apm.currentWorkers) * 0.1
	adjustedLatency := float64(baseLatency) * (1.0 + overhead)

	return time.Duration(adjustedLatency)
}

// GetCurrentWorkerCount returns the current number of workers
func (apm *AdaptiveParallelismManager) GetCurrentWorkerCount() int {
	apm.mu.RLock()
	defer apm.mu.RUnlock()
	return apm.currentWorkers
}

// GetWorkerPool returns the current worker pool
func (apm *AdaptiveParallelismManager) GetWorkerPool() *WorkerPool {
	apm.mu.RLock()
	defer apm.mu.RUnlock()
	return apm.workerPool
}

// UpdateWorkerUtilization updates per-worker utilization statistics
func (apm *AdaptiveParallelismManager) UpdateWorkerUtilization(utilization []float64) {
	apm.workloadStats.mu.Lock()
	defer apm.workloadStats.mu.Unlock()

	apm.workloadStats.WorkerUtilization = make([]float64, len(utilization))
	copy(apm.workloadStats.WorkerUtilization, utilization)

	// Update active worker count
	active := 0
	for _, util := range utilization {
		if util > 0.1 { // Consider worker active if >10% utilized
			active++
		}
	}
	apm.activeWorkers = active
}

// Stop shuts down the adaptive parallelism manager
func (apm *AdaptiveParallelismManager) Stop() error {
	close(apm.stopChan)

	// Wait for monitoring to stop
	select {
	case <-apm.monitoringDone:
	case <-time.After(1 * time.Second):
		// Timeout waiting for clean shutdown
	}

	apm.mu.Lock()
	defer apm.mu.Unlock()

	// Close worker pool
	if apm.workerPool != nil {
		apm.workerPool.Close()
	}

	// Log final statistics
	if apm.adaptiveCtx != nil {
		stats := NewRuntimeStats(0)
		apm.adaptiveCtx.LogAdaptiveDecision(
			"AdaptiveParallelism",
			"Shutdown",
			fmt.Sprintf("TotalAdjustments=%d, FinalWorkers=%d", apm.adjustmentCount, apm.currentWorkers),
			stats,
		)
	}

	return nil
}

// GetAdaptiveStats returns parallelism-specific statistics
func (apm *AdaptiveParallelismManager) GetAdaptiveStats() map[string]interface{} {
	apm.mu.RLock()
	defer apm.mu.RUnlock()

	apm.systemStats.mu.RLock()
	systemCPU := apm.systemStats.CPUUtilization
	systemMem := apm.systemStats.MemoryUtilization
	apm.systemStats.mu.RUnlock()

	apm.workloadStats.mu.RLock()
	efficiency := apm.workloadStats.ParallelEfficiency
	avgThroughput := apm.getAverageThroughput()
	apm.workloadStats.mu.RUnlock()

	stats := make(map[string]interface{})
	stats["current_workers"] = apm.currentWorkers
	stats["active_workers"] = apm.activeWorkers
	stats["min_workers"] = apm.minWorkers
	stats["max_workers"] = apm.maxWorkers
	stats["adjustment_count"] = apm.adjustmentCount
	stats["system_cpu_utilization"] = systemCPU
	stats["system_memory_utilization"] = systemMem
	stats["parallel_efficiency"] = efficiency
	stats["average_throughput"] = avgThroughput

	return stats
}

// Helper functions
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
