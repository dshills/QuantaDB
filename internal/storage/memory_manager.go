package storage

import (
	"sync"
	"time"
)

// MemorySubsystem represents different memory-consuming subsystems
type MemorySubsystem string

const (
	SubsystemBufferPool   MemorySubsystem = "buffer_pool"
	SubsystemVectorized   MemorySubsystem = "vectorized"
	SubsystemResultCache  MemorySubsystem = "result_cache"
	SubsystemIndexes      MemorySubsystem = "indexes"
	SubsystemTempStorage  MemorySubsystem = "temp_storage"
)

// MemoryUsage tracks memory usage for a subsystem
type MemoryUsage struct {
	Current   int64     // Current memory usage in bytes
	Peak      int64     // Peak memory usage in bytes
	Allocated int64     // Total allocated (may be higher than current due to fragmentation)
	LastUpdate time.Time // Last update timestamp
}

// MemoryManagerStats provides overall memory statistics
type MemoryManagerStats struct {
	TotalCurrent   int64                          // Total current usage across all subsystems
	TotalPeak      int64                          // Total peak usage
	TotalLimit     int64                          // Total memory limit
	SubsystemUsage map[MemorySubsystem]MemoryUsage // Per-subsystem usage
	PressureLevel  float64                        // Memory pressure (0.0 to 1.0)
	IsUnderPressure bool                          // Whether system is under memory pressure
}

// MemoryManagerConfig configures memory management behavior
type MemoryManagerConfig struct {
	TotalMemoryLimit       int64   // Total memory limit in bytes
	PressureThreshold      float64 // Pressure threshold (0.0 to 1.0)
	HighPressureThreshold  float64 // High pressure threshold
	CriticalThreshold      float64 // Critical pressure threshold
	MonitoringInterval     time.Duration // How often to check memory usage
	EnablePressureCallbacks bool   // Whether to call pressure callbacks
}

// DefaultMemoryManagerConfig returns default configuration
func DefaultMemoryManagerConfig() *MemoryManagerConfig {
	return &MemoryManagerConfig{
		TotalMemoryLimit:       512 * 1024 * 1024, // 512MB
		PressureThreshold:      0.75,              // 75%
		HighPressureThreshold:  0.85,              // 85%
		CriticalThreshold:      0.95,              // 95%
		MonitoringInterval:     5 * time.Second,
		EnablePressureCallbacks: true,
	}
}

// MemoryPressureCallback is called when memory pressure changes
type MemoryPressureCallback func(level float64, stats MemoryManagerStats)

// MemoryManager coordinates memory usage across subsystems
type MemoryManager struct {
	config    *MemoryManagerConfig
	usage     map[MemorySubsystem]MemoryUsage
	bufferPool *BufferPool
	
	// Callbacks for memory pressure events
	pressureCallbacks []MemoryPressureCallback
	
	// State tracking
	lastPressureLevel float64
	lastStatsUpdate   time.Time
	
	// Thread safety
	mu sync.RWMutex
}

// NewMemoryManager creates a new memory manager
func NewMemoryManager(config *MemoryManagerConfig, bufferPool *BufferPool) *MemoryManager {
	if config == nil {
		config = DefaultMemoryManagerConfig()
	}
	
	return &MemoryManager{
		config:     config,
		usage:      make(map[MemorySubsystem]MemoryUsage),
		bufferPool: bufferPool,
		lastStatsUpdate: time.Now(),
	}
}

// RegisterPressureCallback registers a callback for memory pressure events
func (mm *MemoryManager) RegisterPressureCallback(callback MemoryPressureCallback) {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	
	mm.pressureCallbacks = append(mm.pressureCallbacks, callback)
}

// UpdateSubsystemUsage updates memory usage for a subsystem
func (mm *MemoryManager) UpdateSubsystemUsage(subsystem MemorySubsystem, current int64) {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	
	usage := mm.usage[subsystem]
	usage.Current = current
	usage.LastUpdate = time.Now()
	
	// Update peak if necessary
	if current > usage.Peak {
		usage.Peak = current
	}
	
	mm.usage[subsystem] = usage
	
	// Check if we should trigger pressure callbacks
	if mm.config.EnablePressureCallbacks {
		mm.checkPressureCallbacks()
	}
}

// GetSubsystemUsage returns memory usage for a specific subsystem
func (mm *MemoryManager) GetSubsystemUsage(subsystem MemorySubsystem) MemoryUsage {
	mm.mu.RLock()
	defer mm.mu.RUnlock()
	
	return mm.usage[subsystem]
}

// GetAvailableMemory returns available memory for a subsystem
func (mm *MemoryManager) GetAvailableMemory(subsystem MemorySubsystem) int64 {
	mm.mu.RLock()
	defer mm.mu.RUnlock()
	
	totalUsed := mm.calculateTotalUsage()
	
	// Get buffer pool usage if buffer pool is available
	if mm.bufferPool != nil {
		bufferPoolUsage := mm.bufferPool.GetMemoryUsageBytes()
		// Update buffer pool usage in our tracking
		mm.updateBufferPoolUsageUnsafe(bufferPoolUsage)
		totalUsed = mm.calculateTotalUsage()
	}
	
	available := mm.config.TotalMemoryLimit - totalUsed
	if available < 0 {
		available = 0
	}
	
	return available
}

// GetStats returns comprehensive memory statistics
func (mm *MemoryManager) GetStats() MemoryManagerStats {
	mm.mu.RLock()
	defer mm.mu.RUnlock()
	
	// Update buffer pool stats if available
	if mm.bufferPool != nil {
		bufferPoolUsage := mm.bufferPool.GetMemoryUsageBytes()
		mm.updateBufferPoolUsageUnsafe(bufferPoolUsage)
	}
	
	totalCurrent := mm.calculateTotalUsage()
	totalPeak := mm.calculateTotalPeak()
	pressureLevel := float64(totalCurrent) / float64(mm.config.TotalMemoryLimit)
	
	// Make a copy of usage map
	usageCopy := make(map[MemorySubsystem]MemoryUsage)
	for k, v := range mm.usage {
		usageCopy[k] = v
	}
	
	return MemoryManagerStats{
		TotalCurrent:    totalCurrent,
		TotalPeak:      totalPeak,
		TotalLimit:     mm.config.TotalMemoryLimit,
		SubsystemUsage: usageCopy,
		PressureLevel:  pressureLevel,
		IsUnderPressure: pressureLevel >= mm.config.PressureThreshold,
	}
}

// IsMemoryPressure checks if system is under memory pressure
func (mm *MemoryManager) IsMemoryPressure() bool {
	stats := mm.GetStats()
	return stats.IsUnderPressure
}

// GetPressureLevel returns current memory pressure level (0.0 to 1.0)
func (mm *MemoryManager) GetPressureLevel() float64 {
	stats := mm.GetStats()
	return stats.PressureLevel
}

// calculateTotalUsage calculates total memory usage across all subsystems (unsafe)
func (mm *MemoryManager) calculateTotalUsage() int64 {
	total := int64(0)
	for _, usage := range mm.usage {
		total += usage.Current
	}
	return total
}

// calculateTotalPeak calculates total peak memory usage (unsafe)
func (mm *MemoryManager) calculateTotalPeak() int64 {
	total := int64(0)
	for _, usage := range mm.usage {
		total += usage.Peak
	}
	return total
}

// updateBufferPoolUsageUnsafe updates buffer pool usage without acquiring lock
func (mm *MemoryManager) updateBufferPoolUsageUnsafe(current int64) {
	usage := mm.usage[SubsystemBufferPool]
	usage.Current = current
	usage.LastUpdate = time.Now()
	
	if current > usage.Peak {
		usage.Peak = current
	}
	
	mm.usage[SubsystemBufferPool] = usage
}

// checkPressureCallbacks checks if pressure callbacks should be triggered (unsafe)
func (mm *MemoryManager) checkPressureCallbacks() {
	totalCurrent := mm.calculateTotalUsage()
	currentPressure := float64(totalCurrent) / float64(mm.config.TotalMemoryLimit)
	
	// Only trigger callbacks if pressure level changed significantly
	pressureDiff := currentPressure - mm.lastPressureLevel
	if pressureDiff > 0.05 || pressureDiff < -0.05 { // 5% change threshold
		stats := MemoryManagerStats{
			TotalCurrent:    totalCurrent,
			TotalPeak:      mm.calculateTotalPeak(),
			TotalLimit:     mm.config.TotalMemoryLimit,
			SubsystemUsage: mm.usage,
			PressureLevel:  currentPressure,
			IsUnderPressure: currentPressure >= mm.config.PressureThreshold,
		}
		
		// Call callbacks without holding lock to avoid deadlocks
		callbacks := make([]MemoryPressureCallback, len(mm.pressureCallbacks))
		copy(callbacks, mm.pressureCallbacks)
		
		mm.lastPressureLevel = currentPressure
		
		// Release lock before calling callbacks
		mm.mu.Unlock()
		for _, callback := range callbacks {
			callback(currentPressure, stats)
		}
		mm.mu.Lock()
	}
}

// ResetStats resets all subsystem statistics
func (mm *MemoryManager) ResetStats() {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	
	for subsystem := range mm.usage {
		usage := mm.usage[subsystem]
		usage.Peak = usage.Current
		mm.usage[subsystem] = usage
	}
}