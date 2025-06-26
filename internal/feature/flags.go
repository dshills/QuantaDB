package feature

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// Flag represents a feature flag
type Flag string

// Feature flags for QuantaDB
const (
	// Execution Engine Flags
	VectorizedExecution   Flag = "vectorized_execution"
	AdaptiveExecution     Flag = "adaptive_execution"
	ParallelExecution     Flag = "parallel_execution"
	ResultCaching         Flag = "result_caching"
	CostBasedOptimization Flag = "cost_based_optimization"
	
	// Storage Engine Flags
	CompressionEnabled  Flag = "compression_enabled"
	ParallelIO          Flag = "parallel_io"
	AutoVacuum          Flag = "auto_vacuum"
	IncrementalCheckpoint Flag = "incremental_checkpoint"
	
	// Index Features
	BitmapIndexes       Flag = "bitmap_indexes"
	CoveringIndexes     Flag = "covering_indexes"
	IndexIntersection   Flag = "index_intersection"
	AutoIndexing        Flag = "auto_indexing"
	
	// Query Features
	WindowFunctions     Flag = "window_functions"
	CTESupport          Flag = "cte_support"
	PreparedStatements  Flag = "prepared_statements"
	
	// Distributed Features
	Replication         Flag = "replication"
	Sharding            Flag = "sharding"
	DistributedQueries  Flag = "distributed_queries"
	
	// Monitoring & Debug
	DetailedMetrics     Flag = "detailed_metrics"
	QueryProfiling      Flag = "query_profiling"
	DebugLogging        Flag = "debug_logging"
	
	// Experimental Features
	ExperimentalJoinReorder Flag = "experimental_join_reorder"
	ExperimentalCompression Flag = "experimental_compression"
	ExperimentalVectorOps   Flag = "experimental_vector_ops"
)

// FlagMetadata contains metadata about a feature flag
type FlagMetadata struct {
	Name         Flag
	Description  string
	DefaultValue bool
	Category     string
	Stability    string // "stable", "beta", "experimental"
}

// Manager manages feature flags
type Manager struct {
	flags      map[Flag]*flagState
	mu         sync.RWMutex
	onChange   []func(Flag, bool)
	metadata   map[Flag]*FlagMetadata
}

// flagState represents the state of a single flag
type flagState struct {
	enabled     atomic.Bool
	overridden  bool
	envVar      string
}

// Global feature flag manager
var globalManager = newManager()

// newManager creates a new feature flag manager
func newManager() *Manager {
	m := &Manager{
		flags:    make(map[Flag]*flagState),
		metadata: make(map[Flag]*FlagMetadata),
		onChange: make([]func(Flag, bool), 0),
	}
	
	// Register all flags with metadata
	m.registerFlags()
	
	// Load from environment
	m.loadFromEnvironment()
	
	return m
}

// registerFlags registers all feature flags with their metadata
func (m *Manager) registerFlags() {
	// Execution Engine
	m.register(VectorizedExecution, &FlagMetadata{
		Name:         VectorizedExecution,
		Description:  "Enable vectorized query execution",
		DefaultValue: true,
		Category:     "execution",
		Stability:    "stable",
	})
	
	m.register(AdaptiveExecution, &FlagMetadata{
		Name:         AdaptiveExecution,
		Description:  "Enable adaptive execution mode switching",
		DefaultValue: true,
		Category:     "execution",
		Stability:    "stable",
	})
	
	m.register(ParallelExecution, &FlagMetadata{
		Name:         ParallelExecution,
		Description:  "Enable parallel query execution",
		DefaultValue: true,
		Category:     "execution",
		Stability:    "stable",
	})
	
	m.register(ResultCaching, &FlagMetadata{
		Name:         ResultCaching,
		Description:  "Enable query result caching",
		DefaultValue: true,
		Category:     "execution",
		Stability:    "stable",
	})
	
	m.register(CostBasedOptimization, &FlagMetadata{
		Name:         CostBasedOptimization,
		Description:  "Enable cost-based query optimization",
		DefaultValue: true,
		Category:     "execution",
		Stability:    "stable",
	})
	
	// Storage Engine
	m.register(CompressionEnabled, &FlagMetadata{
		Name:         CompressionEnabled,
		Description:  "Enable page compression",
		DefaultValue: true,
		Category:     "storage",
		Stability:    "stable",
	})
	
	m.register(ParallelIO, &FlagMetadata{
		Name:         ParallelIO,
		Description:  "Enable parallel I/O operations",
		DefaultValue: true,
		Category:     "storage",
		Stability:    "stable",
	})
	
	m.register(AutoVacuum, &FlagMetadata{
		Name:         AutoVacuum,
		Description:  "Enable automatic vacuum process",
		DefaultValue: true,
		Category:     "storage",
		Stability:    "stable",
	})
	
	// Index Features
	m.register(CoveringIndexes, &FlagMetadata{
		Name:         CoveringIndexes,
		Description:  "Enable covering indexes with INCLUDE clause",
		DefaultValue: true,
		Category:     "index",
		Stability:    "stable",
	})
	
	m.register(IndexIntersection, &FlagMetadata{
		Name:         IndexIntersection,
		Description:  "Enable index intersection for multi-column queries",
		DefaultValue: true,
		Category:     "index",
		Stability:    "stable",
	})
	
	m.register(AutoIndexing, &FlagMetadata{
		Name:         AutoIndexing,
		Description:  "Enable automatic index recommendations",
		DefaultValue: false,
		Category:     "index",
		Stability:    "beta",
	})
	
	// Query Features
	m.register(WindowFunctions, &FlagMetadata{
		Name:         WindowFunctions,
		Description:  "Enable window function support",
		DefaultValue: false,
		Category:     "query",
		Stability:    "beta",
	})
	
	m.register(CTESupport, &FlagMetadata{
		Name:         CTESupport,
		Description:  "Enable Common Table Expression support",
		DefaultValue: false,
		Category:     "query",
		Stability:    "beta",
	})
	
	m.register(PreparedStatements, &FlagMetadata{
		Name:         PreparedStatements,
		Description:  "Enable prepared statement support",
		DefaultValue: false,
		Category:     "query",
		Stability:    "beta",
	})
	
	// Distributed Features
	m.register(Replication, &FlagMetadata{
		Name:         Replication,
		Description:  "Enable replication features",
		DefaultValue: true,
		Category:     "distributed",
		Stability:    "stable",
	})
	
	m.register(DistributedQueries, &FlagMetadata{
		Name:         DistributedQueries,
		Description:  "Enable distributed query execution",
		DefaultValue: true,
		Category:     "distributed",
		Stability:    "stable",
	})
	
	// Monitoring
	m.register(DetailedMetrics, &FlagMetadata{
		Name:         DetailedMetrics,
		Description:  "Enable detailed performance metrics collection",
		DefaultValue: true,
		Category:     "monitoring",
		Stability:    "stable",
	})
	
	m.register(QueryProfiling, &FlagMetadata{
		Name:         QueryProfiling,
		Description:  "Enable query profiling and analysis",
		DefaultValue: false,
		Category:     "monitoring",
		Stability:    "stable",
	})
	
	// Experimental
	m.register(ExperimentalJoinReorder, &FlagMetadata{
		Name:         ExperimentalJoinReorder,
		Description:  "Enable experimental join reordering algorithm",
		DefaultValue: false,
		Category:     "experimental",
		Stability:    "experimental",
	})
	
	m.register(ExperimentalVectorOps, &FlagMetadata{
		Name:         ExperimentalVectorOps,
		Description:  "Enable experimental vector operations",
		DefaultValue: false,
		Category:     "experimental",
		Stability:    "experimental",
	})
}

// register adds a flag to the manager
func (m *Manager) register(flag Flag, metadata *FlagMetadata) {
	state := &flagState{
		envVar: flagToEnvVar(flag),
	}
	state.enabled.Store(metadata.DefaultValue)
	
	m.flags[flag] = state
	m.metadata[flag] = metadata
}

// loadFromEnvironment loads flag values from environment variables
func (m *Manager) loadFromEnvironment() {
	for _, state := range m.flags {
		if val := os.Getenv(state.envVar); val != "" {
			if enabled, err := strconv.ParseBool(val); err == nil {
				state.enabled.Store(enabled)
				state.overridden = true
			}
		}
	}
}

// IsEnabled checks if a feature flag is enabled
func IsEnabled(flag Flag) bool {
	return globalManager.IsEnabled(flag)
}

// IsEnabled checks if a feature flag is enabled
func (m *Manager) IsEnabled(flag Flag) bool {
	m.mu.RLock()
	state, exists := m.flags[flag]
	m.mu.RUnlock()
	
	if !exists {
		return false
	}
	
	return state.enabled.Load()
}

// Enable enables a feature flag
func Enable(flag Flag) {
	globalManager.Enable(flag)
}

// Enable enables a feature flag
func (m *Manager) Enable(flag Flag) {
	m.setFlag(flag, true)
}

// Disable disables a feature flag
func Disable(flag Flag) {
	globalManager.Disable(flag)
}

// Disable disables a feature flag
func (m *Manager) Disable(flag Flag) {
	m.setFlag(flag, false)
}

// setFlag sets a flag value and notifies listeners
func (m *Manager) setFlag(flag Flag, enabled bool) {
	m.mu.RLock()
	state, exists := m.flags[flag]
	callbacks := m.onChange
	m.mu.RUnlock()
	
	if !exists {
		return
	}
	
	oldValue := state.enabled.Load()
	if oldValue != enabled {
		state.enabled.Store(enabled)
		
		// Notify listeners
		for _, cb := range callbacks {
			cb(flag, enabled)
		}
	}
}

// OnChange registers a callback for flag changes
func OnChange(callback func(Flag, bool)) {
	globalManager.OnChange(callback)
}

// OnChange registers a callback for flag changes
func (m *Manager) OnChange(callback func(Flag, bool)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onChange = append(m.onChange, callback)
}

// GetAll returns all flag states
func GetAll() map[Flag]bool {
	return globalManager.GetAll()
}

// GetAll returns all flag states
func (m *Manager) GetAll() map[Flag]bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	result := make(map[Flag]bool)
	for flag, state := range m.flags {
		result[flag] = state.enabled.Load()
	}
	return result
}

// GetMetadata returns metadata for a flag
func GetMetadata(flag Flag) (*FlagMetadata, bool) {
	return globalManager.GetMetadata(flag)
}

// GetMetadata returns metadata for a flag
func (m *Manager) GetMetadata(flag Flag) (*FlagMetadata, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	metadata, exists := m.metadata[flag]
	return metadata, exists
}

// GetByCategory returns all flags in a category
func GetByCategory(category string) []Flag {
	return globalManager.GetByCategory(category)
}

// GetByCategory returns all flags in a category
func (m *Manager) GetByCategory(category string) []Flag {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	var result []Flag
	for flag, metadata := range m.metadata {
		if metadata.Category == category {
			result = append(result, flag)
		}
	}
	return result
}

// Reset resets all flags to their default values
func Reset() {
	globalManager.Reset()
}

// Reset resets all flags to their default values
func (m *Manager) Reset() {
	m.mu.RLock()
	flagsCopy := make(map[Flag]*flagState)
	for k, v := range m.flags {
		flagsCopy[k] = v
	}
	m.mu.RUnlock()
	
	for flag, state := range flagsCopy {
		if metadata, exists := m.metadata[flag]; exists {
			m.setFlag(flag, metadata.DefaultValue)
			state.overridden = false
		}
	}
}

// flagToEnvVar converts a flag name to an environment variable name
func flagToEnvVar(flag Flag) string {
	return fmt.Sprintf("QUANTADB_FEATURE_%s", 
		strings.ToUpper(strings.ReplaceAll(string(flag), "_", "_")))
}

// DebugString returns a debug string with all flag states
func DebugString() string {
	return globalManager.DebugString()
}

// DebugString returns a debug string with all flag states
func (m *Manager) DebugString() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	var b strings.Builder
	b.WriteString("Feature Flags:\n")
	
	// Group by category
	categories := make(map[string][]Flag)
	for flag, metadata := range m.metadata {
		categories[metadata.Category] = append(categories[metadata.Category], flag)
	}
	
	for category, flags := range categories {
		b.WriteString(fmt.Sprintf("\n%s:\n", strings.Title(category)))
		for _, flag := range flags {
			state := m.flags[flag]
			metadata := m.metadata[flag]
			enabled := state.enabled.Load()
			
			status := "disabled"
			if enabled {
				status = "enabled"
			}
			
			override := ""
			if state.overridden {
				override = " (overridden)"
			}
			
			b.WriteString(fmt.Sprintf("  %-30s: %-8s [%s]%s - %s\n",
				flag, status, metadata.Stability, override, metadata.Description))
		}
	}
	
	return b.String()
}