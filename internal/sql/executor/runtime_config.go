package executor

import (
	"sync/atomic"
	"time"
)

// ExecutorRuntimeConfig holds runtime configuration for the executor
type ExecutorRuntimeConfig struct {
	// Vectorized execution settings
	EnableVectorizedExecution bool
	VectorizedBatchSize       int
	VectorizedMemoryLimit     int64 // in bytes

	// Result caching settings
	EnableResultCaching   bool
	ResultCacheMaxSize    int64 // in bytes
	ResultCacheMaxEntries int
	ResultCacheTTL        time.Duration

	// Query execution settings
	MaxParallelWorkers int
	WorkQueueSize      int
	
	// Memory management
	QueryMemoryLimit int64 // per-query memory limit in bytes
	
	// Statistics and monitoring
	EnableStatistics bool
	StatsSampleRate  float64 // 0.0 to 1.0
	
	// Feature flags (atomic for runtime updates)
	vectorizedEnabled atomic.Bool
	cachingEnabled    atomic.Bool
}

// NewExecutorRuntimeConfig creates a new runtime configuration with defaults
func NewExecutorRuntimeConfig() *ExecutorRuntimeConfig {
	cfg := &ExecutorRuntimeConfig{
		EnableVectorizedExecution: true,
		VectorizedBatchSize:       1024,
		VectorizedMemoryLimit:     256 * 1024 * 1024, // 256MB
		EnableResultCaching:       true,
		ResultCacheMaxSize:        64 * 1024 * 1024, // 64MB
		ResultCacheMaxEntries:     1000,
		ResultCacheTTL:            5 * time.Minute,
		MaxParallelWorkers:        4,
		WorkQueueSize:             100,
		QueryMemoryLimit:          512 * 1024 * 1024, // 512MB per query
		EnableStatistics:          true,
		StatsSampleRate:           1.0, // Sample all queries by default
	}
	
	// Initialize atomic flags
	cfg.vectorizedEnabled.Store(cfg.EnableVectorizedExecution)
	cfg.cachingEnabled.Store(cfg.EnableResultCaching)
	
	return cfg
}

// SetVectorizedEnabled enables or disables vectorized execution at runtime
func (c *ExecutorRuntimeConfig) SetVectorizedEnabled(enabled bool) {
	c.vectorizedEnabled.Store(enabled)
	c.EnableVectorizedExecution = enabled
}

// SetCachingEnabled enables or disables result caching at runtime
func (c *ExecutorRuntimeConfig) SetCachingEnabled(enabled bool) {
	c.cachingEnabled.Store(enabled)
	c.EnableResultCaching = enabled
}

// IsVectorizedEnabled returns true if vectorized execution is enabled
func (c *ExecutorRuntimeConfig) IsVectorizedEnabled() bool {
	return c.vectorizedEnabled.Load()
}

// IsCachingEnabled returns true if result caching is enabled
func (c *ExecutorRuntimeConfig) IsCachingEnabled() bool {
	return c.cachingEnabled.Load()
}