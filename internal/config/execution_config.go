package config

import (
	"os"
	"strconv"
	"time"
)

// ExecutionConfig controls query execution behavior
type ExecutionConfig struct {
	// Vectorized execution settings
	EnableVectorizedExecution   bool  `json:"enable_vectorized_execution"`
	VectorizedBatchSize         int   `json:"vectorized_batch_size"`
	VectorizedMemoryLimit       int64 `json:"vectorized_memory_limit"`
	VectorizedFallbackThreshold int   `json:"vectorized_fallback_threshold"`

	// Result caching settings
	EnableResultCaching  bool          `json:"enable_result_caching"`
	ResultCacheMaxSize   int           `json:"result_cache_max_size"`
	ResultCacheMaxMemory int64         `json:"result_cache_max_memory"`
	ResultCacheTTL       time.Duration `json:"result_cache_ttl"`

	// Parallel execution settings
	EnableParallelExecution bool `json:"enable_parallel_execution"`
	MaxWorkerThreads        int  `json:"max_worker_threads"`
	ParallelThresholdRows   int  `json:"parallel_threshold_rows"`

	// Adaptive execution settings
	EnableAdaptiveExecution bool  `json:"enable_adaptive_execution"`
	AdaptiveMemoryLimit     int64 `json:"adaptive_memory_limit"`

	// Performance monitoring
	EnablePerformanceStats  bool          `json:"enable_performance_stats"`
	StatsCollectionInterval time.Duration `json:"stats_collection_interval"`
}

// DefaultExecutionConfig returns production-ready defaults
func DefaultExecutionConfig() *ExecutionConfig {
	return &ExecutionConfig{
		// Vectorized execution (enabled by default for performance)
		EnableVectorizedExecution:   true,
		VectorizedBatchSize:         1024,
		VectorizedMemoryLimit:       64 * 1024 * 1024, // 64MB
		VectorizedFallbackThreshold: 100,              // Fall back to scalar if batch < 100 rows

		// Result caching (enabled by default)
		EnableResultCaching:  true,
		ResultCacheMaxSize:   1000,
		ResultCacheMaxMemory: 128 * 1024 * 1024, // 128MB
		ResultCacheTTL:       10 * time.Minute,

		// Parallel execution (enabled for larger datasets)
		EnableParallelExecution: true,
		MaxWorkerThreads:        4,
		ParallelThresholdRows:   10000,

		// Adaptive execution (enabled)
		EnableAdaptiveExecution: true,
		AdaptiveMemoryLimit:     256 * 1024 * 1024, // 256MB

		// Performance monitoring
		EnablePerformanceStats:  true,
		StatsCollectionInterval: 30 * time.Second,
	}
}

// LoadExecutionConfigFromEnv loads configuration from environment variables
func LoadExecutionConfigFromEnv() *ExecutionConfig {
	config := DefaultExecutionConfig()

	// Vectorized execution
	if val := os.Getenv("QUANTADB_ENABLE_VECTORIZED"); val != "" {
		if enabled, err := strconv.ParseBool(val); err == nil {
			config.EnableVectorizedExecution = enabled
		}
	}

	if val := os.Getenv("QUANTADB_VECTORIZED_BATCH_SIZE"); val != "" {
		if size, err := strconv.Atoi(val); err == nil && size > 0 {
			config.VectorizedBatchSize = size
		}
	}

	if val := os.Getenv("QUANTADB_VECTORIZED_MEMORY_LIMIT"); val != "" {
		if limit, err := strconv.ParseInt(val, 10, 64); err == nil && limit > 0 {
			config.VectorizedMemoryLimit = limit
		}
	}

	// Result caching
	if val := os.Getenv("QUANTADB_ENABLE_RESULT_CACHE"); val != "" {
		if enabled, err := strconv.ParseBool(val); err == nil {
			config.EnableResultCaching = enabled
		}
	}

	if val := os.Getenv("QUANTADB_RESULT_CACHE_MAX_SIZE"); val != "" {
		if size, err := strconv.Atoi(val); err == nil && size > 0 {
			config.ResultCacheMaxSize = size
		}
	}

	// Parallel execution
	if val := os.Getenv("QUANTADB_ENABLE_PARALLEL"); val != "" {
		if enabled, err := strconv.ParseBool(val); err == nil {
			config.EnableParallelExecution = enabled
		}
	}

	if val := os.Getenv("QUANTADB_MAX_WORKER_THREADS"); val != "" {
		if threads, err := strconv.Atoi(val); err == nil && threads > 0 {
			config.MaxWorkerThreads = threads
		}
	}

	return config
}

// Validate ensures the configuration is valid
func (ec *ExecutionConfig) Validate() error {
	if ec.VectorizedBatchSize <= 0 {
		ec.VectorizedBatchSize = 1024
	}

	if ec.VectorizedMemoryLimit <= 0 {
		ec.VectorizedMemoryLimit = 64 * 1024 * 1024
	}

	if ec.MaxWorkerThreads <= 0 {
		ec.MaxWorkerThreads = 4
	}

	if ec.ParallelThresholdRows <= 0 {
		ec.ParallelThresholdRows = 10000
	}

	return nil
}

// IsVectorizedExecutionEnabled returns true if vectorized execution is enabled
func (ec *ExecutionConfig) IsVectorizedExecutionEnabled() bool {
	return ec.EnableVectorizedExecution
}

// IsResultCachingEnabled returns true if result caching is enabled
func (ec *ExecutionConfig) IsResultCachingEnabled() bool {
	return ec.EnableResultCaching
}

// IsParallelExecutionEnabled returns true if parallel execution is enabled
func (ec *ExecutionConfig) IsParallelExecutionEnabled() bool {
	return ec.EnableParallelExecution
}

// ShouldUseVectorized determines if vectorized execution should be used for a given row count
func (ec *ExecutionConfig) ShouldUseVectorized(estimatedRows int) bool {
	if !ec.EnableVectorizedExecution {
		return false
	}
	return estimatedRows >= ec.VectorizedFallbackThreshold
}

// ShouldUseParallel determines if parallel execution should be used for a given row count
func (ec *ExecutionConfig) ShouldUseParallel(estimatedRows int) bool {
	if !ec.EnableParallelExecution {
		return false
	}
	return estimatedRows >= ec.ParallelThresholdRows
}
