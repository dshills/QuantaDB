package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/dshills/QuantaDB/internal/network"
	"github.com/dshills/QuantaDB/internal/wal"
)

// Config represents the complete server configuration.
type Config struct {
	// Server configuration
	Host     string `json:"host"`
	Port     int    `json:"port"`
	DataDir  string `json:"data_dir"`
	LogLevel string `json:"log_level"`

	// Network configuration
	Network NetworkConfig `json:"network"`

	// Storage configuration
	Storage StorageConfig `json:"storage"`

	// WAL configuration
	WAL WALConfig `json:"wal"`

	// Transaction configuration
	Transaction TransactionConfig `json:"transaction"`

	// Cluster configuration
	Cluster ClusterConfig `json:"cluster"`

	// Executor configuration
	Executor ExecutorConfig `json:"executor"`
}

// NetworkConfig represents network-specific configuration.
type NetworkConfig struct {
	MaxConnections    int `json:"max_connections"`
	ConnectionTimeout int `json:"connection_timeout"`
	ReadBufferSize    int `json:"read_buffer_size"`
	WriteBufferSize   int `json:"write_buffer_size"`
}

// StorageConfig represents storage-specific configuration.
type StorageConfig struct {
	BufferPoolSize int    `json:"buffer_pool_size"` // in MB
	PageSize       int    `json:"page_size"`
	DatabaseFile   string `json:"database_file"`
}

// WALConfig represents WAL-specific configuration.
type WALConfig struct {
	Enabled           bool   `json:"enabled"`
	Directory         string `json:"directory"`
	SegmentSize       int64  `json:"segment_size"`
	RetentionDuration string `json:"retention_duration"`
}

// TransactionConfig represents transaction-specific configuration.
type TransactionConfig struct {
	IsolationLevel string `json:"isolation_level"`
	DefaultTimeout int    `json:"default_timeout"` // in seconds
}

// ClusterConfig represents cluster-specific configuration.
type ClusterConfig struct {
	// Node configuration
	NodeID   string `json:"node_id"`
	Mode     string `json:"mode"`     // "none", "primary", "replica"
	DataDir  string `json:"data_dir"`

	// Replication configuration
	Replication ReplicationConfig `json:"replication"`
}

// ReplicationConfig represents replication-specific configuration.
type ReplicationConfig struct {
	// Primary node address (for replicas)
	PrimaryAddress string `json:"primary_address"`

	// Streaming configuration
	StreamBufferSize int `json:"stream_buffer_size"` // in bytes
	BatchSize        int `json:"batch_size"`         // records per batch
	FlushInterval    int `json:"flush_interval"`     // in milliseconds

	// Heartbeat configuration
	HeartbeatInterval int `json:"heartbeat_interval"` // in seconds
	HeartbeatTimeout  int `json:"heartbeat_timeout"`  // in seconds

	// Lag thresholds
	MaxLagBytes int `json:"max_lag_bytes"` // in bytes
	MaxLagTime  int `json:"max_lag_time"`  // in seconds

	// Connection settings
	ConnectTimeout    int `json:"connect_timeout"`     // in seconds
	ReconnectInterval int `json:"reconnect_interval"`  // in seconds
	MaxReconnectTries int `json:"max_reconnect_tries"`
}

// ExecutorConfig represents query executor configuration.
type ExecutorConfig struct {
	// Vectorized execution settings
	EnableVectorizedExecution bool  `json:"enable_vectorized_execution"`
	VectorizedBatchSize       int   `json:"vectorized_batch_size"`
	VectorizedMemoryLimit     int64 `json:"vectorized_memory_limit"` // in bytes

	// Result caching settings
	EnableResultCaching   bool   `json:"enable_result_caching"`
	ResultCacheMaxSize    int64  `json:"result_cache_max_size"`    // in bytes
	ResultCacheMaxEntries int    `json:"result_cache_max_entries"`
	ResultCacheTTL        string `json:"result_cache_ttl"` // duration string

	// Query execution settings
	MaxParallelWorkers int `json:"max_parallel_workers"`
	WorkQueueSize      int `json:"work_queue_size"`
	
	// Memory management
	QueryMemoryLimit int64 `json:"query_memory_limit"` // per-query memory limit in bytes
	
	// Statistics and monitoring
	EnableStatistics bool `json:"enable_statistics"`
	StatsSampleRate  float64 `json:"stats_sample_rate"` // 0.0 to 1.0
}

// DefaultConfig returns a configuration with sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		Host:     "localhost",
		Port:     5432,
		DataDir:  "./data",
		LogLevel: "info",
		Network: NetworkConfig{
			MaxConnections:    100,
			ConnectionTimeout: 30,
			ReadBufferSize:    8192,
			WriteBufferSize:   8192,
		},
		Storage: StorageConfig{
			BufferPoolSize: 128, // 128MB
			PageSize:       8192,
			DatabaseFile:   "quantadb.db",
		},
		WAL: WALConfig{
			Enabled:           true,
			Directory:         "wal",
			SegmentSize:       16 * 1024 * 1024, // 16MB
			RetentionDuration: "24h",
		},
		Transaction: TransactionConfig{
			IsolationLevel: "read_committed",
			DefaultTimeout: 300, // 5 minutes
		},
		Cluster: ClusterConfig{
			NodeID:  "",    // Empty means single-node mode
			Mode:    "none", // "none", "primary", "replica"
			DataDir: "./cluster",
			Replication: ReplicationConfig{
				PrimaryAddress:    "",
				StreamBufferSize:  1024 * 1024, // 1MB
				BatchSize:         100,
				FlushInterval:     100, // 100ms
				HeartbeatInterval: 10,  // 10 seconds
				HeartbeatTimeout:  30,  // 30 seconds
				MaxLagBytes:       16 * 1024 * 1024, // 16MB
				MaxLagTime:        300, // 5 minutes
				ConnectTimeout:    30,  // 30 seconds
				ReconnectInterval: 5,   // 5 seconds
				MaxReconnectTries: 10,
			},
		},
		Executor: ExecutorConfig{
			EnableVectorizedExecution: true,
			VectorizedBatchSize:       1024,
			VectorizedMemoryLimit:     256 * 1024 * 1024, // 256MB
			EnableResultCaching:       true,
			ResultCacheMaxSize:        64 * 1024 * 1024, // 64MB
			ResultCacheMaxEntries:     1000,
			ResultCacheTTL:            "5m",
			MaxParallelWorkers:        4,
			WorkQueueSize:             100,
			QueryMemoryLimit:          512 * 1024 * 1024, // 512MB per query
			EnableStatistics:          true,
			StatsSampleRate:           1.0, // Sample all queries by default
		},
	}
}

// LoadFromFile loads configuration from a JSON file.
func LoadFromFile(path string) (*Config, error) {
	// Start with defaults
	cfg := DefaultConfig()

	// Read file
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// Parse JSON
	if err := json.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	// Validate and normalize
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return cfg, nil
}

// LoadFromFlags merges command-line flags into the configuration.
func (c *Config) LoadFromFlags(host string, port int, dataDir string, logLevel string) {
	if host != "" {
		c.Host = host
	}
	if port > 0 {
		c.Port = port
	}
	if dataDir != "" {
		c.DataDir = dataDir
	}
	if logLevel != "" {
		c.LogLevel = logLevel
	}
}

// Validate checks if the configuration is valid.
func (c *Config) Validate() error {
	// Validate port
	if c.Port < 1 || c.Port > 65535 {
		return fmt.Errorf("invalid port: %d", c.Port)
	}

	// Validate log level
	switch c.LogLevel {
	case "debug", "info", "warn", "error":
		// Valid
	default:
		return fmt.Errorf("invalid log level: %s", c.LogLevel)
	}

	// Validate storage
	if c.Storage.BufferPoolSize < 1 {
		return fmt.Errorf("buffer pool size must be at least 1MB")
	}
	if c.Storage.PageSize < 1024 || c.Storage.PageSize > 65536 {
		return fmt.Errorf("page size must be between 1KB and 64KB")
	}

	// Validate network
	if c.Network.MaxConnections < 1 {
		return fmt.Errorf("max connections must be at least 1")
	}

	// Validate cluster configuration
	if err := c.validateCluster(); err != nil {
		return fmt.Errorf("invalid cluster configuration: %w", err)
	}

	// Validate executor configuration
	if err := c.validateExecutor(); err != nil {
		return fmt.Errorf("invalid executor configuration: %w", err)
	}

	return nil
}

// validateCluster validates cluster-specific configuration
func (c *Config) validateCluster() error {
	switch c.Cluster.Mode {
	case "none", "primary", "replica":
		// Valid modes
	default:
		return fmt.Errorf("invalid cluster mode: %s", c.Cluster.Mode)
	}

	// If replica mode, primary address is required
	if c.Cluster.Mode == "replica" && c.Cluster.Replication.PrimaryAddress == "" {
		return fmt.Errorf("primary address is required for replica mode")
	}

	// Validate replication settings
	if c.Cluster.Replication.StreamBufferSize < 1024 {
		return fmt.Errorf("stream buffer size must be at least 1KB")
	}
	if c.Cluster.Replication.BatchSize < 1 {
		return fmt.Errorf("batch size must be at least 1")
	}
	if c.Cluster.Replication.HeartbeatInterval < 1 {
		return fmt.Errorf("heartbeat interval must be at least 1 second")
	}
	if c.Cluster.Replication.HeartbeatTimeout < c.Cluster.Replication.HeartbeatInterval {
		return fmt.Errorf("heartbeat timeout must be greater than heartbeat interval")
	}

	return nil
}

// validateExecutor validates executor-specific configuration
func (c *Config) validateExecutor() error {
	// Skip validation if executor config is not enabled
	if !c.Executor.EnableVectorizedExecution && !c.Executor.EnableResultCaching && !c.Executor.EnableStatistics {
		return nil
	}
	
	// Validate vectorized execution settings
	if c.Executor.EnableVectorizedExecution && (c.Executor.VectorizedBatchSize < 1 || c.Executor.VectorizedBatchSize > 65536) {
		return fmt.Errorf("vectorized batch size must be between 1 and 65536")
	}
	if c.Executor.VectorizedMemoryLimit < 0 {
		return fmt.Errorf("vectorized memory limit cannot be negative")
	}

	// Validate result cache settings
	if c.Executor.ResultCacheMaxSize < 0 {
		return fmt.Errorf("result cache max size cannot be negative")
	}
	if c.Executor.ResultCacheMaxEntries < 0 {
		return fmt.Errorf("result cache max entries cannot be negative")
	}
	if c.Executor.ResultCacheTTL != "" {
		if _, err := time.ParseDuration(c.Executor.ResultCacheTTL); err != nil {
			return fmt.Errorf("invalid result cache TTL: %w", err)
		}
	}

	// Validate parallel execution settings
	if c.Executor.MaxParallelWorkers < 0 {
		return fmt.Errorf("max parallel workers cannot be negative")
	}
	if c.Executor.MaxParallelWorkers > 0 && c.Executor.WorkQueueSize < 1 {
		return fmt.Errorf("work queue size must be at least 1 when parallel workers are enabled")
	}

	// Validate memory settings
	if c.Executor.QueryMemoryLimit < 0 {
		return fmt.Errorf("query memory limit cannot be negative")
	}

	// Validate statistics settings
	if c.Executor.StatsSampleRate < 0.0 || c.Executor.StatsSampleRate > 1.0 {
		return fmt.Errorf("stats sample rate must be between 0.0 and 1.0")
	}

	return nil
}

// GetDatabasePath returns the full path to the database file.
func (c *Config) GetDatabasePath() string {
	return filepath.Join(c.DataDir, c.Storage.DatabaseFile)
}

// GetWALDirectory returns the full path to the WAL directory.
func (c *Config) GetWALDirectory() string {
	return filepath.Join(c.DataDir, c.WAL.Directory)
}

// ToNetworkConfig converts to network.Config.
func (c *Config) ToNetworkConfig() network.Config {
	cfg := network.DefaultConfig()
	cfg.Host = c.Host
	cfg.Port = c.Port
	cfg.MaxConnections = c.Network.MaxConnections
	// Convert connection timeout to time.Duration
	cfg.ReadTimeout = time.Duration(c.Network.ConnectionTimeout) * time.Second
	cfg.WriteTimeout = time.Duration(c.Network.ConnectionTimeout) * time.Second
	return cfg
}

// ToWALConfig converts to wal.Config.
func (c *Config) ToWALConfig() *wal.Config {
	cfg := wal.DefaultConfig()
	cfg.Directory = c.GetWALDirectory()
	cfg.SegmentSize = c.WAL.SegmentSize
	// Note: RetentionDuration would need parsing from string
	return cfg
}

// IsClusterEnabled returns true if clustering is enabled
func (c *Config) IsClusterEnabled() bool {
	return c.Cluster.Mode != "none" && c.Cluster.Mode != ""
}

// IsPrimary returns true if this node is configured as primary
func (c *Config) IsPrimary() bool {
	return c.Cluster.Mode == "primary"
}

// IsReplica returns true if this node is configured as replica
func (c *Config) IsReplica() bool {
	return c.Cluster.Mode == "replica"
}

// GetClusterDataDir returns the full path to the cluster data directory
func (c *Config) GetClusterDataDir() string {
	if c.Cluster.DataDir == "" {
		return filepath.Join(c.DataDir, "cluster")
	}
	return c.Cluster.DataDir
}

// GetReplicationAddress returns the replication listening address
func (c *Config) GetReplicationAddress() string {
	// Use same host as main server but different port (main port + 1000)
	return fmt.Sprintf("%s:%d", c.Host, c.Port+1000)
}
