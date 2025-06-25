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
