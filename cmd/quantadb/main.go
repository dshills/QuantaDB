package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/config"
	"github.com/dshills/QuantaDB/internal/engine"
	"github.com/dshills/QuantaDB/internal/index"
	"github.com/dshills/QuantaDB/internal/log"
	"github.com/dshills/QuantaDB/internal/network"
	"github.com/dshills/QuantaDB/internal/sql/executor"
	"github.com/dshills/QuantaDB/internal/storage"
	"github.com/dshills/QuantaDB/internal/txn"
	"github.com/dshills/QuantaDB/internal/wal"
)

var (
	version = "0.1.0"
	commit  = "unknown"
)

func main() {
	var (
		configFile  = flag.String("config", "", "Path to configuration file")
		showVersion = flag.Bool("version", false, "Show version information")
		host        = flag.String("host", "localhost", "Host to listen on")
		port        = flag.Int("port", 5432, "Port to listen on")
		dataDir     = flag.String("data", "./data", "Data directory")
		logLevel    = flag.String("log-level", "info", "Log level (debug, info, warn, error)")
	)

	flag.Parse()

	if *showVersion {
		fmt.Printf("QuantaDB v%s (commit: %s)\n", version, commit)
		os.Exit(0)
	}

	// Load configuration
	var cfg *config.Config
	if *configFile != "" {
		var err error
		cfg, err = config.LoadFromFile(*configFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to load config file: %v\n", err)
			os.Exit(1)
		}
	} else {
		cfg = config.DefaultConfig()
	}

	// Override config with command-line flags
	cfg.LoadFromFlags(*host, *port, *dataDir, *logLevel)

	// Initialize logger
	level := slog.LevelInfo
	switch cfg.LogLevel {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	logger := log.NewTextLogger(level)

	logger.Info("Starting QuantaDB server",
		"version", version,
		"commit", commit,
		"config", *configFile,
		"host", cfg.Host,
		"port", cfg.Port,
		"data_dir", cfg.DataDir)

	// Ensure data directory exists
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		logger.Error("Failed to create data directory", "error", err)
		os.Exit(1)
	}

	// Initialize catalog
	cat := catalog.NewMemoryCatalog()
	// Initialize storage components
	dbPath := cfg.GetDatabasePath()
	diskManager, err := storage.NewDiskManager(dbPath)
	if err != nil {
		logger.Error("Failed to create disk manager", "error", err)
		os.Exit(1)
	}
	defer diskManager.Close()

	// Create buffer pool
	bufferPoolPages := cfg.Storage.BufferPoolSize * 1024 * 1024 / cfg.Storage.PageSize
	bufferPool := storage.NewBufferPool(diskManager, bufferPoolPages)

	// Initialize storage engine (for backward compatibility)
	eng := engine.NewMemoryEngine()
	defer eng.Close()

	// Initialize transaction manager
	txnManager := txn.NewManager(eng, nil)

	// Initialize WAL manager (optional - can be nil)
	var walManager *wal.Manager
	if cfg.WAL.Enabled {
		walConfig := cfg.ToWALConfig()
		walManager, err = wal.NewManager(walConfig)
		if err != nil {
			logger.Warn("Failed to create WAL manager, continuing without WAL", "error", err)
			walManager = nil
		}
	}
	if walManager != nil {
		defer walManager.Close()
	}

	// Create MVCC storage backend
	storageBackend := executor.NewMVCCStorageBackend(bufferPool, cat, walManager, txnManager)

	// Create index manager
	indexMgr := index.NewManager(cat)

	// Configure server
	networkConfig := cfg.ToNetworkConfig()

	// Create and start server
	server := network.NewServerWithTxnManager(networkConfig, cat, eng, txnManager, logger)
	server.SetStorageBackend(storageBackend)
	server.SetIndexManager(indexMgr)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := server.Start(ctx); err != nil {
		logger.Error("Failed to start server", "error", err)
		os.Exit(1)
	}

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	logger.Info("Shutting down server")
	if err := server.Stop(); err != nil {
		logger.Error("Failed to stop server", "error", err)
		os.Exit(1)
	}

	logger.Info("Server stopped")
}
