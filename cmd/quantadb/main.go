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
	"github.com/dshills/QuantaDB/internal/engine"
	"github.com/dshills/QuantaDB/internal/log"
	"github.com/dshills/QuantaDB/internal/network"
	"github.com/dshills/QuantaDB/internal/sql/types"
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

	// Initialize logger
	level := slog.LevelInfo
	switch *logLevel {
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
		"host", *host,
		"port", *port,
		"data_dir", *dataDir)

	// Initialize catalog
	cat := catalog.NewMemoryCatalog()
	
	// Create some test tables for development
	if err := createTestTables(cat); err != nil {
		logger.Error("Failed to create test tables", "error", err)
		os.Exit(1)
	}

	// Initialize storage engine
	eng := engine.NewMemoryEngine()
	defer eng.Close()

	// Configure server
	config := network.Config{
		Host:           *host,
		Port:           *port,
		MaxConnections: 100,
	}

	// Create and start server
	server := network.NewServer(config, cat, eng, logger)
	
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

// createTestTables creates some test tables for development
func createTestTables(cat catalog.Catalog) error {
	// Create users table
	usersTable := &catalog.TableSchema{
		SchemaName: "public",
		TableName:  "users",
		Columns: []catalog.ColumnDef{
			{Name: "id", DataType: types.Integer, IsNullable: false},
			{Name: "name", DataType: types.Text, IsNullable: false},
			{Name: "email", DataType: types.Text, IsNullable: true},
			{Name: "age", DataType: types.Integer, IsNullable: true},
		},
		Constraints: []catalog.Constraint{
			catalog.PrimaryKeyConstraint{Columns: []string{"id"}},
		},
	}
	
	if _, err := cat.CreateTable(usersTable); err != nil {
		return fmt.Errorf("failed to create users table: %w", err)
	}

	// Create products table
	productsTable := &catalog.TableSchema{
		SchemaName: "public",
		TableName:  "products",
		Columns: []catalog.ColumnDef{
			{Name: "id", DataType: types.Integer, IsNullable: false},
			{Name: "name", DataType: types.Text, IsNullable: false},
			{Name: "price", DataType: types.Integer, IsNullable: false},
			{Name: "stock", DataType: types.Integer, IsNullable: false},
		},
		Constraints: []catalog.Constraint{
			catalog.PrimaryKeyConstraint{Columns: []string{"id"}},
		},
	}
	
	if _, err := cat.CreateTable(productsTable); err != nil {
		return fmt.Errorf("failed to create products table: %w", err)
	}

	return nil
}
