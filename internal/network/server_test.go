package network

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/engine"
	"github.com/dshills/QuantaDB/internal/log"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

func TestServerStartStop(t *testing.T) {
	// Create test dependencies
	cat := catalog.NewMemoryCatalog()
	eng := engine.NewMemoryEngine()
	defer eng.Close()
	logger := log.NewTextLogger(slog.LevelInfo)

	// Create test table
	tableSchema := &catalog.TableSchema{
		SchemaName: "public",
		TableName:  "test",
		Columns: []catalog.ColumnDef{
			{Name: "id", DataType: types.Integer, IsNullable: false},
			{Name: "name", DataType: types.Text, IsNullable: true},
		},
	}
	if _, err := cat.CreateTable(tableSchema); err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Create server with test config
	config := Config{
		Host:           "localhost",
		Port:           54321, // Non-standard port for testing
		MaxConnections: 10,
		ReadTimeout:    5 * time.Second,
		WriteTimeout:   5 * time.Second,
	}

	server := NewServer(config, cat, eng, logger)

	// Start server
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Start(ctx); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}

	// Give server time to start
	time.Sleep(100 * time.Millisecond)

	// Check that server is listening
	if server.GetConnectionCount() != 0 {
		t.Errorf("Expected 0 connections, got %d", server.GetConnectionCount())
	}

	// Stop server
	if err := server.Stop(); err != nil {
		t.Fatalf("Failed to stop server: %v", err)
	}
}