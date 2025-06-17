package executor

import (
	"testing"

	"github.com/dshills/QuantaDB/internal/sql/types"
)

// Simple test to verify physical planner basics.
func TestPhysicalPlannerBasics(t *testing.T) {
	catalog := &mockCatalogProvider{
		tables: map[string]*TableStats{
			"public.users": {
				RowCount:   1000,
				BlockCount: 10,
				AvgRowSize: 100,
			},
		},
	}
	stats := &mockStatsProvider{}
	planner := NewPhysicalPlanner(catalog, stats)

	if planner == nil {
		t.Fatal("Failed to create physical planner")
	}

	// Test that we can create basic structures
	scan := &PhysicalScan{
		TableName:   "users",
		cardinality: 1000,
		cost:        10.0,
	}

	if scan.Cardinality() != 1000 {
		t.Errorf("Expected cardinality 1000, got %d", scan.Cardinality())
	}

	if scan.Cost() != 10.0 {
		t.Errorf("Expected cost 10.0, got %f", scan.Cost())
	}
}

// Mock implementations for testing.
type mockCatalogProvider struct {
	tables map[string]*TableStats
}

func (m *mockCatalogProvider) GetTableStats(schema, table string) (*TableStats, error) {
	key := schema + "." + table
	if stats, ok := m.tables[key]; ok {
		return stats, nil
	}
	// Return default stats
	return &TableStats{
		RowCount:   1000,
		BlockCount: 10,
		AvgRowSize: 100,
	}, nil
}

type mockStatsProvider struct{}

func (m *mockStatsProvider) GetColumnStats(schema, table, column string) (*ColumnStats, error) {
	return &ColumnStats{
		DistinctCount: 100,
		NullCount:     10,
		MinValue:      types.NewValue(int64(1)),
		MaxValue:      types.NewValue(int64(1000)),
	}, nil
}
