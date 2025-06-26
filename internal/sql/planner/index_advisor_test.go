package planner

import (
	"testing"
	"time"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

func TestIndexAdvisor(t *testing.T) {
	// Create test catalog
	cat := createTestCatalogForAdvisor()
	costEstimator := NewCostEstimator(cat)
	advisor := NewIndexAdvisor(cat, costEstimator)

	t.Run("CompositeIndexRecommendation", func(t *testing.T) {
		// Create query patterns that would benefit from a composite index
		patterns := []QueryPattern{
			{
				Fingerprint:     "test1",
				QueryType:       QueryTypeSelect,
				PrimaryTable:    "orders",
				FilterColumns:   []string{"customer_id", "order_date"},
				ProjectColumns:  []string{"order_id", "total_amount"},
				ExecutionCount:  50,
				AverageDuration: 100 * time.Millisecond,
				ActualCost:      200.0,
				LastSeen:        time.Now(),
				EqualityFilters: map[string][]types.Value{
					"customer_id": {types.NewIntegerValue(100)},
				},
				RangeFilters: map[string]RangeFilter{
					"order_date": {
						HasLowerBound: true,
						LowerBound:    types.NewTextValue("2024-01-01"),
						Operator:      ">=",
					},
				},
			},
			{
				Fingerprint:     "test2",
				QueryType:       QueryTypeSelect,
				PrimaryTable:    "orders",
				FilterColumns:   []string{"customer_id", "order_date", "status"},
				ProjectColumns:  []string{"order_id", "total_amount", "status"},
				ExecutionCount:  30,
				AverageDuration: 150 * time.Millisecond,
				ActualCost:      300.0,
				LastSeen:        time.Now(),
				EqualityFilters: map[string][]types.Value{
					"customer_id": {types.NewIntegerValue(200)},
					"status":      {types.NewTextValue("pending")},
				},
			},
		}

		err := advisor.AnalyzeQueries(patterns)
		if err != nil {
			t.Fatalf("Failed to analyze queries: %v", err)
		}

		recommendations := advisor.GetRecommendations()
		if len(recommendations) == 0 {
			t.Fatal("Expected at least one recommendation")
		}

		// Should recommend a composite index on customer_id, order_date
		found := false
		for _, rec := range recommendations {
			if rec.IndexType == IndexTypeComposite &&
				len(rec.Columns) >= 2 &&
				contains(rec.Columns, "customer_id") &&
				contains(rec.Columns, "order_date") {
				found = true

				if rec.EstimatedBenefit <= 1.0 {
					t.Errorf("Expected benefit > 1.0, got %f", rec.EstimatedBenefit)
				}

				if rec.QueryCount < 30 { // At least one of the patterns should be included
					t.Errorf("Expected query count >= 30, got %d", rec.QueryCount)
				}

				if rec.Priority == PriorityLow {
					t.Errorf("Expected higher priority than Low")
				}
				break
			}
		}

		if !found {
			t.Error("Expected composite index recommendation not found")
		}
	})

	t.Run("CoveringIndexRecommendation", func(t *testing.T) {
		patterns := []QueryPattern{
			{
				Fingerprint:     "covering1",
				QueryType:       QueryTypeSelect,
				PrimaryTable:    "orders",
				FilterColumns:   []string{"customer_id"},
				ProjectColumns:  []string{"order_id", "total_amount", "order_date"},
				ExecutionCount:  100,
				AverageDuration: 80 * time.Millisecond,
				ActualCost:      400.0,
				LastSeen:        time.Now(),
				EqualityFilters: map[string][]types.Value{
					"customer_id": {types.NewIntegerValue(100)},
				},
			},
		}

		err := advisor.AnalyzeQueries(patterns)
		if err != nil {
			t.Fatalf("Failed to analyze queries: %v", err)
		}

		recommendations := advisor.GetRecommendations()

		// Should recommend a covering index
		found := false
		for _, rec := range recommendations {
			if rec.IndexType == IndexTypeCovering && len(rec.IncludeColumns) > 0 {
				found = true

				if !contains(rec.Columns, "customer_id") {
					t.Error("Expected customer_id in key columns")
				}

				if len(rec.IncludeColumns) == 0 {
					t.Error("Expected include columns for covering index")
				}

				if rec.EstimatedBenefit <= 1.0 {
					t.Errorf("Expected benefit > 1.0, got %f", rec.EstimatedBenefit)
				}
				break
			}
		}

		if !found {
			t.Error("Expected covering index recommendation not found")
		}
	})

	t.Run("SingleColumnRecommendation", func(t *testing.T) {
		patterns := []QueryPattern{
			{
				Fingerprint:     "single1",
				QueryType:       QueryTypeSelect,
				PrimaryTable:    "customers",
				FilterColumns:   []string{"email"},
				ProjectColumns:  []string{"customer_id", "name", "email"},
				ExecutionCount:  200,
				AverageDuration: 200 * time.Millisecond,
				ActualCost:      500.0,
				LastSeen:        time.Now(),
				EqualityFilters: map[string][]types.Value{
					"email": {types.NewTextValue("user@example.com")},
				},
			},
		}

		err := advisor.AnalyzeQueries(patterns)
		if err != nil {
			t.Fatalf("Failed to analyze queries: %v", err)
		}

		recommendations := advisor.GetRecommendations()

		// Should recommend a covering index on email (better than single-column since query projects other columns)
		found := false
		for _, rec := range recommendations {
			if rec.IndexType == IndexTypeCovering &&
				len(rec.Columns) == 1 &&
				rec.Columns[0] == "email" {
				found = true

				if rec.QueryCount != 200 {
					t.Errorf("Expected query count 200, got %d", rec.QueryCount)
				}

				if len(rec.IncludeColumns) == 0 {
					t.Error("Expected include columns for covering index")
				}
				break
			}
		}

		if !found {
			t.Error("Expected covering index recommendation not found")
		}
	})

	t.Run("FilterByFrequency", func(t *testing.T) {
		// Test that infrequent queries don't generate recommendations
		patterns := []QueryPattern{
			{
				Fingerprint:    "infrequent",
				QueryType:      QueryTypeSelect,
				PrimaryTable:   "orders",
				FilterColumns:  []string{"rare_column"},
				ExecutionCount: 2, // Below minimum threshold
				ActualCost:     100.0,
				LastSeen:       time.Now(),
			},
		}

		err := advisor.AnalyzeQueries(patterns)
		if err != nil {
			t.Fatalf("Failed to analyze queries: %v", err)
		}

		recommendations := advisor.GetRecommendations()

		// Should not recommend index for infrequent queries
		for _, rec := range recommendations {
			if contains(rec.Columns, "rare_column") {
				t.Error("Should not recommend index for infrequent queries")
			}
		}
	})

	t.Run("RecommendationPriorities", func(t *testing.T) {
		patterns := []QueryPattern{
			{
				Fingerprint:    "high_impact",
				QueryType:      QueryTypeSelect,
				PrimaryTable:   "orders",
				FilterColumns:  []string{"urgent_column"},
				ExecutionCount: 500,    // High frequency
				ActualCost:     1000.0, // High cost
				LastSeen:       time.Now(),
			},
			{
				Fingerprint:    "low_impact",
				QueryType:      QueryTypeSelect,
				PrimaryTable:   "orders",
				FilterColumns:  []string{"normal_column"},
				ExecutionCount: 10,
				ActualCost:     50.0,
				LastSeen:       time.Now(),
			},
		}

		err := advisor.AnalyzeQueries(patterns)
		if err != nil {
			t.Fatalf("Failed to analyze queries: %v", err)
		}

		recommendations := advisor.GetRecommendations()

		// Verify recommendations are sorted by priority
		if len(recommendations) >= 2 {
			// Higher priority recommendations should come first
			if recommendations[0].Priority < recommendations[1].Priority {
				t.Error("Recommendations should be sorted by priority (highest first)")
			}
		}
	})
}

func TestQueryPatternAnalyzer(t *testing.T) {
	analyzer := NewQueryPatternAnalyzer()

	t.Run("RecordAndAnalyzeQuery", func(t *testing.T) {
		// Create a simple logical plan
		scan := NewLogicalScan("users", "", nil)
		predicate := &BinaryOp{
			Left:     &ColumnRef{ColumnName: "user_id", ColumnType: types.Integer},
			Operator: OpEqual,
			Right:    &Literal{Value: types.NewIntegerValue(123), Type: types.Integer},
			Type:     types.Boolean,
		}
		filter := NewLogicalFilter(scan, predicate)

		sql := "SELECT * FROM users WHERE user_id = 123"
		executionTime := 50 * time.Millisecond

		err := analyzer.RecordQuery(sql, executionTime, 1000, 10, filter)
		if err != nil {
			t.Fatalf("Failed to record query: %v", err)
		}

		patterns := analyzer.GetPatterns()
		if len(patterns) != 1 {
			t.Fatalf("Expected 1 pattern, got %d", len(patterns))
		}

		pattern := patterns[0]
		if pattern.QueryType != QueryTypeSelect {
			t.Errorf("Expected SELECT query type, got %s", pattern.QueryType)
		}

		if pattern.PrimaryTable != "users" {
			t.Errorf("Expected primary table 'users', got '%s'", pattern.PrimaryTable)
		}

		if len(pattern.FilterColumns) == 0 {
			t.Error("Expected filter columns to be detected")
		}

		if pattern.ExecutionCount != 1 {
			t.Errorf("Expected execution count 1, got %d", pattern.ExecutionCount)
		}

		if pattern.AverageDuration != executionTime {
			t.Errorf("Expected duration %v, got %v", executionTime, pattern.AverageDuration)
		}
	})

	t.Run("MultipleExecutions", func(t *testing.T) {
		scan := NewLogicalScan("products", "", nil)
		filter := NewLogicalFilter(scan, &BinaryOp{
			Left:     &ColumnRef{ColumnName: "category", ColumnType: types.Text},
			Operator: OpEqual,
			Right:    &Literal{Value: types.NewTextValue("electronics"), Type: types.Text},
			Type:     types.Boolean,
		})

		sql := "SELECT * FROM products WHERE category = 'electronics'"

		// Record the same query pattern multiple times
		for i := 0; i < 5; i++ {
			err := analyzer.RecordQuery(sql, 30*time.Millisecond, 500, 25, filter)
			if err != nil {
				t.Fatalf("Failed to record query %d: %v", i, err)
			}
		}

		patterns := analyzer.GetPatterns()

		var productPattern *QueryPattern
		for _, p := range patterns {
			if p.PrimaryTable == "products" {
				productPattern = &p
				break
			}
		}

		if productPattern == nil {
			t.Fatal("Product query pattern not found")
		}

		if productPattern.ExecutionCount != 5 {
			t.Errorf("Expected execution count 5, got %d", productPattern.ExecutionCount)
		}

		if productPattern.AverageDuration != 30*time.Millisecond {
			t.Errorf("Expected average duration 30ms, got %v", productPattern.AverageDuration)
		}
	})

	t.Run("FrequentPatterns", func(t *testing.T) {
		frequent := analyzer.GetFrequentPatterns(3)

		// Should include patterns with >= 3 executions
		for _, pattern := range frequent {
			if pattern.ExecutionCount < 3 {
				t.Errorf("Frequent patterns should have >= 3 executions, got %d", pattern.ExecutionCount)
			}
		}
	})
}

// Helper function to create test catalog
func createTestCatalogForAdvisor() *MockCatalog {
	return &MockCatalog{
		tables: map[string]*catalog.Table{
			"public.orders": {
				ID:         1,
				SchemaName: "public",
				TableName:  "orders",
				Columns: []*catalog.Column{
					{ID: 1, Name: "order_id", DataType: types.Integer, OrdinalPosition: 1},
					{ID: 2, Name: "customer_id", DataType: types.Integer, OrdinalPosition: 2},
					{ID: 3, Name: "order_date", DataType: types.Date, OrdinalPosition: 3},
					{ID: 4, Name: "total_amount", DataType: types.Decimal(10, 2), OrdinalPosition: 4},
					{ID: 5, Name: "status", DataType: types.Text, OrdinalPosition: 5},
				},
				Indexes: []*catalog.Index{
					// Existing primary key index
					{
						ID:      1,
						Name:    "orders_pkey",
						TableID: 1,
						Type:    catalog.BTreeIndex,
						Columns: []catalog.IndexColumn{
							{Column: &catalog.Column{Name: "order_id", DataType: types.Integer}, Position: 0},
						},
					},
				},
			},
			"public.customers": {
				ID:         2,
				SchemaName: "public",
				TableName:  "customers",
				Columns: []*catalog.Column{
					{ID: 1, Name: "customer_id", DataType: types.Integer, OrdinalPosition: 1},
					{ID: 2, Name: "name", DataType: types.Text, OrdinalPosition: 2},
					{ID: 3, Name: "email", DataType: types.Text, OrdinalPosition: 3},
					{ID: 4, Name: "created_at", DataType: types.Timestamp, OrdinalPosition: 4},
				},
				Indexes: []*catalog.Index{
					{
						ID:      2,
						Name:    "customers_pkey",
						TableID: 2,
						Type:    catalog.BTreeIndex,
						Columns: []catalog.IndexColumn{
							{Column: &catalog.Column{Name: "customer_id", DataType: types.Integer}, Position: 0},
						},
					},
				},
			},
		},
	}
}
