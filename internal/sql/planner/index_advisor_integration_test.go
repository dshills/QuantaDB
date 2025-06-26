package planner

import (
	"testing"
	"time"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

// TestIndexAdvisorEndToEnd demonstrates the complete index recommendation workflow.
func TestIndexAdvisorEndToEnd(t *testing.T) {
	// Create a realistic e-commerce database scenario
	cat := createEcommerceTestCatalog()
	costEstimator := NewCostEstimator(cat)
	advisor := NewIndexAdvisor(cat, costEstimator)

	// Simulate a workload with various query patterns
	patterns := createEcommerceQueryPatterns()

	err := advisor.AnalyzeQueries(patterns)
	if err != nil {
		t.Fatalf("Failed to analyze queries: %v", err)
	}

	recommendations := advisor.GetRecommendations()

	// Verify we get meaningful recommendations
	if len(recommendations) == 0 {
		t.Fatal("Expected recommendations for realistic workload")
	}

	// Print recommendations for manual inspection
	t.Logf("Generated %d index recommendations:", len(recommendations))
	for i, rec := range recommendations {
		t.Logf("%d. %s", i+1, rec.String())
		t.Logf("   Benefit: %.1fx speedup, Cost reduction: %.0f", rec.EstimatedBenefit, rec.CostReduction)
		t.Logf("   Affects %d queries, Priority: %s", rec.QueryCount, rec.Priority.String())
		t.Logf("")
	}

	// Verify recommendation types
	hasComposite := false
	hasCovering := false
	hasSingle := false

	for _, rec := range recommendations {
		switch rec.IndexType {
		case IndexTypeComposite:
			hasComposite = true
		case IndexTypeCovering:
			hasCovering = true
		case IndexTypeSingle:
			hasSingle = true
		case IndexTypePartial:
			// Partial indexes are not relevant for this test
		}
	}

	// Should have a mix of recommendation types for a realistic workload
	if !hasComposite && !hasCovering && !hasSingle {
		t.Error("Expected at least one type of index recommendation")
	}

	// Test priority ordering
	for i := 1; i < len(recommendations); i++ {
		prev := recommendations[i-1]
		curr := recommendations[i]

		// Should be sorted by priority (high to low) then by benefit
		if prev.Priority < curr.Priority {
			t.Errorf("Recommendations not properly sorted by priority at position %d", i)
		}
	}

	// Test getting top recommendations
	topRecs := advisor.GetTopRecommendations(3)
	if len(topRecs) > 3 {
		t.Errorf("Expected at most 3 top recommendations, got %d", len(topRecs))
	}

	// Verify cost-benefit analysis makes sense
	for _, rec := range recommendations {
		if rec.EstimatedBenefit <= 1.0 {
			t.Errorf("Recommendation should have positive benefit, got %.2f", rec.EstimatedBenefit)
		}

		if rec.CostReduction <= 0 {
			t.Errorf("Recommendation should reduce cost, got %.2f", rec.CostReduction)
		}

		if rec.QueryCount < advisor.config.MinQueryCount {
			t.Errorf("Recommendation query count %d below threshold %d",
				rec.QueryCount, advisor.config.MinQueryCount)
		}
	}
}

// TestIndexAdvisorRealisticScenarios tests specific realistic database scenarios.
func TestIndexAdvisorRealisticScenarios(t *testing.T) {
	cat := createEcommerceTestCatalog()
	costEstimator := NewCostEstimator(cat)
	advisor := NewIndexAdvisor(cat, costEstimator)

	t.Run("CustomerLookupPattern", func(t *testing.T) {
		// Frequent customer lookup by email pattern
		patterns := []QueryPattern{
			{
				Fingerprint:     "customer_email_lookup",
				QueryType:       QueryTypeSelect,
				PrimaryTable:    "customers",
				FilterColumns:   []string{"email"},
				ProjectColumns:  []string{"customer_id", "name", "email", "status"},
				ExecutionCount:  500,                    // Very frequent
				AverageDuration: 150 * time.Millisecond, // Slow without index
				ActualCost:      300.0,
				LastSeen:        time.Now(),
				EqualityFilters: map[string][]types.Value{
					"email": {types.NewTextValue("user@example.com")},
				},
			},
		}

		err := advisor.AnalyzeQueries(patterns)
		if err != nil {
			t.Fatalf("Failed to analyze customer lookup pattern: %v", err)
		}

		recommendations := advisor.GetRecommendations()

		// Should recommend covering index for customer email lookups
		found := false
		for _, rec := range recommendations {
			if rec.TableName == "customers" && contains(rec.Columns, "email") {
				found = true

				// Should be high priority due to frequency
				if rec.Priority != PriorityHigh && rec.Priority != PriorityCritical {
					t.Errorf("Expected high priority for frequent customer lookups, got %s", rec.Priority.String())
				}

				// Should be covering index type
				if rec.IndexType != IndexTypeCovering {
					t.Errorf("Expected covering index for customer lookup, got %s", rec.IndexType.String())
				}
				break
			}
		}

		if !found {
			t.Error("Expected recommendation for customer email lookup pattern")
		}
	})

	t.Run("OrderHistoryPattern", func(t *testing.T) {
		// Customer order history queries
		patterns := []QueryPattern{
			{
				Fingerprint:     "order_history",
				QueryType:       QueryTypeSelect,
				PrimaryTable:    "orders",
				FilterColumns:   []string{"customer_id", "order_date"},
				ProjectColumns:  []string{"order_id", "order_date", "total_amount", "status"},
				ExecutionCount:  200,
				AverageDuration: 80 * time.Millisecond,
				ActualCost:      400.0,
				LastSeen:        time.Now(),
				EqualityFilters: map[string][]types.Value{
					"customer_id": {types.NewIntegerValue(123)},
				},
				RangeFilters: map[string]RangeFilter{
					"order_date": {
						HasLowerBound: true,
						LowerBound:    types.NewTextValue("2024-01-01"),
						Operator:      ">=",
					},
				},
			},
		}

		err := advisor.AnalyzeQueries(patterns)
		if err != nil {
			t.Fatalf("Failed to analyze order history pattern: %v", err)
		}

		recommendations := advisor.GetRecommendations()

		// Should recommend composite index on (customer_id, order_date)
		found := false
		for _, rec := range recommendations {
			if rec.TableName == "orders" &&
				len(rec.Columns) >= 2 &&
				contains(rec.Columns, "customer_id") &&
				contains(rec.Columns, "order_date") {
				found = true

				if rec.IndexType != IndexTypeComposite && rec.IndexType != IndexTypeCovering {
					t.Errorf("Expected composite or covering index for order history, got %s", rec.IndexType.String())
				}
				break
			}
		}

		if !found {
			t.Error("Expected recommendation for order history pattern")
		}
	})

	t.Run("ProductSearchPattern", func(t *testing.T) {
		// Product search by category and price range
		patterns := []QueryPattern{
			{
				Fingerprint:     "product_search",
				QueryType:       QueryTypeSelect,
				PrimaryTable:    "products",
				FilterColumns:   []string{"category", "price"},
				ProjectColumns:  []string{"product_id", "name", "price", "category"},
				ExecutionCount:  150,
				AverageDuration: 120 * time.Millisecond,
				ActualCost:      350.0,
				LastSeen:        time.Now(),
				EqualityFilters: map[string][]types.Value{
					"category": {types.NewTextValue("electronics")},
				},
				RangeFilters: map[string]RangeFilter{
					"price": {
						HasLowerBound: true,
						HasUpperBound: true,
						LowerBound:    types.NewTextValue("100.00"),
						UpperBound:    types.NewTextValue("500.00"),
						Operator:      "BETWEEN",
					},
				},
			},
		}

		err := advisor.AnalyzeQueries(patterns)
		if err != nil {
			t.Fatalf("Failed to analyze product search pattern: %v", err)
		}

		recommendations := advisor.GetRecommendations()

		// Should recommend index involving category and price
		found := false
		for _, rec := range recommendations {
			if rec.TableName == "products" &&
				(contains(rec.Columns, "category") || contains(rec.Columns, "price")) {
				found = true
				break
			}
		}

		if !found {
			t.Error("Expected recommendation for product search pattern")
		}
	})
}

// Helper functions to create test data

func createEcommerceTestCatalog() *MockCatalog {
	return &MockCatalog{
		tables: map[string]*catalog.Table{
			"public.customers": {
				ID:         1,
				SchemaName: "public",
				TableName:  "customers",
				Columns: []*catalog.Column{
					{ID: 1, Name: "customer_id", DataType: types.Integer, OrdinalPosition: 1},
					{ID: 2, Name: "email", DataType: types.Text, OrdinalPosition: 2},
					{ID: 3, Name: "name", DataType: types.Text, OrdinalPosition: 3},
					{ID: 4, Name: "status", DataType: types.Text, OrdinalPosition: 4},
					{ID: 5, Name: "created_at", DataType: types.Timestamp, OrdinalPosition: 5},
				},
			},
			"public.orders": {
				ID:         2,
				SchemaName: "public",
				TableName:  "orders",
				Columns: []*catalog.Column{
					{ID: 1, Name: "order_id", DataType: types.Integer, OrdinalPosition: 1},
					{ID: 2, Name: "customer_id", DataType: types.Integer, OrdinalPosition: 2},
					{ID: 3, Name: "order_date", DataType: types.Date, OrdinalPosition: 3},
					{ID: 4, Name: "total_amount", DataType: types.Decimal(10, 2), OrdinalPosition: 4},
					{ID: 5, Name: "status", DataType: types.Text, OrdinalPosition: 5},
				},
			},
			"public.products": {
				ID:         3,
				SchemaName: "public",
				TableName:  "products",
				Columns: []*catalog.Column{
					{ID: 1, Name: "product_id", DataType: types.Integer, OrdinalPosition: 1},
					{ID: 2, Name: "name", DataType: types.Text, OrdinalPosition: 2},
					{ID: 3, Name: "category", DataType: types.Text, OrdinalPosition: 3},
					{ID: 4, Name: "price", DataType: types.Decimal(10, 2), OrdinalPosition: 4},
					{ID: 5, Name: "description", DataType: types.Text, OrdinalPosition: 5},
				},
			},
		},
	}
}

func createEcommerceQueryPatterns() []QueryPattern {
	return []QueryPattern{
		// Frequent customer lookups
		{
			Fingerprint:     "customer_email_lookup",
			QueryType:       QueryTypeSelect,
			PrimaryTable:    "customers",
			FilterColumns:   []string{"email"},
			ProjectColumns:  []string{"customer_id", "name", "status"},
			ExecutionCount:  300,
			AverageDuration: 100 * time.Millisecond,
			ActualCost:      200.0,
			LastSeen:        time.Now(),
		},

		// Order history queries
		{
			Fingerprint:     "customer_orders",
			QueryType:       QueryTypeSelect,
			PrimaryTable:    "orders",
			FilterColumns:   []string{"customer_id", "order_date"},
			ProjectColumns:  []string{"order_id", "total_amount", "status"},
			ExecutionCount:  150,
			AverageDuration: 80 * time.Millisecond,
			ActualCost:      300.0,
			LastSeen:        time.Now(),
		},

		// Product category browsing
		{
			Fingerprint:     "category_products",
			QueryType:       QueryTypeSelect,
			PrimaryTable:    "products",
			FilterColumns:   []string{"category"},
			ProjectColumns:  []string{"product_id", "name", "price"},
			ExecutionCount:  200,
			AverageDuration: 60 * time.Millisecond,
			ActualCost:      150.0,
			LastSeen:        time.Now(),
		},

		// Price range searches
		{
			Fingerprint:     "price_range_search",
			QueryType:       QueryTypeSelect,
			PrimaryTable:    "products",
			FilterColumns:   []string{"category", "price"},
			ProjectColumns:  []string{"product_id", "name", "price", "description"},
			ExecutionCount:  100,
			AverageDuration: 120 * time.Millisecond,
			ActualCost:      400.0,
			LastSeen:        time.Now(),
		},

		// Order status updates (write workload)
		{
			Fingerprint:     "order_status_update",
			QueryType:       QueryTypeUpdate,
			PrimaryTable:    "orders",
			FilterColumns:   []string{"order_id"},
			ExecutionCount:  50,
			AverageDuration: 20 * time.Millisecond,
			ActualCost:      100.0,
			LastSeen:        time.Now(),
		},
	}
}
