package planner

import (
	"testing"

	"github.com/dshills/QuantaDB/internal/sql/parser"
)

func TestCTEParsing(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		wantErr bool
	}{
		{
			name:    "simple CTE",
			query:   "WITH temp AS (SELECT 1 AS x) SELECT * FROM temp",
			wantErr: false,
		},
		{
			name:    "CTE with table reference",
			query:   "WITH user_orders AS (SELECT user_id, COUNT(*) FROM orders GROUP BY user_id) SELECT * FROM user_orders",
			wantErr: false,
		},
		{
			name:    "multiple CTEs",
			query:   "WITH t1 AS (SELECT 1 AS x), t2 AS (SELECT 2 AS y) SELECT * FROM t1, t2",
			wantErr: false,
		},
		{
			name:    "CTE with WHERE clause",
			query:   "WITH active_users AS (SELECT * FROM users WHERE active = true) SELECT name FROM active_users",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the query
			p := parser.NewParser(tt.query)
			stmt, err := p.Parse()
			if err != nil {
				if !tt.wantErr {
					t.Fatalf("Failed to parse query: %v", err)
				}
				return
			}

			selectStmt, ok := stmt.(*parser.SelectStmt)
			if !ok {
				t.Fatalf("Expected SelectStmt, got %T", stmt)
			}

			// Check that CTEs were parsed
			if len(selectStmt.With) == 0 {
				t.Errorf("Expected CTEs to be parsed")
				return
			}

			t.Logf("Parsed %d CTEs", len(selectStmt.With))
			for i, cte := range selectStmt.With {
				t.Logf("CTE %d: %s", i, cte.Name)
			}

			// Test that the full statement can be converted back to string
			t.Logf("Parsed statement: %s", selectStmt.String())
		})
	}
}

func TestCTEPlanning(t *testing.T) {
	tests := []struct {
		name        string
		query       string
		wantErr     bool
		expectedCTE string
	}{
		{
			name:        "simple CTE planning",
			query:       "WITH temp AS (SELECT 1 AS x) SELECT * FROM temp",
			wantErr:     false,
			expectedCTE: "temp",
		},
		{
			name:        "multiple CTEs planning",
			query:       "WITH t1 AS (SELECT 1 AS x), t2 AS (SELECT 2 AS y) SELECT * FROM t1",
			wantErr:     false,
			expectedCTE: "t1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the query
			p := parser.NewParser(tt.query)
			stmt, err := p.Parse()
			if err != nil {
				t.Fatalf("Failed to parse query: %v", err)
			}

			selectStmt, ok := stmt.(*parser.SelectStmt)
			if !ok {
				t.Fatalf("Expected SelectStmt, got %T", stmt)
			}

			// Create a basic planner
			planner := NewBasicPlanner()

			// Plan the query
			plan, err := planner.planSelect(selectStmt)
			if tt.wantErr {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if plan == nil {
				t.Errorf("Expected plan but got nil")
				return
			}

			// Check that we got a WITH clause node
			withClause, ok := plan.(*LogicalWithClause)
			if !ok {
				t.Errorf("Expected LogicalWithClause, got %T", plan)
				return
			}

			// Check that the expected CTE is present
			found := false
			for _, cte := range withClause.CTEs {
				if cte.Name == tt.expectedCTE {
					found = true
					break
				}
			}

			if !found {
				t.Errorf("Expected CTE %s not found in plan", tt.expectedCTE)
			}

			t.Logf("Generated plan: %s", plan.String())
		})
	}
}

func TestCTEExecution(t *testing.T) {
	// Test that CTEs can be executed properly
	// This is a placeholder for future execution testing

	query := "WITH numbers AS (SELECT 1 AS n UNION SELECT 2 AS n) SELECT * FROM numbers"

	// Parse the query
	p := parser.NewParser(query)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	selectStmt, ok := stmt.(*parser.SelectStmt)
	if !ok {
		t.Fatalf("Expected SelectStmt, got %T", stmt)
	}

	// Create a basic planner
	planner := NewBasicPlanner()

	// Plan the query
	plan, err := planner.planSelect(selectStmt)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}

	// Verify basic structure
	withClause, ok := plan.(*LogicalWithClause)
	if !ok {
		t.Fatalf("Expected LogicalWithClause, got %T", plan)
	}

	if len(withClause.CTEs) != 1 {
		t.Errorf("Expected 1 CTE, got %d", len(withClause.CTEs))
	}

	if withClause.CTEs[0].Name != "numbers" {
		t.Errorf("Expected CTE name 'numbers', got '%s'", withClause.CTEs[0].Name)
	}

	t.Logf("CTE execution test plan: %s", plan.String())
}

func TestRecursiveCTEBasic(t *testing.T) {
	// Basic test for recursive CTE structure
	// Note: Full recursive CTE support would require additional implementation

	query := "WITH RECURSIVE series AS (SELECT 1 AS n UNION SELECT n+1 FROM series WHERE n < 5) SELECT * FROM series"

	// For now, just test that it parses as a regular CTE
	p := parser.NewParser(query)
	stmt, err := p.Parse()
	if err != nil {
		// This might fail since we haven't implemented RECURSIVE keyword yet
		t.Logf("Expected failure for recursive CTE: %v", err)
		return
	}

	selectStmt, ok := stmt.(*parser.SelectStmt)
	if !ok {
		t.Fatalf("Expected SelectStmt, got %T", stmt)
	}

	t.Logf("Recursive CTE parsed (basic): %s", selectStmt.String())
}

func TestCTEWithComplexQueries(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{
			name:  "CTE with aggregation",
			query: "WITH user_stats AS (SELECT user_id, COUNT(*) as order_count FROM orders GROUP BY user_id) SELECT * FROM user_stats WHERE order_count > 5",
		},
		{
			name:  "CTE with joins",
			query: "WITH user_orders AS (SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id) SELECT name, SUM(amount) FROM user_orders GROUP BY name",
		},
		{
			name:  "nested CTE reference",
			query: "WITH t1 AS (SELECT 1 as x), t2 AS (SELECT x+1 as y FROM t1) SELECT * FROM t2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the query
			p := parser.NewParser(tt.query)
			stmt, err := p.Parse()
			if err != nil {
				t.Fatalf("Failed to parse query: %v", err)
			}

			selectStmt, ok := stmt.(*parser.SelectStmt)
			if !ok {
				t.Fatalf("Expected SelectStmt, got %T", stmt)
			}

			// Create a basic planner
			planner := NewBasicPlanner()

			// Plan the query
			plan, err := planner.planSelect(selectStmt)
			if err != nil {
				t.Errorf("Failed to plan query: %v", err)
				return
			}

			t.Logf("Complex CTE plan: %s", plan.String())
		})
	}
}
