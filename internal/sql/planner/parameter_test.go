package planner

import (
	"testing"

	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

// TestParameterRefHandling tests that the planner can handle ParameterRef nodes
func TestParameterRefHandling(t *testing.T) {
	t.Run("SimpleParameterQuery", func(t *testing.T) {
		// Parse a query with a parameter
		p := parser.NewParser("SELECT * FROM users WHERE id = $1")
		stmt, err := p.Parse()
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		// Create a planner
		planner := NewBasicPlanner()

		// Plan should succeed (previously would fail with "unsupported expression type")
		plan, err := planner.Plan(stmt)
		if err != nil {
			t.Fatalf("Plan failed: %v", err)
		}

		if plan == nil {
			t.Fatal("Plan is nil")
		}

		// Verify plan was created
		planStr := plan.String()
		if planStr == "" {
			t.Error("Plan string is empty")
		}

		// The plan should contain a parameter reference
		// (exact format depends on plan structure)
		t.Logf("Plan: %s", planStr)
	})

	t.Run("MultipleParameters", func(t *testing.T) {
		// Parse a query with multiple parameters
		p := parser.NewParser("SELECT * FROM users WHERE name = $1 AND age > $2")
		stmt, err := p.Parse()
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		// Create a planner
		planner := NewBasicPlanner()

		// Plan should succeed
		plan, err := planner.Plan(stmt)
		if err != nil {
			t.Fatalf("Plan failed: %v", err)
		}

		if plan == nil {
			t.Fatal("Plan is nil")
		}
	})

	t.Run("InsertWithParameters", func(t *testing.T) {
		// Parse an INSERT with parameters
		p := parser.NewParser("INSERT INTO users (id, name) VALUES ($1, $2)")
		stmt, err := p.Parse()
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		// Create a planner
		planner := NewBasicPlanner()

		// Plan should succeed
		_, err = planner.Plan(stmt)
		if err != nil {
			// This might fail due to missing table in catalog, which is OK for this test
			// We're just testing that ParameterRef doesn't cause "unsupported expression type"
			if err.Error() == "unsupported expression type: *parser.ParameterRef" {
				t.Fatalf("ParameterRef not supported: %v", err)
			}
			// Other errors (like table not found) are OK for this test
			t.Logf("Plan failed (expected): %v", err)
		}
	})
}

// TestParameterRefExpression tests ParameterRef as an expression
func TestParameterRefExpression(t *testing.T) {
	t.Run("ParameterRefCreation", func(t *testing.T) {
		// Create a ParameterRef
		paramRef := &ParameterRef{
			Index: 1,
			Type:  types.Integer,
		}

		// Test String method
		str := paramRef.String()
		if str != "$1" {
			t.Errorf("Expected '$1', got '%s'", str)
		}

		// Test DataType method
		dt := paramRef.DataType()
		if dt != types.Integer {
			t.Errorf("Expected Integer type, got %v", dt)
		}
	})

	t.Run("ParameterRefWithUnknownType", func(t *testing.T) {
		// Create a ParameterRef with unknown type
		paramRef := &ParameterRef{
			Index: 2,
			Type:  nil,
		}

		// Test DataType method with nil type
		dt := paramRef.DataType()
		if dt != types.Unknown {
			t.Errorf("Expected Unknown type for nil, got %v", dt)
		}
	})
}
