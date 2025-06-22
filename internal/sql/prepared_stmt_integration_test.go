package sql_test

import (
	"testing"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/executor"
	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/planner"
)

func TestPreparedStatementIntegration(t *testing.T) {
	// Create a catalog
	cat := catalog.NewMemoryCatalog()
	
	// Create planner and executor
	plan := planner.NewBasicPlannerWithCatalog(cat)
	exec := executor.NewBasicExecutor(cat, nil)
	
	// Create execution context
	ctx := &executor.ExecContext{
		Catalog: cat,
		Stats:   &executor.ExecStats{},
		Planner: plan,
	}
	
	// Test PREPARE statement
	t.Run("PREPARE", func(t *testing.T) {
		sql := "PREPARE stmt1 AS SELECT * FROM users WHERE id = $1"
		
		// Parse
		p := parser.NewParser(sql)
		stmt, err := p.Parse()
		if err != nil {
			t.Fatalf("failed to parse: %v", err)
		}
		
		// Plan
		logicalPlan, err := plan.Plan(stmt)
		if err != nil {
			t.Fatalf("failed to plan: %v", err)
		}
		
		// Execute
		result, err := exec.Execute(logicalPlan, ctx)
		if err != nil {
			t.Fatalf("failed to execute: %v", err)
		}
		defer result.Close()
		
		// Check result
		row, err := result.Next()
		if err != nil {
			t.Fatalf("failed to get result: %v", err)
		}
		
		if row == nil {
			t.Fatal("expected a result row")
		}
		
		if len(row.Values) != 1 {
			t.Fatalf("expected 1 value, got %d", len(row.Values))
		}
		
		// Verify prepared statement was stored
		if ctx.PreparedStatements == nil || len(ctx.PreparedStatements) != 1 {
			t.Fatal("prepared statement not stored in context")
		}
		
		if _, exists := ctx.PreparedStatements["stmt1"]; !exists {
			t.Fatal("prepared statement 'stmt1' not found")
		}
		
	})
	
	// Test DEALLOCATE statement
	t.Run("DEALLOCATE", func(t *testing.T) {
		
		sql := "DEALLOCATE stmt1"
		
		// Parse
		p := parser.NewParser(sql)
		stmt, err := p.Parse()
		if err != nil {
			t.Fatalf("failed to parse: %v", err)
		}
		
		// Plan
		logicalPlan, err := plan.Plan(stmt)
		if err != nil {
			t.Fatalf("failed to plan: %v", err)
		}
		
		// Execute
		result, err := exec.Execute(logicalPlan, ctx)
		if err != nil {
			t.Fatalf("failed to execute: %v", err)
		}
		defer result.Close()
		
		// Check result
		row, err := result.Next()
		if err != nil {
			t.Fatalf("failed to get result: %v", err)
		}
		
		if row == nil {
			t.Fatal("expected a result row")
		}
		
		// Verify prepared statement was removed
		if _, exists := ctx.PreparedStatements["stmt1"]; exists {
			t.Fatal("prepared statement 'stmt1' should have been removed")
		}
		
		// Check if context still has statements map
		if ctx.PreparedStatements == nil {
			t.Fatal("PreparedStatements map should not be nil after DEALLOCATE")
		}
	})
	
	// Test EXECUTE statement (should fail as not fully implemented)
	t.Run("EXECUTE", func(t *testing.T) {
		
		// First prepare a statement
		prepareSQL := "PREPARE stmt2 AS SELECT 1"
		p := parser.NewParser(prepareSQL)
		stmt, err := p.Parse()
		if err != nil {
			t.Fatalf("failed to parse PREPARE: %v", err)
		}
		
		logicalPlan, err := plan.Plan(stmt)
		if err != nil {
			t.Fatalf("failed to plan PREPARE: %v", err)
		}
		
		result, err := exec.Execute(logicalPlan, ctx)
		if err != nil {
			t.Fatalf("failed to execute PREPARE: %v", err)
		}
		
		// Get the result row
		row, err := result.Next()
		if err != nil {
			t.Fatalf("failed to get PREPARE result: %v", err)
		}
		if row == nil {
			t.Fatal("expected a result row from PREPARE")
		}
		
		result.Close()
		
		// Verify stmt2 exists
		if ctx.PreparedStatements == nil {
			t.Fatal("PreparedStatements is nil after PREPARE")
		}
		
		if ctx.PreparedStatements["stmt2"] == nil {
			t.Fatal("stmt2 not found in context after PREPARE")
		}
		
		// Try to execute it
		executeSQL := "EXECUTE stmt2"
		p = parser.NewParser(executeSQL)
		stmt, err = p.Parse()
		if err != nil {
			t.Fatalf("failed to parse: %v", err)
		}
		
		// Plan
		logicalPlan, err = plan.Plan(stmt)
		if err != nil {
			t.Fatalf("failed to plan: %v", err)
		}
		
		// Execute - should fail with "not fully implemented"
		_, err = exec.Execute(logicalPlan, ctx)
		if err == nil {
			t.Fatal("expected error for EXECUTE")
		}
		
		if err.Error() != "failed to open operator: EXECUTE not fully implemented yet" {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}