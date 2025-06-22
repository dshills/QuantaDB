package planner

import (
	"testing"

	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

func TestPlanPrepare(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name: "simple PREPARE",
			sql:  "PREPARE stmt1 AS SELECT * FROM users WHERE id = $1",
		},
		{
			name: "PREPARE with parameter types",
			sql:  "PREPARE stmt2 (INTEGER, VARCHAR) AS SELECT * FROM users WHERE id = $1 AND name = $2",
		},
		{
			name: "PREPARE with INSERT",
			sql:  "PREPARE insert_user AS INSERT INTO users (name, email) VALUES ($1, $2)",
		},
		{
			name: "PREPARE with UPDATE",
			sql:  "PREPARE update_user AS UPDATE users SET name = $1 WHERE id = $2",
		},
		{
			name: "PREPARE with DELETE",
			sql:  "PREPARE delete_user AS DELETE FROM users WHERE id = $1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the SQL
			p := parser.NewParser(tt.sql)
			stmt, err := p.Parse()
			if err != nil {
				t.Fatalf("failed to parse: %v", err)
			}

			// Plan the statement
			planner := NewBasicPlanner()
			plan, err := planner.Plan(stmt)
			if (err != nil) != tt.wantErr {
				t.Errorf("Plan() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if err == nil {
				// Verify it's a LogicalPrepare
				prepPlan, ok := plan.(*LogicalPrepare)
				if !ok {
					t.Errorf("expected *LogicalPrepare, got %T", plan)
					return
				}

				// Verify the plan has correct type
				if prepPlan.Type() != "Prepare" {
					t.Errorf("expected Type() = 'Prepare', got '%s'", prepPlan.Type())
				}

				// Verify the schema
				schema := prepPlan.Schema()
				if schema == nil {
					t.Error("expected non-nil schema")
				} else if len(schema.Columns) != 1 {
					t.Errorf("expected 1 column, got %d", len(schema.Columns))
				} else if schema.Columns[0].Name != "result" {
					t.Errorf("expected column name 'result', got '%s'", schema.Columns[0].Name)
				}
			}
		})
	}
}

func TestPlanExecute(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name: "EXECUTE without parameters",
			sql:  "EXECUTE stmt1",
		},
		{
			name: "EXECUTE with parameters",
			sql:  "EXECUTE stmt1 (123, 'test')",
		},
		{
			name: "EXECUTE with expressions",
			sql:  "EXECUTE calc_stmt (1 + 2, user_id * 10)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the SQL
			p := parser.NewParser(tt.sql)
			stmt, err := p.Parse()
			if err != nil {
				t.Fatalf("failed to parse: %v", err)
			}

			// Plan the statement
			planner := NewBasicPlanner()
			plan, err := planner.Plan(stmt)
			if (err != nil) != tt.wantErr {
				t.Errorf("Plan() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if err == nil {
				// Verify it's a LogicalExecute
				execPlan, ok := plan.(*LogicalExecute)
				if !ok {
					t.Errorf("expected *LogicalExecute, got %T", plan)
					return
				}

				// Verify the plan has correct type
				if execPlan.Type() != "Execute" {
					t.Errorf("expected Type() = 'Execute', got '%s'", execPlan.Type())
				}

				// The schema should be empty as it depends on the prepared statement
				schema := execPlan.Schema()
				if schema == nil {
					t.Error("expected non-nil schema")
				} else if len(schema.Columns) != 0 {
					t.Errorf("expected 0 columns, got %d", len(schema.Columns))
				}
			}
		})
	}
}

func TestPlanDeallocate(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name: "DEALLOCATE simple",
			sql:  "DEALLOCATE stmt1",
		},
		{
			name: "DEALLOCATE PREPARE",
			sql:  "DEALLOCATE PREPARE stmt2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the SQL
			p := parser.NewParser(tt.sql)
			stmt, err := p.Parse()
			if err != nil {
				t.Fatalf("failed to parse: %v", err)
			}

			// Plan the statement
			planner := NewBasicPlanner()
			plan, err := planner.Plan(stmt)
			if (err != nil) != tt.wantErr {
				t.Errorf("Plan() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if err == nil {
				// Verify it's a LogicalDeallocate
				deallocPlan, ok := plan.(*LogicalDeallocate)
				if !ok {
					t.Errorf("expected *LogicalDeallocate, got %T", plan)
					return
				}

				// Verify the plan has correct type
				if deallocPlan.Type() != "Deallocate" {
					t.Errorf("expected Type() = 'Deallocate', got '%s'", deallocPlan.Type())
				}

				// Verify the schema
				schema := deallocPlan.Schema()
				if schema == nil {
					t.Error("expected non-nil schema")
				} else if len(schema.Columns) != 1 {
					t.Errorf("expected 1 column, got %d", len(schema.Columns))
				} else if schema.Columns[0].Name != "result" {
					t.Errorf("expected column name 'result', got '%s'", schema.Columns[0].Name)
				}
			}
		})
	}
}

// TestPreparedStatementPlanString tests the string representation of prepared statement plans
func TestPreparedStatementPlanString(t *testing.T) {
	tests := []struct {
		name     string
		plan     LogicalPlan
		expected string
	}{
		{
			name:     "LogicalPrepare",
			plan:     NewLogicalPrepare("stmt1", []types.DataType{types.Integer, types.Text}, nil),
			expected: "Prepare(stmt1)",
		},
		{
			name:     "LogicalExecute",
			plan:     NewLogicalExecute("stmt1", nil),
			expected: "Execute(stmt1)",
		},
		{
			name:     "LogicalDeallocate",
			plan:     NewLogicalDeallocate("stmt1"),
			expected: "Deallocate(stmt1)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.plan.String()
			if got != tt.expected {
				t.Errorf("String() = %q, want %q", got, tt.expected)
			}
		})
	}
}