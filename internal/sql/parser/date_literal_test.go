package parser

import (
	"testing"
	"time"

	"github.com/dshills/QuantaDB/internal/sql/types"
)

func TestDateLiteral(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
		wantDate string
	}{
		{
			name:     "Simple date literal",
			sql:      "SELECT date '1995-03-15'",
			wantDate: "1995-03-15",
		},
		{
			name:     "Date in WHERE clause", 
			sql:      "SELECT * FROM orders WHERE o_orderdate < date '1995-03-15'",
			wantDate: "1995-03-15",
		},
		{
			name:     "Date with dashes",
			sql:      "SELECT date '2023-12-25'",
			wantDate: "2023-12-25",
		},
		{
			name:    "Invalid date format",
			sql:     "SELECT date 'invalid-date'",
			wantErr: true,
		},
		{
			name:    "Missing quotes",
			sql:     "SELECT date 1995-03-15",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.sql)
			stmt, err := p.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}

			// Extract the date literal from the parsed statement
			selectStmt, ok := stmt.(*SelectStmt)
			if !ok {
				t.Fatalf("Expected SelectStmt, got %T", stmt)
			}

			// Find the date literal
			var dateLiteral *Literal
			
			// Helper function to find date literals in expressions
			var findDateLiteral func(Expression) *Literal
			findDateLiteral = func(expr Expression) *Literal {
				switch e := expr.(type) {
				case *Literal:
					if _, ok := e.Value.Data.(time.Time); ok {
						return e
					}
				case *BinaryExpr:
					if lit := findDateLiteral(e.Left); lit != nil {
						return lit
					}
					if lit := findDateLiteral(e.Right); lit != nil {
						return lit
					}
				case *ComparisonExpr:
					if lit := findDateLiteral(e.Left); lit != nil {
						return lit
					}
					if lit := findDateLiteral(e.Right); lit != nil {
						return lit
					}
				}
				return nil
			}
			
			// Check in SELECT columns
			if len(selectStmt.Columns) > 0 {
				if lit := findDateLiteral(selectStmt.Columns[0].Expr); lit != nil {
					dateLiteral = lit
				}
			}
			
			// Check in WHERE clause
			if dateLiteral == nil && selectStmt.Where != nil {
				dateLiteral = findDateLiteral(selectStmt.Where)
			}

			if dateLiteral == nil {
				t.Logf("SelectStmt columns: %v", selectStmt.Columns)
				if selectStmt.Where != nil {
					t.Logf("WHERE clause: %T - %v", selectStmt.Where, selectStmt.Where)
				}
				t.Fatal("Could not find date literal in parsed statement")
			}

			// Verify it's a time.Time value
			timeVal, ok := dateLiteral.Value.Data.(time.Time)
			if !ok {
				t.Fatalf("Expected time.Time, got %T", dateLiteral.Value.Data)
			}

			// Check the date matches
			gotDate := timeVal.Format("2006-01-02")
			if gotDate != tt.wantDate {
				t.Errorf("Date = %v, want %v", gotDate, tt.wantDate)
			}
		})
	}
}

func TestDateComparison(t *testing.T) {
	// Test that date comparisons work correctly
	date1, err := types.ParseDate("1995-03-15")
	if err != nil {
		t.Fatalf("Failed to parse date1: %v", err)
	}

	date2, err := types.ParseDate("1995-03-16") 
	if err != nil {
		t.Fatalf("Failed to parse date2: %v", err)
	}

	// Test comparison
	cmp := types.Date.Compare(date1, date2)
	if cmp >= 0 {
		t.Errorf("Expected date1 < date2, got compare result %d", cmp)
	}

	// Test equality
	date3, err := types.ParseDate("1995-03-15")
	if err != nil {
		t.Fatalf("Failed to parse date3: %v", err)
	}

	cmp = types.Date.Compare(date1, date3)
	if cmp != 0 {
		t.Errorf("Expected date1 == date3, got compare result %d", cmp)
	}
}