package parser

import (
	"testing"
)

func TestTPCHCompatibleFeatures(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		wantErr bool
	}{
		{
			name:    "aggregate with GROUP BY",
			query:   "SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) as revenue FROM lineitem GROUP BY l_orderkey ORDER BY revenue DESC",
			wantErr: false,
		},
		{
			name:    "COUNT with DISTINCT",
			query:   "SELECT COUNT(DISTINCT c_custkey) FROM customer WHERE c_acctbal > 0",
			wantErr: false,
		},
		{
			name:    "date comparison",
			query:   "SELECT * FROM orders WHERE o_orderdate < date '1995-03-15'",
			wantErr: false,
		},
		{
			name:    "EXTRACT function usage",
			query:   "SELECT EXTRACT(YEAR FROM o_orderdate) as order_year, COUNT(*) FROM orders GROUP BY EXTRACT(YEAR FROM o_orderdate)",
			wantErr: false,
		},
		{
			name:    "CASE in aggregate",
			query:   "SELECT SUM(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0 END) as brazil_volume FROM sales",
			wantErr: false,
		},
		{
			name:    "complex arithmetic in SELECT",
			query:   "SELECT l_extendedprice * (1 - l_discount) * (1 + l_tax) as charge FROM lineitem",
			wantErr: false,
		},
		{
			name:    "HAVING with aggregate",
			query:   "SELECT c_custkey, COUNT(*) as order_count FROM orders GROUP BY c_custkey HAVING COUNT(*) > 10",
			wantErr: false,
		},
		{
			name:    "nested CASE expression",
			query:   "SELECT CASE WHEN price > 100 THEN CASE WHEN discount > 0.1 THEN 'high_discount' ELSE 'low_discount' END ELSE 'cheap' END FROM products",
			wantErr: false,
		},
		{
			name:    "multiple aggregates",
			query:   "SELECT COUNT(*), SUM(amount), AVG(amount), MIN(amount), MAX(amount) FROM orders",
			wantErr: false,
		},
		{
			name:    "ORDER BY with multiple columns",
			query:   "SELECT * FROM lineitem ORDER BY l_shipdate ASC, l_orderkey DESC",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.query)
			stmt, err := p.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err == nil && stmt == nil {
				t.Error("Parse() returned nil statement without error")
			}
		})
	}
}

func TestTPCHExpressionParsing(t *testing.T) {
	// Test specific TPC-H style expressions
	tests := []struct {
		name    string
		expr    string
		context string
	}{
		{
			name:    "discount calculation",
			expr:    "l_extendedprice * (1 - l_discount)",
			context: "SELECT %s FROM lineitem",
		},
		{
			name:    "tax calculation",
			expr:    "l_extendedprice * (1 - l_discount) * (1 + l_tax)",
			context: "SELECT %s FROM lineitem",
		},
		{
			name:    "date range check",
			expr:    "o_orderdate >= date '1994-01-01' AND o_orderdate < date '1995-01-01'",
			context: "SELECT * FROM orders WHERE %s",
		},
		{
			name:    "year extraction in GROUP BY",
			expr:    "EXTRACT(YEAR FROM o_orderdate)",
			context: "SELECT %s, COUNT(*) FROM orders GROUP BY %s",
		},
		{
			name:    "conditional aggregation",
			expr:    "SUM(CASE WHEN p_type LIKE 'PROMO%%' THEN l_extendedprice * (1 - l_discount) ELSE 0 END)",
			context: "SELECT %s FROM lineitem",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Format the expression into a complete query
			var query string
			if tt.context == "SELECT %s, COUNT(*) FROM orders GROUP BY %s" {
				query = "SELECT " + tt.expr + ", COUNT(*) FROM orders GROUP BY " + tt.expr
			} else {
				query = tt.context
				query = replaceFirst(query, "%s", tt.expr)
			}

			p := NewParser(query)
			stmt, err := p.Parse()
			if err != nil {
				t.Errorf("Failed to parse %s: %v", tt.name, err)
				return
			}
			if stmt == nil {
				t.Errorf("Parse() returned nil statement for %s", tt.name)
			}
		})
	}
}

func replaceFirst(s, old, new string) string {
	// Simple helper to replace first occurrence
	for i := 0; i < len(s)-len(old)+1; i++ {
		if s[i:i+len(old)] == old {
			return s[:i] + new + s[i+len(old):]
		}
	}
	return s
}
