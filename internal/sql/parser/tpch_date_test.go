package parser

import (
	"testing"
)

func TestTPCHDateQueries(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		wantErr bool
	}{
		{
			name: "TPC-H Q3 with date literals",
			query: `SELECT l_orderkey, o_orderdate, o_shippriority, 
				SUM(l_extendedprice * (1 - l_discount)) as revenue
			FROM customer, orders, lineitem
			WHERE c_custkey = o_custkey
			  AND o_orderkey = l_orderkey
			  AND c_mktsegment = 'BUILDING'
			  AND o_orderdate < date '1995-03-15'
			  AND l_shipdate > date '1995-03-15'
			GROUP BY l_orderkey, o_orderdate, o_shippriority
			ORDER BY revenue desc, o_orderdate
			LIMIT 10`,
		},
		{
			name: "TPC-H Q5 with date range",
			query: `SELECT n_name, SUM(l_extendedprice * (1 - l_discount)) as revenue
			FROM customer, orders, lineitem, supplier, nation, region
			WHERE c_custkey = o_custkey
			  AND l_orderkey = o_orderkey
			  AND l_suppkey = s_suppkey
			  AND c_nationkey = s_nationkey
			  AND s_nationkey = n_nationkey
			  AND n_regionkey = r_regionkey
			  AND r_name = 'ASIA'
			  AND o_orderdate >= date '1994-01-01'
			  AND o_orderdate < date '1995-01-01'
			GROUP BY n_name
			ORDER BY revenue desc`,
		},
		{
			name: "TPC-H Q8 with BETWEEN dates",
			query: `SELECT o_year, 
				SUM(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0 END) / SUM(volume) as mkt_share
			FROM (
			  SELECT EXTRACT(year FROM o_orderdate) as o_year,
				l_extendedprice * (1 - l_discount) as volume,
				n2.n_name as nation
			  FROM part, supplier, lineitem, orders, customer, nation n1, nation n2, region
			  WHERE p_partkey = l_partkey
				AND s_suppkey = l_suppkey
				AND l_orderkey = o_orderkey
				AND o_custkey = c_custkey
				AND c_nationkey = n1.n_nationkey
				AND n1.n_regionkey = r_regionkey
				AND r_name = 'AMERICA'
				AND s_nationkey = n2.n_nationkey
				AND o_orderdate between date '1995-01-01' and date '1996-12-31'
				AND p_type = 'ECONOMY ANODIZED STEEL'
			) as all_nations
			GROUP BY o_year
			ORDER BY o_year`,
		},
		{
			name:  "Simple date comparison",
			query: `SELECT * FROM orders WHERE o_orderdate >= date '2023-01-01'`,
		},
		{
			name: "Multiple date comparisons",
			query: `SELECT * FROM lineitem 
			WHERE l_shipdate >= date '1994-01-01' 
			  AND l_commitdate < date '1994-02-01'
			  AND l_receiptdate > date '1994-01-15'`,
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

func TestDateLiteralFormats(t *testing.T) {
	tests := []struct {
		name    string
		dateLit string
		wantErr bool
	}{
		{
			name:    "Standard ISO format",
			dateLit: "date '2023-12-25'",
		},
		{
			name:    "Year 2000",
			dateLit: "date '2000-01-01'",
		},
		{
			name:    "Leap year date",
			dateLit: "date '2024-02-29'",
		},
		{
			name:    "Invalid leap year",
			dateLit: "date '2023-02-29'",
			wantErr: true,
		},
		{
			name:    "Invalid month",
			dateLit: "date '2023-13-01'",
			wantErr: true,
		},
		{
			name:    "Invalid day",
			dateLit: "date '2023-01-32'",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := "SELECT " + tt.dateLit
			p := NewParser(query)
			_, err := p.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
