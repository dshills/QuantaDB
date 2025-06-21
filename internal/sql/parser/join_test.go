package parser

import (
	"testing"
)

func TestJoinParsing(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "simple inner join",
			sql:     "SELECT * FROM users JOIN orders ON users.id = orders.user_id",
			wantErr: false,
		},
		{
			name:    "inner join with INNER keyword",
			sql:     "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id",
			wantErr: false,
		},
		{
			name:    "left join",
			sql:     "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id",
			wantErr: false,
		},
		{
			name:    "left outer join",
			sql:     "SELECT * FROM users LEFT OUTER JOIN orders ON users.id = orders.user_id",
			wantErr: false,
		},
		{
			name:    "right join",
			sql:     "SELECT * FROM users RIGHT JOIN orders ON users.id = orders.user_id",
			wantErr: false,
		},
		{
			name:    "full join",
			sql:     "SELECT * FROM users FULL JOIN orders ON users.id = orders.user_id",
			wantErr: false,
		},
		{
			name:    "cross join",
			sql:     "SELECT * FROM users CROSS JOIN orders",
			wantErr: false,
		},
		{
			name:    "join with table aliases",
			sql:     "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id",
			wantErr: false,
		},
		{
			name:    "join with AS alias",
			sql:     "SELECT * FROM users AS u JOIN orders AS o ON u.id = o.user_id",
			wantErr: false,
		},
		{
			name:    "multiple joins",
			sql:     "SELECT * FROM users u JOIN orders o ON u.id = o.user_id JOIN items i ON o.id = i.order_id",
			wantErr: false,
		},
		{
			name:    "join with WHERE clause",
			sql:     "SELECT * FROM users u JOIN orders o ON u.id = o.user_id WHERE u.active = true",
			wantErr: false,
		},
		{
			name:    "join with complex ON condition",
			sql:     "SELECT * FROM users u JOIN orders o ON u.id = o.user_id AND o.status = 'active'",
			wantErr: false,
		},
		{
			name:    "missing ON clause for inner join",
			sql:     "SELECT * FROM users JOIN orders",
			wantErr: true,
		},
		{
			name:    "cross join with ON clause should work",
			sql:     "SELECT * FROM users CROSS JOIN orders ON users.id = orders.user_id",
			wantErr: false,
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
			if err == nil && stmt == nil {
				t.Error("Parse() returned nil statement without error")
			}
			
			// If parsing succeeded, verify it's a SELECT statement with proper FROM clause
			if err == nil {
				selectStmt, ok := stmt.(*SelectStmt)
				if !ok {
					t.Errorf("expected SelectStmt, got %T", stmt)
					return
				}
				
				if selectStmt.From == nil {
					t.Error("expected FROM clause to be non-nil")
				}
			}
		})
	}
}

func TestJoinTypes(t *testing.T) {
	// Test that JoinType String() methods work correctly
	tests := []struct {
		joinType JoinType
		expected string
	}{
		{InnerJoin, "INNER JOIN"},
		{LeftJoin, "LEFT JOIN"},
		{RightJoin, "RIGHT JOIN"},
		{FullJoin, "FULL JOIN"},
		{CrossJoin, "CROSS JOIN"},
	}

	for _, tt := range tests {
		if got := tt.joinType.String(); got != tt.expected {
			t.Errorf("JoinType.String() = %v, want %v", got, tt.expected)
		}
	}
}

func TestTableRefParsing(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantName string
		wantAlias string
	}{
		{
			name:     "simple table",
			sql:      "SELECT * FROM users",
			wantName: "users",
			wantAlias: "",
		},
		{
			name:     "table with implicit alias",
			sql:      "SELECT * FROM users u",
			wantName: "users",
			wantAlias: "u",
		},
		{
			name:     "table with AS alias",
			sql:      "SELECT * FROM users AS u",
			wantName: "users",
			wantAlias: "u",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.sql)
			stmt, err := p.Parse()
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}

			selectStmt := stmt.(*SelectStmt)
			tableRef, ok := selectStmt.From.(*TableRef)
			if !ok {
				t.Fatalf("expected TableRef, got %T", selectStmt.From)
			}

			if tableRef.TableName != tt.wantName {
				t.Errorf("TableName = %v, want %v", tableRef.TableName, tt.wantName)
			}
			if tableRef.Alias != tt.wantAlias {
				t.Errorf("Alias = %v, want %v", tableRef.Alias, tt.wantAlias)
			}
		})
	}
}