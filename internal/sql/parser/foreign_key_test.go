package parser

import (
	"testing"
)

func TestParseForeignKey(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantErr  bool
		validate func(*testing.T, *CreateTableStmt)
	}{
		{
			name: "simple foreign key",
			input: `CREATE TABLE orders (
				id INTEGER PRIMARY KEY,
				customer_id INTEGER,
				FOREIGN KEY (customer_id) REFERENCES customers (id)
			)`,
			validate: func(t *testing.T, stmt *CreateTableStmt) {
				// Check columns
				if len(stmt.Columns) != 2 {
					t.Fatalf("expected 2 columns, got %d", len(stmt.Columns))
				}
				
				// Check that id column has PRIMARY KEY constraint
				if len(stmt.Columns[0].Constraints) != 1 {
					t.Fatalf("expected 1 constraint on id column, got %d", len(stmt.Columns[0].Constraints))
				}
				if _, ok := stmt.Columns[0].Constraints[0].(PrimaryKeyConstraint); !ok {
					t.Errorf("expected PRIMARY KEY constraint on id column")
				}
				
				// Check table constraints
				if len(stmt.Constraints) != 1 {
					t.Fatalf("expected 1 table constraint, got %d", len(stmt.Constraints))
				}
				
				// Check foreign key
				fk, ok := stmt.Constraints[0].(TableForeignKeyConstraint)
				if !ok {
					t.Fatalf("expected first constraint to be FOREIGN KEY, got %T", stmt.Constraints[0])
				}
				if len(fk.Columns) != 1 || fk.Columns[0] != "customer_id" {
					t.Errorf("expected FOREIGN KEY on (customer_id), got %v", fk.Columns)
				}
				if fk.RefTable != "customers" {
					t.Errorf("expected reference to customers table, got %s", fk.RefTable)
				}
				if len(fk.RefColumns) != 1 || fk.RefColumns[0] != "id" {
					t.Errorf("expected reference to (id), got %v", fk.RefColumns)
				}
			},
		},
		{
			name: "foreign key with ON DELETE CASCADE",
			input: `CREATE TABLE orders (
				id INTEGER PRIMARY KEY,
				customer_id INTEGER,
				FOREIGN KEY (customer_id) REFERENCES customers (id) ON DELETE CASCADE
			)`,
			validate: func(t *testing.T, stmt *CreateTableStmt) {
				if len(stmt.Constraints) != 1 {
					t.Fatalf("expected 1 constraint, got %d", len(stmt.Constraints))
				}
				
				fk, ok := stmt.Constraints[0].(TableForeignKeyConstraint)
				if !ok {
					t.Fatalf("expected first constraint to be FOREIGN KEY, got %T", stmt.Constraints[0])
				}
				if fk.OnDelete != "CASCADE" {
					t.Errorf("expected ON DELETE CASCADE, got %s", fk.OnDelete)
				}
			},
		},
		{
			name: "foreign key with ON UPDATE RESTRICT",
			input: `CREATE TABLE orders (
				id INTEGER PRIMARY KEY,
				customer_id INTEGER,
				FOREIGN KEY (customer_id) REFERENCES customers (id) ON UPDATE RESTRICT
			)`,
			validate: func(t *testing.T, stmt *CreateTableStmt) {
				if len(stmt.Constraints) != 1 {
					t.Fatalf("expected 1 constraint, got %d", len(stmt.Constraints))
				}
				
				fk, ok := stmt.Constraints[0].(TableForeignKeyConstraint)
				if !ok {
					t.Fatalf("expected first constraint to be FOREIGN KEY, got %T", stmt.Constraints[0])
				}
				if fk.OnUpdate != "RESTRICT" {
					t.Errorf("expected ON UPDATE RESTRICT, got %s", fk.OnUpdate)
				}
			},
		},
		{
			name: "foreign key with both ON DELETE and ON UPDATE",
			input: `CREATE TABLE orders (
				id INTEGER PRIMARY KEY,
				customer_id INTEGER,
				FOREIGN KEY (customer_id) REFERENCES customers (id) ON DELETE CASCADE ON UPDATE RESTRICT
			)`,
			validate: func(t *testing.T, stmt *CreateTableStmt) {
				if len(stmt.Constraints) != 1 {
					t.Fatalf("expected 1 constraint, got %d", len(stmt.Constraints))
				}
				
				fk, ok := stmt.Constraints[0].(TableForeignKeyConstraint)
				if !ok {
					t.Fatalf("expected first constraint to be FOREIGN KEY, got %T", stmt.Constraints[0])
				}
				if fk.OnDelete != "CASCADE" {
					t.Errorf("expected ON DELETE CASCADE, got %s", fk.OnDelete)
				}
				if fk.OnUpdate != "RESTRICT" {
					t.Errorf("expected ON UPDATE RESTRICT, got %s", fk.OnUpdate)
				}
			},
		},
		{
			name: "named foreign key constraint",
			input: `CREATE TABLE orders (
				id INTEGER PRIMARY KEY,
				customer_id INTEGER,
				CONSTRAINT fk_customer FOREIGN KEY (customer_id) REFERENCES customers (id)
			)`,
			validate: func(t *testing.T, stmt *CreateTableStmt) {
				if len(stmt.Constraints) != 1 {
					t.Fatalf("expected 1 constraint, got %d", len(stmt.Constraints))
				}
				
				fk, ok := stmt.Constraints[0].(TableForeignKeyConstraint)
				if !ok {
					t.Fatalf("expected first constraint to be FOREIGN KEY, got %T", stmt.Constraints[0])
				}
				if fk.Name != "fk_customer" {
					t.Errorf("expected constraint name 'fk_customer', got %s", fk.Name)
				}
			},
		},
		{
			name: "composite foreign key",
			input: `CREATE TABLE order_items (
				order_id INTEGER,
				product_id INTEGER,
				PRIMARY KEY (order_id, product_id),
				FOREIGN KEY (order_id, product_id) REFERENCES products (id, variant_id)
			)`,
			validate: func(t *testing.T, stmt *CreateTableStmt) {
				if len(stmt.Constraints) != 2 {
					t.Fatalf("expected 2 constraints, got %d", len(stmt.Constraints))
				}
				
				fk, ok := stmt.Constraints[1].(TableForeignKeyConstraint)
				if !ok {
					t.Fatalf("expected second constraint to be FOREIGN KEY, got %T", stmt.Constraints[1])
				}
				if len(fk.Columns) != 2 || fk.Columns[0] != "order_id" || fk.Columns[1] != "product_id" {
					t.Errorf("expected FOREIGN KEY on (order_id, product_id), got %v", fk.Columns)
				}
				if len(fk.RefColumns) != 2 || fk.RefColumns[0] != "id" || fk.RefColumns[1] != "variant_id" {
					t.Errorf("expected reference to (id, variant_id), got %v", fk.RefColumns)
				}
			},
		},
		{
			name: "foreign key without column specification",
			input: `CREATE TABLE orders (
				id INTEGER PRIMARY KEY,
				customer_id INTEGER,
				FOREIGN KEY (customer_id) REFERENCES customers
			)`,
			validate: func(t *testing.T, stmt *CreateTableStmt) {
				if len(stmt.Constraints) != 1 {
					t.Fatalf("expected 1 constraint, got %d", len(stmt.Constraints))
				}
				
				fk, ok := stmt.Constraints[0].(TableForeignKeyConstraint)
				if !ok {
					t.Fatalf("expected first constraint to be FOREIGN KEY, got %T", stmt.Constraints[0])
				}
				if len(fk.RefColumns) != 0 {
					t.Errorf("expected no reference columns, got %v", fk.RefColumns)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.input)
			stmt, err := p.Parse()
			
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			
			createStmt, ok := stmt.(*CreateTableStmt)
			if !ok {
				t.Fatalf("expected *CreateTableStmt, got %T", stmt)
			}
			
			if tt.validate != nil {
				tt.validate(t, createStmt)
			}
		})
	}
}

func TestParseCheckConstraint(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantErr  bool
		validate func(*testing.T, *CreateTableStmt)
	}{
		{
			name: "simple CHECK constraint",
			input: `CREATE TABLE products (
				id INTEGER PRIMARY KEY,
				price DECIMAL,
				CHECK (price > 0)
			)`,
			validate: func(t *testing.T, stmt *CreateTableStmt) {
				if len(stmt.Constraints) != 1 {
					t.Fatalf("expected 1 constraint, got %d", len(stmt.Constraints))
				}
				
				check, ok := stmt.Constraints[0].(TableCheckConstraint)
				if !ok {
					t.Fatalf("expected first constraint to be CHECK, got %T", stmt.Constraints[0])
				}
				if check.Name != "" {
					t.Errorf("expected unnamed CHECK constraint, got name %s", check.Name)
				}
				if check.Expression == nil {
					t.Error("expected CHECK expression to be non-nil")
				}
			},
		},
		{
			name: "named CHECK constraint",
			input: `CREATE TABLE products (
				id INTEGER PRIMARY KEY,
				price DECIMAL,
				CONSTRAINT positive_price CHECK (price > 0)
			)`,
			validate: func(t *testing.T, stmt *CreateTableStmt) {
				if len(stmt.Constraints) != 1 {
					t.Fatalf("expected 1 constraint, got %d", len(stmt.Constraints))
				}
				
				check, ok := stmt.Constraints[0].(TableCheckConstraint)
				if !ok {
					t.Fatalf("expected first constraint to be CHECK, got %T", stmt.Constraints[0])
				}
				if check.Name != "positive_price" {
					t.Errorf("expected constraint name 'positive_price', got %s", check.Name)
				}
			},
		},
		{
			name: "CHECK with complex expression",
			input: `CREATE TABLE orders (
				id INTEGER PRIMARY KEY,
				quantity INTEGER,
				unit_price DECIMAL,
				total_price DECIMAL,
				CHECK (total_price = quantity * unit_price)
			)`,
			validate: func(t *testing.T, stmt *CreateTableStmt) {
				if len(stmt.Constraints) != 1 {
					t.Fatalf("expected 1 constraint, got %d", len(stmt.Constraints))
				}
				
				check, ok := stmt.Constraints[0].(TableCheckConstraint)
				if !ok {
					t.Fatalf("expected first constraint to be CHECK, got %T", stmt.Constraints[0])
				}
				if check.Expression == nil {
					t.Error("expected CHECK expression to be non-nil")
				}
			},
		},
		{
			name: "multiple CHECK constraints",
			input: `CREATE TABLE products (
				id INTEGER PRIMARY KEY,
				price DECIMAL,
				discount DECIMAL,
				CHECK (price > 0),
				CHECK (discount >= 0 AND discount <= 100)
			)`,
			validate: func(t *testing.T, stmt *CreateTableStmt) {
				if len(stmt.Constraints) != 2 {
					t.Fatalf("expected 2 constraints, got %d", len(stmt.Constraints))
				}
				
				// Both should be CHECK constraints
				for i := 0; i < 2; i++ {
					_, ok := stmt.Constraints[i].(TableCheckConstraint)
					if !ok {
						t.Errorf("expected constraint %d to be CHECK, got %T", i, stmt.Constraints[i])
					}
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.input)
			stmt, err := p.Parse()
			
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			
			createStmt, ok := stmt.(*CreateTableStmt)
			if !ok {
				t.Fatalf("expected *CreateTableStmt, got %T", stmt)
			}
			
			if tt.validate != nil {
				tt.validate(t, createStmt)
			}
		})
	}
}

func TestForeignKeyString(t *testing.T) {
	tests := []struct {
		name     string
		fk       TableForeignKeyConstraint
		expected string
	}{
		{
			name: "simple foreign key",
			fk: TableForeignKeyConstraint{
				Columns:    []string{"customer_id"},
				RefTable:   "customers",
				RefColumns: []string{"id"},
			},
			expected: "FOREIGN KEY (customer_id) REFERENCES customers (id)",
		},
		{
			name: "named foreign key",
			fk: TableForeignKeyConstraint{
				Name:       "fk_customer",
				Columns:    []string{"customer_id"},
				RefTable:   "customers",
				RefColumns: []string{"id"},
			},
			expected: "CONSTRAINT fk_customer FOREIGN KEY (customer_id) REFERENCES customers (id)",
		},
		{
			name: "foreign key with CASCADE",
			fk: TableForeignKeyConstraint{
				Columns:    []string{"customer_id"},
				RefTable:   "customers",
				RefColumns: []string{"id"},
				OnDelete:   "CASCADE",
				OnUpdate:   "RESTRICT",
			},
			expected: "FOREIGN KEY (customer_id) REFERENCES customers (id) ON DELETE CASCADE ON UPDATE RESTRICT",
		},
		{
			name: "composite foreign key",
			fk: TableForeignKeyConstraint{
				Columns:    []string{"order_id", "product_id"},
				RefTable:   "products",
				RefColumns: []string{"id", "variant_id"},
			},
			expected: "FOREIGN KEY (order_id, product_id) REFERENCES products (id, variant_id)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.fk.String()
			if got != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, got)
			}
		})
	}
}