package executor

import (
	"strings"
	"testing"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/storage"
	"github.com/dshills/QuantaDB/internal/txn"
)

func TestForeignKeyConstraints(t *testing.T) {
	// Create storage and catalog
	st := storage.NewMemoryStorage()
	cat := catalog.NewMemoryCatalog()

	// Create planner and executor
	p := planner.NewBasicPlanner(cat)
	exec := NewBasicExecutor(cat, nil)
	storageBackend := NewStorageBackend(st, nil, nil)
	exec.SetStorageBackend(storageBackend)

	// Create execution context
	ctx := &ExecContext{
		Catalog:             cat,
		Planner:             p,
		ConstraintValidator: NewSimpleConstraintValidator(cat, storageBackend, p),
	}

	// Create parent table (customers)
	createCustomers := `CREATE TABLE customers (
		id INTEGER PRIMARY KEY,
		name VARCHAR(100) NOT NULL
	)`

	stmt, err := parser.NewParser(createCustomers).Parse()
	if err != nil {
		t.Fatalf("Failed to parse CREATE TABLE customers: %v", err)
	}

	plan, err := p.Plan(stmt)
	if err != nil {
		t.Fatalf("Failed to plan CREATE TABLE customers: %v", err)
	}

	op, err := exec.Build(plan, ctx)
	if err != nil {
		t.Fatalf("Failed to build CREATE TABLE customers operator: %v", err)
	}

	_, err = op.Next()
	if err != nil {
		t.Fatalf("Failed to execute CREATE TABLE customers: %v", err)
	}

	// Insert some customers
	insertCustomers := `INSERT INTO customers (id, name) VALUES (1, 'Alice'), (2, 'Bob')`

	stmt, err = parser.NewParser(insertCustomers).Parse()
	if err != nil {
		t.Fatalf("Failed to parse INSERT customers: %v", err)
	}

	plan, err = p.Plan(stmt)
	if err != nil {
		t.Fatalf("Failed to plan INSERT customers: %v", err)
	}

	op, err = exec.Build(plan, ctx)
	if err != nil {
		t.Fatalf("Failed to build INSERT customers operator: %v", err)
	}

	_, err = op.Next()
	if err != nil {
		t.Fatalf("Failed to execute INSERT customers: %v", err)
	}

	// Create child table (orders) with foreign key
	createOrders := `CREATE TABLE orders (
		id INTEGER PRIMARY KEY,
		customer_id INTEGER,
		amount DECIMAL(10,2),
		FOREIGN KEY (customer_id) REFERENCES customers (id)
	)`

	stmt, err = parser.NewParser(createOrders).Parse()
	if err != nil {
		t.Fatalf("Failed to parse CREATE TABLE orders: %v", err)
	}

	plan, err = p.Plan(stmt)
	if err != nil {
		t.Fatalf("Failed to plan CREATE TABLE orders: %v", err)
	}

	op, err = exec.Build(plan, ctx)
	if err != nil {
		t.Fatalf("Failed to build CREATE TABLE orders operator: %v", err)
	}

	_, err = op.Next()
	if err != nil {
		t.Fatalf("Failed to execute CREATE TABLE orders: %v", err)
	}

	// Test 1: Insert valid order (should succeed)
	insertValidOrder := `INSERT INTO orders (id, customer_id, amount) VALUES (1, 1, 100.00)`

	stmt, err = parser.NewParser(insertValidOrder).Parse()
	if err != nil {
		t.Fatalf("Failed to parse INSERT valid order: %v", err)
	}

	plan, err = p.Plan(stmt)
	if err != nil {
		t.Fatalf("Failed to plan INSERT valid order: %v", err)
	}

	op, err = exec.Build(plan, ctx)
	if err != nil {
		t.Fatalf("Failed to build INSERT valid order operator: %v", err)
	}

	_, err = op.Next()
	if err != nil {
		t.Errorf("Failed to insert valid order: %v", err)
	}

	// Test 2: Insert order with non-existent customer (should fail)
	insertInvalidOrder := `INSERT INTO orders (id, customer_id, amount) VALUES (2, 999, 200.00)`

	stmt, err = parser.NewParser(insertInvalidOrder).Parse()
	if err != nil {
		t.Fatalf("Failed to parse INSERT invalid order: %v", err)
	}

	plan, err = p.Plan(stmt)
	if err != nil {
		t.Fatalf("Failed to plan INSERT invalid order: %v", err)
	}

	op, err = exec.Build(plan, ctx)
	if err != nil {
		t.Fatalf("Failed to build INSERT invalid order operator: %v", err)
	}

	_, err = op.Next()
	if err == nil {
		t.Error("Expected foreign key violation error, but insert succeeded")
	} else if !containsString(err.Error(), "foreign key") {
		t.Errorf("Expected foreign key violation error, got: %v", err)
	}

	// Test 3: Insert order with NULL customer_id (should succeed)
	insertNullOrder := `INSERT INTO orders (id, customer_id, amount) VALUES (3, NULL, 300.00)`

	stmt, err = parser.NewParser(insertNullOrder).Parse()
	if err != nil {
		t.Fatalf("Failed to parse INSERT null order: %v", err)
	}

	plan, err = p.Plan(stmt)
	if err != nil {
		t.Fatalf("Failed to plan INSERT null order: %v", err)
	}

	op, err = exec.Build(plan, ctx)
	if err != nil {
		t.Fatalf("Failed to build INSERT null order operator: %v", err)
	}

	_, err = op.Next()
	if err != nil {
		t.Errorf("Failed to insert order with NULL customer_id: %v", err)
	}
}

func TestForeignKeyCascadeDelete(t *testing.T) {
	// Create storage and catalog
	st := storage.NewMemoryStorage()
	cat := catalog.NewMemoryCatalog()

	// Create planner and executor
	p := planner.NewBasicPlanner(cat)
	exec := NewBasicExecutor(cat, nil)
	storageBackend := NewStorageBackend(st, nil, nil)
	exec.SetStorageBackend(storageBackend)

	// Create execution context
	ctx := &ExecContext{
		Catalog:             cat,
		Planner:             p,
		ConstraintValidator: NewSimpleConstraintValidator(cat, storageBackend, p),
	}

	// Create parent table
	createDepartments := `CREATE TABLE departments (
		id INTEGER PRIMARY KEY,
		name VARCHAR(100) NOT NULL
	)`

	executeSQL(t, createDepartments, p, exec, ctx)

	// Create child table with CASCADE DELETE
	createEmployees := `CREATE TABLE employees (
		id INTEGER PRIMARY KEY,
		name VARCHAR(100) NOT NULL,
		dept_id INTEGER,
		FOREIGN KEY (dept_id) REFERENCES departments (id) ON DELETE CASCADE
	)`

	executeSQL(t, createEmployees, p, exec, ctx)

	// Insert test data
	executeSQL(t, `INSERT INTO departments (id, name) VALUES (1, 'Engineering')`, p, exec, ctx)
	executeSQL(t, `INSERT INTO employees (id, name, dept_id) VALUES (1, 'Alice', 1), (2, 'Bob', 1)`, p, exec, ctx)

	// Delete department (should cascade delete employees)
	executeSQL(t, `DELETE FROM departments WHERE id = 1`, p, exec, ctx)

	// Verify employees were deleted
	result := executeSQL(t, `SELECT COUNT(*) FROM employees WHERE dept_id = 1`, p, exec, ctx)
	row, err := result.Next()
	if err != nil {
		t.Fatalf("Failed to get count: %v", err)
	}

	count := row.Values[0].Data.(int64)
	if count != 0 {
		t.Errorf("Expected 0 employees after cascade delete, got %d", count)
	}
}

func TestCheckConstraints(t *testing.T) {
	// Create storage and catalog
	st := storage.NewMemoryStorage()
	cat := catalog.NewMemoryCatalog()

	// Create planner and executor
	p := planner.NewBasicPlanner(cat)
	exec := NewBasicExecutor(cat, nil)
	storageBackend := NewStorageBackend(st, nil, nil)
	exec.SetStorageBackend(storageBackend)

	// Create execution context
	ctx := &ExecContext{
		Catalog:             cat,
		Planner:             p,
		ConstraintValidator: NewSimpleConstraintValidator(cat, storageBackend, p),
	}

	// Create table with CHECK constraint
	createProducts := `CREATE TABLE products (
		id INTEGER PRIMARY KEY,
		name VARCHAR(100) NOT NULL,
		price DECIMAL(10,2),
		CHECK (price > 0)
	)`

	executeSQL(t, createProducts, p, exec, ctx)

	// Test 1: Insert valid product (should succeed)
	executeSQL(t, `INSERT INTO products (id, name, price) VALUES (1, 'Widget', 10.00)`, p, exec, ctx)

	// Test 2: Insert product with negative price (should fail)
	stmt, err := parser.NewParser(`INSERT INTO products (id, name, price) VALUES (2, 'Gadget', -5.00)`).Parse()
	if err != nil {
		t.Fatalf("Failed to parse INSERT: %v", err)
	}

	plan, err := p.Plan(stmt)
	if err != nil {
		t.Fatalf("Failed to plan INSERT: %v", err)
	}

	op, err := exec.Build(plan, ctx)
	if err != nil {
		t.Fatalf("Failed to build INSERT operator: %v", err)
	}

	_, err = op.Next()
	if err == nil {
		t.Error("Expected check constraint violation error, but insert succeeded")
	} else if !containsString(err.Error(), "check constraint") {
		t.Errorf("Expected check constraint violation error, got: %v", err)
	}

	// Test 3: Insert product with zero price (should fail)
	stmt, err = parser.NewParser(`INSERT INTO products (id, name, price) VALUES (3, 'Thing', 0.00)`).Parse()
	if err != nil {
		t.Fatalf("Failed to parse INSERT: %v", err)
	}

	plan, err = p.Plan(stmt)
	if err != nil {
		t.Fatalf("Failed to plan INSERT: %v", err)
	}

	op, err = exec.Build(plan, ctx)
	if err != nil {
		t.Fatalf("Failed to build INSERT operator: %v", err)
	}

	_, err = op.Next()
	if err == nil {
		t.Error("Expected check constraint violation error, but insert succeeded")
	} else if !containsString(err.Error(), "check constraint") {
		t.Errorf("Expected check constraint violation error, got: %v", err)
	}
}

// Helper function to execute SQL and return result
func executeSQL(t *testing.T, sql string, p planner.Planner, exec *BasicExecutor, ctx *ExecContext) Operator {
	stmt, err := parser.NewParser(sql).Parse()
	if err != nil {
		t.Fatalf("Failed to parse SQL: %v", err)
	}

	plan, err := p.Plan(stmt)
	if err != nil {
		t.Fatalf("Failed to plan SQL: %v", err)
	}

	op, err := exec.Build(plan, ctx)
	if err != nil {
		t.Fatalf("Failed to build operator: %v", err)
	}

	return op
}

func containsString(s, substr string) bool {
	return strings.Contains(s, substr)
}
