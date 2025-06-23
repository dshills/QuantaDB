package executor

import (
	"os"
	"strings"
	"testing"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/storage"
)

func TestForeignKeyConstraints(t *testing.T) {
	// Create storage and catalog
	cat := catalog.NewMemoryCatalog()

	// Create disk manager and buffer pool for disk storage
	diskMgr, err := storage.NewDiskManager("test_foreign_key.db")
	if err != nil {
		t.Fatalf("Failed to create disk manager: %v", err)
	}
	defer func() {
		diskMgr.Close()
		os.Remove("test_foreign_key.db")
	}()
	bufferPool := storage.NewBufferPool(diskMgr, 1024) // 1024 pages

	// Create planner and executor
	p := planner.NewBasicPlannerWithCatalog(cat)
	exec := NewBasicExecutor(cat, nil)
	storageBackend := NewDiskStorageBackend(bufferPool, cat)
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

	result, err := exec.Execute(plan, ctx)
	if err != nil {
		t.Fatalf("Failed to execute CREATE TABLE customers: %v", err)
	}
	result.Close()

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

	result, err = exec.Execute(plan, ctx)
	if err != nil {
		t.Fatalf("Failed to execute INSERT customers: %v", err)
	}
	result.Close()

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

	result, err = exec.Execute(plan, ctx)
	if err != nil {
		t.Fatalf("Failed to execute CREATE TABLE orders: %v", err)
	}
	result.Close()

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

	result, err = exec.Execute(plan, ctx)
	if err != nil {
		t.Errorf("Failed to insert valid order: %v", err)
	} else {
		result.Close()
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

	result, err = exec.Execute(plan, ctx)
	if err == nil {
		result.Close()
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

	result, err = exec.Execute(plan, ctx)
	if err != nil {
		t.Errorf("Failed to insert order with NULL customer_id: %v", err)
	} else {
		result.Close()
	}
}

func TestForeignKeyCascadeDelete(t *testing.T) {
	t.Skip("CASCADE DELETE not yet implemented")
	// Create storage and catalog
	cat := catalog.NewMemoryCatalog()

	// Create disk manager and buffer pool for disk storage
	diskMgr, err := storage.NewDiskManager("test_cascade_delete.db")
	if err != nil {
		t.Fatalf("Failed to create disk manager: %v", err)
	}
	defer func() {
		diskMgr.Close()
		os.Remove("test_cascade_delete.db")
	}()
	bufferPool := storage.NewBufferPool(diskMgr, 1024) // 1024 pages

	// Create planner and executor
	p := planner.NewBasicPlannerWithCatalog(cat)
	exec := NewBasicExecutor(cat, nil)
	storageBackend := NewDiskStorageBackend(bufferPool, cat)
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

	result := executeSQL(t, createDepartments, p, exec, ctx)
	result.Close()

	// Create child table with CASCADE DELETE
	createEmployees := `CREATE TABLE employees (
		id INTEGER PRIMARY KEY,
		name VARCHAR(100) NOT NULL,
		dept_id INTEGER,
		FOREIGN KEY (dept_id) REFERENCES departments (id) ON DELETE CASCADE
	)`

	result = executeSQL(t, createEmployees, p, exec, ctx)
	result.Close()

	// Insert test data
	result = executeSQL(t, `INSERT INTO departments (id, name) VALUES (1, 'Engineering')`, p, exec, ctx)
	result.Close()
	result = executeSQL(t, `INSERT INTO employees (id, name, dept_id) VALUES (1, 'Alice', 1), (2, 'Bob', 1)`, p, exec, ctx)
	result.Close()

	// Delete department (should cascade delete employees)
	result = executeSQL(t, `DELETE FROM departments WHERE id = 1`, p, exec, ctx)
	result.Close()

	// Verify employees were deleted
	result = executeSQL(t, `SELECT COUNT(*) FROM employees WHERE dept_id = 1`, p, exec, ctx)
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
	t.Skip("CHECK constraints expression parsing not yet fully implemented")
	// Create storage and catalog
	cat := catalog.NewMemoryCatalog()

	// Create disk manager and buffer pool for disk storage
	diskMgr, err := storage.NewDiskManager("test_check_constraints.db")
	if err != nil {
		t.Fatalf("Failed to create disk manager: %v", err)
	}
	defer func() {
		diskMgr.Close()
		os.Remove("test_check_constraints.db")
	}()
	bufferPool := storage.NewBufferPool(diskMgr, 1024) // 1024 pages

	// Create planner and executor
	p := planner.NewBasicPlannerWithCatalog(cat)
	exec := NewBasicExecutor(cat, nil)
	storageBackend := NewDiskStorageBackend(bufferPool, cat)
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

	result := executeSQL(t, createProducts, p, exec, ctx)
	result.Close()

	// Test 1: Insert valid product (should succeed)
	result = executeSQL(t, `INSERT INTO products (id, name, price) VALUES (1, 'Widget', 10.00)`, p, exec, ctx)
	result.Close()

	// Test 2: Insert product with negative price (should fail)
	stmt2, err2 := parser.NewParser(`INSERT INTO products (id, name, price) VALUES (2, 'Gadget', -5.00)`).Parse()
	if err2 != nil {
		t.Fatalf("Failed to parse INSERT: %v", err2)
	}

	plan2, err2 := p.Plan(stmt2)
	if err2 != nil {
		t.Fatalf("Failed to plan INSERT: %v", err2)
	}

	result2, err2 := exec.Execute(plan2, ctx)
	if err2 == nil {
		result2.Close()
		t.Error("Expected check constraint violation error, but insert succeeded")
	} else if !containsString(err2.Error(), "check constraint") {
		t.Errorf("Expected check constraint violation error, got: %v", err2)
	}

	// Test 3: Insert product with zero price (should fail)
	stmt3, err3 := parser.NewParser(`INSERT INTO products (id, name, price) VALUES (3, 'Thing', 0.00)`).Parse()
	if err3 != nil {
		t.Fatalf("Failed to parse INSERT: %v", err3)
	}

	plan3, err3 := p.Plan(stmt3)
	if err3 != nil {
		t.Fatalf("Failed to plan INSERT: %v", err3)
	}

	result3, err3 := exec.Execute(plan3, ctx)
	if err3 == nil {
		result3.Close()
		t.Error("Expected check constraint violation error, but insert succeeded")
	} else if !containsString(err3.Error(), "check constraint") {
		t.Errorf("Expected check constraint violation error, got: %v", err3)
	}
}

// Helper function to execute SQL and return result
func executeSQL(t *testing.T, sql string, p planner.Planner, exec *BasicExecutor, ctx *ExecContext) Result {
	stmt, err := parser.NewParser(sql).Parse()
	if err != nil {
		t.Fatalf("Failed to parse SQL: %v", err)
	}

	plan, err := p.Plan(stmt)
	if err != nil {
		t.Fatalf("Failed to plan SQL: %v", err)
	}

	result, err := exec.Execute(plan, ctx)
	if err != nil {
		t.Fatalf("Failed to execute SQL: %v", err)
	}

	return result
}

func containsString(s, substr string) bool {
	return strings.Contains(s, substr)
}
