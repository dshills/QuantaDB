package executor

import (
	"fmt"
	"os"
	"testing"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/storage"
)

func TestCascadeDelete(t *testing.T) {
	// Create storage and catalog
	cat := catalog.NewMemoryCatalog()

	// Create disk manager and buffer pool
	diskMgr, err := storage.NewDiskManager("test_cascade.db")
	if err != nil {
		t.Fatalf("Failed to create disk manager: %v", err)
	}
	defer func() {
		diskMgr.Close()
		os.Remove("test_cascade.db")
	}()
	bufferPool := storage.NewBufferPool(diskMgr, 1024)

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

	// Helper function to execute SQL and check for errors
	execSQL := func(sql string) error {
		stmt, err := parser.NewParser(sql).Parse()
		if err != nil {
			return err
		}
		plan, err := p.Plan(stmt)
		if err != nil {
			return err
		}
		result, err := exec.Execute(plan, ctx)
		if err != nil {
			return err
		}
		result.Close()
		return nil
	}

	// Helper function to get count from a table
	getCount := func(tableName string) int64 {
		stmt, _ := parser.NewParser("SELECT COUNT(*) FROM " + tableName).Parse()
		plan, _ := p.Plan(stmt)
		result, _ := exec.Execute(plan, ctx)
		defer result.Close()
		row, _ := result.Next()
		return row.Values[0].Data.(int64)
	}

	// Test 1: Simple CASCADE DELETE
	t.Run("SimpleCascadeDelete", func(t *testing.T) {
		// Create parent table
		if err := execSQL(`CREATE TABLE departments (
			id INTEGER PRIMARY KEY,
			name VARCHAR(100) NOT NULL
		)`); err != nil {
			t.Fatalf("Failed to create departments table: %v", err)
		}

		// Create child table with CASCADE DELETE
		if err := execSQL(`CREATE TABLE employees (
			id INTEGER PRIMARY KEY,
			name VARCHAR(100) NOT NULL,
			dept_id INTEGER,
			FOREIGN KEY (dept_id) REFERENCES departments (id) ON DELETE CASCADE
		)`); err != nil {
			t.Fatalf("Failed to create employees table: %v", err)
		}

		// Insert test data
		if err := execSQL(`INSERT INTO departments (id, name) VALUES (1, 'Engineering'), (2, 'Sales')`); err != nil {
			t.Fatalf("Failed to insert departments: %v", err)
		}

		if err := execSQL(`INSERT INTO employees (id, name, dept_id) VALUES 
			(1, 'Alice', 1), (2, 'Bob', 1), (3, 'Charlie', 2)`); err != nil {
			t.Fatalf("Failed to insert employees: %v", err)
		}

		// Verify initial state
		if count := getCount("departments"); count != 2 {
			t.Errorf("Expected 2 departments, got %d", count)
		}
		if count := getCount("employees"); count != 3 {
			t.Errorf("Expected 3 employees, got %d", count)
		}

		// Delete Engineering department (should cascade delete Alice and Bob)
		if err := execSQL(`DELETE FROM departments WHERE id = 1`); err != nil {
			t.Fatalf("Failed to delete department: %v", err)
		}

		// Verify cascade delete worked
		if count := getCount("departments"); count != 1 {
			t.Errorf("Expected 1 department after delete, got %d", count)
		}
		if count := getCount("employees"); count != 1 {
			t.Errorf("Expected 1 employee after cascade delete, got %d", count)
		}

		// Verify only Charlie remains
		stmt, _ := parser.NewParser(`SELECT name FROM employees`).Parse()
		plan, _ := p.Plan(stmt)
		result, _ := exec.Execute(plan, ctx)
		row, _ := result.Next()
		result.Close()

		if row == nil || row.Values[0].Data.(string) != "Charlie" {
			t.Error("Expected only Charlie to remain after cascade delete")
		}
	})

	// Test 2: Multi-level CASCADE DELETE
	t.Run("MultiLevelCascadeDelete", func(t *testing.T) {
		// Clean up previous test
		execSQL(`DROP TABLE IF EXISTS employees`)
		execSQL(`DROP TABLE IF EXISTS departments`)

		// Create three-level hierarchy
		if err := execSQL(`CREATE TABLE companies (
			id INTEGER PRIMARY KEY,
			name VARCHAR(100) NOT NULL
		)`); err != nil {
			t.Fatalf("Failed to create companies table: %v", err)
		}

		if err := execSQL(`CREATE TABLE divisions (
			id INTEGER PRIMARY KEY,
			name VARCHAR(100) NOT NULL,
			company_id INTEGER,
			FOREIGN KEY (company_id) REFERENCES companies (id) ON DELETE CASCADE
		)`); err != nil {
			t.Fatalf("Failed to create divisions table: %v", err)
		}

		if err := execSQL(`CREATE TABLE teams (
			id INTEGER PRIMARY KEY,
			name VARCHAR(100) NOT NULL,
			division_id INTEGER,
			FOREIGN KEY (division_id) REFERENCES divisions (id) ON DELETE CASCADE
		)`); err != nil {
			t.Fatalf("Failed to create teams table: %v", err)
		}

		// Insert test data
		execSQL(`INSERT INTO companies (id, name) VALUES (1, 'TechCorp')`)
		execSQL(`INSERT INTO divisions (id, name, company_id) VALUES (1, 'R&D', 1), (2, 'Marketing', 1)`)
		execSQL(`INSERT INTO teams (id, name, division_id) VALUES (1, 'Backend', 1), (2, 'Frontend', 1), (3, 'Social Media', 2)`)

		// Delete the company (should cascade through all levels)
		if err := execSQL(`DELETE FROM companies WHERE id = 1`); err != nil {
			t.Fatalf("Failed to delete company: %v", err)
		}

		// Verify all related records were deleted
		if count := getCount("companies"); count != 0 {
			t.Errorf("Expected 0 companies, got %d", count)
		}
		if count := getCount("divisions"); count != 0 {
			t.Errorf("Expected 0 divisions after cascade, got %d", count)
		}
		if count := getCount("teams"); count != 0 {
			t.Errorf("Expected 0 teams after cascade, got %d", count)
		}
	})

	// Test 3: SET NULL on DELETE
	t.Run("SetNullOnDelete", func(t *testing.T) {
		// Clean up
		execSQL(`DROP TABLE IF EXISTS teams`)
		execSQL(`DROP TABLE IF EXISTS divisions`)
		execSQL(`DROP TABLE IF EXISTS companies`)

		// Create tables with SET NULL
		execSQL(`CREATE TABLE categories (
			id INTEGER PRIMARY KEY,
			name VARCHAR(100) NOT NULL
		)`)

		execSQL(`CREATE TABLE products (
			id INTEGER PRIMARY KEY,
			name VARCHAR(100) NOT NULL,
			category_id INTEGER,
			FOREIGN KEY (category_id) REFERENCES categories (id) ON DELETE SET NULL
		)`)

		// Insert test data
		execSQL(`INSERT INTO categories (id, name) VALUES (1, 'Electronics'), (2, 'Books')`)
		execSQL(`INSERT INTO products (id, name, category_id) VALUES 
			(1, 'Laptop', 1), (2, 'Phone', 1), (3, 'Novel', 2)`)

		// Delete Electronics category
		if err := execSQL(`DELETE FROM categories WHERE id = 1`); err != nil {
			t.Fatalf("Failed to delete category: %v", err)
		}

		// Debug: Check what's in the products table
		stmt, _ := parser.NewParser(`SELECT id, name, category_id FROM products ORDER BY id`).Parse()
		plan, _ := p.Plan(stmt)
		result, _ := exec.Execute(plan, ctx)
		t.Log("Products after delete:")
		for {
			row, err := result.Next()
			if err != nil || row == nil {
				break
			}
			catID := "NULL"
			if !row.Values[2].IsNull() {
				catID = fmt.Sprintf("%v", row.Values[2].Data)
			}
			t.Logf("  ID: %v, Name: %v, Category: %s", row.Values[0].Data, row.Values[1].Data, catID)
		}
		result.Close()

		// Verify products still exist but category_id is NULL
		stmt2, _ := parser.NewParser(`SELECT id, name, category_id FROM products ORDER BY id`).Parse()
		plan2, _ := p.Plan(stmt2)
		result2, _ := exec.Execute(plan2, ctx)

		// Check all products and verify based on ID
		productsFound := 0
		for {
			row, err := result2.Next()
			if err != nil || row == nil {
				break
			}
			productsFound++

			// Handle both int32 and int64
			var id int64
			switch v := row.Values[0].Data.(type) {
			case int64:
				id = v
			case int32:
				id = int64(v)
			default:
				t.Fatalf("Unexpected type for id: %T", v)
			}
			name := row.Values[1].Data.(string)

			switch id {
			case 1: // Laptop - should have NULL category_id
				if !row.Values[2].IsNull() {
					t.Errorf("Expected category_id to be NULL for Laptop (id=1), got %v", row.Values[2].Data)
				}
			case 2: // Phone - should have NULL category_id
				if !row.Values[2].IsNull() {
					t.Errorf("Expected category_id to be NULL for Phone (id=2), got %v", row.Values[2].Data)
				}
			case 3: // Novel - should still have category_id = 2
				if row.Values[2].IsNull() {
					t.Errorf("Expected category_id to be 2 for Novel (id=3), but got NULL")
				} else {
					// Handle both int32 and int64
					var catID int64
					switch v := row.Values[2].Data.(type) {
					case int64:
						catID = v
					case int32:
						catID = int64(v)
					}
					if catID != 2 {
						t.Errorf("Expected category_id to be 2 for Novel (id=3), got %v", catID)
					}
				}
			default:
				t.Errorf("Unexpected product with id=%d, name=%s", id, name)
			}
		}

		if productsFound != 3 {
			t.Errorf("Expected 3 products, found %d", productsFound)
		}

		result2.Close()
	})

	// Test 4: Mixed referential actions
	t.Run("MixedReferentialActions", func(t *testing.T) {
		// Clean up
		execSQL(`DROP TABLE IF EXISTS products`)
		execSQL(`DROP TABLE IF EXISTS categories`)

		// Create parent table
		execSQL(`CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			name VARCHAR(100) NOT NULL
		)`)

		// Create child tables with different referential actions
		execSQL(`CREATE TABLE posts (
			id INTEGER PRIMARY KEY,
			user_id INTEGER,
			content TEXT,
			FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE
		)`)

		execSQL(`CREATE TABLE comments (
			id INTEGER PRIMARY KEY,
			user_id INTEGER,
			post_id INTEGER,
			content TEXT,
			FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE SET NULL,
			FOREIGN KEY (post_id) REFERENCES posts (id) ON DELETE CASCADE
		)`)

		// Insert test data
		execSQL(`INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob')`)
		execSQL(`INSERT INTO posts (id, user_id, content) VALUES (1, 1, 'Post by Alice'), (2, 2, 'Post by Bob')`)
		execSQL(`INSERT INTO comments (id, user_id, post_id, content) VALUES 
			(1, 1, 1, 'Alice comments on her post'),
			(2, 2, 1, 'Bob comments on Alice post')`)

		// Delete Alice (user_id = 1)
		if err := execSQL(`DELETE FROM users WHERE id = 1`); err != nil {
			t.Fatalf("Failed to delete user: %v", err)
		}

		// Verify:
		// - Alice's posts should be deleted (CASCADE)
		// - Comments by Alice should have NULL user_id (SET NULL)
		// - Comments on Alice's posts should be deleted (CASCADE from posts)

		if count := getCount("posts"); count != 1 {
			t.Errorf("Expected 1 post after cascade delete, got %d", count)
		}

		if count := getCount("comments"); count != 0 {
			t.Errorf("Expected 0 comments after cascade delete, got %d", count)
		}
	})

	// Test 5: RESTRICT behavior (should block delete)
	t.Run("RestrictOnDelete", func(t *testing.T) {
		// Clean up
		execSQL(`DROP TABLE IF EXISTS comments`)
		execSQL(`DROP TABLE IF EXISTS posts`)
		execSQL(`DROP TABLE IF EXISTS users`)

		// Create tables with RESTRICT
		execSQL(`CREATE TABLE authors (
			id INTEGER PRIMARY KEY,
			name VARCHAR(100) NOT NULL
		)`)

		execSQL(`CREATE TABLE books (
			id INTEGER PRIMARY KEY,
			title VARCHAR(200) NOT NULL,
			author_id INTEGER,
			FOREIGN KEY (author_id) REFERENCES authors (id) ON DELETE RESTRICT
		)`)

		// Insert test data
		execSQL(`INSERT INTO authors (id, name) VALUES (1, 'Stephen King')`)
		execSQL(`INSERT INTO books (id, title, author_id) VALUES (1, 'The Shining', 1)`)

		// Try to delete author (should fail due to RESTRICT)
		err := execSQL(`DELETE FROM authors WHERE id = 1`)
		if err == nil {
			t.Error("Expected delete to fail due to RESTRICT, but it succeeded")
		}
		if !containsString(err.Error(), "foreign key constraint") {
			t.Errorf("Expected foreign key constraint error, got: %v", err)
		}

		// Verify author still exists
		if count := getCount("authors"); count != 1 {
			t.Errorf("Expected 1 author (delete should have been blocked), got %d", count)
		}
	})
}
