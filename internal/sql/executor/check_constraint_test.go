package executor

import (
	"os"
	"testing"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/storage"
)

func TestCheckConstraintsComprehensive(t *testing.T) {
	// Create storage and catalog
	cat := catalog.NewMemoryCatalog()

	// Create disk manager and buffer pool
	diskMgr, err := storage.NewDiskManager("test_check_comprehensive.db")
	if err != nil {
		t.Fatalf("Failed to create disk manager: %v", err)
	}
	defer func() {
		diskMgr.Close()
		os.Remove("test_check_comprehensive.db")
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

	// Helper function to execute SQL
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

	// Test 1: Simple comparison operators
	t.Run("ComparisonOperators", func(t *testing.T) {
		// Create table with various CHECK constraints
		if err := execSQL(`CREATE TABLE test_comparisons (
			id INTEGER PRIMARY KEY,
			age INTEGER,
			score DECIMAL(5,2),
			quantity INTEGER,
			discount DECIMAL(3,2),
			CHECK (age >= 0),
			CHECK (score >= 0.0 AND score <= 100.0),
			CHECK (quantity > 0),
			CHECK (discount < 1.0)
		)`); err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		// Valid inserts
		tests := []struct {
			sql   string
			valid bool
			desc  string
		}{
			{
				sql:   `INSERT INTO test_comparisons (id, age, score, quantity, discount) VALUES (1, 25, 85.5, 10, 0.15)`,
				valid: true,
				desc:  "all valid values",
			},
			{
				sql:   `INSERT INTO test_comparisons (id, age, score, quantity, discount) VALUES (2, 0, 0.0, 1, 0.0)`,
				valid: true,
				desc:  "boundary values (valid)",
			},
			{
				sql:   `INSERT INTO test_comparisons (id, age, score, quantity, discount) VALUES (3, -1, 50.0, 5, 0.5)`,
				valid: false,
				desc:  "negative age",
			},
			{
				sql:   `INSERT INTO test_comparisons (id, age, score, quantity, discount) VALUES (4, 30, -10.0, 5, 0.5)`,
				valid: false,
				desc:  "negative score",
			},
			{
				sql:   `INSERT INTO test_comparisons (id, age, score, quantity, discount) VALUES (5, 30, 101.0, 5, 0.5)`,
				valid: false,
				desc:  "score too high",
			},
			{
				sql:   `INSERT INTO test_comparisons (id, age, score, quantity, discount) VALUES (6, 30, 50.0, 0, 0.5)`,
				valid: false,
				desc:  "zero quantity",
			},
			{
				sql:   `INSERT INTO test_comparisons (id, age, score, quantity, discount) VALUES (7, 30, 50.0, 5, 1.0)`,
				valid: false,
				desc:  "discount too high",
			},
		}

		for _, test := range tests {
			err := execSQL(test.sql)
			if test.valid && err != nil {
				t.Errorf("Expected valid insert (%s) to succeed, but got error: %v", test.desc, err)
			} else if !test.valid && err == nil {
				t.Errorf("Expected invalid insert (%s) to fail, but it succeeded", test.desc)
			} else if !test.valid && err != nil && !containsString(err.Error(), "check constraint") {
				t.Errorf("Expected check constraint error for (%s), but got: %v", test.desc, err)
			}
		}
	})

	// Test 2: Boolean logic
	t.Run("BooleanLogic", func(t *testing.T) {
		execSQL(`DROP TABLE IF EXISTS test_comparisons`)

		if err := execSQL(`CREATE TABLE test_logic (
			id INTEGER PRIMARY KEY,
			a INTEGER,
			b INTEGER,
			c INTEGER,
			CHECK (a > 0 AND b > 0),
			CHECK (a < 100 OR b < 100),
			CHECK (NOT (c = 0))
		)`); err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		tests := []struct {
			sql   string
			valid bool
			desc  string
		}{
			{
				sql:   `INSERT INTO test_logic (id, a, b, c) VALUES (1, 10, 20, 5)`,
				valid: true,
				desc:  "all constraints satisfied",
			},
			{
				sql:   `INSERT INTO test_logic (id, a, b, c) VALUES (2, -1, 20, 5)`,
				valid: false,
				desc:  "a <= 0 violates AND constraint",
			},
			{
				sql:   `INSERT INTO test_logic (id, a, b, c) VALUES (3, 10, -1, 5)`,
				valid: false,
				desc:  "b <= 0 violates AND constraint",
			},
			{
				sql:   `INSERT INTO test_logic (id, a, b, c) VALUES (4, 100, 100, 5)`,
				valid: false,
				desc:  "both a and b >= 100 violates OR constraint",
			},
			{
				sql:   `INSERT INTO test_logic (id, a, b, c) VALUES (5, 10, 20, 0)`,
				valid: false,
				desc:  "c = 0 violates NOT constraint",
			},
		}

		for _, test := range tests {
			err := execSQL(test.sql)
			if test.valid && err != nil {
				t.Errorf("Expected valid insert (%s) to succeed, but got error: %v", test.desc, err)
			} else if !test.valid && err == nil {
				t.Errorf("Expected invalid insert (%s) to fail, but it succeeded", test.desc)
			}
		}
	})

	// Test 3: String functions
	t.Run("StringFunctions", func(t *testing.T) {
		execSQL(`DROP TABLE IF EXISTS test_logic`)

		if err := execSQL(`CREATE TABLE test_strings (
			id INTEGER PRIMARY KEY,
			name VARCHAR(50),
			email VARCHAR(100),
			code VARCHAR(10),
			status VARCHAR(20),
			CHECK (LENGTH(name) > 0),
			CHECK (LENGTH(email) >= 5),
			CHECK (UPPER(code) = code),
			CHECK (LOWER(status) = status)
		)`); err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		tests := []struct {
			sql   string
			valid bool
			desc  string
		}{
			{
				sql:   `INSERT INTO test_strings (id, name, email, code, status) VALUES (1, 'John', 'j@a.co', 'ABC123', 'active')`,
				valid: true,
				desc:  "all valid strings",
			},
			{
				sql:   `INSERT INTO test_strings (id, name, email, code, status) VALUES (2, '', 'test@example.com', 'XYZ', 'pending')`,
				valid: false,
				desc:  "empty name",
			},
			{
				sql:   `INSERT INTO test_strings (id, name, email, code, status) VALUES (3, 'Jane', 'j@co', 'DEF456', 'inactive')`,
				valid: false,
				desc:  "email too short",
			},
			{
				sql:   `INSERT INTO test_strings (id, name, email, code, status) VALUES (4, 'Bob', 'bob@test.com', 'abc123', 'active')`,
				valid: false,
				desc:  "code not uppercase",
			},
			{
				sql:   `INSERT INTO test_strings (id, name, email, code, status) VALUES (5, 'Alice', 'alice@test.com', 'GHI789', 'Active')`,
				valid: false,
				desc:  "status not lowercase",
			},
		}

		for _, test := range tests {
			err := execSQL(test.sql)
			if test.valid && err != nil {
				t.Errorf("Expected valid insert (%s) to succeed, but got error: %v", test.desc, err)
			} else if !test.valid && err == nil {
				t.Errorf("Expected invalid insert (%s) to fail, but it succeeded", test.desc)
			}
		}
	})

	// Test 4: Numeric functions
	t.Run("NumericFunctions", func(t *testing.T) {
		execSQL(`DROP TABLE IF EXISTS test_strings`)

		if err := execSQL(`CREATE TABLE test_numbers (
			id INTEGER PRIMARY KEY,
			value DECIMAL(10,2),
			percentage INTEGER,
			sqrt_val DECIMAL(10,2),
			CHECK (ABS(value) <= 1000),
			CHECK (MOD(percentage, 5) = 0),
			CHECK (SQRT(sqrt_val) <= 10)
		)`); err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		tests := []struct {
			sql   string
			valid bool
			desc  string
		}{
			{
				sql:   `INSERT INTO test_numbers (id, value, percentage, sqrt_val) VALUES (1, -500.0, 25, 100.0)`,
				valid: true,
				desc:  "all valid numbers",
			},
			{
				sql:   `INSERT INTO test_numbers (id, value, percentage, sqrt_val) VALUES (2, 500.0, 50, 25.0)`,
				valid: true,
				desc:  "positive values",
			},
			{
				sql:   `INSERT INTO test_numbers (id, value, percentage, sqrt_val) VALUES (3, -1500.0, 20, 50.0)`,
				valid: false,
				desc:  "absolute value too large",
			},
			{
				sql:   `INSERT INTO test_numbers (id, value, percentage, sqrt_val) VALUES (4, 100.0, 23, 50.0)`,
				valid: false,
				desc:  "percentage not divisible by 5",
			},
			{
				sql:   `INSERT INTO test_numbers (id, value, percentage, sqrt_val) VALUES (5, 100.0, 20, 101.0)`,
				valid: false,
				desc:  "square root > 10",
			},
		}

		for _, test := range tests {
			err := execSQL(test.sql)
			if test.valid && err != nil {
				t.Errorf("Expected valid insert (%s) to succeed, but got error: %v", test.desc, err)
			} else if !test.valid && err == nil {
				t.Errorf("Expected invalid insert (%s) to fail, but it succeeded", test.desc)
			}
		}
	})

	// Test 5: NULL handling
	t.Run("NullHandling", func(t *testing.T) {
		execSQL(`DROP TABLE IF EXISTS test_numbers`)

		if err := execSQL(`CREATE TABLE test_nulls (
			id INTEGER PRIMARY KEY,
			required INTEGER,
			optional INTEGER,
			coalesce_val INTEGER,
			CHECK (required IS NOT NULL),
			CHECK (optional IS NULL OR optional > 0),
			CHECK (COALESCE(coalesce_val, 0) >= 0)
		)`); err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		tests := []struct {
			sql   string
			valid bool
			desc  string
		}{
			{
				sql:   `INSERT INTO test_nulls (id, required, optional, coalesce_val) VALUES (1, 10, 5, 10)`,
				valid: true,
				desc:  "all non-null valid values",
			},
			{
				sql:   `INSERT INTO test_nulls (id, required, optional, coalesce_val) VALUES (2, 20, NULL, NULL)`,
				valid: true,
				desc:  "NULL optional and coalesce_val",
			},
			{
				sql:   `INSERT INTO test_nulls (id, required, optional, coalesce_val) VALUES (3, NULL, 5, 5)`,
				valid: false,
				desc:  "NULL required field",
			},
			{
				sql:   `INSERT INTO test_nulls (id, required, optional, coalesce_val) VALUES (4, 30, -5, 5)`,
				valid: false,
				desc:  "negative optional value",
			},
			{
				sql:   `INSERT INTO test_nulls (id, required, optional, coalesce_val) VALUES (5, 40, 5, -10)`,
				valid: false,
				desc:  "negative coalesce_val",
			},
		}

		for _, test := range tests {
			err := execSQL(test.sql)
			if test.valid && err != nil {
				t.Errorf("Expected valid insert (%s) to succeed, but got error: %v", test.desc, err)
			} else if !test.valid && err == nil {
				t.Errorf("Expected invalid insert (%s) to fail, but it succeeded", test.desc)
			}
		}
	})

	// Test 6: BETWEEN operator
	t.Run("BetweenOperator", func(t *testing.T) {
		execSQL(`DROP TABLE IF EXISTS test_nulls`)

		if err := execSQL(`CREATE TABLE test_between (
			id INTEGER PRIMARY KEY,
			age INTEGER,
			grade CHAR(1),
			score DECIMAL(5,2),
			CHECK (age BETWEEN 0 AND 120),
			CHECK (grade BETWEEN 'A' AND 'F'),
			CHECK (score NOT BETWEEN 0 AND 59.99)
		)`); err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		tests := []struct {
			sql   string
			valid bool
			desc  string
		}{
			{
				sql:   `INSERT INTO test_between (id, age, grade, score) VALUES (1, 25, 'B', 85.5)`,
				valid: true,
				desc:  "all valid values",
			},
			{
				sql:   `INSERT INTO test_between (id, age, grade, score) VALUES (2, 0, 'A', 60.0)`,
				valid: true,
				desc:  "boundary values",
			},
			{
				sql:   `INSERT INTO test_between (id, age, grade, score) VALUES (3, -1, 'C', 70.0)`,
				valid: false,
				desc:  "age below range",
			},
			{
				sql:   `INSERT INTO test_between (id, age, grade, score) VALUES (4, 121, 'D', 70.0)`,
				valid: false,
				desc:  "age above range",
			},
			{
				sql:   `INSERT INTO test_between (id, age, grade, score) VALUES (5, 30, 'G', 70.0)`,
				valid: false,
				desc:  "grade out of range",
			},
			{
				sql:   `INSERT INTO test_between (id, age, grade, score) VALUES (6, 30, 'C', 30.0)`,
				valid: false,
				desc:  "score in NOT BETWEEN range",
			},
		}

		for _, test := range tests {
			err := execSQL(test.sql)
			if test.valid && err != nil {
				t.Errorf("Expected valid insert (%s) to succeed, but got error: %v", test.desc, err)
			} else if !test.valid && err == nil {
				t.Errorf("Expected invalid insert (%s) to fail, but it succeeded", test.desc)
			}
		}
	})

	// Test 7: IN operator
	t.Run("InOperator", func(t *testing.T) {
		execSQL(`DROP TABLE IF EXISTS test_between`)

		if err := execSQL(`CREATE TABLE test_in (
			id INTEGER PRIMARY KEY,
			status VARCHAR(20),
			priority INTEGER,
			category VARCHAR(10),
			CHECK (status IN ('active', 'inactive', 'pending')),
			CHECK (priority IN (1, 2, 3, 4, 5)),
			CHECK (category NOT IN ('spam', 'junk', 'trash'))
		)`); err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		tests := []struct {
			sql   string
			valid bool
			desc  string
		}{
			{
				sql:   `INSERT INTO test_in (id, status, priority, category) VALUES (1, 'active', 1, 'normal')`,
				valid: true,
				desc:  "all valid values",
			},
			{
				sql:   `INSERT INTO test_in (id, status, priority, category) VALUES (2, 'pending', 5, 'important')`,
				valid: true,
				desc:  "different valid values",
			},
			{
				sql:   `INSERT INTO test_in (id, status, priority, category) VALUES (3, 'unknown', 3, 'normal')`,
				valid: false,
				desc:  "invalid status",
			},
			{
				sql:   `INSERT INTO test_in (id, status, priority, category) VALUES (4, 'active', 6, 'normal')`,
				valid: false,
				desc:  "priority out of range",
			},
			{
				sql:   `INSERT INTO test_in (id, status, priority, category) VALUES (5, 'active', 2, 'spam')`,
				valid: false,
				desc:  "category in NOT IN list",
			},
		}

		for _, test := range tests {
			err := execSQL(test.sql)
			if test.valid && err != nil {
				t.Errorf("Expected valid insert (%s) to succeed, but got error: %v", test.desc, err)
			} else if !test.valid && err == nil {
				t.Errorf("Expected invalid insert (%s) to fail, but it succeeded", test.desc)
			}
		}
	})
}
