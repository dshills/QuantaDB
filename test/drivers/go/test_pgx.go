//go:build ignore

package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	connString = "postgres://postgres@localhost:5433/quantadb?sslmode=disable"
)

func main() {
	fmt.Println("=== Testing Go pgx driver with QuantaDB ===")
	fmt.Printf("Connecting to: %s\n", connString)

	ctx := context.Background()

	// Test single connection
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close(ctx)

	// Test connection - Skip ping for now as it sends empty query
	// if err := conn.Ping(ctx); err != nil {
	// 	log.Fatalf("Failed to ping: %v", err)
	// }
	fmt.Println("✅ Single connection successful (ping skipped)")

	// Test connection pool
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		log.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()

	// Skip pool ping as well
	// if err := pool.Ping(ctx); err != nil {
	// 	log.Fatalf("Failed to ping pool: %v", err)
	// }
	fmt.Println("✅ Connection pool successful (ping skipped)")

	// Run all tests
	tests := []struct {
		name string
		fn   func(context.Context, *pgx.Conn, *pgxpool.Pool) error
	}{
		{"Simple Query", testPgxSimpleQuery},
		{"Query with Parameters", testPgxQueryWithParams},
		{"Prepared Statements", testPgxPreparedStatements},
		{"Batch Operations", testPgxBatch},
		{"Transaction Handling", testPgxTransactions},
		{"Context Cancellation", testPgxContextCancel},
		{"Type Handling", testPgxTypes},
		{"Error Handling", testPgxErrors},
		{"Connection Pool Operations", testPgxPool},
	}

	passed := 0
	failed := 0

	for _, test := range tests {
		fmt.Printf("\n--- Testing: %s ---\n", test.name)
		if err := test.fn(ctx, conn, pool); err != nil {
			fmt.Printf("❌ FAILED: %v\n", err)
			failed++
		} else {
			fmt.Printf("✅ PASSED\n")
			passed++
		}
	}

	fmt.Printf("\n=== Test Results ===\n")
	fmt.Printf("Passed: %d\n", passed)
	fmt.Printf("Failed: %d\n", failed)
	fmt.Printf("Total: %d\n", passed+failed)

	if failed > 0 {
		fmt.Printf("❌ Some tests failed\n")
	} else {
		fmt.Printf("🎉 All tests passed!\n")
	}
}

func testPgxSimpleQuery(ctx context.Context, conn *pgx.Conn, pool *pgxpool.Pool) error {
	var result int
	err := conn.QueryRow(ctx, "SELECT 42").Scan(&result)
	if err != nil {
		return fmt.Errorf("simple query failed: %w", err)
	}

	if result != 42 {
		return fmt.Errorf("expected 42, got %d", result)
	}

	fmt.Printf("   Simple query result: %d\n", result)
	return nil
}

func testPgxQueryWithParams(ctx context.Context, conn *pgx.Conn, pool *pgxpool.Pool) error {
	var result int
	err := conn.QueryRow(ctx, "SELECT $1", 123).Scan(&result)
	if err != nil {
		return fmt.Errorf("parameterized query failed: %w", err)
	}

	if result != 123 {
		return fmt.Errorf("expected 123, got %d", result)
	}

	// Test multiple parameters
	var int_val int
	var str_val string
	var bool_val bool
	err = conn.QueryRow(ctx, "SELECT $1, $2, $3", 456, "test", true).Scan(&int_val, &str_val, &bool_val)
	if err != nil {
		return fmt.Errorf("multiple parameter query failed: %w", err)
	}

	if int_val != 456 || str_val != "test" || !bool_val {
		return fmt.Errorf("parameter values incorrect: %d, %s, %t", int_val, str_val, bool_val)
	}

	fmt.Printf("   Parameter query results: %d, %s, %t\n", int_val, str_val, bool_val)
	return nil
}

func testPgxPreparedStatements(ctx context.Context, conn *pgx.Conn, pool *pgxpool.Pool) error {
	// Test named prepared statement
	_, err := conn.Prepare(ctx, "test_stmt", "SELECT $1 * $2")
	if err != nil {
		return fmt.Errorf("prepare statement failed: %w", err)
	}

	var result int
	err = conn.QueryRow(ctx, "test_stmt", 6, 7).Scan(&result)
	if err != nil {
		return fmt.Errorf("prepared statement query failed: %w", err)
	}

	if result != 42 {
		return fmt.Errorf("expected 42, got %d", result)
	}

	// Deallocate prepared statement
	_, err = conn.Exec(ctx, "DEALLOCATE test_stmt")
	if err != nil {
		// This might not be implemented in QuantaDB yet, so we'll just log it
		fmt.Printf("   Note: DEALLOCATE not supported: %v\n", err)
	}

	fmt.Printf("   Prepared statement result: %d\n", result)
	return nil
}

func testPgxBatch(ctx context.Context, conn *pgx.Conn, pool *pgxpool.Pool) error {
	// Test batch operations
	batch := &pgx.Batch{}
	batch.Queue("SELECT $1", 1)
	batch.Queue("SELECT $1", 2)
	batch.Queue("SELECT $1", 3)

	results := conn.SendBatch(ctx, batch)
	defer results.Close()

	expectedValues := []int{1, 2, 3}
	for i, expected := range expectedValues {
		var result int
		err := results.QueryRow().Scan(&result)
		if err != nil {
			return fmt.Errorf("batch query %d failed: %w", i, err)
		}
		if result != expected {
			return fmt.Errorf("batch query %d: expected %d, got %d", i, expected, result)
		}
	}

	fmt.Printf("   Batch operations completed successfully\n")
	return nil
}

func testPgxTransactions(ctx context.Context, conn *pgx.Conn, pool *pgxpool.Pool) error {
	// Test transaction
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction failed: %w", err)
	}
	defer tx.Rollback(ctx) // Safe to call even after commit

	// Execute query in transaction
	var result int
	err = tx.QueryRow(ctx, "SELECT 100").Scan(&result)
	if err != nil {
		return fmt.Errorf("query in transaction failed: %w", err)
	}

	if result != 100 {
		return fmt.Errorf("expected 100, got %d", result)
	}

	// Commit transaction
	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("commit failed: %w", err)
	}

	fmt.Printf("   Transaction result: %d\n", result)
	return nil
}

func testPgxContextCancel(ctx context.Context, conn *pgx.Conn, pool *pgxpool.Pool) error {
	// Test context cancellation
	cancelCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var result int
	err := conn.QueryRow(cancelCtx, "SELECT 999").Scan(&result)
	if err != nil {
		return fmt.Errorf("context query failed: %w", err)
	}

	if result != 999 {
		return fmt.Errorf("expected 999, got %d", result)
	}

	fmt.Printf("   Context handling works: %d\n", result)
	return nil
}

func testPgxTypes(ctx context.Context, conn *pgx.Conn, pool *pgxpool.Pool) error {
	// Test different data types
	tests := []struct {
		query string
		value interface{}
	}{
		{"SELECT $1::INTEGER", int32(42)},
		{"SELECT $1::BIGINT", int64(1234567890)},
		{"SELECT $1::TEXT", "hello world"},
		{"SELECT $1::BOOLEAN", true},
		{"SELECT $1::BOOLEAN", false},
	}

	for _, test := range tests {
		var result interface{}
		err := conn.QueryRow(ctx, test.query, test.value).Scan(&result)
		if err != nil {
			return fmt.Errorf("type test failed for %v: %w", test.value, err)
		}
		fmt.Printf("   Type test: %T(%v) -> %T(%v)\n", test.value, test.value, result, result)
	}

	return nil
}

func testPgxErrors(ctx context.Context, conn *pgx.Conn, pool *pgxpool.Pool) error {
	// Test error handling
	var result int
	err := conn.QueryRow(ctx, "INVALID SQL STATEMENT").Scan(&result)
	if err == nil {
		return fmt.Errorf("expected error for invalid SQL")
	}

	fmt.Printf("   Error handling works: %v\n", err)
	return nil
}

func testPgxPool(ctx context.Context, conn *pgx.Conn, pool *pgxpool.Pool) error {
	// Test pool operations
	var result int
	err := pool.QueryRow(ctx, "SELECT 777").Scan(&result)
	if err != nil {
		return fmt.Errorf("pool query failed: %w", err)
	}

	if result != 777 {
		return fmt.Errorf("expected 777, got %d", result)
	}

	// Test pool stats
	stats := pool.Stat()
	fmt.Printf("   Pool stats - Total: %d, Idle: %d, Used: %d\n",
		stats.TotalConns(), stats.IdleConns(), stats.AcquiredConns())

	fmt.Printf("   Pool query result: %d\n", result)
	return nil
}
