package test

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/lib/pq" // PostgreSQL driver
	"github.com/stretchr/testify/require"
)

func TestExistsCorrelation(t *testing.T) {
	// Connect to QuantaDB
	db, err := sql.Open("postgres", "host=localhost port=5432 user=test password=test dbname=test sslmode=disable")
	require.NoError(t, err)
	defer db.Close()

	// Ensure the database is accessible
	err = db.Ping()
	require.NoError(t, err)

	// Clean up any existing tables
	db.Exec("DROP TABLE IF EXISTS test_orders")
	db.Exec("DROP TABLE IF EXISTS test_users")

	// Create test tables
	_, err = db.Exec(`
		CREATE TABLE test_users (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL
		)
	`)
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE test_orders (
			id INTEGER PRIMARY KEY,
			user_id INTEGER NOT NULL,
			amount INTEGER NOT NULL
		)
	`)
	require.NoError(t, err)

	// Insert test data
	// Users: 1 has orders, 2 has orders, 3 has no orders
	_, err = db.Exec(`
		INSERT INTO test_users (id, name) VALUES 
		(1, 'Alice'),
		(2, 'Bob'),
		(3, 'Charlie')
	`)
	require.NoError(t, err)

	_, err = db.Exec(`
		INSERT INTO test_orders (id, user_id, amount) VALUES 
		(1, 1, 100),
		(2, 1, 200),
		(3, 2, 300)
	`)
	require.NoError(t, err)

	// Test EXISTS with correlation
	t.Run("EXISTS with correlation", func(t *testing.T) {
		rows, err := db.Query(`
			SELECT u.id, u.name, 
			       EXISTS (SELECT 1 FROM test_orders o WHERE o.user_id = u.id) as has_orders
			FROM test_users u
			ORDER BY u.id
		`)
		require.NoError(t, err)
		defer rows.Close()

		type result struct {
			id         int
			name       string
			has_orders bool
		}

		var results []result
		for rows.Next() {
			var r result
			err := rows.Scan(&r.id, &r.name, &r.has_orders)
			require.NoError(t, err)
			results = append(results, r)
		}

		require.Len(t, results, 3)
		
		// Alice (id=1) has orders
		require.Equal(t, 1, results[0].id)
		require.Equal(t, "Alice", results[0].name)
		require.True(t, results[0].has_orders, "Alice should have orders")
		
		// Bob (id=2) has orders
		require.Equal(t, 2, results[1].id)
		require.Equal(t, "Bob", results[1].name)
		require.True(t, results[1].has_orders, "Bob should have orders")
		
		// Charlie (id=3) has NO orders
		require.Equal(t, 3, results[2].id)
		require.Equal(t, "Charlie", results[2].name)
		require.False(t, results[2].has_orders, "Charlie should NOT have orders")
	})

	// Test NOT EXISTS with correlation
	t.Run("NOT EXISTS with correlation", func(t *testing.T) {
		rows, err := db.Query(`
			SELECT u.id, u.name, 
			       NOT EXISTS (SELECT 1 FROM test_orders o WHERE o.user_id = u.id) as no_orders
			FROM test_users u
			ORDER BY u.id
		`)
		require.NoError(t, err)
		defer rows.Close()

		type result struct {
			id        int
			name      string
			no_orders bool
		}

		var results []result
		for rows.Next() {
			var r result
			err := rows.Scan(&r.id, &r.name, &r.no_orders)
			require.NoError(t, err)
			results = append(results, r)
		}

		require.Len(t, results, 3)
		
		// Alice and Bob have orders, so no_orders should be false
		require.False(t, results[0].no_orders, "Alice has orders")
		require.False(t, results[1].no_orders, "Bob has orders")
		
		// Charlie has no orders, so no_orders should be true
		require.True(t, results[2].no_orders, "Charlie has no orders")
	})

	// Test EXISTS with additional predicates
	t.Run("EXISTS with additional predicates", func(t *testing.T) {
		rows, err := db.Query(`
			SELECT u.id, u.name, 
			       EXISTS (SELECT 1 FROM test_orders o WHERE o.user_id = u.id AND o.amount > 150) as has_large_orders
			FROM test_users u
			ORDER BY u.id
		`)
		require.NoError(t, err)
		defer rows.Close()

		type result struct {
			id               int
			name             string
			has_large_orders bool
		}

		var results []result
		for rows.Next() {
			var r result
			err := rows.Scan(&r.id, &r.name, &r.has_large_orders)
			require.NoError(t, err)
			results = append(results, r)
		}

		require.Len(t, results, 3)
		
		// Alice has an order with amount=200 > 150
		require.True(t, results[0].has_large_orders, "Alice should have large orders")
		
		// Bob has an order with amount=300 > 150
		require.True(t, results[1].has_large_orders, "Bob should have large orders")
		
		// Charlie has no orders at all
		require.False(t, results[2].has_large_orders, "Charlie should not have large orders")
	})

	// Clean up
	db.Exec("DROP TABLE IF EXISTS test_orders")
	db.Exec("DROP TABLE IF EXISTS test_users")
}

func TestMain(m *testing.M) {
	// Run tests
	fmt.Println("Running EXISTS correlation tests...")
	m.Run()
}