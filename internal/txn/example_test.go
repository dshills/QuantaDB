package txn

import (
	"context"
	"fmt"
	"testing"

	"github.com/dshills/QuantaDB/internal/engine"
)

// Example demonstrates basic transaction usage.
func ExampleManager() {
	// Create storage engine and transaction manager
	eng := engine.NewMemoryEngine()
	defer eng.Close()

	manager := NewManager(eng, nil)
	ctx := context.Background()

	// Begin a transaction
	txn, err := manager.BeginTransaction(ctx, ReadCommitted)
	if err != nil {
		panic(err)
	}

	// Store some data
	key := []byte("user:1")
	value := []byte(`{"name": "Alice", "balance": 1000}`)

	err = txn.Put(key, value)
	if err != nil {
		txn.Rollback()
		panic(err)
	}

	// Read the data back (read-your-writes)
	retrieved, err := txn.Get(key)
	if err != nil {
		txn.Rollback()
		panic(err)
	}

	fmt.Printf("Retrieved: %s\n", string(retrieved))

	// Commit the transaction
	err = txn.Commit()
	if err != nil {
		panic(err)
	}

	// Output: Retrieved: {"name": "Alice", "balance": 1000}
}

// TestExampleMvccIsolation demonstrates MVCC isolation.
func TestExampleMvccIsolation(t *testing.T) {
	eng := engine.NewMemoryEngine()
	defer eng.Close()

	manager := NewManager(eng, nil)
	ctx := context.Background()

	// Create initial data
	setupTxn, _ := manager.BeginTransaction(ctx, ReadCommitted)
	setupTxn.Put([]byte("account:alice"), []byte("1000"))
	setupTxn.Commit()

	// Start two concurrent transactions
	txn1, _ := manager.BeginTransaction(ctx, RepeatableRead)
	txn2, _ := manager.BeginTransaction(ctx, ReadCommitted)

	// Transaction 1 reads Alice's balance
	balance1, _ := txn1.Get([]byte("account:alice"))
	fmt.Printf("TXN1 reads Alice's balance: %s\n", string(balance1))

	// Transaction 2 updates Alice's balance
	txn2.Put([]byte("account:alice"), []byte("1500"))
	txn2.Commit()
	fmt.Println("TXN2 committed Alice's balance update")

	// Transaction 1 reads again - should still see old value due to RepeatableRead
	balance1Again, _ := txn1.Get([]byte("account:alice"))
	fmt.Printf("TXN1 reads Alice's balance again: %s\n", string(balance1Again))

	txn1.Rollback()

	// New transaction should see the updated value
	txn3, _ := manager.BeginTransaction(ctx, ReadCommitted)
	balance3, _ := txn3.Get([]byte("account:alice"))
	fmt.Printf("TXN3 reads Alice's balance: %s\n", string(balance3))
	txn3.Rollback()

	// Output:
	// TXN1 reads Alice's balance: 1000
	// TXN2 committed Alice's balance update
	// TXN1 reads Alice's balance again: 1000
	// TXN3 reads Alice's balance: 1500
}

// TestExampleTransactionRollback demonstrates transaction rollback.
func TestExampleTransactionRollback(t *testing.T) {
	eng := engine.NewMemoryEngine()
	defer eng.Close()

	manager := NewManager(eng, nil)
	ctx := context.Background()

	// Begin transaction and make changes
	txn, _ := manager.BeginTransaction(ctx, ReadCommitted)

	key := []byte("temp:data")
	value := []byte("should not persist")

	txn.Put(key, value)

	// Verify we can read our own writes
	retrieved, _ := txn.Get(key)
	fmt.Printf("Before rollback: %s\n", string(retrieved))

	// Rollback the transaction
	txn.Rollback()
	fmt.Println("Transaction rolled back")

	// Start new transaction - should not see the data
	newTxn, _ := manager.BeginTransaction(ctx, ReadCommitted)
	_, err := newTxn.Get(key)
	if err == engine.ErrKeyNotFound {
		fmt.Println("Data not found after rollback (as expected)")
	}
	newTxn.Rollback()

	// Output:
	// Before rollback: should not persist
	// Transaction rolled back
	// Data not found after rollback (as expected)
}

// BenchmarkTransactionThroughput measures transaction throughput.
func BenchmarkTransactionThroughput(b *testing.B) {
	eng := engine.NewMemoryEngine()
	defer eng.Close()

	manager := NewManager(eng, nil)
	ctx := context.Background()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			txn, err := manager.BeginTransaction(ctx, ReadCommitted)
			if err != nil {
				b.Fatal(err)
			}

			key := []byte(fmt.Sprintf("key:%d", b.N))
			value := []byte("benchmark_value")

			err = txn.Put(key, value)
			if err != nil {
				txn.Rollback()
				b.Fatal(err)
			}

			_, err = txn.Get(key)
			if err != nil {
				txn.Rollback()
				b.Fatal(err)
			}

			err = txn.Commit()
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkConcurrentReads measures concurrent read performance.
func BenchmarkConcurrentReads(b *testing.B) {
	eng := engine.NewMemoryEngine()
	defer eng.Close()

	manager := NewManager(eng, nil)
	ctx := context.Background()

	// Setup data
	setupTxn, _ := manager.BeginTransaction(ctx, ReadCommitted)
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("read_key:%d", i))
		value := []byte(fmt.Sprintf("value_%d", i))
		setupTxn.Put(key, value)
	}
	setupTxn.Commit()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		txn, _ := manager.BeginTransaction(ctx, ReadCommitted)
		defer txn.Rollback()

		for pb.Next() {
			key := []byte(fmt.Sprintf("read_key:%d", b.N%1000))
			_, err := txn.Get(key)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
