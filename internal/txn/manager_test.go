package txn

import (
	"context"
	"testing"
	"time"

	"github.com/dshills/QuantaDB/internal/engine"
)

func TestTransactionManager(t *testing.T) {
	// Create test engine and manager
	eng := engine.NewMemoryEngine()
	defer eng.Close()

	manager := NewManager(eng, &ManagerOptions{
		GCThreshold:   time.Minute,
		MaxActiveTime: time.Minute,
	})

	ctx := context.Background()

	t.Run("Begin transaction", func(t *testing.T) {
		txn, err := manager.BeginTransaction(ctx, ReadCommitted)
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		if txn.ID() == 0 {
			t.Error("Expected non-zero transaction ID")
		}

		if txn.IsolationLevel() != ReadCommitted {
			t.Errorf("Expected ReadCommitted isolation, got %v", txn.IsolationLevel())
		}

		// Clean up
		err = txn.Rollback()
		if err != nil {
			t.Errorf("Failed to rollback: %v", err)
		}
	})

	t.Run("Transaction operations", func(t *testing.T) {
		txn, err := manager.BeginTransaction(ctx, ReadCommitted)
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		// Test Put operation
		key := []byte("test_key")
		value := []byte("test_value")
		err = txn.Put(key, value)
		if err != nil {
			t.Errorf("Failed to put: %v", err)
		}

		// Test Get operation (read your writes)
		retrieved, err := txn.Get(key)
		if err != nil {
			t.Errorf("Failed to get: %v", err)
		}

		if string(retrieved) != string(value) {
			t.Errorf("Expected %s, got %s", string(value), string(retrieved))
		}

		// Commit transaction
		err = txn.Commit()
		if err != nil {
			t.Errorf("Failed to commit: %v", err)
		}
	})

	t.Run("Transaction isolation", func(t *testing.T) {
		// Start two transactions
		txn1, err := manager.BeginTransaction(ctx, ReadCommitted)
		if err != nil {
			t.Fatalf("Failed to begin transaction 1: %v", err)
		}

		txn2, err := manager.BeginTransaction(ctx, ReadCommitted)
		if err != nil {
			t.Fatalf("Failed to begin transaction 2: %v", err)
		}

		// Transaction 1 writes a key
		key := []byte("isolation_test")
		value1 := []byte("value_from_txn1")
		err = txn1.Put(key, value1)
		if err != nil {
			t.Errorf("Failed to put in txn1: %v", err)
		}

		// Transaction 2 should not see uncommitted changes
		_, err = txn2.Get(key)
		if err != engine.ErrKeyNotFound {
			t.Errorf("Expected key not found, got: %v", err)
		}

		// Commit transaction 1
		err = txn1.Commit()
		if err != nil {
			t.Errorf("Failed to commit txn1: %v", err)
		}

		// Transaction 2 should still not see it (depending on isolation level)
		// For ReadCommitted, it might see it in a new read
		// For RepeatableRead, it should not see it

		// Clean up
		err = txn2.Rollback()
		if err != nil {
			t.Errorf("Failed to rollback txn2: %v", err)
		}
	})

	t.Run("Transaction rollback", func(t *testing.T) {
		txn, err := manager.BeginTransaction(ctx, ReadCommitted)
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		// Write some data
		key := []byte("rollback_test")
		value := []byte("should_not_persist")
		err = txn.Put(key, value)
		if err != nil {
			t.Errorf("Failed to put: %v", err)
		}

		// Rollback transaction
		err = txn.Rollback()
		if err != nil {
			t.Errorf("Failed to rollback: %v", err)
		}

		// Start new transaction and verify data is not there
		txn2, err := manager.BeginTransaction(ctx, ReadCommitted)
		if err != nil {
			t.Fatalf("Failed to begin second transaction: %v", err)
		}

		_, err = txn2.Get(key)
		if err != engine.ErrKeyNotFound {
			t.Errorf("Expected key not found after rollback, got: %v", err)
		}

		err = txn2.Rollback()
		if err != nil {
			t.Errorf("Failed to rollback second transaction: %v", err)
		}
	})

	t.Run("Manager stats", func(t *testing.T) {
		// Begin a few transactions
		txn1, _ := manager.BeginTransaction(ctx, ReadCommitted)
		txn2, _ := manager.BeginTransaction(ctx, RepeatableRead)

		stats := manager.Stats()
		if stats.ActiveTransactions < 2 {
			t.Errorf("Expected at least 2 active transactions, got %d", stats.ActiveTransactions)
		}

		// Clean up
		txn1.Rollback()
		txn2.Rollback()
	})
}

func TestIsolationLevels(t *testing.T) {
	eng := engine.NewMemoryEngine()
	defer eng.Close()

	manager := NewManager(eng, nil)
	ctx := context.Background()

	t.Run("ReadCommitted isolation", func(t *testing.T) {
		txn, err := manager.BeginTransaction(ctx, ReadCommitted)
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		if txn.IsolationLevel() != ReadCommitted {
			t.Errorf("Expected ReadCommitted, got %v", txn.IsolationLevel())
		}

		txn.Rollback()
	})

	t.Run("RepeatableRead isolation", func(t *testing.T) {
		txn, err := manager.BeginTransaction(ctx, RepeatableRead)
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		if txn.IsolationLevel() != RepeatableRead {
			t.Errorf("Expected RepeatableRead, got %v", txn.IsolationLevel())
		}

		txn.Rollback()
	})

	t.Run("Serializable isolation", func(t *testing.T) {
		txn, err := manager.BeginTransaction(ctx, Serializable)
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		if txn.IsolationLevel() != Serializable {
			t.Errorf("Expected Serializable, got %v", txn.IsolationLevel())
		}

		// For serializable, read and write timestamps should be the same
		if txn.ReadTimestamp() != txn.WriteTimestamp() {
			t.Errorf("Expected same read/write timestamp for serializable, got read=%d write=%d",
				txn.ReadTimestamp(), txn.WriteTimestamp())
		}

		txn.Rollback()
	})
}

func TestMVCCVersioning(t *testing.T) {
	eng := engine.NewMemoryEngine()
	defer eng.Close()

	manager := NewManager(eng, nil)
	ctx := context.Background()

	t.Run("MVCC visibility", func(t *testing.T) {
		// Create initial version
		txn1, err := manager.BeginTransaction(ctx, ReadCommitted)
		if err != nil {
			t.Fatalf("Failed to begin transaction 1: %v", err)
		}

		key := []byte("mvcc_test")
		value1 := []byte("version_1")
		err = txn1.Put(key, value1)
		if err != nil {
			t.Errorf("Failed to put version 1: %v", err)
		}

		err = txn1.Commit()
		if err != nil {
			t.Errorf("Failed to commit transaction 1: %v", err)
		}

		// Start a long-running transaction that should see version 1
		txnLong, err := manager.BeginTransaction(ctx, RepeatableRead)
		if err != nil {
			t.Fatalf("Failed to begin long transaction: %v", err)
		}

		// Read version 1
		retrieved, err := txnLong.Get(key)
		if err != nil {
			t.Errorf("Failed to get from long transaction: %v", err)
		}
		if string(retrieved) != string(value1) {
			t.Errorf("Expected %s, got %s", string(value1), string(retrieved))
		}

		// Create a new version
		txn2, err := manager.BeginTransaction(ctx, ReadCommitted)
		if err != nil {
			t.Fatalf("Failed to begin transaction 2: %v", err)
		}

		value2 := []byte("version_2")
		err = txn2.Put(key, value2)
		if err != nil {
			t.Errorf("Failed to put version 2: %v", err)
		}

		err = txn2.Commit()
		if err != nil {
			t.Errorf("Failed to commit transaction 2: %v", err)
		}

		// Long-running transaction should still see version 1 (RepeatableRead)
		retrieved, err = txnLong.Get(key)
		if err != nil {
			t.Errorf("Failed to get from long transaction after version 2: %v", err)
		}
		if string(retrieved) != string(value1) {
			t.Errorf("Expected %s (version 1), got %s", string(value1), string(retrieved))
		}

		// New transaction should see version 2
		txn3, err := manager.BeginTransaction(ctx, ReadCommitted)
		if err != nil {
			t.Fatalf("Failed to begin transaction 3: %v", err)
		}

		retrieved, err = txn3.Get(key)
		if err != nil {
			t.Errorf("Failed to get from transaction 3: %v", err)
		}
		if string(retrieved) != string(value2) {
			t.Errorf("Expected %s (version 2), got %s", string(value2), string(retrieved))
		}

		// Clean up
		txnLong.Rollback()
		txn3.Rollback()
	})
}

func TestTransactionTimeout(t *testing.T) {
	eng := engine.NewMemoryEngine()
	defer eng.Close()

	// Set very short timeout for testing
	manager := NewManager(eng, &ManagerOptions{
		GCThreshold:   time.Minute,
		MaxActiveTime: time.Millisecond * 10,
	})

	ctx := context.Background()

	txn, err := manager.BeginTransaction(ctx, ReadCommitted)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Wait for timeout
	time.Sleep(time.Millisecond * 50)

	// Try to commit - should fail due to timeout
	err = txn.Commit()
	if err == nil {
		t.Error("Expected timeout error on commit")
	}

	// Clean up
	txn.Rollback()
}
