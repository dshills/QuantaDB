package txn

import (
	"context"
	"testing"

	"github.com/dshills/QuantaDB/internal/engine"
)

func TestMvccTransaction(t *testing.T) {
	eng := engine.NewMemoryEngine()
	defer eng.Close()

	manager := NewManager(eng, nil)
	ctx := context.Background()

	t.Run("Basic operations", func(t *testing.T) {
		txn, err := manager.BeginTransaction(ctx, ReadCommitted)
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		// Test Put and Get
		key := []byte("test_key")
		value := []byte("test_value")

		err = txn.Put(key, value)
		if err != nil {
			t.Errorf("Failed to put: %v", err)
		}

		retrieved, err := txn.Get(key)
		if err != nil {
			t.Errorf("Failed to get: %v", err)
		}

		if string(retrieved) != string(value) {
			t.Errorf("Expected %s, got %s", string(value), string(retrieved))
		}

		err = txn.Commit()
		if err != nil {
			t.Errorf("Failed to commit: %v", err)
		}
	})

	t.Run("Delete operations", func(t *testing.T) {
		// First create a value
		txn1, err := manager.BeginTransaction(ctx, ReadCommitted)
		if err != nil {
			t.Fatalf("Failed to begin transaction 1: %v", err)
		}

		key := []byte("delete_test")
		value := []byte("to_be_deleted")

		err = txn1.Put(key, value)
		if err != nil {
			t.Errorf("Failed to put: %v", err)
		}

		err = txn1.Commit()
		if err != nil {
			t.Errorf("Failed to commit transaction 1: %v", err)
		}

		// Now delete it
		txn2, err := manager.BeginTransaction(ctx, ReadCommitted)
		if err != nil {
			t.Fatalf("Failed to begin transaction 2: %v", err)
		}

		err = txn2.Delete(key)
		if err != nil {
			t.Errorf("Failed to delete: %v", err)
		}

		// Should not be able to get it in same transaction
		_, err = txn2.Get(key)
		if err != engine.ErrKeyNotFound {
			t.Errorf("Expected key not found after delete, got: %v", err)
		}

		err = txn2.Commit()
		if err != nil {
			t.Errorf("Failed to commit transaction 2: %v", err)
		}

		// Should not be found in new transaction
		txn3, err := manager.BeginTransaction(ctx, ReadCommitted)
		if err != nil {
			t.Fatalf("Failed to begin transaction 3: %v", err)
		}

		_, err = txn3.Get(key)
		if err != engine.ErrKeyNotFound {
			t.Errorf("Expected key not found in new transaction, got: %v", err)
		}

		txn3.Rollback()
	})

	t.Run("Read-your-writes consistency", func(t *testing.T) {
		txn, err := manager.BeginTransaction(ctx, ReadCommitted)
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		key := []byte("ryw_test")
		value1 := []byte("first_value")
		value2 := []byte("second_value")

		// Write first value
		err = txn.Put(key, value1)
		if err != nil {
			t.Errorf("Failed to put first value: %v", err)
		}

		// Read should return first value
		retrieved, err := txn.Get(key)
		if err != nil {
			t.Errorf("Failed to get first value: %v", err)
		}
		if string(retrieved) != string(value1) {
			t.Errorf("Expected %s, got %s", string(value1), string(retrieved))
		}

		// Overwrite with second value
		err = txn.Put(key, value2)
		if err != nil {
			t.Errorf("Failed to put second value: %v", err)
		}

		// Read should return second value
		retrieved, err = txn.Get(key)
		if err != nil {
			t.Errorf("Failed to get second value: %v", err)
		}
		if string(retrieved) != string(value2) {
			t.Errorf("Expected %s, got %s", string(value2), string(retrieved))
		}

		txn.Rollback()
	})

	t.Run("Transaction closed error", func(t *testing.T) {
		txn, err := manager.BeginTransaction(ctx, ReadCommitted)
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		// Commit the transaction
		err = txn.Commit()
		if err != nil {
			t.Errorf("Failed to commit: %v", err)
		}

		// Try to use after commit - should fail
		key := []byte("closed_test")
		value := []byte("should_fail")

		err = txn.Put(key, value)
		if err != engine.ErrTransactionClosed {
			t.Errorf("Expected transaction closed error, got: %v", err)
		}

		_, err = txn.Get(key)
		if err != engine.ErrTransactionClosed {
			t.Errorf("Expected transaction closed error, got: %v", err)
		}

		err = txn.Delete(key)
		if err != engine.ErrTransactionClosed {
			t.Errorf("Expected transaction closed error, got: %v", err)
		}
	})
}

func TestVersionedValue(t *testing.T) {
	t.Run("Visibility rules", func(t *testing.T) {
		// Create a versioned value
		vv := &VersionedValue{
			Value:        []byte("test_value"),
			CreatedByTxn: 1,
			CreatedAt:    10,
		}

		// Should be visible to read at timestamp 15
		activeTxns := make(map[TransactionID]bool)
		if !vv.IsVisible(15, activeTxns) {
			t.Error("Should be visible to later timestamp")
		}

		// Should not be visible to read at timestamp 5
		if vv.IsVisible(5, activeTxns) {
			t.Error("Should not be visible to earlier timestamp")
		}

		// Should not be visible if created by active transaction
		activeTxns[1] = true
		if vv.IsVisible(15, activeTxns) {
			t.Error("Should not be visible if created by active transaction")
		}
	})

	t.Run("Deletion visibility", func(t *testing.T) {
		// Create a deleted versioned value
		vv := &VersionedValue{
			Value:        []byte("deleted_value"),
			CreatedByTxn: 1,
			CreatedAt:    10,
			DeletedByTxn: 2,
			DeletedAt:    20,
		}

		activeTxns := make(map[TransactionID]bool)

		// Should be visible to read at timestamp 15 (before deletion)
		if !vv.IsVisible(15, activeTxns) {
			t.Error("Should be visible before deletion timestamp")
		}

		// Should not be visible to read at timestamp 25 (after deletion)
		if vv.IsVisible(25, activeTxns) {
			t.Error("Should not be visible after deletion timestamp")
		}

		// Should be visible if deleted by active transaction (deletion not committed)
		activeTxns[2] = true
		if !vv.IsVisible(25, activeTxns) {
			t.Error("Should be visible if deleted by active transaction")
		}
	})

	t.Run("IsDeleted check", func(t *testing.T) {
		// Non-deleted value
		vv1 := &VersionedValue{
			Value:        []byte("active_value"),
			CreatedByTxn: 1,
			CreatedAt:    10,
		}

		if vv1.IsDeleted() {
			t.Error("Should not be marked as deleted")
		}

		// Deleted value
		vv2 := &VersionedValue{
			Value:        []byte("deleted_value"),
			CreatedByTxn: 1,
			CreatedAt:    10,
			DeletedAt:    20,
		}

		if !vv2.IsDeleted() {
			t.Error("Should be marked as deleted")
		}
	})
}

func TestTimestampGeneration(t *testing.T) {
	t.Run("Monotonic timestamps", func(t *testing.T) {
		ts1 := NextTimestamp()
		ts2 := NextTimestamp()
		ts3 := NextTimestamp()

		if ts2 <= ts1 {
			t.Errorf("Timestamps should be monotonic: %d, %d", ts1, ts2)
		}

		if ts3 <= ts2 {
			t.Errorf("Timestamps should be monotonic: %d, %d", ts2, ts3)
		}
	})

	t.Run("Unique transaction IDs", func(t *testing.T) {
		id1 := NextTransactionID()
		id2 := NextTransactionID()
		id3 := NextTransactionID()

		if id1 == id2 || id2 == id3 || id1 == id3 {
			t.Errorf("Transaction IDs should be unique: %d, %d, %d", id1, id2, id3)
		}
	})
}

func TestConcurrentTransactions(t *testing.T) {
	eng := engine.NewMemoryEngine()
	defer eng.Close()

	manager := NewManager(eng, nil)
	ctx := context.Background()

	t.Run("Concurrent reads", func(t *testing.T) {
		// Setup initial data
		setupTxn, err := manager.BeginTransaction(ctx, ReadCommitted)
		if err != nil {
			t.Fatalf("Failed to begin setup transaction: %v", err)
		}

		key := []byte("concurrent_test")
		value := []byte("initial_value")
		err = setupTxn.Put(key, value)
		if err != nil {
			t.Errorf("Failed to put initial value: %v", err)
		}

		err = setupTxn.Commit()
		if err != nil {
			t.Errorf("Failed to commit setup: %v", err)
		}

		// Start multiple concurrent read transactions
		txn1, err := manager.BeginTransaction(ctx, ReadCommitted)
		if err != nil {
			t.Fatalf("Failed to begin transaction 1: %v", err)
		}

		txn2, err := manager.BeginTransaction(ctx, ReadCommitted)
		if err != nil {
			t.Fatalf("Failed to begin transaction 2: %v", err)
		}

		// Both should be able to read the value
		val1, err := txn1.Get(key)
		if err != nil {
			t.Errorf("Transaction 1 failed to read: %v", err)
		}

		val2, err := txn2.Get(key)
		if err != nil {
			t.Errorf("Transaction 2 failed to read: %v", err)
		}

		if string(val1) != string(value) || string(val2) != string(value) {
			t.Errorf("Concurrent reads returned different values: %s, %s", string(val1), string(val2))
		}

		// Clean up
		txn1.Rollback()
		txn2.Rollback()
	})
}
