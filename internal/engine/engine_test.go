package engine

import (
	"context"
	"testing"

	"github.com/dshills/QuantaDB/internal/testutil"
)

func TestMemoryEngine(t *testing.T) {
	ctx := context.Background()
	engine := NewMemoryEngine()
	defer engine.Close()
	
	// Test basic operations
	t.Run("BasicOperations", func(t *testing.T) {
		key := []byte("test-key")
		value := []byte("test-value")
		
		// Get non-existent key
		_, err := engine.Get(ctx, key)
		testutil.AssertEqual(t, ErrKeyNotFound, err)
		
		// Put key-value
		err = engine.Put(ctx, key, value)
		testutil.AssertNoError(t, err)
		
		// Get existing key
		result, err := engine.Get(ctx, key)
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, string(value), string(result))
		
		// Update value
		newValue := []byte("new-value")
		err = engine.Put(ctx, key, newValue)
		testutil.AssertNoError(t, err)
		
		result, err = engine.Get(ctx, key)
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, string(newValue), string(result))
		
		// Delete key
		err = engine.Delete(ctx, key)
		testutil.AssertNoError(t, err)
		
		_, err = engine.Get(ctx, key)
		testutil.AssertEqual(t, ErrKeyNotFound, err)
	})
	
	// Test scan
	t.Run("Scan", func(t *testing.T) {
		// Insert test data
		testData := map[string]string{
			"a": "value-a",
			"b": "value-b",
			"c": "value-c",
			"d": "value-d",
			"e": "value-e",
		}
		
		for k, v := range testData {
			err := engine.Put(ctx, []byte(k), []byte(v))
			testutil.AssertNoError(t, err)
		}
		
		// Scan all
		iter, err := engine.Scan(ctx, nil, nil)
		testutil.AssertNoError(t, err)
		defer iter.Close()
		
		count := 0
		for iter.Next() {
			key := string(iter.Key())
			value := string(iter.Value())
			expectedValue, exists := testData[key]
			testutil.AssertTrue(t, exists, "unexpected key: "+key)
			testutil.AssertEqual(t, expectedValue, value)
			count++
		}
		testutil.AssertEqual(t, len(testData), count)
		testutil.AssertNoError(t, iter.Error())
		
		// Scan range [b, d)
		iter, err = engine.Scan(ctx, []byte("b"), []byte("d"))
		testutil.AssertNoError(t, err)
		defer iter.Close()
		
		expectedKeys := []string{"b", "c"}
		i := 0
		for iter.Next() {
			key := string(iter.Key())
			testutil.AssertEqual(t, expectedKeys[i], key)
			i++
		}
		testutil.AssertEqual(t, len(expectedKeys), i)
	})
}

func TestMemoryTransaction(t *testing.T) {
	ctx := context.Background()
	engine := NewMemoryEngine()
	defer engine.Close()
	
	t.Run("CommitTransaction", func(t *testing.T) {
		// Prepare initial data
		err := engine.Put(ctx, []byte("key1"), []byte("value1"))
		testutil.AssertNoError(t, err)
		
		// Start transaction
		tx, err := engine.BeginTransaction(ctx)
		testutil.AssertNoError(t, err)
		
		// Read existing key
		value, err := tx.Get([]byte("key1"))
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, "value1", string(value))
		
		// Write new key
		err = tx.Put([]byte("key2"), []byte("value2"))
		testutil.AssertNoError(t, err)
		
		// Update existing key
		err = tx.Put([]byte("key1"), []byte("updated1"))
		testutil.AssertNoError(t, err)
		
		// Delete a key
		err = tx.Put([]byte("key3"), []byte("value3"))
		testutil.AssertNoError(t, err)
		err = tx.Delete([]byte("key3"))
		testutil.AssertNoError(t, err)
		
		// Verify changes are not visible outside transaction
		value, err = engine.Get(ctx, []byte("key1"))
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, "value1", string(value))
		
		_, err = engine.Get(ctx, []byte("key2"))
		testutil.AssertEqual(t, ErrKeyNotFound, err)
		
		// Commit transaction
		err = tx.Commit()
		testutil.AssertNoError(t, err)
		
		// Verify changes are now visible
		value, err = engine.Get(ctx, []byte("key1"))
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, "updated1", string(value))
		
		value, err = engine.Get(ctx, []byte("key2"))
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, "value2", string(value))
		
		_, err = engine.Get(ctx, []byte("key3"))
		testutil.AssertEqual(t, ErrKeyNotFound, err)
	})
	
	t.Run("RollbackTransaction", func(t *testing.T) {
		// Prepare initial data
		err := engine.Put(ctx, []byte("rollback-key"), []byte("initial"))
		testutil.AssertNoError(t, err)
		
		// Start transaction
		tx, err := engine.BeginTransaction(ctx)
		testutil.AssertNoError(t, err)
		
		// Make changes
		err = tx.Put([]byte("rollback-key"), []byte("modified"))
		testutil.AssertNoError(t, err)
		err = tx.Put([]byte("new-key"), []byte("new-value"))
		testutil.AssertNoError(t, err)
		
		// Rollback
		err = tx.Rollback()
		testutil.AssertNoError(t, err)
		
		// Verify no changes were applied
		value, err := engine.Get(ctx, []byte("rollback-key"))
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, "initial", string(value))
		
		_, err = engine.Get(ctx, []byte("new-key"))
		testutil.AssertEqual(t, ErrKeyNotFound, err)
	})
	
	t.Run("ClosedTransaction", func(t *testing.T) {
		tx, err := engine.BeginTransaction(ctx)
		testutil.AssertNoError(t, err)
		
		// Commit transaction
		err = tx.Commit()
		testutil.AssertNoError(t, err)
		
		// Try to use closed transaction
		err = tx.Put([]byte("key"), []byte("value"))
		testutil.AssertEqual(t, ErrTransactionClosed, err)
		
		_, err = tx.Get([]byte("key"))
		testutil.AssertEqual(t, ErrTransactionClosed, err)
		
		err = tx.Delete([]byte("key"))
		testutil.AssertEqual(t, ErrTransactionClosed, err)
		
		err = tx.Commit()
		testutil.AssertEqual(t, ErrTransactionClosed, err)
	})
}

func TestMemoryEngineIsolation(t *testing.T) {
	ctx := context.Background()
	engine := NewMemoryEngine()
	defer engine.Close()
	
	// Test that modifications to returned values don't affect stored data
	key := []byte("isolation-key")
	originalValue := []byte("original-value")
	
	err := engine.Put(ctx, key, originalValue)
	testutil.AssertNoError(t, err)
	
	// Get value and modify it
	value, err := engine.Get(ctx, key)
	testutil.AssertNoError(t, err)
	value[0] = 'X' // Modify the returned value
	
	// Verify stored value is unchanged
	storedValue, err := engine.Get(ctx, key)
	testutil.AssertNoError(t, err)
	testutil.AssertEqual(t, "original-value", string(storedValue))
}

func TestMemoryEngineMultipleTransactions(t *testing.T) {
	ctx := context.Background()
	engine := NewMemoryEngine()
	defer engine.Close()
	
	// Create initial data
	err := engine.Put(ctx, []byte("counter"), []byte("0"))
	testutil.AssertNoError(t, err)
	
	// Start two transactions
	tx1, err := engine.BeginTransaction(ctx)
	testutil.AssertNoError(t, err)
	
	tx2, err := engine.BeginTransaction(ctx)
	testutil.AssertNoError(t, err)
	
	// Both read the same value
	val1, err := tx1.Get([]byte("counter"))
	testutil.AssertNoError(t, err)
	testutil.AssertEqual(t, "0", string(val1))
	
	val2, err := tx2.Get([]byte("counter"))
	testutil.AssertNoError(t, err)
	testutil.AssertEqual(t, "0", string(val2))
	
	// Both update with different values
	err = tx1.Put([]byte("counter"), []byte("1"))
	testutil.AssertNoError(t, err)
	
	err = tx2.Put([]byte("counter"), []byte("2"))
	testutil.AssertNoError(t, err)
	
	// Commit both (last writer wins in this simple implementation)
	err = tx1.Commit()
	testutil.AssertNoError(t, err)
	
	err = tx2.Commit()
	testutil.AssertNoError(t, err)
	
	// Check final value
	finalValue, err := engine.Get(ctx, []byte("counter"))
	testutil.AssertNoError(t, err)
	testutil.AssertEqual(t, "2", string(finalValue))
}