package engine

import (
	"context"
	"testing"
)

func TestMemoryEngineCloseHandling(t *testing.T) {
	ctx := context.Background()
	engine := NewMemoryEngine()

	// Test normal operations work before closing
	err := engine.Put(ctx, []byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Put before close failed: %v", err)
	}

	value, err := engine.Get(ctx, []byte("key1"))
	if err != nil {
		t.Fatalf("Get before close failed: %v", err)
	}
	if string(value) != "value1" {
		t.Fatalf("Expected value1, got %s", string(value))
	}

	// Close the engine
	err = engine.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Test that all operations return ErrEngineClosed after closing
	t.Run("GetAfterClose", func(t *testing.T) {
		_, err := engine.Get(ctx, []byte("key1"))
		if err != ErrEngineClosed {
			t.Errorf("Expected ErrEngineClosed, got %v", err)
		}
	})

	t.Run("PutAfterClose", func(t *testing.T) {
		err := engine.Put(ctx, []byte("key2"), []byte("value2"))
		if err != ErrEngineClosed {
			t.Errorf("Expected ErrEngineClosed, got %v", err)
		}
	})

	t.Run("DeleteAfterClose", func(t *testing.T) {
		err := engine.Delete(ctx, []byte("key1"))
		if err != ErrEngineClosed {
			t.Errorf("Expected ErrEngineClosed, got %v", err)
		}
	})

	t.Run("ScanAfterClose", func(t *testing.T) {
		_, err := engine.Scan(ctx, nil, nil)
		if err != ErrEngineClosed {
			t.Errorf("Expected ErrEngineClosed, got %v", err)
		}
	})

	t.Run("BeginTransactionAfterClose", func(t *testing.T) {
		_, err := engine.BeginTransaction(ctx)
		if err != ErrEngineClosed {
			t.Errorf("Expected ErrEngineClosed, got %v", err)
		}
	})

	t.Run("MultipleClose", func(t *testing.T) {
		// Closing again should be a no-op and return nil
		err := engine.Close()
		if err != nil {
			t.Errorf("Expected nil on second close, got %v", err)
		}
	})
}
