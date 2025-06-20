package txn

import (
	"sync"
	"testing"
)

// TestTimestampServiceConcurrency verifies thread-safe timestamp operations
func TestTimestampServiceConcurrency(t *testing.T) {
	ts := NewTimestampService()
	
	// Test concurrent reads don't cause race conditions
	t.Run("ConcurrentReads", func(t *testing.T) {
		const goroutines = 100
		const reads = 1000
		
		var wg sync.WaitGroup
		wg.Add(goroutines)
		
		for i := 0; i < goroutines; i++ {
			go func() {
				defer wg.Done()
				for j := 0; j < reads; j++ {
					_ = ts.GetCurrentTimestamp()
				}
			}()
		}
		
		wg.Wait()
	})
	
	// Test mixed reads and writes
	t.Run("MixedOperations", func(t *testing.T) {
		const goroutines = 50
		const operations = 100
		
		var wg sync.WaitGroup
		wg.Add(goroutines * 2)
		
		// Half goroutines read
		for i := 0; i < goroutines; i++ {
			go func() {
				defer wg.Done()
				for j := 0; j < operations; j++ {
					_ = ts.GetCurrentTimestamp()
				}
			}()
		}
		
		// Half goroutines advance
		for i := 0; i < goroutines; i++ {
			go func() {
				defer wg.Done()
				for j := 0; j < operations; j++ {
					_ = ts.GetNextTimestamp()
				}
			}()
		}
		
		wg.Wait()
	})
	
	// Test GetSnapshotTimestamp with nil transaction
	t.Run("SnapshotWithNilTxn", func(t *testing.T) {
		const goroutines = 100
		const calls = 1000
		
		var wg sync.WaitGroup
		wg.Add(goroutines)
		
		for i := 0; i < goroutines; i++ {
			go func() {
				defer wg.Done()
				for j := 0; j < calls; j++ {
					_ = ts.GetSnapshotTimestamp(nil)
				}
			}()
		}
		
		wg.Wait()
	})
}