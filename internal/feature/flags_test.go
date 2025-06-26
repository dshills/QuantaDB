package feature

import (
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFeatureFlags(t *testing.T) {
	// Save current state
	originalFlags := GetAll()
	defer func() {
		// Restore original state
		for flag, enabled := range originalFlags {
			if enabled {
				Enable(flag)
			} else {
				Disable(flag)
			}
		}
	}()

	t.Run("BasicEnableDisable", func(t *testing.T) {
		// Test a stable feature (default enabled)
		assert.True(t, IsEnabled(VectorizedExecution))
		
		Disable(VectorizedExecution)
		assert.False(t, IsEnabled(VectorizedExecution))
		
		Enable(VectorizedExecution)
		assert.True(t, IsEnabled(VectorizedExecution))
		
		// Test an experimental feature (default disabled)
		assert.False(t, IsEnabled(ExperimentalJoinReorder))
		
		Enable(ExperimentalJoinReorder)
		assert.True(t, IsEnabled(ExperimentalJoinReorder))
		
		Disable(ExperimentalJoinReorder)
		assert.False(t, IsEnabled(ExperimentalJoinReorder))
	})

	t.Run("EnvironmentVariables", func(t *testing.T) {
		// Set environment variable
		envVar := "QUANTADB_FEATURE_RESULT_CACHING"
		os.Setenv(envVar, "false")
		defer os.Unsetenv(envVar)
		
		// Create new manager to pick up env var
		m := newManager()
		
		// Should be disabled due to env var
		assert.False(t, m.IsEnabled(ResultCaching))
		
		// Programmatic enable should work
		m.Enable(ResultCaching)
		assert.True(t, m.IsEnabled(ResultCaching))
	})

	t.Run("OnChangeCallbacks", func(t *testing.T) {
		var changes []struct {
			flag    Flag
			enabled bool
		}
		var mu sync.Mutex
		
		// Register callback
		OnChange(func(flag Flag, enabled bool) {
			mu.Lock()
			changes = append(changes, struct {
				flag    Flag
				enabled bool
			}{flag, enabled})
			mu.Unlock()
		})
		
		// Make changes
		originalValue := IsEnabled(ParallelExecution)
		Enable(ParallelExecution)
		Disable(ParallelExecution)
		Enable(ParallelExecution)
		
		// Check callbacks were fired
		mu.Lock()
		defer mu.Unlock()
		
		expectedCount := 0
		if !originalValue {
			expectedCount++ // First enable
		}
		expectedCount += 2 // Disable then enable
		
		assert.GreaterOrEqual(t, len(changes), expectedCount)
	})

	t.Run("GetMetadata", func(t *testing.T) {
		metadata, exists := GetMetadata(VectorizedExecution)
		require.True(t, exists)
		assert.Equal(t, VectorizedExecution, metadata.Name)
		assert.Equal(t, "execution", metadata.Category)
		assert.Equal(t, "stable", metadata.Stability)
		assert.True(t, metadata.DefaultValue)
		
		// Non-existent flag
		_, exists = GetMetadata("non_existent_flag")
		assert.False(t, exists)
	})

	t.Run("GetByCategory", func(t *testing.T) {
		executionFlags := GetByCategory("execution")
		assert.Contains(t, executionFlags, VectorizedExecution)
		assert.Contains(t, executionFlags, AdaptiveExecution)
		assert.Contains(t, executionFlags, ParallelExecution)
		
		experimentalFlags := GetByCategory("experimental")
		assert.Contains(t, experimentalFlags, ExperimentalJoinReorder)
		assert.Contains(t, experimentalFlags, ExperimentalVectorOps)
	})

	t.Run("Reset", func(t *testing.T) {
		// Change some flags
		Enable(ExperimentalJoinReorder)  // Default false
		Disable(VectorizedExecution)     // Default true
		
		// Verify changes
		assert.True(t, IsEnabled(ExperimentalJoinReorder))
		assert.False(t, IsEnabled(VectorizedExecution))
		
		// Reset
		Reset()
		
		// Should be back to defaults
		assert.False(t, IsEnabled(ExperimentalJoinReorder))
		assert.True(t, IsEnabled(VectorizedExecution))
	})

	t.Run("ConcurrentAccess", func(t *testing.T) {
		var wg sync.WaitGroup
		numWorkers := 10
		opsPerWorker := 1000
		
		// Concurrent readers
		for i := 0; i < numWorkers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < opsPerWorker; j++ {
					_ = IsEnabled(VectorizedExecution)
					_ = IsEnabled(ParallelExecution)
					_ = GetAll()
				}
			}()
		}
		
		// Concurrent writers
		for i := 0; i < numWorkers/2; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				for j := 0; j < opsPerWorker; j++ {
					if j%2 == 0 {
						Enable(ResultCaching)
					} else {
						Disable(ResultCaching)
					}
				}
			}(i)
		}
		
		wg.Wait()
	})

	t.Run("DebugString", func(t *testing.T) {
		debug := DebugString()
		assert.Contains(t, debug, "Feature Flags:")
		assert.Contains(t, debug, "Execution:")  // Capitalized
		assert.Contains(t, debug, "vectorized_execution")
		assert.Contains(t, debug, "enabled")
		assert.Contains(t, debug, "[stable]")
	})
}

func TestFeatureFlagIntegration(t *testing.T) {
	t.Run("ExecutionEngineIntegration", func(t *testing.T) {
		// Simulate execution engine checking flags
		executeQuery := func() string {
			if IsEnabled(VectorizedExecution) && IsEnabled(AdaptiveExecution) {
				return "vectorized_adaptive"
			} else if IsEnabled(VectorizedExecution) {
				return "vectorized_only"
			} else if IsEnabled(ParallelExecution) {
				return "parallel_only"
			}
			return "row_based"
		}
		
		// Test different configurations
		Enable(VectorizedExecution)
		Enable(AdaptiveExecution)
		assert.Equal(t, "vectorized_adaptive", executeQuery())
		
		Disable(AdaptiveExecution)
		assert.Equal(t, "vectorized_only", executeQuery())
		
		Disable(VectorizedExecution)
		Enable(ParallelExecution)
		assert.Equal(t, "parallel_only", executeQuery())
		
		Disable(ParallelExecution)
		assert.Equal(t, "row_based", executeQuery())
	})

	t.Run("StorageEngineIntegration", func(t *testing.T) {
		// Simulate storage engine configuration
		type StorageConfig struct {
			UseCompression bool
			UseParallelIO  bool
			AutoVacuum     bool
		}
		
		getStorageConfig := func() StorageConfig {
			return StorageConfig{
				UseCompression: IsEnabled(CompressionEnabled),
				UseParallelIO:  IsEnabled(ParallelIO),
				AutoVacuum:     IsEnabled(AutoVacuum),
			}
		}
		
		// Default should have all enabled
		config := getStorageConfig()
		assert.True(t, config.UseCompression)
		assert.True(t, config.UseParallelIO)
		assert.True(t, config.AutoVacuum)
		
		// Disable for low-resource environment
		Disable(CompressionEnabled)
		Disable(ParallelIO)
		
		config = getStorageConfig()
		assert.False(t, config.UseCompression)
		assert.False(t, config.UseParallelIO)
		assert.True(t, config.AutoVacuum) // Still enabled
	})
}

func BenchmarkFeatureFlags(b *testing.B) {
	b.Run("IsEnabled", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = IsEnabled(VectorizedExecution)
		}
	})

	b.Run("ConcurrentIsEnabled", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_ = IsEnabled(VectorizedExecution)
			}
		})
	})

	b.Run("EnableDisable", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if i%2 == 0 {
				Enable(ResultCaching)
			} else {
				Disable(ResultCaching)
			}
		}
	})

	b.Run("GetAll", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = GetAll()
		}
	})
}