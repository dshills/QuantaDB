# Feature Flag Framework

## Overview

A comprehensive feature flag framework has been implemented for QuantaDB to enable controlled rollout of features, A/B testing, and safe experimentation in production.

## Design Principles

1. **Zero-Allocation Fast Path**: Flag checks use atomic operations
2. **Thread-Safe**: All operations are safe for concurrent use
3. **Environment Override**: Flags can be controlled via environment variables
4. **Change Notification**: Callbacks for dynamic configuration updates
5. **Categorization**: Flags organized by functional area and stability

## Architecture

### Core Components

```go
// Flag represents a feature flag
type Flag string

// Manager manages feature flags
type Manager struct {
    flags      map[Flag]*flagState
    mu         sync.RWMutex
    onChange   []func(Flag, bool)
    metadata   map[Flag]*FlagMetadata
}

// Global API
func IsEnabled(flag Flag) bool
func Enable(flag Flag)
func Disable(flag Flag)
```

### Flag Categories

1. **Execution Engine** (`execution`)
   - VectorizedExecution
   - AdaptiveExecution
   - ParallelExecution
   - ResultCaching
   - CostBasedOptimization

2. **Storage Engine** (`storage`)
   - CompressionEnabled
   - ParallelIO
   - AutoVacuum

3. **Index Features** (`index`)
   - CoveringIndexes
   - IndexIntersection
   - AutoIndexing

4. **Query Features** (`query`)
   - WindowFunctions
   - CTESupport
   - PreparedStatements

5. **Distributed** (`distributed`)
   - Replication
   - DistributedQueries

6. **Monitoring** (`monitoring`)
   - DetailedMetrics
   - QueryProfiling

7. **Experimental** (`experimental`)
   - ExperimentalJoinReorder
   - ExperimentalVectorOps

### Stability Levels

- **stable**: Production-ready, enabled by default
- **beta**: Feature complete, disabled by default
- **experimental**: Under development, disabled by default

## Usage Examples

### Basic Usage

```go
import "github.com/dshills/QuantaDB/internal/feature"

// Check if a feature is enabled
if feature.IsEnabled(feature.VectorizedExecution) {
    // Use vectorized execution path
} else {
    // Use row-based execution path
}

// Enable/disable features
feature.Enable(feature.ExperimentalJoinReorder)
feature.Disable(feature.AutoIndexing)
```

### Integration in Executor

```go
func (e *Executor) Execute(plan Plan, ctx *ExecContext) (Result, error) {
    // Check feature flags
    if feature.IsEnabled(feature.ResultCaching) {
        if cached := e.cache.Get(plan); cached != nil {
            return cached, nil
        }
    }
    
    var operator Operator
    if feature.IsEnabled(feature.VectorizedExecution) {
        operator = createVectorizedOperator(plan)
    } else {
        operator = createRowBasedOperator(plan)
    }
    
    // Execute...
}
```

### Integration in Planner

```go
func (p *Planner) optimizePlan(plan LogicalPlan) (PhysicalPlan, error) {
    if feature.IsEnabled(feature.CostBasedOptimization) {
        plan = p.applyCostBasedOptimizations(plan)
    }
    
    if feature.IsEnabled(feature.ExperimentalJoinReorder) {
        plan = p.applyExperimentalJoinReordering(plan)
    }
    
    return p.createPhysicalPlan(plan)
}
```

### Environment Variables

All flags can be controlled via environment variables:

```bash
# Disable vectorized execution
export QUANTADB_FEATURE_VECTORIZED_EXECUTION=false

# Enable experimental features
export QUANTADB_FEATURE_EXPERIMENTAL_JOIN_REORDER=true
```

### Change Notifications

```go
// Register for flag changes
feature.OnChange(func(flag feature.Flag, enabled bool) {
    log.Printf("Feature %s changed to %v", flag, enabled)
    
    // Reconfigure components
    if flag == feature.ResultCaching {
        if enabled {
            executor.EnableCache()
        } else {
            executor.DisableCache()
        }
    }
})
```

## Performance Characteristics

### Benchmarks

```
BenchmarkFeatureFlags/IsEnabled-10         	1000000000	         0.5 ns/op	       0 B/op	       0 allocs/op
BenchmarkFeatureFlags/ConcurrentIsEnabled-10	300000000	         2.1 ns/op	       0 B/op	       0 allocs/op
BenchmarkFeatureFlags/EnableDisable-10      	20000000	        65.3 ns/op	       0 B/op	       0 allocs/op
BenchmarkFeatureFlags/GetAll-10             	 5000000	       285 ns/op	     896 B/op	       1 allocs/op
```

Key findings:
- Flag checks are essentially free (0.5ns)
- No allocations on the hot path
- Thread-safe with minimal contention
- GetAll() allocates for the result map

## Best Practices

### 1. Fail-Safe Defaults

```go
// Good: Default to stable behavior
if feature.IsEnabled(feature.ExperimentalVectorOps) {
    return experimentalImplementation()
}
return stableImplementation()
```

### 2. Feature Flag Naming

- Use descriptive names: `vectorized_execution` not `vec_exec`
- Group related flags: `experimental_*` for experimental features
- Be consistent with naming patterns

### 3. Gradual Rollout

```go
// Start with experimental
feature.ExperimentalNewFeature

// Move to beta when stable
feature.BetaNewFeature

// Finally promote to stable
feature.NewFeature
```

### 4. Clean Up Old Flags

```go
// After feature is fully rolled out and stable
// 1. Change default to true
// 2. Remove flag checks after deprecation period
// 3. Remove flag definition
```

## Testing

### Unit Testing with Flags

```go
func TestFeatureWithFlags(t *testing.T) {
    // Save original state
    original := feature.IsEnabled(feature.VectorizedExecution)
    defer func() {
        if original {
            feature.Enable(feature.VectorizedExecution)
        } else {
            feature.Disable(feature.VectorizedExecution)
        }
    }()
    
    // Test with feature enabled
    feature.Enable(feature.VectorizedExecution)
    result := executeQuery()
    assert.Equal(t, "vectorized", result.Mode)
    
    // Test with feature disabled
    feature.Disable(feature.VectorizedExecution)
    result = executeQuery()
    assert.Equal(t, "row_based", result.Mode)
}
```

### Integration Testing

```go
func TestIntegrationWithFeatures(t *testing.T) {
    tests := []struct {
        name     string
        features map[feature.Flag]bool
        expected string
    }{
        {
            name: "full_features",
            features: map[feature.Flag]bool{
                feature.VectorizedExecution: true,
                feature.ParallelExecution:   true,
                feature.ResultCaching:       true,
            },
            expected: "optimized",
        },
        {
            name: "minimal_features",
            features: map[feature.Flag]bool{
                feature.VectorizedExecution: false,
                feature.ParallelExecution:   false,
                feature.ResultCaching:       false,
            },
            expected: "basic",
        },
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            // Set features
            for flag, enabled := range tt.features {
                if enabled {
                    feature.Enable(flag)
                } else {
                    feature.Disable(flag)
                }
            }
            
            // Run test
            result := runIntegrationTest()
            assert.Equal(t, tt.expected, result)
        })
    }
}
```

## Monitoring

### Metrics Collection

```go
// Track feature usage
var (
    featureUsageCounter = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Name: "quantadb_feature_usage_total",
            Help: "Total number of times a feature was used",
        },
        []string{"feature", "enabled"},
    )
)

// In code
if feature.IsEnabled(feature.VectorizedExecution) {
    featureUsageCounter.WithLabelValues("vectorized_execution", "true").Inc()
    // Use feature
} else {
    featureUsageCounter.WithLabelValues("vectorized_execution", "false").Inc()
}
```

### Debug Output

```go
// Get current state of all flags
fmt.Println(feature.DebugString())

// Output:
// Feature Flags:
// 
// Execution:
//   vectorized_execution          : enabled  [stable] - Enable vectorized query execution
//   adaptive_execution            : enabled  [stable] - Enable adaptive execution mode switching
//   parallel_execution            : enabled  [stable] - Enable parallel query execution
//   ...
```

## Future Enhancements

1. **Dynamic Reloading**: Watch configuration files for changes
2. **Percentage Rollout**: Enable features for percentage of queries
3. **User/Session Targeting**: Enable features for specific users
4. **A/B Testing Integration**: Built-in experiment tracking
5. **Admin UI**: Web interface for flag management
6. **Audit Trail**: Log all flag changes with timestamp and user

## Conclusion

The feature flag framework provides a robust foundation for:
- Safe feature rollout
- Performance experimentation
- Quick rollback capability
- Configuration without deployment
- A/B testing infrastructure

It's designed to have minimal performance impact while providing maximum flexibility for QuantaDB's evolution.