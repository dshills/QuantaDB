# Query Planner Integration - Implementation Plan

## Executive Summary

This document outlines the implementation plan for integrating vectorized execution and result caching with QuantaDB's cost-based query optimizer. The goal is to create an intelligent planner that makes optimal execution decisions based on query characteristics, data statistics, and system state.

## Current State Analysis

### Existing Infrastructure
- ✅ **Cost-Based Optimizer**: PostgreSQL-inspired cost model with configurable parameters
- ✅ **Vectorized Cost Model**: Basic framework for vectorized vs scalar cost comparison
- ✅ **Rule-Based Optimization**: Predicate pushdown, index selection, subquery decorrelation
- ✅ **Expression System**: Visitor pattern for expression analysis
- ✅ **Statistics Framework**: Table and column statistics with histograms

### Gaps Identified
- ❌ **Physical Planning Phase**: Logical plans directly map to execution without physical optimization
- ❌ **Operator-Level Vectorization**: All-or-nothing vectorization vs per-operator decisions
- ❌ **Expression Complexity Analysis**: No vectorization feasibility assessment
- ❌ **Runtime Adaptability**: No feedback mechanism for improving future decisions
- ❌ **Memory-Aware Planning**: Vectorization decisions don't consider memory pressure

## Implementation Phases

### Phase 1: Physical Planning Infrastructure
**Duration**: 3-4 days
**Priority**: Critical Foundation

#### 1.1 Enhanced Physical Plan Framework
```go
// internal/sql/planner/physical.go
type PhysicalPlan interface {
    Plan
    EstimateCost(context *PlanContext) *Cost
    EstimateMemory() int64
    GetOperatorType() OperatorType
    RequiresVectorization() bool
}

type PhysicalPlanContext struct {
    CostModel         *VectorizedCostModel
    MemoryAvailable   int64
    RuntimeStats      *RuntimeStatistics
    TableStats        map[string]*catalog.TableStats
    IndexStats        map[string]*catalog.IndexStats
}
```

#### 1.2 Physical Plan Nodes
```go
// Hybrid physical nodes that can be vectorized or scalar
type PhysicalScan struct {
    *BasePhysicalPlan
    Table           *catalog.Table
    Predicate       Expression
    ExecutionMode   ExecutionMode // Vectorized, Scalar, Adaptive
    IndexAccess     *IndexAccessPath
}

type PhysicalFilter struct {
    *BasePhysicalPlan
    Input         PhysicalPlan
    Predicate     Expression
    ExecutionMode ExecutionMode
    BatchSize     int
}

type PhysicalJoin struct {
    *BasePhysicalPlan
    Left, Right   PhysicalPlan
    JoinType      JoinType
    Condition     Expression
    Algorithm     JoinAlgorithm // Hash, NestedLoop, Merge
    ExecutionMode ExecutionMode
}
```

#### 1.3 Physical Plan Generator
```go
type PhysicalPlanner struct {
    costEstimator    *CostEstimator
    vectorizedModel  *VectorizedCostModel
    memoryManager    *MemoryManager
    runtimeStats     *RuntimeStatistics
}

func (pp *PhysicalPlanner) GeneratePhysicalPlan(
    logical LogicalPlan,
    context *PhysicalPlanContext,
) (PhysicalPlan, error) {
    // Convert logical plan to physical with cost-based decisions
    return pp.convertToPhysical(logical, context)
}
```

### Phase 2: Expression Vectorization Analysis
**Duration**: 2-3 days
**Priority**: High

#### 2.1 Expression Complexity Analyzer
```go
// internal/sql/planner/vectorization_analyzer.go
type VectorizationAnalyzer struct {
    vectorizableFunctions map[string]bool
    complexityWeights     map[ExpressionType]int
}

type VectorizationAssessment struct {
    IsVectorizable    bool
    ComplexityScore   int
    BlockingFactors   []string
    EstimatedSpeedup  float64
    MemoryRequirement int64
}

func (va *VectorizationAnalyzer) AnalyzeExpression(
    expr Expression,
    inputCardinality int64,
) *VectorizationAssessment
```

#### 2.2 Expression Visitor Enhancement
```go
type VectorizationVisitor struct {
    assessment        *VectorizationAssessment
    currentContext    *ExpressionContext
    supportedOps      map[BinaryOperator]bool
    supportedFuncs    map[string]*FunctionVectorizability
}

// Enhanced visitor methods
func (vv *VectorizationVisitor) VisitBinaryOp(expr *BinaryOp) error {
    // Assess vectorization feasibility
    // Update complexity score
    // Check for blocking factors (UDFs, complex expressions)
}

func (vv *VectorizationVisitor) VisitFunctionCall(expr *FunctionCall) error {
    // Check if function is vectorizable
    // Assess argument complexity
    // Estimate vectorization benefit
}
```

### Phase 3: Cost-Based Vectorization Decisions
**Duration**: 3-4 days
**Priority**: High

#### 3.1 Enhanced Cost Model
```go
// internal/sql/planner/vectorized_cost_model.go
type EnhancedVectorizedCostModel struct {
    *VectorizedCostModel
    
    // Per-operator vectorization parameters
    scanVectorizationThreshold    int64
    filterVectorizationThreshold  int64
    joinVectorizationThreshold    int64
    
    // Expression complexity factors
    arithmeticSpeedup   float64
    comparisonSpeedup   float64
    functionCallPenalty float64
    
    // Memory pressure handling
    memoryPressureThresholds map[MemoryPressureLevel]float64
    vectorizedMemoryOverhead float64
}

func (evcm *EnhancedVectorizedCostModel) EstimateOperatorCost(
    opType OperatorType,
    inputCardinality int64,
    selectivity float64,
    expression Expression,
    memoryAvailable int64,
) *OperatorCostEstimate

type OperatorCostEstimate struct {
    ScalarCost         float64
    VectorizedCost     float64
    MemoryRequirement  int64
    RecommendedMode    ExecutionMode
    ConfidenceLevel    float64
}
```

#### 3.2 Adaptive Decision Framework
```go
type AdaptiveVectorizationDecider struct {
    costModel       *EnhancedVectorizedCostModel
    historyTracker  *OperatorHistoryTracker
    memoryMonitor   *MemoryPressureMonitor
}

func (avd *AdaptiveVectorizationDecider) MakeVectorizationDecision(
    operator LogicalPlan,
    context *PhysicalPlanContext,
) *VectorizationDecision

type VectorizationDecision struct {
    UseVectorized     bool
    BatchSize         int
    FallbackStrategy  FallbackStrategy
    Reasoning         []string
    ConfidenceScore   float64
}
```

### Phase 4: Result Caching Integration
**Duration**: 2-3 days
**Priority**: Medium-High

#### 4.1 Cache-Aware Cost Model
```go
type CacheAwareCostModel struct {
    baseCostModel   *EnhancedVectorizedCostModel
    cacheStats      *ResultCacheStats
    cachePolicy     *CachingPolicy
}

type CachingDecision struct {
    ShouldCache       bool
    EstimatedBenefit  float64
    CacheTTL          time.Duration
    EvictionPriority  int
    Dependencies      []TableDependency
}

func (cacm *CacheAwareCostModel) EvaluateCachingBenefit(
    plan PhysicalPlan,
    executionCost float64,
    accessFrequency float64,
) *CachingDecision
```

#### 4.2 Cache-Integrated Physical Planning
```go
type CacheIntegratedPlanner struct {
    physicalPlanner *PhysicalPlanner
    cacheModel      *CacheAwareCostModel
    resultCache     *ResultCache
}

func (cip *CacheIntegratedPlanner) GeneratePhysicalPlan(
    logical LogicalPlan,
    context *PhysicalPlanContext,
) (PhysicalPlan, error) {
    // Generate physical plan with caching decisions
    // Add cache check operators where beneficial
    // Optimize plan for cache-friendly access patterns
}
```

### Phase 5: Runtime Adaptability
**Duration**: 2-3 days
**Priority**: Medium

#### 5.1 Runtime Feedback System
```go
type RuntimeFeedbackCollector struct {
    operatorPerformance map[OperatorSignature]*PerformanceMetrics
    memoryUsagePatterns map[QuerySignature]*MemoryProfile
    cacheEffectiveness  map[CacheKey]*CachePerformance
}

type PerformanceMetrics struct {
    ActualExecutionTime  time.Duration
    PredictedTime       time.Duration
    MemoryUsed          int64
    RowsProcessed       int64
    VectorizationRatio  float64
}

func (rfc *RuntimeFeedbackCollector) UpdateCostModel(
    model *EnhancedVectorizedCostModel,
) error
```

#### 5.2 Adaptive Query Re-Planning
```go
type AdaptiveQueryExecutor struct {
    planner         *CacheIntegratedPlanner
    feedbackSystem  *RuntimeFeedbackCollector
    replanThreshold float64
}

func (aqe *AdaptiveQueryExecutor) ExecuteWithAdaptation(
    plan PhysicalPlan,
    context *ExecContext,
) (Result, error) {
    // Execute with runtime monitoring
    // Detect when predictions are significantly wrong
    // Trigger re-planning if beneficial
}
```

## Implementation Timeline

### Week 1: Foundation (Phase 1)
- **Day 1-2**: Physical plan framework and base classes
- **Day 3-4**: Physical plan nodes (Scan, Filter, Join)
- **Day 5**: Physical plan generator and cost integration

### Week 2: Intelligence (Phases 2-3)
- **Day 1-2**: Expression vectorization analysis
- **Day 3-4**: Enhanced cost model with operator-specific decisions
- **Day 5**: Adaptive decision framework

### Week 3: Integration (Phases 4-5)
- **Day 1-2**: Result caching integration
- **Day 3-4**: Runtime feedback system
- **Day 5**: Testing and performance validation

## Success Metrics

### Performance Targets
- **20-30% improvement** in mixed workload performance
- **Sub-millisecond planning time** for typical queries
- **90%+ accuracy** in vectorization decisions
- **50%+ cache hit rate** for repeated query patterns

### Quality Metrics
- **Zero performance regressions** on existing TPC-H queries
- **Graceful degradation** under memory pressure
- **Predictable behavior** across different data distributions
- **Comprehensive test coverage** (>95%)

## Risk Mitigation

### Technical Risks
1. **Performance Regression**: Maintain scalar fallback for all operators
2. **Memory Overhead**: Implement aggressive memory monitoring and limits
3. **Complexity**: Use gradual rollout with feature flags
4. **Cost Model Accuracy**: Implement runtime calibration

### Implementation Risks
1. **Timeline Pressure**: Focus on core functionality first, optimize later
2. **Integration Issues**: Comprehensive integration testing at each phase
3. **Maintenance Burden**: Clear documentation and modular design

## Testing Strategy

### Unit Testing
- Individual cost estimation functions
- Expression vectorization analysis
- Physical plan generation logic
- Cache decision algorithms

### Integration Testing
- End-to-end query execution with hybrid plans
- Performance comparison vs existing implementation
- Memory usage validation
- Cache effectiveness measurement

### Performance Testing
- TPC-H benchmark suite validation
- Stress testing with concurrent workloads
- Memory pressure scenarios
- Large dataset scalability

## Configuration and Monitoring

### Configuration Parameters
```json
{
  "query_planner": {
    "enable_hybrid_execution": true,
    "vectorization_threshold": 1000,
    "memory_pressure_threshold": 0.8,
    "cost_model_calibration": "adaptive",
    "cache_integration": {
      "enable_cache_aware_planning": true,
      "cache_benefit_threshold": 2.0,
      "max_cache_memory_ratio": 0.3
    },
    "runtime_adaptation": {
      "enable_feedback": true,
      "replan_threshold": 3.0,
      "calibration_interval": "1h"
    }
  }
}
```

### Monitoring Metrics
- Planning time distribution
- Vectorization decision accuracy
- Cache hit rates by query type
- Memory usage efficiency
- Performance prediction accuracy

This comprehensive plan ensures that vectorized execution and result caching are seamlessly integrated with intelligent, cost-based decision making that adapts to actual workload characteristics and system conditions.