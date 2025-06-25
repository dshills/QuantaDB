package executor

import (
	"fmt"
	"time"
)

// JoinMethod represents different join algorithm implementations
type JoinMethod int

const (
	HashJoinMethod JoinMethod = iota
	NestedLoopJoinMethod
	MergeJoinMethod
	AutoJoinMethod // Let adaptive logic decide
)

func (jm JoinMethod) String() string {
	switch jm {
	case HashJoinMethod:
		return "Hash"
	case NestedLoopJoinMethod:
		return "NestedLoop"
	case MergeJoinMethod:
		return "Merge"
	case AutoJoinMethod:
		return "Auto"
	default:
		return "Unknown"
	}
}

// AdaptiveJoinOperator dynamically selects and switches join algorithms
type AdaptiveJoinOperator struct {
	baseOperator

	// Child operators
	left  Operator
	right Operator

	// Join configuration
	leftKeys  []ExprEvaluator
	rightKeys []ExprEvaluator
	predicate ExprEvaluator
	joinType  JoinType

	// Adaptive components
	adaptiveCtx   *AdaptiveContext
	initialMethod JoinMethod
	currentMethod JoinMethod
	currentImpl   Operator // Current join implementation

	// Runtime statistics
	leftStats  *RuntimeStats
	rightStats *RuntimeStats
	joinStats  *RuntimeStats

	// Adaptation configuration
	switchThreshold    int64         // Row count threshold for considering switch
	evaluationInterval time.Duration // How often to check for adaptation
	lastEvaluation     time.Time

	// Execution state
	phase            JoinPhase
	leftMaterialized []*Row // Materialized left side for switching
	rightIndex       int    // Position in right side
	switchPoint      int64  // Row count where we switched
	hasSwitched      bool
}

// JoinPhase represents the current phase of join execution
type JoinPhase int

const (
	BuildPhase JoinPhase = iota
	ProbePhase
	CompletedPhase
)

// NewAdaptiveJoinOperator creates a new adaptive join operator
func NewAdaptiveJoinOperator(
	left, right Operator,
	leftKeys, rightKeys []ExprEvaluator,
	predicate ExprEvaluator,
	joinType JoinType,
	adaptiveCtx *AdaptiveContext,
) *AdaptiveJoinOperator {
	// Build combined schema
	leftSchema := left.Schema()
	rightSchema := right.Schema()

	columns := make([]Column, 0, len(leftSchema.Columns)+len(rightSchema.Columns))
	columns = append(columns, leftSchema.Columns...)
	columns = append(columns, rightSchema.Columns...)

	schema := &Schema{Columns: columns}

	return &AdaptiveJoinOperator{
		baseOperator: baseOperator{
			schema: schema,
		},
		left:               left,
		right:              right,
		leftKeys:           leftKeys,
		rightKeys:          rightKeys,
		predicate:          predicate,
		joinType:           joinType,
		adaptiveCtx:        adaptiveCtx,
		initialMethod:      AutoJoinMethod,
		currentMethod:      AutoJoinMethod,
		switchThreshold:    10000, // Consider switching after 10K rows
		evaluationInterval: 100 * time.Millisecond,
		phase:              BuildPhase,
		leftMaterialized:   make([]*Row, 0),
	}
}

// Open initializes the adaptive join
func (aj *AdaptiveJoinOperator) Open(ctx *ExecContext) error {
	aj.ctx = ctx

	// Initialize statistics
	aj.leftStats = NewRuntimeStats(1000) // Default estimate
	aj.rightStats = NewRuntimeStats(1000)
	aj.joinStats = NewRuntimeStats(1000)

	// Initialize statistics collection
	aj.initStats(1000)

	// Open child operators
	if err := aj.left.Open(ctx); err != nil {
		return fmt.Errorf("failed to open left child: %w", err)
	}

	if err := aj.right.Open(ctx); err != nil {
		return fmt.Errorf("failed to open right child: %w", err)
	}

	// Choose initial join method
	aj.currentMethod = aj.selectInitialJoinMethod()

	// Create initial join implementation
	impl, err := aj.createJoinImplementation(aj.currentMethod)
	if err != nil {
		return fmt.Errorf("failed to create initial join implementation: %w", err)
	}
	aj.currentImpl = impl

	// Log initial decision
	if aj.adaptiveCtx != nil {
		aj.adaptiveCtx.LogAdaptiveDecision(
			"AdaptiveJoin",
			fmt.Sprintf("InitialMethod=%s", aj.currentMethod),
			"Initial join method selection",
			aj.joinStats,
		)
	}

	return aj.currentImpl.Open(ctx)
}

// selectInitialJoinMethod chooses the best initial join method
func (aj *AdaptiveJoinOperator) selectInitialJoinMethod() JoinMethod {
	// Simple heuristics for initial selection
	// In a real implementation, this would use cost-based optimization

	// If we have join keys, prefer hash join for larger datasets
	if len(aj.leftKeys) > 0 && len(aj.rightKeys) > 0 {
		return HashJoinMethod
	}

	// Fall back to nested loop for simple cases
	return NestedLoopJoinMethod
}

// createJoinImplementation creates a specific join implementation
func (aj *AdaptiveJoinOperator) createJoinImplementation(method JoinMethod) (Operator, error) {
	switch method {
	case HashJoinMethod:
		return NewHashJoinOperator(
			aj.left, aj.right,
			aj.leftKeys, aj.rightKeys,
			aj.predicate, aj.joinType,
		), nil

	case NestedLoopJoinMethod:
		return NewNestedLoopJoinOperator(
			aj.left, aj.right,
			aj.predicate, aj.joinType,
		), nil

	case MergeJoinMethod:
		// For now, fall back to hash join
		// In a complete implementation, we'd have a merge join operator
		return NewHashJoinOperator(
			aj.left, aj.right,
			aj.leftKeys, aj.rightKeys,
			aj.predicate, aj.joinType,
		), nil

	default:
		return nil, fmt.Errorf("unsupported join method: %s", method)
	}
}

// Next returns the next joined row with adaptive behavior
func (aj *AdaptiveJoinOperator) Next() (*Row, error) {
	// Check if we should consider adaptation
	if aj.shouldEvaluateAdaptation() {
		if err := aj.evaluateAdaptation(); err != nil {
			return nil, fmt.Errorf("adaptation evaluation failed: %w", err)
		}
	}

	// Get next row from current implementation
	row, err := aj.currentImpl.Next()
	if err != nil {
		return nil, err
	}

	if row != nil {
		// Update statistics
		aj.joinStats.UpdateCardinality(aj.joinStats.ActualRows + 1)
		aj.recordRow()
	} else {
		// EOF - finalize statistics
		aj.finishStats()
		if aj.ctx != nil && aj.ctx.StatsCollector != nil && aj.stats != nil {
			aj.ctx.StatsCollector(aj, aj.stats)
		}
	}

	return row, nil
}

// shouldEvaluateAdaptation determines if we should check for adaptation
func (aj *AdaptiveJoinOperator) shouldEvaluateAdaptation() bool {
	if aj.adaptiveCtx == nil || !aj.adaptiveCtx.Enabled {
		return false
	}

	// Don't adapt if we've already switched
	if aj.hasSwitched {
		return false
	}

	// Don't adapt too frequently
	if time.Since(aj.lastEvaluation) < aj.evaluationInterval {
		return false
	}

	// Don't adapt until we have enough data
	if aj.joinStats.ActualRows < aj.switchThreshold {
		return false
	}

	return true
}

// evaluateAdaptation checks if we should switch join methods
func (aj *AdaptiveJoinOperator) evaluateAdaptation() error {
	aj.lastEvaluation = time.Now()

	// Collect current runtime statistics
	snapshot := aj.joinStats.GetSnapshot()

	// Check if current performance is poor
	shouldSwitch, newMethod, reason := aj.shouldSwitchJoinMethod(&snapshot)

	if shouldSwitch {
		if err := aj.switchJoinMethod(newMethod, reason); err != nil {
			return fmt.Errorf("failed to switch join method: %w", err)
		}
	}

	return nil
}

// shouldSwitchJoinMethod determines if we should switch join algorithms
func (aj *AdaptiveJoinOperator) shouldSwitchJoinMethod(stats *RuntimeStats) (bool, JoinMethod, string) {
	// Check for memory pressure
	if aj.adaptiveCtx.MaxMemory > 0 && stats.IsMemoryPressure(aj.adaptiveCtx.MaxMemory, 0.9) {
		if aj.currentMethod == HashJoinMethod {
			return true, NestedLoopJoinMethod, "High memory pressure - switch to nested loop"
		}
	}

	// Check for poor cardinality estimates
	cardinalityError := stats.GetCardinalityError()
	if cardinalityError > 2.0 { // 200% error
		switch aj.currentMethod {
		case HashJoinMethod:
			// If hash join is much slower than expected, try nested loop
			return true, NestedLoopJoinMethod, fmt.Sprintf("Poor cardinality estimate (%.1fx error)", cardinalityError)
		case NestedLoopJoinMethod:
			// If nested loop is slower than expected, try hash join
			return true, HashJoinMethod, fmt.Sprintf("Poor cardinality estimate (%.1fx error)", cardinalityError)
		}
	}

	// Check for data skew (would require more sophisticated detection)
	if stats.IsDataSkewed(1.5) && aj.currentMethod == HashJoinMethod {
		return true, NestedLoopJoinMethod, "Data skew detected - switch to nested loop"
	}

	return false, aj.currentMethod, ""
}

// switchJoinMethod performs the actual switch to a new join algorithm
func (aj *AdaptiveJoinOperator) switchJoinMethod(newMethod JoinMethod, reason string) error {
	// Log the decision
	if aj.adaptiveCtx != nil {
		aj.adaptiveCtx.LogAdaptiveDecision(
			"AdaptiveJoin",
			fmt.Sprintf("Switch %s -> %s", aj.currentMethod, newMethod),
			reason,
			aj.joinStats,
		)
	}

	// Close current implementation
	if aj.currentImpl != nil {
		if err := aj.currentImpl.Close(); err != nil {
			return fmt.Errorf("failed to close current join implementation: %w", err)
		}
	}

	// Create new implementation
	newImpl, err := aj.createJoinImplementation(newMethod)
	if err != nil {
		return fmt.Errorf("failed to create new join implementation: %w", err)
	}

	// Initialize new implementation
	if err := newImpl.Open(aj.ctx); err != nil {
		return fmt.Errorf("failed to open new join implementation: %w", err)
	}

	// Update state
	aj.currentMethod = newMethod
	aj.currentImpl = newImpl
	aj.switchPoint = aj.joinStats.ActualRows
	aj.hasSwitched = true

	return nil
}

// Close cleans up the adaptive join
func (aj *AdaptiveJoinOperator) Close() error {
	var err error

	// Close current implementation
	if aj.currentImpl != nil {
		if closeErr := aj.currentImpl.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}

	// Close child operators
	if aj.left != nil {
		if closeErr := aj.left.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}

	if aj.right != nil {
		if closeErr := aj.right.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}

	// Ensure stats are finalized
	aj.finishStats()
	if aj.ctx != nil && aj.ctx.StatsCollector != nil && aj.stats != nil {
		aj.ctx.StatsCollector(aj, aj.stats)
	}

	return err
}

// GetAdaptiveStats returns adaptive-specific statistics
func (aj *AdaptiveJoinOperator) GetAdaptiveStats() map[string]interface{} {
	stats := make(map[string]interface{})

	stats["initial_method"] = aj.initialMethod.String()
	stats["current_method"] = aj.currentMethod.String()
	stats["has_switched"] = aj.hasSwitched
	stats["switch_point"] = aj.switchPoint

	if aj.adaptiveCtx != nil && aj.adaptiveCtx.DecisionLog != nil {
		stats["decision_count"] = aj.adaptiveCtx.DecisionLog.GetDecisionCount()
	}

	return stats
}

// AdaptiveJoinConfig configures adaptive join behavior
type AdaptiveJoinConfig struct {
	// Enable adaptive behavior
	EnableAdaptation bool

	// Threshold for considering method switches
	SwitchThreshold int64

	// How often to evaluate adaptation
	EvaluationInterval time.Duration

	// Memory pressure threshold for triggering switches
	MemoryPressureThreshold float64

	// Cardinality error threshold for triggering switches
	CardinalityErrorThreshold float64
}

// DefaultAdaptiveJoinConfig returns reasonable defaults
func DefaultAdaptiveJoinConfig() *AdaptiveJoinConfig {
	return &AdaptiveJoinConfig{
		EnableAdaptation:          true,
		SwitchThreshold:           10000,
		EvaluationInterval:        100 * time.Millisecond,
		MemoryPressureThreshold:   0.9,
		CardinalityErrorThreshold: 2.0,
	}
}

// Make AdaptiveJoinOperator implement ParallelizableOperator interface
func (aj *AdaptiveJoinOperator) CanParallelize() bool {
	// Adaptive joins can be parallelized, but current implementation focuses on algorithm switching
	return false
}

func (aj *AdaptiveJoinOperator) GetParallelDegree() int {
	return 1
}

func (aj *AdaptiveJoinOperator) CreateParallelInstance(pc *ParallelContext) (Operator, error) {
	// For now, return self - future enhancement could add parallel adaptation
	return aj, nil
}
