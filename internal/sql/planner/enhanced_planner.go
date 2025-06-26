package planner

import (
	"fmt"
	"time"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/parser"
)

// EnhancedPlanner extends BasicPlanner with physical planning capabilities
type EnhancedPlanner struct {
	*BasicPlanner
	physicalPlanner       *PhysicalPlanner
	vectorizationAnalyzer *VectorizationAnalyzer
	config                *EnhancedPlannerConfig
	statsCollector        *PlanningStatsCollector
}

// EnhancedPlannerConfig controls the enhanced planner behavior
type EnhancedPlannerConfig struct {
	EnablePhysicalPlanning   bool
	EnableVectorization      bool
	EnableResultCaching      bool
	EnableAdaptivePlanning   bool
	PhysicalPlannerConfig    *PhysicalPlannerConfig
	CollectPlanningStats     bool
	MaxPlanningTimeMs        int64
	MemoryAvailable          int64
	DefaultVectorizationMode VectorizationMode
}

// DefaultEnhancedPlannerConfig returns sensible defaults
func DefaultEnhancedPlannerConfig() *EnhancedPlannerConfig {
	return &EnhancedPlannerConfig{
		EnablePhysicalPlanning:   true,
		EnableVectorization:      true,
		EnableResultCaching:      true,
		EnableAdaptivePlanning:   true,
		PhysicalPlannerConfig:    DefaultPhysicalPlannerConfig(),
		CollectPlanningStats:     true,
		MaxPlanningTimeMs:        200,                // 200ms max planning time
		MemoryAvailable:          1024 * 1024 * 1024, // 1GB
		DefaultVectorizationMode: VectorizationModeAdaptive,
	}
}

// PlanningStatsCollector tracks planning performance metrics
type PlanningStatsCollector struct {
	TotalPlans              int64
	PhysicalPlans           int64
	VectorizedPlans         int64
	CachedPlans             int64
	AvgPlanningTime         time.Duration
	AvgLogicalPlanningTime  time.Duration
	AvgPhysicalPlanningTime time.Duration
	PlanningErrors          int64
	LastReset               time.Time
}

// NewPlanningStatsCollector creates a new stats collector
func NewPlanningStatsCollector() *PlanningStatsCollector {
	return &PlanningStatsCollector{
		LastReset: time.Now(),
	}
}

// NewEnhancedPlanner creates a new enhanced planner
func NewEnhancedPlanner(catalog catalog.Catalog) *EnhancedPlanner {
	basicPlanner := NewBasicPlannerWithCatalog(catalog)

	// Create cost estimator and vectorized cost model
	costEstimator := NewCostEstimator(catalog)
	vectorizedModel := NewVectorizedCostModel(true, 1024, 1024*1024*1024, 1000)

	// Create physical planner
	physicalPlanner := NewPhysicalPlanner(costEstimator, vectorizedModel, catalog)

	return &EnhancedPlanner{
		BasicPlanner:          basicPlanner,
		physicalPlanner:       physicalPlanner,
		vectorizationAnalyzer: NewVectorizationAnalyzer(),
		config:                DefaultEnhancedPlannerConfig(),
		statsCollector:        NewPlanningStatsCollector(),
	}
}

// SetConfig updates the planner configuration
func (ep *EnhancedPlanner) SetConfig(config *EnhancedPlannerConfig) {
	ep.config = config
	if config.PhysicalPlannerConfig != nil {
		ep.physicalPlanner.SetConfig(config.PhysicalPlannerConfig)
	}
}

// Plan creates an optimized query plan with physical planning
func (ep *EnhancedPlanner) Plan(stmt parser.Statement) (Plan, error) {
	planStart := time.Now()
	defer func() {
		if ep.config.CollectPlanningStats {
			ep.updatePlanningStats(time.Since(planStart))
		}
	}()

	// Phase 1: Build logical plan
	logicalStart := time.Now()
	logical, err := ep.buildLogicalPlan(stmt)
	if err != nil {
		ep.statsCollector.PlanningErrors++
		return nil, fmt.Errorf("failed to build logical plan: %w", err)
	}
	logicalTime := time.Since(logicalStart)

	// Phase 2: Apply logical optimizations
	optimized := ep.optimize(logical)

	// Phase 3: Physical planning (if enabled)
	if ep.config.EnablePhysicalPlanning {
		physicalStart := time.Now()
		physical, err := ep.generatePhysicalPlan(optimized, stmt)
		if err != nil {
			// Fall back to logical plan if physical planning fails
			ep.statsCollector.PlanningErrors++
			return optimized, nil
		}
		physicalTime := time.Since(physicalStart)

		if ep.config.CollectPlanningStats {
			ep.statsCollector.PhysicalPlans++
			ep.statsCollector.AvgPhysicalPlanningTime =
				ep.updateAverage(ep.statsCollector.AvgPhysicalPlanningTime, physicalTime, ep.statsCollector.PhysicalPlans)
		}

		return physical, nil
	}

	if ep.config.CollectPlanningStats {
		ep.statsCollector.AvgLogicalPlanningTime =
			ep.updateAverage(ep.statsCollector.AvgLogicalPlanningTime, logicalTime, ep.statsCollector.TotalPlans)
	}

	return optimized, nil
}

// generatePhysicalPlan converts logical plan to optimized physical plan
func (ep *EnhancedPlanner) generatePhysicalPlan(
	logical LogicalPlan,
	stmt parser.Statement,
) (PhysicalPlan, error) {
	// Create physical planning context
	context := ep.createPhysicalPlanContext(stmt)

	// Generate physical plan
	physical, err := ep.physicalPlanner.GeneratePhysicalPlan(logical, context)
	if err != nil {
		return nil, fmt.Errorf("physical planning failed: %w", err)
	}

	// Collect statistics
	if ep.config.CollectPlanningStats {
		ep.collectPhysicalPlanStats(physical)
	}

	return physical, nil
}

// createPhysicalPlanContext creates context for physical planning
func (ep *EnhancedPlanner) createPhysicalPlanContext(stmt parser.Statement) *PhysicalPlanContext {
	context := &PhysicalPlanContext{
		CostModel:         ep.physicalPlanner.vectorizedModel,
		MemoryAvailable:   ep.config.MemoryAvailable,
		RuntimeStats:      ep.physicalPlanner.runtimeStats,
		TableStats:        make(map[string]*catalog.TableStats),
		IndexStats:        make(map[string]*catalog.IndexStats),
		VectorizationMode: ep.config.DefaultVectorizationMode,
		BatchSize:         ep.config.PhysicalPlannerConfig.DefaultBatchSize,
		EnableCaching:     ep.config.EnableResultCaching,
	}

	// Override vectorization mode if disabled
	if !ep.config.EnableVectorization {
		context.VectorizationMode = VectorizationModeDisabled
	}

	// Collect table and index statistics
	ep.populateStatistics(context, stmt)

	return context
}

// populateStatistics gathers table and index statistics for planning
func (ep *EnhancedPlanner) populateStatistics(context *PhysicalPlanContext, stmt parser.Statement) {
	// Extract table names from the statement
	tableNames := ep.extractTableNames(stmt)

	for _, tableName := range tableNames {
		if table, err := ep.catalog.GetTable("public", tableName); err == nil {
			if table.Stats != nil {
				context.TableStats[tableName] = table.Stats
			}

			// Collect index statistics
			for _, index := range table.Indexes {
				if index.Stats != nil {
					indexKey := fmt.Sprintf("%s.%s", tableName, index.Name)
					context.IndexStats[indexKey] = index.Stats
				}
			}
		}
	}
}

// extractTableNames extracts table names from a statement
func (ep *EnhancedPlanner) extractTableNames(stmt parser.Statement) []string {
	var tableNames []string

	switch s := stmt.(type) {
	case *parser.SelectStmt:
		tableNames = append(tableNames, ep.extractTableNamesFromSelect(s)...)
	case *parser.InsertStmt:
		tableNames = append(tableNames, s.TableName)
	case *parser.UpdateStmt:
		tableNames = append(tableNames, s.TableName)
	case *parser.DeleteStmt:
		tableNames = append(tableNames, s.TableName)
	}

	return tableNames
}

// extractTableNamesFromSelect extracts table names from SELECT statement
func (ep *EnhancedPlanner) extractTableNamesFromSelect(stmt *parser.SelectStmt) []string {
	var tableNames []string

	// Extract from FROM clause
	if stmt.From != nil {
		switch f := stmt.From.(type) {
		case *parser.TableRef:
			tableNames = append(tableNames, f.TableName)
		case *parser.JoinExpr:
			tableNames = append(tableNames, ep.extractTableNamesFromJoin(f)...)
		}
	}

	return tableNames
}

// extractTableNamesFromJoin extracts table names from JOIN expressions
func (ep *EnhancedPlanner) extractTableNamesFromJoin(join *parser.JoinExpr) []string {
	var tableNames []string

	if leftTable, ok := join.Left.(*parser.TableRef); ok {
		tableNames = append(tableNames, leftTable.TableName)
	}

	if rightTable, ok := join.Right.(*parser.TableRef); ok {
		tableNames = append(tableNames, rightTable.TableName)
	}

	return tableNames
}

// collectPhysicalPlanStats collects statistics from the generated physical plan
func (ep *EnhancedPlanner) collectPhysicalPlanStats(plan PhysicalPlan) {
	ep.statsCollector.PhysicalPlans++

	// Count vectorized operators
	if ep.countVectorizedOperators(plan) > 0 {
		ep.statsCollector.VectorizedPlans++
	}

	// Check for caching opportunities
	if ep.hasCachingOpportunities(plan) {
		ep.statsCollector.CachedPlans++
	}
}

// countVectorizedOperators recursively counts vectorized operators in the plan
func (ep *EnhancedPlanner) countVectorizedOperators(plan PhysicalPlan) int {
	count := 0

	if plan.RequiresVectorization() {
		count++
	}

	for _, input := range plan.GetInputs() {
		count += ep.countVectorizedOperators(input)
	}

	return count
}

// hasCachingOpportunities checks if the plan has caching opportunities
func (ep *EnhancedPlanner) hasCachingOpportunities(plan PhysicalPlan) bool {
	// Simple heuristic: plans with aggregates or joins might benefit from caching
	switch plan.GetOperatorType() {
	case OperatorTypeAggregate, OperatorTypeJoin:
		return true
	default:
		// Check inputs recursively
		for _, input := range plan.GetInputs() {
			if ep.hasCachingOpportunities(input) {
				return true
			}
		}
		return false
	}
}

// updatePlanningStats updates overall planning statistics
func (ep *EnhancedPlanner) updatePlanningStats(planningTime time.Duration) {
	ep.statsCollector.TotalPlans++
	ep.statsCollector.AvgPlanningTime =
		ep.updateAverage(ep.statsCollector.AvgPlanningTime, planningTime, ep.statsCollector.TotalPlans)
}

// updateAverage updates a running average
func (ep *EnhancedPlanner) updateAverage(currentAvg time.Duration, newValue time.Duration, count int64) time.Duration {
	if count <= 1 {
		return newValue
	}

	// Calculate new average: ((old_avg * (count-1)) + new_value) / count
	totalNanos := int64(currentAvg)*(count-1) + int64(newValue)
	return time.Duration(totalNanos / count)
}

// GetPlanningStats returns current planning statistics
func (ep *EnhancedPlanner) GetPlanningStats() *PlanningStatsCollector {
	return ep.statsCollector
}

// ResetPlanningStats resets planning statistics
func (ep *EnhancedPlanner) ResetPlanningStats() {
	ep.statsCollector = NewPlanningStatsCollector()
}

// AnalyzeExpression provides vectorization analysis for an expression
func (ep *EnhancedPlanner) AnalyzeExpression(expr Expression, cardinality int64) *VectorizationAssessment {
	return ep.vectorizationAnalyzer.AnalyzeExpression(expr, cardinality)
}

// GetVectorizationCapabilities returns current vectorization capabilities
func (ep *EnhancedPlanner) GetVectorizationCapabilities() map[string]interface{} {
	return map[string]interface{}{
		"supported_operators": ep.vectorizationAnalyzer.GetSupportedOperators(),
		"supported_types":     ep.vectorizationAnalyzer.GetSupportedTypes(),
		"vectorization_mode":  ep.config.DefaultVectorizationMode,
		"batch_size":          ep.config.PhysicalPlannerConfig.DefaultBatchSize,
		"memory_available":    ep.config.MemoryAvailable,
	}
}

// SetVectorizationMode dynamically changes vectorization mode
func (ep *EnhancedPlanner) SetVectorizationMode(mode VectorizationMode) {
	ep.config.DefaultVectorizationMode = mode
}

// SetMemoryLimit dynamically adjusts available memory for planning
func (ep *EnhancedPlanner) SetMemoryLimit(memoryBytes int64) {
	ep.config.MemoryAvailable = memoryBytes
}

// UpdateRuntimeStats updates runtime statistics used for adaptive planning
func (ep *EnhancedPlanner) UpdateRuntimeStats(stats *RuntimeStatistics) {
	ep.physicalPlanner.SetRuntimeStats(stats)
}

// EstimatePlanCost estimates the cost of executing a plan
func (ep *EnhancedPlanner) EstimatePlanCost(plan Plan) (*Cost, error) {
	if physicalPlan, ok := plan.(PhysicalPlan); ok {
		context := ep.createPhysicalPlanContext(nil) // No statement context needed for estimation
		return physicalPlan.EstimateCost(context), nil
	}

	// For logical plans, use basic estimation
	return &Cost{TotalCost: 1000.0}, nil
}

// CreateExplainPlan creates a plan with detailed execution information
func (ep *EnhancedPlanner) CreateExplainPlan(stmt parser.Statement, analyze bool) (Plan, error) {
	// Generate the normal plan
	plan, err := ep.Plan(stmt)
	if err != nil {
		return nil, err
	}

	// If it's a physical plan, we can provide detailed cost information
	if physicalPlan, ok := plan.(PhysicalPlan); ok {
		// Create enhanced explain plan with cost and cardinality information
		return ep.createDetailedExplainPlan(physicalPlan, analyze)
	}

	return plan, nil
}

// createDetailedExplainPlan creates a plan with execution details
func (ep *EnhancedPlanner) createDetailedExplainPlan(plan PhysicalPlan, analyze bool) (Plan, error) {
	// For now, return the plan as-is
	// In a full implementation, this would wrap the plan with explain operators
	// that collect and report execution statistics
	return plan, nil
}

// ValidateConfiguration validates the current planner configuration
func (ep *EnhancedPlanner) ValidateConfiguration() error {
	if ep.config.MaxPlanningTimeMs <= 0 {
		return fmt.Errorf("invalid max planning time: %d", ep.config.MaxPlanningTimeMs)
	}

	if ep.config.MemoryAvailable <= 0 {
		return fmt.Errorf("invalid memory limit: %d", ep.config.MemoryAvailable)
	}

	if ep.config.PhysicalPlannerConfig.DefaultBatchSize <= 0 {
		return fmt.Errorf("invalid batch size: %d", ep.config.PhysicalPlannerConfig.DefaultBatchSize)
	}

	return nil
}
