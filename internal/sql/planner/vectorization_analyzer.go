package planner

import (
	"fmt"
	"strings"

	"github.com/dshills/QuantaDB/internal/sql/types"
)

// VectorizationAnalyzer analyzes expressions for vectorization feasibility
type VectorizationAnalyzer struct {
	vectorizableFunctions map[string]*FunctionVectorizability
	complexityWeights     map[ExpressionType]int
	supportedOperators    map[BinaryOperator]bool
	supportedTypes        map[types.DataType]bool
}

// ExpressionType categorizes expressions for complexity analysis
type ExpressionType int

const (
	ExprTypeSimple ExpressionType = iota
	ExprTypeArithmetic
	ExprTypeComparison
	ExprTypeLogical
	ExprTypeFunction
	ExprTypeAggregate
	ExprTypeSubquery
	ExprTypeCase
	ExprTypeComplex
)

// FunctionVectorizability describes how well a function can be vectorized
type FunctionVectorizability struct {
	IsVectorizable    bool
	ComplexityScore   int
	MemoryMultiplier  float64
	SpeedupFactor     float64
	RequiredTypes     []types.DataType
	BlockingFactors   []string
}

// VectorizationAssessment provides detailed analysis of expression vectorization
type VectorizationAssessment struct {
	IsVectorizable      bool
	ComplexityScore     int
	BlockingFactors     []string
	EstimatedSpeedup    float64
	MemoryRequirement   int64
	SupportedOperators  int
	UnsupportedFeatures []string
	RecommendedAction   VectorizationRecommendation
}

// VectorizationRecommendation suggests the best course of action
type VectorizationRecommendation int

const (
	RecommendVectorized VectorizationRecommendation = iota
	RecommendScalar
	RecommendFallback
	RecommendHybrid
	RecommendAdaptive
)

func (vr VectorizationRecommendation) String() string {
	switch vr {
	case RecommendVectorized:
		return "vectorized"
	case RecommendScalar:
		return "scalar"
	case RecommendFallback:
		return "fallback"
	case RecommendHybrid:
		return "hybrid"
	case RecommendAdaptive:
		return "adaptive"
	default:
		return "unknown"
	}
}

// NewVectorizationAnalyzer creates a new analyzer with default configuration
func NewVectorizationAnalyzer() *VectorizationAnalyzer {
	va := &VectorizationAnalyzer{
		vectorizableFunctions: make(map[string]*FunctionVectorizability),
		complexityWeights:     make(map[ExpressionType]int),
		supportedOperators:    make(map[BinaryOperator]bool),
		supportedTypes:        make(map[types.DataType]bool),
	}
	
	va.initializeDefaults()
	return va
}

// initializeDefaults sets up default vectorization capabilities
func (va *VectorizationAnalyzer) initializeDefaults() {
	// Initialize complexity weights
	va.complexityWeights[ExprTypeSimple] = 1
	va.complexityWeights[ExprTypeArithmetic] = 2
	va.complexityWeights[ExprTypeComparison] = 2
	va.complexityWeights[ExprTypeLogical] = 2
	va.complexityWeights[ExprTypeFunction] = 5
	va.complexityWeights[ExprTypeAggregate] = 8
	va.complexityWeights[ExprTypeSubquery] = 15
	va.complexityWeights[ExprTypeCase] = 4
	va.complexityWeights[ExprTypeComplex] = 10
	
	// Initialize supported operators
	va.supportedOperators[OpAdd] = true
	va.supportedOperators[OpSubtract] = true
	va.supportedOperators[OpMultiply] = true
	va.supportedOperators[OpDivide] = true
	va.supportedOperators[OpEqual] = true
	va.supportedOperators[OpNotEqual] = true
	va.supportedOperators[OpLess] = true
	va.supportedOperators[OpLessEqual] = true
	va.supportedOperators[OpGreater] = true
	va.supportedOperators[OpGreaterEqual] = true
	va.supportedOperators[OpAnd] = true
	va.supportedOperators[OpOr] = true
	
	// Unsupported operators
	va.supportedOperators[OpModulo] = false
	va.supportedOperators[OpConcat] = false
	va.supportedOperators[OpLike] = false
	va.supportedOperators[OpNotLike] = false
	va.supportedOperators[OpIn] = false
	va.supportedOperators[OpNotIn] = false
	
	// Initialize supported types
	va.supportedTypes[types.Integer] = true
	va.supportedTypes[types.BigInt] = true
	va.supportedTypes[types.Float] = true
	va.supportedTypes[types.Double] = true
	va.supportedTypes[types.Boolean] = true
	
	// Partially supported types
	va.supportedTypes[types.Text] = false
	va.supportedTypes[types.Bytea] = false
	va.supportedTypes[types.Date] = false
	va.supportedTypes[types.Timestamp] = false
	
	// Initialize vectorizable functions
	va.initializeVectorizableFunctions()
}

// initializeVectorizableFunctions sets up function vectorization capabilities
func (va *VectorizationAnalyzer) initializeVectorizableFunctions() {
	// Mathematical functions
	va.vectorizableFunctions["ABS"] = &FunctionVectorizability{
		IsVectorizable:   true,
		ComplexityScore:  2,
		MemoryMultiplier: 1.0,
		SpeedupFactor:    1.5,
		RequiredTypes:    []types.DataType{types.Integer, types.BigInt, types.Float, types.Double},
	}
	
	va.vectorizableFunctions["SQRT"] = &FunctionVectorizability{
		IsVectorizable:   true,
		ComplexityScore:  3,
		MemoryMultiplier: 1.0,
		SpeedupFactor:    1.3,
		RequiredTypes:    []types.DataType{types.Float, types.Double},
	}
	
	va.vectorizableFunctions["FLOOR"] = &FunctionVectorizability{
		IsVectorizable:   true,
		ComplexityScore:  2,
		MemoryMultiplier: 1.0,
		SpeedupFactor:    1.4,
		RequiredTypes:    []types.DataType{types.Float, types.Double},
	}
	
	// String functions (not vectorizable in current implementation)
	va.vectorizableFunctions["SUBSTRING"] = &FunctionVectorizability{
		IsVectorizable:   false,
		ComplexityScore:  6,
		MemoryMultiplier: 2.0,
		SpeedupFactor:    0.8,
		BlockingFactors:  []string{"string_manipulation", "variable_length_output"},
	}
	
	va.vectorizableFunctions["LENGTH"] = &FunctionVectorizability{
		IsVectorizable:   false,
		ComplexityScore:  3,
		MemoryMultiplier: 1.0,
		SpeedupFactor:    0.9,
		BlockingFactors:  []string{"string_processing"},
	}
	
	// Date/time functions (not vectorizable in current implementation)
	va.vectorizableFunctions["EXTRACT"] = &FunctionVectorizability{
		IsVectorizable:   false,
		ComplexityScore:  4,
		MemoryMultiplier: 1.0,
		SpeedupFactor:    0.9,
		BlockingFactors:  []string{"date_time_processing"},
	}
	
	// Aggregate functions (require special handling)
	va.vectorizableFunctions["SUM"] = &FunctionVectorizability{
		IsVectorizable:   true,
		ComplexityScore:  3,
		MemoryMultiplier: 1.0,
		SpeedupFactor:    2.0,
		RequiredTypes:    []types.DataType{types.Integer, types.BigInt, types.Float, types.Double},
		BlockingFactors:  []string{"requires_aggregate_context"},
	}
	
	va.vectorizableFunctions["COUNT"] = &FunctionVectorizability{
		IsVectorizable:   true,
		ComplexityScore:  2,
		MemoryMultiplier: 1.0,
		SpeedupFactor:    2.5,
		BlockingFactors:  []string{"requires_aggregate_context"},
	}
}

// AnalyzeExpression performs comprehensive vectorization analysis
func (va *VectorizationAnalyzer) AnalyzeExpression(
	expr Expression,
	inputCardinality int64,
) *VectorizationAssessment {
	assessment := &VectorizationAssessment{
		IsVectorizable:      true,
		ComplexityScore:     0,
		BlockingFactors:     []string{},
		EstimatedSpeedup:    1.0,
		MemoryRequirement:   0,
		SupportedOperators:  0,
		UnsupportedFeatures: []string{},
	}
	
	if expr == nil {
		assessment.RecommendedAction = RecommendScalar
		return assessment
	}
	
	// Analyze the expression tree
	va.analyzeExpressionRecursive(expr, assessment)
	
	// Calculate final metrics
	va.calculateFinalAssessment(assessment, inputCardinality)
	
	return assessment
}

// analyzeExpressionRecursive recursively analyzes expression components
func (va *VectorizationAnalyzer) analyzeExpressionRecursive(
	expr Expression,
	assessment *VectorizationAssessment,
) {
	switch e := expr.(type) {
	case *BinaryOp:
		va.analyzeBinaryOp(e, assessment)
		
	case *UnaryOp:
		va.analyzeUnaryOp(e, assessment)
		
	case *FunctionCall:
		va.analyzeFunctionCall(e, assessment)
		
	case *AggregateExpr:
		va.analyzeAggregateExpr(e, assessment)
		
	case *SubqueryExpr:
		va.analyzeSubqueryExpr(e, assessment)
		
	case *CaseExpr:
		va.analyzeCaseExpr(e, assessment)
		
	case *ColumnRef:
		va.analyzeColumnRef(e, assessment)
		
	case *Literal:
		va.analyzeLiteral(e, assessment)
		
	default:
		assessment.UnsupportedFeatures = append(assessment.UnsupportedFeatures, 
			fmt.Sprintf("unknown_expression_type_%T", expr))
		assessment.IsVectorizable = false
	}
}

// analyzeBinaryOp analyzes binary operations
func (va *VectorizationAnalyzer) analyzeBinaryOp(op *BinaryOp, assessment *VectorizationAssessment) {
	// Check if operator is supported
	if supported, exists := va.supportedOperators[op.Operator]; !exists || !supported {
		assessment.BlockingFactors = append(assessment.BlockingFactors, 
			fmt.Sprintf("unsupported_operator_%s", op.Operator.String()))
		assessment.IsVectorizable = false
	} else {
		assessment.SupportedOperators++
		
		// Estimate speedup based on operator type
		switch op.Operator {
		case OpAdd, OpSubtract, OpMultiply:
			assessment.EstimatedSpeedup *= 1.25
		case OpEqual, OpNotEqual, OpLess, OpGreater, OpLessEqual, OpGreaterEqual:
			assessment.EstimatedSpeedup *= 1.22
		case OpAnd, OpOr:
			assessment.EstimatedSpeedup *= 1.15
		case OpDivide:
			assessment.EstimatedSpeedup *= 1.10 // Slightly less due to division complexity
		}
	}
	
	// Add complexity
	if isArithmeticOp(op.Operator) {
		assessment.ComplexityScore += va.complexityWeights[ExprTypeArithmetic]
	} else if isComparisonOp(op.Operator) {
		assessment.ComplexityScore += va.complexityWeights[ExprTypeComparison]
	} else if isLogicalOp(op.Operator) {
		assessment.ComplexityScore += va.complexityWeights[ExprTypeLogical]
	}
	
	// Recursively analyze operands
	va.analyzeExpressionRecursive(op.Left, assessment)
	va.analyzeExpressionRecursive(op.Right, assessment)
}

// analyzeUnaryOp analyzes unary operations
func (va *VectorizationAnalyzer) analyzeUnaryOp(op *UnaryOp, assessment *VectorizationAssessment) {
	switch op.Operator {
	case OpNot:
		// NOT is vectorizable for boolean operations
		assessment.SupportedOperators++
		assessment.EstimatedSpeedup *= 1.15
	case OpNegate:
		// Negation is vectorizable for numeric types
		assessment.SupportedOperators++
		assessment.EstimatedSpeedup *= 1.20
	case OpIsNull, OpIsNotNull:
		// NULL checks are vectorizable
		assessment.SupportedOperators++
		assessment.EstimatedSpeedup *= 1.30
	default:
		assessment.BlockingFactors = append(assessment.BlockingFactors, 
			fmt.Sprintf("unsupported_unary_operator_%s", op.Operator.String()))
		assessment.IsVectorizable = false
	}
	
	assessment.ComplexityScore += va.complexityWeights[ExprTypeSimple]
	va.analyzeExpressionRecursive(op.Expr, assessment)
}

// analyzeFunctionCall analyzes function calls
func (va *VectorizationAnalyzer) analyzeFunctionCall(fn *FunctionCall, assessment *VectorizationAssessment) {
	funcName := strings.ToUpper(fn.Name)
	
	if vectorizability, exists := va.vectorizableFunctions[funcName]; exists {
		if vectorizability.IsVectorizable {
			assessment.EstimatedSpeedup *= vectorizability.SpeedupFactor
			assessment.MemoryRequirement += int64(float64(assessment.MemoryRequirement) * vectorizability.MemoryMultiplier)
		} else {
			assessment.BlockingFactors = append(assessment.BlockingFactors, 
				fmt.Sprintf("non_vectorizable_function_%s", funcName))
			assessment.BlockingFactors = append(assessment.BlockingFactors, vectorizability.BlockingFactors...)
			assessment.IsVectorizable = false
		}
		assessment.ComplexityScore += vectorizability.ComplexityScore
	} else {
		// Unknown function - assume not vectorizable
		assessment.BlockingFactors = append(assessment.BlockingFactors, 
			fmt.Sprintf("unknown_function_%s", funcName))
		assessment.IsVectorizable = false
		assessment.ComplexityScore += va.complexityWeights[ExprTypeFunction]
	}
	
	// Analyze function arguments
	for _, arg := range fn.Args {
		va.analyzeExpressionRecursive(arg, assessment)
	}
}

// analyzeAggregateExpr analyzes aggregate expressions
func (va *VectorizationAnalyzer) analyzeAggregateExpr(agg *AggregateExpr, assessment *VectorizationAssessment) {
	funcName := agg.Function.String()
	
	if vectorizability, exists := va.vectorizableFunctions[funcName]; exists {
		if vectorizability.IsVectorizable {
			// Aggregates can be vectorized but require special context
			assessment.EstimatedSpeedup *= vectorizability.SpeedupFactor
			assessment.BlockingFactors = append(assessment.BlockingFactors, "requires_aggregate_context")
		} else {
			assessment.BlockingFactors = append(assessment.BlockingFactors, 
				fmt.Sprintf("non_vectorizable_aggregate_%s", funcName))
			assessment.IsVectorizable = false
		}
	} else {
		assessment.BlockingFactors = append(assessment.BlockingFactors, 
			fmt.Sprintf("unknown_aggregate_%s", funcName))
		assessment.IsVectorizable = false
	}
	
	assessment.ComplexityScore += va.complexityWeights[ExprTypeAggregate]
	
	// Analyze aggregate arguments
	for _, arg := range agg.Args {
		va.analyzeExpressionRecursive(arg, assessment)
	}
}

// analyzeSubqueryExpr analyzes subquery expressions
func (va *VectorizationAnalyzer) analyzeSubqueryExpr(sub *SubqueryExpr, assessment *VectorizationAssessment) {
	// Subqueries block vectorization
	assessment.BlockingFactors = append(assessment.BlockingFactors, "contains_subquery")
	assessment.IsVectorizable = false
	assessment.ComplexityScore += va.complexityWeights[ExprTypeSubquery]
}

// analyzeCaseExpr analyzes CASE expressions
func (va *VectorizationAnalyzer) analyzeCaseExpr(caseExpr *CaseExpr, assessment *VectorizationAssessment) {
	// CASE expressions can be vectorized but with reduced efficiency
	assessment.ComplexityScore += va.complexityWeights[ExprTypeCase]
	assessment.EstimatedSpeedup *= 0.9 // Slight penalty for branching
	
	// Analyze all WHEN conditions and results
	for _, when := range caseExpr.WhenList {
		va.analyzeExpressionRecursive(when.Condition, assessment)
		va.analyzeExpressionRecursive(when.Result, assessment)
	}
	
	if caseExpr.Else != nil {
		va.analyzeExpressionRecursive(caseExpr.Else, assessment)
	}
	
	if caseExpr.Expr != nil {
		va.analyzeExpressionRecursive(caseExpr.Expr, assessment)
	}
}

// analyzeColumnRef analyzes column references
func (va *VectorizationAnalyzer) analyzeColumnRef(col *ColumnRef, assessment *VectorizationAssessment) {
	// Column references are highly vectorizable
	assessment.ComplexityScore += va.complexityWeights[ExprTypeSimple]
	assessment.EstimatedSpeedup *= 1.1
	
	// Check if column type is supported
	if supported, exists := va.supportedTypes[col.ColumnType]; exists && !supported {
		assessment.BlockingFactors = append(assessment.BlockingFactors, 
			"unsupported_column_type")
		assessment.IsVectorizable = false
	}
}

// analyzeLiteral analyzes literal values
func (va *VectorizationAnalyzer) analyzeLiteral(lit *Literal, assessment *VectorizationAssessment) {
	// Literals are highly vectorizable (broadcast to all rows)
	assessment.ComplexityScore += va.complexityWeights[ExprTypeSimple]
	assessment.EstimatedSpeedup *= 1.05
	
	// Check if literal type is supported
	if supported, exists := va.supportedTypes[lit.Type]; exists && !supported {
		assessment.BlockingFactors = append(assessment.BlockingFactors, 
			"unsupported_literal_type")
		assessment.IsVectorizable = false
	}
}

// calculateFinalAssessment computes final metrics and recommendations
func (va *VectorizationAnalyzer) calculateFinalAssessment(
	assessment *VectorizationAssessment,
	inputCardinality int64,
) {
	// Calculate memory requirement based on cardinality
	baseMemoryPerRow := int64(64) // Estimated bytes per row for vectorized processing
	assessment.MemoryRequirement += inputCardinality * baseMemoryPerRow
	
	// Adjust speedup based on complexity
	if assessment.ComplexityScore > 10 {
		assessment.EstimatedSpeedup *= 0.8 // High complexity penalty
	} else if assessment.ComplexityScore > 5 {
		assessment.EstimatedSpeedup *= 0.9 // Medium complexity penalty
	}
	
	// Apply cardinality scaling factor
	if inputCardinality > 10000 {
		assessment.EstimatedSpeedup *= 1.1 // Bonus for large datasets
	} else if inputCardinality < 1000 {
		assessment.EstimatedSpeedup *= 0.9 // Penalty for small datasets
	}
	
	// Determine final recommendation
	if !assessment.IsVectorizable {
		if len(assessment.BlockingFactors) == 1 && 
		   strings.Contains(assessment.BlockingFactors[0], "requires_aggregate_context") {
			assessment.RecommendedAction = RecommendFallback
		} else {
			assessment.RecommendedAction = RecommendScalar
		}
	} else if assessment.EstimatedSpeedup > 1.5 {
		assessment.RecommendedAction = RecommendVectorized
	} else if assessment.EstimatedSpeedup > 1.2 {
		assessment.RecommendedAction = RecommendAdaptive
	} else if assessment.ComplexityScore > 8 {
		assessment.RecommendedAction = RecommendHybrid
	} else {
		assessment.RecommendedAction = RecommendScalar
	}
}

// Helper functions for operator classification
func isArithmeticOp(op BinaryOperator) bool {
	return op == OpAdd || op == OpSubtract || op == OpMultiply || op == OpDivide || op == OpModulo
}

func isComparisonOp(op BinaryOperator) bool {
	return op == OpEqual || op == OpNotEqual || op == OpLess || op == OpLessEqual ||
		   op == OpGreater || op == OpGreaterEqual
}

func isLogicalOp(op BinaryOperator) bool {
	return op == OpAnd || op == OpOr
}

// GetSupportedOperators returns a list of operators that support vectorization
func (va *VectorizationAnalyzer) GetSupportedOperators() []BinaryOperator {
	var supported []BinaryOperator
	for op, isSupported := range va.supportedOperators {
		if isSupported {
			supported = append(supported, op)
		}
	}
	return supported
}

// GetSupportedTypes returns a list of data types that support vectorization
func (va *VectorizationAnalyzer) GetSupportedTypes() []types.DataType {
	var supported []types.DataType
	for dataType, isSupported := range va.supportedTypes {
		if isSupported {
			supported = append(supported, dataType)
		}
	}
	return supported
}

// AddVectorizableFunction allows adding custom vectorizable functions
func (va *VectorizationAnalyzer) AddVectorizableFunction(name string, vectorizability *FunctionVectorizability) {
	va.vectorizableFunctions[strings.ToUpper(name)] = vectorizability
}

// SetComplexityWeight allows customizing complexity weights
func (va *VectorizationAnalyzer) SetComplexityWeight(exprType ExpressionType, weight int) {
	va.complexityWeights[exprType] = weight
}