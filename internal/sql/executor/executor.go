package executor

import (
	"fmt"
	"strings"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/engine"
	"github.com/dshills/QuantaDB/internal/index"
	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/sql/types"
	"github.com/dshills/QuantaDB/internal/txn"
)

// Executor executes query plans.
type Executor interface {
	Execute(plan planner.Plan, ctx *ExecContext) (Result, error)
}

// Result represents the result of query execution.
type Result interface {
	// Next returns the next row or nil when done.
	Next() (*Row, error)
	// Close releases resources.
	Close() error
	// Schema returns the result schema.
	Schema() *Schema
}

// ExecContext provides context for query execution.
type ExecContext struct {
	// Catalog for metadata lookups
	Catalog catalog.Catalog
	// Storage engine for data access
	Engine engine.Engine
	// Transaction manager for MVCC transactions
	TxnManager *txn.Manager
	// Current transaction (optional)
	Txn *txn.MvccTransaction
	// Snapshot timestamp for consistent reads
	SnapshotTS int64
	// Isolation level
	IsolationLevel txn.IsolationLevel
	// Parameters for prepared statements
	Params []types.Value
	// Statistics collector
	Stats *ExecStats
	// Prepared statements for SQL-level PREPARE/EXECUTE
	PreparedStatements map[string]*PreparedStatement
	// Planner for re-planning queries
	Planner planner.Planner
	// Constraint validator
	ConstraintValidator interface {
		ValidateInsert(table *catalog.Table, row *Row) error
		ValidateUpdate(table *catalog.Table, oldRow, newRow *Row) error
		ValidateDelete(table *catalog.Table, row *Row) error
	}
}

// Row represents a row of data.
type Row struct {
	Values []types.Value
}

// Schema represents the schema of a result set.
type Schema struct {
	Columns []Column
}

// Column represents a column in a schema.
type Column struct {
	Name     string
	Type     types.DataType
	Nullable bool
}

// ExecStats collects execution statistics.
type ExecStats struct {
	RowsRead       int64
	RowsReturned   int64
	BytesRead      int64
	IndexOnlyScans int64
}

// PreparedStatement represents a prepared SQL statement for SQL-level PREPARE/EXECUTE.
// This is different from the protocol-level prepared statements in the sql package.
type PreparedStatement struct {
	Name       string
	ParamTypes []types.DataType
	Query      parser.Statement
}

// BasicExecutor is a basic query executor.
type BasicExecutor struct {
	catalog  catalog.Catalog
	engine   engine.Engine
	storage  StorageBackend
	indexMgr interface{} // Will be set to *index.Manager when available
}

// NewBasicExecutor creates a new basic executor.
func NewBasicExecutor(catalog catalog.Catalog, engine engine.Engine) *BasicExecutor {
	return &BasicExecutor{
		catalog:  catalog,
		engine:   engine,
		storage:  nil, // Will be set later with SetStorageBackend
		indexMgr: nil, // Will be set later with SetIndexManager
	}
}

// SetStorageBackend sets the storage backend for the executor
func (e *BasicExecutor) SetStorageBackend(storage StorageBackend) {
	e.storage = storage
}

// SetIndexManager sets the index manager for the executor
func (e *BasicExecutor) SetIndexManager(indexMgr interface{}) {
	e.indexMgr = indexMgr
}

// Execute executes a query plan.
func (e *BasicExecutor) Execute(plan planner.Plan, ctx *ExecContext) (Result, error) {
	// Set default context values if not provided
	if ctx == nil {
		ctx = &ExecContext{
			Catalog: e.catalog,
			Engine:  e.engine,
			Stats:   &ExecStats{},
		}
	}

	// Convert logical plan to physical operators
	operator, err := e.buildOperator(plan, ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to build operator: %w", err)
	}

	// Open the operator
	if err := operator.Open(ctx); err != nil {
		return nil, fmt.Errorf("failed to open operator: %w", err)
	}

	// Return a result wrapper
	// Preserve operator schema if plan schema conversion fails
	schema := convertSchema(plan.Schema())
	if schema == nil {
		schema = operator.Schema()
	}

	// Special handling for aggregate operators: prefer their schema over plan schema
	// because they have correctly resolved column names
	if _, isAggregate := operator.(*AggregateOperator); isAggregate {
		schema = operator.Schema()
	}

	return &operatorResult{
		operator: operator,
		schema:   schema,
	}, nil
}

// buildOperator converts a plan node to an operator.
func (e *BasicExecutor) buildOperator(plan planner.Plan, ctx *ExecContext) (Operator, error) {
	switch p := plan.(type) {
	case *planner.LogicalScan:
		return e.buildScanOperator(p, ctx)
	case *planner.LogicalFilter:
		return e.buildFilterOperator(p, ctx)
	case *planner.LogicalProject:
		return e.buildProjectOperator(p, ctx)
	case *planner.LogicalSort:
		return e.buildSortOperator(p, ctx)
	case *planner.LogicalLimit:
		return e.buildLimitOperator(p, ctx)
	case *planner.LogicalJoin:
		return e.buildJoinOperator(p, ctx)
	case *planner.LogicalAggregate:
		return e.buildAggregateOperator(p, ctx)
	case *planner.LogicalCreateTable:
		return e.buildCreateTableOperator(p, ctx)
	case *planner.LogicalInsert:
		return e.buildInsertOperator(p, ctx)
	case *planner.LogicalUpdate:
		return e.buildUpdateOperator(p, ctx)
	case *planner.LogicalDelete:
		return e.buildDeleteOperator(p, ctx)
	case *planner.LogicalCreateIndex:
		return e.buildCreateIndexOperator(p, ctx)
	case *planner.LogicalDropTable:
		return e.buildDropTableOperator(p, ctx)
	case *planner.LogicalAlterTableAddColumn:
		return e.buildAlterTableAddColumnOperator(p, ctx)
	case *planner.LogicalAlterTableDropColumn:
		return e.buildAlterTableDropColumnOperator(p, ctx)
	case *planner.LogicalDropIndex:
		return e.buildDropIndexOperator(p, ctx)
	case *planner.LogicalAnalyze:
		return e.buildAnalyzeOperator(p, ctx)
	case *planner.LogicalVacuum:
		return e.buildVacuumOperator(p, ctx)
	case *planner.IndexScan:
		return e.buildIndexScanOperator(p, ctx)
	case *planner.CompositeIndexScan:
		return e.buildCompositeIndexScanOperator(p, ctx)
	case *planner.IndexOnlyScan:
		return e.buildIndexOnlyScanOperator(p, ctx)
	case *planner.LogicalValues:
		return e.buildValuesOperator(p, ctx)
	case *planner.BitmapIndexScan:
		return e.buildBitmapIndexScanOperator(p, ctx)
	case *planner.BitmapAnd:
		return e.buildBitmapAndOperator(p, ctx)
	case *planner.BitmapOr:
		return e.buildBitmapOrOperator(p, ctx)
	case *planner.BitmapHeapScan:
		return e.buildBitmapHeapScanOperator(p, ctx)
	case *planner.LogicalCopy:
		return e.buildCopyOperator(p, ctx)
	case *planner.LogicalPrepare:
		return e.buildPrepareOperator(p, ctx)
	case *planner.LogicalExecute:
		return e.buildExecuteOperator(p, ctx)
	case *planner.LogicalDeallocate:
		return e.buildDeallocateOperator(p, ctx)
	default:
		return nil, fmt.Errorf("unsupported plan node: %T", plan)
	}
}

// buildScanOperator builds a scan operator.
func (e *BasicExecutor) buildScanOperator(plan *planner.LogicalScan, ctx *ExecContext) (Operator, error) {
	// Get table from catalog
	table, err := ctx.Catalog.GetTable("public", plan.TableName)
	if err != nil {
		return nil, fmt.Errorf("table not found: %w", err)
	}

	// Use storage-backed scan if available
	if e.storage != nil {
		return NewStorageScanOperator(table, e.storage), nil
	}

	// Storage backend is required for MVCC compliance
	return nil, fmt.Errorf("storage backend not configured - non-MVCC scans are deprecated")
}

// getTableFromCatalog tries to get a table from catalog using multiple schemas
func (e *BasicExecutor) getTableFromCatalog(ctx *ExecContext, tableName string) (*catalog.Table, error) {
	// Try "public" schema first, then "test", then empty
	table, err := ctx.Catalog.GetTable("public", tableName)
	if err != nil {
		table, err = ctx.Catalog.GetTable("test", tableName)
		if err != nil {
			table, err = ctx.Catalog.GetTable("", tableName)
			if err != nil {
				return nil, fmt.Errorf("table not found: %w", err)
			}
		}
	}
	return table, nil
}

// buildIndexScanOperator builds an index scan operator.
func (e *BasicExecutor) buildIndexScanOperator(plan *planner.IndexScan, ctx *ExecContext) (Operator, error) {
	if e.storage == nil {
		return nil, fmt.Errorf("storage backend not configured for index scan")
	}

	if e.indexMgr == nil {
		return nil, fmt.Errorf("index manager not configured for index scan")
	}

	// Type assert to get the actual index.Manager
	indexMgr, ok := e.indexMgr.(*index.Manager)
	if !ok {
		return nil, fmt.Errorf("invalid index manager type")
	}

	// Get table from catalog
	table, err := e.getTableFromCatalog(ctx, plan.TableName)
	if err != nil {
		return nil, err
	}

	return NewIndexScanOperatorWithPredicates(table, plan.Index, indexMgr, e.storage, plan.StartKey, plan.EndKey, plan.PushedPredicates), nil
}

// buildCompositeIndexScanOperator builds a composite index scan operator.
func (e *BasicExecutor) buildCompositeIndexScanOperator(plan *planner.CompositeIndexScan, ctx *ExecContext) (Operator, error) {
	if e.storage == nil {
		return nil, fmt.Errorf("storage backend not configured for composite index scan")
	}

	if e.indexMgr == nil {
		return nil, fmt.Errorf("index manager not configured for composite index scan")
	}

	// Type assert to get the actual index.Manager
	indexMgr, ok := e.indexMgr.(*index.Manager)
	if !ok {
		return nil, fmt.Errorf("invalid index manager type")
	}

	// Get table from catalog
	table, err := e.getTableFromCatalog(ctx, plan.TableName)
	if err != nil {
		return nil, err
	}

	// Use the new constructor that supports pushed predicates
	if plan.PushedPredicates != nil {
		return NewCompositeIndexScanOperatorWithPredicates(
			table, plan.Index, indexMgr, e.storage,
			plan.StartValues, plan.EndValues, plan.PushedPredicates,
		), nil
	}

	return NewCompositeIndexScanOperator(table, plan.Index, indexMgr, e.storage, plan.StartValues, plan.EndValues), nil
}

// buildIndexOnlyScanOperator builds an index-only scan operator.
func (e *BasicExecutor) buildIndexOnlyScanOperator(plan *planner.IndexOnlyScan, ctx *ExecContext) (Operator, error) {
	if e.indexMgr == nil {
		return nil, fmt.Errorf("index manager not configured for index-only scan")
	}

	// Type assert to get the actual index.Manager
	indexMgr, ok := e.indexMgr.(*index.Manager)
	if !ok {
		return nil, fmt.Errorf("invalid index manager type")
	}

	// Get table from catalog - try multiple schemas
	var table *catalog.Table
	var err error

	table, err = ctx.Catalog.GetTable("public", plan.TableName)
	if err != nil {
		table, err = ctx.Catalog.GetTable("test", plan.TableName)
		if err != nil {
			table, err = ctx.Catalog.GetTable("", plan.TableName)
			if err != nil {
				return nil, fmt.Errorf("table not found: %w", err)
			}
		}
	}

	return NewIndexOnlyScanOperator(table, plan.Index, indexMgr, plan.StartValues, plan.EndValues, plan.ProjectedColumns), nil
}

// buildFilterOperator builds a filter operator.
func (e *BasicExecutor) buildFilterOperator(plan *planner.LogicalFilter, ctx *ExecContext) (Operator, error) {
	// Build child operator
	child, err := e.buildOperator(plan.Children()[0], ctx)
	if err != nil {
		return nil, err
	}

	// Build predicate evaluator
	predicate, err := buildExprEvaluator(plan.Predicate)
	if err != nil {
		return nil, fmt.Errorf("failed to build predicate: %w", err)
	}

	return NewFilterOperator(child, predicate), nil
}

// buildProjectOperator builds a projection operator.
func (e *BasicExecutor) buildProjectOperator(plan *planner.LogicalProject, ctx *ExecContext) (Operator, error) {
	// Build child operator
	child, err := e.buildOperator(plan.Children()[0], ctx)
	if err != nil {
		return nil, err
	}

	// Check if this is a star projection
	if len(plan.Projections) == 1 {
		if _, isStar := plan.Projections[0].(*planner.Star); isStar {
			// For star projections, return the child operator directly
			// This preserves the input schema
			return child, nil
		}
	}

	// Build projection evaluators
	projections := make([]ExprEvaluator, len(plan.Projections))
	childSchema := child.Schema()

	// Special handling for aggregate children: remap group_X column references
	// to actual column names from the aggregate schema
	var remappedProjections []planner.Expression
	if _, isAggregate := child.(*AggregateOperator); isAggregate {
		remappedProjections = make([]planner.Expression, len(plan.Projections))
		for i, expr := range plan.Projections {
			remappedExpr := remapGroupColumns(expr, childSchema)
			remappedProjections[i] = remappedExpr
		}
	} else {
		remappedProjections = plan.Projections
	}

	for i, expr := range remappedProjections {
		eval, err := buildExprEvaluatorWithExecutor(expr, childSchema, e)
		if err != nil {
			return nil, fmt.Errorf("failed to build projection %d: %w", i, err)
		}
		projections[i] = eval
	}

	// Build output schema
	schema := convertSchema(plan.Schema())
	if schema == nil {
		// If no schema from plan, use child schema
		schema = child.Schema()
	}

	return NewProjectOperator(child, projections, schema), nil
}

// buildSortOperator builds a sort operator.
func (e *BasicExecutor) buildSortOperator(plan *planner.LogicalSort, ctx *ExecContext) (Operator, error) {
	// Build child operator
	child, err := e.buildOperator(plan.Children()[0], ctx)
	if err != nil {
		return nil, err
	}

	// Build ORDER BY expressions
	orderBy := make([]OrderByExpr, len(plan.OrderBy))
	childSchema := child.Schema()
	for i, orderExpr := range plan.OrderBy {
		eval, err := buildExprEvaluatorWithExecutor(orderExpr.Expr, childSchema, e)
		if err != nil {
			return nil, fmt.Errorf("failed to build ORDER BY expression %d: %w", i, err)
		}
		orderBy[i] = OrderByExpr{
			Expr:  eval,
			Order: orderExpr.Order,
		}
	}

	return NewSortOperator(child, orderBy), nil
}

// buildLimitOperator builds a limit operator.
func (e *BasicExecutor) buildLimitOperator(plan *planner.LogicalLimit, ctx *ExecContext) (Operator, error) {
	// Build child operator
	child, err := e.buildOperator(plan.Children()[0], ctx)
	if err != nil {
		return nil, err
	}

	return NewLimitOperator(child, plan.Limit, plan.Offset), nil
}

// buildJoinOperator builds a join operator.
func (e *BasicExecutor) buildJoinOperator(plan *planner.LogicalJoin, ctx *ExecContext) (Operator, error) {
	if len(plan.Children()) != 2 {
		return nil, fmt.Errorf("join must have exactly 2 children, got %d", len(plan.Children()))
	}

	// Build left and right operators
	left, err := e.buildOperator(plan.Children()[0], ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to build left child: %w", err)
	}

	right, err := e.buildOperator(plan.Children()[1], ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to build right child: %w", err)
	}

	// Build join predicate
	var predicate ExprEvaluator
	if plan.Condition != nil {
		// For join predicates, we need the combined schema
		leftSchema := left.Schema()
		rightSchema := right.Schema()
		combinedSchema := &Schema{
			Columns: make([]Column, 0, len(leftSchema.Columns)+len(rightSchema.Columns)),
		}
		combinedSchema.Columns = append(combinedSchema.Columns, leftSchema.Columns...)
		combinedSchema.Columns = append(combinedSchema.Columns, rightSchema.Columns...)

		predicate, err = buildExprEvaluatorWithExecutor(plan.Condition, combinedSchema, e)
		if err != nil {
			return nil, fmt.Errorf("failed to build join predicate: %w", err)
		}
	}

	// Map planner join type to executor join type
	var joinType JoinType
	var isSemiAnti bool
	switch plan.JoinType {
	case planner.InnerJoin:
		joinType = InnerJoin
	case planner.LeftJoin:
		joinType = LeftJoin
	case planner.RightJoin:
		joinType = RightJoin
	case planner.FullJoin:
		joinType = FullJoin
	case planner.CrossJoin:
		joinType = InnerJoin // Cross join is inner join without predicate
	case planner.SemiJoin:
		isSemiAnti = true
	case planner.AntiJoin:
		isSemiAnti = true
	default:
		return nil, fmt.Errorf("unsupported join type: %v", plan.JoinType)
	}

	// Handle semi/anti joins separately
	if isSemiAnti {
		return e.buildSemiAntiJoin(left, right, plan, predicate)
	}

	// Choose join algorithm based on join condition and available indexes
	return e.chooseJoinAlgorithm(left, right, plan, predicate, joinType)
}

// chooseJoinAlgorithm selects the best join algorithm based on the join condition
func (e *BasicExecutor) chooseJoinAlgorithm(left, right Operator, plan *planner.LogicalJoin, predicate ExprEvaluator, joinType JoinType) (Operator, error) {
	// Extract join keys from the condition
	leftKeys, rightKeys := e.extractJoinKeys(plan.Condition, left.Schema(), right.Schema())

	// If we have equi-join keys, we can use hash join or merge join
	if len(leftKeys) > 0 && len(leftKeys) == len(rightKeys) {
		// Check if inputs are already sorted
		leftSorted := e.isSorted(plan.Children()[0], leftKeys)
		rightSorted := e.isSorted(plan.Children()[1], rightKeys)

		// Use merge join if both inputs are sorted or if we have large inputs
		if leftSorted && rightSorted {
			// Convert column names to indices
			leftIndices := e.namesToIndices(leftKeys, left.Schema())
			rightIndices := e.namesToIndices(rightKeys, right.Schema())

			return NewMergeJoinOperator(left, right, joinType, predicate, leftIndices, rightIndices), nil
		}

		// Otherwise use hash join for equi-joins
		// For now, just use the first join key
		// TODO: Support multi-column hash joins
		if len(leftKeys) == 1 {
			leftExpr := &planner.ColumnRef{ColumnName: leftKeys[0]}
			rightExpr := &planner.ColumnRef{ColumnName: rightKeys[0]}

			leftEval, err := buildExprEvaluatorWithExecutor(leftExpr, left.Schema(), e)
			if err != nil {
				return nil, err
			}

			rightEval, err := buildExprEvaluatorWithExecutor(rightExpr, right.Schema(), e)
			if err != nil {
				return nil, err
			}

			return NewHashJoinOperator(left, right, []ExprEvaluator{leftEval}, []ExprEvaluator{rightEval}, predicate, joinType), nil
		}
	}

	// Fall back to nested loop join for non-equi joins or complex conditions
	return NewNestedLoopJoinOperator(left, right, predicate, joinType), nil
}

// buildSemiAntiJoin builds a semi or anti join operator
func (e *BasicExecutor) buildSemiAntiJoin(left, right Operator, plan *planner.LogicalJoin, predicate ExprEvaluator) (Operator, error) {
	// Map join type
	var semiJoinType SemiJoinType
	hasNullHandling := false // TODO: Detect from query pattern

	switch plan.JoinType {
	case planner.SemiJoin:
		semiJoinType = SemiJoinTypeSemi
	case planner.AntiJoin:
		semiJoinType = SemiJoinTypeAnti
	default:
		return nil, fmt.Errorf("invalid semi/anti join type: %v", plan.JoinType)
	}

	// Extract join keys
	leftKeys, rightKeys := e.extractJoinKeys(plan.Condition, left.Schema(), right.Schema())

	// If we have equi-join keys, use hash-based semi/anti join
	if len(leftKeys) > 0 && len(leftKeys) == len(rightKeys) {
		leftEvals := make([]ExprEvaluator, len(leftKeys))
		rightEvals := make([]ExprEvaluator, len(rightKeys))

		for i, leftKey := range leftKeys {
			leftExpr := &planner.ColumnRef{ColumnName: leftKey}
			leftEval, err := buildExprEvaluatorWithExecutor(leftExpr, left.Schema(), e)
			if err != nil {
				return nil, err
			}
			leftEvals[i] = leftEval

			rightExpr := &planner.ColumnRef{ColumnName: rightKeys[i]}
			rightEval, err := buildExprEvaluatorWithExecutor(rightExpr, right.Schema(), e)
			if err != nil {
				return nil, err
			}
			rightEvals[i] = rightEval
		}

		return NewSemiJoinOperator(left, right, leftEvals, rightEvals, predicate, semiJoinType, hasNullHandling), nil
	}

	// Fall back to nested loop for complex conditions
	return NewNestedLoopSemiJoinOperator(left, right, predicate, semiJoinType, hasNullHandling), nil
}

// extractJoinKeys extracts equi-join keys from a join condition
func (e *BasicExecutor) extractJoinKeys(condition planner.Expression, leftSchema, rightSchema *Schema) ([]string, []string) {
	if condition == nil {
		return nil, nil
	}

	// Look for equality conditions
	switch expr := condition.(type) {
	case *planner.BinaryOp:
		switch expr.Operator {
		case planner.OpEqual:
			// Check if it's a column = column comparison
			leftCol, leftOk := expr.Left.(*planner.ColumnRef)
			rightCol, rightOk := expr.Right.(*planner.ColumnRef)

			if leftOk && rightOk {
				// Determine which column belongs to which side
				if e.columnBelongsTo(leftCol.ColumnName, leftSchema) && e.columnBelongsTo(rightCol.ColumnName, rightSchema) {
					return []string{leftCol.ColumnName}, []string{rightCol.ColumnName}
				} else if e.columnBelongsTo(rightCol.ColumnName, leftSchema) && e.columnBelongsTo(leftCol.ColumnName, rightSchema) {
					return []string{rightCol.ColumnName}, []string{leftCol.ColumnName}
				}
			}
		case planner.OpAnd:
			// Recursively extract from AND conditions
			leftKeys1, rightKeys1 := e.extractJoinKeys(expr.Left, leftSchema, rightSchema)
			leftKeys2, rightKeys2 := e.extractJoinKeys(expr.Right, leftSchema, rightSchema)

			if leftKeys1 != nil && leftKeys2 != nil {
				return append(leftKeys1, leftKeys2...), append(rightKeys1, rightKeys2...)
			}
		default:
			// Other operators not relevant for join key extraction
		}
	}

	return nil, nil
}

// columnBelongsTo checks if a column name belongs to a schema
func (e *BasicExecutor) columnBelongsTo(columnName string, schema *Schema) bool {
	for _, col := range schema.Columns {
		if col.Name == columnName {
			return true
		}
	}
	return false
}

// isSorted checks if a plan node produces sorted output
func (e *BasicExecutor) isSorted(plan planner.Plan, columns []string) bool {
	switch p := plan.(type) {
	case *planner.LogicalSort:
		// Check if sort keys match the required columns
		if len(p.OrderBy) >= len(columns) {
			for i, col := range columns {
				if colRef, ok := p.OrderBy[i].Expr.(*planner.ColumnRef); ok {
					if colRef.ColumnName != col {
						return false
					}
				} else {
					return false
				}
			}
			return true
		}
	case *planner.LogicalScan:
		// TODO: Check if we're scanning an index that provides the required order
		return false
	}

	// Check children
	for _, child := range plan.Children() {
		if e.isSorted(child, columns) {
			return true
		}
	}

	return false
}

// namesToIndices converts column names to indices in a schema
func (e *BasicExecutor) namesToIndices(names []string, schema *Schema) []int {
	indices := make([]int, len(names))
	for i, name := range names {
		for j, col := range schema.Columns {
			if col.Name == name {
				indices[i] = j
				break
			}
		}
	}
	return indices
}

// buildAggregateOperator builds an aggregate operator.
func (e *BasicExecutor) buildAggregateOperator(plan *planner.LogicalAggregate, ctx *ExecContext) (Operator, error) {
	// Build child operator
	child, err := e.buildOperator(plan.Children()[0], ctx)
	if err != nil {
		return nil, err
	}

	// Build GROUP BY expressions
	childSchema := child.Schema()
	groupBy := make([]ExprEvaluator, len(plan.GroupBy))
	groupByNames := make([]string, len(plan.GroupBy))
	for i, expr := range plan.GroupBy {
		eval, err := buildExprEvaluatorWithExecutor(expr, childSchema, e)
		if err != nil {
			return nil, fmt.Errorf("failed to build GROUP BY expression %d: %w", i, err)
		}
		groupBy[i] = eval

		// Extract column name from the GROUP BY expression
		if colRef, ok := expr.(*planner.ColumnRef); ok {
			groupByNames[i] = colRef.ColumnName
		} else {
			groupByNames[i] = fmt.Sprintf("group_%d", i)
		}
	}

	// Build aggregate expressions
	aggregates := make([]AggregateExpr, len(plan.Aggregates))
	for i, aggExpr := range plan.Aggregates {
		// Create aggregate function based on the type
		var funcName string
		switch aggExpr.Function {
		case planner.AggCount:
			funcName = AggregateCOUNT
		case planner.AggSum:
			funcName = AggregateSUM
		case planner.AggAvg:
			funcName = AggregateAVG
		case planner.AggMin:
			funcName = AggregateMIN
		case planner.AggMax:
			funcName = AggregateMAX
		default:
			return nil, fmt.Errorf("unsupported aggregate function: %v", aggExpr.Function)
		}

		// Create aggregate function
		aggFunc, err := CreateAggregateFunction(funcName, aggExpr.Args)
		if err != nil {
			return nil, fmt.Errorf("failed to create aggregate function %d: %w", i, err)
		}

		// Build expression evaluator for the argument
		var argEval ExprEvaluator
		if len(aggExpr.Args) > 0 {
			// Check if this is COUNT(*)
			if funcName == "COUNT" {
				if _, isStar := aggExpr.Args[0].(*planner.Star); isStar {
					// For COUNT(*), use a literal 1
					argEval = &literalEvaluator{value: types.NewValue(int64(1))}
				} else {
					// COUNT(expr)
					argEval, err = buildExprEvaluatorWithExecutor(aggExpr.Args[0], childSchema, e)
					if err != nil {
						return nil, fmt.Errorf("failed to build aggregate argument %d: %w", i, err)
					}
				}
			} else {
				// Other aggregate functions
				argEval, err = buildExprEvaluatorWithExecutor(aggExpr.Args[0], childSchema, e)
				if err != nil {
					return nil, fmt.Errorf("failed to build aggregate argument %d: %w", i, err)
				}
			}
		} else {
			// No arguments - shouldn't happen for standard aggregates
			argEval = &literalEvaluator{value: types.NewValue(int64(1))}
		}

		// Use the alias from the planner, or generate one based on function name
		alias := aggExpr.Alias
		if alias == "" {
			// Get function name for a meaningful default alias
			var funcName string
			switch aggExpr.Function {
			case planner.AggCount:
				funcName = "COUNT"
			case planner.AggSum:
				funcName = "SUM"
			case planner.AggAvg:
				funcName = "AVG"
			case planner.AggMin:
				funcName = "MIN"
			case planner.AggMax:
				funcName = "MAX"
			default:
				funcName = fmt.Sprintf("FUNC_%d", i)
			}
			alias = fmt.Sprintf("agg_%s", funcName)
		}

		aggregates[i] = AggregateExpr{
			Function: aggFunc,
			Expr:     argEval,
			Alias:    alias,
		}
	}

	return NewAggregateOperatorWithNames(child, groupBy, aggregates, groupByNames), nil
}

// buildCreateTableOperator builds a CREATE TABLE operator.
func (e *BasicExecutor) buildCreateTableOperator(plan *planner.LogicalCreateTable, ctx *ExecContext) (Operator, error) {
	if e.storage == nil {
		return nil, fmt.Errorf("storage backend not configured")
	}

	return NewCreateTableOperator(
		plan.SchemaName,
		plan.TableName,
		plan.Columns,
		plan.Constraints,
		ctx.Catalog,
		e.storage,
	), nil
}

// buildInsertOperator builds an INSERT operator.
func (e *BasicExecutor) buildInsertOperator(plan *planner.LogicalInsert, ctx *ExecContext) (Operator, error) {
	_ = ctx // Currently unused, but will be needed for transaction context
	if e.storage == nil {
		return nil, fmt.Errorf("storage backend not configured")
	}

	return NewInsertOperator(plan.TableRef, e.storage, plan.Values), nil
}

// buildUpdateOperator builds an UPDATE operator.
func (e *BasicExecutor) buildUpdateOperator(plan *planner.LogicalUpdate, ctx *ExecContext) (Operator, error) {
	_ = ctx // Currently unused, but will be needed for transaction context
	if e.storage == nil {
		return nil, fmt.Errorf("storage backend not configured")
	}

	return NewUpdateOperator(plan.TableRef, e.storage, plan.Assignments, plan.Where), nil
}

// buildDeleteOperator builds a DELETE operator.
func (e *BasicExecutor) buildDeleteOperator(plan *planner.LogicalDelete, ctx *ExecContext) (Operator, error) {
	_ = ctx // Currently unused, but will be needed for transaction context
	if e.storage == nil {
		return nil, fmt.Errorf("storage backend not configured")
	}

	return NewDeleteOperator(plan.TableRef, e.storage, plan.Where), nil
}

// buildCreateIndexOperator builds a CREATE INDEX operator.
func (e *BasicExecutor) buildCreateIndexOperator(plan *planner.LogicalCreateIndex, ctx *ExecContext) (Operator, error) {
	if e.storage == nil {
		return nil, fmt.Errorf("storage backend not configured")
	}

	if e.indexMgr == nil {
		return nil, fmt.Errorf("index manager not configured")
	}

	// Type assert to get the actual index.Manager
	indexMgr, ok := e.indexMgr.(*index.Manager)
	if !ok {
		return nil, fmt.Errorf("invalid index manager type")
	}

	return NewCreateIndexOperator(
		plan.SchemaName,
		plan.TableName,
		plan.IndexName,
		plan.Columns,
		plan.Unique,
		plan.IndexType,
		ctx.Catalog,
		e.storage,
		indexMgr,
	), nil
}

// buildDropTableOperator builds a DROP TABLE operator.
func (e *BasicExecutor) buildDropTableOperator(plan *planner.LogicalDropTable, ctx *ExecContext) (Operator, error) {
	if e.storage == nil {
		return nil, fmt.Errorf("storage backend not configured")
	}

	return NewDropTableOperator(
		plan.SchemaName,
		plan.TableName,
		ctx.Catalog,
		e.storage,
	), nil
}

// buildAlterTableAddColumnOperator builds an ALTER TABLE ADD COLUMN operator.
func (e *BasicExecutor) buildAlterTableAddColumnOperator(plan *planner.LogicalAlterTableAddColumn, ctx *ExecContext) (Operator, error) {
	if e.storage == nil {
		return nil, fmt.Errorf("storage backend not configured")
	}

	return NewAlterTableAddColumnOperator(
		plan.SchemaName,
		plan.TableName,
		plan.Column,
		ctx.Catalog,
		e.storage,
	), nil
}

// buildAlterTableDropColumnOperator builds an ALTER TABLE DROP COLUMN operator.
func (e *BasicExecutor) buildAlterTableDropColumnOperator(plan *planner.LogicalAlterTableDropColumn, ctx *ExecContext) (Operator, error) {
	if e.storage == nil {
		return nil, fmt.Errorf("storage backend not configured")
	}

	return NewAlterTableDropColumnOperator(
		plan.SchemaName,
		plan.TableName,
		plan.ColumnName,
		ctx.Catalog,
		e.storage,
	), nil
}

// buildDropIndexOperator builds a DROP INDEX operator.
func (e *BasicExecutor) buildDropIndexOperator(plan *planner.LogicalDropIndex, ctx *ExecContext) (Operator, error) {
	if e.storage == nil {
		return nil, fmt.Errorf("storage backend not configured")
	}

	if e.indexMgr == nil {
		return nil, fmt.Errorf("index manager not configured")
	}

	// Type assert to get the actual index.Manager
	indexMgr, ok := e.indexMgr.(*index.Manager)
	if !ok {
		return nil, fmt.Errorf("invalid index manager type")
	}

	return NewDropIndexOperator(
		plan.SchemaName,
		plan.TableName,
		plan.IndexName,
		ctx.Catalog,
		indexMgr,
	), nil
}

// buildAnalyzeOperator builds an ANALYZE operator.
func (e *BasicExecutor) buildAnalyzeOperator(plan *planner.LogicalAnalyze, ctx *ExecContext) (Operator, error) {
	if e.storage == nil {
		return nil, fmt.Errorf("storage backend not configured")
	}

	return NewAnalyzeOperator(plan, ctx.Catalog, e.storage), nil
}

// buildVacuumOperator builds a VACUUM operator.
func (e *BasicExecutor) buildVacuumOperator(plan *planner.LogicalVacuum, ctx *ExecContext) (Operator, error) {
	if e.storage == nil {
		return nil, fmt.Errorf("storage backend not configured")
	}

	// Get storage backend as MVCCStorageBackend
	mvccStorage, ok := e.storage.(*MVCCStorageBackend)
	if !ok {
		return nil, fmt.Errorf("vacuum requires MVCC storage backend")
	}

	// Get table if specified
	var table *catalog.Table
	if plan.TableName != "" {
		var err error
		table, err = ctx.Catalog.GetTable(plan.SchemaName, plan.TableName)
		if err != nil {
			return nil, fmt.Errorf("table not found: %w", err)
		}
	}

	if plan.Analyze {
		// Create VACUUM ANALYZE operator
		return NewVacuumAnalyzeOperator(mvccStorage, ctx.Catalog, table), nil
	}

	// Create regular VACUUM operator
	return NewVacuumOperator(mvccStorage, table), nil
}

// buildCopyOperator builds a COPY operator.
func (e *BasicExecutor) buildCopyOperator(plan *planner.LogicalCopy, ctx *ExecContext) (Operator, error) {
	if e.storage == nil {
		return nil, fmt.Errorf("storage backend not configured")
	}

	return NewCopyOperator(plan, ctx.Catalog, e.storage, ctx.TxnManager), nil
}

// buildPrepareOperator builds a PREPARE operator.
func (e *BasicExecutor) buildPrepareOperator(plan *planner.LogicalPrepare, ctx *ExecContext) (Operator, error) {
	return NewPrepareOperator(plan), nil
}

// buildExecuteOperator builds an EXECUTE operator.
func (e *BasicExecutor) buildExecuteOperator(plan *planner.LogicalExecute, ctx *ExecContext) (Operator, error) {
	return NewExecuteOperator(plan), nil
}

// buildDeallocateOperator builds a DEALLOCATE operator.
func (e *BasicExecutor) buildDeallocateOperator(plan *planner.LogicalDeallocate, ctx *ExecContext) (Operator, error) {
	return NewDeallocateOperator(plan), nil
}

// convertSchema converts a planner schema to executor schema.
func convertSchema(planSchema *planner.Schema) *Schema {
	if planSchema == nil {
		return nil
	}

	schema := &Schema{
		Columns: make([]Column, len(planSchema.Columns)),
	}

	for i, col := range planSchema.Columns {
		schema.Columns[i] = Column{
			Name:     col.Name,
			Type:     col.DataType,
			Nullable: col.Nullable,
		}
	}

	return schema
}

// operatorResult wraps an operator as a Result.
type operatorResult struct {
	operator Operator
	schema   *Schema
}

func (r *operatorResult) Next() (*Row, error) {
	return r.operator.Next()
}

func (r *operatorResult) Close() error {
	return r.operator.Close()
}

func (r *operatorResult) Schema() *Schema {
	return r.schema
}

// buildValuesOperator builds a values operator for constant values.
func (e *BasicExecutor) buildValuesOperator(plan *planner.LogicalValues, ctx *ExecContext) (Operator, error) {
	_ = ctx // Context not needed for values operator
	return NewValuesOperator(plan.Rows, convertSchema(plan.Schema())), nil
}

// buildBitmapIndexScanOperator builds a bitmap index scan operator.
func (e *BasicExecutor) buildBitmapIndexScanOperator(plan *planner.BitmapIndexScan, ctx *ExecContext) (Operator, error) {
	if e.indexMgr == nil {
		return nil, fmt.Errorf("index manager not configured for bitmap index scan")
	}

	// Type assert to get the actual index.Manager
	indexMgr, ok := e.indexMgr.(*index.Manager)
	if !ok {
		return nil, fmt.Errorf("invalid index manager type")
	}

	// Get table from catalog
	table, err := e.getTableFromCatalog(ctx, plan.TableName)
	if err != nil {
		return nil, err
	}

	return NewBitmapIndexScanOperator(table, plan.Index, indexMgr, plan.StartKey, plan.EndKey), nil
}

// buildBitmapAndOperator builds a bitmap AND operator.
func (e *BasicExecutor) buildBitmapAndOperator(plan *planner.BitmapAnd, ctx *ExecContext) (Operator, error) {
	children := make([]Operator, 0, len(plan.BitmapChildren))
	for _, child := range plan.BitmapChildren {
		op, err := e.buildOperator(child, ctx)
		if err != nil {
			return nil, err
		}
		children = append(children, op)
	}
	return NewBitmapAndOperator(children), nil
}

// buildBitmapOrOperator builds a bitmap OR operator.
func (e *BasicExecutor) buildBitmapOrOperator(plan *planner.BitmapOr, ctx *ExecContext) (Operator, error) {
	children := make([]Operator, 0, len(plan.BitmapChildren))
	for _, child := range plan.BitmapChildren {
		op, err := e.buildOperator(child, ctx)
		if err != nil {
			return nil, err
		}
		children = append(children, op)
	}
	return NewBitmapOrOperator(children), nil
}

// buildBitmapHeapScanOperator builds a bitmap heap scan operator.
func (e *BasicExecutor) buildBitmapHeapScanOperator(plan *planner.BitmapHeapScan, ctx *ExecContext) (Operator, error) {
	if e.storage == nil {
		return nil, fmt.Errorf("storage backend not configured for bitmap heap scan")
	}

	// Get table from catalog
	table, err := e.getTableFromCatalog(ctx, plan.TableName)
	if err != nil {
		return nil, err
	}

	// Build bitmap source operator
	bitmapSource, err := e.buildOperator(plan.BitmapSource, ctx)
	if err != nil {
		return nil, err
	}

	return NewBitmapHeapScanOperator(table, bitmapSource, e.storage), nil
}

// remapGroupColumns recursively remaps group_X column references to actual column names
// from the aggregate schema. This fixes the issue where planner uses temporary names
// like "group_0" but aggregate operator produces actual column names like "c_mktsegment".
func remapGroupColumns(expr planner.Expression, aggregateSchema *Schema) planner.Expression {
	switch e := expr.(type) {
	case *planner.ColumnRef:
		// Check if this is a group_X reference
		if strings.HasPrefix(e.ColumnName, "group_") {
			// Extract the group index
			var groupIndex int
			if n, err := fmt.Sscanf(e.ColumnName, "group_%d", &groupIndex); n == 1 && err == nil {
				// Map to actual column name from aggregate schema
				if groupIndex >= 0 && groupIndex < len(aggregateSchema.Columns) {
					return &planner.ColumnRef{
						ColumnName: aggregateSchema.Columns[groupIndex].Name,
						TableAlias: e.TableAlias,
						ColumnType: e.ColumnType,
					}
				}
			}
		}
		return e
	case *planner.BinaryOp:
		return &planner.BinaryOp{
			Left:     remapGroupColumns(e.Left, aggregateSchema),
			Right:    remapGroupColumns(e.Right, aggregateSchema),
			Operator: e.Operator,
			Type:     e.Type,
		}
	case *planner.UnaryOp:
		return &planner.UnaryOp{
			Expr:     remapGroupColumns(e.Expr, aggregateSchema),
			Operator: e.Operator,
			Type:     e.Type,
		}
	case *planner.FunctionCall:
		remappedArgs := make([]planner.Expression, len(e.Args))
		for i, arg := range e.Args {
			remappedArgs[i] = remapGroupColumns(arg, aggregateSchema)
		}
		return &planner.FunctionCall{
			Name: e.Name,
			Args: remappedArgs,
			Type: e.Type,
		}
	// Add more expression types as needed
	default:
		// For other expression types (literals, etc.), return as-is
		return e
	}
}
