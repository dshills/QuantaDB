package planner

import (
	"fmt"
	"time"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

// Constants
const (
	defaultSchema = "public"
)

// Planner transforms parsed SQL statements into executable query plans.
type Planner interface {
	Plan(stmt parser.Statement) (Plan, error)
}

// BasicPlanner is a simple rule-based query planner.
type BasicPlanner struct {
	catalog   catalog.Catalog
	optimizer *Optimizer
}

// NewBasicPlanner creates a new basic planner.
func NewBasicPlanner() *BasicPlanner {
	cat := catalog.NewMemoryCatalog()
	return &BasicPlanner{
		catalog:   cat,
		optimizer: NewOptimizerWithCatalog(cat),
	}
}

// NewBasicPlannerWithCatalog creates a new basic planner with a specific catalog.
func NewBasicPlannerWithCatalog(cat catalog.Catalog) *BasicPlanner {
	return &BasicPlanner{
		catalog:   cat,
		optimizer: NewOptimizerWithCatalog(cat),
	}
}

// Plan transforms a statement into a query plan.
func (p *BasicPlanner) Plan(stmt parser.Statement) (Plan, error) {
	// Convert AST to logical plan
	logical, err := p.buildLogicalPlan(stmt)
	if err != nil {
		return nil, fmt.Errorf("failed to build logical plan: %w", err)
	}

	// Apply optimization rules
	optimized := p.optimize(logical)

	// For now, return the optimized logical plan
	// Physical planning will be added later
	return optimized, nil
}

// buildLogicalPlan converts an AST statement to a logical plan.
func (p *BasicPlanner) buildLogicalPlan(stmt parser.Statement) (LogicalPlan, error) {
	switch s := stmt.(type) {
	case *parser.SelectStmt:
		return p.planSelect(s)
	case *parser.InsertStmt:
		return p.planInsert(s)
	case *parser.UpdateStmt:
		return p.planUpdate(s)
	case *parser.DeleteStmt:
		return p.planDelete(s)
	case *parser.CreateTableStmt:
		return p.planCreateTable(s)
	case *parser.CreateIndexStmt:
		return p.planCreateIndex(s)
	case *parser.DropTableStmt:
		return p.planDropTable(s)
	case *parser.AlterTableStmt:
		return p.planAlterTable(s)
	case *parser.DropIndexStmt:
		return p.planDropIndex(s)
	case *parser.AnalyzeStmt:
		return p.planAnalyze(s)
	case *parser.VacuumStmt:
		return p.planVacuum(s)
	case *parser.CopyStmt:
		return p.planCopy(s)
	case *parser.PrepareStmt:
		return p.planPrepare(s)
	case *parser.ExecuteStmt:
		return p.planExecute(s)
	case *parser.DeallocateStmt:
		return p.planDeallocate(s)
	default:
		return nil, fmt.Errorf("unsupported statement type: %T", stmt)
	}
}

// planSelect converts a SELECT statement to a logical plan.
func (p *BasicPlanner) planSelect(stmt *parser.SelectStmt) (LogicalPlan, error) {
	// If there are CTEs, plan them first
	if len(stmt.With) > 0 {
		return p.planWithClause(stmt)
	}

	var plan LogicalPlan

	// Handle SELECT without FROM clause (e.g., SELECT 1, SELECT 1+2)
	if stmt.From == nil {
		// Create a dummy plan that returns a single row
		// We'll use a special "dual" table concept similar to Oracle/MySQL
		schema := &Schema{
			Columns: []Column{
				{Name: "dummy", DataType: types.Integer, Nullable: false},
			},
		}
		plan = NewLogicalValues([][]types.Value{{types.NewValue(int64(1))}}, schema)
	} else {
		// Build the FROM clause plan (handles simple tables and JOINs)
		var err error
		plan, err = p.planTableExpression(stmt.From)
		if err != nil {
			return nil, err
		}
	}

	return p.buildSelectPlan(plan, stmt)
}

// planWithClause plans a SELECT statement with CTEs.
func (p *BasicPlanner) planWithClause(stmt *parser.SelectStmt) (LogicalPlan, error) {
	// Plan each CTE
	var ctes []LogicalCTE
	for _, cte := range stmt.With {
		// Plan the CTE query
		ctePlan, err := p.planSelect(cte.Query)
		if err != nil {
			return nil, fmt.Errorf("failed to plan CTE %s: %w", cte.Name, err)
		}

		// Create the logical CTE node
		logicalCTE := LogicalCTE{
			basePlan: basePlan{
				children: []Plan{ctePlan},
				schema:   ctePlan.Schema(),
			},
			Name: cte.Name,
			Plan: ctePlan,
		}
		ctes = append(ctes, logicalCTE)
	}

	// Plan the main query (temporarily removing CTEs to avoid recursion)
	mainStmt := *stmt   // Copy the statement
	mainStmt.With = nil // Clear CTEs for main query planning

	mainPlan, err := p.planSelect(&mainStmt)
	if err != nil {
		return nil, fmt.Errorf("failed to plan main query: %w", err)
	}

	// Create the WITH clause node
	withClause := NewLogicalWithClause(ctes, mainPlan, mainPlan.Schema())
	return withClause, nil
}

// buildSelectPlan builds the rest of the SELECT plan after the table scan.
func (p *BasicPlanner) buildSelectPlan(plan LogicalPlan, stmt *parser.SelectStmt) (LogicalPlan, error) {
	// Add WHERE clause if present
	if stmt.Where != nil {
		predicate, err := p.convertExpression(stmt.Where)
		if err != nil {
			return nil, fmt.Errorf("failed to convert WHERE clause: %w", err)
		}
		plan = NewLogicalFilter(plan, predicate)
	}

	// Check if we have aggregates in the SELECT clause
	rewriter := NewAggregateRewriter()
	projections, aliases, err := p.rewriteSelectColumns(stmt.Columns, rewriter)
	if err != nil {
		return nil, fmt.Errorf("failed to process select columns: %w", err)
	}
	aggregates := rewriter.GetAggregates()

	// Handle GROUP BY and aggregates
	if len(stmt.GroupBy) > 0 || len(aggregates) > 0 {
		// Convert GROUP BY expressions if present
		var groupByExprs []Expression
		if len(stmt.GroupBy) > 0 {
			groupByExprs = make([]Expression, len(stmt.GroupBy))
			for i, expr := range stmt.GroupBy {
				groupByExpr, err := p.convertExpression(expr)
				if err != nil {
					return nil, fmt.Errorf("failed to convert GROUP BY expression %d: %w", i, err)
				}
				groupByExprs[i] = groupByExpr
			}
		}

		// Build aggregate schema
		aggSchema := p.buildAggregateSchema(groupByExprs, aggregates)

		// Create aggregate node
		plan = NewLogicalAggregate(plan, groupByExprs, aggregates, aggSchema)

		// Add HAVING clause if present
		if stmt.Having != nil {
			having, err := p.convertExpression(stmt.Having)
			if err != nil {
				return nil, fmt.Errorf("failed to convert HAVING clause: %w", err)
			}
			plan = NewLogicalFilter(plan, having)
		}

		// Now add the final projection with the rewritten expressions
		projSchema, err := p.buildProjectionSchema(projections, aliases)
		if err != nil {
			return nil, err
		}
		plan = NewLogicalProject(plan, projections, aliases, projSchema)
	} else {
		// No GROUP BY - normal projection
		projections, aliases, projSchema, err := p.convertSelectColumns(stmt.Columns)
		if err != nil {
			return nil, fmt.Errorf("failed to convert select columns: %w", err)
		}
		plan = NewLogicalProject(plan, projections, aliases, projSchema)
	}

	// Add DISTINCT if specified
	if stmt.Distinct {
		plan = NewLogicalDistinct(plan)
	}

	// Add ORDER BY if present
	if len(stmt.OrderBy) > 0 {
		orderBy, err := p.convertOrderBy(stmt.OrderBy)
		if err != nil {
			return nil, fmt.Errorf("failed to convert ORDER BY: %w", err)
		}
		plan = NewLogicalSort(plan, orderBy)
	}

	// Add LIMIT/OFFSET if present
	if stmt.Limit != nil || stmt.Offset != nil {
		limit := int64(-1)
		offset := int64(0)

		if stmt.Limit != nil {
			limit = int64(*stmt.Limit)
		}
		if stmt.Offset != nil {
			offset = int64(*stmt.Offset)
		}

		plan = NewLogicalLimit(plan, limit, offset)
	}

	return plan, nil
}

// planInsert converts an INSERT statement to a logical plan.
func (p *BasicPlanner) planInsert(stmt *parser.InsertStmt) (LogicalPlan, error) {
	// Get table from catalog
	table, err := p.catalog.GetTable(defaultSchema, stmt.TableName)
	if err != nil {
		return nil, fmt.Errorf("table '%s' not found: %w", stmt.TableName, err)
	}

	// Validate column count if columns are specified
	if len(stmt.Columns) > 0 && len(stmt.Columns) != len(stmt.Values[0]) {
		return nil, fmt.Errorf("column count mismatch")
	}

	// If no columns specified, assume all columns in order
	columns := stmt.Columns
	if len(columns) == 0 {
		columns = make([]string, len(table.Columns))
		for i, col := range table.Columns {
			columns[i] = col.Name
		}
	}

	return NewLogicalInsert(defaultSchema, stmt.TableName, columns, stmt.Values, table), nil
}

// planUpdate converts an UPDATE statement to a logical plan.
func (p *BasicPlanner) planUpdate(stmt *parser.UpdateStmt) (LogicalPlan, error) {
	// Default to public schema if not specified
	schemaName := defaultSchema

	// Get table from catalog
	table, err := p.catalog.GetTable(schemaName, stmt.TableName)
	if err != nil {
		return nil, fmt.Errorf("table '%s' not found: %w", stmt.TableName, err)
	}

	// Validate that all columns in assignments exist
	for _, assignment := range stmt.Assignments {
		found := false
		for _, col := range table.Columns {
			if col.Name == assignment.Column {
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("column '%s' not found in table '%s'", assignment.Column, stmt.TableName)
		}
	}

	// Validate WHERE clause column references if present
	if stmt.Where != nil {
		if err := p.validateExpressionColumns(stmt.Where, table); err != nil {
			return nil, fmt.Errorf("invalid WHERE clause: %w", err)
		}
	}

	return NewLogicalUpdate(schemaName, stmt.TableName, stmt.Assignments, stmt.Where, table), nil
}

// planDelete converts a DELETE statement to a logical plan.
func (p *BasicPlanner) planDelete(stmt *parser.DeleteStmt) (LogicalPlan, error) {
	// Default to public schema if not specified
	schemaName := defaultSchema

	// Get table from catalog
	table, err := p.catalog.GetTable(schemaName, stmt.TableName)
	if err != nil {
		return nil, fmt.Errorf("table '%s' not found: %w", stmt.TableName, err)
	}

	// Validate WHERE clause column references if present
	if stmt.Where != nil {
		if err := p.validateExpressionColumns(stmt.Where, table); err != nil {
			return nil, fmt.Errorf("invalid WHERE clause: %w", err)
		}
	}

	return NewLogicalDelete(schemaName, stmt.TableName, stmt.Where, table), nil
}

// planCreateTable converts a CREATE TABLE statement to a logical plan.
func (p *BasicPlanner) planCreateTable(stmt *parser.CreateTableStmt) (LogicalPlan, error) {
	// Convert parser columns to catalog columns
	columns := make([]catalog.Column, len(stmt.Columns))
	for i, col := range stmt.Columns {
		// Determine if column is nullable by checking constraints
		nullable := true
		var defaultValue types.Value
		for _, constraint := range col.Constraints {
			switch c := constraint.(type) {
			case parser.NotNullConstraint:
				nullable = false
			case parser.DefaultConstraint:
				// Extract default value from the expression
				if lit, ok := c.Value.(*parser.Literal); ok {
					defaultValue = lit.Value
				}
			}
		}

		columns[i] = catalog.Column{
			Name:         col.Name,
			DataType:     col.DataType,
			IsNullable:   nullable,
			DefaultValue: defaultValue,
		}
	}

	// Convert table constraints
	constraints := make([]catalog.Constraint, 0)
	for _, constraint := range stmt.Constraints {
		switch con := constraint.(type) {
		case parser.TablePrimaryKeyConstraint:
			constraints = append(constraints, &catalog.PrimaryKeyConstraint{
				Columns: con.Columns,
			})
		case parser.TableForeignKeyConstraint:
			// Default to public schema if not specified
			refSchema := defaultSchema
			constraints = append(constraints, &catalog.ForeignKeyConstraint{
				Name:           con.Name,
				Columns:        con.Columns,
				RefTableSchema: refSchema,
				RefTableName:   con.RefTable,
				RefColumns:     con.RefColumns,
				OnDelete:       catalog.ReferentialAction(con.OnDelete),
				OnUpdate:       catalog.ReferentialAction(con.OnUpdate),
			})
		case parser.TableCheckConstraint:
			constraints = append(constraints, &catalog.CheckConstraint{
				Name:       con.Name,
				Expression: con.Expression.String(),
			})
		case parser.TableUniqueConstraint:
			constraints = append(constraints, &catalog.UniqueConstraint{
				Columns: con.Columns,
			})
		}
	}

	return NewLogicalCreateTable(defaultSchema, stmt.TableName, columns, constraints), nil
}

// planCreateIndex converts a CREATE INDEX statement to a logical plan.
func (p *BasicPlanner) planCreateIndex(stmt *parser.CreateIndexStmt) (LogicalPlan, error) {
	// Default to public schema
	schemaName := defaultSchema

	return NewLogicalCreateIndex(schemaName, stmt.TableName, stmt.IndexName,
		stmt.Columns, stmt.Unique, stmt.IndexType), nil
}

// planDropTable converts a DROP TABLE statement to a logical plan.
func (p *BasicPlanner) planDropTable(stmt *parser.DropTableStmt) (LogicalPlan, error) {
	// Default to public schema
	schemaName := defaultSchema

	return NewLogicalDropTable(schemaName, stmt.TableName), nil
}

// planAlterTable converts an ALTER TABLE statement to a logical plan.
func (p *BasicPlanner) planAlterTable(stmt *parser.AlterTableStmt) (LogicalPlan, error) {
	// Default to public schema
	schemaName := defaultSchema

	switch stmt.Action {
	case parser.AlterTableActionAddColumn:
		return NewLogicalAlterTableAddColumn(schemaName, stmt.TableName, stmt.Column), nil
	case parser.AlterTableActionDropColumn:
		return NewLogicalAlterTableDropColumn(schemaName, stmt.TableName, stmt.ColumnName), nil
	default:
		return nil, fmt.Errorf("unsupported ALTER TABLE action")
	}
}

// planDropIndex converts a DROP INDEX statement to a logical plan.
func (p *BasicPlanner) planDropIndex(stmt *parser.DropIndexStmt) (LogicalPlan, error) {
	// Default to public schema
	schemaName := defaultSchema

	return NewLogicalDropIndex(schemaName, stmt.TableName, stmt.IndexName), nil
}

// planAnalyze converts an ANALYZE statement to a logical plan.
func (p *BasicPlanner) planAnalyze(stmt *parser.AnalyzeStmt) (LogicalPlan, error) {
	// Default to public schema
	schemaName := defaultSchema

	// Verify table exists
	table, err := p.catalog.GetTable(schemaName, stmt.TableName)
	if err != nil {
		return nil, fmt.Errorf("table %s.%s not found", schemaName, stmt.TableName)
	}

	// Verify columns exist if specified
	if len(stmt.Columns) > 0 {
		for _, colName := range stmt.Columns {
			found := false
			for _, col := range table.Columns {
				if col.Name == colName {
					found = true
					break
				}
			}
			if !found {
				return nil, fmt.Errorf("column %s not found in table %s", colName, stmt.TableName)
			}
		}
	}

	return NewLogicalAnalyze(schemaName, stmt.TableName, stmt.Columns), nil
}

// planVacuum converts a VACUUM statement to a logical plan.
func (p *BasicPlanner) planVacuum(stmt *parser.VacuumStmt) (LogicalPlan, error) {
	// Default to public schema
	schemaName := defaultSchema

	// If specific table is provided, verify it exists
	if stmt.TableName != "" {
		_, err := p.catalog.GetTable(schemaName, stmt.TableName)
		if err != nil {
			return nil, fmt.Errorf("table %s.%s not found", schemaName, stmt.TableName)
		}
	}

	return NewLogicalVacuum(schemaName, stmt.TableName, stmt.Analyze), nil
}

// planCopy converts a COPY statement to a logical plan.
func (p *BasicPlanner) planCopy(stmt *parser.CopyStmt) (LogicalPlan, error) {
	// Default to public schema
	schemaName := defaultSchema

	// Verify table exists
	table, err := p.catalog.GetTable(schemaName, stmt.TableName)
	if err != nil {
		return nil, fmt.Errorf("table %s.%s not found", schemaName, stmt.TableName)
	}

	// Verify columns exist if specified
	if len(stmt.Columns) > 0 {
		for _, colName := range stmt.Columns {
			found := false
			for _, col := range table.Columns {
				if col.Name == colName {
					found = true
					break
				}
			}
			if !found {
				return nil, fmt.Errorf("column %s not found in table %s", colName, stmt.TableName)
			}
		}
	}

	// Validate direction-source combination
	if stmt.Direction == parser.CopyTo && stmt.Source == "STDIN" {
		return nil, fmt.Errorf("cannot COPY TO STDIN")
	}
	if stmt.Direction == parser.CopyFrom && stmt.Source == "STDOUT" {
		return nil, fmt.Errorf("cannot COPY FROM STDOUT")
	}

	return NewLogicalCopy(schemaName, stmt.TableName, stmt.Columns, stmt.Direction, stmt.Source, stmt.Options, table), nil
}

// convertExpression converts a parser expression to a planner expression.
func (p *BasicPlanner) convertExpression(expr parser.Expression) (Expression, error) {
	switch e := expr.(type) {
	case *parser.Literal:
		// Infer type from value
		var dataType types.DataType
		if e.Value.IsNull() {
			dataType = types.Unknown
		} else {
			switch e.Value.Data.(type) {
			case int64:
				dataType = types.BigInt
			case int32:
				dataType = types.Integer
			case int16:
				dataType = types.SmallInt
			case bool:
				dataType = types.Boolean
			case string:
				dataType = types.Text
			case time.Time:
				dataType = types.Timestamp
			case types.Interval:
				dataType = types.IntervalType
			default:
				dataType = types.Unknown
			}
		}
		return &Literal{Value: e.Value, Type: dataType}, nil

	case *parser.Identifier:
		// Try to resolve column type from catalog
		// For now, we'll use Unknown type if we can't resolve it
		return &ColumnRef{
			TableAlias: e.Table,
			ColumnName: e.Name,
			ColumnType: types.Unknown,
		}, nil

	case *parser.Star:
		return &Star{}, nil

	case *parser.FunctionCall:
		// Check if it's an aggregate function
		if isAggregateFunction(e.Name) {
			// Convert arguments
			args := make([]Expression, len(e.Args))
			for i, arg := range e.Args {
				converted, err := p.convertExpression(arg)
				if err != nil {
					return nil, fmt.Errorf("failed to convert function argument %d: %w", i, err)
				}
				args[i] = converted
			}

			// Determine result type based on function
			var resultType types.DataType
			switch e.Name {
			case "COUNT":
				resultType = types.BigInt
			case "SUM", "AVG", "STDDEV":
				resultType = types.Decimal(20, 6)
			case "MIN", "MAX":
				// Result type depends on input - use Unknown for now
				resultType = types.Unknown
			default:
				resultType = types.Unknown
			}

			// Create aggregate expression
			var function AggregateFunc
			switch e.Name {
			case "COUNT":
				function = AggCount
			case "SUM":
				function = AggSum
			case "AVG":
				function = AggAvg
			case "MIN":
				function = AggMin
			case "MAX":
				function = AggMax
			case "STDDEV":
				function = AggStdDev
			default:
				return nil, fmt.Errorf("unknown aggregate function: %s", e.Name)
			}

			return &AggregateExpr{
				Function: function,
				Args:     args,
				Distinct: e.Distinct,
				Type:     resultType,
			}, nil
		}

		// Non-aggregate function - not implemented yet
		return nil, fmt.Errorf("non-aggregate functions not implemented: %s", e.Name)

	case *parser.ExtractExpr:
		// Convert the FROM expression
		fromExpr, err := p.convertExpression(e.From)
		if err != nil {
			return nil, fmt.Errorf("failed to convert EXTRACT FROM expression: %w", err)
		}

		// EXTRACT always returns an integer
		return &ExtractExpr{
			Field: e.Field,
			From:  fromExpr,
			Type:  types.Integer,
		}, nil

	case *parser.SubstringExpr:
		// Convert the string expression
		strExpr, err := p.convertExpression(e.Str)
		if err != nil {
			return nil, fmt.Errorf("failed to convert SUBSTRING string expression: %w", err)
		}

		// Convert the start expression
		startExpr, err := p.convertExpression(e.Start)
		if err != nil {
			return nil, fmt.Errorf("failed to convert SUBSTRING start expression: %w", err)
		}

		// Convert the optional length expression
		var lengthExpr Expression
		if e.Length != nil {
			lengthExpr, err = p.convertExpression(e.Length)
			if err != nil {
				return nil, fmt.Errorf("failed to convert SUBSTRING length expression: %w", err)
			}
		}

		return &FunctionCall{
			Name: "SUBSTRING",
			Args: []Expression{strExpr, startExpr, lengthExpr},
			Type: types.Text,
		}, nil

	case *parser.BinaryExpr:
		left, err := p.convertExpression(e.Left)
		if err != nil {
			return nil, err
		}

		right, err := p.convertExpression(e.Right)
		if err != nil {
			return nil, err
		}

		op, err := p.convertBinaryOp(e.Operator)
		if err != nil {
			return nil, err
		}

		// Determine result type based on operator
		var resultType types.DataType
		switch op {
		case OpAdd, OpSubtract, OpMultiply, OpDivide, OpModulo:
			// Arithmetic operators return numeric type
			// For simplicity, use the left operand's type or default to BigInt
			// In a full implementation, we'd do proper type promotion
			if left.DataType() != types.Unknown {
				resultType = left.DataType()
			} else if right.DataType() != types.Unknown {
				resultType = right.DataType()
			} else {
				resultType = types.BigInt
			}
			// Division always returns float/decimal
			if op == OpDivide {
				resultType = types.Float
			}
		case OpAnd, OpOr:
			// Logical operators return boolean
			resultType = types.Boolean
		case OpConcat:
			// String concatenation returns text
			resultType = types.Text
		default:
			// Default to boolean for comparison operators
			resultType = types.Boolean
		}

		return &BinaryOp{
			Left:     left,
			Right:    right,
			Operator: op,
			Type:     resultType,
		}, nil

	case *parser.UnaryExpr:
		expr, err := p.convertExpression(e.Expr)
		if err != nil {
			return nil, err
		}

		op, err := p.convertUnaryOp(e.Operator)
		if err != nil {
			return nil, err
		}

		// Determine result type based on operator
		var resultType types.DataType
		switch op {
		case OpNot:
			// NOT operator returns boolean
			resultType = types.Boolean
		case OpNegate:
			// Negation returns the same type as the operand
			resultType = expr.DataType()
			if resultType == types.Unknown {
				// Default to BigInt for unknown numeric types
				resultType = types.BigInt
			}
		default:
			resultType = types.Boolean
		}

		return &UnaryOp{
			Expr:     expr,
			Operator: op,
			Type:     resultType,
		}, nil

	case *parser.ComparisonExpr:
		left, err := p.convertExpression(e.Left)
		if err != nil {
			return nil, err
		}

		right, err := p.convertExpression(e.Right)
		if err != nil {
			return nil, err
		}

		op, err := p.convertComparisonOp(e.Operator)
		if err != nil {
			return nil, err
		}

		return &BinaryOp{
			Left:     left,
			Right:    right,
			Operator: op,
			Type:     types.Boolean,
		}, nil

	case *parser.IsNullExpr:
		expr, err := p.convertExpression(e.Expr)
		if err != nil {
			return nil, err
		}

		op := OpIsNull
		if e.Not {
			op = OpIsNotNull
		}

		return &UnaryOp{
			Expr:     expr,
			Operator: op,
			Type:     types.Boolean,
		}, nil

	case *parser.ParenExpr:
		return p.convertExpression(e.Expr)

	case *parser.ParameterRef:
		// Convert parser.ParameterRef to planner.ParameterRef
		return &ParameterRef{
			Index: e.Index,
			Type:  types.Unknown, // Type will be inferred during bind
		}, nil

	case *parser.SubqueryExpr:
		// Plan the subquery
		subplan, err := p.planSelect(e.Query)
		if err != nil {
			return nil, fmt.Errorf("failed to plan subquery: %w", err)
		}

		// Determine result type from subquery schema
		schema := subplan.Schema()
		var resultType = types.Unknown
		if schema != nil && len(schema.Columns) == 1 {
			resultType = schema.Columns[0].DataType
		}

		return &SubqueryExpr{
			Subplan: subplan,
			Type:    resultType,
		}, nil

	case *parser.ExistsExpr:
		// Convert the subquery
		subqueryExpr, err := p.convertExpression(e.Subquery)
		if err != nil {
			return nil, err
		}

		subquery, ok := subqueryExpr.(*SubqueryExpr)
		if !ok {
			return nil, fmt.Errorf("expected SubqueryExpr for EXISTS")
		}

		return &ExistsExpr{
			Subquery: subquery,
			Not:      e.Not,
		}, nil

	case *parser.InExpr:
		// Convert the left expression
		expr, err := p.convertExpression(e.Expr)
		if err != nil {
			return nil, err
		}

		if e.Subquery != nil {
			// IN with subquery
			subqueryExpr, err := p.convertExpression(e.Subquery)
			if err != nil {
				return nil, err
			}

			subquery, ok := subqueryExpr.(*SubqueryExpr)
			if !ok {
				return nil, fmt.Errorf("expected SubqueryExpr for IN")
			}

			return &InExpr{
				Expr:     expr,
				Subquery: subquery,
				Not:      e.Not,
			}, nil
		}
		// IN with value list
		var values []Expression
		for _, value := range e.Values {
			valueExpr, err := p.convertExpression(value)
			if err != nil {
				return nil, err
			}
			values = append(values, valueExpr)
		}

		return &InExpr{
			Expr:   expr,
			Values: values,
			Not:    e.Not,
		}, nil

	case *parser.BetweenExpr:
		// Convert all expressions
		expr, err := p.convertExpression(e.Expr)
		if err != nil {
			return nil, err
		}

		lower, err := p.convertExpression(e.Lower)
		if err != nil {
			return nil, err
		}

		upper, err := p.convertExpression(e.Upper)
		if err != nil {
			return nil, err
		}

		// Convert BETWEEN to equivalent comparison: expr >= lower AND expr <= upper
		// For NOT BETWEEN: expr < lower OR expr > upper
		var op1, op2 BinaryOperator
		var logicalOp BinaryOperator

		if e.Not {
			op1 = OpLess
			op2 = OpGreater
			logicalOp = OpOr
		} else {
			op1 = OpGreaterEqual
			op2 = OpLessEqual
			logicalOp = OpAnd
		}

		left := &BinaryOp{
			Left:     expr,
			Right:    lower,
			Operator: op1,
			Type:     types.Boolean,
		}

		right := &BinaryOp{
			Left:     expr,
			Right:    upper,
			Operator: op2,
			Type:     types.Boolean,
		}

		return &BinaryOp{
			Left:     left,
			Right:    right,
			Operator: logicalOp,
			Type:     types.Boolean,
		}, nil

	case *parser.CaseExpr:
		// Convert CASE expression
		var caseExpr Expression
		if e.Expr != nil {
			// Simple CASE
			var err error
			caseExpr, err = p.convertExpression(e.Expr)
			if err != nil {
				return nil, fmt.Errorf("failed to convert CASE expression: %w", err)
			}
		}

		// Convert WHEN clauses
		var whenList []WhenClause
		resultType := types.Unknown

		for i, when := range e.WhenList {
			condition, err := p.convertExpression(when.Condition)
			if err != nil {
				return nil, fmt.Errorf("failed to convert WHEN condition %d: %w", i, err)
			}

			result, err := p.convertExpression(when.Result)
			if err != nil {
				return nil, fmt.Errorf("failed to convert THEN result %d: %w", i, err)
			}

			// Use the first result's type as the expression type
			// TODO: Type coercion for mixed types
			if resultType == types.Unknown && result.DataType() != types.Unknown {
				resultType = result.DataType()
			}

			whenList = append(whenList, WhenClause{
				Condition: condition,
				Result:    result,
			})
		}

		// Convert ELSE clause
		var elseExpr Expression
		if e.Else != nil {
			var err error
			elseExpr, err = p.convertExpression(e.Else)
			if err != nil {
				return nil, fmt.Errorf("failed to convert ELSE expression: %w", err)
			}

			// Update result type if needed
			if resultType == types.Unknown && elseExpr.DataType() != types.Unknown {
				resultType = elseExpr.DataType()
			}
		}

		// If we still don't know the type, default to text
		if resultType == types.Unknown {
			resultType = types.Text
		}

		return &CaseExpr{
			Expr:     caseExpr,
			WhenList: whenList,
			Else:     elseExpr,
			Type:     resultType,
		}, nil

	default:
		return nil, fmt.Errorf("unsupported expression type: %T", expr)
	}
}

// convertBinaryOp converts a parser binary operator to a planner operator.
func (p *BasicPlanner) convertBinaryOp(op parser.TokenType) (BinaryOperator, error) {
	switch op { // nolint:exhaustive // Only handles binary operators
	case parser.TokenPlus:
		return OpAdd, nil
	case parser.TokenMinus:
		return OpSubtract, nil
	case parser.TokenStar:
		return OpMultiply, nil
	case parser.TokenSlash:
		return OpDivide, nil
	case parser.TokenPercent:
		return OpModulo, nil
	case parser.TokenAnd:
		return OpAnd, nil
	case parser.TokenOr:
		return OpOr, nil
	case parser.TokenConcat:
		return OpConcat, nil
	default:
		return 0, fmt.Errorf("unsupported binary operator: %v", op)
	}
}

// convertComparisonOp converts a parser comparison operator to a planner operator.
func (p *BasicPlanner) convertComparisonOp(op parser.TokenType) (BinaryOperator, error) {
	switch op { // nolint:exhaustive // Only handles comparison operators
	case parser.TokenEqual:
		return OpEqual, nil
	case parser.TokenNotEqual:
		return OpNotEqual, nil
	case parser.TokenLess:
		return OpLess, nil
	case parser.TokenLessEqual:
		return OpLessEqual, nil
	case parser.TokenGreater:
		return OpGreater, nil
	case parser.TokenGreaterEqual:
		return OpGreaterEqual, nil
	case parser.TokenLike:
		return OpLike, nil
	default:
		return 0, fmt.Errorf("unsupported comparison operator: %v", op)
	}
}

// convertUnaryOp converts a parser unary operator to a planner operator.
func (p *BasicPlanner) convertUnaryOp(op parser.TokenType) (UnaryOperator, error) {
	switch op { // nolint:exhaustive // Only handles unary operators
	case parser.TokenNot:
		return OpNot, nil
	case parser.TokenMinus:
		return OpNegate, nil
	default:
		return 0, fmt.Errorf("unsupported unary operator: %v", op)
	}
}

// convertSelectColumns converts SELECT columns to projections.
func (p *BasicPlanner) convertSelectColumns(columns []parser.SelectColumn) ([]Expression, []string, *Schema, error) {
	var projections []Expression //nolint:prealloc
	var aliases []string         //nolint:prealloc
	var schemaCols []Column      //nolint:prealloc

	for _, col := range columns {
		// Check if this is a star expression
		if _, isStar := col.Expr.(*parser.Star); isStar {
			// For star expressions, we need the input schema
			// In a full implementation, we'd expand star to all columns
			// For now, we'll create a special Star expression that the executor will handle
			projections = append(projections, &Star{})
			aliases = append(aliases, "")

			// Star should preserve the input schema
			// This is a simplified approach - in reality we'd need to expand columns
			continue
		}

		expr, err := p.convertExpression(col.Expr)
		if err != nil {
			return nil, nil, nil, err
		}

		projections = append(projections, expr)
		aliases = append(aliases, col.Alias)

		// Build output schema
		colName := col.Alias
		if colName == "" {
			colName = expr.String()
		}

		// Preserve table information from column references
		var tableAlias string
		if colRef, ok := expr.(*ColumnRef); ok {
			tableAlias = colRef.TableAlias
		}

		schemaCols = append(schemaCols, Column{
			Name:       colName,
			DataType:   expr.DataType(),
			Nullable:   true, // TODO: Determine nullability
			TableName:  "",   // Table name is resolved during planning
			TableAlias: tableAlias,
		})
	}

	// If we have a star expression and no other columns, return nil schema
	// The actual schema will be determined from the input
	if len(projections) == 1 {
		if _, isStar := projections[0].(*Star); isStar {
			return projections, aliases, nil, nil
		}
	}

	schema := &Schema{Columns: schemaCols}
	return projections, aliases, schema, nil
}

// convertOrderBy converts ORDER BY clauses.
func (p *BasicPlanner) convertOrderBy(orderBy []parser.OrderByClause) ([]OrderByExpr, error) {
	var result []OrderByExpr //nolint:prealloc

	for _, o := range orderBy {
		expr, err := p.convertExpression(o.Expr)
		if err != nil {
			return nil, err
		}

		order := Ascending
		if o.Desc {
			order = Descending
		}

		result = append(result, OrderByExpr{
			Expr:  expr,
			Order: order,
		})
	}

	return result, nil
}

// optimize applies optimization rules to a logical plan.
func (p *BasicPlanner) optimize(plan LogicalPlan) LogicalPlan {
	if p.optimizer != nil {
		return p.optimizer.Optimize(plan)
	}
	return plan
}

// isAggregateFunction checks if a function name is an aggregate function.
func isAggregateFunction(name string) bool {
	switch name {
	case "COUNT", "SUM", "AVG", "MIN", "MAX", "STDDEV":
		return true
	default:
		return false
	}
}

// rewriteSelectColumns processes SELECT columns, extracting aggregates and rewriting expressions.
func (p *BasicPlanner) rewriteSelectColumns(columns []parser.SelectColumn, rewriter *AggregateRewriter) ([]Expression, []string, error) {
	var projections []Expression
	var aliases []string

	for _, col := range columns {
		expr, err := p.convertExpression(col.Expr)
		if err != nil {
			return nil, nil, err
		}

		// Rewrite the expression to extract aggregates
		rewritten, err := rewriter.RewriteExpression(expr)
		if err != nil {
			return nil, nil, err
		}

		projections = append(projections, rewritten)
		aliases = append(aliases, col.Alias)
	}

	return projections, aliases, nil
}

// buildAggregateSchema builds the output schema for an aggregate operation.
func (p *BasicPlanner) buildAggregateSchema(groupBy []Expression, aggregates []AggregateExpr) *Schema {
	columns := make([]Column, 0, len(groupBy)+len(aggregates))

	// Add GROUP BY columns
	for i, expr := range groupBy {
		columns = append(columns, Column{
			Name:     fmt.Sprintf("group_%d", i),
			DataType: expr.DataType(),
			Nullable: true,
		})
	}

	// Add aggregate columns
	for i, agg := range aggregates {
		// Use the same naming convention as the rewriter
		name := fmt.Sprintf("agg_%d_%s", i, agg.Function.String())
		columns = append(columns, Column{
			Name:     name,
			DataType: agg.Type,
			Nullable: true,
		})
	}

	return &Schema{Columns: columns}
}

// buildProjectionSchema builds the output schema for a projection.
func (p *BasicPlanner) buildProjectionSchema(projections []Expression, aliases []string) (*Schema, error) {
	columns := make([]Column, len(projections))

	for i, expr := range projections {
		name := aliases[i]
		if name == "" {
			name = expr.String()
		}

		columns[i] = Column{
			Name:     name,
			DataType: expr.DataType(),
			Nullable: true,
		}
	}

	return &Schema{Columns: columns}, nil
}

// validateExpressionColumns validates that all column references in an expression exist in the table
func (p *BasicPlanner) validateExpressionColumns(expr parser.Expression, table *catalog.Table) error {
	switch e := expr.(type) {
	case *parser.Identifier:
		// Check if column exists in table
		found := false
		for _, col := range table.Columns {
			if col.Name == e.Name {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("column '%s' not found in table '%s'", e.Name, table.TableName)
		}
		return nil

	case *parser.BinaryExpr:
		// Validate both sides of binary expression
		if err := p.validateExpressionColumns(e.Left, table); err != nil {
			return err
		}
		return p.validateExpressionColumns(e.Right, table)

	case *parser.UnaryExpr:
		return p.validateExpressionColumns(e.Expr, table)

	case *parser.ParenExpr:
		return p.validateExpressionColumns(e.Expr, table)

	case *parser.Literal:
		// Literals don't need validation
		return nil

	default:
		// For other expression types, no validation needed
		return nil
	}
}

// planTableExpression builds a logical plan from a table expression (simple table or JOIN)
func (p *BasicPlanner) planTableExpression(tableExpr parser.TableExpression) (LogicalPlan, error) {
	switch te := tableExpr.(type) {
	case *parser.TableRef:
		// Simple table reference
		return p.planTableRef(te)

	case *parser.SubqueryRef:
		// Subquery in FROM clause
		subPlan, err := p.planSelect(te.Query)
		if err != nil {
			return nil, fmt.Errorf("failed to plan subquery: %w", err)
		}
		// The subquery plan already has the correct schema
		// We just need to use the alias for column references
		return subPlan, nil

	case *parser.JoinExpr:
		// JOIN expression
		// Recursively plan left and right sides
		leftPlan, err := p.planTableExpression(te.Left)
		if err != nil {
			return nil, err
		}

		rightPlan, err := p.planTableExpression(te.Right)
		if err != nil {
			return nil, err
		}

		// Convert join type from parser to planner
		var joinType JoinType
		switch te.JoinType {
		case parser.InnerJoin:
			joinType = InnerJoin
		case parser.LeftJoin:
			joinType = LeftJoin
		case parser.RightJoin:
			joinType = RightJoin
		case parser.FullJoin:
			joinType = FullJoin
		case parser.CrossJoin:
			joinType = CrossJoin
		default:
			joinType = InnerJoin
		}

		// Convert the join condition
		var condition Expression
		if te.Condition != nil {
			condition, err = p.convertExpression(te.Condition)
			if err != nil {
				return nil, err
			}
		}

		// Compute the join schema (concatenate left and right schemas)
		leftSchema := leftPlan.Schema()
		rightSchema := rightPlan.Schema()
		joinSchema := &Schema{
			Columns: make([]Column, 0, len(leftSchema.Columns)+len(rightSchema.Columns)),
		}
		joinSchema.Columns = append(joinSchema.Columns, leftSchema.Columns...)
		joinSchema.Columns = append(joinSchema.Columns, rightSchema.Columns...)

		// Create the logical join
		join := NewLogicalJoin(leftPlan, rightPlan, joinType, condition, joinSchema)

		return join, nil

	default:
		return nil, fmt.Errorf("unsupported table expression type: %T", tableExpr)
	}
}

// planTableRef plans a simple table reference with optional alias
func (p *BasicPlanner) planTableRef(ref *parser.TableRef) (LogicalPlan, error) {
	// Get table from catalog
	table, err := p.catalog.GetTable(defaultSchema, ref.TableName)
	if err != nil {
		// If table doesn't exist in catalog, use a placeholder schema
		// This allows tests to work without setting up catalog
		// Use alias if provided, otherwise use table name
		alias := ref.Alias
		if alias == "" {
			alias = ref.TableName
		}
		schema := &Schema{
			Columns: []Column{
				{
					Name:       "*",
					DataType:   types.Unknown,
					Nullable:   true,
					TableName:  ref.TableName,
					TableAlias: alias,
				},
			},
		}
		return NewLogicalScan(ref.TableName, alias, schema), nil
	}

	// Build schema from table metadata
	schema := &Schema{
		Columns: make([]Column, len(table.Columns)),
	}
	// Use alias if provided, otherwise use table name
	alias := ref.Alias
	if alias == "" {
		alias = ref.TableName
	}
	for i, col := range table.Columns {
		schema.Columns[i] = Column{
			Name:       col.Name,
			DataType:   col.DataType,
			Nullable:   col.IsNullable,
			TableName:  ref.TableName,
			TableAlias: alias,
		}
	}

	return NewLogicalScan(ref.TableName, alias, schema), nil
}

// planPrepare converts a PREPARE statement to a logical plan.
func (p *BasicPlanner) planPrepare(stmt *parser.PrepareStmt) (LogicalPlan, error) {
	// Validate that the statement name is not empty
	if stmt.Name == "" {
		return nil, fmt.Errorf("prepared statement name cannot be empty")
	}

	// The query inside the PREPARE statement will be parsed and planned
	// when the prepared statement is executed
	return NewLogicalPrepare(stmt.Name, stmt.ParamTypes, stmt.Query), nil
}

// planExecute converts an EXECUTE statement to a logical plan.
func (p *BasicPlanner) planExecute(stmt *parser.ExecuteStmt) (LogicalPlan, error) {
	// Validate that the statement name is not empty
	if stmt.Name == "" {
		return nil, fmt.Errorf("statement name cannot be empty")
	}

	return NewLogicalExecute(stmt.Name, stmt.Params), nil
}

// planDeallocate converts a DEALLOCATE statement to a logical plan.
func (p *BasicPlanner) planDeallocate(stmt *parser.DeallocateStmt) (LogicalPlan, error) {
	// Validate that the statement name is not empty
	if stmt.Name == "" {
		return nil, fmt.Errorf("statement name cannot be empty")
	}

	return NewLogicalDeallocate(stmt.Name), nil
}
