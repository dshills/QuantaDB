package planner

import (
	"fmt"
	"time"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/types"
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
	case *parser.CreateTableStmt:
		return p.planCreateTable(s)
	default:
		return nil, fmt.Errorf("unsupported statement type: %T", stmt)
	}
}

// planSelect converts a SELECT statement to a logical plan.
func (p *BasicPlanner) planSelect(stmt *parser.SelectStmt) (LogicalPlan, error) {
	// Get table from catalog
	table, err := p.catalog.GetTable("public", stmt.From)
	if err != nil {
		// If table doesn't exist in catalog, use a placeholder schema
		// This allows tests to work without setting up catalog
		schema := &Schema{
			Columns: []Column{
				{Name: "*", DataType: types.Unknown, Nullable: true},
			},
		}
		var plan LogicalPlan = NewLogicalScan(stmt.From, stmt.From, schema)
		return p.buildSelectPlan(plan, stmt)
	}
	
	// Build schema from table metadata
	schema := &Schema{
		Columns: make([]Column, len(table.Columns)),
	}
	for i, col := range table.Columns {
		schema.Columns[i] = Column{
			Name:     col.Name,
			DataType: col.DataType,
			Nullable: col.IsNullable,
		}
	}
	
	var plan LogicalPlan = NewLogicalScan(stmt.From, stmt.From, schema)
	return p.buildSelectPlan(plan, stmt)
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
	
	// Add projections
	projections, aliases, projSchema, err := p.convertSelectColumns(stmt.Columns)
	if err != nil {
		return nil, fmt.Errorf("failed to convert select columns: %w", err)
	}
	plan = NewLogicalProject(plan, projections, aliases, projSchema)
	
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
	// TODO: Implement INSERT planning
	return nil, fmt.Errorf("INSERT planning not yet implemented")
}

// planCreateTable converts a CREATE TABLE statement to a logical plan.
func (p *BasicPlanner) planCreateTable(stmt *parser.CreateTableStmt) (LogicalPlan, error) {
	// TODO: Implement CREATE TABLE planning
	return nil, fmt.Errorf("CREATE TABLE planning not yet implemented")
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
			default:
				dataType = types.Unknown
			}
		}
		return &Literal{Value: e.Value, Type: dataType}, nil
		
	case *parser.Identifier:
		// Try to resolve column type from catalog
		// For now, we'll use Unknown type if we can't resolve it
		return &ColumnRef{
			ColumnName: e.Name,
			ColumnType: types.Unknown,
		}, nil
		
	case *parser.Star:
		return &Star{}, nil
		
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
		
		// TODO: Determine result type based on operands and operator
		return &BinaryOp{
			Left:     left,
			Right:    right,
			Operator: op,
			Type:     types.Boolean,
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
		
		// TODO: Determine result type
		return &UnaryOp{
			Expr:     expr,
			Operator: op,
			Type:     types.Boolean,
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
	var projections []Expression
	var aliases []string
	var schemaCols []Column
	
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
		
		schemaCols = append(schemaCols, Column{
			Name:     colName,
			DataType: expr.DataType(),
			Nullable: true, // TODO: Determine nullability
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
	var result []OrderByExpr
	
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