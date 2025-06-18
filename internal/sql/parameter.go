package sql

import (
	"fmt"
	"strconv"

	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

// ParameterSubstitutor substitutes parameter placeholders with actual values.
type ParameterSubstitutor struct {
	values []types.Value
}

// NewParameterSubstitutor creates a new parameter substitutor.
func NewParameterSubstitutor(values []types.Value) *ParameterSubstitutor {
	return &ParameterSubstitutor{values: values}
}

// SubstituteInPlan substitutes parameters in a logical plan.
func (s *ParameterSubstitutor) SubstituteInPlan(plan planner.LogicalPlan) (planner.LogicalPlan, error) {
	// For now, we'll implement a simple version that walks the plan tree
	// In a full implementation, this would use the visitor pattern
	return plan, nil
}

// SubstituteInExpression substitutes parameters in an expression.
func (s *ParameterSubstitutor) SubstituteInExpression(expr planner.Expression) (planner.Expression, error) {
	switch e := expr.(type) {
	case *planner.ParameterRef:
		if e.Index < 1 || e.Index > len(s.values) {
			return nil, fmt.Errorf("parameter $%d out of range (have %d parameters)", e.Index, len(s.values))
		}
		// Replace parameter with literal value
		value := s.values[e.Index-1]
		return &planner.Literal{
			Value: value,
			Type:  e.Type,
		}, nil

	case *planner.BinaryOp:
		left, err := s.SubstituteInExpression(e.Left)
		if err != nil {
			return nil, err
		}
		right, err := s.SubstituteInExpression(e.Right)
		if err != nil {
			return nil, err
		}
		return &planner.BinaryOp{
			Left:     left,
			Right:    right,
			Operator: e.Operator,
			Type:     e.Type,
		}, nil

	case *planner.UnaryOp:
		expr, err := s.SubstituteInExpression(e.Expr)
		if err != nil {
			return nil, err
		}
		return &planner.UnaryOp{
			Operator: e.Operator,
			Expr:     expr,
			Type:     e.Type,
		}, nil

	default:
		// Other expression types don't contain parameters
		return expr, nil
	}
}

// InferParameterTypes attempts to infer parameter types from query context.
func InferParameterTypes(stmt parser.Statement) ([]types.DataType, error) {
	inference := &parameterTypeInference{
		paramTypes: make(map[int]types.DataType),
		maxIndex:   0,
	}

	// Walk the AST to infer types
	if err := inference.inferFromStatement(stmt); err != nil {
		return nil, err
	}

	// If no parameters found, return empty slice
	if inference.maxIndex == 0 {
		return []types.DataType{}, nil
	}

	// Build result array
	result := make([]types.DataType, inference.maxIndex)
	for i := 0; i < inference.maxIndex; i++ {
		if typ, ok := inference.paramTypes[i+1]; ok {
			result[i] = typ
		} else {
			result[i] = types.Unknown // Type not inferred
		}
	}

	return result, nil
}

// parameterTypeInference helps infer parameter types.
type parameterTypeInference struct {
	paramTypes map[int]types.DataType
	maxIndex   int
}

func (p *parameterTypeInference) inferFromStatement(stmt parser.Statement) error {
	switch s := stmt.(type) {
	case *parser.SelectStmt:
		if s.Where != nil {
			return p.inferFromExpression(s.Where, nil)
		}

	case *parser.InsertStmt:
		// For INSERT, we can infer from column types
		// This requires catalog access which we don't have here
		// For now, just track the parameters in VALUES
		for _, row := range s.Values {
			for _, val := range row {
				if err := p.inferFromExpression(val, nil); err != nil {
					return err
				}
			}
		}

	case *parser.UpdateStmt:
		for _, assign := range s.Assignments {
			if err := p.inferFromExpression(assign.Value, nil); err != nil {
				return err
			}
		}
		if s.Where != nil {
			return p.inferFromExpression(s.Where, nil)
		}

	case *parser.DeleteStmt:
		if s.Where != nil {
			return p.inferFromExpression(s.Where, nil)
		}
	}

	return nil
}

func (p *parameterTypeInference) inferFromExpression(expr parser.Expression, expectedType types.DataType) error {
	switch e := expr.(type) {
	case *parser.ParameterRef:
		// Track the maximum parameter index
		if e.Index > p.maxIndex {
			p.maxIndex = e.Index
		}

		if expectedType != nil && expectedType != types.Unknown {
			// We have a type hint from context
			if existing, ok := p.paramTypes[e.Index]; ok {
				// Check for type conflicts
				if existing != expectedType {
					return fmt.Errorf("conflicting types for parameter $%d", e.Index)
				}
			} else {
				p.paramTypes[e.Index] = expectedType
			}
		}

	case *parser.ComparisonExpr:
		// For comparisons, both sides should have compatible types
		leftType := p.getExpressionType(e.Left)
		rightType := p.getExpressionType(e.Right)

		// First, recurse to find all parameters
		if err := p.inferFromExpression(e.Left, nil); err != nil {
			return err
		}
		if err := p.inferFromExpression(e.Right, nil); err != nil {
			return err
		}

		// Then try to infer types based on context
		if leftType != nil && leftType != types.Unknown {
			if err := p.inferFromExpression(e.Right, leftType); err != nil {
				return err
			}
		}
		if rightType != nil && rightType != types.Unknown {
			if err := p.inferFromExpression(e.Left, rightType); err != nil {
				return err
			}
		}

	case *parser.BinaryExpr:
		if err := p.inferFromExpression(e.Left, nil); err != nil {
			return err
		}
		if err := p.inferFromExpression(e.Right, nil); err != nil {
			return err
		}

	case *parser.UnaryExpr:
		if err := p.inferFromExpression(e.Expr, nil); err != nil {
			return err
		}

	case *parser.InExpr:
		if err := p.inferFromExpression(e.Expr, nil); err != nil {
			return err
		}
		for _, val := range e.Values {
			if err := p.inferFromExpression(val, nil); err != nil {
				return err
			}
		}

	case *parser.BetweenExpr:
		if err := p.inferFromExpression(e.Expr, nil); err != nil {
			return err
		}
		if err := p.inferFromExpression(e.Lower, nil); err != nil {
			return err
		}
		if err := p.inferFromExpression(e.Upper, nil); err != nil {
			return err
		}
	}

	return nil
}

func (p *parameterTypeInference) getExpressionType(expr parser.Expression) types.DataType {
	switch e := expr.(type) {
	case *parser.Literal:
		// Infer type from literal value
		switch e.Value.Data.(type) {
		case int64:
			return types.Integer
		case float64:
			return types.Decimal(10, 2) // Default precision/scale
		case string:
			return types.Varchar(255) // Default max length
		case bool:
			return types.Boolean
		default:
			return types.Unknown
		}

	case *parser.Identifier:
		// Would need catalog to look up column type
		return types.Unknown

	default:
		return types.Unknown
	}
}

// ParseParameterValue parses a parameter value from wire protocol format.
func ParseParameterValue(data []byte, dataType types.DataType, format int16) (types.Value, error) {
	if len(data) == 0 {
		return types.NewNullValue(), nil
	}

	if format == 1 {
		// Binary format - not implemented yet
		return types.Value{}, fmt.Errorf("binary parameter format not supported")
	}

	// Text format
	text := string(data)

	// If type is unknown, try to infer from text
	if dataType == types.Unknown || dataType == nil {
		// Try integer
		if i, err := strconv.ParseInt(text, 10, 64); err == nil {
			return types.NewValue(i), nil
		}
		// Try float
		if f, err := strconv.ParseFloat(text, 64); err == nil {
			return types.NewValue(f), nil
		}
		// Try boolean
		if text == "true" || text == "t" {
			return types.NewValue(true), nil
		}
		if text == "false" || text == "f" {
			return types.NewValue(false), nil
		}
		// Default to string
		return types.NewValue(text), nil
	}

	// Parse according to specified type
	// We need to compare by type name since some types are functions
	switch dataType.Name() {
	case "INTEGER":
		i, err := strconv.ParseInt(text, 10, 64)
		if err != nil {
			return types.Value{}, fmt.Errorf("invalid integer value: %s", text)
		}
		return types.NewValue(i), nil

	case "BOOLEAN":
		b, err := strconv.ParseBool(text)
		if err != nil {
			// PostgreSQL compatibility
			if text == "t" {
				return types.NewValue(true), nil
			}
			if text == "f" {
				return types.NewValue(false), nil
			}
			return types.Value{}, fmt.Errorf("invalid boolean value: %s", text)
		}
		return types.NewValue(b), nil

	default:
		// For VARCHAR, TEXT, and other types, store as string
		return types.NewValue(text), nil
	}
}

