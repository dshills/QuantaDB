package sql

import (
	"testing"

	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

// TestParameterSubstitution tests the parameter substitution functionality
func TestParameterSubstitution(t *testing.T) {
	t.Run("SubstituteExpression", func(t *testing.T) {
		// Create parameter values
		params := []types.Value{
			types.NewValue(int64(42)),
		}

		// Create substitutor
		substitutor := NewParameterSubstitutor(params)

		// Create a ParameterRef expression
		paramRef := &planner.ParameterRef{
			Index: 1,
			Type:  types.Integer,
		}

		// Substitute it
		result, err := substitutor.SubstituteInExpression(paramRef)
		if err != nil {
			t.Fatalf("SubstituteInExpression failed: %v", err)
		}

		// Should be a Literal now
		literal, ok := result.(*planner.Literal)
		if !ok {
			t.Fatalf("Expected Literal, got %T", result)
		}

		// Check the value
		if literal.Value.Data != int64(42) {
			t.Errorf("Expected value 42, got %v", literal.Value.Data)
		}
	})

	t.Run("SubstituteOutOfRange", func(t *testing.T) {
		// Create empty parameter list
		params := []types.Value{}

		// Create substitutor
		substitutor := NewParameterSubstitutor(params)

		// Create a ParameterRef expression
		paramRef := &planner.ParameterRef{
			Index: 1,
			Type:  types.Integer,
		}

		// Substitute should fail
		_, err := substitutor.SubstituteInExpression(paramRef)
		if err == nil {
			t.Fatal("Expected error for out of range parameter")
		}

		// Check error message
		if err.Error() != "parameter $1 out of range (have 0 parameters)" {
			t.Errorf("Unexpected error: %v", err)
		}
	})

	t.Run("SubstituteBinaryOp", func(t *testing.T) {
		// Create parameter values
		params := []types.Value{
			types.NewValue(int64(10)),
		}

		// Create substitutor
		substitutor := NewParameterSubstitutor(params)

		// Create a binary expression with a parameter
		binOp := &planner.BinaryOp{
			Left: &planner.Literal{
				Value: types.NewValue(int64(5)),
				Type:  types.Integer,
			},
			Right: &planner.ParameterRef{
				Index: 1,
				Type:  types.Integer,
			},
			Operator: planner.OpAdd,
			Type:     types.Integer,
		}

		// Substitute
		result, err := substitutor.SubstituteInExpression(binOp)
		if err != nil {
			t.Fatalf("SubstituteInExpression failed: %v", err)
		}

		// Should still be a BinaryOp
		resultBinOp, ok := result.(*planner.BinaryOp)
		if !ok {
			t.Fatalf("Expected BinaryOp, got %T", result)
		}

		// Right side should now be a literal
		rightLiteral, ok := resultBinOp.Right.(*planner.Literal)
		if !ok {
			t.Fatalf("Expected right side to be Literal, got %T", resultBinOp.Right)
		}

		if rightLiteral.Value.Data != int64(10) {
			t.Errorf("Expected right value 10, got %v", rightLiteral.Value.Data)
		}
	})
}

// TestParameterValueParsing tests parsing parameter values from wire protocol
func TestParameterValueParsing(t *testing.T) {
	t.Run("ParseIntegerValue", func(t *testing.T) {
		data := []byte("123")
		value, err := ParseParameterValue(data, types.Integer, 0)
		if err != nil {
			t.Fatalf("ParseParameterValue failed: %v", err)
		}

		if value.Data != int32(123) {
			t.Errorf("Expected int32(123), got %v (%T)", value.Data, value.Data)
		}
	})

	t.Run("ParseBooleanValue", func(t *testing.T) {
		// Test PostgreSQL-style boolean 't'
		data := []byte("t")
		value, err := ParseParameterValue(data, types.Boolean, 0)
		if err != nil {
			t.Fatalf("ParseParameterValue failed: %v", err)
		}

		if value.Data != true {
			t.Errorf("Expected true for 't', got %v", value.Data)
		}

		// Test PostgreSQL-style boolean 'f'
		data = []byte("f")
		value, err = ParseParameterValue(data, types.Boolean, 0)
		if err != nil {
			t.Fatalf("ParseParameterValue failed: %v", err)
		}

		if value.Data != false {
			t.Errorf("Expected false for 'f', got %v", value.Data)
		}
	})

	t.Run("ParseNullValue", func(t *testing.T) {
		data := []byte{}
		value, err := ParseParameterValue(data, types.Integer, 0)
		if err != nil {
			t.Fatalf("ParseParameterValue failed: %v", err)
		}

		if !value.IsNull() {
			t.Error("Expected null value for empty data")
		}
	})

	t.Run("ParseUnknownType", func(t *testing.T) {
		// With unknown type, should try to infer
		data := []byte("42")
		value, err := ParseParameterValue(data, types.Unknown, 0)
		if err != nil {
			t.Fatalf("ParseParameterValue failed: %v", err)
		}

		// Should infer as integer (int32 for values that fit)
		if value.Data != int32(42) {
			t.Errorf("Expected 42 as int32, got %v (%T)", value.Data, value.Data)
		}
	})
}

// TestParameterTypeInferenceBasic tests type inference for parameters
func TestParameterTypeInferenceBasic(t *testing.T) {
	t.Run("InferSingleParameter", func(t *testing.T) {
		p := parser.NewParser("SELECT * FROM users WHERE id = $1")
		stmt, err := p.Parse()
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		types, err := InferParameterTypes(stmt)
		if err != nil {
			t.Fatalf("InferParameterTypes failed: %v", err)
		}

		if len(types) != 1 {
			t.Errorf("Expected 1 parameter type, got %d", len(types))
		}
	})

	t.Run("InferMultipleParameters", func(t *testing.T) {
		p := parser.NewParser("INSERT INTO users (id, name, active) VALUES ($1, $2, $3)")
		stmt, err := p.Parse()
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		types, err := InferParameterTypes(stmt)
		if err != nil {
			t.Fatalf("InferParameterTypes failed: %v", err)
		}

		if len(types) != 3 {
			t.Errorf("Expected 3 parameter types, got %d", len(types))
		}
	})

	t.Run("NoParameters", func(t *testing.T) {
		p := parser.NewParser("SELECT * FROM users")
		stmt, err := p.Parse()
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		types, err := InferParameterTypes(stmt)
		if err != nil {
			t.Fatalf("InferParameterTypes failed: %v", err)
		}

		if len(types) != 0 {
			t.Errorf("Expected 0 parameter types, got %d", len(types))
		}
	})
}
