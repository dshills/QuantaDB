package executor

import (
	"fmt"
	"io"

	"github.com/dshills/QuantaDB/internal/sql/types"
)

// SubqueryOperator executes a subquery and caches the result.
type SubqueryOperator struct {
	subplan   Operator
	cached    bool
	result    *types.Value
	resultSet []*Row
	isScalar  bool
	isOpen    bool
}

// NewSubqueryOperator creates a new subquery operator.
func NewSubqueryOperator(subplan Operator, isScalar bool) *SubqueryOperator {
	return &SubqueryOperator{
		subplan:  subplan,
		isScalar: isScalar,
		cached:   false,
	}
}

// Open opens the subquery operator.
func (op *SubqueryOperator) Open(ctx *ExecContext) error {
	op.isOpen = true
	return op.subplan.Open(ctx)
}

// Next executes the subquery and returns the result.
func (op *SubqueryOperator) Next() (*Row, error) {
	if !op.isOpen {
		return nil, fmt.Errorf("subquery operator not open")
	}

	// If already cached, return cached result
	if op.cached {
		if op.isScalar {
			if op.result == nil {
				return nil, io.EOF
			}
			// Return a row with the scalar value
			row := &Row{
				Values: []types.Value{*op.result},
			}
			op.result = nil // Only return once
			return row, nil
		} else {
			// For non-scalar subqueries, this would be used differently
			// in the context of EXISTS/IN evaluation
			return nil, nil // EOF for now
		}
	}

	// Execute the subquery
	err := op.executeSubquery()
	if err != nil {
		return nil, err
	}

	// Mark as cached and return result
	op.cached = true
	return op.Next() // Recursive call to return cached result
}

// Close closes the subquery operator.
func (op *SubqueryOperator) Close() error {
	op.isOpen = false
	return op.subplan.Close()
}

// Schema returns the schema of the subquery.
func (op *SubqueryOperator) Schema() *Schema {
	return op.subplan.Schema()
}

// executeSubquery executes the subquery and caches the result.
func (op *SubqueryOperator) executeSubquery() error {
	if op.isScalar {
		// For scalar subqueries, we expect exactly one row with one value
		row, err := op.subplan.Next()
		if err != nil {
			return err
		}

		if row == nil {
			// No rows returned - scalar subquery returns NULL
			nullValue := types.NullValue(types.Unknown)
			op.result = &nullValue
			return nil
		}

		if len(row.Values) != 1 {
			return fmt.Errorf("scalar subquery must return exactly one column")
		}

		// Store the scalar result
		op.result = &row.Values[0]

		// Check if there are more rows
		nextRow, err := op.subplan.Next()
		if err != nil {
			return err
		}
		if nextRow != nil {
			return fmt.Errorf("scalar subquery returned more than one row")
		}

		return nil
	} else {
		// For non-scalar subqueries (EXISTS, IN), we collect all rows
		op.resultSet = make([]*Row, 0)
		for {
			row, err := op.subplan.Next()
			if err != nil {
				return err
			}
			if row == nil {
				break // EOF
			}
			op.resultSet = append(op.resultSet, row)
		}
		return nil
	}
}

// HasResults returns true if the subquery returned any rows (for EXISTS).
func (op *SubqueryOperator) HasResults() (bool, error) {
	if !op.cached {
		err := op.executeSubquery()
		if err != nil {
			return false, err
		}
		op.cached = true
	}

	return len(op.resultSet) > 0, nil
}

// GetScalarResult returns the scalar result (for scalar subqueries).
func (op *SubqueryOperator) GetScalarResult() (*types.Value, error) {
	if !op.cached {
		err := op.executeSubquery()
		if err != nil {
			return nil, err
		}
		op.cached = true
	}

	return op.result, nil
}

// ContainsValue checks if the result set contains a specific value (for IN).
func (op *SubqueryOperator) ContainsValue(value types.Value) (bool, error) {
	if !op.cached {
		err := op.executeSubquery()
		if err != nil {
			return false, err
		}
		op.cached = true
	}

	// Check each row for the value
	for _, row := range op.resultSet {
		if len(row.Values) != 1 {
			continue // Skip rows that don't have exactly one column
		}

		// Compare values
		if row.Values[0].Equal(value) {
			return true, nil
		}
	}

	return false, nil
}

// String returns a string representation.
func (op *SubqueryOperator) String() string {
	if op.isScalar {
		return "ScalarSubquery"
	}
	return "Subquery"
}
