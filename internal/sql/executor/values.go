package executor

import (
	"github.com/dshills/QuantaDB/internal/sql/types"
)

// ValuesOperator returns constant values without scanning any table.
// Used for queries like SELECT 1, SELECT 'hello' that don't have a FROM clause.
type ValuesOperator struct {
	baseOperator
	rows    [][]types.Value
	current int
}

// NewValuesOperator creates a new values operator.
func NewValuesOperator(rows [][]types.Value, schema *Schema) *ValuesOperator {
	return &ValuesOperator{
		baseOperator: baseOperator{
			schema: schema,
		},
		rows:    rows,
		current: -1,
	}
}

// Open initializes the operator.
func (v *ValuesOperator) Open(ctx *ExecContext) error {
	v.ctx = ctx
	v.current = -1
	return nil
}

// Next returns the next row.
func (v *ValuesOperator) Next() (*Row, error) {
	v.current++
	if v.current >= len(v.rows) {
		return nil, nil // EOF
	}

	return &Row{Values: v.rows[v.current]}, nil
}

// Close cleans up resources.
func (v *ValuesOperator) Close() error {
	return nil
}
