package executor

import (
	"fmt"
	"sort"

	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

// SortOperator implements ORDER BY.
type SortOperator struct {
	baseOperator
	child    Operator
	orderBy  []OrderByExpr
	rows     []*Row
	rowIndex int
	sorted   bool
}

// OrderByExpr represents an ORDER BY expression.
type OrderByExpr struct {
	Expr  ExprEvaluator
	Order planner.SortOrder
}

// NewSortOperator creates a new sort operator.
func NewSortOperator(child Operator, orderBy []OrderByExpr) *SortOperator {
	return &SortOperator{
		baseOperator: baseOperator{
			schema: child.Schema(),
		},
		child:   child,
		orderBy: orderBy,
	}
}

// Open initializes the sort operator.
func (s *SortOperator) Open(ctx *ExecContext) error {
	s.ctx = ctx
	s.rows = nil
	s.rowIndex = 0
	s.sorted = false

	return s.child.Open(ctx)
}

// Next returns the next sorted row.
func (s *SortOperator) Next() (*Row, error) {
	// First time: collect all rows and sort them
	if !s.sorted {
		if err := s.collectAndSort(); err != nil {
			return nil, err
		}
		s.sorted = true
	}

	// Return rows in sorted order
	if s.rowIndex >= len(s.rows) {
		return nil, nil // nolint:nilnil // EOF
	}

	row := s.rows[s.rowIndex]
	s.rowIndex++

	if s.ctx.Stats != nil {
		s.ctx.Stats.RowsReturned++
	}

	return row, nil
}

// collectAndSort collects all rows from the child and sorts them.
func (s *SortOperator) collectAndSort() error {
	// Collect all rows
	for {
		row, err := s.child.Next()
		if err != nil {
			return fmt.Errorf("error reading row for sort: %w", err)
		}
		if row == nil {
			break // EOF
		}

		// Create a copy of the row to avoid issues with reused buffers
		rowCopy := &Row{
			Values: make([]types.Value, len(row.Values)),
		}
		copy(rowCopy.Values, row.Values)
		s.rows = append(s.rows, rowCopy)
	}

	// Sort the rows
	if len(s.orderBy) > 0 && len(s.rows) > 0 {
		sorter := &rowSorter{
			rows:    s.rows,
			orderBy: s.orderBy,
			ctx:     s.ctx,
		}
		sort.Sort(sorter)

		if sorter.err != nil {
			return fmt.Errorf("error during sort: %w", sorter.err)
		}
	}

	return nil
}

// Close cleans up the sort operator.
func (s *SortOperator) Close() error {
	s.rows = nil
	return s.child.Close()
}

// rowSorter implements sort.Interface for sorting rows.
type rowSorter struct {
	rows    []*Row
	orderBy []OrderByExpr
	ctx     *ExecContext
	err     error
}

func (rs *rowSorter) Len() int {
	return len(rs.rows)
}

func (rs *rowSorter) Less(i, j int) bool {
	if rs.err != nil {
		return false
	}

	// Compare based on each ORDER BY expression
	for _, orderExpr := range rs.orderBy {
		// Evaluate expressions for both rows
		valI, err := orderExpr.Expr.Eval(rs.rows[i], rs.ctx)
		if err != nil {
			rs.err = err
			return false
		}

		valJ, err := orderExpr.Expr.Eval(rs.rows[j], rs.ctx)
		if err != nil {
			rs.err = err
			return false
		}

		// Compare values
		cmp := compareValues(valI, valJ)

		if cmp != 0 {
			// Apply sort order (ASC or DESC)
			if orderExpr.Order == planner.Descending {
				return cmp > 0
			}
			return cmp < 0
		}
		// If equal, continue to next ORDER BY expression
	}

	// All ORDER BY expressions are equal
	return false
}

func (rs *rowSorter) Swap(i, j int) {
	rs.rows[i], rs.rows[j] = rs.rows[j], rs.rows[i]
}

// compareValues compares two values for sorting.
// Returns: -1 if a < b, 0 if a == b, 1 if a > b
// NULL values are considered less than non-NULL values.
func compareValues(a, b types.Value) int {
	// Handle NULLs
	if a.IsNull() && b.IsNull() {
		return 0
	}
	if a.IsNull() {
		return -1
	}
	if b.IsNull() {
		return 1
	}

	// Compare based on type
	switch av := a.Data.(type) {
	case int64:
		switch bv := b.Data.(type) {
		case int64:
			if av < bv {
				return -1
			}
			if av > bv {
				return 1
			}
			return 0
		case float64:
			af := float64(av)
			if af < bv {
				return -1
			}
			if af > bv {
				return 1
			}
			return 0
		}
	case float64:
		switch bv := b.Data.(type) {
		case int64:
			bf := float64(bv)
			if av < bf {
				return -1
			}
			if av > bf {
				return 1
			}
			return 0
		case float64:
			if av < bv {
				return -1
			}
			if av > bv {
				return 1
			}
			return 0
		}
	case string:
		if bv, ok := b.Data.(string); ok {
			if av < bv {
				return -1
			}
			if av > bv {
				return 1
			}
			return 0
		}
	case bool:
		if bv, ok := b.Data.(bool); ok {
			// false < true
			if !av && bv {
				return -1
			}
			if av && !bv {
				return 1
			}
			return 0
		}
	}

	// Type mismatch or unsupported type
	return 0
}
