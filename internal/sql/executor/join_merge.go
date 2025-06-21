package executor

import (
	"fmt"

	"github.com/dshills/QuantaDB/internal/sql/types"
)

// MergeJoinOperator implements the sort-merge join algorithm
type MergeJoinOperator struct {
	leftChild     Operator
	rightChild    Operator
	joinType      JoinType
	joinCondition ExprEvaluator
	leftSortKeys  []int // Column indices to sort/compare on
	rightSortKeys []int // Column indices to sort/compare on

	// Execution state
	leftIter      PeekableIterator
	rightIter     PeekableIterator
	leftRow       *Row
	rightRow      *Row
	leftGroup     []*Row // For handling duplicates
	rightGroup    []*Row // For handling duplicates
	groupLeftIdx  int    // Current position in left group
	groupRightIdx int    // Current position in right group

	// For outer joins
	leftMatched  bool
	rightMatched bool

	// Schema info
	leftSchema   *Schema
	rightSchema  *Schema
	outputSchema *Schema
}

// NewMergeJoinOperator creates a new merge join operator
func NewMergeJoinOperator(
	left, right Operator,
	joinType JoinType,
	joinCondition ExprEvaluator,
	leftKeys, rightKeys []int,
) *MergeJoinOperator {
	return &MergeJoinOperator{
		leftChild:     left,
		rightChild:    right,
		joinType:      joinType,
		joinCondition: joinCondition,
		leftSortKeys:  leftKeys,
		rightSortKeys: rightKeys,
	}
}

// Next returns the next joined row
func (m *MergeJoinOperator) Next() (*Row, error) {
	// Initialize iterators on first call
	if m.leftIter == nil {
		if err := m.initialize(); err != nil {
			return nil, err
		}
	}

	for {
		// Try to produce a row from current groups
		if row := m.produceFromGroups(); row != nil {
			return row, nil
		}

		// Need to advance to next groups
		if !m.advanceToNextMatch() {
			// No more matches
			return m.handleEndOfInput()
		}
	}
}

// initialize sets up the iterators and schemas
func (m *MergeJoinOperator) initialize() error {
	// Get schemas
	m.leftSchema = m.leftChild.Schema()
	m.rightSchema = m.rightChild.Schema()
	m.outputSchema = m.createOutputSchema()

	// Create sorted iterators
	leftSorted, err := m.ensureSorted(m.leftChild, m.leftSortKeys)
	if err != nil {
		return fmt.Errorf("failed to sort left input: %w", err)
	}
	m.leftIter = ensurePeekable(leftSorted)

	rightSorted, err := m.ensureSorted(m.rightChild, m.rightSortKeys)
	if err != nil {
		return fmt.Errorf("failed to sort right input: %w", err)
	}
	m.rightIter = ensurePeekable(rightSorted)

	// Get first rows
	m.leftRow, _ = m.leftIter.Next()
	m.rightRow, _ = m.rightIter.Next()

	return nil
}

// ensureSorted returns a sorted iterator for the input
func (m *MergeJoinOperator) ensureSorted(input Operator, sortKeys []int) (SimpleRowIterator, error) {
	// Check if already sorted
	if sortInfo := m.detectSortOrder(input); sortInfo != nil && sortInfo.matchesKeys(sortKeys) {
		// Already sorted correctly
		return &operatorIterator{op: input}, nil
	}

	// Need to sort
	sorter := NewExternalSort(sortKeys, m.createCompareFn(sortKeys))
	return sorter.Sort(&operatorIterator{op: input})
}

// detectSortOrder checks if the operator provides sorted output
func (m *MergeJoinOperator) detectSortOrder(op Operator) *SortInfo {
	// TODO: Implement detection for IndexScanOperator, SortOperator, etc.
	// For now, assume not sorted
	return nil
}

// createCompareFn creates a comparison function for the given keys
func (m *MergeJoinOperator) createCompareFn(keys []int) func(*Row, *Row) int {
	return func(a, b *Row) int {
		for _, keyIdx := range keys {
			cmp := compareJoinValues(a.Values[keyIdx], b.Values[keyIdx])
			if cmp != 0 {
				return cmp
			}
		}
		return 0
	}
}

// compareJoinValues compares two values for join operations
func compareJoinValues(a, b types.Value) int {
	// Handle NULLs
	if a.IsNull() && b.IsNull() {
		return 0
	}
	if a.IsNull() {
		return -1 // NULLs sort first
	}
	if b.IsNull() {
		return 1
	}

	// Compare based on type
	switch a.Type() {
	case types.Integer:
		aInt, _ := a.AsInt()
		bInt, _ := b.AsInt()
		if aInt < bInt {
			return -1
		} else if aInt > bInt {
			return 1
		}
		return 0

	case types.Text:
		aStr, _ := a.AsString()
		bStr, _ := b.AsString()
		if aStr < bStr {
			return -1
		} else if aStr > bStr {
			return 1
		}
		return 0

	case types.Boolean:
		aBool, _ := a.AsBool()
		bBool, _ := b.AsBool()
		if !aBool && bBool {
			return -1
		} else if aBool && !bBool {
			return 1
		}
		return 0

	default:
		// For other types, use string representation
		return compareJoinValues(types.NewTextValue(a.String()), types.NewTextValue(b.String()))
	}
}

// produceFromGroups tries to produce a row from current groups
func (m *MergeJoinOperator) produceFromGroups() *Row {
	// Check if we have groups to process
	if len(m.leftGroup) == 0 || len(m.rightGroup) == 0 {
		return nil
	}

	// Check if we've exhausted current groups
	if m.groupLeftIdx >= len(m.leftGroup) {
		return nil
	}

	// Get current rows from groups
	leftRow := m.leftGroup[m.groupLeftIdx]
	rightRow := m.rightGroup[m.groupRightIdx]

	// Advance position in groups
	m.groupRightIdx++
	if m.groupRightIdx >= len(m.rightGroup) {
		m.groupRightIdx = 0
		m.groupLeftIdx++
	}

	// Mark rows as matched for outer joins
	m.leftMatched = true
	m.rightMatched = true

	// Combine rows and check join condition
	combined := m.combineRows(leftRow, rightRow)
	if m.evaluateJoinCondition(combined) {
		return combined
	}

	// Condition failed, try next combination
	return m.produceFromGroups()
}

// advanceToNextMatch advances iterators to find next matching groups
func (m *MergeJoinOperator) advanceToNextMatch() bool {
	// Clear current groups
	m.leftGroup = nil
	m.rightGroup = nil
	m.groupLeftIdx = 0
	m.groupRightIdx = 0

	for m.leftRow != nil && m.rightRow != nil {
		// Compare join keys
		cmp := m.compareJoinKeys(m.leftRow, m.rightRow)

		switch {
		case cmp < 0:
			// Left < Right: advance left
			if !m.leftMatched && (m.joinType == LeftJoin || m.joinType == FullJoin) {
				// Output unmatched left row
				m.leftGroup = []*Row{m.leftRow}
				m.rightGroup = []*Row{m.createNullRow(m.rightSchema)}
				m.leftRow, _ = m.leftIter.Next()
				m.leftMatched = false
				return true
			}
			m.leftRow, _ = m.leftIter.Next()
			m.leftMatched = false

		case cmp > 0:
			// Left > Right: advance right
			if !m.rightMatched && (m.joinType == RightJoin || m.joinType == FullJoin) {
				// Output unmatched right row
				m.leftGroup = []*Row{m.createNullRow(m.leftSchema)}
				m.rightGroup = []*Row{m.rightRow}
				m.rightRow, _ = m.rightIter.Next()
				m.rightMatched = false
				return true
			}
			m.rightRow, _ = m.rightIter.Next()
			m.rightMatched = false

		default:
			// Equal: collect matching groups
			m.leftGroup = m.collectGroup(m.leftIter, m.leftRow, true)
			m.rightGroup = m.collectGroup(m.rightIter, m.rightRow, false)

			// Advance past the groups
			if len(m.leftGroup) > 0 {
				m.leftRow, _ = m.leftIter.Next()
			}
			if len(m.rightGroup) > 0 {
				m.rightRow, _ = m.rightIter.Next()
			}

			m.leftMatched = false
			m.rightMatched = false

			return true
		}
	}

	return false
}

// collectGroup collects all rows with the same join key
func (m *MergeJoinOperator) collectGroup(iter PeekableIterator, firstRow *Row, isLeft bool) []*Row {
	group := []*Row{firstRow}

	// Determine which keys to use
	sortKeys := m.leftSortKeys
	if !isLeft {
		sortKeys = m.rightSortKeys
	}

	// Create compare function for this side
	compareFn := m.createCompareFn(sortKeys)

	// Collect all rows with same key
	for {
		// Peek at next row
		nextRow, err := iter.Peek()
		if err != nil || nextRow == nil {
			break
		}

		// Check if same key
		if compareFn(firstRow, nextRow) != 0 {
			break
		}

		// Consume the row
		row, _ := iter.Next()
		group = append(group, row)
	}

	return group
}

// compareJoinKeys compares the join keys of two rows
func (m *MergeJoinOperator) compareJoinKeys(left, right *Row) int {
	for i := 0; i < len(m.leftSortKeys); i++ {
		leftVal := left.Values[m.leftSortKeys[i]]
		rightVal := right.Values[m.rightSortKeys[i]]

		cmp := compareJoinValues(leftVal, rightVal)
		if cmp != 0 {
			return cmp
		}
	}
	return 0
}

// handleEndOfInput handles outer join logic when one input is exhausted
func (m *MergeJoinOperator) handleEndOfInput() (*Row, error) {
	// For left/full outer join, output remaining left rows
	if (m.joinType == LeftJoin || m.joinType == FullJoin) && m.leftRow != nil {
		result := m.combineRows(m.leftRow, m.createNullRow(m.rightSchema))
		m.leftRow, _ = m.leftIter.Next()
		return result, nil
	}

	// For right/full outer join, output remaining right rows
	if (m.joinType == RightJoin || m.joinType == FullJoin) && m.rightRow != nil {
		result := m.combineRows(m.createNullRow(m.leftSchema), m.rightRow)
		m.rightRow, _ = m.rightIter.Next()
		return result, nil
	}

	return nil, nil //nolint:nilnil // EOF - standard iterator pattern
}

// combineRows combines a left and right row
func (m *MergeJoinOperator) combineRows(left, right *Row) *Row {
	values := make([]types.Value, 0, len(left.Values)+len(right.Values))
	values = append(values, left.Values...)
	values = append(values, right.Values...)

	return &Row{Values: values}
}

// createNullRow creates a row with all NULL values
func (m *MergeJoinOperator) createNullRow(schema *Schema) *Row {
	values := make([]types.Value, len(schema.Columns))
	for i := range values {
		values[i] = types.NewNullValue()
	}
	return &Row{Values: values}
}

// evaluateJoinCondition evaluates the join condition on a combined row
func (m *MergeJoinOperator) evaluateJoinCondition(row *Row) bool {
	if m.joinCondition == nil {
		return true // No additional condition
	}

	// Create a minimal ExecContext for evaluation
	ctx := &ExecContext{}
	result, err := m.joinCondition.Eval(row, ctx)
	if err != nil {
		return false
	}

	boolVal, err := result.AsBool()
	if err != nil {
		return false
	}

	return boolVal
}

// createOutputSchema creates the schema for joined rows
func (m *MergeJoinOperator) createOutputSchema() *Schema {
	columns := make([]Column, 0, len(m.leftSchema.Columns)+len(m.rightSchema.Columns))

	// Add left columns
	for _, col := range m.leftSchema.Columns {
		columns = append(columns, Column{
			Name:     "left." + col.Name,
			Type:     col.Type,
			Nullable: col.Nullable,
		})
	}

	// Add right columns
	for _, col := range m.rightSchema.Columns {
		columns = append(columns, Column{
			Name:     "right." + col.Name,
			Type:     col.Type,
			Nullable: col.Nullable,
		})
	}

	return &Schema{Columns: columns}
}

// Open initializes the operator
func (m *MergeJoinOperator) Open(ctx *ExecContext) error {
	// Open child operators
	if err := m.leftChild.Open(ctx); err != nil {
		return err
	}
	return m.rightChild.Open(ctx)
}

// Schema returns the output schema
func (m *MergeJoinOperator) Schema() *Schema {
	if m.outputSchema == nil {
		m.outputSchema = m.createOutputSchema()
	}
	return m.outputSchema
}

// Close cleans up resources
func (m *MergeJoinOperator) Close() error {
	var err error

	if m.leftIter != nil {
		if e := m.leftIter.Close(); e != nil {
			err = e
		}
	}
	if m.rightIter != nil {
		if e := m.rightIter.Close(); e != nil && err == nil {
			err = e
		}
	}

	// Close child operators
	if e := m.leftChild.Close(); e != nil && err == nil {
		err = e
	}
	if e := m.rightChild.Close(); e != nil && err == nil {
		err = e
	}

	return err
}

// Helper types and functions

// SortInfo describes the sort order of an operator's output
type SortInfo struct {
	IsSorted bool
	SortKeys []int
}

func (s *SortInfo) matchesKeys(keys []int) bool {
	if !s.IsSorted || len(s.SortKeys) != len(keys) {
		return false
	}
	for i, key := range keys {
		if s.SortKeys[i] != key {
			return false
		}
	}
	return true
}

// operatorIterator wraps an Operator as a RowIterator
type operatorIterator struct {
	op Operator
}

func (o *operatorIterator) Next() (*Row, error) {
	return o.op.Next()
}

func (o *operatorIterator) Close() error {
	return o.op.Close()
}
