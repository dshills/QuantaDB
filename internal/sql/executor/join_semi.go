package executor

import (
	"fmt"
	"hash/fnv"

	"github.com/dshills/QuantaDB/internal/sql/types"
)

// SemiJoinType represents the type of semi/anti join
type SemiJoinType int

const (
	SemiJoinTypeSemi SemiJoinType = iota
	SemiJoinTypeAnti
)

// SemiJoinOperator implements semi and anti joins for EXISTS/IN predicates
type SemiJoinOperator struct {
	baseOperator
	left          Operator
	right         Operator
	leftKeys      []ExprEvaluator
	rightKeys     []ExprEvaluator
	joinCondition ExprEvaluator
	joinType      SemiJoinType

	// For NULL handling in NOT IN
	hasNullHandling bool
	rightHasNull    bool

	// Execution state
	hashTable map[uint64]struct{} // We only need existence, not the rows
	built     bool
}

// NewSemiJoinOperator creates a new semi/anti join operator
func NewSemiJoinOperator(
	left, right Operator,
	leftKeys, rightKeys []ExprEvaluator,
	joinCondition ExprEvaluator,
	joinType SemiJoinType,
	hasNullHandling bool,
) *SemiJoinOperator {
	return &SemiJoinOperator{
		baseOperator: baseOperator{
			schema: left.Schema(), // Semi/anti joins only return left schema
		},
		left:            left,
		right:           right,
		leftKeys:        leftKeys,
		rightKeys:       rightKeys,
		joinCondition:   joinCondition,
		joinType:        joinType,
		hasNullHandling: hasNullHandling,
		hashTable:       make(map[uint64]struct{}),
	}
}

// Open initializes the operator
func (s *SemiJoinOperator) Open(ctx *ExecContext) error {
	s.ctx = ctx
	s.built = false
	s.rightHasNull = false

	// Open children
	if err := s.left.Open(ctx); err != nil {
		return fmt.Errorf("failed to open left child: %w", err)
	}

	if err := s.right.Open(ctx); err != nil {
		return fmt.Errorf("failed to open right child: %w", err)
	}

	return nil
}

// Next returns the next row
func (s *SemiJoinOperator) Next() (*Row, error) {
	// Build hash table on first call
	if !s.built {
		if err := s.buildHashTable(); err != nil {
			return nil, err
		}
		s.built = true
	}

	return processSemiJoinRows(s.left, s.joinType, s.hasNullHandling, s.rightHasNull, s.checkMatch)
}

// buildHashTable builds the hash table from right input
func (s *SemiJoinOperator) buildHashTable() error {
	for {
		rightRow, err := s.right.Next()
		if err != nil {
			return fmt.Errorf("error reading right row: %w", err)
		}
		if rightRow == nil {
			break // End of right input
		}

		// Extract and hash join keys
		hash, hasNull, err := s.hashRightRow(rightRow)
		if err != nil {
			return err
		}

		// Track if we've seen NULL values (important for NOT IN)
		if hasNull {
			s.rightHasNull = true
			if s.hasNullHandling && s.joinType == SemiJoinTypeAnti {
				// For NOT IN, if right has NULL, we can short-circuit
				return nil
			}
			continue // Skip NULL keys
		}

		// Check additional join condition if present
		// For hash table building, we can't evaluate the full condition
		// Just mark that this hash value exists

		s.hashTable[hash] = struct{}{}
	}

	return nil
}

// checkMatch checks if a left row has a match in the hash table
func (s *SemiJoinOperator) checkMatch(leftRow *Row) (bool, error) {
	// Extract and hash join keys
	hash, hasNull, err := s.hashLeftRow(leftRow)
	if err != nil {
		return false, err
	}

	// NULL keys never match
	if hasNull {
		return false, nil
	}

	// Check hash table
	_, exists := s.hashTable[hash]
	if !exists {
		return false, nil
	}

	// If we have additional join conditions, we need to do a full scan
	// This is a limitation of the current hash-based approach
	if s.joinCondition != nil {
		// For now, we assume hash match is sufficient
		// TODO: Implement proper condition checking with stored rows
		return true, nil
	}

	return true, nil
}

// hashLeftRow computes hash of left row join keys
func (s *SemiJoinOperator) hashLeftRow(row *Row) (uint64, bool, error) {
	return s.hashJoinKeys(row, s.leftKeys, s.ctx)
}

// hashRightRow computes hash of right row join keys
func (s *SemiJoinOperator) hashRightRow(row *Row) (uint64, bool, error) {
	return s.hashJoinKeys(row, s.rightKeys, s.ctx)
}

// hashJoinKeys computes hash of join key values
func (s *SemiJoinOperator) hashJoinKeys(row *Row, keys []ExprEvaluator, ctx *ExecContext) (uint64, bool, error) {
	h := fnv.New64()
	hasNull := false

	for _, keyEval := range keys {
		val, err := keyEval.Eval(row, ctx)
		if err != nil {
			return 0, false, err
		}

		if val.IsNull() {
			hasNull = true
			continue
		}

		// Write type identifier
		switch val.Type() {
		case types.Integer:
			h.Write([]byte{1})
			v, _ := val.AsInt()
			h.Write([]byte{byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)})
		case types.Text:
			h.Write([]byte{2})
			v, _ := val.AsString()
			h.Write([]byte(v))
		case types.Boolean:
			h.Write([]byte{3})
			v, _ := val.AsBool()
			if v {
				h.Write([]byte{1})
			} else {
				h.Write([]byte{0})
			}
		default:
			// Use string representation for other types
			h.Write([]byte{4})
			h.Write([]byte(val.String()))
		}
	}

	return h.Sum64(), hasNull, nil
}

// Close cleans up resources
func (s *SemiJoinOperator) Close() error {
	var leftErr, rightErr error

	if s.left != nil {
		leftErr = s.left.Close()
	}

	if s.right != nil {
		rightErr = s.right.Close()
	}

	// Clear hash table
	s.hashTable = make(map[uint64]struct{})
	s.built = false

	if leftErr != nil {
		return leftErr
	}

	return rightErr
}

// processSemiJoinRows contains the common logic for processing rows in semi/anti joins
func processSemiJoinRows(
	left Operator,
	joinType SemiJoinType,
	hasNullHandling bool,
	rightHasNull bool,
	checkMatch func(*Row) (bool, error),
) (*Row, error) {
	// For NOT IN with NULL in right side, return no rows
	if joinType == SemiJoinTypeAnti && hasNullHandling && rightHasNull {
		return nil, nil
	}

	// Process left rows
	for {
		leftRow, err := left.Next()
		if err != nil {
			return nil, err
		}
		if leftRow == nil {
			return nil, nil // EOF
		}

		// Check if row matches
		matches, err := checkMatch(leftRow)
		if err != nil {
			return nil, err
		}

		// For semi join, return rows that match
		// For anti join, return rows that don't match
		if (joinType == SemiJoinTypeSemi && matches) ||
			(joinType == SemiJoinTypeAnti && !matches) {
			return leftRow, nil
		}
	}
}

// NestedLoopSemiJoinOperator implements semi/anti join using nested loops
// This is used when we need to evaluate complex join conditions
type NestedLoopSemiJoinOperator struct {
	baseOperator
	left            Operator
	right           Operator
	joinCondition   ExprEvaluator
	joinType        SemiJoinType
	hasNullHandling bool

	// Execution state
	rightRows    []Row // Materialized right side
	built        bool
	rightHasNull bool
}

// NewNestedLoopSemiJoinOperator creates a new nested loop semi/anti join
func NewNestedLoopSemiJoinOperator(
	left, right Operator,
	joinCondition ExprEvaluator,
	joinType SemiJoinType,
	hasNullHandling bool,
) *NestedLoopSemiJoinOperator {
	return &NestedLoopSemiJoinOperator{
		baseOperator: baseOperator{
			schema: left.Schema(),
		},
		left:            left,
		right:           right,
		joinCondition:   joinCondition,
		joinType:        joinType,
		hasNullHandling: hasNullHandling,
	}
}

// Open initializes the operator
func (n *NestedLoopSemiJoinOperator) Open(ctx *ExecContext) error {
	n.ctx = ctx
	n.built = false
	n.rightHasNull = false
	n.rightRows = nil

	// Open children
	if err := n.left.Open(ctx); err != nil {
		return fmt.Errorf("failed to open left child: %w", err)
	}

	if err := n.right.Open(ctx); err != nil {
		return fmt.Errorf("failed to open right child: %w", err)
	}

	return nil
}

// Next returns the next row
func (n *NestedLoopSemiJoinOperator) Next() (*Row, error) {
	// Materialize right side on first call
	if !n.built {
		if err := n.materializeRight(); err != nil {
			return nil, err
		}
		n.built = true
	}

	return processSemiJoinRows(n.left, n.joinType, n.hasNullHandling, n.rightHasNull, n.checkMatch)
}

// materializeRight loads all right rows into memory
func (n *NestedLoopSemiJoinOperator) materializeRight() error {
	for {
		rightRow, err := n.right.Next()
		if err != nil {
			return fmt.Errorf("error reading right row: %w", err)
		}
		if rightRow == nil {
			break
		}

		// Check for NULLs if needed for NOT IN
		if n.hasNullHandling && n.joinType == SemiJoinTypeAnti {
			for _, val := range rightRow.Values {
				if val.IsNull() {
					n.rightHasNull = true
					break
				}
			}
		}

		n.rightRows = append(n.rightRows, *rightRow)
	}

	return nil
}

// checkMatch checks if left row matches any right row
func (n *NestedLoopSemiJoinOperator) checkMatch(leftRow *Row) (bool, error) {
	// Try each right row
	for i := range n.rightRows {
		rightRow := &n.rightRows[i]

		// Combine rows for condition evaluation
		combinedRow := &Row{
			Values: make([]types.Value, 0, len(leftRow.Values)+len(rightRow.Values)),
		}
		combinedRow.Values = append(combinedRow.Values, leftRow.Values...)
		combinedRow.Values = append(combinedRow.Values, rightRow.Values...)

		// Evaluate join condition
		if n.joinCondition != nil {
			result, err := n.joinCondition.Eval(combinedRow, n.ctx)
			if err != nil {
				return false, err
			}

			// NULL results are treated as false
			if !result.IsNull() {
				if b, _ := result.AsBool(); b {
					// Found a match, can return early for semi join
					return true, nil
				}
			}
		} else {
			// No condition means all rows match (shouldn't happen in practice)
			return true, nil
		}
	}

	// No match found
	return false, nil
}

// Close cleans up resources
func (n *NestedLoopSemiJoinOperator) Close() error {
	var leftErr, rightErr error

	if n.left != nil {
		leftErr = n.left.Close()
	}

	if n.right != nil {
		rightErr = n.right.Close()
	}

	// Clear materialized rows
	n.rightRows = nil
	n.built = false

	if leftErr != nil {
		return leftErr
	}

	return rightErr
}
