package executor

import (
	"fmt"
	"hash/fnv"
	"time"

	"github.com/dshills/QuantaDB/internal/sql/types"
)

// JoinType represents the type of join.
type JoinType int

const (
	InnerJoin JoinType = iota
	LeftJoin
	RightJoin
	FullJoin
)

// NestedLoopJoinOperator implements a nested loop join.
type NestedLoopJoinOperator struct {
	baseOperator
	left      Operator
	right     Operator
	predicate ExprEvaluator
	joinType  JoinType
	leftRow   *Row
	rightOpen bool
}

// NewNestedLoopJoinOperator creates a new nested loop join operator.
func NewNestedLoopJoinOperator(left, right Operator, predicate ExprEvaluator, joinType JoinType) *NestedLoopJoinOperator {
	// Build combined schema
	leftSchema := left.Schema()
	rightSchema := right.Schema()

	columns := make([]Column, 0, len(leftSchema.Columns)+len(rightSchema.Columns))

	// Copy columns from left side, preserving table information
	columns = append(columns, leftSchema.Columns...)

	// Copy columns from right side, preserving table information
	columns = append(columns, rightSchema.Columns...)

	schema := &Schema{Columns: columns}

	return &NestedLoopJoinOperator{
		baseOperator: baseOperator{
			schema: schema,
		},
		left:      left,
		right:     right,
		predicate: predicate,
		joinType:  joinType,
	}
}

// Open initializes the join operator.
func (j *NestedLoopJoinOperator) Open(ctx *ExecContext) error {
	j.ctx = ctx
	j.leftRow = nil
	j.rightOpen = false

	// Open left child
	if err := j.left.Open(ctx); err != nil {
		return fmt.Errorf("failed to open left child: %w", err)
	}

	// Open right child
	if err := j.right.Open(ctx); err != nil {
		return fmt.Errorf("failed to open right child: %w", err)
	}

	return nil
}

// Next returns the next joined row.
func (j *NestedLoopJoinOperator) Next() (*Row, error) {
	for {
		// Get next left row if needed
		if j.leftRow == nil {
			var err error
			j.leftRow, err = j.left.Next()
			if err != nil {
				return nil, fmt.Errorf("error reading left row: %w", err)
			}
			if j.leftRow == nil {
				// No more left rows - EOF using standard iterator pattern
				return nil, nil // nolint:nilnil
			}

			// Reset right child for new left row
			if j.rightOpen {
				if err := j.right.Close(); err != nil {
					return nil, fmt.Errorf("failed to close right child: %w", err)
				}
			}
			if err := j.right.Open(j.ctx); err != nil {
				return nil, fmt.Errorf("failed to reopen right child: %w", err)
			}
			j.rightOpen = true
		}

		// Get next right row
		rightRow, err := j.right.Next()
		if err != nil {
			return nil, fmt.Errorf("error reading right row: %w", err)
		}

		if rightRow == nil {
			// No more right rows for this left row
			j.leftRow = nil
			continue
		}

		// Join the rows
		joinedRow := j.joinRows(j.leftRow, rightRow)

		// Evaluate join predicate
		if j.predicate != nil {
			result, err := j.predicate.Eval(joinedRow, j.ctx)
			if err != nil {
				return nil, fmt.Errorf("error evaluating join predicate: %w", err)
			}

			// Skip if predicate is false or NULL
			if result.IsNull() || (result.Data != nil && !result.Data.(bool)) {
				continue
			}
		}

		if j.ctx.Stats != nil {
			j.ctx.Stats.RowsReturned++
		}

		return joinedRow, nil
	}
}

// joinRows combines left and right rows into a single row.
func (j *NestedLoopJoinOperator) joinRows(left, right *Row) *Row {
	values := make([]types.Value, len(left.Values)+len(right.Values))
	copy(values, left.Values)
	copy(values[len(left.Values):], right.Values)

	return &Row{Values: values}
}

// Close cleans up the join operator.
func (j *NestedLoopJoinOperator) Close() error {
	var leftErr, rightErr error

	if j.left != nil {
		leftErr = j.left.Close()
	}

	if j.right != nil {
		rightErr = j.right.Close()
	}

	if leftErr != nil {
		return leftErr
	}

	return rightErr
}

// HashJoinOperator implements a hash join.
type HashJoinOperator struct {
	baseOperator
	left      Operator
	right     Operator
	leftKeys  []ExprEvaluator
	rightKeys []ExprEvaluator
	predicate ExprEvaluator
	joinType  JoinType
	hashTable map[uint64][]*Row
	rightRows []*Row
	built     bool
	leftRow   *Row
	matchIdx  int
}

// NewHashJoinOperator creates a new hash join operator.
func NewHashJoinOperator(left, right Operator, leftKeys, rightKeys []ExprEvaluator,
	predicate ExprEvaluator, joinType JoinType) *HashJoinOperator {
	// Build combined schema
	leftSchema := left.Schema()
	rightSchema := right.Schema()

	columns := make([]Column, 0, len(leftSchema.Columns)+len(rightSchema.Columns))
	columns = append(columns, leftSchema.Columns...)
	columns = append(columns, rightSchema.Columns...)

	schema := &Schema{Columns: columns}

	return &HashJoinOperator{
		baseOperator: baseOperator{
			schema: schema,
		},
		left:      left,
		right:     right,
		leftKeys:  leftKeys,
		rightKeys: rightKeys,
		predicate: predicate,
		joinType:  joinType,
		hashTable: make(map[uint64][]*Row),
	}
}

// Open initializes the hash join operator.
func (h *HashJoinOperator) Open(ctx *ExecContext) error {
	h.ctx = ctx
	h.built = false
	h.leftRow = nil
	h.matchIdx = 0

	// Initialize statistics collection
	// TODO: Get estimated rows from planner
	h.initStats(1000) // Default estimate

	// Open children
	if err := h.left.Open(ctx); err != nil {
		return fmt.Errorf("failed to open left child: %w", err)
	}

	if err := h.right.Open(ctx); err != nil {
		return fmt.Errorf("failed to open right child: %w", err)
	}

	return nil
}

// Next returns the next joined row.
func (h *HashJoinOperator) Next() (*Row, error) {
	// Build hash table on first call
	if !h.built {
		if err := h.buildHashTable(); err != nil {
			return nil, err
		}
		h.built = true
	}

	for {
		// Get next left row if needed
		if h.leftRow == nil || h.matchIdx >= len(h.rightRows) {
			var err error
			h.leftRow, err = h.left.Next()
			if err != nil {
				return nil, fmt.Errorf("error reading left row: %w", err)
			}
			if h.leftRow == nil {
				// No more left rows - EOF using standard iterator pattern
				return nil, nil // nolint:nilnil
			}

			// Find matching right rows
			hash, err := h.hashRow(h.leftRow, h.leftKeys)
			if err != nil {
				return nil, fmt.Errorf("error hashing left row: %w", err)
			}

			h.rightRows = h.hashTable[hash]
			h.matchIdx = 0

			if len(h.rightRows) == 0 && h.joinType == InnerJoin {
				// No matches for inner join, try next left row
				h.leftRow = nil
				continue
			}
		}

		// For inner join with no matches, we already continued above
		if len(h.rightRows) == 0 {
			// For left join, return left row with NULLs for right side
			if h.joinType == LeftJoin {
				rightSchema := h.right.Schema()
				nullRight := &Row{
					Values: make([]types.Value, len(rightSchema.Columns)),
				}
				for i := range nullRight.Values {
					nullRight.Values[i] = types.NewNullValue()
				}
				joinedRow := h.joinRows(h.leftRow, nullRight)
				h.leftRow = nil // Move to next left row
				return joinedRow, nil
			}
			h.leftRow = nil
			continue
		}

		// Get current match
		rightRow := h.rightRows[h.matchIdx]
		h.matchIdx++

		// Join the rows
		joinedRow := h.joinRows(h.leftRow, rightRow)

		// Evaluate additional join predicate if present
		if h.predicate != nil {
			result, err := h.predicate.Eval(joinedRow, h.ctx)
			if err != nil {
				return nil, fmt.Errorf("error evaluating join predicate: %w", err)
			}

			// Skip if predicate is false or NULL
			if result.IsNull() || (result.Data != nil && !result.Data.(bool)) {
				continue
			}
		}

		if h.ctx.Stats != nil {
			h.ctx.Stats.RowsReturned++
		}

		// Record row produced
		h.recordRow()

		return joinedRow, nil
	}
}

// buildHashTable builds the hash table from the right child.
func (h *HashJoinOperator) buildHashTable() error {
	buildStart := time.Now()
	rowCount := 0

	for {
		row, err := h.right.Next()
		if err != nil {
			return fmt.Errorf("error building hash table: %w", err)
		}
		if row == nil {
			break
		}

		// Hash the row
		hash, err := h.hashRow(row, h.rightKeys)
		if err != nil {
			return fmt.Errorf("error hashing right row: %w", err)
		}

		// Add to hash table
		h.hashTable[hash] = append(h.hashTable[hash], row)
		rowCount++
	}

	// Record hash table statistics
	if h.stats != nil {
		buildTimeMs := time.Since(buildStart).Seconds() * 1000
		h.stats.ExtraInfo["Hash Build Time"] = fmt.Sprintf("%.2f ms", buildTimeMs)
		h.stats.ExtraInfo["Hash Buckets"] = fmt.Sprintf("%d", len(h.hashTable))
		h.stats.ExtraInfo["Hash Rows"] = fmt.Sprintf("%d", rowCount)

		// Estimate memory usage (rough approximation)
		// Each row pointer is 8 bytes, plus map overhead
		memoryKB := int64((rowCount*8 + len(h.hashTable)*32) / 1024)
		h.stats.MemoryUsedKB = memoryKB
	}

	return nil
}

// hashRow computes a hash for the given row using the specified key expressions.
func (h *HashJoinOperator) hashRow(row *Row, keys []ExprEvaluator) (uint64, error) {
	hasher := fnv.New64()

	for _, key := range keys {
		val, err := key.Eval(row, h.ctx)
		if err != nil {
			return 0, fmt.Errorf("error evaluating key: %w", err)
		}

		// Hash the value
		writeValueToHasher(hasher, val)
	}

	return hasher.Sum64(), nil
}

// joinRows combines left and right rows into a single row.
func (h *HashJoinOperator) joinRows(left, right *Row) *Row {
	values := make([]types.Value, len(left.Values)+len(right.Values))
	copy(values, left.Values)
	copy(values[len(left.Values):], right.Values)

	return &Row{Values: values}
}

// Close cleans up the hash join operator.
func (h *HashJoinOperator) Close() error {
	// Finalize and report statistics
	h.finishStats()
	if h.ctx != nil && h.ctx.StatsCollector != nil && h.stats != nil {
		h.ctx.StatsCollector(h, h.stats)
	}

	h.hashTable = nil
	h.rightRows = nil

	var leftErr, rightErr error

	if h.left != nil {
		leftErr = h.left.Close()
	}

	if h.right != nil {
		rightErr = h.right.Close()
	}

	if leftErr != nil {
		return leftErr
	}

	return rightErr
}
