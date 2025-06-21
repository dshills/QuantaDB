package executor

import (
	"fmt"

	"github.com/dshills/QuantaDB/internal/sql/types"
)

// BitmapAndOperator performs a logical AND operation on multiple bitmaps.
type BitmapAndOperator struct {
	baseOperator
	children []Operator
	bitmap   *Bitmap
	isOpen   bool
}

// NewBitmapAndOperator creates a new bitmap AND operator.
func NewBitmapAndOperator(children []Operator) *BitmapAndOperator {
	schema := &Schema{
		Columns: []Column{{Name: "bitmap", Type: types.Unknown}},
	}

	return &BitmapAndOperator{
		baseOperator: baseOperator{
			schema: schema,
		},
		children: children,
	}
}

// Open initializes the bitmap AND operation.
func (op *BitmapAndOperator) Open(ctx *ExecContext) error {
	op.ctx = ctx

	// Open all child operators
	for _, child := range op.children {
		if err := child.Open(ctx); err != nil {
			return fmt.Errorf("failed to open child operator: %w", err)
		}
	}

	// Build the result bitmap
	if err := op.buildBitmap(); err != nil {
		return fmt.Errorf("failed to build AND bitmap: %w", err)
	}

	op.isOpen = true
	return nil
}

// buildBitmap performs AND operation on all child bitmaps.
func (op *BitmapAndOperator) buildBitmap() error {
	if len(op.children) == 0 {
		op.bitmap = NewBitmap()
		return nil
	}

	// Get first bitmap - check for BitmapOperator interface
	firstBitmapOp, ok := op.children[0].(BitmapOperator)
	if !ok {
		return fmt.Errorf("child operator is not a bitmap scan")
	}
	op.bitmap = firstBitmapOp.GetBitmap()

	// AND with remaining bitmaps
	for i := 1; i < len(op.children); i++ {
		bitmapOp, ok := op.children[i].(BitmapOperator)
		if !ok {
			return fmt.Errorf("child operator %d is not a bitmap scan", i)
		}
		op.bitmap = op.bitmap.And(bitmapOp.GetBitmap())
	}

	return nil
}

// GetBitmap returns the result bitmap.
func (op *BitmapAndOperator) GetBitmap() *Bitmap {
	return op.bitmap
}

// Next is not used for bitmap operations.
func (op *BitmapAndOperator) Next() (*Row, error) {
	return nil, fmt.Errorf("bitmap AND does not support Next() - use GetBitmap()")
}

// Close cleans up the operator.
func (op *BitmapAndOperator) Close() error {
	for _, child := range op.children {
		if err := child.Close(); err != nil {
			return err
		}
	}
	op.isOpen = false
	if op.bitmap != nil {
		op.bitmap.Clear()
	}
	return nil
}

// BitmapOrOperator performs a logical OR operation on multiple bitmaps.
type BitmapOrOperator struct {
	baseOperator
	children []Operator
	bitmap   *Bitmap
	isOpen   bool
}

// NewBitmapOrOperator creates a new bitmap OR operator.
func NewBitmapOrOperator(children []Operator) *BitmapOrOperator {
	schema := &Schema{
		Columns: []Column{{Name: "bitmap", Type: types.Unknown}},
	}

	return &BitmapOrOperator{
		baseOperator: baseOperator{
			schema: schema,
		},
		children: children,
	}
}

// Open initializes the bitmap OR operation.
func (op *BitmapOrOperator) Open(ctx *ExecContext) error {
	op.ctx = ctx

	// Open all child operators
	for _, child := range op.children {
		if err := child.Open(ctx); err != nil {
			return fmt.Errorf("failed to open child operator: %w", err)
		}
	}

	// Build the result bitmap
	if err := op.buildBitmap(); err != nil {
		return fmt.Errorf("failed to build OR bitmap: %w", err)
	}

	op.isOpen = true
	return nil
}

// buildBitmap performs OR operation on all child bitmaps.
func (op *BitmapOrOperator) buildBitmap() error {
	op.bitmap = NewBitmap()

	// OR all child bitmaps
	for i, child := range op.children {
		bitmapOp, ok := child.(BitmapOperator)
		if !ok {
			return fmt.Errorf("child operator %d is not a bitmap operator", i)
		}
		op.bitmap = op.bitmap.Or(bitmapOp.GetBitmap())
	}

	return nil
}

// GetBitmap returns the result bitmap.
func (op *BitmapOrOperator) GetBitmap() *Bitmap {
	return op.bitmap
}

// Next is not used for bitmap operations.
func (op *BitmapOrOperator) Next() (*Row, error) {
	return nil, fmt.Errorf("bitmap OR does not support Next() - use GetBitmap()")
}

// Close cleans up the operator.
func (op *BitmapOrOperator) Close() error {
	for _, child := range op.children {
		if err := child.Close(); err != nil {
			return err
		}
	}
	op.isOpen = false
	if op.bitmap != nil {
		op.bitmap.Clear()
	}
	return nil
}
