package executor

// SimpleRowIterator is a simplified iterator interface for merge join
type SimpleRowIterator interface {
	Next() (*Row, error)
	Close() error
}

// BufferedIterator wraps a SimpleRowIterator to support peeking
type BufferedIterator struct {
	base      SimpleRowIterator
	buffer    *Row
	hasBuffer bool
}

// NewBufferedIterator creates a new buffered iterator
func NewBufferedIterator(base SimpleRowIterator) *BufferedIterator {
	return &BufferedIterator{
		base: base,
	}
}

// Next returns the next row
func (b *BufferedIterator) Next() (*Row, error) {
	if b.hasBuffer {
		row := b.buffer
		b.buffer = nil
		b.hasBuffer = false
		return row, nil
	}

	return b.base.Next()
}

// Peek returns the next row without consuming it
func (b *BufferedIterator) Peek() (*Row, error) {
	if b.hasBuffer {
		return b.buffer, nil
	}

	row, err := b.base.Next()
	if err != nil {
		return nil, err
	}

	if row != nil {
		b.buffer = row
		b.hasBuffer = true
	}

	return row, nil
}

// Close closes the iterator
func (b *BufferedIterator) Close() error {
	return b.base.Close()
}

// PeekableIterator is an interface for iterators that support peeking
type PeekableIterator interface {
	SimpleRowIterator
	Peek() (*Row, error)
}

// ensurePeekable wraps an iterator to make it peekable if needed
func ensurePeekable(iter SimpleRowIterator) PeekableIterator {
	if peekable, ok := iter.(PeekableIterator); ok {
		return peekable
	}
	return NewBufferedIterator(iter)
}
