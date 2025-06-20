package executor

import (
	"errors"
	"fmt"
)

// Common errors for executor package
var (
	// ErrTableNotFound is returned when a table doesn't exist
	ErrTableNotFound = errors.New("table not found")

	// ErrRowNotFound is returned when a row doesn't exist
	ErrRowNotFound = errors.New("row not found")

	// ErrRowNotVisible is returned when a row exists but is not visible to the current transaction
	ErrRowNotVisible = errors.New("row not visible to current transaction")

	// ErrRowDeleted is returned when trying to access a deleted row
	ErrRowDeleted = errors.New("row has been deleted")

	// ErrSizeMismatch is returned when trying to update a row with different size
	ErrSizeMismatch = errors.New("cannot update row in place: size changed")

	// ErrInvalidRowID is returned when a row ID is invalid
	ErrInvalidRowID = errors.New("invalid row ID")
)

// TableNotFoundError provides detailed information about a missing table
type TableNotFoundError struct {
	TableID int64
}

func (e *TableNotFoundError) Error() string {
	return fmt.Sprintf("table %d not found", e.TableID)
}

func (e *TableNotFoundError) Is(target error) bool {
	return target == ErrTableNotFound
}

// NewTableNotFoundError creates a new TableNotFoundError
func NewTableNotFoundError(tableID int64) error {
	return &TableNotFoundError{TableID: tableID}
}
