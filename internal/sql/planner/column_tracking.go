package planner

import (
	"sort"
	"strings"
)

// ColumnSet represents a set of columns referenced in a query
type ColumnSet struct {
	columns map[ColumnRef]bool
	hasStar bool // true if SELECT * is used
}

// NewColumnSet creates an empty column set
func NewColumnSet() *ColumnSet {
	return &ColumnSet{
		columns: make(map[ColumnRef]bool),
		hasStar: false,
	}
}

// Note: ColumnRef is defined in expression.go

// Add adds a column to the set
func (cs *ColumnSet) Add(col ColumnRef) {
	cs.columns[col] = true
}

// AddAll adds all columns from another set
func (cs *ColumnSet) AddAll(other *ColumnSet) {
	if other.hasStar {
		cs.hasStar = true
	}
	for col := range other.columns {
		cs.columns[col] = true
	}
}

// Contains checks if a column is in the set
func (cs *ColumnSet) Contains(col ColumnRef) bool {
	return cs.columns[col]
}

// IsEmpty returns true if the set has no columns
func (cs *ColumnSet) IsEmpty() bool {
	return len(cs.columns) == 0 && !cs.hasStar
}

// Size returns the number of columns in the set
func (cs *ColumnSet) Size() int {
	return len(cs.columns)
}

// HasStar returns true if SELECT * is used
func (cs *ColumnSet) HasStar() bool {
	return cs.hasStar
}

// SetStar marks that all columns are needed
func (cs *ColumnSet) SetStar() {
	cs.hasStar = true
}

// ToSlice returns columns as a sorted slice
func (cs *ColumnSet) ToSlice() []ColumnRef {
	result := make([]ColumnRef, 0, len(cs.columns))
	for col := range cs.columns {
		result = append(result, col)
	}
	// Sort for deterministic output
	sort.Slice(result, func(i, j int) bool {
		if result[i].TableAlias != result[j].TableAlias {
			return result[i].TableAlias < result[j].TableAlias
		}
		return result[i].ColumnName < result[j].ColumnName
	})
	return result
}

// Clone creates a copy of the column set
func (cs *ColumnSet) Clone() *ColumnSet {
	clone := NewColumnSet()
	clone.hasStar = cs.hasStar
	for col := range cs.columns {
		clone.columns[col] = true
	}
	return clone
}

// String returns a string representation of the column set
func (cs *ColumnSet) String() string {
	if cs.hasStar {
		return "[*]"
	}
	if cs.IsEmpty() {
		return "[]"
	}
	cols := cs.ToSlice()
	strs := make([]string, len(cols))
	for i, col := range cols {
		strs[i] = col.String()
	}
	return "[" + strings.Join(strs, ", ") + "]"
}

// extractColumns finds all column references in an expression
func extractColumns(expr Expression, cols *ColumnSet) {
	switch e := expr.(type) {
	case *ColumnRef:
		cols.Add(ColumnRef{
			TableAlias: e.TableAlias,
			ColumnName: e.ColumnName,
		})
	case *BinaryOp:
		extractColumns(e.Left, cols)
		extractColumns(e.Right, cols)
	case *UnaryOp:
		extractColumns(e.Expr, cols)
	case *FunctionCall:
		for _, arg := range e.Args {
			extractColumns(arg, cols)
		}
	case *Star:
		// Star requires all columns
		cols.SetStar()
	case *Literal, *ParameterRef:
		// These don't reference columns
	default:
		// For unknown expression types, be conservative
		// and assume we need all columns
		cols.SetStar()
	}
}

// columnExistsInSchema checks if a column name exists in a schema
func columnExistsInSchema(columnName string, schema *Schema) bool {
	if schema == nil {
		return false
	}
	for _, col := range schema.Columns {
		if col.Name == columnName {
			return true
		}
	}
	return false
}
