package catalog

import (
	"fmt"
	"time"

	"github.com/dshills/QuantaDB/internal/sql/types"
)

// Catalog manages database metadata including tables, columns, indexes, and statistics.
type Catalog interface {
	// Table operations
	CreateTable(schema *TableSchema) (*Table, error)
	GetTable(schemaName, tableName string) (*Table, error)
	DropTable(schemaName, tableName string) error
	ListTables(schemaName string) ([]*Table, error)

	// Index operations
	CreateIndex(index *IndexSchema) (*Index, error)
	GetIndex(schemaName, tableName, indexName string) (*Index, error)
	DropIndex(schemaName, tableName, indexName string) error

	// Statistics operations
	UpdateTableStats(schemaName, tableName string) error
	GetTableStats(schemaName, tableName string) (*TableStats, error)

	// Schema operations
	CreateSchema(name string) error
	DropSchema(name string) error
	ListSchemas() ([]string, error)
}

// StatsWriter interface for updating statistics in the catalog.
type StatsWriter interface {
	UpdateTableStatistics(tableID int64, stats *TableStats) error
	UpdateColumnStatistics(tableID int64, columnID int64, stats *ColumnStats) error
}

// TableSchema defines the structure for creating a new table.
type TableSchema struct {
	SchemaName  string
	TableName   string
	Columns     []ColumnDef
	Constraints []Constraint
}

// ColumnDef defines a column in a table.
type ColumnDef struct {
	Name         string
	DataType     types.DataType
	IsNullable   bool
	DefaultValue types.Value
	Constraints  []ColumnConstraint
}

// Table represents a table with its metadata.
type Table struct {
	ID          int64
	SchemaName  string
	TableName   string
	Columns     []*Column
	Indexes     []*Index
	Constraints []Constraint
	Stats       *TableStats
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// Column represents a column with its metadata.
type Column struct {
	ID              int64
	Name            string
	DataType        types.DataType
	OrdinalPosition int
	IsNullable      bool
	DefaultValue    types.Value
	Stats           *ColumnStats
}

// Index represents an index on a table.
type Index struct {
	ID        int64
	Name      string
	TableID   int64
	Type      IndexType
	IsUnique  bool
	IsPrimary bool
	Columns   []IndexColumn
	CreatedAt time.Time
}

// IndexColumn represents a column in an index.
type IndexColumn struct {
	Column    *Column
	SortOrder SortOrder
	Position  int
}

// IndexType represents the type of index.
type IndexType int

const (
	// BTreeIndex is a B-tree index.
	BTreeIndex IndexType = iota
	// HashIndex is a hash index.
	HashIndex
)

func (t IndexType) String() string {
	switch t {
	case BTreeIndex:
		return "BTREE"
	case HashIndex:
		return "HASH"
	default:
		return fmt.Sprintf("Unknown(%d)", t)
	}
}

// SortOrder represents the sort order in an index.
type SortOrder int

const (
	// Ascending sort order.
	Ascending SortOrder = iota
	// Descending sort order.
	Descending
)

func (s SortOrder) String() string {
	if s == Descending {
		return "DESC"
	}
	return "ASC"
}

// Constraint represents a table constraint.
type Constraint interface {
	constraintType() string
	String() string
}

// ColumnConstraint represents a column-level constraint.
type ColumnConstraint interface {
	columnConstraintType() string
	String() string
}

// PrimaryKeyConstraint represents a primary key constraint.
type PrimaryKeyConstraint struct {
	Columns []string
}

func (c PrimaryKeyConstraint) constraintType() string { return "PRIMARY KEY" }
func (c PrimaryKeyConstraint) String() string {
	if len(c.Columns) == 1 {
		return "PRIMARY KEY"
	}
	return fmt.Sprintf("PRIMARY KEY (%s)", joinStrings(c.Columns, ", "))
}

// UniqueConstraint represents a unique constraint.
type UniqueConstraint struct {
	Columns []string
}

func (c UniqueConstraint) constraintType() string { return "UNIQUE" }
func (c UniqueConstraint) String() string {
	if len(c.Columns) == 1 {
		return "UNIQUE"
	}
	return fmt.Sprintf("UNIQUE (%s)", joinStrings(c.Columns, ", "))
}

// NotNullConstraint represents a NOT NULL constraint.
type NotNullConstraint struct{}

func (c NotNullConstraint) columnConstraintType() string { return "NOT NULL" }
func (c NotNullConstraint) String() string               { return "NOT NULL" }

// DefaultConstraint represents a DEFAULT constraint.
type DefaultConstraint struct {
	Value types.Value
}

func (c DefaultConstraint) columnConstraintType() string { return "DEFAULT" }
func (c DefaultConstraint) String() string {
	return fmt.Sprintf("DEFAULT %s", c.Value.String())
}

// Helper function to join strings.
func joinStrings(strs []string, sep string) string {
	result := ""
	for i, s := range strs {
		if i > 0 {
			result += sep
		}
		result += s
	}
	return result
}
