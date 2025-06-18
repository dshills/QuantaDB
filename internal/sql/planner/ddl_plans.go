package planner

import (
	"fmt"
	
	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

// LogicalCreateTable represents a CREATE TABLE operation
type LogicalCreateTable struct {
	basePlan
	TableName   string
	SchemaName  string
	Columns     []catalog.Column
	Constraints []catalog.Constraint
}

// NewLogicalCreateTable creates a new CREATE TABLE plan node
func NewLogicalCreateTable(schemaName, tableName string, columns []catalog.Column, constraints []catalog.Constraint) *LogicalCreateTable {
	return &LogicalCreateTable{
		basePlan:    basePlan{},
		SchemaName:  schemaName,
		TableName:   tableName,
		Columns:     columns,
		Constraints: constraints,
	}
}

// Type returns the plan type
func (p *LogicalCreateTable) Type() string {
	return "CreateTable"
}

// Schema returns the output schema (empty for DDL)
func (p *LogicalCreateTable) Schema() *Schema {
	return &Schema{
		Columns: []Column{
			{
				Name:     "result",
				DataType: types.Text,
				Nullable: false,
			},
		},
	}
}

// String returns a string representation
func (p *LogicalCreateTable) String() string {
	return fmt.Sprintf("CreateTable(%s.%s)", p.SchemaName, p.TableName)
}

// Children returns child plans (none for CREATE TABLE)
func (p *LogicalCreateTable) Children() []Plan {
	return nil
}

// logicalNode marks this as a logical plan node
func (p *LogicalCreateTable) logicalNode() {}

// LogicalInsert represents an INSERT operation
type LogicalInsert struct {
	basePlan
	TableName  string
	SchemaName string
	Columns    []string // Column names (if specified)
	Values     [][]parser.Expression // List of value tuples
	TableRef   *catalog.Table // Reference to table metadata
}

// NewLogicalInsert creates a new INSERT plan node
func NewLogicalInsert(schemaName, tableName string, columns []string, values [][]parser.Expression, tableRef *catalog.Table) *LogicalInsert {
	return &LogicalInsert{
		basePlan:   basePlan{},
		SchemaName: schemaName,
		TableName:  tableName,
		Columns:    columns,
		Values:     values,
		TableRef:   tableRef,
	}
}

// Type returns the plan type
func (p *LogicalInsert) Type() string {
	return "Insert"
}

// Schema returns the output schema (rows affected)
func (p *LogicalInsert) Schema() *Schema {
	return &Schema{
		Columns: []Column{
			{
				Name:     "rows_affected",
				DataType: types.Integer,
				Nullable: false,
			},
		},
	}
}

// String returns a string representation
func (p *LogicalInsert) String() string {
	return fmt.Sprintf("Insert(%s.%s, %d rows)", p.SchemaName, p.TableName, len(p.Values))
}

// Children returns child plans (none for INSERT)
func (p *LogicalInsert) Children() []Plan {
	return nil
}

// logicalNode marks this as a logical plan node
func (p *LogicalInsert) logicalNode() {}