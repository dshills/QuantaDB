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

// LogicalUpdate represents an UPDATE operation
type LogicalUpdate struct {
	basePlan
	TableName   string
	SchemaName  string
	Assignments []parser.Assignment   // Column assignments
	Where       parser.Expression     // WHERE clause (optional)
	TableRef    *catalog.Table        // Reference to table metadata
}

// NewLogicalUpdate creates a new UPDATE plan node
func NewLogicalUpdate(schemaName, tableName string, assignments []parser.Assignment, where parser.Expression, tableRef *catalog.Table) *LogicalUpdate {
	return &LogicalUpdate{
		basePlan:    basePlan{},
		SchemaName:  schemaName,
		TableName:   tableName,
		Assignments: assignments,
		Where:       where,
		TableRef:    tableRef,
	}
}

// Type returns the plan type
func (p *LogicalUpdate) Type() string {
	return "Update"
}

// Schema returns the output schema (rows affected)
func (p *LogicalUpdate) Schema() *Schema {
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
func (p *LogicalUpdate) String() string {
	whereStr := ""
	if p.Where != nil {
		whereStr = " WHERE ..."
	}
	return fmt.Sprintf("Update(%s.%s, %d assignments%s)", p.SchemaName, p.TableName, len(p.Assignments), whereStr)
}

// Children returns child plans (none for UPDATE)
func (p *LogicalUpdate) Children() []Plan {
	return nil
}

// logicalNode marks this as a logical plan node
func (p *LogicalUpdate) logicalNode() {}

// LogicalDelete represents a DELETE operation
type LogicalDelete struct {
	basePlan
	TableName  string
	SchemaName string
	Where      parser.Expression // WHERE clause (optional)
	TableRef   *catalog.Table    // Reference to table metadata
}

// NewLogicalDelete creates a new DELETE plan node
func NewLogicalDelete(schemaName, tableName string, where parser.Expression, tableRef *catalog.Table) *LogicalDelete {
	return &LogicalDelete{
		basePlan:   basePlan{},
		SchemaName: schemaName,
		TableName:  tableName,
		Where:      where,
		TableRef:   tableRef,
	}
}

// Type returns the plan type
func (p *LogicalDelete) Type() string {
	return "Delete"
}

// Schema returns the output schema (rows affected)
func (p *LogicalDelete) Schema() *Schema {
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
func (p *LogicalDelete) String() string {
	whereStr := ""
	if p.Where != nil {
		whereStr = " WHERE ..."
	}
	return fmt.Sprintf("Delete(%s.%s%s)", p.SchemaName, p.TableName, whereStr)
}

// Children returns child plans (none for DELETE)
func (p *LogicalDelete) Children() []Plan {
	return nil
}

// logicalNode marks this as a logical plan node
func (p *LogicalDelete) logicalNode() {}