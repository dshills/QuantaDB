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
	Columns    []string              // Column names (if specified)
	Values     [][]parser.Expression // List of value tuples
	TableRef   *catalog.Table        // Reference to table metadata
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
	Assignments []parser.Assignment // Column assignments
	Where       parser.Expression   // WHERE clause (optional)
	TableRef    *catalog.Table      // Reference to table metadata
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

// LogicalCreateIndex represents a CREATE INDEX operation
type LogicalCreateIndex struct {
	basePlan
	IndexName  string
	TableName  string
	SchemaName string
	Columns    []string
	Unique     bool
	IndexType  string
}

// NewLogicalCreateIndex creates a new CREATE INDEX plan node
func NewLogicalCreateIndex(schemaName, tableName, indexName string, columns []string, unique bool, indexType string) *LogicalCreateIndex {
	return &LogicalCreateIndex{
		basePlan:   basePlan{},
		SchemaName: schemaName,
		TableName:  tableName,
		IndexName:  indexName,
		Columns:    columns,
		Unique:     unique,
		IndexType:  indexType,
	}
}

// Type returns the plan type
func (p *LogicalCreateIndex) Type() string {
	return "CreateIndex"
}

// Schema returns the output schema (result message)
func (p *LogicalCreateIndex) Schema() *Schema {
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
func (p *LogicalCreateIndex) String() string {
	unique := ""
	if p.Unique {
		unique = "UNIQUE "
	}
	return fmt.Sprintf("CreateIndex(%s%s ON %s.%s)", unique, p.IndexName, p.SchemaName, p.TableName)
}

// Children returns child plans (none for CREATE INDEX)
func (p *LogicalCreateIndex) Children() []Plan {
	return nil
}

// logicalNode marks this as a logical plan node
func (p *LogicalCreateIndex) logicalNode() {}

// LogicalDropTable represents a DROP TABLE operation
type LogicalDropTable struct {
	basePlan
	TableName  string
	SchemaName string
}

// NewLogicalDropTable creates a new DROP TABLE plan node
func NewLogicalDropTable(schemaName, tableName string) *LogicalDropTable {
	return &LogicalDropTable{
		basePlan:   basePlan{},
		SchemaName: schemaName,
		TableName:  tableName,
	}
}

// Type returns the plan type
func (p *LogicalDropTable) Type() string {
	return "DropTable"
}

// Schema returns the output schema (result message)
func (p *LogicalDropTable) Schema() *Schema {
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
func (p *LogicalDropTable) String() string {
	return fmt.Sprintf("DropTable(%s.%s)", p.SchemaName, p.TableName)
}

// Children returns child plans (none for DROP TABLE)
func (p *LogicalDropTable) Children() []Plan {
	return nil
}

// logicalNode marks this as a logical plan node
func (p *LogicalDropTable) logicalNode() {}

// LogicalDropIndex represents a DROP INDEX operation
type LogicalDropIndex struct {
	basePlan
	IndexName  string
	TableName  string
	SchemaName string
}

// NewLogicalDropIndex creates a new DROP INDEX plan node
func NewLogicalDropIndex(schemaName, tableName, indexName string) *LogicalDropIndex {
	return &LogicalDropIndex{
		basePlan:   basePlan{},
		SchemaName: schemaName,
		TableName:  tableName,
		IndexName:  indexName,
	}
}

// Type returns the plan type
func (p *LogicalDropIndex) Type() string {
	return "DropIndex"
}

// Schema returns the output schema (result message)
func (p *LogicalDropIndex) Schema() *Schema {
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
func (p *LogicalDropIndex) String() string {
	if p.TableName != "" {
		return fmt.Sprintf("DropIndex(%s ON %s.%s)", p.IndexName, p.SchemaName, p.TableName)
	}
	return fmt.Sprintf("DropIndex(%s)", p.IndexName)
}

// Children returns child plans (none for DROP INDEX)
func (p *LogicalDropIndex) Children() []Plan {
	return nil
}

// logicalNode marks this as a logical plan node
func (p *LogicalDropIndex) logicalNode() {}

// LogicalAnalyze represents an ANALYZE operation
type LogicalAnalyze struct {
	basePlan
	TableName  string
	SchemaName string
	Columns    []string // Empty means all columns
}

// NewLogicalAnalyze creates a new ANALYZE plan node
func NewLogicalAnalyze(schemaName, tableName string, columns []string) *LogicalAnalyze {
	return &LogicalAnalyze{
		basePlan:   basePlan{},
		SchemaName: schemaName,
		TableName:  tableName,
		Columns:    columns,
	}
}

// Type returns the plan type
func (p *LogicalAnalyze) Type() string {
	return "Analyze"
}

// Schema returns the output schema (result message)
func (p *LogicalAnalyze) Schema() *Schema {
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
func (p *LogicalAnalyze) String() string {
	if len(p.Columns) > 0 {
		return fmt.Sprintf("Analyze(%s.%s columns=%v)", p.SchemaName, p.TableName, p.Columns)
	}
	return fmt.Sprintf("Analyze(%s.%s)", p.SchemaName, p.TableName)
}

// Children returns child plans (none for ANALYZE)
func (p *LogicalAnalyze) Children() []Plan {
	return nil
}

// logicalNode marks this as a logical plan node
func (p *LogicalAnalyze) logicalNode() {}
