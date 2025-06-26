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
	IndexName      string
	TableName      string
	SchemaName     string
	Columns        []string
	IncludeColumns []string
	Unique         bool
	IndexType      string
}

// NewLogicalCreateIndex creates a new CREATE INDEX plan node
func NewLogicalCreateIndex(schemaName, tableName, indexName string, columns []string, includeColumns []string, unique bool, indexType string) *LogicalCreateIndex {
	return &LogicalCreateIndex{
		basePlan:       basePlan{},
		SchemaName:     schemaName,
		TableName:      tableName,
		IndexName:      indexName,
		Columns:        columns,
		IncludeColumns: includeColumns,
		Unique:         unique,
		IndexType:      indexType,
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

// LogicalAlterTableAddColumn represents an ALTER TABLE ADD COLUMN operation
type LogicalAlterTableAddColumn struct {
	basePlan
	TableName  string
	SchemaName string
	Column     *parser.ColumnDef
}

// NewLogicalAlterTableAddColumn creates a new ALTER TABLE ADD COLUMN plan node
func NewLogicalAlterTableAddColumn(schemaName, tableName string, column *parser.ColumnDef) *LogicalAlterTableAddColumn {
	return &LogicalAlterTableAddColumn{
		basePlan:   basePlan{},
		SchemaName: schemaName,
		TableName:  tableName,
		Column:     column,
	}
}

// Type returns the plan type
func (p *LogicalAlterTableAddColumn) Type() string {
	return "AlterTableAddColumn"
}

// Schema returns the output schema (result message)
func (p *LogicalAlterTableAddColumn) Schema() *Schema {
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
func (p *LogicalAlterTableAddColumn) String() string {
	return fmt.Sprintf("AlterTableAddColumn(%s.%s, %s)", p.SchemaName, p.TableName, p.Column.String())
}

// Children returns child plans (none for ALTER TABLE ADD COLUMN)
func (p *LogicalAlterTableAddColumn) Children() []Plan {
	return nil
}

// logicalNode marks this as a logical plan node
func (p *LogicalAlterTableAddColumn) logicalNode() {}

// LogicalAlterTableDropColumn represents an ALTER TABLE DROP COLUMN operation
type LogicalAlterTableDropColumn struct {
	basePlan
	TableName  string
	SchemaName string
	ColumnName string
}

// NewLogicalAlterTableDropColumn creates a new ALTER TABLE DROP COLUMN plan node
func NewLogicalAlterTableDropColumn(schemaName, tableName, columnName string) *LogicalAlterTableDropColumn {
	return &LogicalAlterTableDropColumn{
		basePlan:   basePlan{},
		SchemaName: schemaName,
		TableName:  tableName,
		ColumnName: columnName,
	}
}

// Type returns the plan type
func (p *LogicalAlterTableDropColumn) Type() string {
	return "AlterTableDropColumn"
}

// Schema returns the output schema (result message)
func (p *LogicalAlterTableDropColumn) Schema() *Schema {
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
func (p *LogicalAlterTableDropColumn) String() string {
	return fmt.Sprintf("AlterTableDropColumn(%s.%s, %s)", p.SchemaName, p.TableName, p.ColumnName)
}

// Children returns child plans (none for ALTER TABLE DROP COLUMN)
func (p *LogicalAlterTableDropColumn) Children() []Plan {
	return nil
}

// logicalNode marks this as a logical plan node
func (p *LogicalAlterTableDropColumn) logicalNode() {}

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

// LogicalVacuum represents a VACUUM operation
type LogicalVacuum struct {
	basePlan
	TableName  string // Empty means vacuum all tables
	SchemaName string
	Analyze    bool // True for VACUUM ANALYZE
}

// NewLogicalVacuum creates a new VACUUM plan node
func NewLogicalVacuum(schemaName, tableName string, analyze bool) *LogicalVacuum {
	return &LogicalVacuum{
		basePlan:   basePlan{},
		SchemaName: schemaName,
		TableName:  tableName,
		Analyze:    analyze,
	}
}

// Type returns the plan type
func (p *LogicalVacuum) Type() string {
	return "Vacuum"
}

// Schema returns the output schema (vacuum statistics)
func (p *LogicalVacuum) Schema() *Schema {
	return &Schema{
		Columns: []Column{
			{
				Name:     "operation",
				DataType: types.Text,
				Nullable: false,
			},
			{
				Name:     "tables_processed",
				DataType: types.Integer,
				Nullable: false,
			},
			{
				Name:     "versions_scanned",
				DataType: types.Integer,
				Nullable: false,
			},
			{
				Name:     "versions_removed",
				DataType: types.Integer,
				Nullable: false,
			},
			{
				Name:     "space_reclaimed",
				DataType: types.Integer,
				Nullable: false,
			},
			{
				Name:     "duration_ms",
				DataType: types.Integer,
				Nullable: false,
			},
		},
	}
}

// String returns a string representation
func (p *LogicalVacuum) String() string {
	cmd := "Vacuum"
	if p.Analyze {
		cmd = "VacuumAnalyze"
	}
	if p.TableName != "" {
		return fmt.Sprintf("%s(%s.%s)", cmd, p.SchemaName, p.TableName)
	}
	return fmt.Sprintf("%s(all tables)", cmd)
}

// Children returns child plans (none for VACUUM)
func (p *LogicalVacuum) Children() []Plan {
	return nil
}

// logicalNode marks this as a logical plan node
func (p *LogicalVacuum) logicalNode() {}

// LogicalCopy represents a COPY operation for bulk data import/export
type LogicalCopy struct {
	basePlan
	TableName  string
	SchemaName string
	Columns    []string             // Column names (if specified)
	Direction  parser.CopyDirection // FROM or TO
	Source     string               // STDIN, STDOUT, or filename
	Options    map[string]string    // WITH options like FORMAT, DELIMITER, etc.
	TableRef   *catalog.Table       // Reference to table metadata
}

// NewLogicalCopy creates a new COPY plan node
func NewLogicalCopy(schemaName, tableName string, columns []string, direction parser.CopyDirection, source string, options map[string]string, tableRef *catalog.Table) *LogicalCopy {
	return &LogicalCopy{
		basePlan:   basePlan{},
		SchemaName: schemaName,
		TableName:  tableName,
		Columns:    columns,
		Direction:  direction,
		Source:     source,
		Options:    options,
		TableRef:   tableRef,
	}
}

// Type returns the plan type
func (p *LogicalCopy) Type() string {
	return "Copy"
}

// Schema returns the output schema (rows copied)
func (p *LogicalCopy) Schema() *Schema {
	return &Schema{
		Columns: []Column{
			{
				Name:     "rows_copied",
				DataType: types.Integer,
				Nullable: false,
			},
		},
	}
}

// String returns a string representation
func (p *LogicalCopy) String() string {
	dir := "FROM"
	if p.Direction == parser.CopyTo {
		dir = "TO"
	}
	return fmt.Sprintf("Copy(%s.%s %s %s)", p.SchemaName, p.TableName, dir, p.Source)
}

// Children returns child plans (none for COPY)
func (p *LogicalCopy) Children() []Plan {
	return nil
}

// logicalNode marks this as a logical plan node
func (p *LogicalCopy) logicalNode() {}

// LogicalPrepare represents a PREPARE statement execution
type LogicalPrepare struct {
	basePlan
	StatementName string
	ParamTypes    []types.DataType
	Query         parser.Statement
}

// NewLogicalPrepare creates a new PREPARE plan node
func NewLogicalPrepare(name string, paramTypes []types.DataType, query parser.Statement) *LogicalPrepare {
	return &LogicalPrepare{
		basePlan:      basePlan{},
		StatementName: name,
		ParamTypes:    paramTypes,
		Query:         query,
	}
}

// Type returns the plan type
func (p *LogicalPrepare) Type() string {
	return "Prepare"
}

// Schema returns the output schema (result message)
func (p *LogicalPrepare) Schema() *Schema {
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
func (p *LogicalPrepare) String() string {
	return fmt.Sprintf("Prepare(%s)", p.StatementName)
}

// Children returns child plans (none for PREPARE)
func (p *LogicalPrepare) Children() []Plan {
	return nil
}

// logicalNode marks this as a logical plan node
func (p *LogicalPrepare) logicalNode() {}

// LogicalExecute represents an EXECUTE statement execution
type LogicalExecute struct {
	basePlan
	StatementName string
	Params        []parser.Expression
}

// NewLogicalExecute creates a new EXECUTE plan node
func NewLogicalExecute(name string, params []parser.Expression) *LogicalExecute {
	return &LogicalExecute{
		basePlan:      basePlan{},
		StatementName: name,
		Params:        params,
	}
}

// Type returns the plan type
func (p *LogicalExecute) Type() string {
	return "Execute"
}

// Schema returns the output schema (depends on prepared statement)
func (p *LogicalExecute) Schema() *Schema {
	// The actual schema will be determined at execution time
	// based on the prepared statement being executed
	return &Schema{
		Columns: []Column{},
	}
}

// String returns a string representation
func (p *LogicalExecute) String() string {
	return fmt.Sprintf("Execute(%s)", p.StatementName)
}

// Children returns child plans (none for EXECUTE)
func (p *LogicalExecute) Children() []Plan {
	return nil
}

// logicalNode marks this as a logical plan node
func (p *LogicalExecute) logicalNode() {}

// LogicalDeallocate represents a DEALLOCATE statement execution
type LogicalDeallocate struct {
	basePlan
	StatementName string
}

// NewLogicalDeallocate creates a new DEALLOCATE plan node
func NewLogicalDeallocate(name string) *LogicalDeallocate {
	return &LogicalDeallocate{
		basePlan:      basePlan{},
		StatementName: name,
	}
}

// Type returns the plan type
func (p *LogicalDeallocate) Type() string {
	return "Deallocate"
}

// Schema returns the output schema (result message)
func (p *LogicalDeallocate) Schema() *Schema {
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
func (p *LogicalDeallocate) String() string {
	return fmt.Sprintf("Deallocate(%s)", p.StatementName)
}

// Children returns child plans (none for DEALLOCATE)
func (p *LogicalDeallocate) Children() []Plan {
	return nil
}

// logicalNode marks this as a logical plan node
func (p *LogicalDeallocate) logicalNode() {}
