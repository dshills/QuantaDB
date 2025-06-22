package catalog

import (
	"fmt"
	"strings"
)

// ForeignKeyConstraint represents a foreign key constraint.
type ForeignKeyConstraint struct {
	Name           string
	Columns        []string           // Columns in this table
	RefTableSchema string             // Referenced table schema
	RefTableName   string             // Referenced table name
	RefColumns     []string           // Referenced columns
	OnDelete       ReferentialAction  // Action on delete
	OnUpdate       ReferentialAction  // Action on update
}

// ReferentialAction represents actions for ON DELETE/UPDATE clauses.
type ReferentialAction string

const (
	// NoAction - Produce an error indicating that the deletion or update would create a foreign key constraint violation
	NoAction ReferentialAction = "NO ACTION"
	// Restrict - Same as NO ACTION
	Restrict ReferentialAction = "RESTRICT"
	// Cascade - Delete any rows referencing the deleted row, or update the value of the referencing column
	Cascade ReferentialAction = "CASCADE"
	// SetNull - Set the referencing column(s) to null
	SetNull ReferentialAction = "SET NULL"
	// SetDefault - Set the referencing column(s) to their default values
	SetDefault ReferentialAction = "SET DEFAULT"
)

func (c ForeignKeyConstraint) constraintType() string { return "FOREIGN KEY" }

func (c ForeignKeyConstraint) String() string {
	var parts []string
	
	// Name (if provided)
	if c.Name != "" {
		parts = append(parts, fmt.Sprintf("CONSTRAINT %s", c.Name))
	}
	
	// Foreign key columns
	parts = append(parts, fmt.Sprintf("FOREIGN KEY (%s)", strings.Join(c.Columns, ", ")))
	
	// References clause
	refPart := fmt.Sprintf("REFERENCES %s", c.RefTableName)
	if c.RefTableSchema != "" && c.RefTableSchema != "public" {
		refPart = fmt.Sprintf("REFERENCES %s.%s", c.RefTableSchema, c.RefTableName)
	}
	if len(c.RefColumns) > 0 {
		refPart += fmt.Sprintf(" (%s)", strings.Join(c.RefColumns, ", "))
	}
	parts = append(parts, refPart)
	
	// ON DELETE clause
	if c.OnDelete != "" && c.OnDelete != NoAction {
		parts = append(parts, fmt.Sprintf("ON DELETE %s", c.OnDelete))
	}
	
	// ON UPDATE clause
	if c.OnUpdate != "" && c.OnUpdate != NoAction {
		parts = append(parts, fmt.Sprintf("ON UPDATE %s", c.OnUpdate))
	}
	
	return strings.Join(parts, " ")
}

// CheckConstraint represents a CHECK constraint.
type CheckConstraint struct {
	Name       string
	Expression string // The CHECK expression as a string
}

func (c CheckConstraint) constraintType() string { return "CHECK" }

func (c CheckConstraint) String() string {
	if c.Name != "" {
		return fmt.Sprintf("CONSTRAINT %s CHECK (%s)", c.Name, c.Expression)
	}
	return fmt.Sprintf("CHECK (%s)", c.Expression)
}