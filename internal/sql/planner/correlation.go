package planner

import (
	"fmt"

	"github.com/dshills/QuantaDB/internal/sql/parser"
)

// CorrelationFinder finds external column references in expressions.
type CorrelationFinder struct {
	availableColumns map[string]bool   // Columns available in current scope
	externalRefs     map[string]bool   // External column references found
	tableAliases     map[string]string // Map of alias to table name
}

// NewCorrelationFinder creates a new correlation finder.
func NewCorrelationFinder() *CorrelationFinder {
	return &CorrelationFinder{
		availableColumns: make(map[string]bool),
		externalRefs:     make(map[string]bool),
		tableAliases:     make(map[string]string),
	}
}

// AddAvailableColumn adds a column to the available columns set.
func (cf *CorrelationFinder) AddAvailableColumn(tableName, columnName string) {
	cf.availableColumns[columnName] = true
	if tableName != "" {
		cf.availableColumns[fmt.Sprintf("%s.%s", tableName, columnName)] = true
	}
}

// AddTableAlias adds a table alias mapping.
func (cf *CorrelationFinder) AddTableAlias(alias, tableName string) {
	cf.tableAliases[alias] = tableName
}

// FindCorrelations finds external column references in an expression.
func (cf *CorrelationFinder) FindCorrelations(expr parser.Expression) error {
	switch e := expr.(type) {
	case *parser.Identifier:
		// Check if this column is available in current scope
		colName := e.Name

		// Handle qualified identifiers
		if e.Table != "" {
			colName = fmt.Sprintf("%s.%s", e.Table, e.Name)
		}

		// Check if column is available
		if !cf.availableColumns[colName] && !cf.availableColumns[e.Name] {
			// This is an external reference
			cf.externalRefs[colName] = true
		}

	case *parser.BinaryExpr:
		if err := cf.FindCorrelations(e.Left); err != nil {
			return err
		}
		if err := cf.FindCorrelations(e.Right); err != nil {
			return err
		}

	case *parser.UnaryExpr:
		if err := cf.FindCorrelations(e.Expr); err != nil {
			return err
		}

	case *parser.FunctionCall:
		for _, arg := range e.Args {
			if err := cf.FindCorrelations(arg); err != nil {
				return err
			}
		}

	case *parser.CaseExpr:
		if e.Expr != nil {
			if err := cf.FindCorrelations(e.Expr); err != nil {
				return err
			}
		}
		for _, when := range e.WhenList {
			if err := cf.FindCorrelations(when.Condition); err != nil {
				return err
			}
			if err := cf.FindCorrelations(when.Result); err != nil {
				return err
			}
		}
		if e.Else != nil {
			if err := cf.FindCorrelations(e.Else); err != nil {
				return err
			}
		}

	case *parser.SubqueryExpr:
		// Recursively analyze subquery for nested correlations
		if err := cf.analyzeSubquery(e.Query); err != nil {
			return err
		}

	case *parser.ExistsExpr:
		// Analyze EXISTS subquery
		if e.Subquery != nil {
			if err := cf.analyzeSubquery(e.Subquery.Query); err != nil {
				return err
			}
		}

	case *parser.InExpr:
		// Check the expression being tested
		if err := cf.FindCorrelations(e.Expr); err != nil {
			return err
		}
		// If it's a subquery, analyze it
		if e.Subquery != nil {
			if err := cf.analyzeSubquery(e.Subquery.Query); err != nil {
				return err
			}
		}

	case *parser.Literal, *parser.Star:
		// No external references possible

	default:
		// Unknown expression type, conservatively assume no correlations
	}

	return nil
}

// analyzeSubquery analyzes a subquery for correlations.
func (cf *CorrelationFinder) analyzeSubquery(query *parser.SelectStmt) error {
	// Save current available columns
	savedColumns := cf.availableColumns
	cf.availableColumns = make(map[string]bool)

	// Add columns from subquery's FROM clause
	if err := cf.addColumnsFromTable(query.From); err != nil {
		return err
	}

	// Analyze WHERE clause for external references
	if query.Where != nil {
		if err := cf.FindCorrelations(query.Where); err != nil {
			return err
		}
	}

	// Analyze SELECT columns
	for _, col := range query.Columns {
		if err := cf.FindCorrelations(col.Expr); err != nil {
			return err
		}
	}

	// Analyze GROUP BY
	for _, expr := range query.GroupBy {
		if err := cf.FindCorrelations(expr); err != nil {
			return err
		}
	}

	// Analyze HAVING
	if query.Having != nil {
		if err := cf.FindCorrelations(query.Having); err != nil {
			return err
		}
	}

	// Restore available columns
	cf.availableColumns = savedColumns

	return nil
}

// addColumnsFromTable adds columns from a table expression to available columns.
func (cf *CorrelationFinder) addColumnsFromTable(tableExpr parser.TableExpression) error {
	// This is simplified - in reality would need catalog lookup
	switch t := tableExpr.(type) {
	case *parser.TableRef:
		// Add table alias
		if t.Alias != "" {
			cf.AddTableAlias(t.Alias, t.TableName)
		}
		// Would normally look up columns from catalog here
		// For now, just mark that we have this table
		cf.availableColumns[t.TableName] = true
		if t.Alias != "" {
			cf.availableColumns[t.Alias] = true
		}

	case *parser.JoinExpr:
		if err := cf.addColumnsFromTable(t.Left); err != nil {
			return err
		}
		if err := cf.addColumnsFromTable(t.Right); err != nil {
			return err
		}

	case *parser.SubqueryRef:
		// Subquery in FROM - analyze it separately
		if err := cf.analyzeSubquery(t.Query); err != nil {
			return err
		}
	}

	return nil
}

// HasCorrelations returns true if external references were found.
func (cf *CorrelationFinder) HasCorrelations() bool {
	return len(cf.externalRefs) > 0
}

// GetExternalRefs returns the external column references found.
func (cf *CorrelationFinder) GetExternalRefs() []string {
	refs := make([]string, 0, len(cf.externalRefs))
	for ref := range cf.externalRefs {
		refs = append(refs, ref)
	}
	return refs
}
