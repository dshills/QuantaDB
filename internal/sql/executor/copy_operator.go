package executor

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/sql/types"
	"github.com/dshills/QuantaDB/internal/txn"
)

// CopyOperator implements the COPY operation for bulk data import/export.
type CopyOperator struct {
	plan       *planner.LogicalCopy
	catalog    catalog.Catalog
	storage    StorageBackend
	txnManager *txn.Manager
	schema     *Schema
	reader     io.Reader
	writer     io.Writer
	rowCount   int64
}

// NewCopyOperator creates a new COPY operator.
func NewCopyOperator(plan *planner.LogicalCopy, cat catalog.Catalog, storage StorageBackend, txnManager *txn.Manager) *CopyOperator {
	return &CopyOperator{
		plan:       plan,
		catalog:    cat,
		storage:    storage,
		txnManager: txnManager,
		schema: &Schema{
			Columns: []Column{
				{
					Name:     "rows_copied",
					Type:     types.Integer,
					Nullable: false,
				},
			},
		},
	}
}

// Open prepares the operator for execution.
func (op *CopyOperator) Open(ctx *ExecContext) error {
	// For STDIN/STDOUT, the actual I/O streams will be set by the network layer
	// For file operations, open the file here
	if op.plan.Source != "STDIN" && op.plan.Source != "STDOUT" {
		if op.plan.Direction == parser.CopyFrom {
			file, err := os.Open(op.plan.Source)
			if err != nil {
				return fmt.Errorf("cannot open file %s: %w", op.plan.Source, err)
			}
			op.reader = file
		} else {
			file, err := os.Create(op.plan.Source)
			if err != nil {
				return fmt.Errorf("cannot create file %s: %w", op.plan.Source, err)
			}
			op.writer = file
		}
	}
	
	return nil
}

// Next returns the next row (for COPY operations, this returns the final result).
func (op *CopyOperator) Next() (*Row, error) {
	// Execute the COPY operation
	if op.plan.Direction == parser.CopyFrom {
		if err := op.executeCopyFrom(); err != nil {
			return nil, err
		}
	} else {
		if err := op.executeCopyTo(); err != nil {
			return nil, err
		}
	}
	
	// Return the result row with row count
	row := &Row{
		Values: []types.Value{
			types.NewValue(op.rowCount),
		},
	}
	
	// Mark as done for subsequent calls
	op.rowCount = -1
	
	return row, nil
}

// Close releases resources.
func (op *CopyOperator) Close() error {
	// Close file handles if we opened them
	if closer, ok := op.reader.(io.Closer); ok && op.plan.Source != "STDIN" {
		closer.Close()
	}
	if closer, ok := op.writer.(io.Closer); ok && op.plan.Source != "STDOUT" {
		closer.Close()
	}
	return nil
}

// Schema returns the output schema.
func (op *CopyOperator) Schema() *Schema {
	return op.schema
}

// executeCopyFrom handles COPY FROM operations (data import).
func (op *CopyOperator) executeCopyFrom() error {
	if op.reader == nil {
		return fmt.Errorf("no input source for COPY FROM")
	}
	
	// Get table columns
	table := op.plan.TableRef
	columns := op.plan.Columns
	if len(columns) == 0 {
		// Use all columns if not specified
		columns = make([]string, len(table.Columns))
		for i, col := range table.Columns {
			columns[i] = col.Name
		}
	}
	
	// Create column index map
	colIndexMap := make(map[string]int)
	for i, col := range table.Columns {
		colIndexMap[col.Name] = i
	}
	
	// Determine format
	format := op.plan.Options["FORMAT"]
	if format == "" {
		format = "TEXT"
	}
	
	// Parse data based on format
	switch strings.ToUpper(format) {
	case "CSV":
		return op.executeCopyFromCSV(columns, colIndexMap)
	case "BINARY":
		return fmt.Errorf("BINARY format not yet implemented")
	default: // TEXT format
		return op.executeCopyFromText(columns, colIndexMap)
	}
}

// executeCopyFromText handles text format COPY FROM.
func (op *CopyOperator) executeCopyFromText(columns []string, colIndexMap map[string]int) error {
	delimiter := op.plan.Options["DELIMITER"]
	if delimiter == "" {
		delimiter = "\t"
	}
	
	scanner := bufio.NewScanner(op.reader)
	lineNum := 0
	
	for scanner.Scan() {
		lineNum++
		line := scanner.Text()
		
		// Skip empty lines
		if line == "" {
			continue
		}
		
		// Check for end-of-data marker
		if line == "\\." {
			break
		}
		
		// Split by delimiter
		fields := strings.Split(line, delimiter)
		if len(fields) != len(columns) {
			return fmt.Errorf("line %d: expected %d columns, got %d", lineNum, len(columns), len(fields))
		}
		
		// Build row values
		rowValues := make([]types.Value, len(op.plan.TableRef.Columns))
		for i := range rowValues {
			rowValues[i] = types.NewNullValue()
		}
		
		for i, field := range fields {
			colName := columns[i]
			colIdx, ok := colIndexMap[colName]
			if !ok {
				return fmt.Errorf("column %s not found in table", colName)
			}
			
			col := op.plan.TableRef.Columns[colIdx]
			value, err := parseTextValue(field, col.DataType)
			if err != nil {
				return fmt.Errorf("line %d, column %s: %w", lineNum, colName, err)
			}
			rowValues[colIdx] = value
		}
		
		// Insert the row
		if err := op.insertRow(rowValues); err != nil {
			return fmt.Errorf("line %d: %w", lineNum, err)
		}
		
		op.rowCount++
	}
	
	return scanner.Err()
}

// executeCopyFromCSV handles CSV format COPY FROM.
func (op *CopyOperator) executeCopyFromCSV(columns []string, colIndexMap map[string]int) error {
	reader := csv.NewReader(op.reader)
	
	// Configure CSV reader based on options
	if delimiter := op.plan.Options["DELIMITER"]; delimiter != "" && len(delimiter) > 0 {
		reader.Comma = rune(delimiter[0])
	}
	
	// Check if header row should be skipped
	if _, hasHeader := op.plan.Options["HEADER"]; hasHeader {
		if _, err := reader.Read(); err != nil {
			return fmt.Errorf("error reading header: %w", err)
		}
	}
	
	lineNum := 0
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("CSV parse error at line %d: %w", lineNum, err)
		}
		
		lineNum++
		
		if len(record) != len(columns) {
			return fmt.Errorf("line %d: expected %d columns, got %d", lineNum, len(columns), len(record))
		}
		
		// Build row values
		rowValues := make([]types.Value, len(op.plan.TableRef.Columns))
		for i := range rowValues {
			rowValues[i] = types.NewNullValue()
		}
		
		for i, field := range record {
			colName := columns[i]
			colIdx, ok := colIndexMap[colName]
			if !ok {
				return fmt.Errorf("column %s not found in table", colName)
			}
			
			col := op.plan.TableRef.Columns[colIdx]
			// Handle empty string as NULL in CSV
			if field == "" {
				rowValues[colIdx] = types.NewNullValue()
			} else {
				value, err := parseTextValue(field, col.DataType)
				if err != nil {
					return fmt.Errorf("line %d, column %s: %w", lineNum, colName, err)
				}
				rowValues[colIdx] = value
			}
		}
		
		// Insert the row
		if err := op.insertRow(rowValues); err != nil {
			return fmt.Errorf("line %d: %w", lineNum, err)
		}
		
		op.rowCount++
	}
	
	return nil
}

// insertRow inserts a single row into the table.
func (op *CopyOperator) insertRow(values []types.Value) error {
	// Use the storage backend to insert the row
	row := &Row{Values: values}
	_, err := op.storage.InsertRow(op.plan.TableRef.ID, row)
	return err
}

// executeCopyTo handles COPY TO operations (data export).
func (op *CopyOperator) executeCopyTo() error {
	if op.writer == nil {
		return fmt.Errorf("no output destination for COPY TO")
	}
	
	// TODO: Implement COPY TO functionality
	// This would involve:
	// 1. Creating a scan over the table
	// 2. Formatting rows according to the specified format
	// 3. Writing to the output stream
	
	return fmt.Errorf("COPY TO not yet implemented")
}

// parseTextValue parses a text value into the appropriate type.
func parseTextValue(text string, dataType types.DataType) (types.Value, error) {
	// Handle NULL values
	if text == "\\N" {
		return types.NewNullValue(), nil
	}
	
	// Handle escape sequences
	text = unescapeText(text)
	
	// Parse based on data type
	switch dataType.Name() {
	case "INTEGER", "BIGINT", "SMALLINT":
		n, err := strconv.ParseInt(text, 10, 64)
		if err != nil {
			return types.Value{}, fmt.Errorf("invalid integer: %s", text)
		}
		return types.NewValue(n), nil
		
	case "DECIMAL", "NUMERIC":
		f, err := strconv.ParseFloat(text, 64)
		if err != nil {
			return types.Value{}, fmt.Errorf("invalid decimal: %s", text)
		}
		return types.NewValue(f), nil
		
	case "BOOLEAN":
		switch strings.ToLower(text) {
		case "t", "true", "yes", "on", "1":
			return types.NewValue(true), nil
		case "f", "false", "no", "off", "0":
			return types.NewValue(false), nil
		default:
			return types.Value{}, fmt.Errorf("invalid boolean: %s", text)
		}
		
	case "VARCHAR", "CHAR", "TEXT":
		return types.NewValue(text), nil
		
	case "TIMESTAMP":
		t, err := time.Parse("2006-01-02 15:04:05", text)
		if err != nil {
			// Try other formats
			t, err = time.Parse(time.RFC3339, text)
			if err != nil {
				return types.Value{}, fmt.Errorf("invalid timestamp: %s", text)
			}
		}
		return types.NewValue(t), nil
		
	case "DATE":
		t, err := time.Parse("2006-01-02", text)
		if err != nil {
			return types.Value{}, fmt.Errorf("invalid date: %s", text)
		}
		return types.NewValue(t), nil
		
	default:
		// Default to string
		return types.NewValue(text), nil
	}
}

// unescapeText unescapes PostgreSQL text format escape sequences.
func unescapeText(text string) string {
	// Handle common escape sequences
	text = strings.ReplaceAll(text, "\\n", "\n")
	text = strings.ReplaceAll(text, "\\t", "\t")
	text = strings.ReplaceAll(text, "\\r", "\r")
	text = strings.ReplaceAll(text, "\\\\", "\\")
	return text
}


// SetReader sets the input reader for COPY FROM STDIN.
func (op *CopyOperator) SetReader(r io.Reader) {
	op.reader = r
}

// SetWriter sets the output writer for COPY TO STDOUT.
func (op *CopyOperator) SetWriter(w io.Writer) {
	op.writer = w
}