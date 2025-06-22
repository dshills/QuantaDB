package network

import (
	"bufio"
	"fmt"
	"math"
	"time"

	"github.com/dshills/QuantaDB/internal/network/protocol"
	"github.com/dshills/QuantaDB/internal/sql/executor"
	"github.com/dshills/QuantaDB/internal/sql/parser"
)

// BasicResultFormatter implements ResultFormatter interface.
type BasicResultFormatter struct {
	protocolHandler *BasicProtocolHandler
}

// NewBasicResultFormatter creates a new result formatter.
func NewBasicResultFormatter(protocolHandler *BasicProtocolHandler) *BasicResultFormatter {
	return &BasicResultFormatter{
		protocolHandler: protocolHandler,
	}
}

// SendResults formats and sends query results to the client.
func (rf *BasicResultFormatter) SendResults(result executor.Result, stmt parser.Statement, connCtx *ConnectionContext) error {
	writer := rf.protocolHandler.GetWriter()

	// Set write deadline for all result writes
	rf.protocolHandler.SetWriteTimeout(10 * time.Second) // TODO: Make configurable

	schema := result.Schema()

	// Only send row description for statements that return data
	if rf.StatementReturnsData(stmt) {
		if err := rf.sendRowDescription(writer, schema); err != nil {
			return fmt.Errorf("failed to send row description: %w", err)
		}
	}

	// Send data rows and count them
	rowCount, err := rf.sendDataRows(writer, result)
	if err != nil {
		return fmt.Errorf("failed to send data rows: %w", err)
	}

	// Send command complete
	if err := rf.sendCommandComplete(writer, stmt, rowCount); err != nil {
		return fmt.Errorf("failed to send command complete: %w", err)
	}

	return writer.Flush()
}

// sendRowDescription sends the row description message.
func (rf *BasicResultFormatter) sendRowDescription(writer *bufio.Writer, schema *executor.Schema) error {
	rowDesc := &protocol.RowDescription{
		Fields: make([]protocol.FieldDescription, len(schema.Columns)),
	}

	for i, col := range schema.Columns {
		if i >= math.MaxInt16 {
			return fmt.Errorf("too many columns: %d exceeds max int16", i+1)
		}
		rowDesc.Fields[i] = protocol.FieldDescription{
			Name:         col.Name,
			TableOID:     0,            // TODO: Add table OID
			ColumnNumber: int16(i + 1), // PostgreSQL uses 1-based indexing
			DataTypeOID:  rf.GetTypeOID(col.Type.Name()),
			DataTypeSize: rf.GetTypeSize(col.Type.Name()),
			TypeModifier: -1,
			Format:       protocol.FormatText,
		}
	}

	return protocol.WriteMessage(writer, rowDesc.ToMessage())
}

// sendDataRows sends all data rows from the result.
func (rf *BasicResultFormatter) sendDataRows(writer *bufio.Writer, result executor.Result) (int64, error) {
	var rowCount int64

	for {
		row, err := result.Next()
		if err != nil {
			return rowCount, fmt.Errorf("failed to read next row: %w", err)
		}
		if row == nil {
			break // End of results
		}

		dataRow := &protocol.DataRow{
			Values: make([][]byte, len(row.Values)),
		}

		for i, val := range row.Values {
			if val.IsNull() {
				dataRow.Values[i] = nil
			} else {
				dataRow.Values[i] = []byte(val.String())
			}
		}

		if err := protocol.WriteMessage(writer, dataRow.ToMessage()); err != nil {
			return rowCount, fmt.Errorf("failed to write data row: %w", err)
		}

		rowCount++
	}

	return rowCount, nil
}

// sendCommandComplete sends the command completion message.
func (rf *BasicResultFormatter) sendCommandComplete(writer *bufio.Writer, stmt parser.Statement, rowCount int64) error {
	tag := rf.GetCommandTag(stmt, rowCount)
	complete := &protocol.CommandComplete{Tag: tag}
	return protocol.WriteMessage(writer, complete.ToMessage())
}

// GetTypeOID returns the PostgreSQL OID for a given data type.
func (rf *BasicResultFormatter) GetTypeOID(dataType string) uint32 {
	switch dataType {
	case "INTEGER":
		return 23 // int4
	case "BIGINT":
		return 20 // int8
	case "SMALLINT":
		return 21 // int2
	case "TEXT", "VARCHAR", "CHAR":
		return 25 // text
	case "BOOLEAN":
		return 16 // bool
	case "TIMESTAMP":
		return 1114 // timestamp
	case "DATE":
		return 1082 // date
	case "DECIMAL", "NUMERIC":
		return 1700 // numeric
	case "REAL":
		return 700 // float4
	case "DOUBLE PRECISION":
		return 701 // float8
	default:
		return 25 // text as fallback
	}
}

// GetTypeSize returns the size in bytes for a given data type.
func (rf *BasicResultFormatter) GetTypeSize(dataType string) int16 {
	switch dataType {
	case "INTEGER":
		return 4
	case "BIGINT":
		return 8
	case "SMALLINT":
		return 2
	case "BOOLEAN":
		return 1
	case "TIMESTAMP":
		return 8
	case "DATE":
		return 4
	case "REAL":
		return 4
	case "DOUBLE PRECISION":
		return 8
	default:
		return -1 // Variable size
	}
}

// GetCommandTag returns the appropriate command tag for a statement.
func (rf *BasicResultFormatter) GetCommandTag(stmt parser.Statement, rowsAffected int64) string {
	switch stmt.(type) {
	case *parser.SelectStmt:
		return fmt.Sprintf("SELECT %d", rowsAffected)
	case *parser.InsertStmt:
		return fmt.Sprintf("INSERT 0 %d", rowsAffected)
	case *parser.UpdateStmt:
		return fmt.Sprintf("UPDATE %d", rowsAffected)
	case *parser.DeleteStmt:
		return fmt.Sprintf("DELETE %d", rowsAffected)
	case *parser.CreateTableStmt:
		return "CREATE TABLE"
	case *parser.CreateIndexStmt:
		return "CREATE INDEX"
	case *parser.DropTableStmt:
		return "DROP TABLE"
	case *parser.DropIndexStmt:
		return "DROP INDEX"
	case *parser.AlterTableStmt:
		return "ALTER TABLE"
	default:
		return "OK"
	}
}

// StatementReturnsData returns true if the statement returns result rows.
func (rf *BasicResultFormatter) StatementReturnsData(stmt parser.Statement) bool {
	switch stmt.(type) {
	case *parser.SelectStmt:
		return true
	default:
		return false
	}
}
