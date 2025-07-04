package executor

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/dshills/QuantaDB/internal/sql/types"
)

// Additional data type constants not in constants.go
const (
	typeBOOLEAN   = "BOOLEAN"
	typeSMALLINT  = "SMALLINT"
	typeDECIMAL   = "DECIMAL"
	typeFLOAT     = "FLOAT"
	typeDOUBLE    = "DOUBLE"
	typeREAL      = "REAL"
	typeTIMESTAMP = "TIMESTAMP"
	typeDATE      = "DATE"
	typeBYTEA     = "BYTEA"
)

// RowFormat defines the serialization format for rows.
type RowFormat struct {
	Schema *Schema
}

// NewRowFormat creates a new row formatter.
func NewRowFormat(schema *Schema) *RowFormat {
	return &RowFormat{Schema: schema}
}

// Serialize converts a row to bytes.
func (rf *RowFormat) Serialize(row *Row) ([]byte, error) {
	if len(row.Values) != len(rf.Schema.Columns) {
		return nil, fmt.Errorf("row has %d values but schema has %d columns",
			len(row.Values), len(rf.Schema.Columns))
	}

	var buf bytes.Buffer

	// Write number of columns
	colCount := len(row.Values)
	if colCount > 4294967295 { // uint32 max
		return nil, fmt.Errorf("too many columns: %d", colCount)
	}
	if err := binary.Write(&buf, binary.LittleEndian, uint32(colCount)); err != nil {
		return nil, fmt.Errorf("failed to write column count: %w", err)
	}

	// Write each value
	for i, val := range row.Values {
		if err := rf.serializeValue(&buf, val, rf.Schema.Columns[i].Type); err != nil {
			return nil, fmt.Errorf("failed to serialize column %d: %w", i, err)
		}
	}

	return buf.Bytes(), nil
}

// Deserialize converts bytes to a row.
func (rf *RowFormat) Deserialize(data []byte) (*Row, error) {
	buf := bytes.NewReader(data)

	// Read number of columns
	var colCount uint32
	if err := binary.Read(buf, binary.LittleEndian, &colCount); err != nil {
		return nil, fmt.Errorf("failed to read column count: %w", err)
	}

	schemaColCount := len(rf.Schema.Columns)
	if schemaColCount > 4294967295 { // uint32 max
		return nil, fmt.Errorf("schema has too many columns: %d", schemaColCount)
	}
	if colCount != uint32(schemaColCount) {
		return nil, fmt.Errorf("data has %d columns but schema has %d columns",
			colCount, schemaColCount)
	}

	row := &Row{
		Values: make([]types.Value, colCount),
	}

	// Read each value
	for i := uint32(0); i < colCount; i++ {
		val, err := rf.deserializeValue(buf, rf.Schema.Columns[i].Type)
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize column %d: %w", i, err)
		}
		row.Values[i] = val
	}

	return row, nil
}

// serializeValue writes a single value to the buffer.
func (rf *RowFormat) serializeValue(w io.Writer, val types.Value, dataType types.DataType) error {
	// Write null flag
	isNull := val.IsNull()
	if err := binary.Write(w, binary.LittleEndian, isNull); err != nil {
		return fmt.Errorf("failed to write null flag: %w", err)
	}

	if isNull {
		return nil
	}

	// Get base type name (handle parameterized types)
	typeName := dataType.Name()
	if idx := strings.Index(typeName, "("); idx > 0 {
		typeName = typeName[:idx]
	}

	// Write value based on type name
	switch typeName {
	case typeBOOLEAN:
		v, ok := val.Data.(bool)
		if !ok {
			return fmt.Errorf("expected bool, got %T", val.Data)
		}
		if err := binary.Write(w, binary.LittleEndian, v); err != nil {
			return err
		}

	case typeINTEGER:
		v, ok := val.Data.(int32)
		if !ok {
			return fmt.Errorf("expected int32, got %T", val.Data)
		}
		if err := binary.Write(w, binary.LittleEndian, v); err != nil {
			return err
		}

	case typeBIGINT:
		v, ok := val.Data.(int64)
		if !ok {
			return fmt.Errorf("expected int64, got %T", val.Data)
		}
		if err := binary.Write(w, binary.LittleEndian, v); err != nil {
			return err
		}

	case typeSMALLINT:
		v, ok := val.Data.(int16)
		if !ok {
			return fmt.Errorf("expected int16, got %T", val.Data)
		}
		if err := binary.Write(w, binary.LittleEndian, v); err != nil {
			return err
		}

	case typeDECIMAL:
		// For now, treat as float64
		v, ok := val.Data.(float64)
		if !ok {
			return fmt.Errorf("expected float64, got %T", val.Data)
		}
		if err := binary.Write(w, binary.LittleEndian, v); err != nil {
			return err
		}

	case typeFLOAT, typeREAL:
		// FLOAT and REAL are 32-bit
		v, ok := val.Data.(float32)
		if !ok {
			return fmt.Errorf("expected float32, got %T", val.Data)
		}
		if err := binary.Write(w, binary.LittleEndian, v); err != nil {
			return err
		}

	case typeDOUBLE:
		// DOUBLE is 64-bit
		v, ok := val.Data.(float64)
		if !ok {
			return fmt.Errorf("expected float64, got %T", val.Data)
		}
		if err := binary.Write(w, binary.LittleEndian, v); err != nil {
			return err
		}

	case typeTEXT, typeVARCHAR, typeCHAR:
		v, ok := val.Data.(string)
		if !ok {
			return fmt.Errorf("expected string, got %T", val.Data)
		}
		// Write string length
		strLen := len(v)
		if strLen > 4294967295 { // uint32 max
			return fmt.Errorf("string too long: %d bytes", strLen)
		}
		if err := binary.Write(w, binary.LittleEndian, uint32(strLen)); err != nil {
			return err
		}
		// Write string data
		if _, err := w.Write([]byte(v)); err != nil {
			return err
		}

	case typeTIMESTAMP, typeDATE:
		// Try int64 first (Unix timestamp)
		if v, ok := val.Data.(int64); ok {
			if err := binary.Write(w, binary.LittleEndian, v); err != nil {
				return err
			}
		} else if v, ok := val.Data.(time.Time); ok {
			// Convert time.Time to Unix timestamp
			if err := binary.Write(w, binary.LittleEndian, v.Unix()); err != nil {
				return err
			}
		} else {
			return fmt.Errorf("expected int64 or time.Time, got %T", val.Data)
		}

	case typeBYTEA:
		v, ok := val.Data.([]byte)
		if !ok {
			return fmt.Errorf("expected []byte, got %T", val.Data)
		}
		// Write byte array length
		arrayLen := len(v)
		if arrayLen > 4294967295 { // uint32 max
			return fmt.Errorf("byte array too long: %d bytes", arrayLen)
		}
		if err := binary.Write(w, binary.LittleEndian, uint32(arrayLen)); err != nil {
			return err
		}
		// Write byte array data
		if _, err := w.Write(v); err != nil {
			return err
		}

	default:
		return fmt.Errorf("unsupported type: %v", dataType.Name())
	}

	return nil
}

// deserializeValue reads a single value from the buffer.
func (rf *RowFormat) deserializeValue(r io.Reader, dataType types.DataType) (types.Value, error) {
	// Read null flag
	var isNull bool
	if err := binary.Read(r, binary.LittleEndian, &isNull); err != nil {
		return types.Value{}, fmt.Errorf("failed to read null flag: %w", err)
	}

	if isNull {
		return types.NewNullValue(), nil
	}

	// Get base type name (handle parameterized types)
	typeName := dataType.Name()
	if idx := strings.Index(typeName, "("); idx > 0 {
		typeName = typeName[:idx]
	}

	// Read value based on type name
	switch typeName {
	case typeBOOLEAN:
		var v bool
		if err := binary.Read(r, binary.LittleEndian, &v); err != nil {
			return types.Value{}, err
		}
		return types.NewValue(v), nil

	case typeINTEGER:
		var v int32
		if err := binary.Read(r, binary.LittleEndian, &v); err != nil {
			return types.Value{}, err
		}
		return types.NewValue(v), nil

	case typeBIGINT:
		var v int64
		if err := binary.Read(r, binary.LittleEndian, &v); err != nil {
			return types.Value{}, err
		}
		return types.NewValue(v), nil

	case typeSMALLINT:
		var v int16
		if err := binary.Read(r, binary.LittleEndian, &v); err != nil {
			return types.Value{}, err
		}
		return types.NewValue(v), nil

	case typeDECIMAL:
		var v float64
		if err := binary.Read(r, binary.LittleEndian, &v); err != nil {
			return types.Value{}, err
		}
		return types.NewValue(v), nil

	case typeFLOAT, typeREAL:
		var v float32
		if err := binary.Read(r, binary.LittleEndian, &v); err != nil {
			return types.Value{}, err
		}
		return types.NewValue(v), nil

	case typeDOUBLE:
		var v float64
		if err := binary.Read(r, binary.LittleEndian, &v); err != nil {
			return types.Value{}, err
		}
		return types.NewValue(v), nil

	case typeTEXT, typeVARCHAR, typeCHAR:
		// Read string length
		var length uint32
		if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
			return types.Value{}, err
		}
		// Read string data
		data := make([]byte, length)
		if _, err := io.ReadFull(r, data); err != nil {
			return types.Value{}, err
		}
		return types.NewValue(string(data)), nil

	case typeTIMESTAMP:
		var v int64
		if err := binary.Read(r, binary.LittleEndian, &v); err != nil {
			return types.Value{}, err
		}
		// Convert Unix timestamp back to time.Time
		t := time.Unix(v, 0).UTC()
		return types.NewTimestampValue(t), nil

	case typeDATE:
		var v int64
		if err := binary.Read(r, binary.LittleEndian, &v); err != nil {
			return types.Value{}, err
		}
		// Convert Unix timestamp back to time.Time
		t := time.Unix(v, 0).UTC()
		return types.NewDateValue(t), nil

	case typeBYTEA:
		// Read byte array length
		var length uint32
		if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
			return types.Value{}, err
		}
		// Read byte array data
		data := make([]byte, length)
		if _, err := io.ReadFull(r, data); err != nil {
			return types.Value{}, err
		}
		return types.NewValue(data), nil

	default:
		return types.Value{}, fmt.Errorf("unsupported type: %v", dataType.Name())
	}
}

// RowKeyFormat defines the format for row keys.
type RowKeyFormat struct {
	TableName  string
	SchemaName string
}

// GenerateRowKey creates a key for a row.
func (rkf *RowKeyFormat) GenerateRowKey(primaryKey interface{}) []byte {
	return []byte(fmt.Sprintf("table:%s:%s:row:%v", rkf.SchemaName, rkf.TableName, primaryKey))
}

// ParseRowKey extracts information from a row key.
func (rkf *RowKeyFormat) ParseRowKey(key []byte) (primaryKey string, err error) {
	keyStr := string(key)
	prefix := fmt.Sprintf("table:%s:%s:row:", rkf.SchemaName, rkf.TableName)

	if len(keyStr) <= len(prefix) {
		return "", fmt.Errorf("invalid row key format")
	}

	primaryKey = keyStr[len(prefix):]
	return primaryKey, nil
}

// IsRowKey checks if a key is a row key for this table.
func (rkf *RowKeyFormat) IsRowKey(key []byte) bool {
	prefix := fmt.Sprintf("table:%s:%s:row:", rkf.SchemaName, rkf.TableName)
	return bytes.HasPrefix(key, []byte(prefix))
}
