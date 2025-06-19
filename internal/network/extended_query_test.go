package network

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/dshills/QuantaDB/internal/network/protocol"
	"github.com/dshills/QuantaDB/internal/sql"
	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

func TestExtendedQueryProtocol(t *testing.T) {
	// Create a session
	session := NewExtendedQuerySession()

	t.Run("Parse message", func(t *testing.T) {
		// Create a Parse message
		buf := new(bytes.Buffer)
		// Statement name
		buf.WriteString("test_stmt")
		buf.WriteByte(0)

		// Query
		buf.WriteString("SELECT * FROM users WHERE id = $1")
		buf.WriteByte(0)
		// Parameter count
		binary.Write(buf, binary.BigEndian, int16(1))

		// Parameter OID (0 = unspecified)
		binary.Write(buf, binary.BigEndian, uint32(0))

		parseMsg, err := protocol.ParseMessage(buf.Bytes())
		if err != nil {
			t.Fatalf("ParseMessage failed: %v", err)
		}
		if parseMsg.Name != "test_stmt" {
			t.Errorf("Expected name 'test_stmt', got '%s'", parseMsg.Name)
		}
		if parseMsg.Query != "SELECT * FROM users WHERE id = $1" {
			t.Errorf("Expected query with parameter, got '%s'", parseMsg.Query)
		}
		if len(parseMsg.ParameterOIDs) != 1 {
			t.Errorf("Expected 1 parameter OID, got %d", len(parseMsg.ParameterOIDs))
		}

		// Parse the SQL and create prepared statement
		p := parser.NewParser(parseMsg.Query)
		stmt, err := p.Parse()
		if err != nil {
			t.Fatalf("Parse SQL failed: %v", err)
		}

		paramTypes, err := sql.InferParameterTypes(stmt)
		if err != nil {
			t.Fatalf("InferParameterTypes failed: %v", err)
		}
		if len(paramTypes) != 1 {
			t.Errorf("Expected 1 parameter type, got %d", len(paramTypes))
		}

		// Create and store prepared statement
		prepStmt := &sql.PreparedStatement{
			Name:       parseMsg.Name,
			SQL:        parseMsg.Query,
			ParseTree:  stmt,
			ParamTypes: paramTypes,
		}

		err = session.StorePreparedStatement(prepStmt)
		if err != nil {
			t.Fatalf("StorePreparedStatement failed: %v", err)
		}

		// Verify we can retrieve it
		retrieved, err := session.GetPreparedStatement("test_stmt")
		if err != nil {
			t.Fatalf("GetPreparedStatement failed: %v", err)
		}
		if retrieved.Name != prepStmt.Name {
			t.Errorf("Expected name '%s', got '%s'", prepStmt.Name, retrieved.Name)
		}
		if retrieved.SQL != prepStmt.SQL {
			t.Errorf("Expected SQL '%s', got '%s'", prepStmt.SQL, retrieved.SQL)
		}
	})

	t.Run("Bind message", func(t *testing.T) {
		// Create a Bind message
		buf := new(bytes.Buffer)

		// Portal name
		buf.WriteString("test_portal")
		buf.WriteByte(0)

		// Statement name
		buf.WriteString("test_stmt")
		buf.WriteByte(0)

		// Parameter format count
		binary.Write(buf, binary.BigEndian, int16(1))
		// Format (0 = text)
		binary.Write(buf, binary.BigEndian, int16(0))

		// Parameter count
		binary.Write(buf, binary.BigEndian, int16(1))
		// Parameter value
		paramValue := []byte("123")
		binary.Write(buf, binary.BigEndian, int32(len(paramValue)))
		buf.Write(paramValue)

		// Result format count
		binary.Write(buf, binary.BigEndian, int16(0)) // 0 means all text

		bindMsg, err := protocol.ParseBind(buf.Bytes())
		if err != nil {
			t.Fatalf("ParseBind failed: %v", err)
		}
		if bindMsg.Portal != "test_portal" {
			t.Errorf("Expected portal 'test_portal', got '%s'", bindMsg.Portal)
		}
		if bindMsg.Statement != "test_stmt" {
			t.Errorf("Expected statement 'test_stmt', got '%s'", bindMsg.Statement)
		}
		if len(bindMsg.ParameterValues) != 1 {
			t.Errorf("Expected 1 parameter value, got %d", len(bindMsg.ParameterValues))
		}
		if !bytes.Equal(bindMsg.ParameterValues[0], paramValue) {
			t.Errorf("Parameter value mismatch")
		}

		// Get the prepared statement
		prepStmt, err := session.GetPreparedStatement("test_stmt")
		if err != nil {
			t.Fatalf("GetPreparedStatement failed: %v", err)
		}

		// Parse parameter values
		paramValues := make([]types.Value, len(bindMsg.ParameterValues))
		for i, data := range bindMsg.ParameterValues {
			format := int16(0)
			if i < len(bindMsg.ParameterFormats) {
				format = bindMsg.ParameterFormats[i]
			}

			value, err := sql.ParseParameterValue(data, prepStmt.ParamTypes[i], format)
			if err != nil {
				t.Fatalf("ParseParameterValue failed: %v", err)
			}
			paramValues[i] = value
		}

		// Create portal
		portal := &sql.Portal{
			Name:          bindMsg.Portal,
			Statement:     prepStmt,
			ParamValues:   paramValues,
			ParamFormats:  bindMsg.ParameterFormats,
			ResultFormats: bindMsg.ResultFormats,
		}

		err = session.CreatePortal(portal)
		if err != nil {
			t.Fatalf("CreatePortal failed: %v", err)
		}

		// Verify we can retrieve it
		retrieved, err := session.GetPortal("test_portal")
		if err != nil {
			t.Fatalf("GetPortal failed: %v", err)
		}
		if retrieved.Name != portal.Name {
			t.Errorf("Expected portal name '%s', got '%s'", portal.Name, retrieved.Name)
		}
		if retrieved.Statement.Name != portal.Statement.Name {
			t.Errorf("Expected statement name '%s', got '%s'", portal.Statement.Name, retrieved.Statement.Name)
		}
		if len(retrieved.ParamValues) != 1 {
			t.Errorf("Expected 1 parameter value, got %d", len(retrieved.ParamValues))
		}
		if retrieved.ParamValues[0].Data != int64(123) {
			t.Errorf("Expected parameter value 123, got %v", retrieved.ParamValues[0].Data)
		}
	})

	t.Run("Execute message", func(t *testing.T) {
		// Create an Execute message
		buf := new(bytes.Buffer)

		// Portal name
		buf.WriteString("test_portal")
		buf.WriteByte(0)

		// Max rows (0 = no limit)
		binary.Write(buf, binary.BigEndian, int32(0))

		execMsg, err := protocol.ParseExecute(buf.Bytes())
		if err != nil {
			t.Fatalf("ParseExecute failed: %v", err)
		}
		if execMsg.Portal != "test_portal" {
			t.Errorf("Expected portal 'test_portal', got '%s'", execMsg.Portal)
		}
		if execMsg.MaxRows != int32(0) {
			t.Errorf("Expected max rows 0, got %d", execMsg.MaxRows)
		}

		// Verify portal exists
		portal, err := session.GetPortal("test_portal")
		if err != nil {
			t.Fatalf("GetPortal failed: %v", err)
		}
		if portal == nil {
			t.Error("Portal should exist")
		}
	})

	t.Run("Unnamed statement and portal", func(t *testing.T) {
		// Parse with empty name (unnamed statement)
		prepStmt := &sql.PreparedStatement{
			Name:       "",
			SQL:        "SELECT 1",
			ParseTree:  &parser.SelectStmt{},
			ParamTypes: []types.DataType{},
		}

		err := session.StorePreparedStatement(prepStmt)
		if err != nil {
			t.Fatalf("StorePreparedStatement (unnamed) failed: %v", err)
		}

		// Verify we can retrieve unnamed statement
		retrieved, err := session.GetPreparedStatement("")
		if err != nil {
			t.Fatalf("GetPreparedStatement (unnamed) failed: %v", err)
		}
		if retrieved.SQL != prepStmt.SQL {
			t.Errorf("Expected SQL '%s', got '%s'", prepStmt.SQL, retrieved.SQL)
		}

		// Create unnamed portal
		portal := &sql.Portal{
			Name:      "",
			Statement: prepStmt,
		}

		err = session.CreatePortal(portal)
		if err != nil {
			t.Fatalf("CreatePortal (unnamed) failed: %v", err)
		}

		// Verify we can retrieve unnamed portal
		retrieved2, err := session.GetPortal("")
		if err != nil {
			t.Fatalf("GetPortal (unnamed) failed: %v", err)
		}
		if retrieved2.Statement.SQL != portal.Statement.SQL {
			t.Errorf("Expected SQL '%s', got '%s'", portal.Statement.SQL, retrieved2.Statement.SQL)
		}

		// Storing a new unnamed statement should destroy unnamed portal
		prepStmt2 := &sql.PreparedStatement{
			Name: "",
			SQL:  "SELECT 2",
		}
		err = session.StorePreparedStatement(prepStmt2)
		if err != nil {
			t.Fatalf("StorePreparedStatement (unnamed 2) failed: %v", err)
		}

		// Unnamed portal should be gone
		_, err = session.GetPortal("")
		if err == nil {
			t.Error("Unnamed portal should have been destroyed")
		}
	})

	t.Run("Close statement and portal", func(t *testing.T) {
		// Create a new statement first
		prepStmt := &sql.PreparedStatement{
			Name:       "close_test_stmt",
			SQL:        "SELECT 1",
			ParseTree:  &parser.SelectStmt{},
			ParamTypes: []types.DataType{},
		}
		err := session.StorePreparedStatement(prepStmt)
		if err != nil {
			t.Fatalf("StorePreparedStatement failed: %v", err)
		}

		// Close named statement
		err = session.CloseStatement("close_test_stmt")
		if err != nil {
			t.Fatalf("CloseStatement failed: %v", err)
		}

		// Should not be retrievable
		_, err = session.GetPreparedStatement("close_test_stmt")
		if err == nil {
			t.Error("Statement should have been closed")
		}

		// Create a portal to close
		portal := &sql.Portal{
			Name:      "close_test_portal",
			Statement: &sql.PreparedStatement{},
		}
		err = session.CreatePortal(portal)
		if err != nil {
			t.Fatalf("CreatePortal failed: %v", err)
		}

		// Close named portal
		err = session.ClosePortal("close_test_portal")
		if err != nil {
			t.Fatalf("ClosePortal failed: %v", err)
		}

		// Should not be retrievable
		_, err = session.GetPortal("close_test_portal")
		if err == nil {
			t.Error("Portal should have been closed")
		}

		// Closing non-existent portal should not error (PostgreSQL behavior)
		err = session.ClosePortal("non_existent")
		if err != nil {
			t.Error("Closing non-existent portal should not error")
		}
	})
}

func TestParameterParsing(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		dataType types.DataType
		format   int16
		expected interface{}
		isNull   bool
		wantErr  bool
	}{
		{
			name:     "Integer text format",
			data:     []byte("42"),
			dataType: types.Integer,
			format:   0,
			expected: int64(42),
		},
		{
			name:     "String text format",
			data:     []byte("hello"),
			dataType: types.Varchar(50),
			format:   0,
			expected: "hello",
		},
		{
			name:     "Boolean true",
			data:     []byte("true"),
			dataType: types.Boolean,
			format:   0,
			expected: true,
		},
		{
			name:     "Boolean t",
			data:     []byte("t"),
			dataType: types.Boolean,
			format:   0,
			expected: true,
		},
		{
			name:     "Boolean false",
			data:     []byte("false"),
			dataType: types.Boolean,
			format:   0,
			expected: false,
		},
		{
			name:     "Null value",
			data:     []byte{},
			dataType: types.Integer,
			format:   0,
			isNull:   true,
		},
		{
			name:     "Unknown type infers integer",
			data:     []byte("123"),
			dataType: types.Unknown,
			format:   0,
			expected: int64(123),
		},
		{
			name:     "Unknown type infers string",
			data:     []byte("not a number"),
			dataType: types.Unknown,
			format:   0,
			expected: "not a number",
		},
		{
			name:     "Binary format not supported",
			data:     []byte{0, 0, 0, 42},
			dataType: types.Integer,
			format:   1,
			wantErr:  true,
		},
		{
			name:     "Invalid integer",
			data:     []byte("not an int"),
			dataType: types.Integer,
			format:   0,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, err := sql.ParseParameterValue(tt.data, tt.dataType, tt.format)

			if tt.wantErr {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("ParseParameterValue failed: %v", err)
			}

			if tt.isNull {
				if !value.IsNull() {
					t.Error("Expected null value")
				}
			} else {
				if value.Data != tt.expected {
					t.Errorf("Expected %v, got %v", tt.expected, value.Data)
				}
			}
		})
	}
}
