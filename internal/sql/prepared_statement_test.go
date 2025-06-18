package sql

import (
	"testing"

	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

func TestStatementCache(t *testing.T) {
	cache := NewStatementCache()
	
	// Test storing and retrieving
	stmt1 := &PreparedStatement{
		Name: "test_stmt",
		SQL:  "SELECT * FROM users WHERE id = $1",
		ParamTypes: []types.DataType{types.Integer},
	}
	
	err := cache.Store(stmt1)
	if err != nil {
		t.Fatalf("Failed to store statement: %v", err)
	}
	
	// Retrieve the statement
	retrieved, err := cache.Get("test_stmt")
	if err != nil {
		t.Fatalf("Failed to get statement: %v", err)
	}
	
	if retrieved.SQL != stmt1.SQL {
		t.Errorf("Retrieved statement SQL mismatch: got %s, want %s", retrieved.SQL, stmt1.SQL)
	}
	
	// Test getting non-existent statement
	_, err = cache.Get("non_existent")
	if err == nil {
		t.Error("Expected error for non-existent statement")
	}
	
	// Test deleting statement
	err = cache.Delete("test_stmt")
	if err != nil {
		t.Fatalf("Failed to delete statement: %v", err)
	}
	
	_, err = cache.Get("test_stmt")
	if err == nil {
		t.Error("Statement should not exist after deletion")
	}
	
	// Test storing unnamed statement
	unnamedStmt := &PreparedStatement{
		Name: "",
		SQL:  "SELECT 1",
	}
	
	err = cache.Store(unnamedStmt)
	if err == nil {
		t.Error("Should not be able to store unnamed statement in cache")
	}
}

func TestPortalManager(t *testing.T) {
	manager := NewPortalManager()
	
	// Create a prepared statement for testing
	stmt := &PreparedStatement{
		Name: "test_stmt",
		SQL:  "SELECT * FROM users WHERE id = $1",
		ParamTypes: []types.DataType{types.Integer},
	}
	
	// Test creating named portal
	portal1 := &Portal{
		Name:      "test_portal",
		Statement: stmt,
		ParamValues: []types.Value{
			types.NewValue(int64(123)),
		},
	}
	
	err := manager.Create(portal1)
	if err != nil {
		t.Fatalf("Failed to create portal: %v", err)
	}
	
	// Retrieve the portal
	retrieved, err := manager.Get("test_portal")
	if err != nil {
		t.Fatalf("Failed to get portal: %v", err)
	}
	
	if retrieved.Name != portal1.Name {
		t.Errorf("Retrieved portal name mismatch: got %s, want %s", retrieved.Name, portal1.Name)
	}
	
	// Test creating duplicate portal
	err = manager.Create(portal1)
	if err == nil {
		t.Error("Should not be able to create duplicate portal")
	}
	
	// Test unnamed portal
	unnamedPortal := &Portal{
		Name:      "",
		Statement: stmt,
		ParamValues: []types.Value{
			types.NewValue(int64(456)),
		},
	}
	
	err = manager.Create(unnamedPortal)
	if err != nil {
		t.Fatalf("Failed to create unnamed portal: %v", err)
	}
	
	// Retrieve unnamed portal
	retrieved, err = manager.Get("")
	if err != nil {
		t.Fatalf("Failed to get unnamed portal: %v", err)
	}
	
	if len(retrieved.ParamValues) != 1 || retrieved.ParamValues[0].Data.(int64) != 456 {
		t.Error("Unnamed portal parameter mismatch")
	}
	
	// Test destroying unnamed portal
	manager.DestroyUnnamedPortal()
	
	_, err = manager.Get("")
	if err == nil {
		t.Error("Unnamed portal should not exist after destruction")
	}
	
	// Test deleting named portal
	err = manager.Delete("test_portal")
	if err != nil {
		t.Fatalf("Failed to delete portal: %v", err)
	}
	
	_, err = manager.Get("test_portal")
	if err == nil {
		t.Error("Portal should not exist after deletion")
	}
	
	// Test deleting non-existent portal (should not error)
	err = manager.Delete("non_existent")
	if err != nil {
		t.Error("Deleting non-existent portal should not error")
	}
}

func TestParameterTypeInference(t *testing.T) {
	tests := []struct {
		name         string
		sql          string
		expectedTypes []types.DataType
	}{
		{
			name: "Simple WHERE clause",
			sql:  "SELECT * FROM users WHERE id = $1",
			expectedTypes: []types.DataType{types.Unknown}, // Need catalog for proper inference
		},
		{
			name: "Multiple parameters",
			sql:  "SELECT * FROM users WHERE id = $1 AND name = $2",
			expectedTypes: []types.DataType{types.Unknown, types.Unknown},
		},
		{
			name: "INSERT with literals",
			sql:  "INSERT INTO users (id, name) VALUES ($1, $2)",
			expectedTypes: []types.DataType{types.Unknown, types.Unknown},
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := parser.NewParser(tt.sql)
			stmt, err := p.Parse()
			if err != nil {
				t.Fatalf("Failed to parse SQL: %v", err)
			}
			
			types, err := InferParameterTypes(stmt)
			if err != nil {
				t.Fatalf("Failed to infer parameter types: %v", err)
			}
			
			if len(types) != len(tt.expectedTypes) {
				t.Logf("Statement type: %T", stmt)
				if sel, ok := stmt.(*parser.SelectStmt); ok && sel.Where != nil {
					t.Logf("WHERE type: %T", sel.Where)
				}
				t.Errorf("Type count mismatch: got %d, want %d", len(types), len(tt.expectedTypes))
			}
		})
	}
}

func TestParseParameterValue(t *testing.T) {
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
			name:     "Integer value",
			data:     []byte("123"),
			dataType: types.Integer,
			format:   0,
			expected: int64(123),
		},
		{
			name:     "String value",
			data:     []byte("hello"),
			dataType: types.Varchar(255),
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
			name:     "Unknown type integer",
			data:     []byte("456"),
			dataType: types.Unknown,
			format:   0,
			expected: int64(456),
		},
		{
			name:     "Unknown type string",
			data:     []byte("world"),
			dataType: types.Unknown,
			format:   0,
			expected: "world",
		},
		{
			name:     "Binary format",
			data:     []byte{1, 2, 3},
			dataType: types.Integer,
			format:   1,
			wantErr:  true,
		},
		{
			name:     "Invalid integer",
			data:     []byte("not_a_number"),
			dataType: types.Integer,
			format:   0,
			wantErr:  true,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, err := ParseParameterValue(tt.data, tt.dataType, tt.format)
			
			if tt.wantErr {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}
			
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			
			if tt.isNull {
				if !value.IsNull() {
					t.Error("Expected null value")
				}
				return
			}
			
			if value.IsNull() {
				t.Error("Expected non-null value")
			}
			
			if value.Data != tt.expected {
				t.Errorf("Value mismatch: got %v (%T), want %v (%T)", 
					value.Data, value.Data, tt.expected, tt.expected)
			}
		})
	}
}