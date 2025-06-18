package sql

import (
	"errors"
	"fmt"
	"sync"

	"github.com/dshills/QuantaDB/internal/sql/parser"
	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

// Common errors.
var (
	ErrStatementNotFound = errors.New("prepared statement not found")
	ErrPortalNotFound    = errors.New("portal not found")
)

// PreparedStatement represents a parsed and prepared SQL statement.
type PreparedStatement struct {
	// Name is the statement name (empty for unnamed statements).
	Name string

	// SQL is the original SQL text
	SQL string

	// ParseTree is the parsed AST
	ParseTree parser.Statement

	// ParamTypes are the types of parameters (may be unknown initially)
	ParamTypes []types.DataType

	// ResultFields describes the result columns (for SELECT, etc.)
	ResultFields []FieldDescription

	// QueryPlan is the cached logical plan (optional)
	QueryPlan planner.LogicalPlan

	// IsCacheable indicates if the query plan can be cached
	IsCacheable bool
}

// FieldDescription describes a result field.
type FieldDescription struct {
	Name            string
	TableOID        uint32 // OID of table (0 if not a simple table column)
	ColumnAttribute int16  // Attribute number of column (0 if not a simple table column)
	DataType        types.DataType
	TypeOID         uint32 // PostgreSQL type OID
	TypeSize        int16  // Size of type (-1 for variable)
	TypeModifier    int32  // Type modifier
	Format          int16  // 0 = text, 1 = binary
}

// Portal represents a ready-to-execute statement with bound parameters.
type Portal struct {
	// Name is the portal name (empty for unnamed portal)
	Name string

	// Statement is the prepared statement
	Statement *PreparedStatement

	// ParamValues are the bound parameter values
	ParamValues []types.Value

	// ParamFormats specifies format for each parameter (0=text, 1=binary)
	ParamFormats []int16

	// ResultFormats specifies format for each result column (0=text, 1=binary)
	ResultFormats []int16

	// CurrentRow tracks position for Execute with row limit
	CurrentRow int64

	// IsSuspended indicates if portal execution was suspended due to row limit
	IsSuspended bool
}

// StatementCache manages prepared statements for a session.
type StatementCache struct {
	mu         sync.RWMutex
	statements map[string]*PreparedStatement
}

// NewStatementCache creates a new statement cache.
func NewStatementCache() *StatementCache {
	return &StatementCache{
		statements: make(map[string]*PreparedStatement),
	}
}

// Store stores a prepared statement.
func (c *StatementCache) Store(stmt *PreparedStatement) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if stmt.Name == "" {
		return fmt.Errorf("cannot store unnamed statement")
	}

	c.statements[stmt.Name] = stmt
	return nil
}

// Get retrieves a prepared statement by name.
func (c *StatementCache) Get(name string) (*PreparedStatement, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	stmt, ok := c.statements[name]
	if !ok {
		return nil, fmt.Errorf("prepared statement %q not found", name)
	}

	return stmt, nil
}

// Delete removes a prepared statement.
func (c *StatementCache) Delete(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.statements[name]; !ok {
		return fmt.Errorf("prepared statement %q not found", name)
	}

	delete(c.statements, name)
	return nil
}

// Clear removes all prepared statements.
func (c *StatementCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.statements = make(map[string]*PreparedStatement)
}

// PortalManager manages portals for a session.
type PortalManager struct {
	mu            sync.RWMutex
	portals       map[string]*Portal
	unnamedPortal *Portal // Special handling for unnamed portal
}

// NewPortalManager creates a new portal manager.
func NewPortalManager() *PortalManager {
	return &PortalManager{
		portals: make(map[string]*Portal),
	}
}

// Create creates a new portal.
func (m *PortalManager) Create(portal *Portal) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if portal.Name == "" {
		// Unnamed portal - replace any existing unnamed portal
		m.unnamedPortal = portal
		return nil
	}

	if _, exists := m.portals[portal.Name]; exists {
		return fmt.Errorf("portal %q already exists", portal.Name)
	}

	m.portals[portal.Name] = portal
	return nil
}

// Get retrieves a portal by name.
func (m *PortalManager) Get(name string) (*Portal, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if name == "" {
		if m.unnamedPortal == nil {
			return nil, fmt.Errorf("unnamed portal does not exist")
		}
		return m.unnamedPortal, nil
	}

	portal, ok := m.portals[name]
	if !ok {
		return nil, fmt.Errorf("portal %q not found", name)
	}

	return portal, nil
}

// Delete removes a portal.
func (m *PortalManager) Delete(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if name == "" {
		m.unnamedPortal = nil
		return nil
	}

	if _, ok := m.portals[name]; !ok {
		return nil // PostgreSQL doesn't error on closing non-existent portal
	}

	delete(m.portals, name)
	return nil
}

// DestroyUnnamedPortal destroys the unnamed portal (called on Parse).
func (m *PortalManager) DestroyUnnamedPortal() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.unnamedPortal = nil
}

// Clear removes all portals.
func (m *PortalManager) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.portals = make(map[string]*Portal)
	m.unnamedPortal = nil
}

