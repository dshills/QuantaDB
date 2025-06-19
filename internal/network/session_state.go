package network

import (
	"sync"

	"github.com/dshills/QuantaDB/internal/sql"
)

// ExtendedQuerySession manages extended query protocol state for a connection.
type ExtendedQuerySession struct {
	mu sync.RWMutex

	// Prepared statements
	statements  *sql.StatementCache
	unnamedStmt *sql.PreparedStatement // Unnamed statement

	// Portals
	portals *sql.PortalManager

	// Transaction state
	inTransaction bool
	transactionID uint64
}

// NewExtendedQuerySession creates a new extended query session.
func NewExtendedQuerySession() *ExtendedQuerySession {
	return &ExtendedQuerySession{
		statements: sql.NewStatementCache(),
		portals:    sql.NewPortalManager(),
	}
}

// StorePreparedStatement stores a prepared statement.
func (s *ExtendedQuerySession) StorePreparedStatement(stmt *sql.PreparedStatement) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if stmt.Name == "" {
		// Unnamed statement - special handling
		s.unnamedStmt = stmt
		// Destroy unnamed portal per PostgreSQL protocol
		s.portals.DestroyUnnamedPortal()
		return nil
	}

	return s.statements.Store(stmt)
}

// GetPreparedStatement retrieves a prepared statement.
func (s *ExtendedQuerySession) GetPreparedStatement(name string) (*sql.PreparedStatement, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if name == "" {
		if s.unnamedStmt == nil {
			return nil, sql.ErrStatementNotFound
		}
		return s.unnamedStmt, nil
	}

	return s.statements.Get(name)
}

// CreatePortal creates a new portal.
func (s *ExtendedQuerySession) CreatePortal(portal *sql.Portal) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.portals.Create(portal)
}

// GetPortal retrieves a portal.
func (s *ExtendedQuerySession) GetPortal(name string) (*sql.Portal, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.portals.Get(name)
}

// CloseStatement closes a prepared statement.
func (s *ExtendedQuerySession) CloseStatement(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if name == "" {
		s.unnamedStmt = nil
		return nil
	}

	return s.statements.Delete(name)
}

// ClosePortal closes a portal.
func (s *ExtendedQuerySession) ClosePortal(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.portals.Delete(name)
}

// Clear clears all session state (used on connection close or reset).
func (s *ExtendedQuerySession) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.statements.Clear()
	s.unnamedStmt = nil
	s.portals.Clear()
}

// SetTransactionState updates transaction state.
func (s *ExtendedQuerySession) SetTransactionState(inTxn bool, txnID uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.inTransaction = inTxn
	s.transactionID = txnID
}

// IsInTransaction returns whether a transaction is active.
func (s *ExtendedQuerySession) IsInTransaction() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.inTransaction
}

// GetTransactionID returns the current transaction ID.
func (s *ExtendedQuerySession) GetTransactionID() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.transactionID
}
