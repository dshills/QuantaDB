package network

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"

	"github.com/dshills/QuantaDB/internal/log"
	"github.com/dshills/QuantaDB/internal/network/protocol"
)

// Connection states are defined in connection.go

// BasicAuthenticationHandler implements AuthenticationHandler interface.
type BasicAuthenticationHandler struct {
	protocolHandler ProtocolHandler
	logger          log.Logger
	state           int
}

// NewBasicAuthenticationHandler creates a new authentication handler.
func NewBasicAuthenticationHandler(protocolHandler ProtocolHandler, logger log.Logger) *BasicAuthenticationHandler {
	return &BasicAuthenticationHandler{
		protocolHandler: protocolHandler,
		logger:          logger,
		state:           StateStartup,
	}
}

// HandleStartup processes the startup message and negotiates connection parameters.
func (ah *BasicAuthenticationHandler) HandleStartup(connCtx *ConnectionContext) error {
	// For now, use a simplified implementation
	// TODO: Properly extract startup handling from original connection.go
	
	// Store connection parameters
	if connCtx.Params == nil {
		connCtx.Params = make(map[string]string)
	}
	
	// Set default parameters
	connCtx.Params["user"] = "quantadb"
	connCtx.Params["database"] = "quantadb"
	connCtx.Params["application_name"] = "quantadb"

	// Generate a secret key for this connection
	var keyBytes [4]byte
	if _, err := rand.Read(keyBytes[:]); err != nil {
		return fmt.Errorf("failed to generate secret key: %w", err)
	}
	connCtx.SecretKey = binary.BigEndian.Uint32(keyBytes[:])

	// Send authentication OK (no authentication required for now)
	writer := ah.protocolHandler.(*BasicProtocolHandler).GetWriter()
	auth := &protocol.Authentication{
		Type: protocol.AuthOK, // No authentication for now
	}
	if err := protocol.WriteMessage(writer, auth.ToMessage()); err != nil {
		return fmt.Errorf("failed to send authentication OK: %w", err)
	}

	// Send required parameter status messages
	requiredParams := map[string]string{
		"server_version":           "14.0", // Mimic PostgreSQL 14
		"server_encoding":          "UTF8",
		"client_encoding":          "UTF8",
		"application_name":         connCtx.Params["application_name"],
		"is_superuser":            "off",
		"session_authorization":   connCtx.Params["user"],
		"DateStyle":               "ISO, MDY",
		"IntervalStyle":           "postgres",
		"TimeZone":                "UTC",
		"integer_datetimes":       "on",
		"standard_conforming_strings": "on",
		"default_transaction_isolation": "read committed",
		"in_hot_standby":          "off",
	}

	for key, value := range requiredParams {
		if value == "" {
			continue // Skip empty values
		}
		paramStatus := &protocol.ParameterStatus{
			Name:  key,
			Value: value,
		}
		if err := protocol.WriteMessage(writer, paramStatus.ToMessage()); err != nil {
			return fmt.Errorf("failed to send parameter status: %w", err)
		}
	}

	// Send backend key data
	backendKey := &protocol.BackendKeyData{
		ProcessID: connCtx.ID,
		SecretKey: connCtx.SecretKey,
	}
	if err := protocol.WriteMessage(writer, backendKey.ToMessage()); err != nil {
		return fmt.Errorf("failed to send backend key data: %w", err)
	}

	// Send ready for query
	if err := ah.protocolHandler.SendReadyForQuery('I'); err != nil { // I = idle
		return fmt.Errorf("failed to send ready for query: %w", err)
	}

	ah.SetState(StateReady)
	return nil
}

// HandleAuthentication handles the authentication process.
func (ah *BasicAuthenticationHandler) HandleAuthentication(connCtx *ConnectionContext) error {
	// For now, we accept all connections without authentication
	// This is a placeholder for future authentication implementation
	ah.logger.Debug("Authentication successful (no auth required)", "conn_id", connCtx.ID)
	ah.SetState(StateReady)
	return nil
}

// SetState sets the connection state and logs the transition.
func (ah *BasicAuthenticationHandler) SetState(newState int) {
	if ah.state != newState {
		oldStateName := stateNames[ah.state]
		newStateName := stateNames[newState]
		
		ah.logger.Debug("Connection state change",
			"old_state", oldStateName,
			"new_state", newStateName)
		ah.state = newState
	}
}

// GetState returns the current connection state.
func (ah *BasicAuthenticationHandler) GetState() int {
	return ah.state
}