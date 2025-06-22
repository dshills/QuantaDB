package network

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/dshills/QuantaDB/internal/log"
	"github.com/dshills/QuantaDB/internal/network/protocol"
)

// Connection states are defined in connection.go

// BasicAuthenticationHandler implements AuthenticationHandler interface.
type BasicAuthenticationHandler struct {
	protocolHandler ProtocolHandler
	logger          log.Logger
	state           int
	server          *Server // Need access to server config for auth settings
}

// NewBasicAuthenticationHandler creates a new authentication handler.
func NewBasicAuthenticationHandler(protocolHandler ProtocolHandler, logger log.Logger, server *Server) *BasicAuthenticationHandler {
	return &BasicAuthenticationHandler{
		protocolHandler: protocolHandler,
		logger:          logger,
		state:           StateStartup,
		server:          server,
	}
}

// HandleStartup processes the startup message and negotiates connection parameters.
func (ah *BasicAuthenticationHandler) HandleStartup(connCtx *ConnectionContext) error {
	// Read and parse the startup message
	reader := ah.protocolHandler.(*BasicProtocolHandler).GetReader()

	// Parse startup message
	params, err := ah.readStartupMessage(reader)
	if err != nil {
		return fmt.Errorf("failed to read startup message: %w", err)
	}

	// Store connection parameters
	if connCtx.Params == nil {
		connCtx.Params = make(map[string]string)
	}

	// Copy parsed parameters
	for key, value := range params {
		connCtx.Params[key] = value
	}

	// Set defaults for missing required parameters
	if connCtx.Params["user"] == "" {
		connCtx.Params["user"] = "quantadb"
	}
	if connCtx.Params["database"] == "" {
		connCtx.Params["database"] = "quantadb"
	}
	if connCtx.Params["application_name"] == "" {
		connCtx.Params["application_name"] = "quantadb"
	}

	// Generate a secret key for this connection
	var keyBytes [4]byte
	if _, err := rand.Read(keyBytes[:]); err != nil {
		return fmt.Errorf("failed to generate secret key: %w", err)
	}
	connCtx.SecretKey = binary.BigEndian.Uint32(keyBytes[:])

	// Send authentication request based on server configuration
	writer := ah.protocolHandler.(*BasicProtocolHandler).GetWriter()

	// Determine authentication method
	authType, authData, err := ah.determineAuthMethod(connCtx)
	if err != nil {
		return fmt.Errorf("failed to determine auth method: %w", err)
	}

	auth := &protocol.Authentication{
		Type: authType,
		Data: authData,
	}
	if err := protocol.WriteMessage(writer, auth.ToMessage()); err != nil {
		return fmt.Errorf("failed to send authentication request: %w", err)
	}

	// If no authentication required, complete startup
	if authType == protocol.AuthOK {
		return ah.completeStartup(connCtx)
	}

	// Otherwise, wait for authentication response
	ah.SetState(StateAuthentication)
	return nil
}

// HandleAuthentication handles the authentication process.
func (ah *BasicAuthenticationHandler) HandleAuthentication(connCtx *ConnectionContext) error {
	// Read authentication response from client
	reader := ah.protocolHandler.(*BasicProtocolHandler).GetReader()
	writer := ah.protocolHandler.(*BasicProtocolHandler).GetWriter()

	// Read password message
	msg, err := protocol.ReadMessage(reader)
	if err != nil {
		return fmt.Errorf("failed to read authentication message: %w", err)
	}

	if msg.Type != protocol.MsgPasswordMessage {
		return fmt.Errorf("expected password message, got %c", msg.Type)
	}

	// Parse password (null-terminated string)
	passwordBytes := bytes.TrimSuffix(msg.Data, []byte{0})
	password := string(passwordBytes)

	// Validate credentials
	username := connCtx.Params["user"]
	authenticated := ah.validateCredentials(username, password, connCtx)

	if !authenticated {
		// Send authentication failure
		auth := &protocol.Authentication{
			Type: protocol.AuthOK, // This will be an error response in practice
		}
		protocol.WriteMessage(writer, auth.ToMessage())
		return fmt.Errorf("authentication failed for user %s", username)
	}

	// Send authentication OK
	auth := &protocol.Authentication{
		Type: protocol.AuthOK,
	}
	if err := protocol.WriteMessage(writer, auth.ToMessage()); err != nil {
		return fmt.Errorf("failed to send authentication OK: %w", err)
	}

	// Complete startup process
	return ah.completeStartup(connCtx)
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

// readStartupMessage reads and parses the startup message from the client
func (ah *BasicAuthenticationHandler) readStartupMessage(reader io.Reader) (map[string]string, error) {
	// Read the startup message
	params, err := protocol.ReadStartupMessage(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to parse startup message: %w", err)
	}

	ah.logger.Info("Client connected",
		"user", params["user"],
		"database", params["database"],
		"application", params["application_name"])

	return params, nil
}

// determineAuthMethod determines the authentication method based on server config
func (ah *BasicAuthenticationHandler) determineAuthMethod(connCtx *ConnectionContext) (int32, []byte, error) {
	switch ah.server.config.AuthMethod {
	case "none":
		return protocol.AuthOK, nil, nil

	case "password":
		// Check if user exists
		if ah.server.config.UserStore != nil && !ah.server.config.UserStore.UserExists(connCtx.Params["user"]) {
			return 0, nil, fmt.Errorf("user %s does not exist", connCtx.Params["user"])
		}
		return protocol.AuthCleartextPassword, nil, nil

	case "md5":
		// Check if user exists
		if ah.server.config.UserStore != nil && !ah.server.config.UserStore.UserExists(connCtx.Params["user"]) {
			return 0, nil, fmt.Errorf("user %s does not exist", connCtx.Params["user"])
		}

		// Generate 4-byte salt for MD5 authentication
		salt := make([]byte, 4)
		if _, err := rand.Read(salt); err != nil {
			return 0, nil, fmt.Errorf("failed to generate MD5 salt: %w", err)
		}

		// Store salt in connection context for later validation
		connCtx.Params["_md5_salt"] = string(salt)

		return protocol.AuthMD5Password, salt, nil

	default:
		return 0, nil, fmt.Errorf("unsupported authentication method: %s", ah.server.config.AuthMethod)
	}
}

// validateCredentials validates user credentials based on the auth method
func (ah *BasicAuthenticationHandler) validateCredentials(username, password string, connCtx *ConnectionContext) bool {
	if ah.server.config.UserStore == nil {
		// No user store configured, allow all connections (for backwards compatibility)
		return true
	}

	switch ah.server.config.AuthMethod {
	case "none":
		return true

	case "password":
		return ah.server.config.UserStore.Authenticate(username, password)

	case "md5":
		// For MD5, the password is the MD5 hash, and we need to validate it
		salt := connCtx.Params["_md5_salt"]
		expectedHash, exists := ah.server.config.UserStore.GetUserMD5(username, salt)
		if !exists {
			return false
		}
		return password == expectedHash

	default:
		ah.logger.Error("Unsupported authentication method", "method", ah.server.config.AuthMethod)
		return false
	}
}

// completeStartup sends all the required startup messages after authentication
func (ah *BasicAuthenticationHandler) completeStartup(connCtx *ConnectionContext) error {
	writer := ah.protocolHandler.(*BasicProtocolHandler).GetWriter()

	// Send required parameter status messages
	requiredParams := map[string]string{
		"server_version":                "14.0", // Mimic PostgreSQL 14
		"server_encoding":               "UTF8",
		"client_encoding":               "UTF8",
		"application_name":              connCtx.Params["application_name"],
		"is_superuser":                  "off",
		"session_authorization":         connCtx.Params["user"],
		"DateStyle":                     "ISO, MDY",
		"IntervalStyle":                 "postgres",
		"TimeZone":                      "UTC",
		"integer_datetimes":             "on",
		"standard_conforming_strings":   "on",
		"default_transaction_isolation": "read committed",
		"in_hot_standby":                "off",
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
	ah.logger.Debug("Connection startup completed", "user", connCtx.Params["user"])
	return nil
}
