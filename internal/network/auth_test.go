package network

import (
	"testing"

	"github.com/dshills/QuantaDB/internal/log"
	"github.com/dshills/QuantaDB/internal/network/protocol"
)

func TestInMemoryUserStore(t *testing.T) {
	store := NewInMemoryUserStore()

	// Test adding users
	store.AddUser("alice", "password123")
	store.AddUser("bob", "secret456")

	// Test user existence
	if !store.UserExists("alice") {
		t.Error("alice should exist")
	}
	if !store.UserExists("bob") {
		t.Error("bob should exist")
	}
	if store.UserExists("charlie") {
		t.Error("charlie should not exist")
	}

	// Test authentication
	if !store.Authenticate("alice", "password123") {
		t.Error("alice authentication should succeed")
	}
	if !store.Authenticate("bob", "secret456") {
		t.Error("bob authentication should succeed")
	}
	if store.Authenticate("alice", "wrongpassword") {
		t.Error("alice authentication should fail with wrong password")
	}
	if store.Authenticate("charlie", "anypassword") {
		t.Error("charlie authentication should fail (user doesn't exist)")
	}

	// Test user count
	if count := store.GetUserCount(); count != 2 {
		t.Errorf("Expected 2 users, got %d", count)
	}

	// Test listing users
	users := store.ListUsers()
	if len(users) != 2 {
		t.Errorf("Expected 2 users in list, got %d", len(users))
	}

	// Test removing user
	store.RemoveUser("alice")
	if store.UserExists("alice") {
		t.Error("alice should not exist after removal")
	}
	if count := store.GetUserCount(); count != 1 {
		t.Errorf("Expected 1 user after removal, got %d", count)
	}
}

func TestInMemoryUserStoreMD5(t *testing.T) {
	store := NewInMemoryUserStore()
	store.AddUser("testuser", "testpass")

	// Test MD5 hash generation
	salt := "abcd"
	hash, exists := store.GetUserMD5("testuser", salt)
	if !exists {
		t.Error("GetUserMD5 should return true for existing user")
	}

	// Verify hash format (should start with "md5")
	if len(hash) != 35 || hash[:3] != "md5" {
		t.Errorf("Expected MD5 hash to be 35 characters and start with 'md5', got: %s", hash)
	}

	// Test with non-existent user
	_, exists = store.GetUserMD5("nonexistent", salt)
	if exists {
		t.Error("GetUserMD5 should return false for non-existent user")
	}

	// Test that same input produces same hash
	hash2, _ := store.GetUserMD5("testuser", salt)
	if hash != hash2 {
		t.Error("Same input should produce same hash")
	}

	// Test that different salt produces different hash
	hash3, _ := store.GetUserMD5("testuser", "efgh")
	if hash == hash3 {
		t.Error("Different salt should produce different hash")
	}
}

func TestAuthenticationConfiguration(t *testing.T) {
	// Test default configuration
	config := DefaultConfig()
	if config.AuthMethod != "none" {
		t.Errorf("Expected default auth method 'none', got '%s'", config.AuthMethod)
	}
	if config.UserStore != nil {
		t.Error("Expected default user store to be nil")
	}

	// Test with user store
	store := NewInMemoryUserStore()
	store.AddUser("admin", "admin123")

	config.AuthMethod = "password"
	config.UserStore = store

	if config.AuthMethod != "password" {
		t.Error("Auth method should be 'password'")
	}
	if config.UserStore == nil {
		t.Error("User store should not be nil")
	}
	if !config.UserStore.UserExists("admin") {
		t.Error("admin user should exist in user store")
	}
}

func TestServerAuthConfiguration(t *testing.T) {
	config := DefaultConfig()
	logger := log.Default()

	// Create server (we can't fully test without catalog/engine)
	server := &Server{
		config: config,
		logger: logger,
	}

	// Test setting authentication method
	store := NewInMemoryUserStore()
	store.AddUser("testuser", "testpass")

	server.config.AuthMethod = "md5"
	server.config.UserStore = store

	if server.config.AuthMethod != "md5" {
		t.Error("Server auth method should be 'md5'")
	}
	if server.config.UserStore == nil {
		t.Error("Server user store should not be nil")
	}
	if !server.config.UserStore.UserExists("testuser") {
		t.Error("testuser should exist in server user store")
	}
}

// Helper function to create a mock connection context for testing
func createMockConnectionContext() *ConnectionContext {
	return &ConnectionContext{
		ID:     1,
		Params: make(map[string]string),
	}
}

func TestAuthMethodDetermination(t *testing.T) {
	config := DefaultConfig()
	logger := log.Default()
	server := &Server{
		config: config,
		logger: logger,
	}

	// Mock protocol handler (we'll need to be careful here since it's an interface)
	// For this test, we'll just test the auth handler creation
	authHandler := &BasicAuthenticationHandler{
		server: server,
		logger: logger,
		state:  StateStartup,
	}

	connCtx := createMockConnectionContext()
	connCtx.Params["user"] = "testuser"

	// Test "none" method
	server.config.AuthMethod = "none"
	authType, authData, err := authHandler.determineAuthMethod(connCtx)
	if err != nil {
		t.Errorf("determineAuthMethod failed for 'none': %v", err)
	}
	if authType != protocol.AuthOK {
		t.Errorf("Expected AuthOK for 'none' method, got %d", authType)
	}
	if authData != nil {
		t.Error("Expected nil auth data for 'none' method")
	}

	// Test "password" method with user store
	store := NewInMemoryUserStore()
	store.AddUser("testuser", "testpass")
	server.config.AuthMethod = "password"
	server.config.UserStore = store

	authType, authData, err = authHandler.determineAuthMethod(connCtx)
	if err != nil {
		t.Errorf("determineAuthMethod failed for 'password': %v", err)
	}
	if authType != protocol.AuthCleartextPassword {
		t.Errorf("Expected AuthCleartextPassword for 'password' method, got %d", authType)
	}
	if authData != nil {
		t.Error("Expected nil auth data for 'password' method")
	}

	// Test "md5" method
	server.config.AuthMethod = "md5"
	authType, authData, err = authHandler.determineAuthMethod(connCtx)
	if err != nil {
		t.Errorf("determineAuthMethod failed for 'md5': %v", err)
	}
	if authType != protocol.AuthMD5Password {
		t.Errorf("Expected AuthMD5Password for 'md5' method, got %d", authType)
	}
	if len(authData) != 4 {
		t.Errorf("Expected 4-byte salt for 'md5' method, got %d bytes", len(authData))
	}

	// Test with non-existent user
	connCtx.Params["user"] = "nonexistent"
	_, _, err = authHandler.determineAuthMethod(connCtx)
	if err == nil {
		t.Error("Expected error for non-existent user")
	}
}

func TestCredentialValidation(t *testing.T) {
	config := DefaultConfig()
	logger := log.Default()
	store := NewInMemoryUserStore()
	store.AddUser("testuser", "testpass")

	server := &Server{
		config: config,
		logger: logger,
	}
	server.config.UserStore = store

	authHandler := &BasicAuthenticationHandler{
		server: server,
		logger: logger,
		state:  StateStartup,
	}

	connCtx := createMockConnectionContext()
	connCtx.Params["user"] = "testuser"

	// Test "none" method (should always return true)
	server.config.AuthMethod = "none"
	if !authHandler.validateCredentials("testuser", "anypassword", connCtx) {
		t.Error("'none' method should always validate successfully")
	}

	// Test "password" method
	server.config.AuthMethod = "password"
	if !authHandler.validateCredentials("testuser", "testpass", connCtx) {
		t.Error("Valid password should authenticate successfully")
	}
	if authHandler.validateCredentials("testuser", "wrongpass", connCtx) {
		t.Error("Invalid password should fail authentication")
	}
	if authHandler.validateCredentials("nonexistent", "anypass", connCtx) {
		t.Error("Non-existent user should fail authentication")
	}

	// Test "md5" method
	server.config.AuthMethod = "md5"
	salt := "test"
	connCtx.Params["_md5_salt"] = salt

	// Get the expected hash
	expectedHash, _ := store.GetUserMD5("testuser", salt)

	if !authHandler.validateCredentials("testuser", expectedHash, connCtx) {
		t.Error("Valid MD5 hash should authenticate successfully")
	}
	if authHandler.validateCredentials("testuser", "wronghash", connCtx) {
		t.Error("Invalid MD5 hash should fail authentication")
	}
}
