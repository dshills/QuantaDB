package network

import (
	"crypto/md5"
	"fmt"
	"sync"
)

// InMemoryUserStore provides a simple in-memory user store for authentication
type InMemoryUserStore struct {
	mu    sync.RWMutex
	users map[string]string // username -> password
}

// NewInMemoryUserStore creates a new in-memory user store
func NewInMemoryUserStore() *InMemoryUserStore {
	return &InMemoryUserStore{
		users: make(map[string]string),
	}
}

// AddUser adds a user with a plaintext password
func (store *InMemoryUserStore) AddUser(username, password string) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.users[username] = password
}

// RemoveUser removes a user from the store
func (store *InMemoryUserStore) RemoveUser(username string) {
	store.mu.Lock()
	defer store.mu.Unlock()
	delete(store.users, username)
}

// Authenticate validates user credentials
func (store *InMemoryUserStore) Authenticate(username, password string) bool {
	store.mu.RLock()
	defer store.mu.RUnlock()
	
	storedPassword, exists := store.users[username]
	if !exists {
		return false
	}
	
	return storedPassword == password
}

// GetUserMD5 returns the MD5 hash for a user (for MD5 authentication)
// PostgreSQL MD5 format: md5(password + username) + salt
func (store *InMemoryUserStore) GetUserMD5(username, salt string) (string, bool) {
	store.mu.RLock()
	defer store.mu.RUnlock()
	
	password, exists := store.users[username]
	if !exists {
		return "", false
	}
	
	// PostgreSQL MD5 calculation: md5(md5(password + username) + salt)
	firstHash := md5.Sum([]byte(password + username))
	firstHashHex := fmt.Sprintf("%x", firstHash)
	
	secondHash := md5.Sum([]byte(firstHashHex + salt))
	secondHashHex := fmt.Sprintf("md5%x", secondHash)
	
	return secondHashHex, true
}

// UserExists checks if a user exists
func (store *InMemoryUserStore) UserExists(username string) bool {
	store.mu.RLock()
	defer store.mu.RUnlock()
	
	_, exists := store.users[username]
	return exists
}

// ListUsers returns a list of all usernames (for testing/admin purposes)
func (store *InMemoryUserStore) ListUsers() []string {
	store.mu.RLock()
	defer store.mu.RUnlock()
	
	users := make([]string, 0, len(store.users))
	for username := range store.users {
		users = append(users, username)
	}
	return users
}

// GetUserCount returns the number of users in the store
func (store *InMemoryUserStore) GetUserCount() int {
	store.mu.RLock()
	defer store.mu.RUnlock()
	return len(store.users)
}