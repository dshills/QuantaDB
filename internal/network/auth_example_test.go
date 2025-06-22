package network

import (
	"fmt"
)

// ExampleInMemoryUserStore demonstrates how to use the in-memory user store
func ExampleInMemoryUserStore() {
	// Create a new user store
	store := NewInMemoryUserStore()
	
	// Add users
	store.AddUser("alice", "password123")
	store.AddUser("bob", "secret456")
	store.AddUser("admin", "admin123")
	
	// Check if users exist
	fmt.Println("alice exists:", store.UserExists("alice"))
	fmt.Println("charlie exists:", store.UserExists("charlie"))
	
	// Authenticate users
	fmt.Println("alice auth:", store.Authenticate("alice", "password123"))
	fmt.Println("alice wrong password:", store.Authenticate("alice", "wrong"))
	
	// Get user count
	fmt.Println("User count:", store.GetUserCount())
	
	// List all users
	users := store.ListUsers()
	fmt.Println("Users:", users)
	
	// Output:
	// alice exists: true
	// charlie exists: false
	// alice auth: true
	// alice wrong password: false
	// User count: 3
	// Users: [alice bob admin]
}

// Example_serverAuthentication demonstrates how to configure server authentication
func Example_serverAuthentication() {
	// Create server configuration with authentication
	config := DefaultConfig()
	config.AuthMethod = "password"  // or "md5" or "none"
	
	// Create and configure user store
	userStore := NewInMemoryUserStore()
	userStore.AddUser("admin", "admin123")
	userStore.AddUser("user1", "password1")
	userStore.AddUser("user2", "password2")
	
	config.UserStore = userStore
	
	fmt.Printf("Auth method: %s\n", config.AuthMethod)
	fmt.Printf("User store configured: %t\n", config.UserStore != nil)
	fmt.Printf("Number of users: %d\n", userStore.GetUserCount())
	
	// Output:
	// Auth method: password
	// User store configured: true
	// Number of users: 3
}

// Example_md5Authentication demonstrates MD5 authentication
func Example_md5Authentication() {
	store := NewInMemoryUserStore()
	store.AddUser("testuser", "mypassword")
	
	// Generate MD5 hash for authentication
	salt := "abcd"
	hash, exists := store.GetUserMD5("testuser", salt)
	
	fmt.Printf("User exists: %t\n", exists)
	fmt.Printf("Hash starts with md5: %t\n", hash[:3] == "md5")
	fmt.Printf("Hash length: %d\n", len(hash))
	
	// Same salt produces same hash
	hash2, _ := store.GetUserMD5("testuser", salt)
	fmt.Printf("Consistent hashing: %t\n", hash == hash2)
	
	// Different salt produces different hash
	hash3, _ := store.GetUserMD5("testuser", "efgh")
	fmt.Printf("Different salt gives different hash: %t\n", hash != hash3)
	
	// Output:
	// User exists: true
	// Hash starts with md5: true
	// Hash length: 35
	// Consistent hashing: true
	// Different salt gives different hash: true
}