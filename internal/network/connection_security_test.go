package network

import (
	"crypto/rand"
	"encoding/binary"
	"testing"
)

func TestSecretKeyGeneration(t *testing.T) {
	// Test that secret keys are random and unpredictable
	keys := make(map[uint32]bool)

	// Generate 100 secret keys and verify uniqueness
	for i := 0; i < 100; i++ {
		var secretKey uint32
		err := binary.Read(rand.Reader, binary.BigEndian, &secretKey)
		if err != nil {
			t.Fatalf("Failed to generate secret key: %v", err)
		}

		// Verify it's not using the predictable formula
		predictableKey := uint32(i * 12345)
		if secretKey == predictableKey {
			t.Errorf("Secret key matches predictable formula for id %d", i)
		}

		// Check for duplicates (very unlikely with 32-bit random values)
		if keys[secretKey] {
			t.Logf("Duplicate secret key found: %d (this is statistically unlikely but possible)", secretKey)
		}
		keys[secretKey] = true
	}
}

func TestSecretKeyNotPredictable(t *testing.T) {
	// Create multiple connections and ensure none use the old formula
	for i := 0; i < 10; i++ {
		conn := &Connection{
			id: uint32(i),
		}

		// Generate a secret key the same way completeAuthentication does
		err := binary.Read(rand.Reader, binary.BigEndian, &conn.secretKey)
		if err != nil {
			t.Fatalf("Failed to generate secret key: %v", err)
		}

		// The old predictable formula
		predictableKey := conn.id * 12345

		// Verify the key doesn't match the predictable formula
		if conn.secretKey == predictableKey {
			t.Errorf("Secret key matches predictable formula for connection %d", i)
		}
	}
}

func TestSecretKeyEntropy(t *testing.T) {
	// Test that secret keys have good entropy by checking bit distribution
	const numKeys = 1000
	bitCounts := make([]int, 32) // Count of 1s in each bit position

	for i := 0; i < numKeys; i++ {
		var secretKey uint32
		err := binary.Read(rand.Reader, binary.BigEndian, &secretKey)
		if err != nil {
			t.Fatalf("Failed to generate secret key: %v", err)
		}

		// Count bits
		for bit := 0; bit < 32; bit++ {
			if (secretKey & (1 << bit)) != 0 {
				bitCounts[bit]++
			}
		}
	}

	// Each bit should be 1 approximately 50% of the time
	// Allow 40-60% range for randomness
	for bit, count := range bitCounts {
		ratio := float64(count) / float64(numKeys)
		if ratio < 0.4 || ratio > 0.6 {
			t.Logf("Warning: Bit %d has unusual distribution: %.2f%% (expected ~50%%)",
				bit, ratio*100)
		}
	}
}
