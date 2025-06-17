package testutil

import (
	"crypto/rand"
	"fmt"
)

// GenerateKey generates a random key with given prefix
func GenerateKey(prefix string, n int) []byte {
	return []byte(fmt.Sprintf("%s_%d", prefix, n))
}

// GenerateValue generates random value of given size
func GenerateValue(size int) []byte {
	value := make([]byte, size)
	if _, err := rand.Read(value); err != nil {
		panic(err)
	}
	return value
}

// GenerateKeyValuePairs generates n key-value pairs
func GenerateKeyValuePairs(n int, valueSize int) (keys [][]byte, values [][]byte) {
	keys = make([][]byte, n)
	values = make([][]byte, n)

	for i := 0; i < n; i++ {
		keys[i] = GenerateKey("key", i)
		values[i] = GenerateValue(valueSize)
	}

	return keys, values
}
