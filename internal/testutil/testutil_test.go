package testutil

import (
	"os"
	"testing"
)

func TestTempDir(t *testing.T) {
	dir, cleanup := TempDir(t)
	defer cleanup()

	// Check directory exists
	info, err := os.Stat(dir)
	AssertNoError(t, err)
	AssertTrue(t, info.IsDir(), "expected directory")

	// Create a file in the directory
	testFile := dir + "/test.txt"
	err = os.WriteFile(testFile, []byte("test"), 0644)
	AssertNoError(t, err)

	// Verify file exists
	_, err = os.Stat(testFile)
	AssertNoError(t, err)
}

func TestAssertions(t *testing.T) {
	// Test AssertEqual
	AssertEqual(t, 42, 42)
	AssertEqual(t, "hello", "hello")
	AssertEqual(t, []int{1, 2, 3}, []int{1, 2, 3})

	// Test AssertNoError
	AssertNoError(t, nil)

	// Test AssertTrue/False
	AssertTrue(t, true, "should be true")
	AssertFalse(t, false, "should be false")
}

func TestDataGeneration(t *testing.T) {
	// Test key generation
	key1 := GenerateKey("test", 1)
	key2 := GenerateKey("test", 2)
	AssertEqual(t, "test_1", string(key1))
	AssertEqual(t, "test_2", string(key2))

	// Test value generation
	value := GenerateValue(32)
	AssertEqual(t, 32, len(value))

	// Test key-value pair generation
	keys, values := GenerateKeyValuePairs(10, 64)
	AssertEqual(t, 10, len(keys))
	AssertEqual(t, 10, len(values))

	for i := range keys {
		AssertEqual(t, string(GenerateKey("key", i)), string(keys[i]))
		AssertEqual(t, 64, len(values[i]))
	}
}
