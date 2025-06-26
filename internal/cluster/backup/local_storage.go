package backup

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// LocalStorageBackend implements BackupStorage for local filesystem
type LocalStorageBackend struct {
	basePath string
}

// NewLocalStorage creates a new local storage backend
func NewLocalStorage(basePath string) *LocalStorageBackend {
	return &LocalStorageBackend{
		basePath: basePath,
	}
}

// Store stores data to a local file
func (ls *LocalStorageBackend) Store(ctx context.Context, key string, data io.Reader) error {
	// Sanitize the key to prevent path traversal
	cleanKey := filepath.Clean(key)
	if strings.Contains(cleanKey, "..") {
		return fmt.Errorf("invalid key: path traversal not allowed")
	}

	fullPath := filepath.Join(ls.basePath, cleanKey)
	
	// Ensure directory exists
	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	// Create the file
	file, err := os.Create(fullPath)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", fullPath, err)
	}
	defer file.Close()

	// Copy data to file
	_, err = io.Copy(file, data)
	if err != nil {
		return fmt.Errorf("failed to write data to %s: %w", fullPath, err)
	}

	return nil
}

// Retrieve retrieves data from a local file
func (ls *LocalStorageBackend) Retrieve(ctx context.Context, key string) (io.ReadCloser, error) {
	// Sanitize the key
	cleanKey := filepath.Clean(key)
	if strings.Contains(cleanKey, "..") {
		return nil, fmt.Errorf("invalid key: path traversal not allowed")
	}

	fullPath := filepath.Join(ls.basePath, cleanKey)
	
	file, err := os.Open(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("file not found: %s", key)
		}
		return nil, fmt.Errorf("failed to open file %s: %w", fullPath, err)
	}

	return file, nil
}

// Delete deletes a file from local storage
func (ls *LocalStorageBackend) Delete(ctx context.Context, key string) error {
	// Sanitize the key
	cleanKey := filepath.Clean(key)
	if strings.Contains(cleanKey, "..") {
		return fmt.Errorf("invalid key: path traversal not allowed")
	}

	fullPath := filepath.Join(ls.basePath, cleanKey)
	
	err := os.Remove(fullPath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete file %s: %w", fullPath, err)
	}

	return nil
}

// List lists files with the given prefix
func (ls *LocalStorageBackend) List(ctx context.Context, prefix string) ([]string, error) {
	// Sanitize the prefix
	cleanPrefix := filepath.Clean(prefix)
	if strings.Contains(cleanPrefix, "..") {
		return nil, fmt.Errorf("invalid prefix: path traversal not allowed")
	}

	searchPath := filepath.Join(ls.basePath, cleanPrefix)
	var files []string

	err := filepath.Walk(searchPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			// If the search path doesn't exist, return empty list
			if os.IsNotExist(err) && path == searchPath {
				return filepath.SkipDir
			}
			return err
		}

		// Skip directories
		if info.IsDir() {
			return nil
		}

		// Get relative path from base
		relPath, err := filepath.Rel(ls.basePath, path)
		if err != nil {
			return err
		}

		files = append(files, relPath)
		return nil
	})

	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("failed to list files with prefix %s: %w", prefix, err)
	}

	return files, nil
}

// Exists checks if a file exists
func (ls *LocalStorageBackend) Exists(ctx context.Context, key string) (bool, error) {
	// Sanitize the key
	cleanKey := filepath.Clean(key)
	if strings.Contains(cleanKey, "..") {
		return false, fmt.Errorf("invalid key: path traversal not allowed")
	}

	fullPath := filepath.Join(ls.basePath, cleanKey)
	
	_, err := os.Stat(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("failed to check file existence %s: %w", fullPath, err)
	}

	return true, nil
}

// GetMetadata returns metadata about a file
func (ls *LocalStorageBackend) GetMetadata(ctx context.Context, key string) (*StorageMetadata, error) {
	// Sanitize the key
	cleanKey := filepath.Clean(key)
	if strings.Contains(cleanKey, "..") {
		return nil, fmt.Errorf("invalid key: path traversal not allowed")
	}

	fullPath := filepath.Join(ls.basePath, cleanKey)
	
	info, err := os.Stat(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("file not found: %s", key)
		}
		return nil, fmt.Errorf("failed to get file info %s: %w", fullPath, err)
	}

	return &StorageMetadata{
		Size:         info.Size(),
		LastModified: info.ModTime(),
		ContentType:  "application/octet-stream", // Default content type
	}, nil
}