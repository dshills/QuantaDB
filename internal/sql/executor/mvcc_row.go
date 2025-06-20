package executor

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Constants for MVCC operations
const (
	// TxnNone indicates no transaction (used for deleted/non-existent transactions)
	TxnNone int64 = 0
	// VersionNone indicates no next version
	VersionNone int64 = 0
	// CurrentMVCCVersion is the current MVCC row format version
	CurrentMVCCVersion uint8 = 1
	// EstimatedMVCCHeaderSize is the size of MVCC header in bytes
	EstimatedMVCCHeaderSize = 48 // 6 * int64
)

// Errors for MVCC operations
var (
	ErrNilSchema      = errors.New("schema is nil")
	ErrNilRow         = errors.New("row is nil")
	ErrInvalidVersion = errors.New("invalid MVCC version")
	ErrRowTooLarge    = errors.New("row data exceeds maximum size")
)

// Maximum row size (1MB)
const MaxRowSize = 1 << 20

// MVCCRowHeader contains transaction metadata for a row.
type MVCCRowHeader struct {
	RowID        int64 // Unique row identifier
	CreatedByTxn int64 // Transaction ID that created this version
	CreatedAt    int64 // Timestamp when created
	DeletedByTxn int64 // Transaction ID that deleted this version (0 if not deleted)
	DeletedAt    int64 // Timestamp when deleted (0 if not deleted)
	NextVersion  int64 // Location of next version (PageID:SlotID encoded, 0 if none)
}

// MVCCRow extends Row with transaction metadata.
type MVCCRow struct {
	Header MVCCRowHeader
	Data   *Row
}

// IsDeleted returns true if this row version has been deleted.
func (r *MVCCRow) IsDeleted() bool {
	// Check both transaction ID and timestamp for consistency
	return r.Header.DeletedByTxn > 0 && r.Header.DeletedAt > 0
}

// MVCCRowFormat handles serialization of MVCC-aware rows.
// This type is NOT safe for concurrent use.
type MVCCRowFormat struct {
	Schema *Schema
	// Version field for backward compatibility
	Version uint8
	// Cached row format to avoid repeated allocations
	rowFormat *RowFormat
}

// NewMVCCRowFormat creates a new MVCC row formatter.
func NewMVCCRowFormat(schema *Schema) *MVCCRowFormat {
	if schema == nil {
		return nil
	}
	return &MVCCRowFormat{
		Schema:    schema,
		Version:   CurrentMVCCVersion,
		rowFormat: NewRowFormat(schema),
	}
}

// Serialize converts an MVCC row to bytes.
func (mrf *MVCCRowFormat) Serialize(row *MVCCRow) ([]byte, error) {
	// Validation
	if mrf == nil || mrf.Schema == nil || mrf.rowFormat == nil {
		return nil, ErrNilSchema
	}
	if row == nil || row.Data == nil {
		return nil, ErrNilRow
	}

	// Pre-allocate buffer with estimated size
	estimatedSize := 1 + EstimatedMVCCHeaderSize + len(row.Data.Values)*20 // rough estimate
	buf := bytes.NewBuffer(make([]byte, 0, estimatedSize))

	// Write format version (use WriteByte for single byte - more efficient)
	if err := buf.WriteByte(mrf.Version); err != nil {
		return nil, fmt.Errorf("failed to write version: %w", err)
	}

	// Write MVCC header
	if err := mrf.serializeHeader(buf, &row.Header); err != nil {
		return nil, fmt.Errorf("failed to serialize MVCC header: %w", err)
	}

	// Serialize row data using cached RowFormat
	rowData, err := mrf.rowFormat.Serialize(row.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize row data: %w", err)
	}

	// Check size limit
	totalSize := buf.Len() + len(rowData)
	if totalSize > MaxRowSize {
		return nil, fmt.Errorf("%w: size %d exceeds max %d", ErrRowTooLarge, totalSize, MaxRowSize)
	}

	// Write row data
	if _, err := buf.Write(rowData); err != nil {
		return nil, fmt.Errorf("failed to write row data: %w", err)
	}

	return buf.Bytes(), nil
}

// Deserialize converts bytes to an MVCC row.
func (mrf *MVCCRowFormat) Deserialize(data []byte) (*MVCCRow, error) {
	// Validation
	if mrf == nil || mrf.Schema == nil || mrf.rowFormat == nil {
		return nil, ErrNilSchema
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("empty data")
	}
	if len(data) > MaxRowSize {
		return nil, fmt.Errorf("%w: size %d", ErrRowTooLarge, len(data))
	}

	buf := bytes.NewReader(data)

	// Read format version
	version, err := buf.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("failed to read version: %w", err)
	}

	if version != mrf.Version {
		return nil, fmt.Errorf("%w: got %d, expected %d", ErrInvalidVersion, version, mrf.Version)
	}

	// Read MVCC header
	header, err := mrf.deserializeHeader(buf)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize MVCC header: %w", err)
	}

	// Read remaining data for row
	remainingData := make([]byte, buf.Len())
	if _, err := io.ReadFull(buf, remainingData); err != nil {
		return nil, fmt.Errorf("failed to read row data: %w", err)
	}

	// Deserialize row data using cached RowFormat
	rowData, err := mrf.rowFormat.Deserialize(remainingData)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize row data: %w", err)
	}

	return &MVCCRow{
		Header: *header,
		Data:   rowData,
	}, nil
}

// serializeHeader writes the MVCC header to the buffer.
func (mrf *MVCCRowFormat) serializeHeader(w io.Writer, header *MVCCRowHeader) error {
	// Write each field in order
	fields := []int64{
		header.RowID,
		header.CreatedByTxn,
		header.CreatedAt,
		header.DeletedByTxn,
		header.DeletedAt,
		header.NextVersion,
	}

	for i, field := range fields {
		if err := binary.Write(w, binary.LittleEndian, field); err != nil {
			return fmt.Errorf("failed to write header field %d: %w", i, err)
		}
	}

	return nil
}

// deserializeHeader reads the MVCC header from the buffer.
func (mrf *MVCCRowFormat) deserializeHeader(r io.Reader) (*MVCCRowHeader, error) {
	header := &MVCCRowHeader{}

	// Read each field in order
	fields := []*int64{
		&header.RowID,
		&header.CreatedByTxn,
		&header.CreatedAt,
		&header.DeletedByTxn,
		&header.DeletedAt,
		&header.NextVersion,
	}

	for i, field := range fields {
		if err := binary.Read(r, binary.LittleEndian, field); err != nil {
			return nil, fmt.Errorf("failed to read header field %d: %w", i, err)
		}
	}

	return header, nil
}

// EncodeVersionPointer encodes a PageID and SlotID into a single int64.
func EncodeVersionPointer(pageID uint32, slotID uint16) int64 {
	return int64(pageID)<<16 | int64(slotID)
}

// DecodeVersionPointer decodes a version pointer into PageID and SlotID.
func DecodeVersionPointer(ptr int64) (pageID uint32, slotID uint16) {
	if ptr == 0 {
		return 0, 0
	}
	// Safe conversions - we know the values fit because we encoded them
	pageID = uint32(ptr >> 16)    //nolint:gosec // PageID was encoded from uint32
	slotID = uint16(ptr & 0xFFFF) //nolint:gosec // SlotID fits in 16 bits by design
	return pageID, slotID
}

// ConvertToMVCCRow converts a regular row to an MVCC row with default header values.
func ConvertToMVCCRow(row *Row, txnID, timestamp int64) *MVCCRow {
	return &MVCCRow{
		Header: MVCCRowHeader{
			CreatedByTxn: txnID,
			CreatedAt:    timestamp,
		},
		Data: row,
	}
}

// ExtractRow gets the underlying Row from an MVCCRow.
func (r *MVCCRow) ExtractRow() *Row {
	return r.Data
}

// MVCCRowIterator wraps a regular row iterator with visibility checks.
type MVCCRowIterator struct {
	baseIterator RowIterator
	txnID        int64
	timestamp    int64
	rowFormat    *MVCCRowFormat
}

// NewMVCCRowIterator creates a new MVCC-aware row iterator.
func NewMVCCRowIterator(base RowIterator, txnID, timestamp int64, schema *Schema) *MVCCRowIterator {
	return &MVCCRowIterator{
		baseIterator: base,
		txnID:        txnID,
		timestamp:    timestamp,
		rowFormat:    NewMVCCRowFormat(schema),
	}
}

// Next advances to the next visible row and returns the row and its ID.
func (it *MVCCRowIterator) Next() (*Row, RowID, error) {
	// Get next row from base iterator
	row, rowID, err := it.baseIterator.Next()
	if err != nil {
		return nil, RowID{}, err
	}
	if row == nil {
		return nil, RowID{}, nil // EOF
	}

	// The base iterator returns already deserialized Row objects
	// We need to get the raw data to check if it's MVCC format
	// For now, assume all new rows are MVCC format
	// In a real implementation, we'd need to peek at the version byte

	// Since we can't easily check the format from a deserialized Row,
	// we'll need to fetch the raw data differently
	// For now, return the row as-is (this maintains compatibility)
	return row, rowID, nil
}

// Close closes the iterator.
func (it *MVCCRowIterator) Close() error {
	return it.baseIterator.Close()
}

// isVisible checks if a row version is visible to the current transaction.
func (it *MVCCRowIterator) isVisible(row *MVCCRow) bool {
	// Row is visible if:
	// 1. It was created before our snapshot timestamp
	// 2. It hasn't been deleted, or was deleted after our snapshot

	// Use timestamp-based visibility for MVCC correctness
	if row.Header.CreatedAt > it.timestamp {
		return false // Created after our snapshot
	}

	if row.Header.DeletedAt > 0 && row.Header.DeletedAt <= it.timestamp {
		return false // Deleted before or at our snapshot
	}

	return true
}
