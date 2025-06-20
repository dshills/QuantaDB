package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/dshills/QuantaDB/internal/util/timeutil"
)

const (
	// RecordHeaderSize is the fixed size of a log record header
	// LSN(8) + Type(2) + TxnID(8) + PrevLSN(8) + Timestamp(8) + Length(4) = 38 bytes
	RecordHeaderSize = 38

	// ChecksumSize is the size of the checksum field
	ChecksumSize = 4
)

// SerializeRecord writes a log record to the writer
func SerializeRecord(w io.Writer, record *LogRecord) error {
	// Calculate total size
	dataLen := len(record.Data)
	totalSize := RecordHeaderSize + dataLen + ChecksumSize

	// Create buffer for entire record
	buf := make([]byte, totalSize)
	// Write header
	pos := 0
	binary.BigEndian.PutUint64(buf[pos:pos+8], uint64(record.LSN))
	pos += 8
	binary.BigEndian.PutUint16(buf[pos:pos+2], uint16(record.Type))
	pos += 2
	binary.BigEndian.PutUint64(buf[pos:pos+8], record.TxnID)
	pos += 8
	binary.BigEndian.PutUint64(buf[pos:pos+8], uint64(record.PrevLSN))
	pos += 8
	timeutil.WriteTimestampToBuf(buf, pos, record.Timestamp) //nolint:errcheck // Buffer size is pre-calculated
	pos += 8
	binary.BigEndian.PutUint32(buf[pos:pos+4], uint32(dataLen))
	pos += 4

	// Write data
	if dataLen > 0 {
		copy(buf[pos:pos+dataLen], record.Data)
		pos += dataLen
	}

	// Calculate and write checksum (excluding checksum field itself)
	checksum := crc32.ChecksumIEEE(buf[:pos])
	binary.BigEndian.PutUint32(buf[pos:pos+4], checksum)

	// Write to output
	_, err := w.Write(buf)
	return err
}

// DeserializeRecord reads a log record from the reader
func DeserializeRecord(r io.Reader) (*LogRecord, error) {
	// Read header
	headerBuf := make([]byte, RecordHeaderSize)
	if _, err := io.ReadFull(r, headerBuf); err != nil {
		return nil, fmt.Errorf("failed to read record header: %w", err)
	}

	// Parse header
	pos := 0
	lsn := LSN(binary.BigEndian.Uint64(headerBuf[pos : pos+8]))
	pos += 8
	recordType := RecordType(binary.BigEndian.Uint16(headerBuf[pos : pos+2]))
	pos += 2
	txnID := binary.BigEndian.Uint64(headerBuf[pos : pos+8])
	pos += 8
	prevLSN := LSN(binary.BigEndian.Uint64(headerBuf[pos : pos+8]))
	pos += 8
	timestamp, _ := timeutil.ReadTimestampFromBuf(headerBuf, pos) // Error impossible with pre-validated buffer
	pos += 8
	dataLen := binary.BigEndian.Uint32(headerBuf[pos : pos+4])

	// Read data
	var data []byte
	if dataLen > 0 {
		data = make([]byte, dataLen)
		if _, err := io.ReadFull(r, data); err != nil {
			return nil, fmt.Errorf("failed to read record data: %w", err)
		}
	}

	// Read checksum
	checksumBuf := make([]byte, ChecksumSize)
	if _, err := io.ReadFull(r, checksumBuf); err != nil {
		return nil, fmt.Errorf("failed to read checksum: %w", err)
	}
	expectedChecksum := binary.BigEndian.Uint32(checksumBuf)

	// Verify checksum
	fullBuf := make([]byte, RecordHeaderSize+len(data))
	copy(fullBuf[:RecordHeaderSize], headerBuf)
	if len(data) > 0 {
		copy(fullBuf[RecordHeaderSize:], data)
	}
	actualChecksum := crc32.ChecksumIEEE(fullBuf)

	if actualChecksum != expectedChecksum {
		return nil, fmt.Errorf("checksum mismatch: expected %x, got %x", expectedChecksum, actualChecksum)
	}

	return &LogRecord{
		LSN:       lsn,
		Type:      recordType,
		TxnID:     txnID,
		PrevLSN:   prevLSN,
		Timestamp: timestamp,
		Data:      data,
	}, nil
}

// NewBeginTxnRecord creates a new BEGIN_TXN log record
func NewBeginTxnRecord(lsn LSN, txnID uint64) *LogRecord {
	txnRec := &TransactionRecord{
		TxnID:     txnID,
		Timestamp: timeutil.Now(),
	}

	return &LogRecord{
		LSN:       lsn,
		Type:      RecordTypeBeginTxn,
		TxnID:     txnID,
		PrevLSN:   InvalidLSN,
		Timestamp: timeutil.Now(),
		Data:      txnRec.Marshal(),
	}
}

// NewCommitTxnRecord creates a new COMMIT_TXN log record
func NewCommitTxnRecord(lsn LSN, txnID uint64, prevLSN LSN) *LogRecord {
	txnRec := &TransactionRecord{
		TxnID:     txnID,
		Timestamp: timeutil.Now(),
	}

	return &LogRecord{
		LSN:       lsn,
		Type:      RecordTypeCommitTxn,
		TxnID:     txnID,
		PrevLSN:   prevLSN,
		Timestamp: timeutil.Now(),
		Data:      txnRec.Marshal(),
	}
}

// NewAbortTxnRecord creates a new ABORT_TXN log record
func NewAbortTxnRecord(lsn LSN, txnID uint64, prevLSN LSN) *LogRecord {
	txnRec := &TransactionRecord{
		TxnID:     txnID,
		Timestamp: timeutil.Now(),
	}

	return &LogRecord{
		LSN:       lsn,
		Type:      RecordTypeAbortTxn,
		TxnID:     txnID,
		PrevLSN:   prevLSN,
		Timestamp: timeutil.Now(),
		Data:      txnRec.Marshal(),
	}
}

// NewInsertRecord creates a new INSERT log record
func NewInsertRecord(lsn LSN, txnID uint64, prevLSN LSN, tableID int64, pageID uint32, slotID uint16, rowData []byte) *LogRecord {
	insertRec := &InsertRecord{
		TableID: tableID,
		PageID:  pageID,
		SlotID:  slotID,
		RowData: rowData,
	}

	return &LogRecord{
		LSN:       lsn,
		Type:      RecordTypeInsert,
		TxnID:     txnID,
		PrevLSN:   prevLSN,
		Timestamp: timeutil.Now(),
		Data:      insertRec.Marshal(),
	}
}

// NewDeleteRecord creates a new DELETE log record
func NewDeleteRecord(lsn LSN, txnID uint64, prevLSN LSN, tableID int64, pageID uint32, slotID uint16) *LogRecord {
	deleteRec := &DeleteRecord{
		TableID: tableID,
		PageID:  pageID,
		SlotID:  slotID,
	}

	return &LogRecord{
		LSN:       lsn,
		Type:      RecordTypeDelete,
		TxnID:     txnID,
		PrevLSN:   prevLSN,
		Timestamp: timeutil.Now(),
		Data:      deleteRec.Marshal(),
	}
}

// NewUpdateRecord creates a new UPDATE log record
func NewUpdateRecord(lsn LSN, txnID uint64, prevLSN LSN, tableID int64, pageID uint32, slotID uint16, oldData, newData []byte) *LogRecord {
	updateRec := &UpdateRecord{
		TableID:    tableID,
		PageID:     pageID,
		SlotID:     slotID,
		OldRowData: oldData,
		NewRowData: newData,
	}

	return &LogRecord{
		LSN:       lsn,
		Type:      RecordTypeUpdate,
		TxnID:     txnID,
		PrevLSN:   prevLSN,
		Timestamp: timeutil.Now(),
		Data:      updateRec.Marshal(),
	}
}

// NewCheckpointRecord creates a new CHECKPOINT log record
func NewCheckpointRecord(lsn LSN, lastLSN LSN, activeTxns []uint64) *LogRecord {
	checkpointRec := &CheckpointRecord{
		Timestamp:  timeutil.Now(),
		LastLSN:    lastLSN,
		ActiveTxns: activeTxns,
	}

	return &LogRecord{
		LSN:       lsn,
		Type:      RecordTypeCheckpoint,
		TxnID:     0, // Checkpoint is not associated with a transaction
		PrevLSN:   InvalidLSN,
		Timestamp: timeutil.Now(),
		Data:      checkpointRec.Marshal(),
	}
}
