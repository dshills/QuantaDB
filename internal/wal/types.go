package wal

import (
	"encoding/binary"
	"fmt"
	"time"
)

// LSN represents a Log Sequence Number
type LSN uint64

// InvalidLSN represents an invalid or uninitialized LSN
const InvalidLSN LSN = 0

// RecordType represents the type of a WAL record
type RecordType uint16

const (
	RecordTypeInvalid RecordType = iota
	RecordTypeBeginTxn
	RecordTypeCommitTxn
	RecordTypeAbortTxn
	RecordTypeInsert
	RecordTypeDelete
	RecordTypeUpdate
	RecordTypeCreateTable
	RecordTypeCheckpoint
)

// String returns a string representation of the record type
func (rt RecordType) String() string {
	switch rt {
	case RecordTypeBeginTxn:
		return "BEGIN_TXN"
	case RecordTypeCommitTxn:
		return "COMMIT_TXN"
	case RecordTypeAbortTxn:
		return "ABORT_TXN"
	case RecordTypeInsert:
		return "INSERT"
	case RecordTypeDelete:
		return "DELETE"
	case RecordTypeUpdate:
		return "UPDATE"
	case RecordTypeCreateTable:
		return "CREATE_TABLE"
	case RecordTypeCheckpoint:
		return "CHECKPOINT"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", rt)
	}
}

// LogRecord represents a single WAL record
type LogRecord struct {
	LSN      LSN
	Type     RecordType
	TxnID    uint64
	PrevLSN  LSN       // Previous LSN for this transaction
	Timestamp time.Time
	Data     []byte
}

// Size returns the size of the log record in bytes
func (r *LogRecord) Size() int {
	// LSN(8) + Type(2) + TxnID(8) + PrevLSN(8) + Timestamp(8) + Length(4) + Data(len) + Checksum(4)
	return 8 + 2 + 8 + 8 + 8 + 4 + len(r.Data) + 4
}

// InsertRecord represents data for an INSERT operation
type InsertRecord struct {
	TableID int64
	PageID  uint32
	SlotID  uint16
	RowData []byte
}

// Marshal serializes the insert record
func (r *InsertRecord) Marshal() []byte {
	size := 8 + 4 + 2 + len(r.RowData)
	buf := make([]byte, size)
	
	binary.BigEndian.PutUint64(buf[0:8], uint64(r.TableID))
	binary.BigEndian.PutUint32(buf[8:12], r.PageID)
	binary.BigEndian.PutUint16(buf[12:14], r.SlotID)
	copy(buf[14:], r.RowData)
	
	return buf
}

// Unmarshal deserializes the insert record
func (r *InsertRecord) Unmarshal(data []byte) error {
	if len(data) < 14 {
		return fmt.Errorf("insert record too short: %d bytes", len(data))
	}
	
	r.TableID = int64(binary.BigEndian.Uint64(data[0:8]))
	r.PageID = binary.BigEndian.Uint32(data[8:12])
	r.SlotID = binary.BigEndian.Uint16(data[12:14])
	r.RowData = data[14:]
	
	return nil
}

// DeleteRecord represents data for a DELETE operation
type DeleteRecord struct {
	TableID int64
	PageID  uint32
	SlotID  uint16
}

// Marshal serializes the delete record
func (r *DeleteRecord) Marshal() []byte {
	buf := make([]byte, 14)
	
	binary.BigEndian.PutUint64(buf[0:8], uint64(r.TableID))
	binary.BigEndian.PutUint32(buf[8:12], r.PageID)
	binary.BigEndian.PutUint16(buf[12:14], r.SlotID)
	
	return buf
}

// Unmarshal deserializes the delete record
func (r *DeleteRecord) Unmarshal(data []byte) error {
	if len(data) < 14 {
		return fmt.Errorf("delete record too short: %d bytes", len(data))
	}
	
	r.TableID = int64(binary.BigEndian.Uint64(data[0:8]))
	r.PageID = binary.BigEndian.Uint32(data[8:12])
	r.SlotID = binary.BigEndian.Uint16(data[12:14])
	
	return nil
}

// UpdateRecord represents data for an UPDATE operation
type UpdateRecord struct {
	TableID    int64
	PageID     uint32
	SlotID     uint16
	OldRowData []byte
	NewRowData []byte
}

// Marshal serializes the update record
func (r *UpdateRecord) Marshal() []byte {
	oldLen := len(r.OldRowData)
	newLen := len(r.NewRowData)
	size := 8 + 4 + 2 + 4 + oldLen + 4 + newLen
	buf := make([]byte, size)
	
	pos := 0
	binary.BigEndian.PutUint64(buf[pos:pos+8], uint64(r.TableID))
	pos += 8
	binary.BigEndian.PutUint32(buf[pos:pos+4], r.PageID)
	pos += 4
	binary.BigEndian.PutUint16(buf[pos:pos+2], r.SlotID)
	pos += 2
	
	// Old row data with length prefix
	binary.BigEndian.PutUint32(buf[pos:pos+4], uint32(oldLen))
	pos += 4
	copy(buf[pos:pos+oldLen], r.OldRowData)
	pos += oldLen
	
	// New row data with length prefix
	binary.BigEndian.PutUint32(buf[pos:pos+4], uint32(newLen))
	pos += 4
	copy(buf[pos:pos+newLen], r.NewRowData)
	
	return buf
}

// Unmarshal deserializes the update record
func (r *UpdateRecord) Unmarshal(data []byte) error {
	if len(data) < 18 { // Minimum size without row data
		return fmt.Errorf("update record too short: %d bytes", len(data))
	}
	
	pos := 0
	r.TableID = int64(binary.BigEndian.Uint64(data[pos : pos+8]))
	pos += 8
	r.PageID = binary.BigEndian.Uint32(data[pos : pos+4])
	pos += 4
	r.SlotID = binary.BigEndian.Uint16(data[pos : pos+2])
	pos += 2
	
	// Old row data
	oldLen := binary.BigEndian.Uint32(data[pos : pos+4])
	pos += 4
	if pos+int(oldLen) > len(data) {
		return fmt.Errorf("old row data exceeds record size")
	}
	r.OldRowData = make([]byte, oldLen)
	copy(r.OldRowData, data[pos:pos+int(oldLen)])
	pos += int(oldLen)
	
	// New row data
	if pos+4 > len(data) {
		return fmt.Errorf("new row length missing")
	}
	newLen := binary.BigEndian.Uint32(data[pos : pos+4])
	pos += 4
	if pos+int(newLen) > len(data) {
		return fmt.Errorf("new row data exceeds record size")
	}
	r.NewRowData = make([]byte, newLen)
	copy(r.NewRowData, data[pos:pos+int(newLen)])
	
	return nil
}

// TransactionRecord represents data for transaction operations
type TransactionRecord struct {
	TxnID     uint64
	Timestamp time.Time
}

// Marshal serializes the transaction record
func (r *TransactionRecord) Marshal() []byte {
	buf := make([]byte, 16)
	
	binary.BigEndian.PutUint64(buf[0:8], r.TxnID)
	binary.BigEndian.PutUint64(buf[8:16], uint64(r.Timestamp.UnixNano()))
	
	return buf
}

// Unmarshal deserializes the transaction record
func (r *TransactionRecord) Unmarshal(data []byte) error {
	if len(data) < 16 {
		return fmt.Errorf("transaction record too short: %d bytes", len(data))
	}
	
	r.TxnID = binary.BigEndian.Uint64(data[0:8])
	r.Timestamp = time.Unix(0, int64(binary.BigEndian.Uint64(data[8:16])))
	
	return nil
}

// CheckpointRecord represents data for a checkpoint
type CheckpointRecord struct {
	Timestamp      time.Time
	LastLSN        LSN
	ActiveTxns     []uint64
	DirtyPages     []DirtyPageInfo // Pages that need to be flushed
}

// DirtyPageInfo tracks a dirty page during checkpoint
type DirtyPageInfo struct {
	PageID    uint32
	TableID   int64
	RecLSN    LSN // First LSN that made this page dirty
}

// Marshal serializes the checkpoint record
func (r *CheckpointRecord) Marshal() []byte {
	// Calculate size: timestamp(8) + lastLSN(8) + numTxns(4) + txns + numPages(4) + pages
	size := 8 + 8 + 4 + (8 * len(r.ActiveTxns)) + 4 + (20 * len(r.DirtyPages))
	buf := make([]byte, 0, size)
	
	// Timestamp
	buf = binary.BigEndian.AppendUint64(buf, uint64(r.Timestamp.UnixNano()))
	
	// Last LSN
	buf = binary.BigEndian.AppendUint64(buf, uint64(r.LastLSN))
	
	// Active transactions
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(r.ActiveTxns)))
	for _, txnID := range r.ActiveTxns {
		buf = binary.BigEndian.AppendUint64(buf, txnID)
	}
	
	// Dirty pages
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(r.DirtyPages)))
	for _, dp := range r.DirtyPages {
		buf = binary.BigEndian.AppendUint32(buf, dp.PageID)
		buf = binary.BigEndian.AppendUint64(buf, uint64(dp.TableID))
		buf = binary.BigEndian.AppendUint64(buf, uint64(dp.RecLSN))
	}
	
	return buf
}

// Unmarshal deserializes the checkpoint record
func (r *CheckpointRecord) Unmarshal(data []byte) error {
	if len(data) < 20 {
		return fmt.Errorf("checkpoint record too short: %d bytes", len(data))
	}
	
	pos := 0
	r.Timestamp = time.Unix(0, int64(binary.BigEndian.Uint64(data[pos:pos+8])))
	pos += 8
	r.LastLSN = LSN(binary.BigEndian.Uint64(data[pos : pos+8]))
	pos += 8
	
	// Active transactions
	numTxns := binary.BigEndian.Uint32(data[pos : pos+4])
	pos += 4
	
	if pos+int(numTxns)*8 > len(data) {
		return fmt.Errorf("active transaction list exceeds record size")
	}
	
	r.ActiveTxns = make([]uint64, numTxns)
	for i := uint32(0); i < numTxns; i++ {
		r.ActiveTxns[i] = binary.BigEndian.Uint64(data[pos : pos+8])
		pos += 8
	}
	
	// Dirty pages (if present in the record)
	if pos+4 <= len(data) {
		numPages := binary.BigEndian.Uint32(data[pos : pos+4])
		pos += 4
		
		if pos+int(numPages)*20 > len(data) {
			return fmt.Errorf("dirty page list exceeds record size")
		}
		
		r.DirtyPages = make([]DirtyPageInfo, numPages)
		for i := uint32(0); i < numPages; i++ {
			r.DirtyPages[i].PageID = binary.BigEndian.Uint32(data[pos : pos+4])
			pos += 4
			r.DirtyPages[i].TableID = int64(binary.BigEndian.Uint64(data[pos : pos+8]))
			pos += 8
			r.DirtyPages[i].RecLSN = LSN(binary.BigEndian.Uint64(data[pos : pos+8]))
			pos += 8
		}
	}
	
	return nil
}