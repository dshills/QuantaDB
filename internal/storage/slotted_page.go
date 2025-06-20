package storage

import (
	"encoding/binary"
	"fmt"
)

// SlottedPage implements a slotted page layout for variable-length records
// Layout:
// [PageHeader][Slot1][Slot2]...[SlotN][FreeSpace][RecordN]...[Record2][Record1]
// Records grow from the end backward, slots grow from the beginning forward
type SlottedPage struct {
	*Page
}

// Slot represents a record slot in the page
type Slot struct {
	Offset uint16 // Offset from start of page
	Length uint16 // Length of the record (0 means deleted)
}

const SlotSize = 4 // Size of a slot entry in bytes

// NewSlottedPage creates a new slotted page
func NewSlottedPage(id PageID) *SlottedPage {
	return &SlottedPage{
		Page: NewPage(id, PageTypeData),
	}
}

// AddRecord adds a record to the page and returns its slot number
func (sp *SlottedPage) AddRecord(data []byte) (uint16, error) {
	if len(data) > 65535 {
		return 0, fmt.Errorf("record too large: %d bytes (max 65535)", len(data))
	}
	recordLen := uint16(len(data)) //nolint:gosec // Bounds checked above
	requiredSpace := recordLen + SlotSize

	// Check if we have enough free space
	if sp.Header.FreeSpace < requiredSpace {
		return 0, fmt.Errorf("insufficient space in page: need %d bytes, have %d",
			requiredSpace, sp.Header.FreeSpace)
	}

	// Calculate positions
	slotNum := sp.Header.ItemCount
	slotEnd := PageHeaderSize + (slotNum+1)*SlotSize
	recordOffset := sp.Header.FreeSpacePtr - recordLen

	// Check if slot and record areas would overlap
	if slotEnd > recordOffset {
		return 0, fmt.Errorf("page is full: slot and record areas would overlap (slot end: %d, record start: %d)",
			slotEnd, recordOffset)
	}

	// Add slot entry
	slotOffset := PageHeaderSize + slotNum*SlotSize

	// Write slot
	binary.LittleEndian.PutUint16(sp.Data[slotOffset-PageHeaderSize:], recordOffset)
	binary.LittleEndian.PutUint16(sp.Data[slotOffset-PageHeaderSize+2:], recordLen)

	// Write record data
	copy(sp.Data[recordOffset-PageHeaderSize:], data)

	// Update header
	sp.Header.ItemCount++
	sp.Header.FreeSpace -= requiredSpace
	sp.Header.FreeSpacePtr = recordOffset

	return slotNum, nil
}

// GetRecord retrieves a record by slot number
func (sp *SlottedPage) GetRecord(slotNum uint16) ([]byte, error) {
	if slotNum >= sp.Header.ItemCount {
		return nil, fmt.Errorf("invalid slot number: %d (max: %d)", slotNum, sp.Header.ItemCount-1)
	}

	// Read slot
	slotOffset := PageHeaderSize + slotNum*SlotSize
	offset := binary.LittleEndian.Uint16(sp.Data[slotOffset-PageHeaderSize:])
	length := binary.LittleEndian.Uint16(sp.Data[slotOffset-PageHeaderSize+2:])

	if length == 0 {
		return nil, fmt.Errorf("record at slot %d has been deleted", slotNum)
	}

	// Read record data
	data := make([]byte, length)
	copy(data, sp.Data[offset-PageHeaderSize:offset-PageHeaderSize+length])

	return data, nil
}

// UpdateRecord updates a record in place if it fits, otherwise returns error
func (sp *SlottedPage) UpdateRecord(slotNum uint16, data []byte) error {
	if slotNum >= sp.Header.ItemCount {
		return fmt.Errorf("invalid slot number: %d", slotNum)
	}

	// Read current slot
	slotOffset := PageHeaderSize + slotNum*SlotSize
	offset := binary.LittleEndian.Uint16(sp.Data[slotOffset-PageHeaderSize:])
	oldLength := binary.LittleEndian.Uint16(sp.Data[slotOffset-PageHeaderSize+2:])
	if len(data) > 65535 {
		return fmt.Errorf("record too large for update: %d bytes (max 65535)", len(data))
	}
	newLength := uint16(len(data)) //nolint:gosec // Bounds checked above

	if oldLength == 0 {
		return fmt.Errorf("cannot update deleted record at slot %d", slotNum)
	}

	// If new record is same size or smaller, update in place
	if newLength <= oldLength {
		copy(sp.Data[offset-PageHeaderSize:], data)
		if newLength < oldLength {
			// Update slot with new length
			binary.LittleEndian.PutUint16(sp.Data[slotOffset-PageHeaderSize+2:], newLength)
			// Free space increases by the difference
			sp.Header.FreeSpace += oldLength - newLength
		}
		return nil
	}

	// For larger records, would need to implement record relocation
	return fmt.Errorf("record too large for in-place update: old=%d, new=%d", oldLength, newLength)
}

// DeleteRecord marks a record as deleted by setting its length to 0
func (sp *SlottedPage) DeleteRecord(slotNum uint16) error {
	if slotNum >= sp.Header.ItemCount {
		return fmt.Errorf("invalid slot number: %d", slotNum)
	}

	// Read slot
	slotOffset := PageHeaderSize + slotNum*SlotSize
	_ = binary.LittleEndian.Uint16(sp.Data[slotOffset-PageHeaderSize:]) // offset not needed for delete
	length := binary.LittleEndian.Uint16(sp.Data[slotOffset-PageHeaderSize+2:])

	if length == 0 {
		return nil // Already deleted
	}

	// Mark as deleted by setting length to 0
	binary.LittleEndian.PutUint16(sp.Data[slotOffset-PageHeaderSize+2:], 0)

	// Update free space
	sp.Header.FreeSpace += length

	// TODO: Implement compaction to reclaim space

	return nil
}

// GetSlots returns all slot information for debugging
func (sp *SlottedPage) GetSlots() []Slot {
	slots := make([]Slot, sp.Header.ItemCount)
	for i := uint16(0); i < sp.Header.ItemCount; i++ {
		slotOffset := PageHeaderSize + i*SlotSize
		slots[i].Offset = binary.LittleEndian.Uint16(sp.Data[slotOffset-PageHeaderSize:])
		slots[i].Length = binary.LittleEndian.Uint16(sp.Data[slotOffset-PageHeaderSize+2:])
	}
	return slots
}

// Compact reorganizes the page to reclaim space from deleted records
func (sp *SlottedPage) Compact() {
	// TODO: Implement page compaction
	// This would involve:
	// 1. Collecting all live records
	// 2. Rewriting them contiguously from the end
	// 3. Updating all slot offsets
	// 4. Updating free space pointers
}
