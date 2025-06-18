package storage

import (
	"encoding/binary"
	"fmt"
)

const (
	// PageSize is the size of a database page in bytes
	PageSize = 8192

	// PageHeaderSize is the size of the page header
	PageHeaderSize = 24

	// MaxPayloadSize is the maximum size of data that can fit in a page
	MaxPayloadSize = PageSize - PageHeaderSize
)

// PageType represents the type of a database page
type PageType uint8

const (
	PageTypeData PageType = iota
	PageTypeIndex
	PageTypeFree
	PageTypeOverflow
)

// PageID is a unique identifier for a page
type PageID uint32

// InvalidPageID represents an invalid page ID
const InvalidPageID PageID = 0

// PageHeader contains metadata about a page
type PageHeader struct {
	PageID       PageID
	Type         PageType
	Flags        uint8
	LSN          uint64 // Log Sequence Number for recovery
	NextPageID   PageID // For linked pages (overflow, free list)
	FreeSpace    uint16 // Bytes of free space in the page
	ItemCount    uint16 // Number of items in the page
	FreeSpacePtr uint16 // Pointer to start of free space
}

// Page represents a database page
type Page struct {
	Header PageHeader
	Data   [MaxPayloadSize]byte
}

// NewPage creates a new page with the given ID and type
func NewPage(id PageID, pageType PageType) *Page {
	p := &Page{
		Header: PageHeader{
			PageID:       id,
			Type:         pageType,
			FreeSpace:    MaxPayloadSize,
			FreeSpacePtr: PageHeaderSize,
		},
	}
	return p
}

// Serialize writes the page to a byte slice
func (p *Page) Serialize() []byte {
	buf := make([]byte, PageSize)
	
	// Write header
	binary.LittleEndian.PutUint32(buf[0:4], uint32(p.Header.PageID))
	buf[4] = byte(p.Header.Type)
	buf[5] = p.Header.Flags
	binary.LittleEndian.PutUint64(buf[6:14], p.Header.LSN)
	binary.LittleEndian.PutUint32(buf[14:18], uint32(p.Header.NextPageID))
	binary.LittleEndian.PutUint16(buf[18:20], p.Header.FreeSpace)
	binary.LittleEndian.PutUint16(buf[20:22], p.Header.ItemCount)
	binary.LittleEndian.PutUint16(buf[22:24], p.Header.FreeSpacePtr)
	
	// Copy data
	copy(buf[PageHeaderSize:], p.Data[:])
	
	return buf
}

// Deserialize reads a page from a byte slice
func (p *Page) Deserialize(buf []byte) error {
	if len(buf) != PageSize {
		return fmt.Errorf("invalid page size: expected %d, got %d", PageSize, len(buf))
	}
	
	// Read header
	p.Header.PageID = PageID(binary.LittleEndian.Uint32(buf[0:4]))
	p.Header.Type = PageType(buf[4])
	p.Header.Flags = buf[5]
	p.Header.LSN = binary.LittleEndian.Uint64(buf[6:14])
	p.Header.NextPageID = PageID(binary.LittleEndian.Uint32(buf[14:18]))
	p.Header.FreeSpace = binary.LittleEndian.Uint16(buf[18:20])
	p.Header.ItemCount = binary.LittleEndian.Uint16(buf[20:22])
	p.Header.FreeSpacePtr = binary.LittleEndian.Uint16(buf[22:24])
	
	// Copy data
	copy(p.Data[:], buf[PageHeaderSize:])
	
	return nil
}

// GetFreeSpace returns the amount of free space in the page
func (p *Page) GetFreeSpace() uint16 {
	return p.Header.FreeSpace
}

// HasSpaceFor returns true if the page has enough space for the given size
func (p *Page) HasSpaceFor(size uint16) bool {
	// Need space for the data plus a slot entry (4 bytes)
	return p.Header.FreeSpace >= size+4
}