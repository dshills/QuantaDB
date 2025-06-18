package protocol

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

// Query represents a simple query message
type Query struct {
	Query string
}

// Parse parses a Query message from data
func (q *Query) Parse(data []byte) error {
	// Query string is null-terminated
	nullIdx := bytes.IndexByte(data, 0)
	if nullIdx == -1 {
		return fmt.Errorf("query string not null-terminated")
	}
	q.Query = string(data[:nullIdx])
	return nil
}

// RowDescription represents row metadata
type RowDescription struct {
	Fields []FieldDescription
}

// FieldDescription describes a single field
type FieldDescription struct {
	Name         string
	TableOID     uint32
	ColumnNumber int16
	DataTypeOID  uint32
	DataTypeSize int16
	TypeModifier int32
	Format       int16
}

// ToMessage converts RowDescription to a Message
func (r *RowDescription) ToMessage() *Message {
	buf := new(bytes.Buffer)

	// Field count
	binary.Write(buf, binary.BigEndian, int16(len(r.Fields)))

	// Field descriptions
	for _, field := range r.Fields {
		// Field name (null-terminated)
		buf.WriteString(field.Name)
		buf.WriteByte(0)

		// Table OID (0 for no table)
		binary.Write(buf, binary.BigEndian, field.TableOID)

		// Column number (0 for no column)
		binary.Write(buf, binary.BigEndian, field.ColumnNumber)

		// Data type OID
		binary.Write(buf, binary.BigEndian, field.DataTypeOID)

		// Data type size
		binary.Write(buf, binary.BigEndian, field.DataTypeSize)

		// Type modifier
		binary.Write(buf, binary.BigEndian, field.TypeModifier)

		// Format code (0 = text, 1 = binary)
		binary.Write(buf, binary.BigEndian, field.Format)
	}

	return &Message{
		Type: MsgRowDescription,
		Data: buf.Bytes(),
	}
}

// DataRow represents a single row of data
type DataRow struct {
	Values [][]byte
}

// ToMessage converts DataRow to a Message
func (d *DataRow) ToMessage() *Message {
	buf := new(bytes.Buffer)

	// Column count
	binary.Write(buf, binary.BigEndian, int16(len(d.Values)))

	// Column values
	for _, value := range d.Values {
		if value == nil {
			// NULL value
			binary.Write(buf, binary.BigEndian, int32(-1))
		} else {
			// Value length and data
			binary.Write(buf, binary.BigEndian, int32(len(value)))
			buf.Write(value)
		}
	}

	return &Message{
		Type: MsgDataRow,
		Data: buf.Bytes(),
	}
}

// CommandComplete represents command completion
type CommandComplete struct {
	Tag string
}

// ToMessage converts CommandComplete to a Message
func (c *CommandComplete) ToMessage() *Message {
	buf := new(bytes.Buffer)
	buf.WriteString(c.Tag)
	buf.WriteByte(0)

	return &Message{
		Type: MsgCommandComplete,
		Data: buf.Bytes(),
	}
}

// ReadyForQuery represents server ready status
type ReadyForQuery struct {
	Status byte // 'I' = idle, 'T' = in transaction, 'E' = failed transaction
}

// ToMessage converts ReadyForQuery to a Message
func (r *ReadyForQuery) ToMessage() *Message {
	return &Message{
		Type: MsgReadyForQuery,
		Data: []byte{r.Status},
	}
}

// ParameterStatus represents a server parameter
type ParameterStatus struct {
	Name  string
	Value string
}

// ToMessage converts ParameterStatus to a Message
func (p *ParameterStatus) ToMessage() *Message {
	buf := new(bytes.Buffer)
	buf.WriteString(p.Name)
	buf.WriteByte(0)
	buf.WriteString(p.Value)
	buf.WriteByte(0)

	return &Message{
		Type: MsgParameterStatus,
		Data: buf.Bytes(),
	}
}

// Authentication represents an authentication request
type Authentication struct {
	Type int32
	Data []byte
}

// ToMessage converts Authentication to a Message
func (a *Authentication) ToMessage() *Message {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, a.Type)
	if len(a.Data) > 0 {
		buf.Write(a.Data)
	}

	return &Message{
		Type: MsgAuthentication,
		Data: buf.Bytes(),
	}
}

// BackendKeyData represents backend process info
type BackendKeyData struct {
	ProcessID uint32
	SecretKey uint32
}

// ToMessage converts BackendKeyData to a Message
func (b *BackendKeyData) ToMessage() *Message {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, b.ProcessID)
	binary.Write(buf, binary.BigEndian, b.SecretKey)

	return &Message{
		Type: MsgBackendKeyData,
		Data: buf.Bytes(),
	}
}

// Parse represents a Parse message (prepare statement)
type Parse struct {
	Name          string
	Query         string
	ParameterOIDs []uint32
}

// ParseMessage parses a Parse message from data
func ParseMessage(data []byte) (*Parse, error) {
	buf := bytes.NewReader(data)
	p := &Parse{}

	// Read statement name
	name, err := readCString(buf)
	if err != nil {
		return nil, err
	}
	p.Name = name

	// Read query
	query, err := readCString(buf)
	if err != nil {
		return nil, err
	}
	p.Query = query

	// Read parameter count
	var paramCount int16
	if err := binary.Read(buf, binary.BigEndian, &paramCount); err != nil {
		return nil, err
	}

	// Read parameter OIDs
	p.ParameterOIDs = make([]uint32, paramCount)
	for i := 0; i < int(paramCount); i++ {
		if err := binary.Read(buf, binary.BigEndian, &p.ParameterOIDs[i]); err != nil {
			return nil, err
		}
	}

	return p, nil
}

// Bind represents a Bind message
type Bind struct {
	Portal          string
	Statement       string
	ParameterFormats []int16
	ParameterValues [][]byte
	ResultFormats   []int16
}

// ParseBind parses a Bind message from data
func ParseBind(data []byte) (*Bind, error) {
	buf := bytes.NewReader(data)
	b := &Bind{}

	// Read portal name
	portal, err := readCString(buf)
	if err != nil {
		return nil, err
	}
	b.Portal = portal

	// Read statement name
	stmt, err := readCString(buf)
	if err != nil {
		return nil, err
	}
	b.Statement = stmt

	// Read parameter format count
	var formatCount int16
	if err := binary.Read(buf, binary.BigEndian, &formatCount); err != nil {
		return nil, err
	}

	// Read parameter formats
	b.ParameterFormats = make([]int16, formatCount)
	for i := 0; i < int(formatCount); i++ {
		if err := binary.Read(buf, binary.BigEndian, &b.ParameterFormats[i]); err != nil {
			return nil, err
		}
	}

	// Read parameter value count
	var paramCount int16
	if err := binary.Read(buf, binary.BigEndian, &paramCount); err != nil {
		return nil, err
	}

	// Read parameter values
	b.ParameterValues = make([][]byte, paramCount)
	for i := 0; i < int(paramCount); i++ {
		var length int32
		if err := binary.Read(buf, binary.BigEndian, &length); err != nil {
			return nil, err
		}
		if length == -1 {
			// NULL value
			b.ParameterValues[i] = nil
		} else {
			// Read value bytes
			b.ParameterValues[i] = make([]byte, length)
			if _, err := io.ReadFull(buf, b.ParameterValues[i]); err != nil {
				return nil, err
			}
		}
	}

	// Read result format count
	var resultFormatCount int16
	if err := binary.Read(buf, binary.BigEndian, &resultFormatCount); err != nil {
		return nil, err
	}

	// Read result formats
	b.ResultFormats = make([]int16, resultFormatCount)
	for i := 0; i < int(resultFormatCount); i++ {
		if err := binary.Read(buf, binary.BigEndian, &b.ResultFormats[i]); err != nil {
			return nil, err
		}
	}

	return b, nil
}

// Execute represents an Execute message
type Execute struct {
	Portal  string
	MaxRows int32
}

// ParseExecute parses an Execute message from data
func ParseExecute(data []byte) (*Execute, error) {
	buf := bytes.NewReader(data)
	e := &Execute{}

	// Read portal name
	portal, err := readCString(buf)
	if err != nil {
		return nil, err
	}
	e.Portal = portal

	// Read max rows
	if err := binary.Read(buf, binary.BigEndian, &e.MaxRows); err != nil {
		return nil, err
	}

	return e, nil
}

// Helper function to read null-terminated string
func readCString(r io.Reader) (string, error) {
	var buf bytes.Buffer
	b := make([]byte, 1)

	for {
		if _, err := r.Read(b); err != nil {
			return "", err
		}
		if b[0] == 0 {
			break
		}
		buf.WriteByte(b[0])
	}

	return buf.String(), nil
}

// ParameterDescription describes prepared statement parameters
type ParameterDescription struct {
	TypeOIDs []uint32
}

// ToMessage converts ParameterDescription to a Message
func (p *ParameterDescription) ToMessage() *Message {
	buf := new(bytes.Buffer)
	
	// Parameter count
	binary.Write(buf, binary.BigEndian, int16(len(p.TypeOIDs)))
	
	// Parameter type OIDs
	for _, oid := range p.TypeOIDs {
		binary.Write(buf, binary.BigEndian, oid)
	}
	
	return &Message{
		Type: 't', // lowercase 't' for parameter description
		Data: buf.Bytes(),
	}
}

// NoData indicates no data will be returned
type NoData struct{}

// ToMessage converts NoData to a Message
func (n *NoData) ToMessage() *Message {
	return &Message{
		Type: 'n', // 'n' for no data
		Data: []byte{},
	}
}

// CloseComplete indicates successful close
type CloseComplete struct{}

// ToMessage converts CloseComplete to a Message
func (c *CloseComplete) ToMessage() *Message {
	return &Message{
		Type: MsgCloseComplete,
		Data: []byte{},
	}
}

// ParseComplete indicates successful parse
type ParseComplete struct{}

// ToMessage converts ParseComplete to a Message
func (p *ParseComplete) ToMessage() *Message {
	return &Message{
		Type: MsgParseComplete,
		Data: []byte{},
	}
}

// BindComplete indicates successful bind
type BindComplete struct{}

// ToMessage converts BindComplete to a Message  
func (b *BindComplete) ToMessage() *Message {
	return &Message{
		Type: MsgBindComplete,
		Data: []byte{},
	}
}
