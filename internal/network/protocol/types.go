package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Protocol version
const (
	ProtocolVersion = 196608 // 3.0
)

// Message type identifiers
const (
	// Frontend (client to server)
	MsgStartup         = 0 // Special case, no type byte
	MsgQuery           = 'Q'
	MsgParse           = 'P'
	MsgBind            = 'B'
	MsgExecute         = 'E'
	MsgDescribe        = 'D'
	MsgClose           = 'C'
	MsgSync            = 'S'
	MsgFlush           = 'H'
	MsgTerminate       = 'X'
	MsgPasswordMessage = 'p'

	// Backend (server to client)
	MsgAuthentication     = 'R'
	MsgBackendKeyData     = 'K'
	MsgBindComplete       = '2'
	MsgCloseComplete      = '3'
	MsgCommandComplete    = 'C'
	MsgDataRow            = 'D'
	MsgEmptyQueryResponse = 'I'
	MsgErrorResponse      = 'E'
	MsgNoticeResponse     = 'N'
	MsgParameterStatus    = 'S'
	MsgParseComplete      = '1'
	MsgPortalSuspended    = 's'
	MsgReadyForQuery      = 'Z'
	MsgRowDescription     = 'T'
)

// Authentication types
const (
	AuthOK                = 0
	AuthCleartextPassword = 3
	AuthMD5Password       = 5
	AuthSASL              = 10
	AuthSASLContinue      = 11
	AuthSASLFinal         = 12
)

// Transaction status
const (
	TxnStatusIdle        = 'I'
	TxnStatusInBlock     = 'T'
	TxnStatusFailed      = 'E'
)

// Error severity levels
const (
	SeverityError   = "ERROR"
	SeverityFatal   = "FATAL"
	SeverityPanic   = "PANIC"
	SeverityWarning = "WARNING"
	SeverityNotice  = "NOTICE"
	SeverityDebug   = "DEBUG"
	SeverityInfo    = "INFO"
	SeverityLog     = "LOG"
)

// Error field codes
const (
	ErrorFieldSeverity         = 'S'
	ErrorFieldSeverityV        = 'V'
	ErrorFieldCode             = 'C'
	ErrorFieldMessage          = 'M'
	ErrorFieldDetail           = 'D'
	ErrorFieldHint             = 'H'
	ErrorFieldPosition         = 'P'
	ErrorFieldInternalPosition = 'p'
	ErrorFieldInternalQuery    = 'q'
	ErrorFieldWhere            = 'W'
	ErrorFieldSchema           = 's'
	ErrorFieldTable            = 't'
	ErrorFieldColumn           = 'c'
	ErrorFieldDataType         = 'd'
	ErrorFieldConstraint       = 'n'
	ErrorFieldFile             = 'F'
	ErrorFieldLine             = 'L'
	ErrorFieldRoutine          = 'R'
)

// Format codes
const (
	FormatText   = 0
	FormatBinary = 1
)

// Message represents a PostgreSQL protocol message
type Message struct {
	Type byte
	Data []byte
}

// ReadMessage reads a message from the reader
func ReadMessage(r io.Reader) (*Message, error) {
	// Read message type (1 byte)
	typeBuf := make([]byte, 1)
	if _, err := io.ReadFull(r, typeBuf); err != nil {
		return nil, err
	}

	// Read message length (4 bytes, includes itself but not type)
	lengthBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, lengthBuf); err != nil {
		return nil, err
	}
	length := int(binary.BigEndian.Uint32(lengthBuf)) - 4

	if length < 0 {
		return nil, fmt.Errorf("invalid message length: %d", length)
	}

	// Read message data
	data := make([]byte, length)
	if length > 0 {
		if _, err := io.ReadFull(r, data); err != nil {
			return nil, err
		}
	}

	return &Message{
		Type: typeBuf[0],
		Data: data,
	}, nil
}

// WriteMessage writes a message to the writer
func WriteMessage(w io.Writer, msg *Message) error {
	// Write type
	if err := binary.Write(w, binary.BigEndian, msg.Type); err != nil {
		return err
	}

	// Write length (includes itself)
	length := uint32(len(msg.Data) + 4)
	if err := binary.Write(w, binary.BigEndian, length); err != nil {
		return err
	}

	// Write data
	if len(msg.Data) > 0 {
		if _, err := w.Write(msg.Data); err != nil {
			return err
		}
	}

	return nil
}

// ReadStartupMessage reads the special startup message
func ReadStartupMessage(r io.Reader) (map[string]string, error) {
	// Read length (4 bytes, includes itself)
	lengthBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, lengthBuf); err != nil {
		return nil, err
	}
	length := int(binary.BigEndian.Uint32(lengthBuf)) - 4

	// Read message data
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}

	// Check protocol version
	version := binary.BigEndian.Uint32(data[:4])
	if version != ProtocolVersion {
		return nil, fmt.Errorf("unsupported protocol version: %d", version)
	}

	// Parse parameters
	params := make(map[string]string)
	data = data[4:] // Skip version

	for len(data) > 0 {
		// Find null terminator
		nullIdx := -1
		for i, b := range data {
			if b == 0 {
				nullIdx = i
				break
			}
		}

		if nullIdx == -1 {
			break
		}

		// Empty string marks end
		if nullIdx == 0 {
			break
		}

		key := string(data[:nullIdx])
		data = data[nullIdx+1:]

		// Find value null terminator
		nullIdx = -1
		for i, b := range data {
			if b == 0 {
				nullIdx = i
				break
			}
		}

		if nullIdx == -1 {
			return nil, fmt.Errorf("unterminated parameter value")
		}

		value := string(data[:nullIdx])
		data = data[nullIdx+1:]

		params[key] = value
	}

	return params, nil
}

// ErrorResponse represents an error response message
type ErrorResponse struct {
	Severity string
	Code     string
	Message  string
	Detail   string
	Hint     string
	Position string
	Where    string
	Schema   string
	Table    string
	Column   string
	File     string
	Line     string
	Routine  string
}

// ToMessage converts ErrorResponse to a Message
func (e *ErrorResponse) ToMessage() *Message {
	buf := make([]byte, 0, 256)

	// Add severity
	buf = append(buf, ErrorFieldSeverity)
	buf = append(buf, []byte(e.Severity)...)
	buf = append(buf, 0)

	// Add code
	if e.Code != "" {
		buf = append(buf, ErrorFieldCode)
		buf = append(buf, []byte(e.Code)...)
		buf = append(buf, 0)
	}

	// Add message
	buf = append(buf, ErrorFieldMessage)
	buf = append(buf, []byte(e.Message)...)
	buf = append(buf, 0)

	// Add optional fields
	if e.Detail != "" {
		buf = append(buf, ErrorFieldDetail)
		buf = append(buf, []byte(e.Detail)...)
		buf = append(buf, 0)
	}

	if e.Hint != "" {
		buf = append(buf, ErrorFieldHint)
		buf = append(buf, []byte(e.Hint)...)
		buf = append(buf, 0)
	}

	// Terminate with null
	buf = append(buf, 0)

	return &Message{
		Type: MsgErrorResponse,
		Data: buf,
	}
}