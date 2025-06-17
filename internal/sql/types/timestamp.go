package types

import (
	"encoding/binary"
	"fmt"
	"time"
)

func init() {
	Timestamp = &timestampType{}
	Date = &dateType{}
}

// timestampType implements the TIMESTAMP data type
type timestampType struct{}

func (t *timestampType) Name() string {
	return "TIMESTAMP"
}

func (t *timestampType) Size() int {
	return 8 // Store as int64 (Unix nano)
}

func (t *timestampType) Compare(a, b Value) int {
	if a.Null || b.Null {
		return CompareValues(a, b)
	}
	
	aTime := a.Data.(time.Time)
	bTime := b.Data.(time.Time)
	
	if aTime.Before(bTime) {
		return -1
	} else if aTime.After(bTime) {
		return 1
	}
	return 0
}

func (t *timestampType) Serialize(v Value) ([]byte, error) {
	if v.Null {
		return nil, nil
	}
	
	val, ok := v.Data.(time.Time)
	if !ok {
		return nil, fmt.Errorf("expected time.Time, got %T", v.Data)
	}
	
	// Store as Unix nanoseconds
	nano := val.UnixNano()
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(nano))
	return buf, nil
}

func (t *timestampType) Deserialize(data []byte) (Value, error) {
	if data == nil {
		return NewNullValue(), nil
	}
	
	if len(data) != 8 {
		return Value{}, fmt.Errorf("expected 8 bytes for TIMESTAMP, got %d", len(data))
	}
	
	nano := int64(binary.BigEndian.Uint64(data))
	val := time.Unix(0, nano)
	return NewValue(val), nil
}

func (t *timestampType) IsValid(v Value) bool {
	if v.Null {
		return true
	}
	
	_, ok := v.Data.(time.Time)
	return ok
}

func (t *timestampType) Zero() Value {
	return NewValue(time.Unix(0, 0))
}

// dateType implements the DATE data type
type dateType struct{}

func (t *dateType) Name() string {
	return "DATE"
}

func (t *dateType) Size() int {
	return 4 // Store as days since epoch
}

func (t *dateType) Compare(a, b Value) int {
	if a.Null || b.Null {
		return CompareValues(a, b)
	}
	
	aTime := a.Data.(time.Time)
	bTime := b.Data.(time.Time)
	
	// Compare only the date part
	aDate := aTime.Truncate(24 * time.Hour)
	bDate := bTime.Truncate(24 * time.Hour)
	
	if aDate.Before(bDate) {
		return -1
	} else if aDate.After(bDate) {
		return 1
	}
	return 0
}

func (t *dateType) Serialize(v Value) ([]byte, error) {
	if v.Null {
		return nil, nil
	}
	
	val, ok := v.Data.(time.Time)
	if !ok {
		return nil, fmt.Errorf("expected time.Time, got %T", v.Data)
	}
	
	// Store as days since Unix epoch (in UTC)
	epoch := time.Unix(0, 0).UTC()
	days := int32(val.UTC().Sub(epoch).Hours() / 24)
	
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(days))
	return buf, nil
}

func (t *dateType) Deserialize(data []byte) (Value, error) {
	if data == nil {
		return NewNullValue(), nil
	}
	
	if len(data) != 4 {
		return Value{}, fmt.Errorf("expected 4 bytes for DATE, got %d", len(data))
	}
	
	days := int32(binary.BigEndian.Uint32(data))
	// Use UTC epoch to avoid timezone issues
	epoch := time.Unix(0, 0).UTC()
	val := epoch.AddDate(0, 0, int(days))
	
	return NewValue(val), nil
}

func (t *dateType) IsValid(v Value) bool {
	if v.Null {
		return true
	}
	
	_, ok := v.Data.(time.Time)
	return ok
}

func (t *dateType) Zero() Value {
	return NewValue(time.Unix(0, 0).Truncate(24 * time.Hour))
}

// Helper functions for creating time values

// NewTimestampValue creates a new TIMESTAMP value
func NewTimestampValue(t time.Time) Value {
	return NewValue(t)
}

// NewDateValue creates a new DATE value
func NewDateValue(t time.Time) Value {
	// Truncate to date only
	return NewValue(t.Truncate(24 * time.Hour))
}

// ParseTimestamp parses a timestamp string
func ParseTimestamp(s string) (Value, error) {
	// Try common timestamp formats
	formats := []string{
		time.RFC3339,
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05.999999",
	}
	
	for _, format := range formats {
		if t, err := time.Parse(format, s); err == nil {
			return NewTimestampValue(t), nil
		}
	}
	
	return Value{}, fmt.Errorf("unable to parse timestamp: %s", s)
}

// ParseDate parses a date string
func ParseDate(s string) (Value, error) {
	// Try common date formats
	formats := []string{
		"2006-01-02",
		"01/02/2006",
		"01-02-2006",
	}
	
	for _, format := range formats {
		if t, err := time.Parse(format, s); err == nil {
			return NewDateValue(t), nil
		}
	}
	
	return Value{}, fmt.Errorf("unable to parse date: %s", s)
}