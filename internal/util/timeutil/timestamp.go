package timeutil

import (
	"encoding/binary"
	"fmt"
	"time"
)

// Common timestamp constants
const (
	// TimestampSize is the size of a serialized timestamp in bytes
	TimestampSize = 8
)

// Common time format strings
const (
	RFC3339Format     = time.RFC3339
	RFC3339NanoFormat = time.RFC3339Nano
	DateFormat        = "2006-01-02"
	TimeFormat        = "15:04:05"
	DateTimeFormat    = "2006-01-02 15:04:05"
)

// Now returns the current time. This can be overridden for testing.
var Now = func() time.Time {
	return time.Now()
}

// EpochTime returns the Unix epoch time (1970-01-01 00:00:00 UTC)
func EpochTime() time.Time {
	return time.Unix(0, 0)
}

// EpochUTC returns the Unix epoch time in UTC
func EpochUTC() time.Time {
	return time.Unix(0, 0).UTC()
}

// TimeToUnixNano converts a time.Time to Unix nanoseconds
func TimeToUnixNano(t time.Time) int64 {
	return t.UnixNano()
}

// UnixNanoToTime converts Unix nanoseconds to time.Time
func UnixNanoToTime(nano int64) time.Time {
	return time.Unix(0, nano)
}

// TimeToBytes serializes a time.Time to a byte slice using big-endian encoding
func TimeToBytes(t time.Time) []byte {
	buf := make([]byte, TimestampSize)
	binary.BigEndian.PutUint64(buf, uint64(t.UnixNano()))
	return buf
}

// BytesToTime deserializes a byte slice to time.Time using big-endian encoding
func BytesToTime(data []byte) (time.Time, error) {
	if len(data) < TimestampSize {
		return time.Time{}, fmt.Errorf("insufficient data for timestamp: got %d bytes, need %d", len(data), TimestampSize)
	}
	nano := int64(binary.BigEndian.Uint64(data[:TimestampSize]))
	return time.Unix(0, nano), nil
}

// WriteTimestampToBuf writes a timestamp to an existing buffer at the given offset
func WriteTimestampToBuf(buf []byte, offset int, t time.Time) error {
	if len(buf) < offset+TimestampSize {
		return fmt.Errorf("buffer too small: need %d bytes, got %d", offset+TimestampSize, len(buf))
	}
	binary.BigEndian.PutUint64(buf[offset:], uint64(t.UnixNano()))
	return nil
}

// ReadTimestampFromBuf reads a timestamp from a buffer at the given offset
func ReadTimestampFromBuf(buf []byte, offset int) (time.Time, error) {
	if len(buf) < offset+TimestampSize {
		return time.Time{}, fmt.Errorf("buffer too small: need %d bytes, got %d", offset+TimestampSize, len(buf))
	}
	nano := int64(binary.BigEndian.Uint64(buf[offset:]))
	return time.Unix(0, nano), nil
}

// ParseTimestamp attempts to parse a timestamp string using common formats
func ParseTimestamp(s string) (time.Time, error) {
	formats := []string{
		RFC3339NanoFormat,
		RFC3339Format,
		DateTimeFormat,
		DateFormat,
		TimeFormat,
	}

	for _, format := range formats {
		if t, err := time.Parse(format, s); err == nil {
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf("unable to parse timestamp: %s", s)
}

// ParseDate attempts to parse a date string using common date formats
func ParseDate(s string) (time.Time, error) {
	formats := []string{
		DateFormat,
		RFC3339Format,
		DateTimeFormat,
	}

	for _, format := range formats {
		if t, err := time.Parse(format, s); err == nil {
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf("unable to parse date: %s", s)
}

// IsEpoch returns true if the time is the Unix epoch
func IsEpoch(t time.Time) bool {
	return t.Equal(EpochTime()) || t.IsZero()
}

// ToUTC converts a time to UTC timezone
func ToUTC(t time.Time) time.Time {
	return t.UTC()
}

// DurationToMillis converts a duration to milliseconds
func DurationToMillis(d time.Duration) int64 {
	return d.Nanoseconds() / 1e6
}

// MillisToDuration converts milliseconds to a duration
func MillisToDuration(millis int64) time.Duration {
	return time.Duration(millis) * time.Millisecond
}
