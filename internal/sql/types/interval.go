package types

import (
	"encoding/binary"
	"fmt"
	"time"
)

func init() {
	IntervalType = &intervalType{}
}

// intervalType implements the INTERVAL data type
type intervalType struct{}

func (t *intervalType) Name() string {
	return "INTERVAL"
}

func (t *intervalType) Size() int {
	return 16 // 4 bytes months + 4 bytes days + 8 bytes seconds
}

func (t *intervalType) Compare(a, b Value) int {
	if a.Null || b.Null {
		return CompareValues(a, b)
	}

	aVal := a.Data.(Interval)
	bVal := b.Data.(Interval)

	// Compare months first
	if aVal.Months < bVal.Months {
		return -1
	} else if aVal.Months > bVal.Months {
		return 1
	}

	// Then days
	if aVal.Days < bVal.Days {
		return -1
	} else if aVal.Days > bVal.Days {
		return 1
	}

	// Finally seconds
	if aVal.Seconds < bVal.Seconds {
		return -1
	} else if aVal.Seconds > bVal.Seconds {
		return 1
	}

	return 0
}

func (t *intervalType) Serialize(v Value) ([]byte, error) {
	if v.Null {
		return nil, nil
	}

	val, ok := v.Data.(Interval)
	if !ok {
		return nil, fmt.Errorf("expected Interval, got %T", v.Data)
	}

	buf := make([]byte, 16)
	binary.BigEndian.PutUint32(buf[0:4], uint32(val.Months))
	binary.BigEndian.PutUint32(buf[4:8], uint32(val.Days))
	binary.BigEndian.PutUint64(buf[8:16], uint64(val.Seconds))
	return buf, nil
}

func (t *intervalType) Deserialize(data []byte) (Value, error) {
	if data == nil {
		return NewNullValue(), nil
	}

	if len(data) != 16 {
		return Value{}, fmt.Errorf("expected 16 bytes for INTERVAL, got %d", len(data))
	}

	months := int32(binary.BigEndian.Uint32(data[0:4]))
	days := int32(binary.BigEndian.Uint32(data[4:8]))
	seconds := time.Duration(binary.BigEndian.Uint64(data[8:16]))

	return NewValue(Interval{
		Months:  months,
		Days:    days,
		Seconds: seconds,
	}), nil
}

func (t *intervalType) IsValid(v Value) bool {
	if v.Null {
		return true
	}
	_, ok := v.Data.(Interval)
	return ok
}

func (t *intervalType) Zero() Value {
	return NewValue(Interval{Months: 0, Days: 0, Seconds: 0})
}

// IntervalField represents the fields in an interval
type IntervalField int

const (
	IntervalYear IntervalField = iota
	IntervalMonth
	IntervalDay
	IntervalHour
	IntervalMinute
	IntervalSecond
)

// Interval represents a time interval
// PostgreSQL intervals have months and days as separate components because
// months have varying numbers of days
type Interval struct {
	Months  int32         // Total months (years are stored as months)
	Days    int32         // Total days
	Seconds time.Duration // Total seconds (including hours, minutes, seconds, microseconds)
}

// NewInterval creates a new interval
func NewInterval(months, days int32, seconds time.Duration) Interval {
	return Interval{
		Months:  months,
		Days:    days,
		Seconds: seconds,
	}
}

// NewIntervalFromField creates an interval from a single field value
func NewIntervalFromField(field IntervalField, value int64) Interval {
	switch field {
	case IntervalYear:
		return NewInterval(int32(value)*12, 0, 0)
	case IntervalMonth:
		return NewInterval(int32(value), 0, 0)
	case IntervalDay:
		return NewInterval(0, int32(value), 0)
	case IntervalHour:
		return NewInterval(0, 0, time.Duration(value)*time.Hour)
	case IntervalMinute:
		return NewInterval(0, 0, time.Duration(value)*time.Minute)
	case IntervalSecond:
		return NewInterval(0, 0, time.Duration(value)*time.Second)
	default:
		return NewInterval(0, 0, 0)
	}
}

// Add adds two intervals
func (i Interval) Add(other Interval) Interval {
	return Interval{
		Months:  i.Months + other.Months,
		Days:    i.Days + other.Days,
		Seconds: i.Seconds + other.Seconds,
	}
}

// Subtract subtracts an interval from another
func (i Interval) Subtract(other Interval) Interval {
	return Interval{
		Months:  i.Months - other.Months,
		Days:    i.Days - other.Days,
		Seconds: i.Seconds - other.Seconds,
	}
}

// Negate returns the negative of an interval
func (i Interval) Negate() Interval {
	return Interval{
		Months:  -i.Months,
		Days:    -i.Days,
		Seconds: -i.Seconds,
	}
}

// Multiply multiplies an interval by a scalar
func (i Interval) Multiply(factor float64) Interval {
	// Convert everything to the smallest unit for multiplication
	totalSeconds := float64(i.Months)*30*24*3600 + float64(i.Days)*24*3600 + i.Seconds.Seconds()
	totalSeconds *= factor

	// Convert back to interval components
	// This is a simplification - PostgreSQL has more complex rules
	months := int32(totalSeconds / (30 * 24 * 3600))
	totalSeconds -= float64(months) * 30 * 24 * 3600

	days := int32(totalSeconds / (24 * 3600))
	totalSeconds -= float64(days) * 24 * 3600

	return Interval{
		Months:  months,
		Days:    days,
		Seconds: time.Duration(totalSeconds * float64(time.Second)),
	}
}

// String returns a string representation of the interval
func (i Interval) String() string {
	if i.Months == 0 && i.Days == 0 && i.Seconds == 0 {
		return "00:00:00"
	}

	var parts []string

	if i.Months != 0 {
		years := i.Months / 12
		months := i.Months % 12
		if years != 0 && months != 0 {
			parts = append(parts, fmt.Sprintf("%d years %d months", years, months))
		} else if years != 0 {
			parts = append(parts, fmt.Sprintf("%d years", years))
		} else {
			parts = append(parts, fmt.Sprintf("%d months", months))
		}
	}

	if i.Days != 0 {
		parts = append(parts, fmt.Sprintf("%d days", i.Days))
	}

	if i.Seconds != 0 {
		hours := int(i.Seconds.Hours())
		minutes := int(i.Seconds.Minutes()) % 60
		seconds := int(i.Seconds.Seconds()) % 60
		if hours > 0 || minutes > 0 || seconds > 0 {
			parts = append(parts, fmt.Sprintf("%02d:%02d:%02d", hours, minutes, seconds))
		}
	}

	if len(parts) == 0 {
		return "00:00:00"
	}

	result := ""
	for i, part := range parts {
		if i > 0 {
			result += " "
		}
		result += part
	}
	return result
}

// IsZero returns true if the interval is zero
func (i Interval) IsZero() bool {
	return i.Months == 0 && i.Days == 0 && i.Seconds == 0
}

// NewIntervalValue creates a new INTERVAL value
func NewIntervalValue(i Interval) Value {
	return NewValue(i)
}

// NewNullIntervalValue creates a null INTERVAL value
func NewNullIntervalValue() Value {
	return NewNullValue()
}

// AddToTime adds an interval to a time.Time value
func (i Interval) AddToTime(t time.Time) time.Time {
	// Add months first
	if i.Months != 0 {
		t = t.AddDate(0, int(i.Months), 0)
	}

	// Add days
	if i.Days != 0 {
		t = t.AddDate(0, 0, int(i.Days))
	}

	// Add seconds
	if i.Seconds != 0 {
		t = t.Add(i.Seconds)
	}

	return t
}

// SubtractFromTime subtracts an interval from a time.Time value
func (i Interval) SubtractFromTime(t time.Time) time.Time {
	// Subtract months first
	if i.Months != 0 {
		t = t.AddDate(0, -int(i.Months), 0)
	}

	// Subtract days
	if i.Days != 0 {
		t = t.AddDate(0, 0, -int(i.Days))
	}

	// Subtract seconds
	if i.Seconds != 0 {
		t = t.Add(-i.Seconds)
	}

	return t
}

// TimeDifference calculates the interval between two time.Time values
func TimeDifference(t1, t2 time.Time) Interval {
	// For simple intervals, we'll just calculate the duration
	// This is a simplification - PostgreSQL has more complex rules
	duration := t1.Sub(t2)

	// Convert to days and seconds
	days := int32(duration.Hours() / 24)
	seconds := duration - time.Duration(days)*24*time.Hour

	return Interval{
		Months:  0, // We don't calculate months for simple differences
		Days:    days,
		Seconds: seconds,
	}
}
