package timeutil

import (
	"testing"
	"time"
)

func TestTimeConversions(t *testing.T) {
	testTime := time.Date(2024, 12, 20, 10, 30, 45, 123456789, time.UTC)

	// Test TimeToUnixNano and UnixNanoToTime
	nano := TimeToUnixNano(testTime)
	converted := UnixNanoToTime(nano)

	if !testTime.Equal(converted) {
		t.Errorf("Time conversion failed: original=%v, converted=%v", testTime, converted)
	}
}

func TestTimeSerialization(t *testing.T) {
	testTime := time.Date(2024, 12, 20, 10, 30, 45, 123456789, time.UTC)

	// Test TimeToBytes and BytesToTime
	bytes := TimeToBytes(testTime)
	converted, err := BytesToTime(bytes)
	if err != nil {
		t.Fatalf("BytesToTime failed: %v", err)
	}

	if !testTime.Equal(converted) {
		t.Errorf("Serialization failed: original=%v, converted=%v", testTime, converted)
	}
}

func TestBufferOperations(t *testing.T) {
	testTime := time.Date(2024, 12, 20, 10, 30, 45, 123456789, time.UTC)
	buf := make([]byte, 16)

	// Test WriteTimestampToBuf and ReadTimestampFromBuf
	err := WriteTimestampToBuf(buf, 4, testTime)
	if err != nil {
		t.Fatalf("WriteTimestampToBuf failed: %v", err)
	}

	converted, err := ReadTimestampFromBuf(buf, 4)
	if err != nil {
		t.Fatalf("ReadTimestampFromBuf failed: %v", err)
	}

	if !testTime.Equal(converted) {
		t.Errorf("Buffer operations failed: original=%v, converted=%v", testTime, converted)
	}
}

func TestParseTimestamp(t *testing.T) {
	tests := []struct {
		input    string
		expected bool // whether parsing should succeed
	}{
		{"2024-12-20T10:30:45Z", true},
		{"2024-12-20T10:30:45.123456789Z", true},
		{"2024-12-20 10:30:45", true},
		{"2024-12-20", true},
		{"10:30:45", true},
		{"invalid", false},
	}

	for _, test := range tests {
		_, err := ParseTimestamp(test.input)
		success := err == nil

		if success != test.expected {
			t.Errorf("ParseTimestamp(%q): expected success=%v, got success=%v, err=%v",
				test.input, test.expected, success, err)
		}
	}
}

func TestParseDate(t *testing.T) {
	tests := []struct {
		input    string
		expected bool // whether parsing should succeed
	}{
		{"2024-12-20", true},
		{"2024-12-20T10:30:45Z", true},
		{"2024-12-20 10:30:45", true},
		{"10:30:45", false}, // time only, not a date
		{"invalid", false},
	}

	for _, test := range tests {
		_, err := ParseDate(test.input)
		success := err == nil

		if success != test.expected {
			t.Errorf("ParseDate(%q): expected success=%v, got success=%v, err=%v",
				test.input, test.expected, success, err)
		}
	}
}

func TestEpochFunctions(t *testing.T) {
	epoch := EpochTime()
	epochUTC := EpochUTC()

	if !epoch.Equal(time.Unix(0, 0)) {
		t.Errorf("EpochTime() returned %v, expected Unix epoch", epoch)
	}

	if !epochUTC.Equal(time.Unix(0, 0).UTC()) {
		t.Errorf("EpochUTC() returned %v, expected Unix epoch UTC", epochUTC)
	}

	if !IsEpoch(epoch) {
		t.Errorf("IsEpoch() should return true for epoch time")
	}

	if !IsEpoch(time.Time{}) {
		t.Errorf("IsEpoch() should return true for zero time")
	}

	if IsEpoch(time.Now()) {
		t.Errorf("IsEpoch() should return false for current time")
	}
}

func TestDurationConversions(t *testing.T) {
	duration := 1500 * time.Millisecond

	millis := DurationToMillis(duration)
	if millis != 1500 {
		t.Errorf("DurationToMillis(%v) = %d, expected 1500", duration, millis)
	}

	converted := MillisToDuration(millis)
	if converted != duration {
		t.Errorf("MillisToDuration(%d) = %v, expected %v", millis, converted, duration)
	}
}

func TestBytesToTimeErrorCases(t *testing.T) {
	// Test insufficient data
	_, err := BytesToTime([]byte{1, 2, 3})
	if err == nil {
		t.Error("BytesToTime should fail with insufficient data")
	}

	// Test buffer operations with insufficient space
	buf := make([]byte, 4)
	err = WriteTimestampToBuf(buf, 0, time.Now())
	if err == nil {
		t.Error("WriteTimestampToBuf should fail with insufficient buffer space")
	}

	_, err = ReadTimestampFromBuf(buf, 0)
	if err == nil {
		t.Error("ReadTimestampFromBuf should fail with insufficient buffer space")
	}
}
