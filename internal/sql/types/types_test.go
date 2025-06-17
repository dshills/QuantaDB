package types

import (
	"math/big"
	"testing"
	"time"

	"github.com/dshills/QuantaDB/internal/testutil"
)

func TestIntegerTypes(t *testing.T) {
	t.Run("INTEGER", func(t *testing.T) {
		// Test basic properties
		testutil.AssertEqual(t, "INTEGER", Integer.Name())
		testutil.AssertEqual(t, 4, Integer.Size())
		
		// Test values
		v1 := NewIntegerValue(42)
		v2 := NewIntegerValue(-10)
		v3 := NewNullValue()
		
		// Test comparison
		testutil.AssertEqual(t, 1, Integer.Compare(v1, v2))
		testutil.AssertEqual(t, -1, Integer.Compare(v2, v1))
		testutil.AssertEqual(t, 0, Integer.Compare(v1, v1))
		testutil.AssertEqual(t, -1, Integer.Compare(v3, v1))
		
		// Test serialization
		data, err := Integer.Serialize(v1)
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, 4, len(data))
		
		// Test deserialization
		v4, err := Integer.Deserialize(data)
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, int32(42), v4.Data)
		
		// Test null handling
		nullData, err := Integer.Serialize(v3)
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, 0, len(nullData))
		
		v5, err := Integer.Deserialize(nil)
		testutil.AssertNoError(t, err)
		testutil.AssertTrue(t, v5.IsNull(), "should be null")
	})
	
	t.Run("BIGINT", func(t *testing.T) {
		testutil.AssertEqual(t, "BIGINT", BigInt.Name())
		testutil.AssertEqual(t, 8, BigInt.Size())
		
		v1 := NewBigIntValue(int64(1234567890123456))
		data, err := BigInt.Serialize(v1)
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, 8, len(data))
		
		v2, err := BigInt.Deserialize(data)
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, int64(1234567890123456), v2.Data)
	})
	
	t.Run("SMALLINT", func(t *testing.T) {
		testutil.AssertEqual(t, "SMALLINT", SmallInt.Name())
		testutil.AssertEqual(t, 2, SmallInt.Size())
		
		v1 := NewSmallIntValue(int16(32767))
		data, err := SmallInt.Serialize(v1)
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, 2, len(data))
		
		v2, err := SmallInt.Deserialize(data)
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, int16(32767), v2.Data)
	})
}

func TestStringTypes(t *testing.T) {
	t.Run("VARCHAR", func(t *testing.T) {
		varchar10 := Varchar(10)
		testutil.AssertEqual(t, "VARCHAR(10)", varchar10.Name())
		testutil.AssertEqual(t, -1, varchar10.Size())
		
		// Test valid string
		v1 := NewVarcharValue("hello")
		testutil.AssertTrue(t, varchar10.IsValid(v1), "should be valid")
		
		// Test serialization
		data, err := varchar10.Serialize(v1)
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, 4+5, len(data)) // 4 bytes length + 5 bytes string
		
		// Test deserialization
		v2, err := varchar10.Deserialize(data)
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, "hello", v2.Data)
		
		// Test too long string
		v3 := NewVarcharValue("this is too long")
		testutil.AssertFalse(t, varchar10.IsValid(v3), "should be invalid")
		
		// Test comparison
		v4 := NewVarcharValue("world")
		testutil.AssertEqual(t, -1, varchar10.Compare(v1, v4))
		testutil.AssertEqual(t, 1, varchar10.Compare(v4, v1))
	})
	
	t.Run("CHAR", func(t *testing.T) {
		char5 := Char(5)
		testutil.AssertEqual(t, "CHAR(5)", char5.Name())
		testutil.AssertEqual(t, 5, char5.Size())
		
		// Test padding
		v1 := NewCharValue("hi")
		data, err := char5.Serialize(v1)
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, 5, len(data))
		testutil.AssertEqual(t, "hi   ", string(data))
		
		// Test deserialization removes padding
		v2, err := char5.Deserialize(data)
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, "hi", v2.Data)
		
		// Test comparison ignores trailing spaces
		v3 := NewCharValue("hi   ")
		testutil.AssertEqual(t, 0, char5.Compare(v1, v3))
	})
	
	t.Run("TEXT", func(t *testing.T) {
		testutil.AssertEqual(t, "TEXT", Text.Name())
		testutil.AssertEqual(t, -1, Text.Size())
		
		// Test long string
		longStr := "This is a very long text that would not fit in a VARCHAR(255)"
		v1 := NewTextValue(longStr)
		
		data, err := Text.Serialize(v1)
		testutil.AssertNoError(t, err)
		
		v2, err := Text.Deserialize(data)
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, longStr, v2.Data)
	})
}

func TestBooleanType(t *testing.T) {
	testutil.AssertEqual(t, "BOOLEAN", Boolean.Name())
	testutil.AssertEqual(t, 1, Boolean.Size())
	
	vTrue := NewBooleanValue(true)
	vFalse := NewBooleanValue(false)
	
	// Test comparison (false < true)
	testutil.AssertEqual(t, -1, Boolean.Compare(vFalse, vTrue))
	testutil.AssertEqual(t, 1, Boolean.Compare(vTrue, vFalse))
	testutil.AssertEqual(t, 0, Boolean.Compare(vTrue, vTrue))
	
	// Test serialization
	dataTrue, err := Boolean.Serialize(vTrue)
	testutil.AssertNoError(t, err)
	testutil.AssertEqual(t, []byte{1}, dataTrue)
	
	dataFalse, err := Boolean.Serialize(vFalse)
	testutil.AssertNoError(t, err)
	testutil.AssertEqual(t, []byte{0}, dataFalse)
	
	// Test deserialization
	v1, err := Boolean.Deserialize(dataTrue)
	testutil.AssertNoError(t, err)
	testutil.AssertEqual(t, true, v1.Data)
	
	v2, err := Boolean.Deserialize(dataFalse)
	testutil.AssertNoError(t, err)
	testutil.AssertEqual(t, false, v2.Data)
}

func TestTimestampType(t *testing.T) {
	testutil.AssertEqual(t, "TIMESTAMP", Timestamp.Name())
	testutil.AssertEqual(t, 8, Timestamp.Size())
	
	now := time.Now().Truncate(time.Nanosecond) // Ensure no precision loss
	v1 := NewTimestampValue(now)
	
	// Test serialization
	data, err := Timestamp.Serialize(v1)
	testutil.AssertNoError(t, err)
	testutil.AssertEqual(t, 8, len(data))
	
	// Test deserialization
	v2, err := Timestamp.Deserialize(data)
	testutil.AssertNoError(t, err)
	testutil.AssertEqual(t, now.UnixNano(), v2.Data.(time.Time).UnixNano())
	
	// Test comparison
	later := now.Add(time.Hour)
	v3 := NewTimestampValue(later)
	testutil.AssertEqual(t, -1, Timestamp.Compare(v1, v3))
	testutil.AssertEqual(t, 1, Timestamp.Compare(v3, v1))
	
	// Test parsing
	v4, err := ParseTimestamp("2023-12-25 10:30:00")
	testutil.AssertNoError(t, err)
	testutil.AssertFalse(t, v4.IsNull(), "should not be null")
}

func TestDateType(t *testing.T) {
	testutil.AssertEqual(t, "DATE", Date.Name())
	testutil.AssertEqual(t, 4, Date.Size())
	
	date := time.Date(2023, 12, 25, 0, 0, 0, 0, time.UTC)
	v1 := NewDateValue(date)
	
	// Test that time portion is ignored
	dateWithTime := time.Date(2023, 12, 25, 15, 30, 45, 0, time.UTC)
	v2 := NewDateValue(dateWithTime)
	testutil.AssertEqual(t, 0, Date.Compare(v1, v2))
	
	// Test serialization
	data, err := Date.Serialize(v1)
	testutil.AssertNoError(t, err)
	testutil.AssertEqual(t, 4, len(data))
	
	// Test deserialization
	v3, err := Date.Deserialize(data)
	testutil.AssertNoError(t, err)
	v3Time := v3.Data.(time.Time)
	testutil.AssertEqual(t, 2023, v3Time.Year())
	testutil.AssertEqual(t, time.December, v3Time.Month())
	testutil.AssertEqual(t, 25, v3Time.Day())
	
	// Test parsing
	v4, err := ParseDate("2023-12-25")
	testutil.AssertNoError(t, err)
	testutil.AssertFalse(t, v4.IsNull(), "should not be null")
}

func TestDecimalType(t *testing.T) {
	decimal10_2 := Decimal(10, 2)
	testutil.AssertEqual(t, "DECIMAL(10,2)", decimal10_2.Name())
	
	// Test creation from string
	v1, err := NewDecimalValue("123.45")
	testutil.AssertNoError(t, err)
	
	// Test serialization
	data, err := decimal10_2.Serialize(v1)
	testutil.AssertNoError(t, err)
	
	// Test deserialization
	v2, err := decimal10_2.Deserialize(data)
	testutil.AssertNoError(t, err)
	
	// Verify value
	rat1 := v1.Data.(*big.Rat)
	rat2 := v2.Data.(*big.Rat)
	testutil.AssertEqual(t, 0, rat1.Cmp(rat2))
	
	// Test comparison
	v3, _ := NewDecimalValue("100.00")
	testutil.AssertEqual(t, 1, decimal10_2.Compare(v1, v3))
	testutil.AssertEqual(t, -1, decimal10_2.Compare(v3, v1))
	
	// Test from float
	v4 := NewDecimalValueFromFloat(99.99)
	testutil.AssertTrue(t, decimal10_2.IsValid(v4), "should be valid")
	
	// Test from int
	v5 := NewDecimalValueFromInt(100)
	testutil.AssertTrue(t, decimal10_2.IsValid(v5), "should be valid")
}

func TestNullHandling(t *testing.T) {
	nullVal := NewNullValue()
	testutil.AssertTrue(t, nullVal.IsNull(), "should be null")
	testutil.AssertEqual(t, "NULL", nullVal.String())
	
	// Test all types handle null correctly
	types := []DataType{
		Integer, BigInt, SmallInt, Boolean,
		Varchar(10), Char(5), Text,
		Timestamp, Date, Decimal(10, 2),
	}
	
	for _, dt := range types {
		// Serialize null
		data, err := dt.Serialize(nullVal)
		testutil.AssertNoError(t, err)
		testutil.AssertTrue(t, data == nil || len(data) == 0, "null should serialize to nil or empty")
		
		// Deserialize null
		val, err := dt.Deserialize(nil)
		testutil.AssertNoError(t, err)
		testutil.AssertTrue(t, val.IsNull(), "should deserialize to null")
		
		// Validate null
		testutil.AssertTrue(t, dt.IsValid(nullVal), "null should be valid")
		
		// Compare with null
		nonNull := dt.Zero()
		testutil.AssertEqual(t, 1, dt.Compare(nonNull, nullVal))
		testutil.AssertEqual(t, -1, dt.Compare(nullVal, nonNull))
		testutil.AssertEqual(t, 0, dt.Compare(nullVal, nullVal))
	}
}

func TestRow(t *testing.T) {
	// Create a row with mixed types
	row := NewRow(
		NewIntegerValue(42),
		NewVarcharValue("hello"),
		NewBooleanValue(true),
		NewNullValue(),
	)
	
	testutil.AssertEqual(t, 4, len(row.Values))
	testutil.AssertEqual(t, int32(42), row.Get(0).Data)
	testutil.AssertEqual(t, "hello", row.Get(1).Data)
	testutil.AssertEqual(t, true, row.Get(2).Data)
	testutil.AssertTrue(t, row.Get(3).IsNull(), "should be null")
	
	// Test out of bounds
	testutil.AssertTrue(t, row.Get(-1).IsNull(), "should return null for negative index")
	testutil.AssertTrue(t, row.Get(10).IsNull(), "should return null for out of bounds")
}