package types

import (
	"math"
	"testing"
)

func TestFloatType(t *testing.T) {
	ft := Float

	t.Run("Name", func(t *testing.T) {
		if ft.Name() != "FLOAT" {
			t.Errorf("expected FLOAT, got %s", ft.Name())
		}
	})

	t.Run("Size", func(t *testing.T) {
		if ft.Size() != 4 {
			t.Errorf("expected 4, got %d", ft.Size())
		}
	})

	t.Run("SerializeDeserialize", func(t *testing.T) {
		tests := []float32{
			0.0,
			1.5,
			-1.5,
			3.14159,
			math.MaxFloat32,
			math.SmallestNonzeroFloat32,
		}

		for _, val := range tests {
			v := NewValue(val)
			data, err := ft.Serialize(v)
			if err != nil {
				t.Errorf("serialize error for %f: %v", val, err)
				continue
			}

			v2, err := ft.Deserialize(data)
			if err != nil {
				t.Errorf("deserialize error for %f: %v", val, err)
				continue
			}

			if v2.IsNull() {
				t.Errorf("expected non-null value for %f", val)
				continue
			}

			val2, ok := v2.Data.(float32)
			if !ok {
				t.Errorf("expected float32, got %T", v2.Data)
				continue
			}

			if val != val2 {
				t.Errorf("expected %f, got %f", val, val2)
			}
		}
	})

	t.Run("SerializeNull", func(t *testing.T) {
		v := NullValue(Float)
		data, err := ft.Serialize(v)
		if err != nil {
			t.Errorf("serialize null error: %v", err)
		}
		if data != nil {
			t.Errorf("expected nil data for null value")
		}
	})

	t.Run("DeserializeNull", func(t *testing.T) {
		v, err := ft.Deserialize(nil)
		if err != nil {
			t.Errorf("deserialize null error: %v", err)
		}
		if !v.IsNull() {
			t.Errorf("expected null value")
		}
	})

	t.Run("Compare", func(t *testing.T) {
		tests := []struct {
			a, b float32
			want int
		}{
			{1.0, 2.0, -1},
			{2.0, 1.0, 1},
			{1.5, 1.5, 0},
			{-1.0, 1.0, -1},
		}

		for _, tc := range tests {
			va := NewValue(tc.a)
			vb := NewValue(tc.b)
			got := ft.Compare(va, vb)
			if got != tc.want {
				t.Errorf("Compare(%f, %f) = %d, want %d", tc.a, tc.b, got, tc.want)
			}
		}
	})

	t.Run("Zero", func(t *testing.T) {
		v := ft.Zero()
		if v.IsNull() {
			t.Error("expected non-null zero value")
		}
		val, ok := v.Data.(float32)
		if !ok {
			t.Errorf("expected float32, got %T", v.Data)
		}
		if val != 0.0 {
			t.Errorf("expected 0.0, got %f", val)
		}
	})
}

func TestDoubleType(t *testing.T) {
	dt := Double

	t.Run("Name", func(t *testing.T) {
		if dt.Name() != "DOUBLE PRECISION" {
			t.Errorf("expected DOUBLE PRECISION, got %s", dt.Name())
		}
	})

	t.Run("Size", func(t *testing.T) {
		if dt.Size() != 8 {
			t.Errorf("expected 8, got %d", dt.Size())
		}
	})

	t.Run("SerializeDeserialize", func(t *testing.T) {
		tests := []float64{
			0.0,
			1.5,
			-1.5,
			3.14159265359,
			math.MaxFloat64,
			math.SmallestNonzeroFloat64,
		}

		for _, val := range tests {
			v := NewValue(val)
			data, err := dt.Serialize(v)
			if err != nil {
				t.Errorf("serialize error for %f: %v", val, err)
				continue
			}

			v2, err := dt.Deserialize(data)
			if err != nil {
				t.Errorf("deserialize error for %f: %v", val, err)
				continue
			}

			if v2.IsNull() {
				t.Errorf("expected non-null value for %f", val)
				continue
			}

			val2, ok := v2.Data.(float64)
			if !ok {
				t.Errorf("expected float64, got %T", v2.Data)
				continue
			}

			if val != val2 {
				t.Errorf("expected %f, got %f", val, val2)
			}
		}
	})
}

func TestFloatConversions(t *testing.T) {
	t.Run("AsFloat", func(t *testing.T) {
		tests := []struct {
			value Value
			want  float32
			err   bool
		}{
			{NewValue(float32(3.14)), 3.14, false},
			{NewValue(float64(3.14)), 3.14, false},
			{NewValue(int32(42)), 42.0, false},
			{NewValue(int64(42)), 42.0, false},
			{NewValue(int16(42)), 42.0, false},
			{NewValue("string"), 0, true},
			{NullValue(Float), 0, true},
		}

		for _, tc := range tests {
			got, err := tc.value.AsFloat()
			if tc.err {
				if err == nil {
					t.Errorf("expected error for %v", tc.value)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error for %v: %v", tc.value, err)
				}
				if math.Abs(float64(got-tc.want)) > 0.001 {
					t.Errorf("AsFloat(%v) = %f, want %f", tc.value, got, tc.want)
				}
			}
		}
	})

	t.Run("AsDouble", func(t *testing.T) {
		tests := []struct {
			value Value
			want  float64
			err   bool
		}{
			{NewValue(float64(3.14159)), 3.14159, false},
			{NewValue(float32(3.14)), float64(float32(3.14)), false},
			{NewValue(int32(42)), 42.0, false},
			{NewValue(int64(42)), 42.0, false},
			{NewValue(int16(42)), 42.0, false},
			{NewValue("string"), 0, true},
			{NullValue(Double), 0, true},
		}

		for _, tc := range tests {
			got, err := tc.value.AsDouble()
			if tc.err {
				if err == nil {
					t.Errorf("expected error for %v", tc.value)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error for %v: %v", tc.value, err)
				}
				if math.Abs(got-tc.want) > 0.00001 {
					t.Errorf("AsDouble(%v) = %f, want %f", tc.value, got, tc.want)
				}
			}
		}
	})
}
