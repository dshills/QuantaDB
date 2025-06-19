package types

// unknownType represents an unknown type (used for NULL literals without context)
type unknownType struct{}

func (t *unknownType) Name() string {
	return "UNKNOWN"
}

func (t *unknownType) Size() int {
	return 0
}

func (t *unknownType) Compare(a, b Value) int {
	return CompareValues(a, b)
}

func (t *unknownType) Serialize(v Value) ([]byte, error) {
	if v.Null {
		return nil, nil
	}
	return nil, nil
}

func (t *unknownType) Deserialize(data []byte) (Value, error) {
	return NewNullValue(), nil
}

func (t *unknownType) IsValid(v Value) bool {
	return v.Null
}

func (t *unknownType) Zero() Value {
	return NewNullValue()
}

// Unknown is the unknown type instance
var Unknown DataType = &unknownType{}

// NullValue creates a NULL value of the given type
func NullValue(dt DataType) Value {
	return Value{Data: nil, Null: true}
}
