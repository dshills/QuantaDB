package executor

import (
	"fmt"
	"hash"

	"github.com/dshills/QuantaDB/internal/sql/types"
)

// writeValueToHasher writes a value to a hasher in a consistent way.
func writeValueToHasher(hasher hash.Hash, val types.Value) {
	if val.IsNull() {
		hasher.Write([]byte("NULL"))
	} else {
		switch v := val.Data.(type) {
		case int64:
			fmt.Fprintf(hasher, "%d", v)
		case float64:
			fmt.Fprintf(hasher, "%f", v)
		case string:
			hasher.Write([]byte(v))
		case bool:
			if v {
				hasher.Write([]byte("true"))
			} else {
				hasher.Write([]byte("false"))
			}
		default:
			fmt.Fprintf(hasher, "%v", v)
		}
	}
}
