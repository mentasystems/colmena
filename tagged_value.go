package colmena

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"
)

// TaggedValue preserves a driver.Value's Go type across the RPC boundary so
// that database/sql.Scan can reconstruct types like time.Time on the caller.
//
// Wire format (JSON): {"t":"<kind>","v":<value>}.
//
// The set of kinds mirrors the driver.Value contract plus time.Time, which
// modernc.org/sqlite returns for DATETIME-affinity columns.
type TaggedValue struct {
	T string          `json:"t"`
	V json.RawMessage `json:"v,omitempty"`
}

const (
	tagNull   = "null"
	tagInt    = "int"
	tagFloat  = "float"
	tagBool   = "bool"
	tagString = "string"
	tagBytes  = "bytes"
	tagTime   = "time"
)

// encodeTaggedValue converts a driver.Value-like Go value into a TaggedValue.
// Unknown types are encoded as JSON under tagString as a best-effort fallback.
func encodeTaggedValue(v any) TaggedValue {
	switch x := v.(type) {
	case nil:
		return TaggedValue{T: tagNull}
	case bool:
		b, _ := json.Marshal(x)
		return TaggedValue{T: tagBool, V: b}
	case int:
		b, _ := json.Marshal(int64(x))
		return TaggedValue{T: tagInt, V: b}
	case int8:
		b, _ := json.Marshal(int64(x)) // safe-ignore: marshaling an int64 cannot fail
		return TaggedValue{T: tagInt, V: b}
	case int16:
		b, _ := json.Marshal(int64(x)) // safe-ignore: marshaling an int64 cannot fail
		return TaggedValue{T: tagInt, V: b}
	case int32:
		b, _ := json.Marshal(int64(x))
		return TaggedValue{T: tagInt, V: b}
	case int64:
		b, _ := json.Marshal(x)
		return TaggedValue{T: tagInt, V: b}
	case uint:
		b, _ := json.Marshal(int64(x))
		return TaggedValue{T: tagInt, V: b}
	case uint8:
		b, _ := json.Marshal(int64(x)) // safe-ignore: marshaling an int64 cannot fail
		return TaggedValue{T: tagInt, V: b}
	case uint16:
		b, _ := json.Marshal(int64(x)) // safe-ignore: marshaling an int64 cannot fail
		return TaggedValue{T: tagInt, V: b}
	case uint32:
		b, _ := json.Marshal(int64(x))
		return TaggedValue{T: tagInt, V: b}
	case uint64:
		b, _ := json.Marshal(int64(x))
		return TaggedValue{T: tagInt, V: b}
	case float32:
		b, _ := json.Marshal(float64(x))
		return TaggedValue{T: tagFloat, V: b}
	case float64:
		b, _ := json.Marshal(x)
		return TaggedValue{T: tagFloat, V: b}
	case string:
		b, _ := json.Marshal(x)
		return TaggedValue{T: tagString, V: b}
	case []byte:
		b, _ := json.Marshal(base64.StdEncoding.EncodeToString(x))
		return TaggedValue{T: tagBytes, V: b}
	case time.Time:
		b, _ := json.Marshal(x.Format(time.RFC3339Nano))
		return TaggedValue{T: tagTime, V: b}
	default:
		b, err := json.Marshal(x)
		if err != nil {
			b, _ = json.Marshal(fmt.Sprintf("%v", x))
		}
		return TaggedValue{T: tagString, V: b}
	}
}

// decodeTaggedValue materializes a Go value appropriate for database/sql.Scan.
// Returns an error if the tag is unknown or the payload is malformed.
func decodeTaggedValue(tv TaggedValue) (any, error) {
	switch tv.T {
	case tagNull, "":
		return nil, nil
	case tagBool:
		var b bool
		if err := json.Unmarshal(tv.V, &b); err != nil {
			return nil, fmt.Errorf("colmena: decode bool: %w", err)
		}
		return b, nil
	case tagInt:
		var i int64
		if err := json.Unmarshal(tv.V, &i); err != nil {
			return nil, fmt.Errorf("colmena: decode int: %w", err)
		}
		return i, nil
	case tagFloat:
		var f float64
		if err := json.Unmarshal(tv.V, &f); err != nil {
			return nil, fmt.Errorf("colmena: decode float: %w", err)
		}
		return f, nil
	case tagString:
		var s string
		if err := json.Unmarshal(tv.V, &s); err != nil {
			return nil, fmt.Errorf("colmena: decode string: %w", err)
		}
		return s, nil
	case tagBytes:
		var s string
		if err := json.Unmarshal(tv.V, &s); err != nil {
			return nil, fmt.Errorf("colmena: decode bytes: %w", err)
		}
		raw, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			return nil, fmt.Errorf("colmena: decode bytes base64: %w", err)
		}
		return raw, nil
	case tagTime:
		var s string
		if err := json.Unmarshal(tv.V, &s); err != nil {
			return nil, fmt.Errorf("colmena: decode time: %w", err)
		}
		t, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			return nil, fmt.Errorf("colmena: parse time %q: %w", s, err)
		}
		return t, nil
	default:
		return nil, fmt.Errorf("colmena: unknown tagged value kind %q", tv.T)
	}
}
