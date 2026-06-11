package gcvctor

import (
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/spantype/typector"
	"github.com/google/uuid"
)

// BoolFromPtr returns a BOOL GenericColumnValue. A nil pointer yields typed SQL NULL.
func BoolFromPtr(p *bool) spanner.GenericColumnValue {
	if p == nil {
		return NullFromCode(sppb.TypeCode_BOOL)
	}
	return BoolValue(*p)
}

// Int64FromPtr returns an INT64 GenericColumnValue. A nil pointer yields typed SQL NULL.
func Int64FromPtr(p *int64) spanner.GenericColumnValue {
	if p == nil {
		return NullFromCode(sppb.TypeCode_INT64)
	}
	return Int64Value(*p)
}

// Float64FromPtr returns a FLOAT64 GenericColumnValue. A nil pointer yields typed SQL NULL.
func Float64FromPtr(p *float64) spanner.GenericColumnValue {
	if p == nil {
		return NullFromCode(sppb.TypeCode_FLOAT64)
	}
	return Float64Value(*p)
}

// Float32FromPtr returns a FLOAT32 GenericColumnValue. A nil pointer yields typed SQL NULL.
func Float32FromPtr(p *float32) spanner.GenericColumnValue {
	if p == nil {
		return NullFromCode(sppb.TypeCode_FLOAT32)
	}
	return Float32Value(*p)
}

// StringFromPtr returns a STRING GenericColumnValue. A nil pointer yields typed SQL NULL.
func StringFromPtr(p *string) spanner.GenericColumnValue {
	if p == nil {
		return NullFromCode(sppb.TypeCode_STRING)
	}
	return StringValue(*p)
}

// BytesFromSlice returns a BYTES GenericColumnValue. A nil slice yields typed SQL NULL.
// For non-null empty BYTES (wire base64 ""), use [BytesValue] even with a nil slice.
func BytesFromSlice(v []byte) spanner.GenericColumnValue {
	if v == nil {
		return NullFromCode(sppb.TypeCode_BYTES)
	}
	return BytesValue(v)
}

// DateFromPtr returns a DATE GenericColumnValue. A nil pointer yields typed SQL NULL.
func DateFromPtr(p *civil.Date) spanner.GenericColumnValue {
	if p == nil {
		return NullFromCode(sppb.TypeCode_DATE)
	}
	return DateValue(*p)
}

// TimestampFromPtr returns a TIMESTAMP GenericColumnValue. A nil pointer yields typed SQL NULL.
//
// Callers mirroring the official client's encodeValue must handle [cloud.google.com/go/spanner.CommitTimestamp]
// separately; the client checks that sentinel before formatting timestamps.
func TimestampFromPtr(p *time.Time) spanner.GenericColumnValue {
	if p == nil {
		return NullFromCode(sppb.TypeCode_TIMESTAMP)
	}
	return TimestampValue(*p)
}

// UUIDFromPtr returns a UUID GenericColumnValue. A nil pointer yields typed SQL NULL.
func UUIDFromPtr(p *uuid.UUID) spanner.GenericColumnValue {
	if p == nil {
		return NullFromCode(sppb.TypeCode_UUID)
	}
	return UUIDValue(*p)
}

// BoolFromNullable returns a BOOL GenericColumnValue from a Spanner null wrapper.
func BoolFromNullable(n spanner.NullBool) spanner.GenericColumnValue {
	if !n.Valid {
		return NullFromCode(sppb.TypeCode_BOOL)
	}
	return BoolValue(n.Bool)
}

// Int64FromNullable returns an INT64 GenericColumnValue from a Spanner null wrapper.
func Int64FromNullable(n spanner.NullInt64) spanner.GenericColumnValue {
	if !n.Valid {
		return NullFromCode(sppb.TypeCode_INT64)
	}
	return Int64Value(n.Int64)
}

// Float64FromNullable returns a FLOAT64 GenericColumnValue from a Spanner null wrapper.
func Float64FromNullable(n spanner.NullFloat64) spanner.GenericColumnValue {
	if !n.Valid {
		return NullFromCode(sppb.TypeCode_FLOAT64)
	}
	return Float64Value(n.Float64)
}

// Float32FromNullable returns a FLOAT32 GenericColumnValue from a Spanner null wrapper.
func Float32FromNullable(n spanner.NullFloat32) spanner.GenericColumnValue {
	if !n.Valid {
		return NullFromCode(sppb.TypeCode_FLOAT32)
	}
	return Float32Value(n.Float32)
}

// StringFromNullable returns a STRING GenericColumnValue from a Spanner null wrapper.
func StringFromNullable(n spanner.NullString) spanner.GenericColumnValue {
	if !n.Valid {
		return NullFromCode(sppb.TypeCode_STRING)
	}
	return StringValue(n.StringVal)
}

// DateFromNullable returns a DATE GenericColumnValue from a Spanner null wrapper.
func DateFromNullable(n spanner.NullDate) spanner.GenericColumnValue {
	if !n.Valid {
		return NullFromCode(sppb.TypeCode_DATE)
	}
	return DateValue(n.Date)
}

// TimestampFromNullable returns a TIMESTAMP GenericColumnValue from a Spanner null wrapper.
//
// Callers mirroring the official client's encodeValue must handle [cloud.google.com/go/spanner.CommitTimestamp]
// separately; the client checks that sentinel before formatting timestamps.
func TimestampFromNullable(n spanner.NullTime) spanner.GenericColumnValue {
	if !n.Valid {
		return NullFromCode(sppb.TypeCode_TIMESTAMP)
	}
	return TimestampValue(n.Time)
}

// UUIDFromNullable returns a UUID GenericColumnValue from a Spanner null wrapper.
func UUIDFromNullable(n spanner.NullUUID) spanner.GenericColumnValue {
	if !n.Valid {
		return NullFromCode(sppb.TypeCode_UUID)
	}
	return UUIDValue(n.UUID)
}

// IntervalFromPtr returns an INTERVAL GenericColumnValue. A nil pointer yields typed SQL NULL.
func IntervalFromPtr(p *spanner.Interval) spanner.GenericColumnValue {
	if p == nil {
		return NullFromCode(sppb.TypeCode_INTERVAL)
	}
	return IntervalValue(*p)
}

// NumericFromNullable returns a NUMERIC GenericColumnValue from a Spanner null wrapper.
func NumericFromNullable(n spanner.NullNumeric) spanner.GenericColumnValue {
	if !n.Valid {
		return NullFromCode(sppb.TypeCode_NUMERIC)
	}
	return NumericValue(&n.Numeric)
}

// JSONFromNullable returns a JSON GenericColumnValue from a Spanner null wrapper.
// When Valid, n.Value is marshaled like [JSONValue]: a Go string becomes a quoted
// JSON string on the wire, matching the official client's encodeValue. To store
// pre-encoded wire JSON as-is (validated and compacted), pass it as an
// [encoding/json.RawMessage], the same convention the client follows.
func JSONFromNullable(n spanner.NullJSON) (spanner.GenericColumnValue, error) {
	if !n.Valid {
		return NullFromCode(sppb.TypeCode_JSON), nil
	}
	return JSONValue(n.Value)
}

// IntervalFromNullable returns an INTERVAL GenericColumnValue from a Spanner null wrapper.
func IntervalFromNullable(n spanner.NullInterval) spanner.GenericColumnValue {
	if !n.Valid {
		return NullFromCode(sppb.TypeCode_INTERVAL)
	}
	return IntervalValue(n.Interval)
}

// PGNumericFromNullable returns a PostgreSQL-dialect NUMERIC GenericColumnValue from a
// [cloud.google.com/go/spanner.PGNumeric] wrapper. The wire string is stored as-is when Valid.
func PGNumericFromNullable(n spanner.PGNumeric) spanner.GenericColumnValue {
	if !n.Valid {
		return NullOf(typector.PGNumeric())
	}
	return StringBasedValueOf(typector.PGNumeric(), n.Numeric)
}

// PGJSONBFromNullable returns a PostgreSQL-dialect JSON GenericColumnValue from a
// [cloud.google.com/go/spanner.PGJsonB] wrapper. When Valid, n.Value is marshaled
// like [PGJSONBValue]: a Go string becomes a quoted JSON string on the wire,
// matching the official client's encodeValue. To store pre-encoded wire JSON
// as-is (validated and compacted), pass it as an [encoding/json.RawMessage].
func PGJSONBFromNullable(n spanner.PGJsonB) (spanner.GenericColumnValue, error) {
	if !n.Valid {
		return NullOf(typector.PGJSONB()), nil
	}
	return PGJSONBValue(n.Value)
}
