package gcvctor

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"time"

	"github.com/apstndb/spantype"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/spantype/typector"
	"google.golang.org/protobuf/types/known/structpb"
)

var (
	// ErrTypeMismatch is returned by [ArrayValueOf] when an element's type does not match elemType.
	ErrTypeMismatch = errors.New("gcvctor: type mismatch")
	// ErrMismatchedCounts is returned by [StructValueOf] when len(names) != len(gcvs).
	ErrMismatchedCounts = errors.New("gcvctor: mismatched name/value count")
	// ErrNilElementType is returned by [ArrayValueOf] when elemType is nil.
	ErrNilElementType = errors.New("gcvctor: nil array element type")
)

// BoolValue returns a non-null BOOL GenericColumnValue.
func BoolValue(v bool) spanner.GenericColumnValue {
	return spanner.GenericColumnValue{
		Type:  typector.CodeToSimpleType(sppb.TypeCode_BOOL),
		Value: structpb.NewBoolValue(v),
	}
}

// Int64Value returns a non-null INT64 GenericColumnValue (decimal string wire format).
func Int64Value(v int64) spanner.GenericColumnValue {
	return spanner.GenericColumnValue{
		Type:  typector.CodeToSimpleType(sppb.TypeCode_INT64),
		Value: structpb.NewStringValue(strconv.FormatInt(v, 10)),
	}
}

// Float64Value returns a non-null FLOAT64 GenericColumnValue. NaN and ±Inf use string wire values
// matching Spanner's encoding.
func Float64Value(v float64) spanner.GenericColumnValue {
	return spanner.GenericColumnValue{
		Type:  typector.CodeToSimpleType(sppb.TypeCode_FLOAT64),
		Value: float64ToStructpbValue(v),
	}
}

// Float32Value returns a non-null FLOAT32 GenericColumnValue. NaN and ±Inf use string wire values
// matching Spanner's encoding.
func Float32Value(v float32) spanner.GenericColumnValue {
	return spanner.GenericColumnValue{
		Type:  typector.CodeToSimpleType(sppb.TypeCode_FLOAT32),
		Value: float64ToStructpbValue(float64(v)),
	}
}

// float64ToStructpbValue converts a float64 to the appropriate structpb.Value.
// Spanner encodes NaN and ±Infinity as StringValue, finite values as NumberValue.
// The string representations match Spanner's wire format: "NaN", "Infinity", "-Infinity".
func float64ToStructpbValue(v float64) *structpb.Value {
	switch {
	case math.IsNaN(v):
		return structpb.NewStringValue("NaN")
	case math.IsInf(v, 1):
		return structpb.NewStringValue("Infinity")
	case math.IsInf(v, -1):
		return structpb.NewStringValue("-Infinity")
	default:
		return structpb.NewNumberValue(v)
	}
}

// StringValue returns a non-null STRING GenericColumnValue.
func StringValue(v string) spanner.GenericColumnValue {
	return spanner.GenericColumnValue{
		Type:  typector.CodeToSimpleType(sppb.TypeCode_STRING),
		Value: structpb.NewStringValue(v),
	}
}

// BytesValue returns a non-null BYTES GenericColumnValue (base64 wire encoding).
func BytesValue(v []byte) spanner.GenericColumnValue {
	return BytesBasedValueOf(typector.CodeToSimpleType(sppb.TypeCode_BYTES), v)
}

// BytesBasedValueOf constructs a GenericColumnValue with an arbitrary bytes-compatible
// [cloud.google.com/go/spanner/apiv1/spannerpb.Type] and base64-encoded payload in Value.
func BytesBasedValueOf(typ *sppb.Type, v []byte) spanner.GenericColumnValue {
	return spanner.GenericColumnValue{
		Type:  typ,
		Value: structpb.NewStringValue(base64.StdEncoding.EncodeToString(v)),
	}
}

// StringBasedValueFromCode constructs a GenericColumnValue for a simple scalar type code
// with a string wire payload.
func StringBasedValueFromCode(code sppb.TypeCode, v string) spanner.GenericColumnValue {
	return spanner.GenericColumnValue{
		Type:  typector.CodeToSimpleType(code),
		Value: structpb.NewStringValue(v),
	}
}

// DateValue returns a non-null DATE GenericColumnValue.
func DateValue(v civil.Date) spanner.GenericColumnValue {
	return StringBasedValueFromCode(sppb.TypeCode_DATE, v.String())
}

// TimestampValue returns a non-null TIMESTAMP GenericColumnValue (RFC3339Nano string wire format).
func TimestampValue(v time.Time) spanner.GenericColumnValue {
	return StringBasedValueFromCode(sppb.TypeCode_TIMESTAMP, v.Format(time.RFC3339Nano))
}

// NumericValue returns a non-null NUMERIC GenericColumnValue.
func NumericValue(v *big.Rat) spanner.GenericColumnValue {
	return StringBasedValueFromCode(sppb.TypeCode_NUMERIC, spanner.NumericString(v))
}

// IntervalValue returns a non-null INTERVAL GenericColumnValue.
func IntervalValue(v spanner.Interval) spanner.GenericColumnValue {
	return StringBasedValueFromCode(sppb.TypeCode_INTERVAL, v.String())
}

// UUIDValue returns a non-null UUID GenericColumnValue.
func UUIDValue(v uuid.UUID) spanner.GenericColumnValue {
	return StringBasedValueFromCode(sppb.TypeCode_UUID, v.String())
}

// JSONValue marshals v to JSON and returns a non-null JSON GenericColumnValue.
func JSONValue(v any) (spanner.GenericColumnValue, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return spanner.GenericColumnValue{}, err
	}
	return StringBasedValueFromCode(sppb.TypeCode_JSON, string(b)), nil
}

// PGNumericValue returns a non-null PostgreSQL-dialect NUMERIC GenericColumnValue
// ([sppb.TypeAnnotationCode_PG_NUMERIC]).
func PGNumericValue(v *big.Rat) spanner.GenericColumnValue {
	return spanner.GenericColumnValue{
		Type:  typector.PGNumeric(),
		Value: structpb.NewStringValue(spanner.NumericString(v)),
	}
}

// PGJSONBValue marshals v to JSON and returns a non-null PostgreSQL-dialect JSON GenericColumnValue
// ([sppb.TypeAnnotationCode_PG_JSONB]).
func PGJSONBValue(v any) (spanner.GenericColumnValue, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return spanner.GenericColumnValue{}, err
	}
	return spanner.GenericColumnValue{
		Type:  typector.PGJSONB(),
		Value: structpb.NewStringValue(string(b)),
	}, nil
}

// ProtoValue returns a non-null PROTO GenericColumnValue for the fully qualified message name fqn.
func ProtoValue(fqn string, b []byte) spanner.GenericColumnValue {
	return BytesBasedValueOf(typector.FQNToProtoType(fqn), b)
}

// EnumValue returns a non-null ENUM GenericColumnValue for the fully qualified enum name fqn.
func EnumValue(fqn string, v int64) spanner.GenericColumnValue {
	return spanner.GenericColumnValue{
		Type:  typector.FQNToEnumType(fqn),
		Value: structpb.NewStringValue(strconv.FormatInt(v, 10)),
	}
}

// ArrayValue constructs ARRAY GenericColumnValue.
//
// With no elements (including a nil or empty variadic slice), it returns an empty ARRAY<INT64>
// (SQL length zero, not SQL NULL), using a concrete element type so the Type field is a well-formed
// [cloud.google.com/go/spanner/apiv1/spannerpb.Type] (including array_element_type for ARRAY shapes).
// For a typed NULL ARRAY<INT64>, use [NullOf] with
// [github.com/apstndb/spantype/typector.ElemCodeToArrayType] (or [github.com/apstndb/spantype/typector.ElemTypeToArrayType]).
//
// For other element types or explicit typing policy, use [ArrayValueOf] or [EmptyArrayOf].
//
// Note: Currently, it doesn't support implicit type conversion a.k.a. coercion so variant typed input is not supported.
func ArrayValue(vs ...spanner.GenericColumnValue) (spanner.GenericColumnValue, error) {
	if len(vs) == 0 {
		return EmptyArrayFromCode(sppb.TypeCode_INT64), nil
	}
	return ArrayValueOf(vs[0].Type, vs...)
}

// ArrayValueOf constructs ARRAY GenericColumnValue using elemType as the element type
// instead of inferring it from the first element. When elems is empty (nil or length zero), it
// returns an empty ARRAY<elemType> (SQL length zero, not SQL NULL). For a typed NULL ARRAY<elemType>,
// use [NullOf] with [github.com/apstndb/spantype/typector.ElemTypeToArrayType] or [github.com/apstndb/spantype/typector.ElemCodeToArrayType].
//
// Each element's Type must match elemType (no coercion). A nil elemType returns [ErrNilElementType].
func ArrayValueOf(elemType *sppb.Type, elems ...spanner.GenericColumnValue) (spanner.GenericColumnValue, error) {
	if elemType == nil {
		return spanner.GenericColumnValue{}, ErrNilElementType
	}
	if len(elems) == 0 {
		return EmptyArrayOf(elemType), nil
	}
	values := make([]*structpb.Value, len(elems))
	for i, v := range elems {
		if !proto.Equal(elemType, v.Type) {
			return spanner.GenericColumnValue{}, fmt.Errorf("%w: element %d: %v is not %v", ErrTypeMismatch, i, spantype.FormatTypeMoreVerbose(v.Type), spantype.FormatTypeMoreVerbose(elemType))
		}
		values[i] = v.Value
	}
	return spanner.GenericColumnValue{
		Type:  typector.ElemTypeToArrayType(elemType),
		Value: structpb.NewListValue(&structpb.ListValue{Values: values}),
	}, nil
}

// StructValueOf constructs STRUCT GenericColumnValue.
// Note: Currently, it doesn't support implicit type conversion a.k.a. coercion so variant typed input is not supported.
func StructValueOf(names []string, gcvs []spanner.GenericColumnValue) (spanner.GenericColumnValue, error) {
	if len(names) != len(gcvs) {
		return spanner.GenericColumnValue{}, fmt.Errorf("%w: len(names)=%v != len(gcvs)=%v", ErrMismatchedCounts, len(names), len(gcvs))
	}

	var types []*sppb.Type
	var values []*structpb.Value
	for _, gcv := range gcvs {
		types = append(types, gcv.Type)
		values = append(values, gcv.Value)
	}

	typ, err := typector.NameTypeSlicesToStructType(names, types)
	if err != nil {
		return spanner.GenericColumnValue{}, err
	}

	return spanner.GenericColumnValue{
		Type:  typ,
		Value: structpb.NewListValue(&structpb.ListValue{Values: values}),
	}, nil
}

// NullFromCode returns a typed SQL NULL for a simple scalar type code.
// The [cloud.google.com/go/spanner.GenericColumnValue] Value field is always a protobuf
// NullValue; see [NullOf] for STRUCT and ARRAY semantics.
func NullFromCode(code sppb.TypeCode) spanner.GenericColumnValue {
	return spanner.GenericColumnValue{
		Type:  typector.CodeToSimpleType(code),
		Value: structpb.NewNullValue(),
	}
}

// NullOf returns a typed SQL NULL for typ.
// The [cloud.google.com/go/spanner.GenericColumnValue] Value field is always a protobuf
// NullValue, including when typ is STRUCT or ARRAY.
// It does not represent a non-null STRUCT whose fields are all null—use [StructValueOf] with
// per-field nulls (using [NullOf] or [NullFromCode] for each field) when you need that shape.
func NullOf(typ *sppb.Type) spanner.GenericColumnValue {
	return spanner.GenericColumnValue{
		Type:  typ,
		Value: structpb.NewNullValue(),
	}
}

// NullArrayOf returns a typed SQL NULL for ARRAY<elemType>.
func NullArrayOf(elemType *sppb.Type) spanner.GenericColumnValue {
	return NullOf(typector.ElemTypeToArrayType(elemType))
}

// NullArrayFromCode returns a typed SQL NULL for ARRAY<T> where T is a simple scalar type code.
func NullArrayFromCode(elemCode sppb.TypeCode) spanner.GenericColumnValue {
	return NullOf(typector.ElemCodeToArrayType(elemCode))
}

// EmptyArrayOf returns a non-null empty ARRAY<elemType> (length zero).
func EmptyArrayOf(elemType *sppb.Type) spanner.GenericColumnValue {
	return spanner.GenericColumnValue{
		Type:  typector.ElemTypeToArrayType(elemType),
		Value: structpb.NewListValue(&structpb.ListValue{}),
	}
}

// EmptyArrayFromCode returns a non-null empty ARRAY<T> for a simple scalar element type code.
func EmptyArrayFromCode(code sppb.TypeCode) spanner.GenericColumnValue {
	return EmptyArrayOf(typector.CodeToSimpleType(code))
}
