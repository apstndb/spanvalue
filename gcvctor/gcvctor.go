package gcvctor

import (
	"encoding/base64"
	"encoding/json"
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
	ErrTypeMismatch     = fmt.Errorf("type mismatch")
	ErrMismatchedCounts = fmt.Errorf("mismatched name/value count")
)

func BoolValue(v bool) spanner.GenericColumnValue {
	return spanner.GenericColumnValue{
		Type:  typector.CodeToSimpleType(sppb.TypeCode_BOOL),
		Value: structpb.NewBoolValue(v),
	}
}

func Int64Value(v int64) spanner.GenericColumnValue {
	return spanner.GenericColumnValue{
		Type:  typector.CodeToSimpleType(sppb.TypeCode_INT64),
		Value: structpb.NewStringValue(strconv.FormatInt(v, 10)),
	}
}

func Float64Value(v float64) spanner.GenericColumnValue {
	return spanner.GenericColumnValue{
		Type:  typector.CodeToSimpleType(sppb.TypeCode_FLOAT64),
		Value: float64ToStructpbValue(v),
	}
}

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

func StringValue(v string) spanner.GenericColumnValue {
	return spanner.GenericColumnValue{
		Type:  typector.CodeToSimpleType(sppb.TypeCode_STRING),
		Value: structpb.NewStringValue(v),
	}
}

func BytesValue(v []byte) spanner.GenericColumnValue {
	return BytesBasedValue(typector.CodeToSimpleType(sppb.TypeCode_BYTES), v)
}

func BytesBasedValue(typ *sppb.Type, v []byte) spanner.GenericColumnValue {
	return spanner.GenericColumnValue{
		Type:  typ,
		Value: structpb.NewStringValue(base64.StdEncoding.EncodeToString(v)),
	}
}

func StringBasedValue(code sppb.TypeCode, v string) spanner.GenericColumnValue {
	return spanner.GenericColumnValue{
		Type:  typector.CodeToSimpleType(code),
		Value: structpb.NewStringValue(v),
	}
}

func DateValue(v civil.Date) spanner.GenericColumnValue {
	return StringBasedValue(sppb.TypeCode_DATE, v.String())
}

func TimestampValue(v time.Time) spanner.GenericColumnValue {
	return StringBasedValue(sppb.TypeCode_TIMESTAMP, v.Format(time.RFC3339Nano))
}

func NumericValue(v *big.Rat) spanner.GenericColumnValue {
	return StringBasedValue(sppb.TypeCode_NUMERIC, spanner.NumericString(v))
}

func IntervalValue(v spanner.Interval) spanner.GenericColumnValue {
	return StringBasedValue(sppb.TypeCode_INTERVAL, v.String())
}

func UUIDValue(v uuid.UUID) spanner.GenericColumnValue {
	return StringBasedValue(sppb.TypeCode_UUID, v.String())
}

func JSONValue(v any) (spanner.GenericColumnValue, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return spanner.GenericColumnValue{}, err
	}
	return StringBasedValue(sppb.TypeCode_JSON, string(b)), nil
}

func ProtoValue(fqn string, b []byte) spanner.GenericColumnValue {
	return BytesBasedValue(typector.FQNToProtoType(fqn), b)
}

func EnumValue(fqn string, v int64) spanner.GenericColumnValue {
	return spanner.GenericColumnValue{
		Type:  typector.FQNToEnumType(fqn),
		Value: structpb.NewStringValue(strconv.FormatInt(v, 10)),
	}
}

// ArrayValue constructs ARRAY GenericColumnValue.
// With no arguments it returns an empty ARRAY<INT64> (not a scalar NULL). For other
// element types or explicit typing policy, use ArrayValueWithType or ElemTypeToEmptyArray.
// Note: Currently, it doesn't support implicit type conversion a.k.a. coercion so variant typed input is not supported.
func ArrayValue(vs ...spanner.GenericColumnValue) (spanner.GenericColumnValue, error) {
	if len(vs) == 0 {
		return ElemTypeCodeToEmptyArray(sppb.TypeCode_INT64), nil
	}
	return ArrayValueWithType(vs[0].Type, vs...)
}

// ArrayValueWithType constructs ARRAY GenericColumnValue using elemType as the element type
// instead of inferring it from the first element. When elems is empty, it returns an empty
// ARRAY<elemType>. Each element's Type must match elemType (no coercion).
func ArrayValueWithType(elemType *sppb.Type, elems ...spanner.GenericColumnValue) (spanner.GenericColumnValue, error) {
	if elemType == nil {
		return spanner.GenericColumnValue{}, fmt.Errorf("nil element type")
	}
	if len(elems) == 0 {
		return ElemTypeToEmptyArray(elemType), nil
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

// StructValue constructs STRUCT GenericColumnValue.
// Note: Currently, it doesn't support implicit type conversion a.k.a. coercion so variant typed input is not supported.
func StructValue(names []string, gcvs []spanner.GenericColumnValue) (spanner.GenericColumnValue, error) {
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

func SimpleTypedNull(code sppb.TypeCode) spanner.GenericColumnValue {
	return spanner.GenericColumnValue{
		Type:  typector.CodeToSimpleType(code),
		Value: structpb.NewNullValue(),
	}
}

func TypedNull(typ *sppb.Type) spanner.GenericColumnValue {
	return spanner.GenericColumnValue{
		Type:  typ,
		Value: structpb.NewNullValue(),
	}
}

// ArrayTypeTypedNull constructs a NULL ARRAY with the given element type.
func ArrayTypeTypedNull(elemType *sppb.Type) spanner.GenericColumnValue {
	return TypedNull(typector.ElemTypeToArrayType(elemType))
}

// ArrayCodeTypedNull constructs a NULL ARRAY with a simple element type code.
func ArrayCodeTypedNull(elemCode sppb.TypeCode) spanner.GenericColumnValue {
	return TypedNull(typector.ElemCodeToArrayType(elemCode))
}

func ElemTypeToEmptyArray(typ *sppb.Type) spanner.GenericColumnValue {
	return spanner.GenericColumnValue{
		Type:  typector.ElemTypeToArrayType(typ),
		Value: structpb.NewListValue(&structpb.ListValue{}),
	}
}

func ElemTypeCodeToEmptyArray(code sppb.TypeCode) spanner.GenericColumnValue {
	return ElemTypeToEmptyArray(typector.CodeToSimpleType(code))
}
