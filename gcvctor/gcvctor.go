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
	"github.com/apstndb/spanvalue/internal"
	"google.golang.org/protobuf/types/known/structpb"
)

var (
	// ErrTypeMismatch is returned by [ArrayValueOf] when an element's type does not match elemType.
	ErrTypeMismatch = errors.New("gcvctor: type mismatch")
	// ErrMismatchedCounts is returned by [StructValueOf] when len(names) != len(gcvs).
	ErrMismatchedCounts = errors.New("gcvctor: mismatched name/value count")
	// ErrNilElementType is returned by [ArrayValueOf] when elemType is nil.
	ErrNilElementType = errors.New("gcvctor: nil array element type")
	// ErrNilFieldType is returned by [StructValueOf] when a field's Type is nil.
	ErrNilFieldType = errors.New("gcvctor: nil struct field type")
	// ErrNilNumeric is returned by [NumericValueChecked] and [PGNumericValueChecked] when v is nil.
	ErrNilNumeric = errors.New("gcvctor: nil numeric input")
)

// ArrayElementError adds an element index to an ARRAY construction error while preserving
// the wrapped cause for [errors.Is] and [errors.As].
type ArrayElementError struct {
	Index int
	Err   error
}

func (e *ArrayElementError) Error() string {
	return fmt.Sprintf("array element %d: %v", e.Index, e.Err)
}

func (e *ArrayElementError) Unwrap() error {
	return e.Err
}

// StructFieldError adds a field index (and optional field name) to a STRUCT construction
// error while preserving the wrapped cause for [errors.Is] and [errors.As].
type StructFieldError struct {
	Index int
	Name  string
	Err   error
}

func (e *StructFieldError) Error() string {
	if e.Name == "" {
		return fmt.Sprintf("struct field %d: %v", e.Index, e.Err)
	}
	return fmt.Sprintf("struct field %d (%q): %v", e.Index, e.Name, e.Err)
}

func (e *StructFieldError) Unwrap() error {
	return e.Err
}

func wrapArrayElementError(index int, err error) error {
	if err == nil {
		return nil
	}
	return &ArrayElementError{Index: index, Err: err}
}

func wrapStructFieldError(index int, name string, err error) error {
	if err == nil {
		return nil
	}
	return &StructFieldError{Index: index, Name: name, Err: err}
}

func normalizeNilType(typ *sppb.Type) *sppb.Type {
	if typ != nil {
		return typ
	}
	return typector.CodeToSimpleType(sppb.TypeCode_TYPE_CODE_UNSPECIFIED)
}

func normalizeNilArrayType(elemType *sppb.Type) *sppb.Type {
	return typector.ElemTypeToArrayType(normalizeNilType(elemType))
}

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
//
// For [sppb.TypeCode_NUMERIC] and NUMERIC with [sppb.TypeAnnotationCode_PG_NUMERIC], v must
// already be the canonical wire string Spanner uses (for GoogleSQL NUMERIC, the same form as
// [cloud.google.com/go/spanner.NumericString] on a [*big.Rat]). spanvalue formatters read the
// wire string as-is and do not re-normalize. Prefer [NumericValue], [PGNumericValue], or values
// from the Spanner client (including the emulator and Spanner Omni) over passing arbitrary
// decimals here.
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

// DateStringValue validates an RFC3339 full-date string and returns a non-null DATE
// GenericColumnValue using the canonical DATE wire string.
func DateStringValue(v string) (spanner.GenericColumnValue, error) {
	d, err := civil.ParseDate(v)
	if err != nil {
		return spanner.GenericColumnValue{}, err
	}
	return DateValue(d), nil
}

// TimestampValue returns a non-null TIMESTAMP GenericColumnValue (RFC3339Nano string wire format).
func TimestampValue(v time.Time) spanner.GenericColumnValue {
	return StringBasedValueFromCode(sppb.TypeCode_TIMESTAMP, v.Format(time.RFC3339Nano))
}

// TimestampStringValue validates an RFC3339Nano timestamp string and returns a non-null
// TIMESTAMP GenericColumnValue using the canonical UTC wire string.
func TimestampStringValue(v string) (spanner.GenericColumnValue, error) {
	ts, err := time.Parse(time.RFC3339Nano, v)
	if err != nil {
		return spanner.GenericColumnValue{}, err
	}
	return TimestampValue(ts.UTC()), nil
}

// NumericValue returns a NUMERIC GenericColumnValue with a canonical wire string from
// [cloud.google.com/go/spanner.NumericString].
// A nil v returns a typed SQL NULL NUMERIC for backward compatibility; use
// [NumericValueChecked] to reject nil input with [ErrNilNumeric].
func NumericValue(v *big.Rat) spanner.GenericColumnValue {
	if v == nil {
		return NullFromCode(sppb.TypeCode_NUMERIC)
	}
	return StringBasedValueFromCode(sppb.TypeCode_NUMERIC, spanner.NumericString(v))
}

// NumericValueChecked returns a non-null NUMERIC GenericColumnValue.
// A nil v returns [ErrNilNumeric].
func NumericValueChecked(v *big.Rat) (spanner.GenericColumnValue, error) {
	if v == nil {
		return spanner.GenericColumnValue{}, ErrNilNumeric
	}
	return NumericValue(v), nil
}

// IntervalValue returns a non-null INTERVAL GenericColumnValue.
func IntervalValue(v spanner.Interval) spanner.GenericColumnValue {
	return StringBasedValueFromCode(sppb.TypeCode_INTERVAL, v.String())
}

// IntervalStringValue validates an ISO8601 duration string and returns a non-null
// INTERVAL GenericColumnValue using spanner.Interval's canonical wire string.
func IntervalStringValue(v string) (spanner.GenericColumnValue, error) {
	iv, err := spanner.ParseInterval(v)
	if err != nil {
		return spanner.GenericColumnValue{}, err
	}
	return IntervalValue(iv), nil
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

// PGNumericValue returns a PostgreSQL-dialect NUMERIC GenericColumnValue
// ([sppb.TypeAnnotationCode_PG_NUMERIC]) with a canonical wire string from
// [cloud.google.com/go/spanner.NumericString].
// A nil v returns a typed SQL NULL PG NUMERIC for backward compatibility; use
// [PGNumericValueChecked] to reject nil input with [ErrNilNumeric].
func PGNumericValue(v *big.Rat) spanner.GenericColumnValue {
	if v == nil {
		return NullOf(typector.PGNumeric())
	}
	return spanner.GenericColumnValue{
		Type:  typector.PGNumeric(),
		Value: structpb.NewStringValue(spanner.NumericString(v)),
	}
}

// PGNumericValueChecked returns a non-null PostgreSQL-dialect NUMERIC GenericColumnValue
// ([sppb.TypeAnnotationCode_PG_NUMERIC]).
// A nil v returns [ErrNilNumeric].
func PGNumericValueChecked(v *big.Rat) (spanner.GenericColumnValue, error) {
	if v == nil {
		return spanner.GenericColumnValue{}, ErrNilNumeric
	}
	return PGNumericValue(v), nil
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
// The message bytes are stored in the GCV as a base64-encoded string. Delimited export decodes that
// wire payload for SimpleFormatConfig when possible (see writer.TestDelimitedWriterWriteGCVsEnumProto).
func ProtoValue(fqn string, b []byte) spanner.GenericColumnValue {
	return BytesBasedValueOf(typector.FQNToProtoType(fqn), b)
}

// EnumValue returns a non-null ENUM GenericColumnValue for the fully qualified enum name fqn.
// The structpb value is the enum number as a decimal string; delimited export prints that
// string (see writer.TestDelimitedWriterWriteGCVsEnumProto).
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
// If the inferred element type from vs[0] is invalid, the error is wrapped in [ArrayElementError]
// with Index 0.
func ArrayValue(vs ...spanner.GenericColumnValue) (spanner.GenericColumnValue, error) {
	if len(vs) == 0 {
		return EmptyArrayFromCode(sppb.TypeCode_INT64), nil
	}
	if vs[0].Type == nil {
		return spanner.GenericColumnValue{}, wrapArrayElementError(0, ErrNilElementType)
	}
	return ArrayValueOf(vs[0].Type, vs...)
}

// NormalizeArrayElements rewrites SQL NULL elements to [NullOf] with elemType while preserving
// strict type checks for non-NULL elements. A nil elemType returns [ErrNilElementType].
// Per-element failures are wrapped in [ArrayElementError].
func NormalizeArrayElements(elemType *sppb.Type, elems ...spanner.GenericColumnValue) ([]spanner.GenericColumnValue, error) {
	if elemType == nil {
		return nil, ErrNilElementType
	}
	normalized := make([]spanner.GenericColumnValue, len(elems))
	for i, elem := range elems {
		if internal.IsNullGenericColumnValue(elem) {
			normalized[i] = NullOf(elemType)
			continue
		}
		if elem.Type == nil {
			return nil, wrapArrayElementError(i, ErrNilElementType)
		}
		if !proto.Equal(elemType, elem.Type) {
			return nil, wrapArrayElementError(i, fmt.Errorf("%w: %v is not %v", ErrTypeMismatch, spantype.FormatTypeMoreVerbose(elem.Type), spantype.FormatTypeMoreVerbose(elemType)))
		}
		normalized[i] = elem
	}
	return normalized, nil
}

// ArrayValueOf constructs ARRAY GenericColumnValue using elemType as the element type
// instead of inferring it from the first element. When elems is empty (nil or length zero), it
// returns an empty ARRAY<elemType> (SQL length zero, not SQL NULL). For a typed NULL ARRAY<elemType>,
// use [NullOf] with [github.com/apstndb/spantype/typector.ElemTypeToArrayType] or [github.com/apstndb/spantype/typector.ElemCodeToArrayType].
//
// Each element's Type must match elemType (no coercion). A nil elemType returns [ErrNilElementType].
// Per-element failures are wrapped in [ArrayElementError]. To accept SQL NULL elements regardless of
// their current Type metadata, normalize them first with [NormalizeArrayElements].
func ArrayValueOf(elemType *sppb.Type, elems ...spanner.GenericColumnValue) (spanner.GenericColumnValue, error) {
	if elemType == nil {
		return spanner.GenericColumnValue{}, ErrNilElementType
	}
	if len(elems) == 0 {
		return EmptyArrayOf(elemType), nil
	}
	values := make([]*structpb.Value, len(elems))
	for i, v := range elems {
		if v.Type == nil {
			return spanner.GenericColumnValue{}, wrapArrayElementError(i, ErrNilElementType)
		}
		if !proto.Equal(elemType, v.Type) {
			return spanner.GenericColumnValue{}, wrapArrayElementError(i, fmt.Errorf("%w: %v is not %v", ErrTypeMismatch, spantype.FormatTypeMoreVerbose(v.Type), spantype.FormatTypeMoreVerbose(elemType)))
		}
		values[i] = v.Value
	}
	return spanner.GenericColumnValue{
		Type:  typector.ElemTypeToArrayType(elemType),
		Value: structpb.NewListValue(&structpb.ListValue{Values: values}),
	}, nil
}

// StructField pairs one STRUCT field name with its GCV.
// An empty Name is valid for unnamed STRUCT fields; see [StructValueOfFields].
// Prefer [StructFieldKV] at call sites for brevity; composite literals remain valid when
// field names or values need extra clarity.
type StructField struct {
	Name  string
	Value spanner.GenericColumnValue
}

// StructFieldKV returns a [StructField] with the given name and value.
// Empty name is valid for unnamed STRUCT fields.
func StructFieldKV(name string, value spanner.GenericColumnValue) StructField {
	return StructField{Name: name, Value: value}
}

// StructValueOfFields is like [StructValueOf] but takes paired fields.
// Empty field names are valid for unnamed STRUCT fields.
func StructValueOfFields(fields ...StructField) (spanner.GenericColumnValue, error) {
	names := make([]string, len(fields))
	gcvs := make([]spanner.GenericColumnValue, len(fields))
	for i, f := range fields {
		names[i] = f.Name
		gcvs[i] = f.Value
	}
	return StructValueOf(names, gcvs)
}

// StructValueOf constructs STRUCT GenericColumnValue.
// A nil field Type returns [ErrNilFieldType] wrapped in [StructFieldError].
// Note: Currently, it doesn't support implicit type conversion a.k.a. coercion so variant typed input is not supported.
func StructValueOf(names []string, gcvs []spanner.GenericColumnValue) (spanner.GenericColumnValue, error) {
	if len(names) != len(gcvs) {
		return spanner.GenericColumnValue{}, fmt.Errorf("%w: len(names)=%v != len(gcvs)=%v", ErrMismatchedCounts, len(names), len(gcvs))
	}

	types := make([]*sppb.Type, len(gcvs))
	values := make([]*structpb.Value, len(gcvs))
	for i, gcv := range gcvs {
		if gcv.Type == nil {
			return spanner.GenericColumnValue{}, wrapStructFieldError(i, names[i], ErrNilFieldType)
		}
		types[i] = gcv.Type
		values[i] = gcv.Value
	}

	typ, err := typector.NameTypeSlicesToStructType(names, types)
	if err != nil {
		return spanner.GenericColumnValue{}, fmt.Errorf("gcvctor: build struct type: %w", err)
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
// A nil typ is normalized to TYPE_CODE_UNSPECIFIED to avoid a malformed nil Type pointer.
func NullOf(typ *sppb.Type) spanner.GenericColumnValue {
	return spanner.GenericColumnValue{
		Type:  normalizeNilType(typ),
		Value: structpb.NewNullValue(),
	}
}

// NullArrayOf returns a typed SQL NULL for ARRAY<elemType>.
// A nil elemType is normalized to TYPE_CODE_UNSPECIFIED, so NullArrayOf(nil)
// returns a NULL ARRAY<TYPE_CODE_UNSPECIFIED> rather than an invalid ARRAY shape.
func NullArrayOf(elemType *sppb.Type) spanner.GenericColumnValue {
	return NullOf(normalizeNilArrayType(elemType))
}

// NullArrayFromCode returns a typed SQL NULL for ARRAY<T> where T is a simple scalar type code.
func NullArrayFromCode(elemCode sppb.TypeCode) spanner.GenericColumnValue {
	return NullOf(typector.ElemCodeToArrayType(elemCode))
}

// EmptyArrayOf returns a non-null empty ARRAY<elemType> (length zero).
// A nil elemType is normalized to TYPE_CODE_UNSPECIFIED, so EmptyArrayOf(nil)
// returns an empty ARRAY<TYPE_CODE_UNSPECIFIED> rather than an invalid ARRAY shape.
func EmptyArrayOf(elemType *sppb.Type) spanner.GenericColumnValue {
	return spanner.GenericColumnValue{
		Type:  normalizeNilArrayType(elemType),
		Value: structpb.NewListValue(&structpb.ListValue{}),
	}
}

// EmptyArrayFromCode returns a non-null empty ARRAY<T> for a simple scalar element type code.
func EmptyArrayFromCode(code sppb.TypeCode) spanner.GenericColumnValue {
	return EmptyArrayOf(typector.CodeToSimpleType(code))
}
