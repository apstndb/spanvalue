package spanvalue

import (
	"encoding/base64"
	"errors"
	"fmt"
	"slices"
	"strconv"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/samber/lo"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/apstndb/spanvalue/internal"
)

var (
	ErrNilRow         = errors.New("nil row")
	ErrNilStructField = errors.New("nil struct field descriptor")
	// ErrUnknownType is returned when a type code (or, on the Decode path, a Go
	// value type) is not supported by the formatter. It signals a
	// configuration/coverage problem: the value may become formattable by adding
	// a [FormatComplexFunc] plugin or choosing a different preset. For known
	// types whose wire payload is invalid, see [ErrMalformedWire].
	ErrUnknownType = errors.New("unknown type")
	// ErrMalformedWire is returned when the type code of a value is known but
	// its wire payload does not match the encoding Spanner uses for that type —
	// for example a BOOL whose [structpb.Value] kind is a string, a FLOAT64
	// string other than "NaN"/"Infinity"/"-Infinity", or a NULL that
	// unexpectedly reaches the scalar wire validator. Unlike [ErrUnknownType]
	// (a configuration problem that a plugin or preset change can address),
	// ErrMalformedWire means the [cloud.google.com/go/spanner.GenericColumnValue]
	// itself is corrupt: consumers should treat it as a data problem and fail
	// the export rather than reconfigure formatting. It does not match
	// [ErrUnknownType] via [errors.Is].
	ErrMalformedWire              = errors.New("malformed wire value")
	ErrMismatchedFields           = errors.New("mismatched struct value/field count")
	ErrUnexpectedComplexValueKind = errors.New("unexpected complex value kind")
	ErrEmptyTypeFQN               = errors.New("empty type FQN")
	// ErrNilFormatConfig is returned by [*FormatConfig.Validate] when the receiver is nil.
	ErrNilFormatConfig = errors.New("nil format config")
	// ErrEmptyNullString is returned by [*FormatConfig.Validate] when [FormatConfig.NullString] is empty.
	ErrEmptyNullString = errors.New("empty null string")
	// ErrNilFormatComplexPlugin is returned by [*FormatConfig.Validate] when
	// [FormatConfig.FormatComplexPlugins] contains a nil element.
	ErrNilFormatComplexPlugin = errors.New("nil format complex plugin")
	// ErrEmptyFormatComplexPlugins is returned by [*FormatConfig.Validate] when
	// [FormatConfig.FormatComplexPlugins] is empty: with no plugins, every
	// non-NULL value fails with [ErrUnhandledValue], so an empty chain is
	// treated as a construction mistake.
	ErrEmptyFormatComplexPlugins = errors.New("empty format complex plugins")
	// ErrUnhandledValue is returned by [*FormatConfig.FormatColumn] when every
	// plugin in [FormatConfig.FormatComplexPlugins] defers ([ErrFallthrough])
	// for a non-NULL value. The wrapped message includes the value's
	// [sppb.Type]. It signals a coverage problem in the chain: register a
	// plugin that claims the value (for example [PluginForArray],
	// [PluginForStruct], or [PluginFromNullable]) or choose a preset that
	// covers it. NULL values never reach this error; they render as
	// [FormatConfig.NullString] when no plugin claims them.
	//
	// ErrUnhandledValue replaces the pre-v0.8 built-in fallbacks: the
	// ErrFormatNullableRequired error for scalars, the nil FormatArray /
	// FormatStruct callback panics, and the built-in path's ErrUnknownType
	// for unknown scalar type codes.
	ErrUnhandledValue = errors.New("no plugin handled value")
)

const (
	nullStringUpperCase = "NULL"
	nullStringClientLib = "<null>"
)

// NullableValue is the scalar null wrapper type accepted by [FormatNullableFunc].
// It includes [cloud.google.com/go/spanner] null types and [NullBytes] for BYTES/PROTO.
type NullableValue interface {
	spanner.NullableValue
	fmt.Stringer
}

type NullBytes []byte

func (n NullBytes) IsNull() bool {
	return n == nil
}

func (n NullBytes) String() string {
	if n == nil {
		return nullStringClientLib
	}
	return internal.ReadableBytesString(n)
}

var _, _ NullableValue = (NullBytes)(nil), (*NullBytes)(nil)
var _, _ NullableValue = spanner.NullString{}, (*spanner.NullString)(nil)
var _, _ NullableValue = spanner.NullDate{}, (*spanner.NullDate)(nil)
var _, _ NullableValue = spanner.PGNumeric{}, (*spanner.PGNumeric)(nil)
var _, _ NullableValue = spanner.PGJsonB{}, (*spanner.PGJsonB)(nil)

// FormatComplexFunc formats one [cloud.google.com/go/spanner.GenericColumnValue]
// as an element of [FormatConfig.FormatComplexPlugins]. Returning
// [ErrFallthrough] defers the value to the next plugin in the chain (and, when
// every plugin defers, to the built-in NULL handling or [ErrUnhandledValue]).
type FormatComplexFunc func(formatter Formatter, value spanner.GenericColumnValue, toplevel bool) (string, error)

// ErrFallthrough tells [FormatComplexFunc] plugins to defer to the next plugin or built-in path.
var ErrFallthrough = errors.New("fallthrough")

func typeValueToGCV(typ *sppb.Type, value *structpb.Value) spanner.GenericColumnValue {
	return spanner.GenericColumnValue{Type: typ, Value: value}
}

func simpleGCVToNullable(value spanner.GenericColumnValue) (NullableValue, error) {
	switch value.Type.GetCode() {
	case sppb.TypeCode_BOOL:
		return decodeScalar[spanner.NullBool](value)
	case sppb.TypeCode_INT64, sppb.TypeCode_ENUM:
		return decodeScalar[spanner.NullInt64](value)
	case sppb.TypeCode_FLOAT32:
		return decodeScalar[spanner.NullFloat32](value)
	case sppb.TypeCode_FLOAT64:
		return decodeScalar[spanner.NullFloat64](value)
	case sppb.TypeCode_TIMESTAMP:
		return decodeScalar[spanner.NullTime](value)
	case sppb.TypeCode_DATE:
		return decodeScalar[spanner.NullDate](value)
	case sppb.TypeCode_STRING:
		return decodeScalar[spanner.NullString](value)
	case sppb.TypeCode_BYTES, sppb.TypeCode_PROTO:
		return decodeScalar[NullBytes](value)
	case sppb.TypeCode_NUMERIC:
		if value.Type.GetTypeAnnotation() == sppb.TypeAnnotationCode_PG_NUMERIC {
			return decodeScalar[spanner.PGNumeric](value)
		}
		return decodeScalar[spanner.NullNumeric](value)
	case sppb.TypeCode_JSON:
		if value.Type.GetTypeAnnotation() == sppb.TypeAnnotationCode_PG_JSONB {
			return decodeScalar[spanner.PGJsonB](value)
		}
		return decodeScalar[spanner.NullJSON](value)
	case sppb.TypeCode_INTERVAL:
		return decodeScalar[spanner.NullInterval](value)
	case sppb.TypeCode_UUID:
		return decodeScalar[spanner.NullUUID](value)
	case sppb.TypeCode_TYPE_CODE_UNSPECIFIED:
		fallthrough
	default:
		return nil, fmt.Errorf("%w: %v", ErrUnknownType, value.Type.String())
	}
}

func decodeScalar[T NullableValue](gcv spanner.GenericColumnValue) (T, error) {
	var v T
	err := gcv.Decode(&v)
	return v, err
}

func isComplexType(elemCode sppb.TypeCode) bool {
	return sppb.TypeCode_STRUCT == elemCode || sppb.TypeCode_ARRAY == elemCode
}

var (
	_ FormatComplexFunc = FormatProtoAsCast
	_ FormatComplexFunc = FormatEnumAsCast
)

// IsNull reports whether gcv represents a NULL value.
// A nil gcv.Value is treated as NULL.
func IsNull(gcv spanner.GenericColumnValue) bool {
	return internal.IsNullGenericColumnValue(gcv)
}

// FormatProtoAsCast formats PROTO values as CAST(b"..." AS `fqn`) with the
// default (legacy double-quote) bytes-literal quoting. The literal preset
// constructors install a quote-aware equivalent that follows the
// constructor-captured [LiteralQuoteConfig] ([LiteralFormatConfigWithQuote]
// and friends), so quote options apply to PROTO casts only through those
// constructors.
func FormatProtoAsCast(formatter Formatter, value spanner.GenericColumnValue, toplevel bool) (string, error) {
	return formatProtoAsCast(LiteralQuoteConfig{}, formatter, value)
}

// protoAsCastPlugin returns a [FormatProtoAsCast] equivalent whose bytes
// literal quoting follows q, captured at construction.
func protoAsCastPlugin(q LiteralQuoteConfig) FormatComplexFunc {
	q = normalizeLiteralQuote(q)
	return func(formatter Formatter, value spanner.GenericColumnValue, _ bool) (string, error) {
		return formatProtoAsCast(q, formatter, value)
	}
}

func formatProtoAsCast(q LiteralQuoteConfig, formatter Formatter, value spanner.GenericColumnValue) (string, error) {
	if value.Type.GetCode() != sppb.TypeCode_PROTO {
		return "", ErrFallthrough
	}

	if IsNull(value) {
		return formatter.GetNullString(), nil
	}
	if err := requireStringWire(value.Value, sppb.TypeCode_PROTO); err != nil {
		return "", err
	}

	b, err := base64.StdEncoding.DecodeString(value.Value.GetStringValue())
	if err != nil {
		return "", err
	}
	typeFQN, err := requireTypeFQN(value.Type)
	if err != nil {
		return "", err
	}
	policy := toInternalQuotePolicy(q)
	return fmt.Sprintf("CAST(%v AS `%v`)", internal.ToReadableBytesLiteralPolicy(b, policy), typeFQN), nil
}

func FormatEnumAsCast(formatter Formatter, value spanner.GenericColumnValue, toplevel bool) (string, error) {
	if value.Type.GetCode() != sppb.TypeCode_ENUM {
		return "", ErrFallthrough
	}

	if IsNull(value) {
		return formatter.GetNullString(), nil
	}
	if err := requireStringWire(value.Value, sppb.TypeCode_ENUM); err != nil {
		return "", err
	}

	s := value.Value.GetStringValue()
	if _, err := strconv.ParseInt(s, 10, 64); err != nil {
		return "", fmt.Errorf("failed to parse enum wire payload %q: %w", s, err)
	}
	typeFQN, err := requireTypeFQN(value.Type)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("CAST(%v AS `%v`)", s, typeFQN), nil
}

// Formatter is the minimal surface [FormatComplexFunc] plugins use to recurse into nested values.
type Formatter interface {
	FormatColumn(value spanner.GenericColumnValue, toplevel bool) (string, error)
	GetNullString() string
}

// FormatConfig controls how Spanner values are formatted. Behavior lives
// entirely in the two fields: NullString (the global NULL rendering) and
// FormatComplexPlugins (the ordered [FormatComplexFunc] chain).
//
// [*FormatConfig.FormatColumn] tries every plugin in order; a plugin returns
// [ErrFallthrough] to defer. When every plugin defers, NULL values (of any
// type) render as NullString and non-NULL values fail with
// [ErrUnhandledValue]. Coverage is therefore a property of the chain: preset
// constructors ([SimpleFormatConfig], [LiteralFormatConfig],
// [SpannerCLICompatibleFormatConfig], [JSONFormatConfig]) install a scalar
// plugin plus [PluginForArray] and [PluginForStruct] handlers, and
// [NewFormatConfig] assembles a chain from the same combinators with
// build-time validation.
//
// Use [*FormatConfig.Clone] or [*FormatConfig.WithComplexPlugin] (prepends
// plugins, so the most recent addition runs first) to customize a preset
// without mutating shared instances. Call [*FormatConfig.Validate] after
// hand-assembling a config to fail fast on an empty NullString, an empty
// chain, or nil plugins; Validate cannot prove that the chain covers every
// type — coverage gaps surface at format time as [ErrUnhandledValue].
type FormatConfig struct {
	NullString           string
	FormatComplexPlugins []FormatComplexFunc
}

func (fc *FormatConfig) GetNullString() string { return fc.NullString }

type FormatArrayFunc func(typ *sppb.Type, toplevel bool, elemStrings []string) (string, error)
type FormatStructParenFunc func(typ *sppb.Type, toplevel bool, fieldStrings []string) (string, error)

// FormatStructFieldFunc formats one STRUCT field value for [PluginForStruct]
// and [WithStructFormat]. Use formatter.FormatColumn(fieldGCV, false) to
// recurse into the field value through the whole plugin chain.
type FormatStructFieldFunc func(formatter Formatter, field *sppb.StructType_Field, value *structpb.Value) (string, error)

// FormatNullableFunc formats one non-NULL scalar value decoded to its
// [NullableValue] wrapper. Lift it into the plugin chain with
// [PluginFromNullable] (or [WithScalarFormatter] on [NewFormatConfig]).
type FormatNullableFunc func(value NullableValue) (string, error)

func (fc *FormatConfig) FormatColumn(value spanner.GenericColumnValue, toplevel bool) (string, error) {
	// Plugins are tried first so they can handle any type including ARRAY and
	// STRUCT. NULL values are intentionally passed to plugins (not pre-filtered)
	// so that plugins can produce type-specific NULL representations via
	// formatter.GetNullString() or their own logic.
	// Plugins that don't need type-specific NULL handling should check IsNull
	// early and return.
	for _, f := range fc.FormatComplexPlugins {
		if s, err := f(fc, value, toplevel); !errors.Is(err, ErrFallthrough) {
			return s, err
		}
	}
	if IsNull(value) {
		return fc.GetNullString(), nil
	}
	return "", fmt.Errorf("%w: %v", ErrUnhandledValue, value.Type)
}

// formatArrayElems is the non-NULL ARRAY shape behind [PluginForArray]:
// extract the wire list value (non-list payloads are
// [ErrUnexpectedComplexValueKind]), recurse into each element with
// formatter.FormatColumn(elem, false), and hand the element strings to join.
func formatArrayElems(formatter Formatter, value spanner.GenericColumnValue, toplevel bool, join FormatArrayFunc) (string, error) {
	listValue, err := getComplexListValue(sppb.TypeCode_ARRAY, value.Value)
	if err != nil {
		return "", err
	}
	elemStrings, err := lo.MapErr(listValue.GetValues(), func(v *structpb.Value, _ int) (string, error) {
		return formatter.FormatColumn(typeValueToGCV(value.Type.GetArrayElementType(), v), false)
	})
	if err != nil {
		return "", err
	}
	return join(value.Type, toplevel, elemStrings)
}

// formatStructFields is the non-NULL STRUCT shape behind [PluginForStruct]:
// extract the wire list value (non-list payloads are
// [ErrUnexpectedComplexValueKind]), check the value count against the field
// descriptors ([ErrMismatchedFields]), format each field with the field
// callback, and hand the field strings to paren.
func formatStructFields(formatter Formatter, value spanner.GenericColumnValue, toplevel bool, field FormatStructFieldFunc, paren FormatStructParenFunc) (string, error) {
	listValue, err := getComplexListValue(sppb.TypeCode_STRUCT, value.Value)
	if err != nil {
		return "", err
	}
	fields := value.Type.GetStructType().GetFields()
	fieldValues := listValue.GetValues()
	if len(fieldValues) != len(fields) {
		return "", fmt.Errorf("%w: got %d values, want %d", ErrMismatchedFields, len(fieldValues), len(fields))
	}
	fieldStrings, err := lo.MapErr(fields, func(f *sppb.StructType_Field, i int) (string, error) {
		return field(formatter, f, fieldValues[i])
	})
	if err != nil {
		return "", err
	}
	return paren(value.Type, toplevel, fieldStrings)
}

func getComplexListValue(code sppb.TypeCode, value *structpb.Value) (*structpb.ListValue, error) {
	listValue, ok := value.GetKind().(*structpb.Value_ListValue)
	if !ok {
		return nil, fmt.Errorf("%w for %s: got %T, want list value", ErrUnexpectedComplexValueKind, code, value.GetKind())
	}
	return listValue.ListValue, nil
}

func requireTypeFQN(typ *sppb.Type) (string, error) {
	if fqn := typ.GetProtoTypeFqn(); fqn != "" {
		return fqn, nil
	}
	return "", fmt.Errorf("%w for %s", ErrEmptyTypeFQN, typ.GetCode())
}

func (fc *FormatConfig) FormatRow(row *spanner.Row) ([]string, error) {
	if row == nil {
		return nil, ErrNilRow
	}
	gcvs := make([]spanner.GenericColumnValue, row.Size())
	if err := row.Columns(slices.Collect(internal.ToAny(internal.Pointers(gcvs)))...); err != nil {
		return nil, err
	}
	return fc.formatColumns(gcvs)
}

func (fc *FormatConfig) FormatToplevelColumn(value spanner.GenericColumnValue) (string, error) {
	return fc.FormatColumn(value, true)
}
