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
	// ErrNilFormatArray is returned by [*FormatConfig.Validate] when [FormatConfig.FormatArray] is nil.
	ErrNilFormatArray = errors.New("nil format array callback")
	// ErrNilFormatStructField is returned by [*FormatConfig.Validate] when
	// [FormatStruct.FormatStructField] is nil.
	ErrNilFormatStructField = errors.New("nil format struct field callback")
	// ErrNilFormatStructParen is returned by [*FormatConfig.Validate] when
	// [FormatStruct.FormatStructParen] is nil.
	ErrNilFormatStructParen = errors.New("nil format struct paren callback")
	// ErrNilFormatComplexPlugin is returned by [*FormatConfig.Validate] when
	// [FormatConfig.FormatComplexPlugins] contains a nil element.
	ErrNilFormatComplexPlugin = errors.New("nil format complex plugin")
	// ErrFormatNullableRequired is returned from the scalar slow path when
	// the FormatNullable field is nil (no Decode-based formatting), and by
	// [*FormatConfig.Validate] when the FormatNullable field is nil and no preset scalar plugin is configured.
	ErrFormatNullableRequired = errors.New("format nullable required")
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

// FormatComplexFunc is a function to format spanner.GenericColumnValue.
// If it returns ErrFallthrough, value will pass through to next step.
type FormatComplexFunc = func(formatter Formatter, value spanner.GenericColumnValue, toplevel bool) (string, error)

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

func (fc *FormatConfig) formatSimpleColumn(value spanner.GenericColumnValue) (string, error) {
	if IsNull(value) {
		return fc.GetNullString(), nil
	}
	if fc.FormatNullable == nil {
		return "", ErrFormatNullableRequired
	}
	nv, err := simpleGCVToNullable(value)
	if err != nil {
		return "", err
	}
	if nullableFuncsEqual(fc.FormatNullable, formatNullableValueLiteral) {
		return formatNullableValueLiteralWithQuote(fc.Literal.Quote, nv)
	}
	return fc.FormatNullable(nv)
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

func FormatProtoAsCast(formatter Formatter, value spanner.GenericColumnValue, toplevel bool) (string, error) {
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
	policy := toInternalQuotePolicy(literalQuoteForFormatter(formatter))
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

// FormatConfig controls how Spanner values are formatted. Preset constructors
// such as [LiteralFormatConfig] return a fresh instance with non-nil callbacks
// for the value kinds they support. [*FormatConfig.FormatColumn] calls FormatArray,
// FormatStruct, and FormatNullable directly; a nil FormatNullable returns
// [ErrFormatNullableRequired] on the scalar slow path unless a [FormatComplexFunc]
// plugin handles the value first. Nil FormatArray or FormatStruct still panic when invoked.
//
// Nil field behavior:
//   - FormatArray: required for non-NULL ARRAY values. NULL ARRAY values use
//     [*FormatConfig.GetNullString] before FormatArray is called.
//   - FormatStruct.FormatStructField and FormatStruct.FormatStructParen: required
//     for non-NULL STRUCT values. NULL STRUCT values use [*FormatConfig.GetNullString]
//     before struct callbacks run.
//   - FormatComplexPlugins: nil or empty means no plugins run. Preset constructors
//     append a trailing scalar plugin ([FormatSimpleValue], [FormatLiteralValue],
//     [FormatSpannerCLIValue], or [FormatJSONSimpleValue]) that formats scalars directly
//     from GenericColumnValue without Decode; use [FormatConfigWithoutScalarPlugins] or remove
//     them on a clone to use the legacy path. Plugins fall through when the FormatNullable field is set.
//   - FormatNullable: optional extension hook. When set, formats non-NULL scalars after Decode
//     when no scalar plugin handles the value. When nil, the scalar slow path returns
//     [ErrFormatNullableRequired] without Decode. NULL scalars use [*FormatConfig.GetNullString]
//     before the slow path runs (FormatNullable is not called for NULL).
//
// Use [*FormatConfig.Clone] or [*FormatConfig.WithComplexPlugin] (prepends plugins) to customize a preset
// without mutating shared instances.
// Call [*FormatConfig.Validate] after hand-assembling a config to fail fast on nil callbacks
// or an empty [FormatConfig.NullString]. [NewFormatConfig] assembles and validates a config
// whose behavior lives entirely in NullString and FormatComplexPlugins, without the
// deprecated fields below.
type FormatConfig struct {
	NullString string
	// FormatArray formats non-NULL ARRAY values on the built-in path.
	//
	// Deprecated: register [PluginForArray] in FormatComplexPlugins (or build the
	// config with [NewFormatConfig] and [WithArrayFormat]) instead. This field
	// will be removed in the next breaking release (#253).
	FormatArray FormatArrayFunc
	// FormatStruct formats non-NULL STRUCT values on the built-in path.
	//
	// Deprecated: register [PluginForStruct] in FormatComplexPlugins (or build the
	// config with [NewFormatConfig] and [WithStructFormat]) instead. This field
	// will be removed in the next breaking release (#253).
	FormatStruct         FormatStruct
	FormatComplexPlugins []FormatComplexFunc
	// FormatNullable formats non-NULL scalar values after Decode on the built-in
	// slow path, when no plugin handled the value first.
	//
	// Deprecated: append [PluginFromNullable] as the last element of
	// FormatComplexPlugins so it runs after any preset plugins it should not
	// shadow (or build the config with [NewFormatConfig] and
	// [WithScalarFormatter]). This field will be removed in the next breaking
	// release (#252, #253).
	FormatNullable FormatNullableFunc
	// Literal holds options for the literal preset only ([LiteralFormatOptions]).
	// Quote is read when FormatNullable is the preset formatNullableValueLiteral (including the
	// formatSimpleColumn slow-path intercept) and by literal preset complex plugins such as
	// [FormatLiteralValue] and [FormatProtoAsCast]. Custom FormatNullable callbacks do not
	// consult this field. Other presets leave Literal at the zero value. Quote zero value is
	// legacy suitableQuote behavior (QuoteLegacy + PreferredDoubleQuote). Invalid enum values
	// are normalized when literal options are applied and again when Quote is read at format
	// time. Escaping uses GoogleSQL backslash rules; not PostgreSQL (#126).
	//
	// Deprecated: set quote options through the literal preset constructors
	// ([LiteralFormatConfigWithOptions], [LiteralFormatConfigWithQuote],
	// [WithLiteralQuote]); quote options become constructor-captured state of the
	// literal preset's scalar plugin and this field will be removed from
	// FormatConfig in the next breaking release (#253, #185).
	Literal LiteralFormatOptions
}

type FormatStruct struct {
	FormatStructField FormatStructFieldFunc
	FormatStructParen FormatStructParenFunc
}

func (fc *FormatConfig) GetNullString() string { return fc.NullString }

type FormatArrayFunc func(typ *sppb.Type, toplevel bool, elemStrings []string) (string, error)
type FormatStructParenFunc func(typ *sppb.Type, toplevel bool, fieldStrings []string) (string, error)
type FormatStructFieldFunc func(fc *FormatConfig, field *sppb.StructType_Field, value *structpb.Value) (string, error)
type FormatNullableFunc = func(value NullableValue) (string, error)

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

	switch value.Type.GetCode() {
	case sppb.TypeCode_ARRAY:
		if IsNull(value) {
			return fc.GetNullString(), nil
		}
		return formatArrayElems(fc, value, toplevel, fc.FormatArray)
	case sppb.TypeCode_STRUCT:
		if IsNull(value) {
			return fc.GetNullString(), nil
		}
		fs := fc.FormatStruct
		return formatStructFields(value, toplevel, func(field *sppb.StructType_Field, v *structpb.Value) (string, error) {
			return fs.FormatStructField(fc, field, v)
		}, fs.FormatStructParen)
	default:
		return fc.formatSimpleColumn(value)
	}
}

// formatArrayElems is the non-NULL ARRAY shape shared by the built-in
// [*FormatConfig.FormatColumn] branch and [PluginForArray]: extract the wire
// list value (non-list payloads are [ErrUnexpectedComplexValueKind]), recurse
// into each element with formatter.FormatColumn(elem, false), and hand the
// element strings to join.
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

// formatStructFields is the non-NULL STRUCT shape shared by the built-in
// [*FormatConfig.FormatColumn] branch and [PluginForStruct]: extract the wire
// list value (non-list payloads are [ErrUnexpectedComplexValueKind]), check the
// value count against the field descriptors ([ErrMismatchedFields]), format
// each field with the field callback, and hand the field strings to paren.
func formatStructFields(value spanner.GenericColumnValue, toplevel bool, field func(*sppb.StructType_Field, *structpb.Value) (string, error), paren FormatStructParenFunc) (string, error) {
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
		return field(f, fieldValues[i])
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
