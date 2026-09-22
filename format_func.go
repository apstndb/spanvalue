package spanvalue

import (
	"fmt"
	"strings"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/spantype"
	"github.com/samber/lo"
	"google.golang.org/protobuf/types/known/structpb"
)

// FormatTypedStruct renders STRUCT values with parentheses and, at top level,
// a verbose STRUCT<...> type prefix, for example STRUCT<id INT64>(1).
// [LiteralFormatConfig] uses it as its [PluginForStruct] paren callback.
// Nested values use compact STRUCT-like notation, including () and (x).
// Use [FormatTupleStructFormal] for an opt-in GoogleSQL constructor callback.
func FormatTypedStruct(typ *sppb.Type, toplevel bool, fieldStrings []string) (string, error) {
	return fmt.Sprintf("%v(%v)", lo.Ternary(toplevel, spantype.FormatTypeVerbose(typ), ""), strings.Join(fieldStrings, ", ")), nil
}

// FormatTupleStruct renders STRUCT values with parentheses, for example (1, east).
// [SimpleFormatConfig] uses it by default. To combine tuple STRUCT with Spanner CLI
// scalars, see [SpannerCLICompatibleFormatConfig] and the README tuple STRUCT example.
// This is display notation: () and (x) are not GoogleSQL STRUCT constructors.
// Use [FormatTupleStructFormal] when constructor syntax is needed.
func FormatTupleStruct(typ *sppb.Type, toplevel bool, fieldStrings []string) (string, error) {
	return fmt.Sprintf("(%v)", strings.Join(fieldStrings, ", ")), nil
}

// FormatTupleStructFormal renders GoogleSQL STRUCT constructor syntax: STRUCT()
// for no fields, STRUCT(x) for one field, and (x, y) for two or more fields.
// It ignores typ and toplevel, and does not preserve field names or declared types.
//
// Use it with [PluginForStruct] and [FormatSimpleStructField]. Each field string
// must already be a valid GoogleSQL expression without an AS alias; this callback
// does not validate expressions or guarantee type-preserving round trips.
// Existing presets continue to use their compact display callbacks.
func FormatTupleStructFormal(_ *sppb.Type, _ bool, fieldStrings []string) (string, error) {
	if len(fieldStrings) < 2 {
		return "STRUCT(" + strings.Join(fieldStrings, ", ") + ")", nil
	}
	return FormatTupleStruct(nil, false, fieldStrings)
}

// FormatTypelessStructField formats a STRUCT field as the field value followed
// by an " AS name" suffix for named fields. [SimpleFormatConfig] uses it as
// its [PluginForStruct] field callback.
func FormatTypelessStructField(formatter Formatter, field *sppb.StructType_Field, value *structpb.Value) (string, error) {
	exprStr, err := FormatSimpleStructField(formatter, field, value)
	if err != nil {
		return "", err
	}
	return exprStr + lo.Ternary(field.GetName() != "", " AS "+field.GetName(), ""), nil
}

// FormatSimpleStructField formats a STRUCT field as the field value alone,
// recursing through the whole plugin chain with toplevel false.
func FormatSimpleStructField(formatter Formatter, field *sppb.StructType_Field, value *structpb.Value) (string, error) {
	fieldType, err := structFieldType(field)
	if err != nil {
		return "", err
	}
	return formatter.FormatColumn(typeValueToGCV(fieldType, value), false)
}

func structFieldType(field *sppb.StructType_Field) (*sppb.Type, error) {
	if field == nil {
		return nil, ErrNilStructField
	}
	return field.GetType(), nil
}

func FormatUntypedArray(_ *sppb.Type, _ bool, elemStrings []string) (string, error) {
	return "[" + strings.Join(elemStrings, ", ") + "]", nil
}

// FormatOptionallyTypedArray formats ARRAY values for SQL literals. It prefixes the
// bracket list with an ARRAY<...> type annotation only when toplevel is true and the
// array element type is complex (STRUCT or nested ARRAY), independent of element count.
// Scalar element arrays at top level are untyped ([], [1, 2], not ARRAY<INT64>[1, 2]).
// [LiteralFormatConfig] wires this through [PluginForArray].
func FormatOptionallyTypedArray(typ *sppb.Type, toplevel bool, elemStrings []string) (string, error) {
	return fmt.Sprintf("%v[%v]",
		lo.Ternary(toplevel && isComplexType(typ.ArrayElementType.GetCode()), spantype.FormatTypeVerbose(typ), ""),
		strings.Join(elemStrings, ", ")), nil
}
