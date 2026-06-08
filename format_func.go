package spanvalue

import (
	"fmt"
	"strings"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/spantype"
	"github.com/samber/lo"
	"google.golang.org/protobuf/types/known/structpb"
)

func formatTypedStructParen(typ *sppb.Type, toplevel bool, fieldStrings []string) (string, error) {
	return fmt.Sprintf("%v(%v)", lo.Ternary(toplevel, spantype.FormatTypeVerbose(typ), ""), strings.Join(fieldStrings, ", ")), nil
}

// FormatTupleStruct renders STRUCT values with parentheses, for example (1, east).
// [SimpleFormatConfig] uses it by default. To combine tuple STRUCT with Spanner CLI
// scalars, see [SpannerCLICompatibleFormatConfig] and the README tuple STRUCT example.
func FormatTupleStruct(typ *sppb.Type, toplevel bool, fieldStrings []string) (string, error) {
	return fmt.Sprintf("(%v)", strings.Join(fieldStrings, ", ")), nil
}

func formatSimpleStructField(fc *FormatConfig, field *sppb.StructType_Field, value *structpb.Value) (string, error) {
	return FormatSimpleStructField(fc, field, value)
}

func FormatTypelessStructField(fc *FormatConfig, field *sppb.StructType_Field, value *structpb.Value) (string, error) {
	exprStr, err := FormatSimpleStructField(fc, field, value)
	if err != nil {
		return "", err
	}
	return exprStr + lo.Ternary(field.GetName() != "", " AS "+field.GetName(), ""), nil
}

func FormatSimpleStructField(fc *FormatConfig, field *sppb.StructType_Field, value *structpb.Value) (string, error) {
	fieldType, err := structFieldType(field)
	if err != nil {
		return "", err
	}
	return fc.FormatColumn(typeValueToGCV(fieldType, value), false)
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
// [LiteralFormatConfig] wires this as [FormatConfig.FormatArray].
func FormatOptionallyTypedArray(typ *sppb.Type, toplevel bool, elemStrings []string) (string, error) {
	return fmt.Sprintf("%v[%v]",
		lo.Ternary(toplevel && isComplexType(typ.ArrayElementType.GetCode()), spantype.FormatTypeVerbose(typ), ""),
		strings.Join(elemStrings, ", ")), nil
}
