package spanvalue

import (
	"fmt"
	"strings"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/spantype"
	"github.com/samber/lo"
	"google.golang.org/protobuf/types/known/structpb"
)

func formatTypedStructParen(typ *sppb.Type, toplevel bool, fieldStrings []string) string {
	return fmt.Sprintf("%v(%v)", lo.Ternary(toplevel, spantype.FormatTypeVerbose(typ), ""), strings.Join(fieldStrings, ", "))
}

func FormatTupleStruct(typ *sppb.Type, toplevel bool, fieldStrings []string) string {
	return fmt.Sprintf("(%v)", strings.Join(fieldStrings, ", "))
}

func formatSimpleStructField(fc *FormatConfig, field *sppb.StructType_Field, value *structpb.Value) (string, error) {
	return fc.FormatColumn(typeValueToGCV(field.GetType(), value), false)
}

func FormatTypelessStructField(fc *FormatConfig, field *sppb.StructType_Field, value *structpb.Value) (string, error) {
	exprStr, err := fc.FormatColumn(typeValueToGCV(field.GetType(), value), false)
	return exprStr + lo.Ternary(field.GetName() != "", " AS "+field.GetName(), ""), err
}

func FormatSimpleStructField(fc *FormatConfig, field *sppb.StructType_Field, value *structpb.Value) (string, error) {
	return fc.FormatColumn(typeValueToGCV(field.Type, value), false)
}

func FormatUntypedArray(_ *sppb.Type, _ bool, elemStrings []string) string {
	return "[" + strings.Join(elemStrings, ", ") + "]"
}

func FormatOptionallyTypedArray(typ *sppb.Type, toplevel bool, elemStrings []string) string {
	return fmt.Sprintf("%v[%v]",
		lo.Ternary(toplevel && isComplexType(typ.ArrayElementType.GetCode()), spantype.FormatTypeVerbose(typ), ""),
		strings.Join(elemStrings, ", "))
}
