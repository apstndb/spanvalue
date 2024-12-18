package spanvalue

import (
	"fmt"
	"strings"

	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/lox"
	"github.com/apstndb/spantype"
	"google.golang.org/protobuf/types/known/structpb"
)

func formatTypedStructParen(typ *spannerpb.Type, toplevel bool, fieldStrings []string) string {
	return fmt.Sprintf("%v(%v)", lox.IfOrEmpty(toplevel, spantype.FormatTypeVerbose(typ)), strings.Join(fieldStrings, ", "))
}

func FormatTupleStruct(typ *spannerpb.Type, toplevel bool, fieldStrings []string) string {
	return fmt.Sprintf("(%v)", strings.Join(fieldStrings, ", "))
}

func formatSimpleStructField(fc *FormatConfig, field *spannerpb.StructType_Field, value *structpb.Value) (string, error) {
	return fc.FormatColumn(typeValueToGCV(field.GetType(), value), false)
}

func FormatTypelessStructField(fc *FormatConfig, field *spannerpb.StructType_Field, value *structpb.Value) (string, error) {
	exprStr, err := fc.FormatColumn(typeValueToGCV(field.GetType(), value), false)
	return exprStr + lox.IfOrEmpty(field.GetName() != "", " AS "+field.GetName()), err
}

func FormatSimpleStructField(fc *FormatConfig, field *spannerpb.StructType_Field, value *structpb.Value) (string, error) {
	return fc.FormatColumn(typeValueToGCV(field.Type, value), false)
}

func FormatUntypedArray(typ *spannerpb.Type, toplevel bool, elemStrings []string) string {
	return fmt.Sprintf("[%v]", strings.Join(elemStrings, ", "))
}

func FormatOptionallyTypedArray(typ *spannerpb.Type, toplevel bool, elemStrings []string) string {
	return fmt.Sprintf("%v[%v]",
		lox.IfOrEmpty(toplevel && isComplexType(typ.ArrayElementType.GetCode()), spantype.FormatTypeVerbose(typ)),
		strings.Join(elemStrings, ", "))
}
