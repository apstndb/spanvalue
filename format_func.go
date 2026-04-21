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

func FormatOptionallyTypedArray(typ *sppb.Type, toplevel bool, elemStrings []string) (string, error) {
	return fmt.Sprintf("%v[%v]",
		lo.Ternary(toplevel && isComplexType(typ.ArrayElementType.GetCode()), spantype.FormatTypeVerbose(typ), ""),
		strings.Join(elemStrings, ", ")), nil
}
