package gcvctor_test

import (
	"fmt"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/spantype/typector"
	"github.com/google/uuid"

	"github.com/apstndb/spanvalue"
	"github.com/apstndb/spanvalue/gcvctor"
)

func ExampleNullOf() {
	scalar := gcvctor.NullOf(typector.CodeToSimpleType(sppb.TypeCode_INT64))
	array := gcvctor.NullOf(typector.ElemCodeToArrayType(sppb.TypeCode_DATE))

	fmt.Println(scalar.Type.Code.String(), spanvalue.IsNull(scalar))
	fmt.Println(array.Type.Code.String(), array.Type.ArrayElementType.Code.String(), spanvalue.IsNull(array))
	// Output:
	// INT64 true
	// ARRAY DATE true
}

func ExampleNormalizeArrayElements() {
	elemType := typector.CodeToSimpleType(sppb.TypeCode_DATE)
	elems := []spanner.GenericColumnValue{
		must(gcvctor.DateStringValue("2026-04-01")),
		gcvctor.NullOf(nil),
		must(gcvctor.DateStringValue("2026-04-03")),
	}

	normalized := must(gcvctor.NormalizeArrayElements(elemType, elems...))
	array := must(gcvctor.ArrayValueOf(elemType, normalized...))
	values := array.Value.GetListValue().Values

	fmt.Println(array.Type.Code.String(), array.Type.ArrayElementType.Code.String(), len(values))
	fmt.Println(values[0].GetStringValue())
	fmt.Println(spanvalue.IsNull(spanner.GenericColumnValue{Type: elemType, Value: values[1]}))
	fmt.Println(values[2].GetStringValue())
	// Output:
	// ARRAY DATE 3
	// 2026-04-01
	// true
	// 2026-04-03
}

func ExampleStringBasedValueFromCode_validatedDate() {
	raw := gcvctor.StringBasedValueFromCode(sppb.TypeCode_DATE, "not-a-date")
	_, err := gcvctor.DateStringValue("not-a-date")

	fmt.Println(raw.Type.Code.String(), raw.Value.GetStringValue())
	fmt.Println(err != nil)
	// Output:
	// DATE not-a-date
	// true
}

func ExampleUUIDValue() {
	gcv := gcvctor.UUIDValue(uuid.MustParse("550e8400-e29b-41d4-a716-446655440000"))

	fmt.Println(gcv.Type.Code.String(), gcv.Value.GetStringValue())
	// Output:
	// UUID 550e8400-e29b-41d4-a716-446655440000
}

func ExampleIntervalStringValue() {
	gcv := must(gcvctor.IntervalStringValue("P1Y2M3DT4H5M6S"))

	fmt.Println(gcv.Type.Code.String(), gcv.Value.GetStringValue())
	// Output:
	// INTERVAL P1Y2M3DT4H5M6S
}
