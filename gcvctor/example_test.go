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
		gcvctor.MustDateStringValue("2026-04-01"),
		gcvctor.NullOf(nil),
		gcvctor.MustDateStringValue("2026-04-03"),
	}

	normalized := gcvctor.MustNormalizeArrayElements(elemType, elems...)
	array := gcvctor.MustArrayValueOf(elemType, normalized...)
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
	gcv := gcvctor.MustIntervalStringValue("P1Y2M3DT4H5M6S")

	fmt.Println(gcv.Type.Code.String(), gcv.Value.GetStringValue())
	// Output:
	// INTERVAL P1Y2M3DT4H5M6S
}

func ExampleEmptyArrayOf() {
	elemType := typector.CodeToSimpleType(sppb.TypeCode_STRING)
	empty := gcvctor.EmptyArrayOf(elemType)
	viaArrayValueOf := gcvctor.MustArrayValueOf(elemType)

	fmt.Println(spanvalue.IsNull(empty), len(empty.Value.GetListValue().GetValues()))
	fmt.Println(empty.Type.ArrayElementType.Code.String())
	fmt.Println(spanvalue.IsNull(viaArrayValueOf), len(viaArrayValueOf.Value.GetListValue().GetValues()))
	// Output:
	// false 0
	// STRING
	// false 0
}

func ExampleNullArrayOf() {
	elemType := typector.CodeToSimpleType(sppb.TypeCode_STRING)
	nullArray := gcvctor.NullArrayOf(elemType)
	nullViaNullOf := gcvctor.NullOf(typector.ElemTypeToArrayType(elemType))

	fmt.Println(spanvalue.IsNull(nullArray), nullArray.Type.ArrayElementType.Code.String())
	fmt.Println(spanvalue.IsNull(nullViaNullOf))
	// Output:
	// true STRING
	// true
}

func ExampleNullOf_structContainer() {
	structType, err := typector.NameCodeSlicesToStructType(
		[]string{"id", "name"},
		[]sppb.TypeCode{sppb.TypeCode_INT64, sppb.TypeCode_STRING},
	)
	if err != nil {
		panic(err)
	}
	nullStruct := gcvctor.NullOf(structType)
	fieldsAllNull := gcvctor.MustStructValueOf(
		[]string{"id", "name"},
		[]spanner.GenericColumnValue{
			gcvctor.NullOf(typector.CodeToSimpleType(sppb.TypeCode_INT64)),
			gcvctor.NullOf(typector.CodeToSimpleType(sppb.TypeCode_STRING)),
		},
	)

	fmt.Println(spanvalue.IsNull(nullStruct), nullStruct.Type.Code.String())
	fmt.Println(spanvalue.IsNull(fieldsAllNull), fieldsAllNull.Type.Code.String())
	// Output:
	// true STRUCT
	// false STRUCT
}

func ExampleStructValueOfFields() {
	row, err := gcvctor.StructValueOfFields(
		gcvctor.StructFieldOf("Code", gcvctor.StringValue("10")),
		gcvctor.StructFieldOf("DisplayOrder", gcvctor.Int64Value(1)),
	)
	if err != nil {
		panic(err)
	}
	unnamed, err := gcvctor.StructValueOfFields(
		gcvctor.StructFieldOf("", gcvctor.StringValue("value")),
		gcvctor.StructFieldOf("", gcvctor.Int64Value(42)),
	)
	if err != nil {
		panic(err)
	}

	fmt.Println(row.Type.StructType.Fields[0].Name, row.Value.GetListValue().Values[0].GetStringValue())
	fmt.Println(len(unnamed.Type.StructType.Fields), unnamed.Type.StructType.Fields[0].Name == "")
	// Output:
	// Code 10
	// 2 true
}

func ExampleMustStructValueOfFields() {
	row := gcvctor.MustStructValueOfFields(
		gcvctor.StructFieldOf("Code", gcvctor.StringValue("10")),
		gcvctor.StructFieldOf("DisplayOrder", gcvctor.Int64Value(1)),
	)

	fmt.Println(row.Type.StructType.Fields[1].Name, row.Value.GetListValue().Values[1].GetStringValue())
	// Output:
	// DisplayOrder 1
}

func ExampleInt64FromPtr_fromNullable() {
	var optional *int64
	fromPtr := gcvctor.Int64FromPtr(optional)
	fromNullable := gcvctor.Int64FromNullable(spanner.NullInt64{})

	fmt.Println(spanvalue.IsNull(fromPtr))
	fmt.Println(spanvalue.IsNull(fromNullable))

	v := int64(42)
	fromPtr = gcvctor.Int64FromPtr(&v)
	fromNullable = gcvctor.Int64FromNullable(spanner.NullInt64{Int64: 42, Valid: true})

	fmt.Println(fromPtr.Value.GetStringValue())
	fmt.Println(fromNullable.Value.GetStringValue())
	// Output:
	// true
	// true
	// 42
	// 42
}
