package spanvalue_test

import (
	"fmt"

	"cloud.google.com/go/spanner"

	"github.com/apstndb/spanvalue"
	"github.com/apstndb/spanvalue/gcvctor"
)

func ExampleSpannerCLICompatibleTupleStructFormatConfig() {
	fc := spanvalue.SpannerCLICompatibleTupleStructFormatConfig()

	structElem, err := gcvctor.StructValueOf(
		[]string{"id", "region"},
		[]spanner.GenericColumnValue{gcvctor.Int64Value(1), gcvctor.StringValue("east")},
	)
	if err != nil {
		panic(err)
	}
	arrayOfStruct, err := gcvctor.ArrayValue(structElem)
	if err != nil {
		panic(err)
	}

	out, err := fc.FormatToplevelColumn(arrayOfStruct)
	if err != nil {
		panic(err)
	}
	fmt.Println(out)
	// Output:
	// [(1, east)]
}
