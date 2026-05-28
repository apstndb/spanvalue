package spanvalue_test

import (
	"fmt"

	"cloud.google.com/go/spanner"

	"github.com/apstndb/spanvalue"
	"github.com/apstndb/spanvalue/gcvctor"
)

func ExampleSpannerCLICompatibleFormatConfig_tupleStruct() {
	fc := spanvalue.SpannerCLICompatibleFormatConfig().Clone()
	fc.FormatStruct.FormatStructParen = spanvalue.FormatTupleStruct

	value := must(gcvctor.StructValueOf(
		[]string{"id", "region"},
		[]spanner.GenericColumnValue{gcvctor.Int64Value(1), gcvctor.StringValue("east")},
	))

	fmt.Println(must(fc.FormatToplevelColumn(value)))
	// Output:
	// (1, east)
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
