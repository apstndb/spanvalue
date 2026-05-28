package spanvalue_test

import (
	"fmt"

	"cloud.google.com/go/spanner"

	"github.com/apstndb/spanvalue"
	"github.com/apstndb/spanvalue/gcvctor"
	"github.com/samber/lo"
)

func ExampleSpannerCLICompatibleFormatConfig_tupleStruct() {
	fc := spanvalue.SpannerCLICompatibleFormatConfig().Clone()
	fc.FormatStruct.FormatStructParen = spanvalue.FormatTupleStruct

	structElem := lo.Must(gcvctor.StructValueOf(
		[]string{"id", "region"},
		[]spanner.GenericColumnValue{gcvctor.Int64Value(1), gcvctor.StringValue("east")},
	))
	arrayOfStruct := lo.Must(gcvctor.ArrayValue(structElem))

	fmt.Println(lo.Must(fc.FormatToplevelColumn(arrayOfStruct)))
	// Output:
	// [(1, east)]
}
