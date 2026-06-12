package spanvalue_test

import (
	"fmt"
	"math/big"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"

	"github.com/apstndb/spanvalue"
	"github.com/apstndb/spanvalue/gcvctor"
)

// Tuple-style STRUCT in `ARRAY<STRUCT<...>>` while keeping Spanner CLI scalar formatting.
// The prepended PluginForStruct override runs before the preset's STRUCT handler.
func ExampleSpannerCLICompatibleFormatConfig_tupleStruct() {
	fc := spanvalue.SpannerCLICompatibleFormatConfig().WithComplexPlugin(
		spanvalue.PluginForStruct(spanvalue.FormatSimpleStructField, spanvalue.FormatTupleStruct))

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

// Assemble a config from the canonical handlers plus a per-type override:
// the prepended PluginFromNullable claims NUMERIC, everything else reaches
// the WithScalarFormatter tail, and NULLs render as the WithNullString value.
func ExampleNewFormatConfig() {
	fc, err := spanvalue.NewFormatConfig(
		spanvalue.WithNullString("NULL"),
		spanvalue.WithPlugin(spanvalue.PluginFromNullable(spanvalue.NullableFormatterFor(
			func(v spanner.NullNumeric) (string, error) {
				return v.Numeric.FloatString(2), nil
			}))),
		spanvalue.WithArrayFormat(spanvalue.FormatUntypedArray),
		spanvalue.WithStructFormat(spanvalue.FormatSimpleStructField, spanvalue.FormatTupleStruct),
		spanvalue.WithScalarFormatter(spanvalue.FormatNullableSpannerCLICompatible),
	)
	if err != nil {
		panic(err)
	}

	arr, err := gcvctor.ArrayValue(
		gcvctor.NumericValue(big.NewRat(3, 2)),
		gcvctor.NullFromCode(sppb.TypeCode_NUMERIC),
	)
	if err != nil {
		panic(err)
	}

	out, err := fc.FormatToplevelColumn(arr)
	if err != nil {
		panic(err)
	}
	fmt.Println(out)
	// Output:
	// [1.50, NULL]
}
