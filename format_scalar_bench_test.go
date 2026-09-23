package spanvalue

import (
	"math/big"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/apstndb/spanvalue/gcvctor"
	"github.com/samber/lo"
)

func benchmarkScalars() []spanner.GenericColumnValue {
	rat := big.NewRat(314, 100)
	ts := lo.Must(time.Parse(time.RFC3339Nano, "2020-01-02T03:04:05.123456789Z"))
	return []spanner.GenericColumnValue{
		gcvctor.StringValue("production-export-value-with-some-length"),
		gcvctor.Int64Value(42424242),
		gcvctor.BoolValue(true),
		gcvctor.Float64Value(3.141592653589793),
		gcvctor.BytesValue([]byte("ten bytes!")),
		gcvctor.TimestampValue(ts),
		gcvctor.NumericValue(rat),
	}
}

func benchmarkFormatColumn(b *testing.B, fc *FormatConfig, values []spanner.GenericColumnValue) {
	b.Helper()
	b.ReportAllocs()
	for range b.N {
		for _, gcv := range values {
			if _, err := fc.FormatToplevelColumn(gcv); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkFormatColumnSimple_Direct(b *testing.B) {
	benchmarkFormatColumn(b, SimpleFormatConfig(), benchmarkScalars())
}

func BenchmarkFormatColumnSimple_NullablePath(b *testing.B) {
	benchmarkFormatColumn(b, simpleNullablePathConfig(), benchmarkScalars())
}

func BenchmarkFormatColumnLiteral_Direct(b *testing.B) {
	benchmarkFormatColumn(b, LiteralFormatConfig(), benchmarkScalars())
}

func BenchmarkFormatColumnLiteral_NullablePath(b *testing.B) {
	benchmarkFormatColumn(b, literalNullablePathConfig(LiteralQuoteConfig{}), benchmarkScalars())
}

func BenchmarkFormatColumnSpannerCLI_Direct(b *testing.B) {
	benchmarkFormatColumn(b, SpannerCLICompatibleFormatConfig(), benchmarkScalars())
}

func BenchmarkFormatColumnSpannerCLI_NullablePath(b *testing.B) {
	benchmarkFormatColumn(b, spannerCLINullablePathConfig(), benchmarkScalars())
}
