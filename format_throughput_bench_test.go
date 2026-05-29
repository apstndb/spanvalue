// Throughput benchmarks: build many GenericColumnValues up front, then repeatedly
// format and discard every cell. Use to compare scalar FormatComplexFunc plugins vs the
// legacy Decode + FormatNullable path on a realistic mixed-type row shape.
package spanvalue

import (
	"fmt"
	"math/big"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/apstndb/spanvalue/gcvctor"
	"github.com/samber/lo"
)

// benchSink prevents the compiler from eliding format results.
var benchSink string

// benchRowSchema is a fixed mix of scalar types resembling a delimited export row.
func benchRowSchema(row int) []spanner.GenericColumnValue {
	rat := big.NewRat(int64(1000+row), 100)
	ts := lo.Must(time.Parse(time.RFC3339Nano, "2020-01-02T03:04:05.123456789Z"))
	return []spanner.GenericColumnValue{
		gcvctor.Int64Value(int64(row)),
		gcvctor.StringValue(fmt.Sprintf("name-%d-with-padding-for-realistic-width", row)),
		gcvctor.BoolValue(row%2 == 0),
		gcvctor.Float64Value(3.141592653589793 + float64(row)*1e-6),
		gcvctor.BytesValue([]byte("payload-bytes")),
		gcvctor.TimestampValue(ts),
		gcvctor.NumericValue(rat),
		gcvctor.StringValue("tag"),
	}
}

// benchDataset builds cellsToFormat values (rows × columns) once per sub-benchmark.
func benchDataset(rows int) []spanner.GenericColumnValue {
	const cols = 8
	out := make([]spanner.GenericColumnValue, 0, rows*cols)
	for r := range rows {
		out = append(out, benchRowSchema(r)...)
	}
	return out
}

// benchmarkFormatCells formats every value in the slice each iteration and records
// output into benchSink. bytes/op is estimated from the first cell's output length.
func benchmarkFormatCells(b *testing.B, fc *FormatConfig, values []spanner.GenericColumnValue) {
	b.Helper()
	if len(values) == 0 {
		b.Fatal("empty values")
	}

	b.StopTimer()
	sample, err := fc.FormatToplevelColumn(values[0])
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(sample)) * int64(len(values)))
	b.ReportAllocs()
	b.StartTimer()

	for range b.N {
		for _, gcv := range values {
			s, err := fc.FormatToplevelColumn(gcv)
			if err != nil {
				b.Fatal(err)
			}
			benchSink = s
		}
	}
}

func benchmarkFormatThroughput(b *testing.B, fc *FormatConfig, values []spanner.GenericColumnValue) {
	b.Helper()
	benchmarkFormatCells(b, fc, values)
}

func BenchmarkFormatThroughput(b *testing.B) {
	sizes := []int{1_000, 10_000}
	for _, rows := range sizes {
		values := benchDataset(rows)
		b.Run(fmt.Sprintf("rows=%d", rows), func(b *testing.B) {
			b.Run("Simple/Direct", func(b *testing.B) {
				benchmarkFormatThroughput(b, SimpleFormatConfig(), values)
			})
			b.Run("Simple/Nullable", func(b *testing.B) {
				benchmarkFormatThroughput(b, formatConfigNullableOnly(SimpleFormatConfig()), values)
			})
			b.Run("Literal/Direct", func(b *testing.B) {
				benchmarkFormatThroughput(b, LiteralFormatConfig(), values)
			})
			b.Run("Literal/Nullable", func(b *testing.B) {
				benchmarkFormatThroughput(b, formatConfigNullableOnly(LiteralFormatConfig()), values)
			})
			b.Run("SpannerCLI/Direct", func(b *testing.B) {
				benchmarkFormatThroughput(b, SpannerCLICompatibleFormatConfig(), values)
			})
			b.Run("SpannerCLI/Nullable", func(b *testing.B) {
				benchmarkFormatThroughput(b, formatConfigNullableOnly(SpannerCLICompatibleFormatConfig()), values)
			})
		})
	}
}
