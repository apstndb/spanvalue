package spanvalue

import (
	"fmt"
	"math"
	"math/big"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	"github.com/apstndb/spanvalue/gcvctor"
	"github.com/google/uuid"
	"github.com/samber/lo"
)

const benchCellsPerType = 10_000

type benchScalarCase struct {
	name string
	// newCell returns a value for index i; generation runs once before timing.
	newCell func(i int) spanner.GenericColumnValue
}

func benchScalarCases() []benchScalarCase {
	rat := big.NewRat(314, 100)
	pgRat := big.NewRat(22, 7)
	ts := lo.Must(time.Parse(time.RFC3339Nano, "2020-01-02T03:04:05.123456789Z"))
	date := civil.Date{Year: 2020, Month: 1, Day: 2}
	uid := uuid.MustParse("123e4567-e89b-12d3-a456-426614174000")
	jsonGCV := lo.Must(gcvctor.JSONValue(map[string]any{"k": 1}))
	pgJSONGCV := lo.Must(gcvctor.PGJSONBValue(map[string]int{"k": 1}))
	intervalGCV := lo.Must(gcvctor.IntervalStringValue("P1Y2M3DT4H5M6.789S"))

	return []benchScalarCase{
		{name: "BOOL", newCell: func(i int) spanner.GenericColumnValue {
			return gcvctor.BoolValue(i%2 == 0)
		}},
		{name: "INT64", newCell: func(i int) spanner.GenericColumnValue {
			return gcvctor.Int64Value(int64(i))
		}},
		{name: "FLOAT32", newCell: func(i int) spanner.GenericColumnValue {
			return gcvctor.Float32Value(float32(1.25) + float32(i)*1e-6)
		}},
		{name: "FLOAT64", newCell: func(i int) spanner.GenericColumnValue {
			return gcvctor.Float64Value(3.141592653589793 + float64(i)*1e-9)
		}},
		{name: "FLOAT64_NaN", newCell: func(int) spanner.GenericColumnValue {
			return gcvctor.Float64Value(math.NaN())
		}},
		{name: "FLOAT64_Inf", newCell: func(int) spanner.GenericColumnValue {
			return gcvctor.Float64Value(math.Inf(1))
		}},
		{name: "STRING", newCell: func(i int) spanner.GenericColumnValue {
			return gcvctor.StringValue(fmt.Sprintf("value-%d-with-padding", i))
		}},
		{name: "BYTES", newCell: func(int) spanner.GenericColumnValue {
			return gcvctor.BytesValue([]byte("payload-bytes"))
		}},
		{name: "DATE", newCell: func(int) spanner.GenericColumnValue {
			return gcvctor.DateValue(date)
		}},
		{name: "TIMESTAMP", newCell: func(int) spanner.GenericColumnValue {
			return gcvctor.TimestampValue(ts)
		}},
		{name: "NUMERIC", newCell: func(i int) spanner.GenericColumnValue {
			return gcvctor.NumericValue(big.NewRat(int64(1000+i), 100))
		}},
		{name: "PG_NUMERIC", newCell: func(i int) spanner.GenericColumnValue {
			return gcvctor.PGNumericValue(big.NewRat(int64(1000+i), 100))
		}},
		{name: "JSON", newCell: func(int) spanner.GenericColumnValue {
			return jsonGCV
		}},
		{name: "PG_JSONB", newCell: func(int) spanner.GenericColumnValue {
			return pgJSONGCV
		}},
		{name: "INTERVAL", newCell: func(int) spanner.GenericColumnValue {
			return intervalGCV
		}},
		{name: "UUID", newCell: func(int) spanner.GenericColumnValue {
			return gcvctor.UUIDValue(uid)
		}},
		// NUMERIC allocates a new big.Rat per cell in newCell above; compare with shared rat:
		{name: "NUMERIC_sharedRat", newCell: func(int) spanner.GenericColumnValue {
			return gcvctor.NumericValue(rat)
		}},
		{name: "PG_NUMERIC_sharedRat", newCell: func(int) spanner.GenericColumnValue {
			return gcvctor.PGNumericValue(pgRat)
		}},
	}
}

func benchCellsForCase(tc benchScalarCase, n int) []spanner.GenericColumnValue {
	out := make([]spanner.GenericColumnValue, n)
	for i := range n {
		out[i] = tc.newCell(i)
	}
	return out
}

var benchPerTypePresets = []struct {
	name string
	fc   func() *FormatConfig
}{
	{name: "Simple/Direct", fc: SimpleFormatConfig},
	{name: "Simple/Nullable", fc: func() *FormatConfig {
		return simpleNullablePathConfig()
	}},
	{name: "Literal/Direct", fc: LiteralFormatConfig},
	{name: "Literal/Nullable", fc: func() *FormatConfig {
		return literalNullablePathConfig(LiteralQuoteConfig{})
	}},
	{name: "SpannerCLI/Direct", fc: SpannerCLICompatibleFormatConfig},
	{name: "SpannerCLI/Nullable", fc: func() *FormatConfig {
		return spannerCLINullablePathConfig()
	}},
	{name: "JSON/Direct", fc: JSONFormatConfig},
	{name: "JSON/Nullable", fc: func() *FormatConfig {
		return jsonNullablePathConfig()
	}},
}

// BenchmarkFormatPerType formats many cells of a single Spanner type per sub-benchmark.
// Compare Direct (scalar FormatComplexFunc plugin) vs legacy Decode + FormatNullable.
//
// Examples:
//
//	go test -bench='BenchmarkFormatPerType/INT64/Simple' -benchmem .
//	go test -bench='BenchmarkFormatPerType/BYTES' -benchmem -count=3 . | benchstat
func BenchmarkFormatPerType(b *testing.B) {
	for _, tc := range benchScalarCases() {
		values := benchCellsForCase(tc, benchCellsPerType)
		b.Run(tc.name, func(b *testing.B) {
			for _, preset := range benchPerTypePresets {
				b.Run(preset.name, func(b *testing.B) {
					benchmarkFormatCells(b, preset.fc(), values)
				})
			}
		})
	}
}
