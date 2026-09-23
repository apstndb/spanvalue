package spanvalue

import (
	"strings"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/apstndb/spanvalue/gcvctor"
)

// spannerMaxCellBytes is the Spanner STRING/BYTES cell-size limit (10 MiB).
const spannerMaxCellBytes = 10 * 1024 * 1024

// BenchmarkFormatMaxCell formats a single cell at Spanner's 10 MiB STRING/BYTES limit.
//
// Examples:
//
//	go test -bench='BenchmarkFormatMaxCell/STRING/Literal/Direct' -benchmem .
//	go test -bench='BenchmarkFormatMaxCell' -benchmem -count=5 . | benchstat
func BenchmarkFormatMaxCell(b *testing.B) {
	strPayload := strings.Repeat("x", spannerMaxCellBytes)
	strGCV := gcvctor.StringValue(strPayload)
	bytesGCV := gcvctor.BytesValue([]byte(strPayload))

	for _, tc := range []struct {
		name     string
		gcv      spanner.GenericColumnValue
		setBytes int64
	}{
		{name: "STRING", gcv: strGCV, setBytes: spannerMaxCellBytes},
		{name: "BYTES", gcv: bytesGCV, setBytes: spannerMaxCellBytes},
	} {
		b.Run(tc.name, func(b *testing.B) {
			for _, preset := range []struct {
				name string
				fc   *FormatConfig
			}{
				{name: "Literal/Direct", fc: LiteralFormatConfig()},
				{name: "Literal/Nullable", fc: literalNullablePathConfig(LiteralQuoteConfig{})},
				{name: "Simple/Direct", fc: SimpleFormatConfig()},
			} {
				b.Run(preset.name, func(b *testing.B) {
					b.ReportAllocs()
					b.SetBytes(tc.setBytes)
					b.ResetTimer()
					for range b.N {
						if _, err := preset.fc.FormatToplevelColumn(tc.gcv); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	}
}
