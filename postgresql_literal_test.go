package spanvalue

import (
	"math/big"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	"github.com/apstndb/spanvalue/gcvctor"
	"github.com/google/go-cmp/cmp"
)

func TestPostgreSQLLiteralFormatConfig_scaffold(t *testing.T) {
	t.Parallel()

	cfg := PostgreSQLLiteralFormatConfig()
	if cfg == nil {
		t.Fatal("PostgreSQLLiteralFormatConfig() = nil")
	}

	// Baseline behavior available today: single-quoted STRING literals.
	got, err := cfg.FormatToplevelColumn(gcvctor.StringValue("Alice"))
	if err != nil {
		t.Fatalf("FormatToplevelColumn() error = %v", err)
	}
	want := "'Alice'"
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("STRING literal mismatch (-want +got):\n%s", diff)
	}
}

func TestPostgreSQLLiteralFormatConfig_intendedPGForms(t *testing.T) {
	t.Parallel()

	cfg := PostgreSQLLiteralFormatConfig()
	tests := []struct {
		name string
		gcv  spanner.GenericColumnValue
		want string
	}{
		{
			name: "timestamp pg annotation",
			gcv:  gcvctor.TimestampValue(time.Date(2024, 1, 15, 12, 30, 0, 0, time.UTC)),
			want: "TIMESTAMP '2024-01-15T12:30:00Z'",
		},
		{
			name: "date pg annotation",
			gcv:  gcvctor.DateValue(civil.Date{Year: 2024, Month: 1, Day: 15}),
			want: "DATE '2024-01-15'",
		},
		{
			name: "numeric pg form",
			gcv:  gcvctor.NumericValue(big.NewRat(199, 2)),
			want: "99.5",
		},
		{
			name: "jsonb pg cast",
			gcv:  mustGCV(gcvctor.JSONValue(map[string]any{"k": 1})),
			want: `CAST('{"k":1}' AS JSONB)`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			t.Skip("WIP #126: PostgreSQL type annotations not implemented yet")

			got, err := cfg.FormatToplevelColumn(tt.gcv)
			if err != nil {
				t.Fatalf("FormatToplevelColumn() error = %v", err)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Fatalf("literal mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func mustGCV(gcv spanner.GenericColumnValue, err error) spanner.GenericColumnValue {
	if err != nil {
		panic(err)
	}
	return gcv
}
