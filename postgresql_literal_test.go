package spanvalue

import (
	"math/big"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/spantype/typector"
	"github.com/google/uuid"
	"github.com/samber/lo"

	"github.com/apstndb/spanvalue/gcvctor"
)

func TestFormatColumnPostgreSQLLiteral(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value spanner.GenericColumnValue
		want  string
	}{
		{
			name:  "string",
			value: gcvctor.StringValue("that's it"),
			want:  `'that''s it'`,
		},
		{
			name:  "bytes",
			value: gcvctor.BytesValue([]byte("abc")),
			want:  `CAST('\x616263' AS bytea)`,
		},
		{
			name:  "timestamp",
			value: gcvctor.TimestampValue(time.Date(2024, 1, 15, 12, 0, 0, 0, time.FixedZone("+09", 9*60*60))),
			want:  `CAST('2024-01-15T03:00:00Z' AS timestamptz)`,
		},
		{
			name:  "numeric",
			value: gcvctor.NumericValue(big.NewRat(123456, 100)),
			want:  `CAST('1234.560000000' AS numeric)`,
		},
		{
			name:  "pg jsonb",
			value: lo.Must(gcvctor.PGJSONBValue(map[string]any{"msg": "foo"})),
			want:  `CAST('{"msg":"foo"}' AS jsonb)`,
		},
		{
			name: "interval",
			value: gcvctor.IntervalValue(spanner.Interval{
				Months: 13,
				Days:   1,
				Nanos:  big.NewInt((3600 + 60 + 1) * 1000 * 1000 * 1000),
			}),
			want: `CAST('P1Y1M1DT1H1M1S' AS interval)`,
		},
		{
			name:  "uuid",
			value: gcvctor.UUIDValue(uuid.MustParse("858ebda5-f6df-4f5d-9151-aa98796053c4")),
			want:  `CAST('858ebda5-f6df-4f5d-9151-aa98796053c4' AS uuid)`,
		},
		{
			name:  "empty int64 array",
			value: gcvctor.EmptyArrayOf(typector.CodeToSimpleType(sppb.TypeCode_INT64)),
			want:  `CAST(ARRAY[] AS bigint[])`,
		},
		{
			name:  "int64 array",
			value: lo.Must(gcvctor.ArrayValue(gcvctor.Int64Value(1), gcvctor.Int64Value(2))),
			want:  `ARRAY[1, 2]`,
		},
		{
			name: "struct",
			value: lo.Must(gcvctor.StructValueOf(
				[]string{"a", "b"},
				[]spanner.GenericColumnValue{
					gcvctor.Int64Value(1),
					gcvctor.DateValue(civil.Date{Year: 2024, Month: 1, Day: 15}),
				},
			)),
			want: `ROW(1, CAST('2024-01-15' AS date))`,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := FormatColumnPostgreSQLLiteral(tt.value)
			if err != nil {
				t.Fatalf("FormatColumnPostgreSQLLiteral() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("FormatColumnPostgreSQLLiteral() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestQuoteIdentifier(t *testing.T) {
	t.Parallel()

	if got := QuoteIdentifier(SQLDialectGoogleSQL, "a`b"); got != "`a``b`" {
		t.Fatalf("QuoteIdentifier(GoogleSQL) = %q, want %q", got, "`a``b`")
	}
	if got := QuoteIdentifier(SQLDialectPostgreSQL, `a"b`); got != `"a""b"` {
		t.Fatalf("QuoteIdentifier(PostgreSQL) = %q, want %q", got, `"a""b"`)
	}
}

func TestQuoteQualifiedIdentifier(t *testing.T) {
	t.Parallel()

	if got := QuoteQualifiedIdentifier(SQLDialectGoogleSQL, "schema.table"); got != "`schema`.`table`" {
		t.Fatalf("QuoteQualifiedIdentifier(GoogleSQL) = %q, want %q", got, "`schema`.`table`")
	}
	if got := QuoteQualifiedIdentifier(SQLDialectPostgreSQL, "schema.table"); got != `"schema"."table"` {
		t.Fatalf("QuoteQualifiedIdentifier(PostgreSQL) = %q, want %q", got, `"schema"."table"`)
	}
}

func TestFormatPostgreSQLType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		typ  *sppb.Type
		want string
	}{
		{"int64", typector.CodeToSimpleType(sppb.TypeCode_INT64), "bigint"},
		{"json", typector.CodeToSimpleType(sppb.TypeCode_JSON), "json"},
		{"jsonb", typector.PGJSONB(), "jsonb"},
		{"array", typector.ElemCodeToArrayType(sppb.TypeCode_DATE), "date[]"},
		{
			"struct",
			typector.StructTypeFieldsToStructType([]*sppb.StructType_Field{
				{Name: "a", Type: typector.CodeToSimpleType(sppb.TypeCode_INT64)},
				{Type: typector.CodeToSimpleType(sppb.TypeCode_STRING)},
			}),
			"STRUCT<a bigint, text>",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := formatPostgreSQLType(tt.typ); got != tt.want {
				t.Fatalf("formatPostgreSQLType() = %q, want %q", got, tt.want)
			}
		})
	}
}
