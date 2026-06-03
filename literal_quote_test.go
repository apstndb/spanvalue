package spanvalue

import (
	"fmt"
	"math"
	"testing"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	"github.com/apstndb/spanvalue/gcvctor"
	"github.com/apstndb/spanvalue/internal"
	"github.com/google/go-cmp/cmp"
)

func TestNormalizeLiteralQuote(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   LiteralQuoteConfig
		want LiteralQuoteConfig
	}{
		{
			name: "zero value legacy",
			in:   LiteralQuoteConfig{},
			want: LiteralQuoteConfig{Strategy: QuoteLegacy, PreferredQuote: PreferredDoubleQuote},
		},
		{
			name: "legacy keeps preferred single",
			in:   LiteralQuoteConfig{Strategy: QuoteLegacy, PreferredQuote: PreferredSingleQuote},
			want: LiteralQuoteConfig{Strategy: QuoteLegacy, PreferredQuote: PreferredSingleQuote},
		},
		{
			name: "always single",
			in:   LiteralQuoteConfig{Strategy: QuoteAlways, PreferredQuote: PreferredSingleQuote},
			want: LiteralQuoteConfig{Strategy: QuoteAlways, PreferredQuote: PreferredSingleQuote},
		},
		{
			name: "min escape keeps preferred single",
			in:   LiteralQuoteConfig{Strategy: QuoteMinEscape, PreferredQuote: PreferredSingleQuote},
			want: LiteralQuoteConfig{Strategy: QuoteMinEscape, PreferredQuote: PreferredSingleQuote},
		},
		{
			name: "invalid strategy keeps valid preferred",
			in:   LiteralQuoteConfig{Strategy: QuoteStrategy(99), PreferredQuote: PreferredSingleQuote},
			want: LiteralQuoteConfig{Strategy: QuoteLegacy, PreferredQuote: PreferredSingleQuote},
		},
		{
			name: "invalid preferred",
			in:   LiteralQuoteConfig{Strategy: QuoteAlways, PreferredQuote: PreferredQuote(99)},
			want: LiteralQuoteConfig{Strategy: QuoteAlways, PreferredQuote: PreferredDoubleQuote},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := normalizeLiteralQuote(tt.in)
			if got != tt.want {
				t.Fatalf("normalizeLiteralQuote() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestLiteralFormatConfigWithOptions(t *testing.T) {
	t.Parallel()

	fc := LiteralFormatConfigWithOptions(
		WithLiteralQuote(LiteralQuoteConfig{
			Strategy:       QuoteMinEscape,
			PreferredQuote: PreferredSingleQuote,
		}),
	)
	want := LiteralQuoteConfig{Strategy: QuoteMinEscape, PreferredQuote: PreferredSingleQuote}
	if fc.Literal.Quote != want {
		t.Fatalf("Literal.Quote = %+v, want %+v", fc.Literal.Quote, want)
	}

	got, err := fc.FormatToplevelColumn(gcvctor.StringValue("plain"))
	if err != nil {
		t.Fatal(err)
	}
	if got != `'plain'` {
		t.Fatalf("STRING literal = %q, want 'plain'", got)
	}
}

func TestQuoteStrategyString(t *testing.T) {
	t.Parallel()

	if got, want := QuoteLegacy.String(), "QuoteLegacy"; got != want {
		t.Fatalf("QuoteLegacy.String() = %q, want %q", got, want)
	}
	if got, want := QuoteMinEscape.String(), "QuoteMinEscape"; got != want {
		t.Fatalf("QuoteMinEscape.String() = %q, want %q", got, want)
	}
	if got, want := QuoteStrategy(200).String(), "QuoteStrategy(200)"; got != want {
		t.Fatalf("QuoteStrategy(200).String() = %q, want %q", got, want)
	}
	if got, want := PreferredSingleQuote.String(), "PreferredSingleQuote"; got != want {
		t.Fatalf("PreferredSingleQuote.String() = %q, want %q", got, want)
	}
}

func TestLiteralQuoteMinEscape(t *testing.T) {
	t.Parallel()

	fc := LiteralFormatConfigWithQuote(LiteralQuoteConfig{
		Strategy:       QuoteMinEscape,
		PreferredQuote: PreferredDoubleQuote,
	})

	got, err := fc.FormatToplevelColumn(gcvctor.StringValue("it's ok"))
	if err != nil {
		t.Fatal(err)
	}
	if got != `"it's ok"` {
		t.Fatalf("more singles -> double: got %q, want %q", got, `"it's ok"`)
	}

	got, err = fc.FormatToplevelColumn(gcvctor.StringValue(`say "hi"`))
	if err != nil {
		t.Fatal(err)
	}
	if got != `'say "hi"'` {
		t.Fatalf("more doubles -> single: got %q, want %q", got, `'say "hi"'`)
	}

	// Legacy + PreferredDoubleQuote uses preferred when both quote types appear;
	// MinEscape picks the delimiter with fewer characters to escape.
	legacyGot, err := LiteralFormatConfig().FormatToplevelColumn(gcvctor.StringValue(`"it's"`))
	if err != nil {
		t.Fatal(err)
	}
	got, err = fc.FormatToplevelColumn(gcvctor.StringValue(`"it's"`))
	if err != nil {
		t.Fatal(err)
	}
	if got == legacyGot {
		t.Fatalf("min escape should differ from legacy; legacy=%q got=%q", legacyGot, got)
	}
	if got[0] != '\'' || got[len(got)-1] != '\'' {
		t.Fatalf("expected single-quoted outer delimiter, got %q", got)
	}
}

func TestLiteralFormatConfigWithSingleQuotedLiterals(t *testing.T) {
	t.Parallel()

	fc := LiteralFormatConfigWithSingleQuotedLiterals()
	want := LiteralQuoteConfig{Strategy: QuoteAlways, PreferredQuote: PreferredSingleQuote}
	if fc.Literal.Quote != want {
		t.Fatalf("Literal.Quote = %+v, want %+v", fc.Literal.Quote, want)
	}

	got, err := fc.FormatToplevelColumn(gcvctor.DateValue(civil.Date{Year: 2014, Month: 9, Day: 27}))
	if err != nil {
		t.Fatal(err)
	}
	if got != "DATE '2014-09-27'" {
		t.Fatalf("DATE literal = %q, want DATE '2014-09-27'", got)
	}

	got, err = fc.FormatToplevelColumn(gcvctor.StringValue("it's fine"))
	if err != nil {
		t.Fatal(err)
	}
	if got != `'it\'s fine'` {
		t.Fatalf("STRING literal = %q, want 'it\\'s fine'", got)
	}
}

func TestLiteralQuoteLegacyPreferredSingle(t *testing.T) {
	t.Parallel()

	fc := LiteralFormatConfigWithQuote(LiteralQuoteConfig{
		Strategy:       QuoteLegacy,
		PreferredQuote: PreferredSingleQuote,
	})

	tests := []struct {
		name string
		gcv  spanner.GenericColumnValue
		want string
	}{
		{name: "no quotes uses preferred single", gcv: gcvctor.StringValue("plain"), want: `'plain'`},
		{name: "only singles uses opposite double", gcv: gcvctor.StringValue("it's"), want: `"it's"`},
		{name: "only doubles uses opposite single", gcv: gcvctor.StringValue(`say "hi"`), want: `'say "hi"'`},
		{name: "both uses preferred single", gcv: gcvctor.StringValue(`"it's"`), want: internal.ToStringLiteralPolicy(`"it's"`, internal.QuotePolicy{
			Strategy: internal.QuoteStrategyLegacy, Preferred: internal.PreferredQuoteSingle,
		})},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := fc.FormatToplevelColumn(tt.gcv)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestLiteralQuoteLegacyAdaptive(t *testing.T) {
	t.Parallel()

	fc := LiteralFormatConfig()
	tests := []struct {
		name string
		gcv  spanner.GenericColumnValue
		want string
	}{
		{
			name: "only double quote in payload",
			gcv:  gcvctor.StringValue(`say "hi"`),
			want: `'say "hi"'`,
		},
		{
			name: "only single quote in payload",
			gcv:  gcvctor.StringValue("it's"),
			want: `"it's"`,
		},
		{
			name: "both quote characters",
			gcv:  gcvctor.StringValue(`"it's"`),
			want: `"\"it's\""`,
		},
		{
			name: "no quotes default double",
			gcv:  gcvctor.StringValue("plain"),
			want: `"plain"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := fc.FormatToplevelColumn(tt.gcv)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestLiteralQuoteNaNInf(t *testing.T) {
	t.Parallel()

	legacy := LiteralFormatConfig()
	doubleAlways := LiteralFormatConfigWithQuote(LiteralQuoteConfig{
		Strategy:       QuoteAlways,
		PreferredQuote: PreferredDoubleQuote,
	})

	cases := []struct {
		name string
		gcv  spanner.GenericColumnValue
		want string
	}{
		{"float64 nan legacy", gcvctor.Float64Value(math.NaN()), "CAST('nan' AS FLOAT64)"},
		{"float64 inf legacy", gcvctor.Float64Value(math.Inf(1)), "CAST('inf' AS FLOAT64)"},
		{"float64 neg inf legacy", gcvctor.Float64Value(math.Inf(-1)), "CAST('-inf' AS FLOAT64)"},
		{"float32 nan legacy", gcvctor.Float32Value(float32(math.NaN())), "CAST('nan' AS FLOAT32)"},
		{"float32 inf legacy", gcvctor.Float32Value(float32(math.Inf(1))), "CAST('inf' AS FLOAT32)"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := legacy.FormatToplevelColumn(tt.gcv)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("legacy got %q, want %q", got, tt.want)
			}
		})
	}

	doubleCases := []struct {
		name string
		gcv  spanner.GenericColumnValue
		want string
	}{
		{"float64 nan double", gcvctor.Float64Value(math.NaN()), `CAST("nan" AS FLOAT64)`},
		{"float64 inf double", gcvctor.Float64Value(math.Inf(1)), `CAST("inf" AS FLOAT64)`},
		{"float32 nan double", gcvctor.Float32Value(float32(math.NaN())), `CAST("nan" AS FLOAT32)`},
	}
	for _, tt := range doubleCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := doubleAlways.FormatToplevelColumn(tt.gcv)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("double always got %q, want %q", got, tt.want)
			}
		})
	}

	minEscapeDouble := LiteralFormatConfigWithQuote(LiteralQuoteConfig{
		Strategy:       QuoteMinEscape,
		PreferredQuote: PreferredDoubleQuote,
	})
	got, err := minEscapeDouble.FormatToplevelColumn(gcvctor.Float64Value(math.NaN()))
	if err != nil {
		t.Fatal(err)
	}
	if got != `CAST("nan" AS FLOAT64)` {
		t.Fatalf("min escape double NaN = %q, want CAST(\"nan\" AS FLOAT64)", got)
	}
}

func TestFormatNullableValueLiteralMatchesWithQuoteZero(t *testing.T) {
	t.Parallel()

	nv := spanner.NullString{StringVal: "hello", Valid: true}
	got, err := formatNullableValueLiteral(nv)
	if err != nil {
		t.Fatal(err)
	}
	want, err := formatNullableValueLiteralWithQuote(LiteralQuoteConfig{}, nv)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestLiteralQuoteFastPathMatchesSlowPath(t *testing.T) {
	t.Parallel()

	presets := []struct {
		name string
		fc   *FormatConfig
	}{
		{name: "legacy default", fc: LiteralFormatConfig()},
		{name: "single always", fc: LiteralFormatConfigWithSingleQuotedLiterals()},
		{name: "legacy preferred single", fc: LiteralFormatConfigWithQuote(LiteralQuoteConfig{
			Strategy: QuoteLegacy, PreferredQuote: PreferredSingleQuote,
		})},
		{name: "min escape single tie", fc: LiteralFormatConfigWithQuote(LiteralQuoteConfig{
			Strategy: QuoteMinEscape, PreferredQuote: PreferredSingleQuote,
		})},
		{name: "min escape double tie", fc: LiteralFormatConfigWithQuote(LiteralQuoteConfig{
			Strategy: QuoteMinEscape, PreferredQuote: PreferredDoubleQuote,
		})},
		{name: "always double", fc: LiteralFormatConfigWithQuote(LiteralQuoteConfig{
			Strategy: QuoteAlways, PreferredQuote: PreferredDoubleQuote,
		})},
	}

	scalars := []spanner.GenericColumnValue{
		gcvctor.StringValue("hello"),
		gcvctor.StringValue("it's"),
		gcvctor.BytesValue([]byte{0, 1}),
		gcvctor.DateValue(civil.Date{Year: 2020, Month: 1, Day: 2}),
		gcvctor.Float64Value(math.NaN()),
		gcvctor.Float32Value(float32(math.Inf(-1))),
	}

	for _, preset := range presets {
		t.Run(preset.name, func(t *testing.T) {
			t.Parallel()
			legacy := formatConfigNullableOnly(preset.fc)
			for i, gcv := range scalars {
				got, err := preset.fc.FormatToplevelColumn(gcv)
				if err != nil {
					t.Fatalf("scalar[%d] direct: %v", i, err)
				}
				want, err := legacy.FormatToplevelColumn(gcv)
				if err != nil {
					t.Fatalf("scalar[%d] nullable path: %v", i, err)
				}
				if diff := cmp.Diff(want, got); diff != "" {
					t.Fatalf("scalar[%d] mismatch (-nullable +direct):\n%s", i, diff)
				}
			}
		})
	}
}

func TestLiteralQuoteNormalizeAtFormatTime(t *testing.T) {
	t.Parallel()

	fc := LiteralFormatConfig()
	fc.Literal.Quote = LiteralQuoteConfig{
		Strategy:       QuoteStrategy(99),
		PreferredQuote: PreferredSingleQuote,
	}
	got, err := fc.FormatToplevelColumn(gcvctor.StringValue("plain"))
	if err != nil {
		t.Fatal(err)
	}
	want := `'plain'`
	if got != want {
		t.Fatalf("got %q, want %q (invalid strategy -> legacy + preserved single)", got, want)
	}
}

func TestLiteralQuoteEscapeCorners(t *testing.T) {
	t.Parallel()

	configs := []struct {
		name string
		cfg  LiteralQuoteConfig
	}{
		{name: "legacy_double", cfg: LiteralQuoteConfig{}},
		{name: "legacy_single", cfg: LiteralQuoteConfig{Strategy: QuoteLegacy, PreferredQuote: PreferredSingleQuote}},
		{name: "always_single", cfg: LiteralQuoteConfig{Strategy: QuoteAlways, PreferredQuote: PreferredSingleQuote}},
		{name: "always_double", cfg: LiteralQuoteConfig{Strategy: QuoteAlways, PreferredQuote: PreferredDoubleQuote}},
		{name: "minescape_single", cfg: LiteralQuoteConfig{Strategy: QuoteMinEscape, PreferredQuote: PreferredSingleQuote}},
		{name: "minescape_double", cfg: LiteralQuoteConfig{Strategy: QuoteMinEscape, PreferredQuote: PreferredDoubleQuote}},
	}
	payloads := []struct {
		name string
		gcv  spanner.GenericColumnValue
		s    string
	}{
		{name: "plain", gcv: gcvctor.StringValue("plain"), s: "plain"},
		{name: "only_single", gcv: gcvctor.StringValue("it's"), s: "it's"},
		{name: "only_double", gcv: gcvctor.StringValue(`say "hi"`), s: `say "hi"`},
		{name: "both_quotes", gcv: gcvctor.StringValue(`"it's"`), s: `"it's"`},
	}

	for _, config := range configs {
		t.Run(config.name, func(t *testing.T) {
			t.Parallel()
			fc := LiteralFormatConfigWithQuote(config.cfg)
			policy := toInternalQuotePolicy(config.cfg)
			for _, payload := range payloads {
				t.Run(payload.name, func(t *testing.T) {
					t.Parallel()
					got, err := fc.FormatToplevelColumn(payload.gcv)
					if err != nil {
						t.Fatal(err)
					}
					want := internal.ToStringLiteralPolicy(payload.s, policy)
					if got != want {
						t.Fatalf("got %q, want %q", got, want)
					}
				})
			}
		})
	}
}

func TestFormatProtoAsCastLiteralQuote(t *testing.T) {
	t.Parallel()

	gcv := gcvctor.ProtoValue("package.ProtoType", []byte("deadbeef"))

	legacy, err := LiteralFormatConfig().FormatToplevelColumn(gcv)
	if err != nil {
		t.Fatal(err)
	}
	if legacy != "CAST(b\"deadbeef\" AS `package.ProtoType`)" {
		t.Fatalf("legacy proto = %q", legacy)
	}

	single, err := LiteralFormatConfigWithSingleQuotedLiterals().FormatToplevelColumn(gcv)
	if err != nil {
		t.Fatal(err)
	}
	if single != "CAST(b'deadbeef' AS `package.ProtoType`)" {
		t.Fatalf("single-quoted proto = %q", single)
	}
}

func TestLiteralQuoteForFormatterNonFormatConfig(t *testing.T) {
	t.Parallel()

	if got := literalQuoteForFormatter(struct{}{}); got != (LiteralQuoteConfig{}) {
		t.Fatalf("non-FormatConfig formatter: got %+v, want zero", got)
	}
}

func ExampleLiteralFormatConfigWithSingleQuotedLiterals() {
	fc := LiteralFormatConfigWithSingleQuotedLiterals()
	date, _ := fc.FormatToplevelColumn(gcvctor.DateValue(civil.Date{Year: 2014, Month: 9, Day: 27}))
	str, _ := fc.FormatToplevelColumn(gcvctor.StringValue("it's fine"))
	fmt.Println(date)
	fmt.Println(str)
	// Output:
	// DATE '2014-09-27'
	// 'it\'s fine'
}
