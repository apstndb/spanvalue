package spanvalue

import (
	"errors"
	"math"
	"math/big"
	"testing"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/spantype/typector"
	"github.com/apstndb/spanvalue/gcvctor"
	"google.golang.org/protobuf/types/known/structpb"
)

// legacyStructField adapts a legacy FormatStructFieldFunc (taking
// *FormatConfig) to the Formatter-based callback that PluginForStruct and
// WithStructFormat accept. The formatter passed to plugins by
// FormatColumn is always the *FormatConfig itself, so the assertion holds for
// every config under test; plugin authors outside this package write
// Formatter-based callbacks directly (the breaking phase of #253 aligns
// FormatStructFieldFunc itself).
func legacyStructField(f FormatStructFieldFunc) func(Formatter, *sppb.StructType_Field, *structpb.Value) (string, error) {
	return func(formatter Formatter, sf *sppb.StructType_Field, value *structpb.Value) (string, error) {
		return f(formatter.(*FormatConfig), sf, value)
	}
}

type dogfoodCase struct {
	name string
	gcv  spanner.GenericColumnValue
}

// dogfoodBattery is a representative GCV set: every scalar type including
// PG-annotated ones, NULLs of each, arrays (empty, NULL, with NULL elements,
// of STRUCT), nested STRUCTs, and PROTO/ENUM. Every entry must format
// successfully under all four presets so the equivalence check below is over
// outputs, not error strings.
func dogfoodBattery(t *testing.T) []dogfoodCase {
	t.Helper()

	structGCV := gcvctor.MustStructValueOf(
		[]string{"id", "name"},
		[]spanner.GenericColumnValue{gcvctor.Int64Value(1), gcvctor.StringValue("east")},
	)
	protoType := &sppb.Type{Code: sppb.TypeCode_PROTO, ProtoTypeFqn: "examples.Example"}
	enumType := &sppb.Type{Code: sppb.TypeCode_ENUM, ProtoTypeFqn: "examples.Color"}

	cases := []dogfoodCase{
		{"bool", gcvctor.BoolValue(true)},
		{"int64", gcvctor.Int64Value(42)},
		{"float32", gcvctor.Float32Value(3.25)},
		{"float32 integral", gcvctor.Float32Value(2)},
		{"float64", gcvctor.Float64Value(-1.5)},
		{"float64 nan", gcvctor.Float64Value(math.NaN())},
		{"float64 inf", gcvctor.Float64Value(math.Inf(1))},
		{"string", gcvctor.StringValue("hello")},
		{"string quotes", gcvctor.StringValue(`it's "quoted"`)},
		{"bytes", gcvctor.BytesValue([]byte{0x00, 0xde, 0xad})},
		{"timestamp", gcvctor.MustTimestampStringValue("2024-03-01T12:34:56.789Z")},
		{"date", gcvctor.MustDateStringValue("2024-03-01")},
		{"numeric", gcvctor.NumericValue(big.NewRat(314, 100))},
		{"pg numeric", gcvctor.PGNumericValue(big.NewRat(-5, 2))},
		{"json", gcvctor.MustJSONValue(map[string]any{"a": 1})},
		{"pg jsonb", gcvctor.MustPGJSONBValue([]any{1, "x"})},
		{"interval", gcvctor.MustIntervalStringValue("P1Y2M3DT4H5M6S")},
		{"uuid", gcvctor.MustUUIDStringValue("12345678-1234-5678-1234-567812345678")},
		{"proto", gcvctor.ProtoValue("examples.Example", []byte{0x08, 0x01})},
		{"enum", gcvctor.EnumValue("examples.Color", 2)},
		{"null pg numeric", gcvctor.NullOf(typector.PGNumeric())},
		{"null pg jsonb", gcvctor.NullOf(typector.PGJSONB())},
		{"null proto", gcvctor.NullOf(protoType)},
		{"null enum", gcvctor.NullOf(enumType)},
		{"empty array", gcvctor.EmptyArrayFromCode(sppb.TypeCode_INT64)},
		{"null array", gcvctor.NullArrayFromCode(sppb.TypeCode_INT64)},
		{"array with null elem", gcvctor.MustArrayValueOf(
			typector.CodeToSimpleType(sppb.TypeCode_INT64),
			gcvctor.Int64Value(1), gcvctor.NullFromCode(sppb.TypeCode_INT64), gcvctor.Int64Value(3),
		)},
		{"string array", gcvctor.MustArrayValue(gcvctor.StringValue("a"), gcvctor.StringValue("b"))},
		{"array of struct", gcvctor.MustArrayValueOf(structGCV.Type, structGCV)},
		{"struct", structGCV},
		{"struct unnamed field", gcvctor.MustStructValueOf(
			[]string{""},
			[]spanner.GenericColumnValue{gcvctor.Int64Value(7)},
		)},
		{"nested struct", gcvctor.MustStructValueOfFields(
			gcvctor.StructFieldKVOf("arr", gcvctor.MustArrayValue(gcvctor.Int64Value(1), gcvctor.Int64Value(2))),
			gcvctor.StructFieldKVOf("inner", structGCV),
		)},
		{"null struct", gcvctor.NullOf(structGCV.Type)},
	}
	for code, name := range map[sppb.TypeCode]string{
		sppb.TypeCode_BOOL:      "null bool",
		sppb.TypeCode_INT64:     "null int64",
		sppb.TypeCode_FLOAT32:   "null float32",
		sppb.TypeCode_FLOAT64:   "null float64",
		sppb.TypeCode_STRING:    "null string",
		sppb.TypeCode_BYTES:     "null bytes",
		sppb.TypeCode_TIMESTAMP: "null timestamp",
		sppb.TypeCode_DATE:      "null date",
		sppb.TypeCode_NUMERIC:   "null numeric",
		sppb.TypeCode_JSON:      "null json",
		sppb.TypeCode_INTERVAL:  "null interval",
		sppb.TypeCode_UUID:      "null uuid",
	} {
		cases = append(cases, dogfoodCase{name, gcvctor.NullFromCode(code)})
	}
	return cases
}

// TestNewFormatConfigDogfoodsPresets is the #253 acceptance evidence: each
// preset is rebuilt through NewFormatConfig from the preset's own plugin
// functions plus its FormatArray/FormatStruct/FormatNullable behaviors
// expressed via the builder options, and the output must be byte-identical
// across the battery at both toplevel values.
//
// The rebuilds use unexported preset internals (formatNullableValueSimple,
// formatNullableValueLiteral, formatSimpleStructField, formatTypedStructParen)
// that only this internal test package can name; external callers write
// equivalent closures. Note the symmetry: in each preset the scalar fast-path
// plugin shadows the FormatNullable field, and in the rebuilt config the same
// plugin shadows the WithScalarFormatter tail — the canonical chain shape #252
// describes.
//
// Known, intentional gaps (not byte-identical, pinned separately below):
//   - Unknown scalar type codes error differently: presets reach the
//     FormatNullable slow path and report ErrUnknownType; builder-built
//     configs have no FormatNullable field, so the built-in path reports
//     ErrFormatNullableRequired (see the NewFormatConfig doc).
//   - Only the literal preset's default quote options are dogfooded:
//     non-default LiteralFormatOptions live on the deprecated Literal field,
//     which the builder intentionally does not set; #253's breaking phase
//     moves them into constructor-captured plugin state.
func TestNewFormatConfigDogfoodsPresets(t *testing.T) {
	t.Parallel()

	configs := []struct {
		name    string
		preset  *FormatConfig
		rebuilt func() (*FormatConfig, error)
	}{
		{
			name:   "simple",
			preset: SimpleFormatConfig(),
			rebuilt: func() (*FormatConfig, error) {
				return NewFormatConfig(
					WithNullString(nullStringClientLib),
					WithPlugin(FormatSimpleValue),
					WithArrayFormat(FormatUntypedArray),
					WithStructFormat(legacyStructField(FormatTypelessStructField), FormatTupleStruct),
					WithScalarFormatter(formatNullableValueSimple),
				)
			},
		},
		{
			name:   "spanner cli compatible",
			preset: SpannerCLICompatibleFormatConfig(),
			rebuilt: func() (*FormatConfig, error) {
				return NewFormatConfig(
					WithNullString(nullStringUpperCase),
					WithPlugin(FormatSpannerCLIValue),
					WithArrayFormat(FormatUntypedArray),
					WithStructFormat(legacyStructField(FormatSimpleStructField), FormatBracketStruct),
					WithScalarFormatter(FormatNullableSpannerCLICompatible),
				)
			},
		},
		{
			name:   "literal",
			preset: LiteralFormatConfig(),
			rebuilt: func() (*FormatConfig, error) {
				// Most recent WithPlugin runs first, so register in reverse
				// of the preset chain [FormatProtoAsCast, FormatEnumAsCast,
				// FormatLiteralValue].
				return NewFormatConfig(
					WithNullString(nullStringUpperCase),
					WithPlugin(FormatLiteralValue),
					WithPlugin(FormatEnumAsCast),
					WithPlugin(FormatProtoAsCast),
					WithArrayFormat(FormatOptionallyTypedArray),
					WithStructFormat(legacyStructField(formatSimpleStructField), formatTypedStructParen),
					WithScalarFormatter(formatNullableValueLiteral),
				)
			},
		},
		{
			name:   "json",
			preset: JSONFormatConfig(),
			rebuilt: func() (*FormatConfig, error) {
				return NewFormatConfig(
					WithNullString("null"),
					WithPlugin(FormatJSONSimpleValue),
					WithArrayFormat(FormatCompactArray),
					WithStructFormat(legacyStructField(FormatSimpleStructField), NewJSONObjectStructFormatter(nil)),
					WithScalarFormatter(FormatNullableSpannerCLICompatible),
				)
			},
		},
	}

	battery := dogfoodBattery(t)
	for _, cfg := range configs {
		t.Run(cfg.name, func(t *testing.T) {
			t.Parallel()

			rebuilt, err := cfg.rebuilt()
			if err != nil {
				t.Fatalf("NewFormatConfig() error = %v", err)
			}
			for _, tc := range battery {
				for _, toplevel := range []bool{true, false} {
					want, err := cfg.preset.FormatColumn(tc.gcv, toplevel)
					if err != nil {
						t.Errorf("%s (toplevel=%v): preset error = %v", tc.name, toplevel, err)
						continue
					}
					got, err := rebuilt.FormatColumn(tc.gcv, toplevel)
					if err != nil {
						t.Errorf("%s (toplevel=%v): rebuilt error = %v", tc.name, toplevel, err)
						continue
					}
					if got != want {
						t.Errorf("%s (toplevel=%v): rebuilt = %q, preset = %q", tc.name, toplevel, got, want)
					}
				}
			}
		})
	}
}

// TestNewFormatConfigUnknownCodeErrorClass pins the documented divergence for
// type codes outside the scalar domain: the preset's FormatNullable slow path
// reports ErrUnknownType, while a builder-built config (no FormatNullable
// field) reports ErrFormatNullableRequired from the built-in scalar path after
// the PluginFromNullable tail defers.
func TestNewFormatConfigUnknownCodeErrorClass(t *testing.T) {
	t.Parallel()

	unknown := spanner.GenericColumnValue{
		Type:  &sppb.Type{Code: sppb.TypeCode(9999)},
		Value: structpb.NewStringValue("x"),
	}

	if _, err := SimpleFormatConfig().FormatToplevelColumn(unknown); !errors.Is(err, ErrUnknownType) {
		t.Errorf("preset error = %v, want ErrUnknownType", err)
	}

	rebuilt, err := NewFormatConfig(
		WithNullString(nullStringClientLib),
		WithPlugin(FormatSimpleValue),
		WithArrayFormat(FormatUntypedArray),
		WithStructFormat(legacyStructField(FormatTypelessStructField), FormatTupleStruct),
		WithScalarFormatter(formatNullableValueSimple),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rebuilt.FormatToplevelColumn(unknown); !errors.Is(err, ErrFormatNullableRequired) {
		t.Errorf("rebuilt error = %v, want ErrFormatNullableRequired", err)
	}
}
