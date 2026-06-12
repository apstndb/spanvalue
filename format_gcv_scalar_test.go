package spanvalue

import (
	"errors"
	"math"
	"math/big"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/spantype/typector"
	"github.com/apstndb/spanvalue/gcvctor"
	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/samber/lo"
	"google.golang.org/protobuf/types/known/structpb"
)

// The nullablePathConfig helpers rebuild each preset with its scalar plugin
// replaced by the Decode-based [PluginFromNullable] tail, so parity tests and
// benchmarks can compare the direct scalar plugins against the Decode path.
// They mirror the preset constructors' array/struct plugins.

func simpleNullablePathConfig() *FormatConfig {
	return &FormatConfig{
		NullString: nullStringClientLib,
		FormatComplexPlugins: []FormatComplexFunc{
			PluginForArray(FormatUntypedArray),
			PluginForStruct(FormatTypelessStructField, FormatTupleStruct),
			PluginFromNullable(formatNullableValueSimple),
		},
	}
}

func literalNullablePathConfig(q LiteralQuoteConfig) *FormatConfig {
	return &FormatConfig{
		NullString: nullStringUpperCase,
		FormatComplexPlugins: []FormatComplexFunc{
			protoAsCastPlugin(q),
			FormatEnumAsCast,
			PluginForArray(FormatOptionallyTypedArray),
			PluginForStruct(FormatSimpleStructField, FormatTypedStruct),
			PluginFromNullable(func(nv NullableValue) (string, error) {
				return formatNullableValueLiteralWithQuote(q, nv)
			}),
		},
	}
}

func spannerCLINullablePathConfig() *FormatConfig {
	return &FormatConfig{
		NullString: nullStringUpperCase,
		FormatComplexPlugins: []FormatComplexFunc{
			PluginForArray(FormatUntypedArray),
			PluginForStruct(FormatSimpleStructField, FormatBracketStruct),
			PluginFromNullable(FormatNullableSpannerCLICompatible),
		},
	}
}

// jsonNullablePathConfig approximates the JSON preset on the Decode path for
// benchmarks only: FormatNullableSpannerCLICompatible does not emit JSON
// quoting, so it is not output-equivalent to FormatJSONSimpleValue.
func jsonNullablePathConfig() *FormatConfig {
	return &FormatConfig{
		NullString: "null",
		FormatComplexPlugins: []FormatComplexFunc{
			PluginForArray(FormatCompactArray),
			PluginForStruct(FormatSimpleStructField, NewJSONObjectStructFormatter(nil)),
			PluginFromNullable(FormatNullableSpannerCLICompatible),
		},
	}
}

func TestFormatGCVScalarPluginsMatchNullablePath(t *testing.T) {
	t.Parallel()

	rat := big.NewRat(314, 100)
	pgRat := big.NewRat(22, 7)
	ts := lo.Must(time.Parse(time.RFC3339Nano, "2020-01-02T03:04:05.123456789Z"))
	uid := uuid.MustParse("123e4567-e89b-12d3-a456-426614174000")

	scalars := []spanner.GenericColumnValue{
		gcvctor.BoolValue(true),
		gcvctor.Int64Value(42),
		gcvctor.Float32Value(float32(1.25)),
		gcvctor.Float32Value(1),
		gcvctor.Float64Value(math.Pi),
		gcvctor.Float64Value(1),
		gcvctor.StringValue("hello"),
		gcvctor.BytesValue([]byte{0, 1, 2, 255}),
		gcvctor.BytesValue([]byte(`a\b`)),
		gcvctor.DateValue(civil.Date{Year: 2020, Month: 1, Day: 2}),
		gcvctor.TimestampValue(ts),
		gcvctor.NumericValue(rat),
		gcvctor.PGNumericValue(pgRat),
		lo.Must(gcvctor.JSONValue(map[string]any{"k": 1})),
		lo.Must(gcvctor.PGJSONBValue(map[string]int{"k": 1})),
		lo.Must(gcvctor.IntervalStringValue("P1Y2M3DT4H5M6.789S")),
		gcvctor.UUIDValue(uid),
		gcvctor.Float64Value(math.NaN()),
		gcvctor.Float64Value(math.Inf(1)),
	}

	presets := []struct {
		name   string
		fc     *FormatConfig
		legacy *FormatConfig
	}{
		{name: "simple", fc: SimpleFormatConfig(), legacy: simpleNullablePathConfig()},
		{name: "literal", fc: LiteralFormatConfig(), legacy: literalNullablePathConfig(LiteralQuoteConfig{})},
		{name: "spanner_cli", fc: SpannerCLICompatibleFormatConfig(), legacy: spannerCLINullablePathConfig()},
	}

	for _, preset := range presets {
		t.Run(preset.name, func(t *testing.T) {
			t.Parallel()
			legacy := preset.legacy
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
					t.Fatalf("scalar[%d] (%s) mismatch (-nullable +direct):\n%s", i, gcv.Type.GetCode(), diff)
				}
			}
		})
	}
}

// TestFormatConfig_noScalarHandlerErrUnhandledValue pins the runtime coverage
// contract: a chain without a scalar handler fails non-NULL scalars with
// ErrUnhandledValue, while NULL scalars still render as NullString.
func TestFormatConfig_noScalarHandlerErrUnhandledValue(t *testing.T) {
	t.Parallel()

	fc := &FormatConfig{
		NullString: nullStringClientLib,
		FormatComplexPlugins: []FormatComplexFunc{
			PluginForArray(FormatUntypedArray),
		},
	}
	_, err := fc.FormatToplevelColumn(gcvctor.BoolValue(true))
	if !errors.Is(err, ErrUnhandledValue) {
		t.Fatalf("got err %v want %v", err, ErrUnhandledValue)
	}

	got, err := fc.FormatToplevelColumn(gcvctor.NullOf(typector.CodeToSimpleType(sppb.TypeCode_BOOL)))
	if err != nil {
		t.Fatal(err)
	}
	if got != fc.GetNullString() {
		t.Fatalf("got %q want %q", got, fc.GetNullString())
	}
}

// TestFormatConfig_prependedScalarOverrideShadowsPreset pins the migration
// recipe for the removed FormatNullable field: prepend PluginFromNullable so
// the override runs before the preset scalar plugin.
func TestFormatConfig_prependedScalarOverrideShadowsPreset(t *testing.T) {
	t.Parallel()

	fc := SimpleFormatConfig().WithComplexPlugin(
		PluginFromNullable(func(NullableValue) (string, error) { return "CUSTOM", nil }))

	got, err := fc.FormatToplevelColumn(gcvctor.BoolValue(true))
	if err != nil {
		t.Fatal(err)
	}
	if got != "CUSTOM" {
		t.Fatalf("got %q want CUSTOM", got)
	}
}

func TestValidateFloatWire_nonFiniteStringsOnly(t *testing.T) {
	t.Parallel()

	for _, s := range []string{"NaN", "Infinity", "-Infinity"} {
		if err := validateFloatWire(structpb.NewStringValue(s), sppb.TypeCode_FLOAT64); err != nil {
			t.Errorf("validateFloatWire(%q): %v", s, err)
		}
	}
	if err := validateFloatWire(structpb.NewStringValue("1.5"), sppb.TypeCode_FLOAT64); err == nil {
		t.Fatal("expected error for finite float as string wire")
	}
	if err := validateFloatWire(structpb.NewNumberValue(1.5), sppb.TypeCode_FLOAT64); err != nil {
		t.Fatalf("number wire: %v", err)
	}
}

func TestFormatSimpleValue_fallthroughUnknownTypeCode(t *testing.T) {
	t.Parallel()

	const want = "CUSTOM_UNKNOWN"
	handler := FormatComplexFunc(func(_ Formatter, value spanner.GenericColumnValue, _ bool) (string, error) {
		if value.Type.GetCode() == sppb.TypeCode_TYPE_CODE_UNSPECIFIED {
			return want, nil
		}
		return "", ErrFallthrough
	})

	fc := SimpleFormatConfig()
	fc.FormatComplexPlugins = append(fc.FormatComplexPlugins, handler)

	gcv := spanner.GenericColumnValue{
		Type:  typector.CodeToSimpleType(sppb.TypeCode_TYPE_CODE_UNSPECIFIED),
		Value: structpb.NewStringValue("payload"),
	}
	got, err := fc.FormatToplevelColumn(gcv)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestFormatSimpleValue_rejectsMalformedWireKind(t *testing.T) {
	t.Parallel()

	gcv := spanner.GenericColumnValue{
		Type:  typector.CodeToSimpleType(sppb.TypeCode_BOOL),
		Value: structpb.NewStringValue("true"),
	}
	_, err := SimpleFormatConfig().FormatToplevelColumn(gcv)
	if err == nil {
		t.Fatal("expected error for BOOL typed value with string wire")
	}
}
