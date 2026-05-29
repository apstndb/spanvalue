package spanvalue

import (
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

func formatConfigNullableOnly(fc *FormatConfig) *FormatConfig {
	return FormatConfigWithoutScalarPlugins(fc)
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
		gcvctor.Float64Value(math.Pi),
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
		name string
		fc   *FormatConfig
	}{
		{name: "simple", fc: SimpleFormatConfig()},
		{name: "literal", fc: LiteralFormatConfig()},
		{name: "spanner_cli", fc: SpannerCLICompatibleFormatConfig()},
	}

	for _, preset := range presets {
		preset := preset
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
					t.Fatalf("scalar[%d] (%s) mismatch (-nullable +direct):\n%s", i, gcv.Type.GetCode(), diff)
				}
			}
		})
	}
}

func TestFormatConfig_customFormatNullableUsesHook(t *testing.T) {
	t.Parallel()

	fc := SimpleFormatConfig()
	fc.FormatNullable = func(NullableValue) (string, error) { return "CUSTOM", nil }

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
