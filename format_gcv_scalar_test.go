package spanvalue

import (
	"math"
	"math/big"
	"reflect"
	"slices"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	"github.com/apstndb/spanvalue/gcvctor"
	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/samber/lo"
)

var scalarFastPathPluginPCs = func() []uintptr {
	plugins := []FormatComplexFunc{
		FormatSimpleValue,
		FormatLiteralValue,
		FormatSpannerCLIValue,
		FormatJSONSimpleValue,
	}
	pcs := make([]uintptr, len(plugins))
	for i, p := range plugins {
		pcs[i] = reflect.ValueOf(p).Pointer()
	}
	return pcs
}()

func isScalarFastPathPlugin(f FormatComplexFunc) bool {
	if f == nil {
		return false
	}
	return slices.Contains(scalarFastPathPluginPCs, reflect.ValueOf(f).Pointer())
}

func formatConfigNullableOnly(fc *FormatConfig) *FormatConfig {
	clone := fc.Clone()
	clone.FormatComplexPlugins = slices.DeleteFunc(clone.FormatComplexPlugins, isScalarFastPathPlugin)
	return clone
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
