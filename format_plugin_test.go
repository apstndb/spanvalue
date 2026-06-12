package spanvalue_test

import (
	"errors"
	"math/big"
	"testing"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/spantype/typector"
	"github.com/apstndb/spanvalue"
	"github.com/apstndb/spanvalue/gcvctor"
	"google.golang.org/protobuf/types/known/structpb"
)

// constPlugin returns out for every value it sees.
func constPlugin(out string) spanvalue.FormatComplexFunc {
	return func(spanvalue.Formatter, spanner.GenericColumnValue, bool) (string, error) {
		return out, nil
	}
}

func pluginConfig(plugin spanvalue.FormatComplexFunc) *spanvalue.FormatConfig {
	return spanvalue.SimpleFormatConfig().WithComplexPlugin(plugin)
}

func TestPluginForTypeCode(t *testing.T) {
	t.Parallel()

	fc := pluginConfig(spanvalue.PluginForTypeCode(sppb.TypeCode_INT64, constPlugin("matched")))

	got, err := fc.FormatToplevelColumn(gcvctor.Int64Value(1))
	if err != nil || got != "matched" {
		t.Errorf("INT64 = (%q, %v), want (matched, nil)", got, err)
	}

	// Other codes fall through to the built-in formatting.
	got, err = fc.FormatToplevelColumn(gcvctor.StringValue("s"))
	if err != nil || got != "s" {
		t.Errorf("STRING = (%q, %v), want (s, nil)", got, err)
	}

	// NULL of the matched code still reaches the plugin (NULL is not
	// pre-filtered; see PluginSkippingNull for the opt-out).
	got, err = fc.FormatToplevelColumn(gcvctor.NullFromCode(sppb.TypeCode_INT64))
	if err != nil || got != "matched" {
		t.Errorf("NULL INT64 = (%q, %v), want (matched, nil)", got, err)
	}
}

func TestPluginForType(t *testing.T) {
	t.Parallel()

	// Annotation-aware predicate: only PG_JSONB, not plain JSON.
	pgJSONB := func(typ *sppb.Type) bool {
		return typ.GetCode() == sppb.TypeCode_JSON &&
			typ.GetTypeAnnotation() == sppb.TypeAnnotationCode_PG_JSONB
	}
	fc := pluginConfig(spanvalue.PluginForType(pgJSONB, constPlugin("jsonb")))

	pg, err := gcvctor.PGJSONBValue(map[string]int{"a": 1})
	if err != nil {
		t.Fatal(err)
	}
	got, err := fc.FormatToplevelColumn(pg)
	if err != nil || got != "jsonb" {
		t.Errorf("PG_JSONB = (%q, %v), want (jsonb, nil)", got, err)
	}

	plain, err := gcvctor.JSONValue(map[string]int{"a": 1})
	if err != nil {
		t.Fatal(err)
	}
	got, err = fc.FormatToplevelColumn(plain)
	if err != nil || got != `{"a":1}` {
		t.Errorf("plain JSON = (%q, %v), want pass-through to built-in", got, err)
	}
}

func TestPluginSkippingNull(t *testing.T) {
	t.Parallel()

	fc := pluginConfig(spanvalue.PluginForTypeCode(sppb.TypeCode_INT64,
		spanvalue.PluginSkippingNull(constPlugin("matched"))))

	got, err := fc.FormatToplevelColumn(gcvctor.Int64Value(1))
	if err != nil || got != "matched" {
		t.Errorf("INT64 = (%q, %v), want (matched, nil)", got, err)
	}

	// NULL falls through to the built-in scalar handling (GetNullString).
	got, err = fc.FormatToplevelColumn(gcvctor.NullFromCode(sppb.TypeCode_INT64))
	if err != nil || got != fc.GetNullString() {
		t.Errorf("NULL INT64 = (%q, %v), want (%q, nil)", got, err, fc.GetNullString())
	}
}

func TestPluginCombinatorErrorPropagation(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("boom")
	fc := pluginConfig(spanvalue.PluginForTypeCode(sppb.TypeCode_INT64,
		func(spanvalue.Formatter, spanner.GenericColumnValue, bool) (string, error) {
			return "", wantErr
		}))
	if _, err := fc.FormatToplevelColumn(gcvctor.Int64Value(1)); !errors.Is(err, wantErr) {
		t.Errorf("error = %v, want %v", err, wantErr)
	}

	// A body returning ErrFallthrough still defers even when the guard matched.
	fc = pluginConfig(spanvalue.PluginForTypeCode(sppb.TypeCode_STRING,
		func(spanvalue.Formatter, spanner.GenericColumnValue, bool) (string, error) {
			return "", spanvalue.ErrFallthrough
		}))
	got, err := fc.FormatToplevelColumn(gcvctor.StringValue("s"))
	if err != nil || got != "s" {
		t.Errorf("fallthrough body = (%q, %v), want built-in (s, nil)", got, err)
	}
}

func TestPluginFromNullable(t *testing.T) {
	t.Parallel()

	numericOverride := spanvalue.PluginFromNullable(spanvalue.NullableFormatterFor(
		func(v spanner.NullNumeric) (string, error) {
			return "N:" + v.Numeric.FloatString(2), nil
		}))
	fc := pluginConfig(numericOverride)

	numeric := gcvctor.NumericValue(big.NewRat(3, 2))
	got, err := fc.FormatToplevelColumn(numeric)
	if err != nil || got != "N:1.50" {
		t.Errorf("NUMERIC = (%q, %v), want (N:1.50, nil)", got, err)
	}

	// Values the typed formatter does not claim keep the preset behavior.
	got, err = fc.FormatToplevelColumn(gcvctor.StringValue("s"))
	if err != nil || got != "s" {
		t.Errorf("STRING = (%q, %v), want preset (s, nil)", got, err)
	}

	// NULL NUMERIC keeps the preset null string (NULL falls through).
	got, err = fc.FormatToplevelColumn(gcvctor.NullFromCode(sppb.TypeCode_NUMERIC))
	if err != nil || got != fc.GetNullString() {
		t.Errorf("NULL NUMERIC = (%q, %v), want (%q, nil)", got, err, fc.GetNullString())
	}

	// The override also applies inside ARRAY<NUMERIC> (plugins run per
	// element).
	arr, err := gcvctor.ArrayValueOf(typector.Numeric(), numeric)
	if err != nil {
		t.Fatal(err)
	}
	got, err = fc.FormatToplevelColumn(arr)
	if err != nil || got != "[N:1.50]" {
		t.Errorf("ARRAY<NUMERIC> = (%q, %v), want ([N:1.50], nil)", got, err)
	}

	// Unknown type codes fall through to the built-in coverage error
	// instead of becoming this plugin's error.
	_, err = fc.FormatToplevelColumn(spanner.GenericColumnValue{
		Type:  &sppb.Type{Code: sppb.TypeCode(9999)},
		Value: structpb.NewStringValue("x"),
	})
	if !errors.Is(err, spanvalue.ErrUnknownType) {
		t.Errorf("unknown code error = %v, want ErrUnknownType from built-ins", err)
	}
}

// TestPluginFromNullableAnnotationDispatch pins that the Decode-based
// dispatch distinguishes PG-annotated wrappers, so a PGNumeric-typed
// formatter does not claim plain NUMERIC.
func TestPluginFromNullableAnnotationDispatch(t *testing.T) {
	t.Parallel()

	fc := pluginConfig(spanvalue.PluginFromNullable(spanvalue.NullableFormatterFor(
		func(v spanner.PGNumeric) (string, error) {
			return "PG:" + v.Numeric, nil
		})))

	got, err := fc.FormatToplevelColumn(gcvctor.PGNumericValue(big.NewRat(3, 2)))
	if err != nil || got != "PG:1.500000000" {
		t.Errorf("PG_NUMERIC = (%q, %v), want (PG:1.500000000, nil)", got, err)
	}

	// Plain NUMERIC is decoded to NullNumeric, not PGNumeric: falls through.
	got, err = fc.FormatToplevelColumn(gcvctor.NumericValue(big.NewRat(3, 2)))
	if err != nil || got != "1.500000000" {
		t.Errorf("plain NUMERIC = (%q, %v), want preset wire string", got, err)
	}
}
