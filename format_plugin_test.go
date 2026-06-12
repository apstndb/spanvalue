package spanvalue_test

import (
	"errors"
	"fmt"
	"math/big"
	"strings"
	"testing"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/spantype"
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

	// A nil Type falls through without invoking the predicate; the built-in
	// handling classifies the malformed value.
	matchPanics := spanvalue.PluginForType(func(t *sppb.Type) bool {
		if t == nil {
			panic("predicate must not see nil Type")
		}
		return true
	}, constPlugin("never"))
	_, err = pluginConfig(matchPanics).FormatToplevelColumn(spanner.GenericColumnValue{
		Value: structpb.NewStringValue("x"),
	})
	if err == nil {
		t.Error("nil Type: want built-in malformed-wire error, got nil")
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

	// Unknown type codes fall through to the chain's coverage error
	// instead of becoming this plugin's error.
	_, err = fc.FormatToplevelColumn(spanner.GenericColumnValue{
		Type:  &sppb.Type{Code: sppb.TypeCode(9999)},
		Value: structpb.NewStringValue("x"),
	})
	if !errors.Is(err, spanvalue.ErrUnhandledValue) {
		t.Errorf("unknown code error = %v, want ErrUnhandledValue from the chain", err)
	}
}

func TestPluginForArray(t *testing.T) {
	t.Parallel()

	join := func(typ *sppb.Type, toplevel bool, elemStrings []string) (string, error) {
		return fmt.Sprintf("%v<%s>", toplevel, strings.Join(elemStrings, ";")), nil
	}
	fc := pluginConfig(spanvalue.PluginForArray(join))

	// Elements recurse through the whole chain: NULL elements render the
	// config's null string, scalars keep the preset formatting.
	arr, err := gcvctor.ArrayValue(gcvctor.Int64Value(1), gcvctor.NullFromCode(sppb.TypeCode_INT64))
	if err != nil {
		t.Fatal(err)
	}
	got, err := fc.FormatToplevelColumn(arr)
	if want := "true<1;" + fc.GetNullString() + ">"; err != nil || got != want {
		t.Errorf("ARRAY = (%q, %v), want (%q, nil)", got, err, want)
	}

	// The toplevel flag is false for nested values.
	got, err = fc.FormatColumn(arr, false)
	if want := "false<1;" + fc.GetNullString() + ">"; err != nil || got != want {
		t.Errorf("nested ARRAY = (%q, %v), want (%q, nil)", got, err, want)
	}

	// Empty arrays are non-NULL: join sees zero elements.
	got, err = fc.FormatToplevelColumn(gcvctor.EmptyArrayFromCode(sppb.TypeCode_INT64))
	if err != nil || got != "true<>" {
		t.Errorf("empty ARRAY = (%q, %v), want (true<>, nil)", got, err)
	}

	// NULL arrays fall through so the built-in handling renders the null
	// string (see PluginForArray doc for the typed-NULL alternative).
	got, err = fc.FormatToplevelColumn(gcvctor.NullArrayFromCode(sppb.TypeCode_INT64))
	if err != nil || got != fc.GetNullString() {
		t.Errorf("NULL ARRAY = (%q, %v), want (%q, nil)", got, err, fc.GetNullString())
	}

	// Non-ARRAY values fall through to the rest of the chain.
	got, err = fc.FormatToplevelColumn(gcvctor.Int64Value(7))
	if err != nil || got != "7" {
		t.Errorf("INT64 = (%q, %v), want (7, nil)", got, err)
	}

	// Malformed wire (non-list payload) is the same error class as the
	// built-in ARRAY branch, not a fallthrough.
	_, err = fc.FormatToplevelColumn(spanner.GenericColumnValue{
		Type:  typector.ElemCodeToArrayType(sppb.TypeCode_INT64),
		Value: structpb.NewStringValue("not-a-list"),
	})
	if !errors.Is(err, spanvalue.ErrUnexpectedComplexValueKind) {
		t.Errorf("malformed ARRAY error = %v, want ErrUnexpectedComplexValueKind", err)
	}
}

func TestPluginForStruct(t *testing.T) {
	t.Parallel()

	field := func(f spanvalue.Formatter, sf *sppb.StructType_Field, value *structpb.Value) (string, error) {
		s, err := f.FormatColumn(spanner.GenericColumnValue{Type: sf.GetType(), Value: value}, false)
		if err != nil {
			return "", err
		}
		return sf.GetName() + "=" + s, nil
	}
	paren := func(typ *sppb.Type, toplevel bool, fieldStrings []string) (string, error) {
		return fmt.Sprintf("%v{%s}", toplevel, strings.Join(fieldStrings, ",")), nil
	}
	fc := pluginConfig(spanvalue.PluginForStruct(field, paren))

	structValue, err := gcvctor.StructValueOf(
		[]string{"id", "name"},
		[]spanner.GenericColumnValue{gcvctor.Int64Value(1), gcvctor.NullFromCode(sppb.TypeCode_STRING)},
	)
	if err != nil {
		t.Fatal(err)
	}
	got, err := fc.FormatToplevelColumn(structValue)
	if want := "true{id=1,name=" + fc.GetNullString() + "}"; err != nil || got != want {
		t.Errorf("STRUCT = (%q, %v), want (%q, nil)", got, err, want)
	}

	// NULL structs fall through so the built-in handling renders the null string.
	got, err = fc.FormatToplevelColumn(gcvctor.NullOf(structValue.Type))
	if err != nil || got != fc.GetNullString() {
		t.Errorf("NULL STRUCT = (%q, %v), want (%q, nil)", got, err, fc.GetNullString())
	}

	// Non-STRUCT values fall through to the rest of the chain.
	got, err = fc.FormatToplevelColumn(gcvctor.StringValue("s"))
	if err != nil || got != "s" {
		t.Errorf("STRING = (%q, %v), want (s, nil)", got, err)
	}

	// Error classes match the built-in STRUCT branch: count mismatch and
	// malformed wire are real errors, not fallthrough.
	_, err = fc.FormatToplevelColumn(spanner.GenericColumnValue{
		Type: structValue.Type,
		Value: structpb.NewListValue(&structpb.ListValue{
			Values: []*structpb.Value{structpb.NewStringValue("1")},
		}),
	})
	if !errors.Is(err, spanvalue.ErrMismatchedFields) {
		t.Errorf("mismatched STRUCT error = %v, want ErrMismatchedFields", err)
	}
	_, err = fc.FormatToplevelColumn(spanner.GenericColumnValue{
		Type:  structValue.Type,
		Value: structpb.NewStringValue("not-a-list"),
	})
	if !errors.Is(err, spanvalue.ErrUnexpectedComplexValueKind) {
		t.Errorf("malformed STRUCT error = %v, want ErrUnexpectedComplexValueKind", err)
	}
}

// TestPluginForTypeCodeTypedNullArray pins the expressiveness gain #253
// records: the FormatArray field structurally never saw NULL (the built-in
// NULL short-circuit ran first), and PluginForArray keeps that contract by
// deferring NULL — but a plain PluginForTypeCode(ARRAY, ...) override receives
// NULL arrays, so typed NULL rendering such as CAST(NULL AS ARRAY<INT64>) is
// now expressible by plugin authors, composed here with a builder-assembled
// config.
func TestPluginForTypeCodeTypedNullArray(t *testing.T) {
	t.Parallel()

	castNullArray := spanvalue.PluginForTypeCode(sppb.TypeCode_ARRAY,
		func(_ spanvalue.Formatter, value spanner.GenericColumnValue, _ bool) (string, error) {
			if !spanvalue.IsNull(value) {
				return "", spanvalue.ErrFallthrough
			}
			return "CAST(NULL AS " + spantype.FormatTypeVerbose(value.Type) + ")", nil
		})

	fc, err := spanvalue.NewFormatConfig(
		spanvalue.WithNullString("NULL"),
		spanvalue.WithPlugin(castNullArray),
		spanvalue.WithArrayFormat(spanvalue.FormatUntypedArray),
		spanvalue.WithStructFormat(func(f spanvalue.Formatter, sf *sppb.StructType_Field, value *structpb.Value) (string, error) {
			return f.FormatColumn(spanner.GenericColumnValue{Type: sf.GetType(), Value: value}, false)
		}, spanvalue.FormatTupleStruct),
		spanvalue.WithScalarFormatter(spanvalue.FormatNullableSpannerCLICompatible),
	)
	if err != nil {
		t.Fatal(err)
	}

	got, err := fc.FormatToplevelColumn(gcvctor.NullArrayFromCode(sppb.TypeCode_INT64))
	if err != nil || got != "CAST(NULL AS ARRAY<INT64>)" {
		t.Errorf("NULL ARRAY = (%q, %v), want (CAST(NULL AS ARRAY<INT64>), nil)", got, err)
	}

	// Non-NULL arrays defer past the override to the builder's array handler.
	arr, err := gcvctor.ArrayValue(gcvctor.Int64Value(1), gcvctor.Int64Value(2))
	if err != nil {
		t.Fatal(err)
	}
	got, err = fc.FormatToplevelColumn(arr)
	if err != nil || got != "[1, 2]" {
		t.Errorf("ARRAY = (%q, %v), want ([1, 2], nil)", got, err)
	}

	// Scalar NULLs keep the global null string: the override is ARRAY-only.
	got, err = fc.FormatToplevelColumn(gcvctor.NullFromCode(sppb.TypeCode_INT64))
	if err != nil || got != "NULL" {
		t.Errorf("NULL INT64 = (%q, %v), want (NULL, nil)", got, err)
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

// TestComplexCombinatorsNilType pins that PluginForArray and PluginForStruct
// fall through on a nil Type (explicitly, not only via nil-safe getters), so
// the built-in handling classifies the malformed value.
func TestComplexCombinatorsNilType(t *testing.T) {
	t.Parallel()

	malformed := spanner.GenericColumnValue{Value: structpb.NewStringValue("x")}
	plugins := []spanvalue.FormatComplexFunc{
		spanvalue.PluginForArray(func(*sppb.Type, bool, []string) (string, error) {
			return "array", nil
		}),
		spanvalue.PluginForStruct(
			func(spanvalue.Formatter, *sppb.StructType_Field, *structpb.Value) (string, error) {
				return "field", nil
			},
			func(*sppb.Type, bool, []string) (string, error) { return "paren", nil },
		),
	}
	for i, p := range plugins {
		if _, err := pluginConfig(p).FormatToplevelColumn(malformed); err == nil {
			t.Errorf("plugin %d: nil Type want built-in error, got nil", i)
		}
	}
}

// TestPluginForNullable pins the pre-composed combinator's equivalence with
// PluginFromNullable(NullableFormatterFor(f)).
func TestPluginForNullable(t *testing.T) {
	t.Parallel()

	fc := pluginConfig(spanvalue.PluginForNullable(func(v spanner.NullNumeric) (string, error) {
		return "N:" + v.Numeric.FloatString(2), nil
	}))

	got, err := fc.FormatToplevelColumn(gcvctor.NumericValue(big.NewRat(3, 2)))
	if err != nil || got != "N:1.50" {
		t.Errorf("NUMERIC = (%q, %v), want (N:1.50, nil)", got, err)
	}
	// PG_NUMERIC decodes to PGNumeric, not NullNumeric: falls through.
	got, err = fc.FormatToplevelColumn(gcvctor.PGNumericValue(big.NewRat(3, 2)))
	if err != nil || got != "1.500000000" {
		t.Errorf("PG_NUMERIC = (%q, %v), want preset wire string", got, err)
	}
	// NULL and other scalars keep preset behavior.
	got, err = fc.FormatToplevelColumn(gcvctor.NullFromCode(sppb.TypeCode_NUMERIC))
	if err != nil || got != fc.GetNullString() {
		t.Errorf("NULL = (%q, %v), want (%q, nil)", got, err, fc.GetNullString())
	}
	got, err = fc.FormatToplevelColumn(gcvctor.StringValue("s"))
	if err != nil || got != "s" {
		t.Errorf("STRING = (%q, %v), want (s, nil)", got, err)
	}
}
