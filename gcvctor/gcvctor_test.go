// Package gcvctor_test exercises github.com/apstndb/spanvalue/gcvctor.
//
// When an expected value exercises a specific API (e.g. ArrayValueOf),
// build want from typector + structpb (and spanner.GenericColumnValue literals)
// instead of other gcvctor helpers that delegate to the same implementation,
// so the test compares against an independent oracle rather than the code under test.
package gcvctor_test

import (
	"encoding/base64"
	"errors"
	"math"
	"math/big"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/spantype/typector"
	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/samber/lo"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/apstndb/spanvalue/gcvctor"
)

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func TestNumericValueCheckedNil(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		call func() (spanner.GenericColumnValue, error)
	}{
		{
			name: "google sql numeric",
			call: func() (spanner.GenericColumnValue, error) {
				return gcvctor.NumericValueChecked(nil)
			},
		},
		{
			name: "pg numeric",
			call: func() (spanner.GenericColumnValue, error) {
				return gcvctor.PGNumericValueChecked(nil)
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := tt.call()
			if !errors.Is(err, gcvctor.ErrNilNumeric) {
				t.Fatalf("error = %v, want ErrNilNumeric", err)
			}
			if diff := cmp.Diff(spanner.GenericColumnValue{}, got, protocmp.Transform()); diff != "" {
				t.Fatalf("got non-zero value (-want +got):\n%s", diff)
			}
		})
	}
}

func TestNumericValueAndPGNumericValueNilReturnTypedNull(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		got  spanner.GenericColumnValue
		want spanner.GenericColumnValue
	}{
		{
			name: "google sql numeric",
			got:  gcvctor.NumericValue(nil),
			want: spanner.GenericColumnValue{Type: typector.CodeToSimpleType(sppb.TypeCode_NUMERIC), Value: structpb.NewNullValue()},
		},
		{
			name: "pg numeric",
			got:  gcvctor.PGNumericValue(nil),
			want: spanner.GenericColumnValue{Type: typector.PGNumeric(), Value: structpb.NewNullValue()},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if diff := cmp.Diff(tt.want, tt.got, protocmp.Transform()); diff != "" {
				t.Fatalf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestParseExpr(t *testing.T) {
	tests := []struct {
		desc  string
		input spanner.GenericColumnValue
		want  spanner.GenericColumnValue
	}{
		{
			"NULL",
			gcvctor.NullFromCode(sppb.TypeCode_INT64),
			spanner.GenericColumnValue{
				Type:  typector.CodeToSimpleType(sppb.TypeCode_INT64),
				Value: structpb.NewNullValue(),
			},
		},
		{
			"TRUE",
			gcvctor.BoolValue(true),
			spanner.GenericColumnValue{
				Type:  typector.CodeToSimpleType(sppb.TypeCode_BOOL),
				Value: structpb.NewBoolValue(true),
			},
		},
		{
			`FALSE`,
			gcvctor.BoolValue(false),
			spanner.GenericColumnValue{
				Type:  typector.CodeToSimpleType(sppb.TypeCode_BOOL),
				Value: structpb.NewBoolValue(false),
			},
		},
		{
			"1",
			gcvctor.Int64Value(1),
			spanner.GenericColumnValue{
				Type:  typector.CodeToSimpleType(sppb.TypeCode_INT64),
				Value: structpb.NewStringValue("1"),
			},
		},
		{
			`3.14`,
			gcvctor.Float64Value(3.14),
			spanner.GenericColumnValue{
				Type:  typector.CodeToSimpleType(sppb.TypeCode_FLOAT64),
				Value: structpb.NewNumberValue(3.14),
			},
		},
		{
			`NaN`,
			gcvctor.Float64Value(math.NaN()),
			spanner.GenericColumnValue{
				Type:  typector.CodeToSimpleType(sppb.TypeCode_FLOAT64),
				Value: structpb.NewStringValue("NaN"),
			},
		},
		{
			`+Inf`,
			gcvctor.Float64Value(math.Inf(1)),
			spanner.GenericColumnValue{
				Type:  typector.CodeToSimpleType(sppb.TypeCode_FLOAT64),
				Value: structpb.NewStringValue("Infinity"),
			},
		},
		{
			`-Inf`,
			gcvctor.Float64Value(math.Inf(-1)),
			spanner.GenericColumnValue{
				Type:  typector.CodeToSimpleType(sppb.TypeCode_FLOAT64),
				Value: structpb.NewStringValue("-Infinity"),
			},
		},
		{
			`float32(2.5)`,
			gcvctor.Float32Value(2.5),
			spanner.GenericColumnValue{
				Type:  typector.CodeToSimpleType(sppb.TypeCode_FLOAT32),
				Value: structpb.NewNumberValue(2.5),
			},
		},
		{
			`float32 NaN`,
			gcvctor.Float32Value(float32(math.NaN())),
			spanner.GenericColumnValue{
				Type:  typector.CodeToSimpleType(sppb.TypeCode_FLOAT32),
				Value: structpb.NewStringValue("NaN"),
			},
		},
		{
			`float32 +Inf`,
			gcvctor.Float32Value(float32(math.Inf(1))),
			spanner.GenericColumnValue{
				Type:  typector.CodeToSimpleType(sppb.TypeCode_FLOAT32),
				Value: structpb.NewStringValue("Infinity"),
			},
		},
		{
			`float32 -Inf`,
			gcvctor.Float32Value(float32(math.Inf(-1))),
			spanner.GenericColumnValue{
				Type:  typector.CodeToSimpleType(sppb.TypeCode_FLOAT32),
				Value: structpb.NewStringValue("-Infinity"),
			},
		},
		{
			`"foo"`,
			gcvctor.StringValue("foo"),
			spanner.GenericColumnValue{
				Type:  typector.CodeToSimpleType(sppb.TypeCode_STRING),
				Value: structpb.NewStringValue("foo"),
			},
		},
		{
			`b"foo"`,
			gcvctor.BytesValue([]byte("foo")),
			spanner.GenericColumnValue{
				Type:  typector.CodeToSimpleType(sppb.TypeCode_BYTES),
				Value: structpb.NewStringValue(base64.StdEncoding.EncodeToString([]byte("foo"))),
			},
		},
		{
			`DATE "1970-01-01"`,
			gcvctor.DateValue(civil.Date{Year: 1970, Month: time.January, Day: 1}),
			spanner.GenericColumnValue{
				Type:  typector.CodeToSimpleType(sppb.TypeCode_DATE),
				Value: structpb.NewStringValue("1970-01-01"),
			},
		},
		{
			`TIMESTAMP "1970-01-01T00:00:00Z"`,
			gcvctor.TimestampValue(time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC)),
			spanner.GenericColumnValue{
				Type:  typector.CodeToSimpleType(sppb.TypeCode_TIMESTAMP),
				Value: structpb.NewStringValue("1970-01-01T00:00:00Z"),
			},
		},
		// {`NUMERIC "3.14"`, valuector.NumericValue(big.NewRat(314, 100))},

		// Note: Usually, JSON representation is not stable.
		{
			`JSON '{"foo":"bar"}'`,
			must(gcvctor.JSONValue(map[string]string{"foo": "bar"})),
			spanner.GenericColumnValue{
				Type:  typector.CodeToSimpleType(sppb.TypeCode_JSON),
				Value: structpb.NewStringValue(`{"foo":"bar"}`),
			},
		},
		{
			`PG NUMERIC 3.14`,
			gcvctor.PGNumericValue(big.NewRat(314, 100)),
			spanner.GenericColumnValue{
				Type:  typector.PGNumeric(),
				Value: structpb.NewStringValue(spanner.NumericString(big.NewRat(314, 100))),
			},
		},
		{
			`PG JSONB {"foo":"bar"}`,
			must(gcvctor.PGJSONBValue(map[string]string{"foo": "bar"})),
			spanner.GenericColumnValue{
				Type:  typector.PGJSONB(),
				Value: structpb.NewStringValue(`{"foo":"bar"}`),
			},
		},
		{
			`INTERVAL "P1Y1M1DT1H1M1S"`,
			gcvctor.IntervalValue(lo.Must(spanner.ParseInterval(`P1Y1M1DT1H1M1S`))),
			spanner.GenericColumnValue{
				Type:  typector.CodeToSimpleType(sppb.TypeCode_INTERVAL),
				Value: structpb.NewStringValue(`P1Y1M1DT1H1M1S`),
			},
		},
		{
			`UUID "858ebda5-f6df-4f5d-9151-aa98796053c4"`,
			gcvctor.UUIDValue(uuid.MustParse("858ebda5-f6df-4f5d-9151-aa98796053c4")),
			spanner.GenericColumnValue{
				Type:  typector.CodeToSimpleType(sppb.TypeCode_UUID),
				Value: structpb.NewStringValue(`858ebda5-f6df-4f5d-9151-aa98796053c4`),
			},
		},
		{
			`[1, 2, 3]`,
			must(gcvctor.ArrayValue(gcvctor.Int64Value(1), gcvctor.Int64Value(2), gcvctor.Int64Value(3))),
			spanner.GenericColumnValue{
				Type: typector.ElemTypeToArrayType(typector.CodeToSimpleType(sppb.TypeCode_INT64)),
				Value: structpb.NewListValue(
					&structpb.ListValue{
						Values: []*structpb.Value{
							structpb.NewStringValue("1"),
							structpb.NewStringValue("2"),
							structpb.NewStringValue("3"),
						},
					},
				),
			},
		},
		{
			`(1, "foo", 3.14)`,
			must(gcvctor.StructValueOf(
				[]string{"", "", ""},
				[]spanner.GenericColumnValue{gcvctor.Int64Value(1), gcvctor.StringValue("foo"), gcvctor.Float64Value(3.14)},
			)),
			spanner.GenericColumnValue{
				Type: typector.StructTypeFieldsToStructType([]*sppb.StructType_Field{
					typector.CodeToUnnamedStructTypeField(sppb.TypeCode_INT64),
					typector.CodeToUnnamedStructTypeField(sppb.TypeCode_STRING),
					typector.CodeToUnnamedStructTypeField(sppb.TypeCode_FLOAT64),
				}),
				Value: structpb.NewListValue(
					&structpb.ListValue{
						Values: []*structpb.Value{
							structpb.NewStringValue("1"),
							structpb.NewStringValue("foo"),
							structpb.NewNumberValue(3.14),
						},
					},
				),
			},
		},
		{
			`STRUCT(1 AS int64_value, "foo" AS string_value, 3.14 AS float64_value)`,
			must(gcvctor.StructValueOf(
				[]string{"int64_value", "string_value", "float64_value"},
				[]spanner.GenericColumnValue{gcvctor.Int64Value(1), gcvctor.StringValue("foo"), gcvctor.Float64Value(3.14)},
			)),
			spanner.GenericColumnValue{
				Type: must(typector.NameCodeSlicesToStructType(
					[]string{"int64_value", "string_value", "float64_value"},
					[]sppb.TypeCode{sppb.TypeCode_INT64, sppb.TypeCode_STRING, sppb.TypeCode_FLOAT64},
				)),
				Value: structpb.NewListValue(
					&structpb.ListValue{
						Values: []*structpb.Value{
							structpb.NewStringValue("1"),
							structpb.NewStringValue("foo"),
							structpb.NewNumberValue(3.14),
						},
					},
				),
			},
		},
		{
			`STRUCT<int64_value INT64, string_value STRING, float64_value FLOAT64>(1, "foo", 3.14)`,
			must(gcvctor.StructValueOf(
				[]string{"int64_value", "string_value", "float64_value"},
				[]spanner.GenericColumnValue{gcvctor.Int64Value(1), gcvctor.StringValue("foo"), gcvctor.Float64Value(3.14)},
			)),
			spanner.GenericColumnValue{
				Type: must(typector.NameCodeSlicesToStructType(
					[]string{"int64_value", "string_value", "float64_value"},
					[]sppb.TypeCode{sppb.TypeCode_INT64, sppb.TypeCode_STRING, sppb.TypeCode_FLOAT64},
				)),
				Value: structpb.NewListValue(
					&structpb.ListValue{
						Values: []*structpb.Value{
							structpb.NewStringValue("1"),
							structpb.NewStringValue("foo"),
							structpb.NewNumberValue(3.14),
						},
					},
				),
			},
		},
		{
			"(1)",
			gcvctor.Int64Value(1),
			spanner.GenericColumnValue{
				Type:  typector.CodeToSimpleType(sppb.TypeCode_INT64),
				Value: structpb.NewStringValue("1"),
			},
		},
		{
			"ARRAY<INT64>[]",
			gcvctor.EmptyArrayOf(typector.CodeToSimpleType(sppb.TypeCode_INT64)),
			spanner.GenericColumnValue{
				Type:  typector.ElemCodeToArrayType(sppb.TypeCode_INT64),
				Value: structpb.NewListValue(&structpb.ListValue{}),
			},
		},
		{
			"ARRAY<STRUCT<n INT64>>[]",
			gcvctor.EmptyArrayOf(typector.NameCodeToStructType("n", sppb.TypeCode_INT64)),
			spanner.GenericColumnValue{
				Type:  typector.ElemTypeToArrayType(typector.NameCodeToStructType("n", sppb.TypeCode_INT64)),
				Value: structpb.NewListValue(&structpb.ListValue{}),
			},
		},
		{
			"PENDING_COMMIT_TIMESTAMP()",
			gcvctor.StringBasedValueFromCode(sppb.TypeCode_TIMESTAMP, "spanner.commit_timestamp()"),
			spanner.GenericColumnValue{
				Type:  typector.CodeToSimpleType(sppb.TypeCode_TIMESTAMP),
				Value: structpb.NewStringValue("spanner.commit_timestamp()"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			if diff := cmp.Diff(tt.want, tt.input, protocmp.Transform()); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestValidatedStringValueHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		call    func(string) (spanner.GenericColumnValue, error)
		want    spanner.GenericColumnValue
		wantErr bool
	}{
		{
			name:  "DATE valid",
			input: "2024-01-15",
			call:  gcvctor.DateStringValue,
			want: spanner.GenericColumnValue{
				Type:  typector.CodeToSimpleType(sppb.TypeCode_DATE),
				Value: structpb.NewStringValue("2024-01-15"),
			},
		},
		{
			name:    "DATE invalid",
			input:   "2024-02-30",
			call:    gcvctor.DateStringValue,
			wantErr: true,
		},
		{
			name:  "TIMESTAMP valid",
			input: "2024-01-15T12:34:56.789Z",
			call:  gcvctor.TimestampStringValue,
			want: spanner.GenericColumnValue{
				Type:  typector.CodeToSimpleType(sppb.TypeCode_TIMESTAMP),
				Value: structpb.NewStringValue("2024-01-15T12:34:56.789Z"),
			},
		},
		{
			name:  "TIMESTAMP canonical UTC",
			input: "2024-01-15T21:34:56.789+09:00",
			call:  gcvctor.TimestampStringValue,
			want: spanner.GenericColumnValue{
				Type:  typector.CodeToSimpleType(sppb.TypeCode_TIMESTAMP),
				Value: structpb.NewStringValue("2024-01-15T12:34:56.789Z"),
			},
		},
		{
			name:    "TIMESTAMP invalid",
			input:   "2024-01-15 12:34:56",
			call:    gcvctor.TimestampStringValue,
			wantErr: true,
		},
		{
			name:  "INTERVAL valid",
			input: "P1Y2M3DT4H5M6S",
			call:  gcvctor.IntervalStringValue,
			want: spanner.GenericColumnValue{
				Type:  typector.CodeToSimpleType(sppb.TypeCode_INTERVAL),
				Value: structpb.NewStringValue("P1Y2M3DT4H5M6S"),
			},
		},
		{
			name:    "INTERVAL invalid",
			input:   "not-an-interval",
			call:    gcvctor.IntervalStringValue,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := tt.call(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if diff := cmp.Diff(tt.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestNullRawValueFromType_EnumerateSimpleTypes(t *testing.T) {
	for rawcode, typename := range sppb.TypeCode_name {
		if typename == "STRUCT" || typename == "ARRAY" {
			continue
		}
		simpleType := typector.CodeToSimpleType(sppb.TypeCode(rawcode))
		got := gcvctor.NullOf(simpleType)
		want := spanner.GenericColumnValue{Type: simpleType, Value: structpb.NewNullValue()}
		t.Run(typename, func(t *testing.T) {
			if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
				t.Errorf("diff (-want, +got) = %v", diff)
			}
		})
	}
}

func TestNullRawValueFromType_ARRAY(t *testing.T) {
	input := typector.ElemCodeToArrayType(sppb.TypeCode_STRING)
	want := spanner.GenericColumnValue{Type: input, Value: structpb.NewNullValue()}
	got := gcvctor.NullOf(input)

	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Errorf("diff (-want, +got) = %v", diff)
	}
}

func TestNullOf_STRUCT(t *testing.T) {
	structType := typector.NameCodeToStructType("n", sppb.TypeCode_INT64)
	got := gcvctor.NullOf(structType)
	want := spanner.GenericColumnValue{Type: structType, Value: structpb.NewNullValue()}

	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Errorf("diff (-want, +got) = %v", diff)
	}
}

func TestNullOf_STRUCT_MultipleFields(t *testing.T) {
	structType := must(typector.NameCodeSlicesToStructType(
		[]string{"a", "b"},
		[]sppb.TypeCode{sppb.TypeCode_INT64, sppb.TypeCode_STRING},
	))
	want := spanner.GenericColumnValue{Type: structType, Value: structpb.NewNullValue()}
	got := gcvctor.NullOf(structType)

	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Errorf("diff (-want, +got) = %v", diff)
	}

	if _, ok := got.Value.GetKind().(*structpb.Value_NullValue); !ok {
		t.Errorf("Expected NullValue for STRUCT NullOf, got %T", got.Value.GetKind())
	}
}

func TestNullOf_nilTypeNormalizesToUnspecified(t *testing.T) {
	t.Parallel()

	got := gcvctor.NullOf(nil)
	want := spanner.GenericColumnValue{
		Type:  typector.CodeToSimpleType(sppb.TypeCode_TYPE_CODE_UNSPECIFIED),
		Value: structpb.NewNullValue(),
	}
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Errorf("diff (-want, +got) = %v", diff)
	}
}

func TestNullArrayFromCode_matchesNullOf(t *testing.T) {
	t.Parallel()
	got := gcvctor.NullArrayFromCode(sppb.TypeCode_INT64)
	want := spanner.GenericColumnValue{
		Type:  typector.ElemCodeToArrayType(sppb.TypeCode_INT64),
		Value: structpb.NewNullValue(),
	}
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Errorf("diff (-want, +got) = %v", diff)
	}
}

func TestNullArrayOf_matchesNullOf(t *testing.T) {
	t.Parallel()
	elem := typector.NameCodeToStructType("n", sppb.TypeCode_INT64)
	got := gcvctor.NullArrayOf(elem)
	want := spanner.GenericColumnValue{
		Type:  typector.ElemTypeToArrayType(elem),
		Value: structpb.NewNullValue(),
	}
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Errorf("diff (-want, +got) = %v", diff)
	}
}

func TestNullArrayOf_nilElementTypeNormalizesToUnspecified(t *testing.T) {
	t.Parallel()

	got := gcvctor.NullArrayOf(nil)
	want := spanner.GenericColumnValue{
		Type:  typector.ElemCodeToArrayType(sppb.TypeCode_TYPE_CODE_UNSPECIFIED),
		Value: structpb.NewNullValue(),
	}
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Errorf("diff (-want, +got) = %v", diff)
	}
}

func TestEmptyArrayOf_nilElementTypeNormalizesToUnspecified(t *testing.T) {
	t.Parallel()

	got := gcvctor.EmptyArrayOf(nil)
	want := spanner.GenericColumnValue{
		Type:  typector.ElemCodeToArrayType(sppb.TypeCode_TYPE_CODE_UNSPECIFIED),
		Value: structpb.NewListValue(&structpb.ListValue{}),
	}
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Errorf("diff (-want, +got) = %v", diff)
	}
}

func TestArrayValue_zeroLengthIsEmptyInt64Array(t *testing.T) {
	t.Parallel()
	want := spanner.GenericColumnValue{
		Type:  typector.ElemCodeToArrayType(sppb.TypeCode_INT64),
		Value: structpb.NewListValue(&structpb.ListValue{}),
	}

	for _, name := range []string{"no args", "nil slice", "empty slice"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var got spanner.GenericColumnValue
			var err error
			switch name {
			case "no args":
				got, err = gcvctor.ArrayValue()
			case "nil slice":
				var nilSlice []spanner.GenericColumnValue
				got, err = gcvctor.ArrayValue(nilSlice...)
			case "empty slice":
				got, err = gcvctor.ArrayValue([]spanner.GenericColumnValue{}...)
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestArrayValueNilFirstElementTypeReturnsArrayElementError(t *testing.T) {
	t.Parallel()

	_, err := gcvctor.ArrayValue(spanner.GenericColumnValue{})
	if !errors.Is(err, gcvctor.ErrNilElementType) {
		t.Fatalf("errors.Is(err, ErrNilElementType) = false; err = %v", err)
	}

	var ctx *gcvctor.ArrayElementError
	if !errors.As(err, &ctx) {
		t.Fatalf("errors.As(err, *ArrayElementError) = false; err = %v", err)
	}
	if ctx.Index != 0 {
		t.Fatalf("ctx.Index = %d, want 0", ctx.Index)
	}
}

func TestArrayValueOf(t *testing.T) {
	t.Parallel()
	int64Elem := typector.CodeToSimpleType(sppb.TypeCode_INT64)
	stringElem := typector.CodeToSimpleType(sppb.TypeCode_STRING)
	structElem := typector.NameCodeToStructType("n", sppb.TypeCode_INT64)

	tests := []struct {
		desc      string
		elemType  *sppb.Type
		elems     []spanner.GenericColumnValue
		want      spanner.GenericColumnValue
		expectErr bool
		errIs     error
	}{
		{
			desc:     "empty INT64 (nil elems)",
			elemType: int64Elem,
			elems:    nil,
			want: spanner.GenericColumnValue{
				Type:  typector.ElemTypeToArrayType(int64Elem),
				Value: structpb.NewListValue(&structpb.ListValue{}),
			},
		},
		{
			desc:     "empty ARRAY<STRUCT<n INT64>> (nil elems)",
			elemType: structElem,
			elems:    nil,
			want: spanner.GenericColumnValue{
				Type:  typector.ElemTypeToArrayType(structElem),
				Value: structpb.NewListValue(&structpb.ListValue{}),
			},
		},
		{
			desc:     "empty INT64 (non-nil empty slice)",
			elemType: int64Elem,
			elems:    []spanner.GenericColumnValue{},
			want: spanner.GenericColumnValue{
				Type:  typector.ElemTypeToArrayType(int64Elem),
				Value: structpb.NewListValue(&structpb.ListValue{}),
			},
		},
		{
			desc:     "non-empty INT64",
			elemType: int64Elem,
			elems:    []spanner.GenericColumnValue{gcvctor.Int64Value(1), gcvctor.Int64Value(2)},
			want: spanner.GenericColumnValue{
				Type: typector.ElemTypeToArrayType(int64Elem),
				Value: structpb.NewListValue(&structpb.ListValue{
					Values: []*structpb.Value{
						structpb.NewStringValue("1"),
						structpb.NewStringValue("2"),
					},
				}),
			},
		},
		{
			desc:     "explicit STRING type with string elements",
			elemType: stringElem,
			elems:    []spanner.GenericColumnValue{gcvctor.StringValue("a"), gcvctor.StringValue("b")},
			want: spanner.GenericColumnValue{
				Type: typector.ElemTypeToArrayType(stringElem),
				Value: structpb.NewListValue(&structpb.ListValue{
					Values: []*structpb.Value{
						structpb.NewStringValue("a"),
						structpb.NewStringValue("b"),
					},
				}),
			},
		},
		{
			desc:      "nil element type",
			elemType:  nil,
			elems:     []spanner.GenericColumnValue{gcvctor.Int64Value(1)},
			expectErr: true,
			errIs:     gcvctor.ErrNilElementType,
		},
		{
			desc:      "element type mismatch",
			elemType:  stringElem,
			elems:     []spanner.GenericColumnValue{gcvctor.Int64Value(1)},
			expectErr: true,
			errIs:     gcvctor.ErrTypeMismatch,
		},
	}
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			t.Parallel()
			got, err := gcvctor.ArrayValueOf(tt.elemType, tt.elems...)
			if tt.expectErr {
				if err == nil {
					t.Fatal("expected error")
				}
				if tt.errIs != nil && !errors.Is(err, tt.errIs) {
					t.Fatalf("errors.Is(err, %v) = false; err = %v", tt.errIs, err)
				}
				var zero spanner.GenericColumnValue
				if diff := cmp.Diff(zero, got, protocmp.Transform()); diff != "" {
					t.Errorf("expected zero value on error (-want +got):\n%s", diff)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if diff := cmp.Diff(tt.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestArrayValueOfTypeMismatchReturnsArrayElementError(t *testing.T) {
	t.Parallel()

	_, err := gcvctor.ArrayValueOf(typector.CodeToSimpleType(sppb.TypeCode_STRING), gcvctor.Int64Value(1))
	if !errors.Is(err, gcvctor.ErrTypeMismatch) {
		t.Fatalf("errors.Is(err, ErrTypeMismatch) = false; err = %v", err)
	}

	var ctx *gcvctor.ArrayElementError
	if !errors.As(err, &ctx) {
		t.Fatalf("errors.As(err, *ArrayElementError) = false; err = %v", err)
	}
	if ctx.Index != 0 {
		t.Fatalf("ctx.Index = %d, want 0", ctx.Index)
	}
}

func TestStructValueOf(t *testing.T) {
	t.Parallel()

	tests := []struct {
		desc      string
		names     []string
		gcvs      []spanner.GenericColumnValue
		want      spanner.GenericColumnValue
		expectErr bool
		errIs     error
		errSubstr string
	}{
		{
			desc:  "single field",
			names: []string{"a"},
			gcvs:  []spanner.GenericColumnValue{gcvctor.Int64Value(1)},
			want: spanner.GenericColumnValue{
				Type: must(typector.NameCodeSlicesToStructType(
					[]string{"a"},
					[]sppb.TypeCode{sppb.TypeCode_INT64},
				)),
				Value: structpb.NewListValue(&structpb.ListValue{
					Values: []*structpb.Value{structpb.NewStringValue("1")},
				}),
			},
		},
		{
			desc:      "mismatched counts",
			names:     []string{"a"},
			gcvs:      nil,
			expectErr: true,
			errIs:     gcvctor.ErrMismatchedCounts,
		},
		{
			desc:      "nil field type named",
			names:     []string{"broken"},
			gcvs:      []spanner.GenericColumnValue{{}},
			expectErr: true,
			errIs:     gcvctor.ErrNilFieldType,
			errSubstr: `field 0 ("broken")`,
		},
		{
			desc:      "nil field type unnamed",
			names:     []string{""},
			gcvs:      []spanner.GenericColumnValue{{}},
			expectErr: true,
			errIs:     gcvctor.ErrNilFieldType,
			errSubstr: "field 0",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.desc, func(t *testing.T) {
			t.Parallel()

			got, err := gcvctor.StructValueOf(tt.names, tt.gcvs)
			if tt.expectErr {
				if err == nil {
					t.Fatal("expected error")
				}
				if tt.errIs != nil && !errors.Is(err, tt.errIs) {
					t.Fatalf("errors.Is(err, %v) = false; err = %v", tt.errIs, err)
				}
				if tt.errSubstr != "" && !strings.Contains(err.Error(), tt.errSubstr) {
					t.Fatalf("error %q does not contain %q", err, tt.errSubstr)
				}
				var zero spanner.GenericColumnValue
				if diff := cmp.Diff(zero, got, protocmp.Transform()); diff != "" {
					t.Errorf("expected zero value on error (-want +got):\n%s", diff)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if diff := cmp.Diff(tt.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestStructValueOfNilFieldTypeReturnsStructFieldError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		fieldName string
	}{
		{name: "named", fieldName: "broken"},
		{name: "unnamed", fieldName: ""},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := gcvctor.StructValueOf([]string{tt.fieldName}, []spanner.GenericColumnValue{{}})
			if !errors.Is(err, gcvctor.ErrNilFieldType) {
				t.Fatalf("errors.Is(err, ErrNilFieldType) = false; err = %v", err)
			}

			var ctx *gcvctor.StructFieldError
			if !errors.As(err, &ctx) {
				t.Fatalf("errors.As(err, *StructFieldError) = false; err = %v", err)
			}
			if ctx.Index != 0 {
				t.Fatalf("ctx.Index = %d, want 0", ctx.Index)
			}
			if ctx.Name != tt.fieldName {
				t.Fatalf("ctx.Name = %q, want %q", ctx.Name, tt.fieldName)
			}
		})
	}
}
