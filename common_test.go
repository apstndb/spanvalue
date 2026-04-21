package spanvalue

import (
	"errors"
	"math/big"
	"testing"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/spantype"
	"github.com/apstndb/spantype/typector"
	"github.com/apstndb/spanvalue/gcvctor"
	"github.com/google/go-cmp/cmp"
	"github.com/samber/lo"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestFormatColumnComplexPlugins(t *testing.T) {
	t.Parallel()

	arrayValue := lo.Must(gcvctor.ArrayValue(gcvctor.Int64Value(1), gcvctor.Int64Value(2)))
	structValue := lo.Must(gcvctor.StructValueOf(
		[]string{"name"},
		[]spanner.GenericColumnValue{gcvctor.StringValue("Alice")},
	))

	fc := SimpleFormatConfig()
	calls := make([]sppb.TypeCode, 0, 3)
	fc.FormatComplexPlugins = []FormatComplexFunc{
		func(formatter Formatter, value spanner.GenericColumnValue, toplevel bool) (string, error) {
			calls = append(calls, value.Type.GetCode())
			switch value.Type.GetCode() {
			case sppb.TypeCode_ARRAY:
				return "plugin-array", nil
			case sppb.TypeCode_STRUCT:
				return "plugin-struct", nil
			default:
				return "", ErrFallthrough
			}
		},
	}

	tests := []struct {
		name string
		gcv  spanner.GenericColumnValue
		want string
	}{
		{name: "array", gcv: arrayValue, want: "plugin-array"},
		{name: "struct", gcv: structValue, want: "plugin-struct"},
		{name: "scalar", gcv: gcvctor.Int64Value(42), want: "42"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fc.FormatToplevelColumn(tt.gcv)
			if err != nil {
				t.Fatalf("FormatToplevelColumn() error = %v", err)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}

	wantCalls := []sppb.TypeCode{
		sppb.TypeCode_ARRAY,
		sppb.TypeCode_STRUCT,
		sppb.TypeCode_INT64,
	}
	if diff := cmp.Diff(wantCalls, calls); diff != "" {
		t.Errorf("plugin dispatch mismatch (-want +got):\n%s", diff)
	}
}

func TestFormatColumnConstructedNullStruct(t *testing.T) {
	t.Parallel()

	structType := typector.MustNameCodeSlicesToStructType(
		[]string{"name"},
		[]sppb.TypeCode{sppb.TypeCode_STRING},
	)
	constructedNullStruct := spanner.GenericColumnValue{
		Type:  structType,
		Value: structpb.NewNullValue(),
	}

	got, err := SimpleFormatConfig().FormatToplevelColumn(constructedNullStruct)
	if err != nil {
		t.Fatalf("FormatToplevelColumn() error = %v", err)
	}
	if got != SimpleFormatConfig().NullString {
		t.Fatalf("FormatToplevelColumn() = %q, want %q", got, SimpleFormatConfig().NullString)
	}
}

func TestIsNull(t *testing.T) {
	t.Parallel()

	structType := typector.MustNameCodeSlicesToStructType(
		[]string{"a", "b"},
		[]sppb.TypeCode{sppb.TypeCode_INT64, sppb.TypeCode_STRING},
	)

	scalarNullGcv := spanner.GenericColumnValue{
		Type:  structType,
		Value: structpb.NewNullValue(),
	}

	listNullGcv := spanner.GenericColumnValue{
		Type: structType,
		Value: structpb.NewListValue(&structpb.ListValue{
			Values: []*structpb.Value{structpb.NewNullValue(), structpb.NewNullValue()},
		}),
	}

	if !IsNull(scalarNullGcv) {
		t.Errorf("Expected scalarNullGcv to be IsNull == true")
	}
	if IsNull(listNullGcv) {
		t.Errorf("Expected listNullGcv to be IsNull == false")
	}

	if !IsNull(spanner.GenericColumnValue{Type: structType, Value: nil}) {
		t.Errorf("Expected gcv with nil Value to be IsNull == true")
	}
}

func TestFormatColumn_PostgreSQLAnnotatedTypes(t *testing.T) {
	t.Parallel()

	rat := big.NewRat(314, 100)
	numGCV := gcvctor.PGNumericValue(rat)
	wantSimple := spanner.NumericString(rat)
	gotSimple, err := SimpleFormatConfig().FormatToplevelColumn(numGCV)
	if err != nil {
		t.Fatalf("simple PGNumeric: %v", err)
	}
	if gotSimple != wantSimple {
		t.Errorf("simple PGNumeric: got %q want %q", gotSimple, wantSimple)
	}
	wantLiteral, err := FormatColumnLiteral(gcvctor.NumericValue(rat))
	if err != nil {
		t.Fatalf("want literal: %v", err)
	}
	gotLiteral, err := FormatColumnLiteral(numGCV)
	if err != nil {
		t.Fatalf("literal PGNumeric: %v", err)
	}
	if gotLiteral != wantLiteral {
		t.Errorf("literal PGNumeric: got %q want %q", gotLiteral, wantLiteral)
	}

	jsonGCV := lo.Must(gcvctor.PGJSONBValue(map[string]int{"k": 1}))
	wantJSONSimple, err := SimpleFormatConfig().FormatToplevelColumn(lo.Must(gcvctor.JSONValue(map[string]int{"k": 1})))
	if err != nil {
		t.Fatalf("want json simple: %v", err)
	}
	gotJSONSimple, err := SimpleFormatConfig().FormatToplevelColumn(jsonGCV)
	if err != nil {
		t.Fatalf("simple PGJSONB: %v", err)
	}
	if gotJSONSimple != wantJSONSimple {
		t.Errorf("simple PGJSONB: got %q want %q", gotJSONSimple, wantJSONSimple)
	}

	if got, want := spantype.FormatTypeVerbose(typector.PGNumeric()), "NUMERIC(PG_NUMERIC)"; got != want {
		t.Errorf("FormatTypeVerbose(PGNumeric): got %q want %q", got, want)
	}
	if got, want := spantype.FormatTypeVerbose(typector.PGJSONB()), "JSON(PG_JSONB)"; got != want {
		t.Errorf("FormatTypeVerbose(PGJSONB): got %q want %q", got, want)
	}
}

func TestFormatRowNilRow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		call func() error
	}{
		{
			name: "FormatConfig.FormatRow",
			call: func() error {
				_, err := SimpleFormatConfig().FormatRow(nil)
				return err
			},
		},
		{
			name: "FormatRowLiteral",
			call: func() error {
				_, err := FormatRowLiteral(nil)
				return err
			},
		},
		{
			name: "FormatRowSpannerCLICompatible",
			call: func() error {
				_, err := FormatRowSpannerCLICompatible(nil)
				return err
			},
		},
		{
			name: "FormatRowJSONObject",
			call: func() error {
				_, err := FormatRowJSONObject(JSONFormatConfig(), nil, IndexedUnnamedFieldNamer)
				return err
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if err := tt.call(); !errors.Is(err, ErrNilRow) {
				t.Fatalf("error = %v, want ErrNilRow", err)
			}
		})
	}
}

func TestFormatColumnRejectsNonListComplexKinds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		gcv  spanner.GenericColumnValue
	}{
		{
			name: "array string payload",
			gcv: spanner.GenericColumnValue{
				Type:  typector.ElemCodeToArrayType(sppb.TypeCode_INT64),
				Value: structpb.NewStringValue("not-a-list"),
			},
		},
		{
			name: "struct number payload",
			gcv: spanner.GenericColumnValue{
				Type: typector.MustNameCodeSlicesToStructType(
					[]string{"a"},
					[]sppb.TypeCode{sppb.TypeCode_INT64},
				),
				Value: structpb.NewNumberValue(1),
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := SimpleFormatConfig().FormatToplevelColumn(tt.gcv)
			if !errors.Is(err, ErrUnexpectedComplexValueKind) {
				t.Fatalf("error = %v, want ErrUnexpectedComplexValueKind", err)
			}
		})
	}
}

func TestFormatColumnRejectsEmptyTypeFQN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		gcv  spanner.GenericColumnValue
	}{
		{
			name: "proto",
			gcv: spanner.GenericColumnValue{
				Type:  &sppb.Type{Code: sppb.TypeCode_PROTO},
				Value: structpb.NewStringValue("ZGVhZGJlZWY="),
			},
		},
		{
			name: "enum",
			gcv: spanner.GenericColumnValue{
				Type:  &sppb.Type{Code: sppb.TypeCode_ENUM},
				Value: structpb.NewStringValue("42"),
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := LiteralFormatConfig().FormatToplevelColumn(tt.gcv)
			if !errors.Is(err, ErrEmptyTypeFQN) {
				t.Fatalf("error = %v, want ErrEmptyTypeFQN", err)
			}
		})
	}
}
