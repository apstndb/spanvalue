package spanvalue

import (
	"testing"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/spantype/typector"
	"github.com/apstndb/spanvalue/gcvctor"
	"github.com/google/go-cmp/cmp"
	"github.com/samber/lo"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestFormatColumnComplexPlugins(t *testing.T) {
	t.Parallel()

	arrayValue := lo.Must(gcvctor.ArrayValue(gcvctor.Int64Value(1), gcvctor.Int64Value(2)))
	structValue := lo.Must(gcvctor.StructValue(
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
