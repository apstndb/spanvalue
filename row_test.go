package spanvalue

import (
	"testing"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/spantype/typector"
	"github.com/apstndb/spanvalue/gcvctor"
	"github.com/google/go-cmp/cmp"
)

func TestColumnNames(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		fields []*sppb.StructType_Field
		namer  UnnamedFieldNamer
		want   []string
	}{
		{
			name:   "nil namer preserves empty names",
			fields: typector.MustNameCodeSlicesToStructType([]string{"", "name"}, []sppb.TypeCode{sppb.TypeCode_INT64, sppb.TypeCode_STRING}).GetStructType().GetFields(),
			want:   []string{"", "name"},
		},
		{
			name:   "collision avoidance for unnamed fields",
			fields: typector.MustNameCodeSlicesToStructType([]string{"", "", "_1"}, []sppb.TypeCode{sppb.TypeCode_INT64, sppb.TypeCode_INT64, sppb.TypeCode_INT64}).GetStructType().GetFields(),
			namer:  IndexedUnnamedFieldNamer,
			want:   []string{"_0", "_2", "_1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ColumnNames(tt.fields, tt.namer)
			if err != nil {
				t.Fatalf("ColumnNames() error = %v", err)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("ColumnNames() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestColumnNames_Errors(t *testing.T) {
	t.Parallel()

	fields := typector.MustNameCodeSlicesToStructType([]string{""}, []sppb.TypeCode{sppb.TypeCode_INT64}).GetStructType().GetFields()

	t.Run("empty name error", func(t *testing.T) {
		_, err := ColumnNames(fields, func(int) string { return "" })
		if err == nil {
			t.Fatal("ColumnNames() error = nil, want non-nil for empty name")
		}
		want := "unnamed field namer returned empty string (field index 0, generated index 0)"
		if got := err.Error(); got != want {
			t.Errorf("ColumnNames() error = %q, want %q", got, want)
		}
	})

	t.Run("repeated name error", func(t *testing.T) {
		fields2 := typector.MustNameCodeSlicesToStructType([]string{"", "A"}, []sppb.TypeCode{sppb.TypeCode_INT64, sppb.TypeCode_INT64}).GetStructType().GetFields()
		_, err := ColumnNames(fields2, func(int) string { return "A" })
		if err == nil {
			t.Fatal("ColumnNames() error = nil, want non-nil for repeated name")
		}
		want := `unnamed field namer returned repeated colliding name "A" (field index 0, generated index 1)`
		if got := err.Error(); got != want {
			t.Errorf("ColumnNames() error = %q, want %q", got, want)
		}
	})
}

func TestFormatRowColumns(t *testing.T) {
	t.Parallel()

	columnNames := []string{"id", "name"}
	values := []spanner.GenericColumnValue{
		gcvctor.Int64Value(42),
		gcvctor.StringValue("Alice"),
	}

	got, err := FormatRowColumns(SimpleFormatConfig(), columnNames, values)
	if err != nil {
		t.Fatalf("FormatRowColumns() error = %v", err)
	}

	want := []string{"42", "Alice"}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("FormatRowColumns() mismatch (-want +got):\n%s", diff)
	}
}

func TestFormatRowColumns_LengthMismatch(t *testing.T) {
	t.Parallel()

	_, err := FormatRowColumns(SimpleFormatConfig(), []string{"id"}, []spanner.GenericColumnValue{})
	if err == nil {
		t.Fatal("FormatRowColumns() error = nil, want non-nil")
	}
}

func TestFormatRowJSONObjectFromColumns(t *testing.T) {
	t.Parallel()

	columnNames := []string{"", "", "_1"}
	values := []spanner.GenericColumnValue{
		gcvctor.StringValue("Alice"),
		gcvctor.Int64Value(42),
		gcvctor.StringValue("tail"),
	}

	got, err := FormatRowJSONObjectFromColumns(JSONFormatConfig(), columnNames, values, IndexedUnnamedFieldNamer)
	if err != nil {
		t.Fatalf("FormatRowJSONObjectFromColumns() error = %v", err)
	}

	want := `{"_0":"Alice","_2":42,"_1":"tail"}`
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("FormatRowJSONObjectFromColumns() mismatch (-want +got):\n%s", diff)
	}
}
