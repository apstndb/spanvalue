package spanvalue_test

import (
	"fmt"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/apstndb/spanvalue"
	"github.com/apstndb/spanvalue/gcvctor"
	"github.com/google/go-cmp/cmp"
)

func TestFormatTupleStructFormal(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		fields          []string
		formal, display string
	}{
		{nil, "STRUCT()", "()"},
		{[]string{"1"}, "STRUCT(1)", "(1)"},
		{[]string{"1", `"east"`}, `(1, "east")`, `(1, "east")`},
		{[]string{"NULL", "STRUCT(1)", "[2, 3]"}, "(NULL, STRUCT(1), [2, 3])", "(NULL, STRUCT(1), [2, 3])"},
	} {
		t.Run(tt.formal, func(t *testing.T) {
			t.Parallel()
			for _, top := range []bool{false, true} {
				formal, err := spanvalue.FormatTupleStructFormal(nil, top, tt.fields)
				if err != nil {
					t.Fatal(err)
				}
				display, err := spanvalue.FormatTupleStruct(nil, top, tt.fields)
				if err != nil {
					t.Fatal(err)
				}
				if diff := cmp.Diff([]string{tt.formal, tt.display}, []string{formal, display}); diff != "" {
					t.Errorf("(-want +got):\n%s", diff)
				}
			}
		})
	}
}

func TestFormatTupleStructFormalNested(t *testing.T) {
	t.Parallel()
	formal := spanvalue.LiteralFormatConfig().WithComplexPlugin(spanvalue.PluginForStruct(spanvalue.FormatSimpleStructField, spanvalue.FormatTupleStructFormal))
	for _, tt := range []struct {
		size                                                 int
		arrayFormal, arrayDisplay, outerFormal, outerDisplay string
	}{
		{0, "ARRAY<STRUCT<>>[STRUCT()]", "ARRAY<STRUCT<>>[()]", "STRUCT(STRUCT())", "STRUCT<child STRUCT<>>(())"},
		{1, "ARRAY<STRUCT<INT64>>[STRUCT(1)]", "ARRAY<STRUCT<INT64>>[(1)]", "STRUCT(STRUCT(1))", "STRUCT<child STRUCT<INT64>>((1))"},
		{2, "ARRAY<STRUCT<INT64, INT64>>[(1, 2)]", "ARRAY<STRUCT<INT64, INT64>>[(1, 2)]", "STRUCT((1, 2))", "STRUCT<child STRUCT<INT64, INT64>>((1, 2))"},
	} {
		t.Run(fmt.Sprint(tt.size), func(t *testing.T) {
			t.Parallel()
			fields := make([]spanner.GenericColumnValue, tt.size)
			for i := range fields {
				fields[i] = gcvctor.Int64Value(int64(i + 1))
			}
			child, err := gcvctor.StructValueOf(make([]string, tt.size), fields)
			if err != nil {
				t.Fatal(err)
			}
			array, err := gcvctor.ArrayValue(child)
			if err != nil {
				t.Fatal(err)
			}
			outer, err := gcvctor.StructValueOf([]string{"child"}, []spanner.GenericColumnValue{child})
			if err != nil {
				t.Fatal(err)
			}
			var got []string
			for _, value := range []spanner.GenericColumnValue{array, outer} {
				for _, fc := range []*spanvalue.FormatConfig{formal, spanvalue.LiteralFormatConfig()} {
					s, err := fc.FormatToplevelColumn(value)
					if err != nil {
						t.Fatal(err)
					}
					got = append(got, s)
				}
			}
			if diff := cmp.Diff([]string{tt.arrayFormal, tt.arrayDisplay, tt.outerFormal, tt.outerDisplay}, got); diff != "" {
				t.Errorf("(-want +got):\n%s", diff)
			}
		})
	}
}
