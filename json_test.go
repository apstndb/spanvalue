package spanvalue

import (
	"math"
	"strconv"
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
)

func TestJSONFormatConfig(t *testing.T) {
	t.Parallel()

	fc := JSONFormatConfig()

	arrayOfInt64 := lo.Must(gcvctor.ArrayValue(gcvctor.Int64Value(1), gcvctor.Int64Value(2), gcvctor.Int64Value(3)))
	arrayWithNull := lo.Must(gcvctor.ArrayValue(gcvctor.Int64Value(1), gcvctor.SimpleTypedNull(sppb.TypeCode_INT64), gcvctor.Int64Value(3)))
	structVal := lo.Must(gcvctor.StructValue([]string{"name", "age"}, []spanner.GenericColumnValue{gcvctor.StringValue("Alice"), gcvctor.Int64Value(30)}))
	unnamedStruct := lo.Must(gcvctor.StructValue([]string{"", ""}, []spanner.GenericColumnValue{gcvctor.StringValue("value"), gcvctor.Int64Value(42)}))
	structElem := lo.Must(gcvctor.StructValue([]string{"COUNT", "MEAN"}, []spanner.GenericColumnValue{gcvctor.Int64Value(1), gcvctor.Float64Value(0.057294)}))
	arrayOfStruct := lo.Must(gcvctor.ArrayValue(structElem))
	jsonVal := lo.Must(gcvctor.JSONValue(map[string]string{"key": "value"}))

	tests := []struct {
		name     string
		gcv      spanner.GenericColumnValue
		wantJSON string
	}{
		{name: "NULL STRING", gcv: gcvctor.SimpleTypedNull(sppb.TypeCode_STRING), wantJSON: "null"},
		{name: "NULL INT64", gcv: gcvctor.SimpleTypedNull(sppb.TypeCode_INT64), wantJSON: "null"},
		{name: "BOOL true", gcv: gcvctor.BoolValue(true), wantJSON: "true"},
		{name: "BOOL false", gcv: gcvctor.BoolValue(false), wantJSON: "false"},
		{name: "INT64", gcv: gcvctor.Int64Value(42), wantJSON: "42"},
		{name: "INT64 max", gcv: gcvctor.Int64Value(math.MaxInt64), wantJSON: "9223372036854775807"},
		{name: "FLOAT64 finite", gcv: gcvctor.Float64Value(3.14), wantJSON: "3.14"},
		{name: "FLOAT64 NaN", gcv: gcvctor.Float64Value(math.NaN()), wantJSON: `"NaN"`},
		{name: "FLOAT64 +Inf", gcv: gcvctor.Float64Value(math.Inf(1)), wantJSON: `"Infinity"`},
		{name: "FLOAT64 -Inf", gcv: gcvctor.Float64Value(math.Inf(-1)), wantJSON: `"-Infinity"`},
		{name: "FLOAT32 finite", gcv: gcvctor.Float32Value(2.5), wantJSON: "2.5"},
		{name: "FLOAT32 NaN", gcv: gcvctor.Float32Value(float32(math.NaN())), wantJSON: `"NaN"`},
		{name: "FLOAT32 +Inf", gcv: gcvctor.Float32Value(float32(math.Inf(1))), wantJSON: `"Infinity"`},
		{name: "FLOAT32 -Inf", gcv: gcvctor.Float32Value(float32(math.Inf(-1))), wantJSON: `"-Infinity"`},
		{name: "STRING", gcv: gcvctor.StringValue("hello"), wantJSON: `"hello"`},
		{name: "STRING with special chars", gcv: gcvctor.StringValue("line1\nline2"), wantJSON: `"line1\nline2"`},
		{name: "TIMESTAMP", gcv: gcvctor.TimestampValue(time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)), wantJSON: `"2024-01-15T12:00:00Z"`},
		{name: "DATE", gcv: gcvctor.DateValue(civil.Date{Year: 2024, Month: 1, Day: 15}), wantJSON: `"2024-01-15"`},
		{name: "NUMERIC", gcv: gcvctor.StringBasedValue(sppb.TypeCode_NUMERIC, "123.456"), wantJSON: `"123.456"`},
		{name: "JSON column", gcv: jsonVal, wantJSON: `{"key":"value"}`},
		{name: "BYTES", gcv: gcvctor.BytesValue([]byte("Hello")), wantJSON: `"SGVsbG8="`},
		{name: "ENUM", gcv: gcvctor.EnumValue("my.proto.Enum", 42), wantJSON: `42`},
		{name: "INTERVAL", gcv: gcvctor.StringBasedValue(sppb.TypeCode_INTERVAL, "P1Y2M3DT4H5M6.5S"), wantJSON: `"P1Y2M3DT4H5M6.5S"`},
		{name: "UUID", gcv: gcvctor.UUIDValue(uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")), wantJSON: `"550e8400-e29b-41d4-a716-446655440000"`},
		{name: "ARRAY of INT64", gcv: arrayOfInt64, wantJSON: `[1,2,3]`},
		{name: "ARRAY with NULL element", gcv: arrayWithNull, wantJSON: `[1,null,3]`},
		{name: "NULL ARRAY", gcv: gcvctor.TypedNull(typector.ElemCodeToArrayType(sppb.TypeCode_INT64)), wantJSON: "null"},
		{name: "STRUCT", gcv: structVal, wantJSON: `{"name":"Alice","age":30}`},
		{name: "STRUCT with unnamed fields", gcv: unnamedStruct, wantJSON: `{"":"value","":42}`},
		{name: "ARRAY of STRUCT", gcv: arrayOfStruct, wantJSON: `[{"COUNT":1,"MEAN":0.057294}]`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := fc.FormatToplevelColumn(tt.gcv)
			if err != nil {
				t.Fatalf("FormatToplevelColumn() error = %v", err)
			}
			if diff := cmp.Diff(tt.wantJSON, got); diff != "" {
				t.Errorf("JSON output mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestFormatRowJSONObject(t *testing.T) {
	t.Parallel()

	row := lo.Must(spanner.NewRow([]string{"id", "name", "active"}, []interface{}{int64(42), "Alice", true}))
	got := lo.Must(FormatRowJSONObject(JSONFormatConfig(), row, IndexedUnnamedFieldNamer))

	want := `{"id":42,"name":"Alice","active":true}`
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
}

func TestFormatRowJSONObject_UnnamedColumns(t *testing.T) {
	t.Parallel()

	row := lo.Must(spanner.NewRow([]string{"", ""}, []interface{}{int64(2), "hello"}))
	got := lo.Must(FormatRowJSONObject(JSONFormatConfig(), row, IndexedUnnamedFieldNamer))

	want := `{"_0":2,"_1":"hello"}`
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
}

func TestNewJSONObjectStructFormatter_NilNamer(t *testing.T) {
	t.Parallel()

	formatter := NewJSONObjectStructFormatter(nil)
	typ := typector.MustNameCodeSlicesToStructType([]string{"", ""}, []sppb.TypeCode{sppb.TypeCode_INT64, sppb.TypeCode_INT64})
	got := lo.Must(formatter(typ, false, []string{"1", "2"}))
	want := `{"":1,"":2}`
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
}

func TestNewJSONObjectStructFormatter_CustomNamer(t *testing.T) {
	t.Parallel()

	formatter := NewJSONObjectStructFormatter(func(i int) string {
		return "col" + strconv.Itoa(i+1)
	})
	typ := typector.MustNameTypeSlicesToStructType(
		[]string{"", "name"},
		[]*sppb.Type{typector.CodeToSimpleType(sppb.TypeCode_INT64), typector.CodeToSimpleType(sppb.TypeCode_STRING)},
	)
	got := lo.Must(formatter(typ, false, []string{"42", `"Alice"`}))
	want := `{"col1":42,"name":"Alice"}`
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
}

func TestNewJSONObjectStructFormatter_CollisionAvoidance(t *testing.T) {
	t.Parallel()

	formatter := NewJSONObjectStructFormatter(IndexedUnnamedFieldNamer)
	typ := typector.MustNameTypeSlicesToStructType(
		[]string{"", "", "_1"},
		[]*sppb.Type{
			typector.CodeToSimpleType(sppb.TypeCode_INT64),
			typector.CodeToSimpleType(sppb.TypeCode_INT64),
			typector.CodeToSimpleType(sppb.TypeCode_INT64),
		},
	)
	got := lo.Must(formatter(typ, false, []string{"1", "2", "3"}))
	// _0 for first unnamed, _1 is taken by named field, so second unnamed gets _2
	want := `{"_0":1,"_2":2,"_1":3}`
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
}

func TestFormatCompactArray(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		elems []string
		want  string
	}{
		{name: "empty", elems: nil, want: "[]"},
		{name: "single", elems: []string{"1"}, want: "[1]"},
		{name: "multiple", elems: []string{"1", "2", "3"}, want: "[1,2,3]"},
		{name: "strings", elems: []string{`"a"`, `"b"`}, want: `["a","b"]`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := lo.Must(FormatCompactArray(nil, false, tt.elems))
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("FormatCompactArray mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestFormatJSONObjectStruct(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		typ    *sppb.Type
		fields []string
		want   string
	}{
		{
			name:   "named fields",
			typ:    typector.MustNameCodeSlicesToStructType([]string{"id", "name"}, []sppb.TypeCode{sppb.TypeCode_INT64, sppb.TypeCode_STRING}),
			fields: []string{"42", `"Alice"`},
			want:   `{"id":42,"name":"Alice"}`,
		},
		{
			name:   "unnamed fields produce empty keys",
			typ:    typector.MustNameCodeSlicesToStructType([]string{"", ""}, []sppb.TypeCode{sppb.TypeCode_INT64, sppb.TypeCode_STRING}),
			fields: []string{"1", `"hello"`},
			want:   `{"":1,"":"hello"}`,
		},
		{
			name:   "field name with special chars",
			typ:    typector.NameCodeToStructType("col \"quoted\"", sppb.TypeCode_STRING),
			fields: []string{`"val"`},
			want:   `{"col \"quoted\"":"val"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := lo.Must(FormatJSONObjectStruct(tt.typ, false, tt.fields))
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("FormatJSONObjectStruct mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestNewJSONObjectStructFormatter_Error(t *testing.T) {
	t.Parallel()

	t.Run("empty name", func(t *testing.T) {
		t.Parallel()
		formatter := NewJSONObjectStructFormatter(func(i int) string {
			return ""
		})
		typ := typector.MustNameCodeSlicesToStructType([]string{""}, []sppb.TypeCode{sppb.TypeCode_INT64})
		_, err := formatter(typ, false, []string{"1"})
		if err == nil {
			t.Fatal("expected error for empty name, got nil")
		}
		want := "unnamed field namer returned empty string (field index 0, generated index 0)"
		if got := err.Error(); got != want {
			t.Errorf("error = %q, want %q", got, want)
		}
	})

	t.Run("duplicate name", func(t *testing.T) {
		t.Parallel()
		formatter := NewJSONObjectStructFormatter(func(i int) string {
			return "dup"
		})
		typ := typector.MustNameCodeSlicesToStructType([]string{"", ""}, []sppb.TypeCode{sppb.TypeCode_INT64, sppb.TypeCode_INT64})
		_, err := formatter(typ, false, []string{"1", "2"})
		if err == nil {
			t.Fatal("expected error for duplicate name, got nil")
		}
		want := "unnamed field namer returned repeated colliding name \"dup\" (field index 1, generated index 2)"
		if got := err.Error(); got != want {
			t.Errorf("error = %q, want %q", got, want)
		}
	})
}

func TestFormatRowJSONObject_Error(t *testing.T) {
	t.Parallel()

	t.Run("empty name", func(t *testing.T) {
		t.Parallel()
		row := lo.Must(spanner.NewRow([]string{""}, []interface{}{1}))
		_, err := FormatRowJSONObject(JSONFormatConfig(), row, func(i int) string {
			return ""
		})
		if err == nil {
			t.Fatal("expected error for empty name resolution, got nil")
		}
		want := "unnamed field namer returned empty string (field index 0, generated index 0)"
		if got := err.Error(); got != want {
			t.Errorf("error = %q, want %q", got, want)
		}
	})

	t.Run("duplicate name", func(t *testing.T) {
		t.Parallel()
		// First column is named "dup", second is unnamed.
		// Namer returns "dup" for index 0, but it's taken by first column.
		// Namer returns "dup" again for index 1, which should trigger an error.
		row := lo.Must(spanner.NewRow([]string{"dup", ""}, []interface{}{1, 2}))
		_, err := FormatRowJSONObject(JSONFormatConfig(), row, func(i int) string {
			return "dup"
		})
		if err == nil {
			t.Fatal("expected error for duplicate name resolution, got nil")
		}
		want := "unnamed field namer returned repeated colliding name \"dup\" (field index 1, generated index 1)"
		if got := err.Error(); got != want {
			t.Errorf("error = %q, want %q", got, want)
		}
	})
}
