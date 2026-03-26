package spanvalue

import (
	"strconv"
	"testing"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestJSONFormatConfig(t *testing.T) {
	t.Parallel()

	fc := JSONFormatConfig

	tests := []struct {
		name     string
		gcv      spanner.GenericColumnValue
		wantJSON string
	}{
		{
			name: "NULL STRING",
			gcv: spanner.GenericColumnValue{
				Type:  &sppb.Type{Code: sppb.TypeCode_STRING},
				Value: structpb.NewNullValue(),
			},
			wantJSON: "null",
		},
		{
			name: "NULL INT64",
			gcv: spanner.GenericColumnValue{
				Type:  &sppb.Type{Code: sppb.TypeCode_INT64},
				Value: structpb.NewNullValue(),
			},
			wantJSON: "null",
		},
		{
			name: "BOOL true",
			gcv: spanner.GenericColumnValue{
				Type:  &sppb.Type{Code: sppb.TypeCode_BOOL},
				Value: structpb.NewBoolValue(true),
			},
			wantJSON: "true",
		},
		{
			name: "BOOL false",
			gcv: spanner.GenericColumnValue{
				Type:  &sppb.Type{Code: sppb.TypeCode_BOOL},
				Value: structpb.NewBoolValue(false),
			},
			wantJSON: "false",
		},
		{
			name: "INT64",
			gcv: spanner.GenericColumnValue{
				Type:  &sppb.Type{Code: sppb.TypeCode_INT64},
				Value: structpb.NewStringValue("42"),
			},
			wantJSON: "42",
		},
		{
			name: "INT64 max",
			gcv: spanner.GenericColumnValue{
				Type:  &sppb.Type{Code: sppb.TypeCode_INT64},
				Value: structpb.NewStringValue("9223372036854775807"),
			},
			wantJSON: "9223372036854775807",
		},
		{
			name: "FLOAT64 finite",
			gcv: spanner.GenericColumnValue{
				Type:  &sppb.Type{Code: sppb.TypeCode_FLOAT64},
				Value: structpb.NewNumberValue(3.14),
			},
			wantJSON: "3.14",
		},
		{
			name: "FLOAT64 NaN",
			gcv: spanner.GenericColumnValue{
				Type:  &sppb.Type{Code: sppb.TypeCode_FLOAT64},
				Value: structpb.NewStringValue("NaN"),
			},
			wantJSON: `"NaN"`,
		},
		{
			name: "FLOAT64 Infinity",
			gcv: spanner.GenericColumnValue{
				Type:  &sppb.Type{Code: sppb.TypeCode_FLOAT64},
				Value: structpb.NewStringValue("Infinity"),
			},
			wantJSON: `"Infinity"`,
		},
		{
			name: "STRING",
			gcv: spanner.GenericColumnValue{
				Type:  &sppb.Type{Code: sppb.TypeCode_STRING},
				Value: structpb.NewStringValue("hello"),
			},
			wantJSON: `"hello"`,
		},
		{
			name: "STRING with special chars",
			gcv: spanner.GenericColumnValue{
				Type:  &sppb.Type{Code: sppb.TypeCode_STRING},
				Value: structpb.NewStringValue("line1\nline2"),
			},
			wantJSON: `"line1\nline2"`,
		},
		{
			name: "TIMESTAMP",
			gcv: spanner.GenericColumnValue{
				Type:  &sppb.Type{Code: sppb.TypeCode_TIMESTAMP},
				Value: structpb.NewStringValue("2024-01-15T12:00:00Z"),
			},
			wantJSON: `"2024-01-15T12:00:00Z"`,
		},
		{
			name: "DATE",
			gcv: spanner.GenericColumnValue{
				Type:  &sppb.Type{Code: sppb.TypeCode_DATE},
				Value: structpb.NewStringValue("2024-01-15"),
			},
			wantJSON: `"2024-01-15"`,
		},
		{
			name: "NUMERIC",
			gcv: spanner.GenericColumnValue{
				Type:  &sppb.Type{Code: sppb.TypeCode_NUMERIC},
				Value: structpb.NewStringValue("123.456"),
			},
			wantJSON: `"123.456"`,
		},
		{
			name: "JSON column",
			gcv: spanner.GenericColumnValue{
				Type:  &sppb.Type{Code: sppb.TypeCode_JSON},
				Value: structpb.NewStringValue(`{"key":"value"}`),
			},
			wantJSON: `{"key":"value"}`,
		},
		{
			name: "BYTES",
			gcv: spanner.GenericColumnValue{
				Type:  &sppb.Type{Code: sppb.TypeCode_BYTES},
				Value: structpb.NewStringValue("SGVsbG8="),
			},
			wantJSON: `"SGVsbG8="`,
		},
		{
			name: "ENUM",
			gcv: spanner.GenericColumnValue{
				Type:  &sppb.Type{Code: sppb.TypeCode_ENUM},
				Value: structpb.NewStringValue("42"),
			},
			wantJSON: `42`,
		},
		{
			name: "INTERVAL",
			gcv: spanner.GenericColumnValue{
				Type:  &sppb.Type{Code: sppb.TypeCode_INTERVAL},
				Value: structpb.NewStringValue("P1Y2M3DT4H5M6.5S"),
			},
			wantJSON: `"P1Y2M3DT4H5M6.5S"`,
		},
		{
			name: "UUID",
			gcv: spanner.GenericColumnValue{
				Type:  &sppb.Type{Code: sppb.TypeCode_UUID},
				Value: structpb.NewStringValue("550e8400-e29b-41d4-a716-446655440000"),
			},
			wantJSON: `"550e8400-e29b-41d4-a716-446655440000"`,
		},
		{
			name: "ARRAY of INT64",
			gcv: spanner.GenericColumnValue{
				Type: &sppb.Type{
					Code:             sppb.TypeCode_ARRAY,
					ArrayElementType: &sppb.Type{Code: sppb.TypeCode_INT64},
				},
				Value: structpb.NewListValue(&structpb.ListValue{
					Values: []*structpb.Value{
						structpb.NewStringValue("1"),
						structpb.NewStringValue("2"),
						structpb.NewStringValue("3"),
					},
				}),
			},
			wantJSON: `[1,2,3]`,
		},
		{
			name: "ARRAY with NULL element",
			gcv: spanner.GenericColumnValue{
				Type: &sppb.Type{
					Code:             sppb.TypeCode_ARRAY,
					ArrayElementType: &sppb.Type{Code: sppb.TypeCode_INT64},
				},
				Value: structpb.NewListValue(&structpb.ListValue{
					Values: []*structpb.Value{
						structpb.NewStringValue("1"),
						structpb.NewNullValue(),
						structpb.NewStringValue("3"),
					},
				}),
			},
			wantJSON: `[1,null,3]`,
		},
		{
			name: "NULL ARRAY",
			gcv: spanner.GenericColumnValue{
				Type: &sppb.Type{
					Code:             sppb.TypeCode_ARRAY,
					ArrayElementType: &sppb.Type{Code: sppb.TypeCode_INT64},
				},
				Value: structpb.NewNullValue(),
			},
			wantJSON: "null",
		},
		{
			name: "STRUCT",
			gcv: spanner.GenericColumnValue{
				Type: &sppb.Type{
					Code: sppb.TypeCode_STRUCT,
					StructType: &sppb.StructType{
						Fields: []*sppb.StructType_Field{
							{Name: "name", Type: &sppb.Type{Code: sppb.TypeCode_STRING}},
							{Name: "age", Type: &sppb.Type{Code: sppb.TypeCode_INT64}},
						},
					},
				},
				Value: structpb.NewListValue(&structpb.ListValue{
					Values: []*structpb.Value{
						structpb.NewStringValue("Alice"),
						structpb.NewStringValue("30"),
					},
				}),
			},
			wantJSON: `{"name":"Alice","age":30}`,
		},
		{
			name: "STRUCT with unnamed fields",
			gcv: spanner.GenericColumnValue{
				Type: &sppb.Type{
					Code: sppb.TypeCode_STRUCT,
					StructType: &sppb.StructType{
						Fields: []*sppb.StructType_Field{
							{Name: "", Type: &sppb.Type{Code: sppb.TypeCode_STRING}},
							{Name: "", Type: &sppb.Type{Code: sppb.TypeCode_INT64}},
						},
					},
				},
				Value: structpb.NewListValue(&structpb.ListValue{
					Values: []*structpb.Value{
						structpb.NewStringValue("value"),
						structpb.NewStringValue("42"),
					},
				}),
			},
			wantJSON: `{"_0":"value","_1":42}`,
		},
		{
			name: "STRUCT unnamed fields skip colliding named field",
			gcv: spanner.GenericColumnValue{
				Type: &sppb.Type{
					Code: sppb.TypeCode_STRUCT,
					StructType: &sppb.StructType{
						Fields: []*sppb.StructType_Field{
							{Name: "", Type: &sppb.Type{Code: sppb.TypeCode_INT64}},
							{Name: "", Type: &sppb.Type{Code: sppb.TypeCode_INT64}},
							{Name: "_1", Type: &sppb.Type{Code: sppb.TypeCode_INT64}},
						},
					},
				},
				Value: structpb.NewListValue(&structpb.ListValue{
					Values: []*structpb.Value{
						structpb.NewStringValue("1"),
						structpb.NewStringValue("2"),
						structpb.NewStringValue("3"),
					},
				}),
			},
			wantJSON: `{"_0":1,"_2":2,"_1":3}`,
		},
		{
			name: "ARRAY of STRUCT",
			gcv: spanner.GenericColumnValue{
				Type: &sppb.Type{
					Code: sppb.TypeCode_ARRAY,
					ArrayElementType: &sppb.Type{
						Code: sppb.TypeCode_STRUCT,
						StructType: &sppb.StructType{
							Fields: []*sppb.StructType_Field{
								{Name: "COUNT", Type: &sppb.Type{Code: sppb.TypeCode_INT64}},
								{Name: "MEAN", Type: &sppb.Type{Code: sppb.TypeCode_FLOAT64}},
							},
						},
					},
				},
				Value: structpb.NewListValue(&structpb.ListValue{
					Values: []*structpb.Value{
						structpb.NewListValue(&structpb.ListValue{
							Values: []*structpb.Value{
								structpb.NewStringValue("1"),
								structpb.NewNumberValue(0.057294),
							},
						}),
					},
				}),
			},
			wantJSON: `[{"COUNT":1,"MEAN":0.057294}]`,
		},
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

	row, err := spanner.NewRow([]string{"id", "name", "active"}, []interface{}{int64(42), "Alice", true})
	if err != nil {
		t.Fatalf("NewRow: %v", err)
	}

	got, err := FormatRowJSONObject(JSONFormatConfig, row)
	if err != nil {
		t.Fatalf("FormatRowJSONObject: %v", err)
	}

	want := `{"id":42,"name":"Alice","active":true}`
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
}

func TestNewJSONObjectStructFormatter_EmptyNamer(t *testing.T) {
	t.Parallel()

	formatter := NewJSONObjectStructFormatter(EmptyUnnamedFieldNamer)
	typ := &sppb.Type{
		Code: sppb.TypeCode_STRUCT,
		StructType: &sppb.StructType{
			Fields: []*sppb.StructType_Field{
				{Name: "", Type: &sppb.Type{Code: sppb.TypeCode_INT64}},
				{Name: "", Type: &sppb.Type{Code: sppb.TypeCode_INT64}},
			},
		},
	}
	got := formatter(typ, false, []string{"1", "2"})
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
	typ := &sppb.Type{
		Code: sppb.TypeCode_STRUCT,
		StructType: &sppb.StructType{
			Fields: []*sppb.StructType_Field{
				{Name: "", Type: &sppb.Type{Code: sppb.TypeCode_INT64}},
				{Name: "name", Type: &sppb.Type{Code: sppb.TypeCode_STRING}},
			},
		},
	}
	got := formatter(typ, false, []string{"42", `"Alice"`})
	want := `{"col1":42,"name":"Alice"}`
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
			got := FormatCompactArray(nil, false, tt.elems)
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
			name: "named fields",
			typ: &sppb.Type{
				Code: sppb.TypeCode_STRUCT,
				StructType: &sppb.StructType{
					Fields: []*sppb.StructType_Field{
						{Name: "id", Type: &sppb.Type{Code: sppb.TypeCode_INT64}},
						{Name: "name", Type: &sppb.Type{Code: sppb.TypeCode_STRING}},
					},
				},
			},
			fields: []string{"42", `"Alice"`},
			want:   `{"id":42,"name":"Alice"}`,
		},
		{
			name: "unnamed fields",
			typ: &sppb.Type{
				Code: sppb.TypeCode_STRUCT,
				StructType: &sppb.StructType{
					Fields: []*sppb.StructType_Field{
						{Name: "", Type: &sppb.Type{Code: sppb.TypeCode_INT64}},
						{Name: "", Type: &sppb.Type{Code: sppb.TypeCode_STRING}},
					},
				},
			},
			fields: []string{"1", `"hello"`},
			want:   `{"_0":1,"_1":"hello"}`,
		},
		{
			name: "unnamed fields skip colliding names",
			typ: &sppb.Type{
				Code: sppb.TypeCode_STRUCT,
				StructType: &sppb.StructType{
					Fields: []*sppb.StructType_Field{
						{Name: "", Type: &sppb.Type{Code: sppb.TypeCode_INT64}},
						{Name: "", Type: &sppb.Type{Code: sppb.TypeCode_INT64}},
						{Name: "_1", Type: &sppb.Type{Code: sppb.TypeCode_INT64}},
					},
				},
			},
			fields: []string{"1", "2", "3"},
			want:   `{"_0":1,"_2":2,"_1":3}`,
		},
		{
			name: "field name with special chars",
			typ: &sppb.Type{
				Code: sppb.TypeCode_STRUCT,
				StructType: &sppb.StructType{
					Fields: []*sppb.StructType_Field{
						{Name: "col \"quoted\"", Type: &sppb.Type{Code: sppb.TypeCode_STRING}},
					},
				},
			},
			fields: []string{`"val"`},
			want:   `{"col \"quoted\"":"val"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := FormatJSONObjectStruct(tt.typ, false, tt.fields)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("FormatJSONObjectStruct mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
