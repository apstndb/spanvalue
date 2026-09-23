package writer

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/apstndb/spantype/typector"
	"github.com/apstndb/spanvalue"
	"github.com/apstndb/spanvalue/gcvctor"
	"github.com/google/go-cmp/cmp"
)

func TestJSONLRecordFraming(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct{ name, wire, want string }{
		{"multiline", "{\r\n \"b\": 9007199254740993,\n \"a\": 1e+09,\n \"b\": 2\r}", `{"j":{"b":9007199254740993,"a":1e+09,"b":2}}`},
		{"single line", `{ "b": 9007199254740993, "a": 1e+09, "b": 2 }`, `{"j":{ "b": 9007199254740993, "a": 1e+09, "b": 2 }}`},
		{"escaped newline", `{"x":"a\nb\rc"}`, `{"j":{"x":"a\nb\rc"}}`},
	} {
		for _, pg := range []bool{false, true} {
			t.Run(tt.name+map[bool]string{false: "/JSON", true: "/PG_JSONB"}[pg], func(t *testing.T) {
				t.Parallel()
				value, err := gcvctor.JSONStringValue(tt.wire)
				if err != nil {
					t.Fatal(err)
				}
				if pg {
					value.Type = typector.PGJSONB()
				}
				root, err := spanvalue.JSONFormatConfig().FormatToplevelColumn(value)
				if err != nil {
					t.Fatal(err)
				}
				if diff := cmp.Diff(tt.wire, root); diff != "" {
					t.Errorf("root formatter changed wire (-want +got):\n%s", diff)
				}
				values := []spanner.GenericColumnValue{value}
				row := mustNewSpannerRow(t, []string{"j"}, []any{value})
				check := func(got string, err error) {
					t.Helper()
					if err != nil {
						t.Fatal(err)
					}
					if diff := cmp.Diff(tt.want, got); diff != "" {
						t.Errorf("record (-want +got):\n%s", diff)
					}
					if strings.ContainsAny(got, "\r\n") || !json.Valid([]byte(got)) {
						t.Errorf("invalid single-line JSON record: %q", got)
					}
				}
				check(FormatJSONLValues(nil, []string{"j"}, values, nil))
				check(FormatJSONLRow(nil, row, nil))
				for got, err := range FormatJSONLRowSeq(nil, RowSeq(row), nil) {
					check(got, err)
				}
				var out bytes.Buffer
				w, err := NewJSONLWriter(&out, WithColumnNames([]string{"j"}))
				if err != nil {
					t.Fatal(err)
				}
				for range 2 {
					if err := w.WriteGCVs(values); err != nil {
						t.Fatal(err)
					}
				}
				if err := w.Flush(); err != nil {
					t.Fatal(err)
				}
				if diff := cmp.Diff(tt.want+"\n"+tt.want+"\n", out.String()); diff != "" {
					t.Errorf("stream (-want +got):\n%s", diff)
				}
			})
		}
	}
}

func TestJSONLNestedMultilineValue(t *testing.T) {
	t.Parallel()
	value, err := gcvctor.JSONStringValue("[\n 1,\n 2\n]")
	if err != nil {
		t.Fatal(err)
	}
	child, err := gcvctor.StructValueOf([]string{"j"}, []spanner.GenericColumnValue{value})
	if err != nil {
		t.Fatal(err)
	}
	array, err := gcvctor.ArrayValue(child)
	if err != nil {
		t.Fatal(err)
	}
	got, err := FormatJSONLValues(nil, []string{"nested"}, []spanner.GenericColumnValue{array}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(`{"nested":[{"j":[1,2]}]}`, got); diff != "" {
		t.Errorf("(-want +got):\n%s", diff)
	}
}

func TestJSONLRejectsMalformedMultilineRecord(t *testing.T) {
	t.Parallel()
	fc := spanvalue.JSONFormatConfig().WithComplexPlugin(func(spanvalue.Formatter, spanner.GenericColumnValue, bool) (string, error) { return "{\ninvalid}", nil })
	values := []spanner.GenericColumnValue{gcvctor.Int64Value(1)}
	if got, err := FormatJSONLValues(fc, []string{"j"}, values, nil); err == nil || got != "" {
		t.Errorf("got %q, error %v; want compaction failure", got, err)
	}
	var out bytes.Buffer
	w, err := NewJSONLWriter(&out, WithColumnNames([]string{"j"}), WithFormatter(fc))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteGCVs(values); err == nil {
		t.Fatal("want compaction failure")
	}
	if out.Len() != 0 {
		t.Errorf("wrote malformed record: %q", out.String())
	}
}
