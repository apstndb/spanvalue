package writer

import (
	"bytes"
	"errors"
	"testing"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/spanvalue/gcvctor"
	"github.com/google/go-cmp/cmp"
)

func TestCSVWriterWriteValues(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := NewCSVWriter(&out)

	err := w.WriteValues(
		[]string{"name", ""},
		[]spanner.GenericColumnValue{
			gcvctor.StringValue("Alice"),
			gcvctor.SimpleTypedNull(sppb.TypeCode_INT64),
		},
	)
	if err != nil {
		t.Fatalf("WriteValues() error = %v", err)
	}

	err = w.WriteValues(
		[]string{"name", ""},
		[]spanner.GenericColumnValue{
			gcvctor.StringValue("Bob"),
			gcvctor.Int64Value(7),
		},
	)
	if err != nil {
		t.Fatalf("WriteValues() second call error = %v", err)
	}

	want := "name,_0\nAlice,<null>\nBob,7\n"
	if diff := cmp.Diff(want, out.String()); diff != "" {
		t.Fatalf("CSV output mismatch (-want +got):\n%s", diff)
	}
}

func TestJSONLWriterWriteRow(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := NewJSONLWriter(&out)
	row, err := spanner.NewRow([]string{"id", ""}, []interface{}{int64(42), "hello"})
	if err != nil {
		t.Fatalf("spanner.NewRow() error = %v", err)
	}

	if err := w.WriteRow(row); err != nil {
		t.Fatalf("WriteRow() error = %v", err)
	}

	want := "{\"id\":42,\"_0\":\"hello\"}\n"
	if diff := cmp.Diff(want, out.String()); diff != "" {
		t.Fatalf("JSONL output mismatch (-want +got):\n%s", diff)
	}
}

func TestSQLInsertWriterWriteValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		table       string
		columnNames []string
		values      []spanner.GenericColumnValue
		want        string
	}{
		{
			name:        "identifier escaping",
			table:       "user`table",
			columnNames: []string{"id", "na`me"},
			values: []spanner.GenericColumnValue{
				gcvctor.Int64Value(42),
				gcvctor.StringValue("Alice"),
			},
			want: "INSERT INTO `user``table` (`id`, `na``me`) VALUES (42, \"Alice\");\n",
		},
		{
			name:        "value escaping delegated to literal formatter",
			table:       "users",
			columnNames: []string{"payload"},
			values: []spanner.GenericColumnValue{
				gcvctor.StringValue("semi;\nline"),
			},
			want: "INSERT INTO `users` (`payload`) VALUES (\"semi;\\nline\");\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var out bytes.Buffer
			w := NewSQLInsertWriter(&out, tt.table)

			err := w.WriteValues(tt.columnNames, tt.values)
			if err != nil {
				t.Fatalf("WriteValues() error = %v", err)
			}

			if diff := cmp.Diff(tt.want, out.String()); diff != "" {
				t.Fatalf("SQL output mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestSQLInsertWriterWriteValuesEmptyColumnName(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := NewSQLInsertWriter(&out, "users")

	err := w.WriteValues(
		[]string{""},
		[]spanner.GenericColumnValue{gcvctor.Int64Value(42)},
	)
	if !errors.Is(err, ErrEmptyColumnName) {
		t.Fatalf("WriteValues() error = %v, want ErrEmptyColumnName", err)
	}
}
