package writer

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/spanvalue/gcvctor"
	"github.com/google/go-cmp/cmp"
)

func metadataWithColumnNames(names ...string) *sppb.ResultSetMetadata {
	fields := make([]*sppb.StructType_Field, len(names))
	for i, name := range names {
		fields[i] = &sppb.StructType_Field{
			Name: name,
			Type: &sppb.Type{Code: sppb.TypeCode_STRING},
		}
	}
	return &sppb.ResultSetMetadata{
		RowType: &sppb.StructType{Fields: fields},
	}
}

func flushCSVWriter(t *testing.T, w *CSVWriter) {
	t.Helper()
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
}

func TestCSVWriterWriteValues(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := NewCSVWriter(&out)

	err := w.WriteValues(
		[]string{"name", ""},
		[]spanner.GenericColumnValue{
			gcvctor.StringValue("Alice"),
			gcvctor.NullFromCode(sppb.TypeCode_INT64),
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
	flushCSVWriter(t, w)

	want := "name,_0\nAlice,<null>\nBob,7\n"
	if diff := cmp.Diff(want, out.String()); diff != "" {
		t.Fatalf("CSV output mismatch (-want +got):\n%s", diff)
	}
}

func TestCSVWriterWriteGCVsWithMetadata(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := NewCSVWriter(&out, metadataWithColumnNames("name", "age"))

	err := w.WriteGCVs([]spanner.GenericColumnValue{
		gcvctor.StringValue("Alice"),
		gcvctor.Int64Value(42),
	})
	if err != nil {
		t.Fatalf("WriteGCVs() error = %v", err)
	}
	flushCSVWriter(t, w)

	want := "name,age\nAlice,42\n"
	if diff := cmp.Diff(want, out.String()); diff != "" {
		t.Fatalf("CSV output mismatch (-want +got):\n%s", diff)
	}
}

func TestCSVWriterWriteRow(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := NewCSVWriter(&out)

	row, err := spanner.NewRow([]string{"id", ""}, []interface{}{int64(42), "hello"})
	if err != nil {
		t.Fatalf("spanner.NewRow() error = %v", err)
	}

	if err := w.WriteRow(row); err != nil {
		t.Fatalf("WriteRow() error = %v", err)
	}
	flushCSVWriter(t, w)

	want := "id,_0\n42,hello\n"
	if diff := cmp.Diff(want, out.String()); diff != "" {
		t.Fatalf("CSV output mismatch (-want +got):\n%s", diff)
	}
}

func TestCSVWriterWriteHeaderWithMetadata(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := NewCSVWriter(&out, metadataWithColumnNames("name", "age"))

	if err := w.WriteHeader(); err != nil {
		t.Fatalf("WriteHeader() error = %v", err)
	}
	flushCSVWriter(t, w)

	want := "name,age\n"
	if diff := cmp.Diff(want, out.String()); diff != "" {
		t.Fatalf("CSV header mismatch (-want +got):\n%s", diff)
	}
}

func TestCSVWriterWriteHeaderThenWriteGCVs(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := NewCSVWriter(&out, metadataWithColumnNames("name", "age"))

	if err := w.WriteHeader(); err != nil {
		t.Fatalf("WriteHeader() error = %v", err)
	}

	if err := w.WriteGCVs([]spanner.GenericColumnValue{
		gcvctor.StringValue("Alice"),
		gcvctor.Int64Value(42),
	}); err != nil {
		t.Fatalf("WriteGCVs() error = %v", err)
	}
	flushCSVWriter(t, w)

	want := "name,age\nAlice,42\n"
	if diff := cmp.Diff(want, out.String()); diff != "" {
		t.Fatalf("CSV output mismatch (-want +got):\n%s", diff)
	}
}

func TestCSVWriterWriteHeaderWithoutMetadata(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := NewCSVWriter(&out)

	err := w.WriteHeader()
	if !errors.Is(err, ErrMissingColumnNames) {
		t.Fatalf("WriteHeader() error = %v, want ErrMissingColumnNames", err)
	}
}

func TestCSVWriterWriteGCVsWithoutMetadata(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := NewCSVWriter(&out)

	err := w.WriteGCVs([]spanner.GenericColumnValue{gcvctor.StringValue("Alice")})
	if !errors.Is(err, ErrMissingColumnNames) {
		t.Fatalf("WriteGCVs() error = %v, want ErrMissingColumnNames", err)
	}
}

func TestCSVWriterWriteGCVsNilOutputWithoutMetadata(t *testing.T) {
	t.Parallel()

	err := NewCSVWriter(nil).WriteGCVs([]spanner.GenericColumnValue{gcvctor.StringValue("Alice")})
	if !errors.Is(err, ErrNilOutputWriter) {
		t.Fatalf("WriteGCVs() error = %v, want ErrNilOutputWriter", err)
	}
}

func TestCSVWriterWriteHeaderNilOutputWithoutMetadata(t *testing.T) {
	t.Parallel()

	err := NewCSVWriter(nil).WriteHeader()
	if !errors.Is(err, ErrNilOutputWriter) {
		t.Fatalf("WriteHeader() error = %v, want ErrNilOutputWriter", err)
	}
}

func TestWritersReturnErrNilOutputWriter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func() error
	}{
		{
			name: "csv",
			run: func() error {
				w := NewCSVWriter(nil, metadataWithColumnNames("name"))
				return w.WriteGCVs([]spanner.GenericColumnValue{gcvctor.StringValue("Alice")})
			},
		},
		{
			name: "jsonl",
			run: func() error {
				w := NewJSONLWriter(nil, metadataWithColumnNames("name"))
				return w.WriteGCVs([]spanner.GenericColumnValue{gcvctor.StringValue("Alice")})
			},
		},
		{
			name: "sql",
			run: func() error {
				w := NewSQLInsertWriter(nil, "users", metadataWithColumnNames("name"))
				return w.WriteGCVs([]spanner.GenericColumnValue{gcvctor.StringValue("Alice")})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.run()
			if !errors.Is(err, ErrNilOutputWriter) {
				t.Fatalf("error = %v, want ErrNilOutputWriter", err)
			}
		})
	}
}

func TestWritersReturnErrNilRow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func() error
	}{
		{
			name: "csv",
			run: func() error {
				return NewCSVWriter(&bytes.Buffer{}).WriteRow(nil)
			},
		},
		{
			name: "jsonl",
			run: func() error {
				return NewJSONLWriter(&bytes.Buffer{}).WriteRow(nil)
			},
		},
		{
			name: "sql",
			run: func() error {
				return NewSQLInsertWriter(&bytes.Buffer{}, "users").WriteRow(nil)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.run()
			if !errors.Is(err, ErrNilRow) {
				t.Fatalf("error = %v, want ErrNilRow", err)
			}
		})
	}
}

func TestCSVWriterWriteValuesColumnNamesMismatch(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := NewCSVWriter(&out)

	if err := w.WriteValues(
		[]string{"name"},
		[]spanner.GenericColumnValue{gcvctor.StringValue("Alice")},
	); err != nil {
		t.Fatalf("WriteValues() first call error = %v", err)
	}

	err := w.WriteValues(
		[]string{"age"},
		[]spanner.GenericColumnValue{gcvctor.Int64Value(42)},
	)
	if !errors.Is(err, ErrColumnNamesMismatch) {
		t.Fatalf("WriteValues() mismatch error = %v, want ErrColumnNamesMismatch", err)
	}
	if !strings.Contains(err.Error(), "got [age] want [name]") {
		t.Fatalf("WriteValues() mismatch error = %q, want readable column names", err)
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

func TestJSONLWriterWriteGCVsAfterWriteRow(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := NewJSONLWriter(&out)

	row, err := spanner.NewRow([]string{"id", "name"}, []interface{}{int64(42), "hello"})
	if err != nil {
		t.Fatalf("spanner.NewRow() error = %v", err)
	}

	if err := w.WriteRow(row); err != nil {
		t.Fatalf("WriteRow() error = %v", err)
	}

	if err := w.WriteGCVs([]spanner.GenericColumnValue{
		gcvctor.Int64Value(43),
		gcvctor.StringValue("world"),
	}); err != nil {
		t.Fatalf("WriteGCVs() error = %v", err)
	}

	want := "{\"id\":42,\"name\":\"hello\"}\n{\"id\":43,\"name\":\"world\"}\n"
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
		{
			name:        "qualified table name escaping",
			table:       "my`db.users",
			columnNames: []string{"id"},
			values: []spanner.GenericColumnValue{
				gcvctor.Int64Value(42),
			},
			want: "INSERT INTO `my``db`.`users` (`id`) VALUES (42);\n",
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

func TestSQLInsertWriterWriteRow(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := NewSQLInsertWriter(&out, "db.user`table")

	row, err := spanner.NewRow([]string{"na`me", "payload"}, []interface{}{"Alice", "semi;\nline"})
	if err != nil {
		t.Fatalf("spanner.NewRow() error = %v", err)
	}

	if err := w.WriteRow(row); err != nil {
		t.Fatalf("WriteRow() error = %v", err)
	}

	want := "INSERT INTO `db`.`user``table` (`na``me`, `payload`) VALUES (\"Alice\", \"semi;\\nline\");\n"
	if diff := cmp.Diff(want, out.String()); diff != "" {
		t.Fatalf("SQL output mismatch (-want +got):\n%s", diff)
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

func TestSQLInsertWriterWriteValuesRecoverAfterEmptyColumnName(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := NewSQLInsertWriter(&out, "users")

	err := w.WriteValues(
		[]string{""},
		[]spanner.GenericColumnValue{gcvctor.Int64Value(42)},
	)
	if !errors.Is(err, ErrEmptyColumnName) {
		t.Fatalf("first WriteValues() error = %v, want ErrEmptyColumnName", err)
	}

	err = w.WriteValues(
		[]string{"id"},
		[]spanner.GenericColumnValue{gcvctor.Int64Value(42)},
	)
	if err != nil {
		t.Fatalf("second WriteValues() error = %v", err)
	}

	want := "INSERT INTO `users` (`id`) VALUES (42);\n"
	if diff := cmp.Diff(want, out.String()); diff != "" {
		t.Fatalf("SQL output mismatch (-want +got):\n%s", diff)
	}
}

func TestSQLInsertWriterWriteValuesEmptyTableName(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := NewSQLInsertWriter(&out, "")

	err := w.WriteValues(
		[]string{"id"},
		[]spanner.GenericColumnValue{gcvctor.Int64Value(42)},
	)
	if !errors.Is(err, ErrEmptyTableName) {
		t.Fatalf("WriteValues() error = %v, want ErrEmptyTableName", err)
	}
}

func TestSQLInsertWriterWriteGCVsWithoutMetadata(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := NewSQLInsertWriter(&out, "users")

	err := w.WriteGCVs([]spanner.GenericColumnValue{gcvctor.Int64Value(42)})
	if !errors.Is(err, ErrMissingColumnNames) {
		t.Fatalf("WriteGCVs() error = %v, want ErrMissingColumnNames", err)
	}
}

func TestSQLInsertWriterWriteGCVsEmptyTableName(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := NewSQLInsertWriter(&out, "", metadataWithColumnNames("id"))

	err := w.WriteGCVs([]spanner.GenericColumnValue{gcvctor.Int64Value(42)})
	if !errors.Is(err, ErrEmptyTableName) {
		t.Fatalf("WriteGCVs() error = %v, want ErrEmptyTableName", err)
	}
}
