package writer

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/spanvalue"
	"github.com/apstndb/spanvalue/gcvctor"
	"github.com/apstndb/spanvalue/internal"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/types/known/structpb"
)

var (
	_ Writer      = (*DelimitedWriter)(nil)
	_ Writer      = (*JSONLWriter)(nil)
	_ Writer      = (*SQLInsertWriter)(nil)
	_ Flusher     = (*DelimitedWriter)(nil)
	_ Flusher     = (*JSONLWriter)(nil)
	_ Flusher     = (*SQLInsertWriter)(nil)
	_ FlushWriter = (*DelimitedWriter)(nil)
	_ FlushWriter = (*JSONLWriter)(nil)
	_ FlushWriter = (*SQLInsertWriter)(nil)
)

func metadataWithColumnNames(names ...string) *sppb.ResultSetMetadata {
	return &sppb.ResultSetMetadata{RowType: rowTypeWithColumnNames(names...)}
}

func rowTypeWithColumnNames(names ...string) *sppb.StructType {
	fields := make([]*sppb.StructType_Field, len(names))
	for i, name := range names {
		code := sppb.TypeCode_INT64
		switch name {
		case "name", "note", "payload", "full_name":
			code = sppb.TypeCode_STRING
		}
		fields[i] = &sppb.StructType_Field{
			Name: name,
			Type: &sppb.Type{Code: code},
		}
	}
	return &sppb.StructType{Fields: fields}
}

func flushDelimitedWriter(t *testing.T, w *DelimitedWriter) {
	t.Helper()
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
}

func TestNewCSVWriterHelper(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := NewCSVWriter(&out, WithMetadata(metadataWithColumnNames("name")))

	if err := w.WriteGCVs([]spanner.GenericColumnValue{gcvctor.StringValue("Alice")}); err != nil {
		t.Fatalf("WriteGCVs() error = %v", err)
	}
	flushDelimitedWriter(t, w)

	want := "name\nAlice\n"
	if diff := cmp.Diff(want, out.String()); diff != "" {
		t.Fatalf("CSV output mismatch (-want +got):\n%s", diff)
	}
}

func TestDelimitedWriterWriteValues(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := NewDelimitedWriter(&out, Comma)

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
	flushDelimitedWriter(t, w)

	want := "name,_0\nAlice,<null>\nBob,7\n"
	if diff := cmp.Diff(want, out.String()); diff != "" {
		t.Fatalf("CSV output mismatch (-want +got):\n%s", diff)
	}
}

func TestDelimitedWriterWriteValuesWithCustomDelimiter(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := NewDelimitedWriter(&out, '\t')

	err := w.WriteValues(
		[]string{"name", "note", "with_tab"},
		[]spanner.GenericColumnValue{
			gcvctor.StringValue("Alice"),
			gcvctor.StringValue("comma, ok"),
			gcvctor.StringValue("tab\tok"),
		},
	)
	if err != nil {
		t.Fatalf("WriteValues() error = %v", err)
	}
	flushDelimitedWriter(t, w)

	want := "name\tnote\twith_tab\nAlice\tcomma, ok\t\"tab\tok\"\n"
	if diff := cmp.Diff(want, out.String()); diff != "" {
		t.Fatalf("delimited output mismatch (-want +got):\n%s", diff)
	}
}

func TestDelimitedWriterWithOptions(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := NewDelimitedWriterWithOptions(
		&out,
		'\t',
		WithMetadata(metadataWithColumnNames("name", "age")),
		WithFormatter(spanvalue.LiteralFormatConfig()),
		WithHeader(false),
		WithUnnamedFieldNamer(nil),
	)

	err := w.WriteGCVs([]spanner.GenericColumnValue{
		gcvctor.StringValue("Alice"),
		gcvctor.Int64Value(42),
	})
	if err != nil {
		t.Fatalf("WriteGCVs() error = %v", err)
	}
	flushDelimitedWriter(t, w)

	want := "\"\"\"Alice\"\"\"\t42\n"
	if diff := cmp.Diff(want, out.String()); diff != "" {
		t.Fatalf("delimited output mismatch (-want +got):\n%s", diff)
	}
}

func TestDelimitedWriterWriteValuesZeroDelimiterInvalid(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := NewDelimitedWriter(&out, 0)

	err := w.WriteValues(
		[]string{"name", "note"},
		[]spanner.GenericColumnValue{
			gcvctor.StringValue("Alice"),
			gcvctor.StringValue("comma, ok"),
		},
	)
	if !errors.Is(err, ErrInvalidDelimiter) {
		t.Fatalf("WriteValues() error = %v, want ErrInvalidDelimiter", err)
	}
}

func TestDelimitedWriterWriteValuesInvalidDelimiter(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := NewDelimitedWriter(&out, '\n')

	err := w.WriteValues(
		[]string{"name"},
		[]spanner.GenericColumnValue{gcvctor.StringValue("Alice")},
	)
	if !errors.Is(err, ErrInvalidDelimiter) {
		t.Fatalf("WriteValues() error = %v, want ErrInvalidDelimiter", err)
	}
}

func TestDelimitedWriterPrepare(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := NewDelimitedWriter(&out, Comma)
	if err := w.Prepare(metadataWithColumnNames("name", "age")); err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}

	if err := w.WriteGCVs([]spanner.GenericColumnValue{
		gcvctor.StringValue("Alice"),
		gcvctor.Int64Value(42),
	}); err != nil {
		t.Fatalf("WriteGCVs() error = %v", err)
	}
	flushDelimitedWriter(t, w)

	want := "name,age\nAlice,42\n"
	if diff := cmp.Diff(want, out.String()); diff != "" {
		t.Fatalf("CSV output mismatch (-want +got):\n%s", diff)
	}

	err := w.Prepare(metadataWithColumnNames("name", "score"))
	if !errors.Is(err, ErrColumnNamesMismatch) {
		t.Fatalf("Prepare() mismatch error = %v, want ErrColumnNamesMismatch", err)
	}
}

func TestJSONLWriterPrepare(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := NewJSONLWriter(&out)
	if err := w.Prepare(metadataWithColumnNames("", "age")); err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}

	if err := w.WriteGCVs([]spanner.GenericColumnValue{
		gcvctor.StringValue("Alice"),
		gcvctor.Int64Value(42),
	}); err != nil {
		t.Fatalf("WriteGCVs() error = %v", err)
	}

	want := "{\"_0\":\"Alice\",\"age\":42}\n"
	if diff := cmp.Diff(want, out.String()); diff != "" {
		t.Fatalf("JSONL output mismatch (-want +got):\n%s", diff)
	}

	err := w.Prepare(metadataWithColumnNames("", "score"))
	if !errors.Is(err, ErrColumnNamesMismatch) {
		t.Fatalf("Prepare() mismatch error = %v, want ErrColumnNamesMismatch", err)
	}
}

func TestSQLInsertWriterPrepare(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := NewSQLInsertWriter(&out, "users")
	if err := w.Prepare(metadataWithColumnNames("id", "name")); err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}

	if err := w.WriteGCVs([]spanner.GenericColumnValue{
		gcvctor.Int64Value(42),
		gcvctor.StringValue("Alice"),
	}); err != nil {
		t.Fatalf("WriteGCVs() error = %v", err)
	}

	want := "INSERT INTO `users` (`id`, `name`) VALUES (42, \"Alice\");\n"
	if diff := cmp.Diff(want, out.String()); diff != "" {
		t.Fatalf("SQL output mismatch (-want +got):\n%s", diff)
	}

	err := w.Prepare(metadataWithColumnNames("id", "full_name"))
	if !errors.Is(err, ErrColumnNamesMismatch) {
		t.Fatalf("Prepare() mismatch error = %v, want ErrColumnNamesMismatch", err)
	}
}

func TestWritersPrepareWithoutMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		prepare func(*sppb.ResultSetMetadata) error
	}{
		{
			name:    "csv",
			prepare: NewDelimitedWriter(&bytes.Buffer{}, Comma).Prepare,
		},
		{
			name:    "jsonl",
			prepare: NewJSONLWriter(&bytes.Buffer{}).Prepare,
		},
		{
			name:    "sql",
			prepare: NewSQLInsertWriter(&bytes.Buffer{}, "users").Prepare,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.prepare(nil)
			if !errors.Is(err, ErrMissingColumnNames) {
				t.Fatalf("Prepare(nil) error = %v, want ErrMissingColumnNames", err)
			}
		})
	}
}

func TestDelimitedWriterWriteGCVsWithMetadata(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := NewDelimitedWriter(&out, Comma, WithMetadata(metadataWithColumnNames("name", "age")))

	err := w.WriteGCVs([]spanner.GenericColumnValue{
		gcvctor.StringValue("Alice"),
		gcvctor.Int64Value(42),
	})
	if err != nil {
		t.Fatalf("WriteGCVs() error = %v", err)
	}
	flushDelimitedWriter(t, w)

	want := "name,age\nAlice,42\n"
	if diff := cmp.Diff(want, out.String()); diff != "" {
		t.Fatalf("CSV output mismatch (-want +got):\n%s", diff)
	}
}

func TestDelimitedWriterWriteRow(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := NewDelimitedWriter(&out, Comma)

	row, err := spanner.NewRow([]string{"id", ""}, []interface{}{int64(42), "hello"})
	if err != nil {
		t.Fatalf("spanner.NewRow() error = %v", err)
	}

	if err := w.WriteRow(row); err != nil {
		t.Fatalf("WriteRow() error = %v", err)
	}
	flushDelimitedWriter(t, w)

	want := "id,_0\n42,hello\n"
	if diff := cmp.Diff(want, out.String()); diff != "" {
		t.Fatalf("CSV output mismatch (-want +got):\n%s", diff)
	}
}

func TestDelimitedWriterWriteHeaderWithMetadata(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := NewDelimitedWriter(&out, Comma, WithMetadata(metadataWithColumnNames("name", "age")))

	if err := w.WriteHeader(); err != nil {
		t.Fatalf("WriteHeader() error = %v", err)
	}
	flushDelimitedWriter(t, w)

	want := "name,age\n"
	if diff := cmp.Diff(want, out.String()); diff != "" {
		t.Fatalf("CSV header mismatch (-want +got):\n%s", diff)
	}
}

func TestUnbufferedWritersFlushIsNoop(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		writer Flusher
	}{
		{
			name:   "jsonl",
			writer: NewJSONLWriter(&bytes.Buffer{}),
		},
		{
			name:   "sql",
			writer: NewSQLInsertWriter(&bytes.Buffer{}, "users"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if err := tt.writer.Flush(); err != nil {
				t.Fatalf("Flush() error = %v", err)
			}
		})
	}
}

func TestDelimitedWriterWriteHeaderThenWriteGCVs(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := NewDelimitedWriter(&out, Comma, WithMetadata(metadataWithColumnNames("name", "age")))

	if err := w.WriteHeader(); err != nil {
		t.Fatalf("WriteHeader() error = %v", err)
	}

	if err := w.WriteGCVs([]spanner.GenericColumnValue{
		gcvctor.StringValue("Alice"),
		gcvctor.Int64Value(42),
	}); err != nil {
		t.Fatalf("WriteGCVs() error = %v", err)
	}
	flushDelimitedWriter(t, w)

	want := "name,age\nAlice,42\n"
	if diff := cmp.Diff(want, out.String()); diff != "" {
		t.Fatalf("CSV output mismatch (-want +got):\n%s", diff)
	}
}

func TestDelimitedWriterWriteHeaderWithoutMetadata(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := NewDelimitedWriter(&out, Comma)

	err := w.WriteHeader()
	if !errors.Is(err, ErrMissingColumnNames) {
		t.Fatalf("WriteHeader() error = %v, want ErrMissingColumnNames", err)
	}
}

func TestDelimitedWriterWriteHeaderAfterData(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := NewDelimitedWriter(&out, Comma, WithMetadata(metadataWithColumnNames("name", "age")))
	w.Header = false

	if err := w.WriteGCVs([]spanner.GenericColumnValue{
		gcvctor.StringValue("Alice"),
		gcvctor.Int64Value(42),
	}); err != nil {
		t.Fatalf("WriteGCVs() error = %v", err)
	}

	err := w.WriteHeader()
	if !errors.Is(err, ErrHeaderAfterData) {
		t.Fatalf("WriteHeader() error = %v, want ErrHeaderAfterData", err)
	}
}

func TestDelimitedWriterWriteGCVsWithoutMetadata(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := NewDelimitedWriter(&out, Comma)

	err := w.WriteGCVs([]spanner.GenericColumnValue{gcvctor.StringValue("Alice")})
	if !errors.Is(err, ErrMissingColumnNames) {
		t.Fatalf("WriteGCVs() error = %v, want ErrMissingColumnNames", err)
	}
}

func TestDelimitedWriterWriteGCVsNilOutputWithoutMetadata(t *testing.T) {
	t.Parallel()

	err := NewDelimitedWriter(nil, Comma).WriteGCVs([]spanner.GenericColumnValue{gcvctor.StringValue("Alice")})
	if !errors.Is(err, ErrNilOutputWriter) {
		t.Fatalf("WriteGCVs() error = %v, want ErrNilOutputWriter", err)
	}
}

func TestDelimitedWriterWriteHeaderNilOutputWithoutMetadata(t *testing.T) {
	t.Parallel()

	err := NewDelimitedWriter(nil, Comma).WriteHeader()
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
				w := NewDelimitedWriter(nil, Comma, WithMetadata(metadataWithColumnNames("name")))
				return w.WriteGCVs([]spanner.GenericColumnValue{gcvctor.StringValue("Alice")})
			},
		},
		{
			name: "jsonl",
			run: func() error {
				w := NewJSONLWriter(nil, WithMetadata(metadataWithColumnNames("name")))
				return w.WriteGCVs([]spanner.GenericColumnValue{gcvctor.StringValue("Alice")})
			},
		},
		{
			name: "sql",
			run: func() error {
				w := NewSQLInsertWriter(nil, "users", WithMetadata(metadataWithColumnNames("name")))
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
				return NewDelimitedWriter(&bytes.Buffer{}, Comma).WriteRow(nil)
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

func TestDelimitedWriterWriteValuesColumnNamesMismatch(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := NewDelimitedWriter(&out, Comma)

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

func TestJSONLWriterWithOptions(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := NewJSONLWriterWithOptions(
		&out,
		WithMetadata(metadataWithColumnNames("", "age")),
		WithUnnamedFieldNamer(nil),
	)

	if err := w.WriteGCVs([]spanner.GenericColumnValue{
		gcvctor.Int64Value(42),
		gcvctor.Int64Value(7),
	}); err != nil {
		t.Fatalf("WriteGCVs() error = %v", err)
	}

	want := "{\"\":42,\"age\":7}\n"
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

func TestJSONLWriterWriteGCVsKeepsResolvedNamesAfterNamerChange(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := NewJSONLWriter(&out)

	row, err := spanner.NewRow([]string{"", ""}, []interface{}{int64(42), "hello"})
	if err != nil {
		t.Fatalf("spanner.NewRow() error = %v", err)
	}

	if err := w.WriteRow(row); err != nil {
		t.Fatalf("WriteRow() error = %v", err)
	}

	w.UnnamedFieldNamer = nil
	if err := w.WriteGCVs([]spanner.GenericColumnValue{
		gcvctor.Int64Value(43),
		gcvctor.StringValue("world"),
	}); err != nil {
		t.Fatalf("WriteGCVs() error = %v", err)
	}

	want := "{\"_0\":42,\"_1\":\"hello\"}\n{\"_0\":43,\"_1\":\"world\"}\n"
	if diff := cmp.Diff(want, out.String()); diff != "" {
		t.Fatalf("JSONL output mismatch (-want +got):\n%s", diff)
	}
}

func TestJSONLWriterWriteGCVs_MismatchedCachedKeys(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := NewJSONLWriter(&out, WithMetadata(metadataWithColumnNames("name", "age")))
	w.marshaledKeys = [][]byte{[]byte(`"name"`)}

	err := w.WriteGCVs([]spanner.GenericColumnValue{
		gcvctor.StringValue("Alice"),
		gcvctor.Int64Value(42),
	})
	if !errors.Is(err, internal.ErrMismatchedJSONObjectFields) {
		t.Fatalf("WriteGCVs() error = %v, want ErrMismatchedJSONObjectFields", err)
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

func TestSQLInsertWriterInsertKind(t *testing.T) {
	t.Parallel()

	values := []spanner.GenericColumnValue{gcvctor.Int64Value(42)}
	columnNames := []string{"id"}

	tests := []struct {
		name string
		kind SQLInsertKind
		want string
	}{
		{
			name: "insert or ignore",
			kind: SQLInsertOrIgnore,
			want: "INSERT OR IGNORE INTO `users` (`id`) VALUES (42);\n",
		},
		{
			name: "insert or update",
			kind: SQLInsertOrUpdate,
			want: "INSERT OR UPDATE INTO `users` (`id`) VALUES (42);\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var out bytes.Buffer
			w := NewSQLInsertWriter(&out, "users", WithSQLInsertKind(tt.kind))
			if err := w.WriteValues(columnNames, values); err != nil {
				t.Fatalf("WriteValues() error = %v", err)
			}
			if diff := cmp.Diff(tt.want, out.String()); diff != "" {
				t.Fatalf("SQL output mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestSQLInsertWriterWithOptions(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := NewSQLInsertWriterWithOptions(
		&out,
		"users",
		WithMetadata(metadataWithColumnNames("id", "name")),
		WithFormatter(spanvalue.LiteralFormatConfig()),
	)

	if err := w.WriteGCVs([]spanner.GenericColumnValue{
		gcvctor.Int64Value(42),
		gcvctor.StringValue("Alice"),
	}); err != nil {
		t.Fatalf("WriteGCVs() error = %v", err)
	}

	want := "INSERT INTO `users` (`id`, `name`) VALUES (42, \"Alice\");\n"
	if diff := cmp.Diff(want, out.String()); diff != "" {
		t.Fatalf("SQL output mismatch (-want +got):\n%s", diff)
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

func TestSQLInsertWriterWriteValuesTableChangeAfterCache(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := NewSQLInsertWriter(&out, "db.users")

	err := w.WriteValues(
		[]string{"id"},
		[]spanner.GenericColumnValue{gcvctor.Int64Value(1)},
	)
	if err != nil {
		t.Fatalf("first WriteValues() error = %v", err)
	}

	w.Table = "archive.users"
	err = w.WriteGCVs([]spanner.GenericColumnValue{gcvctor.Int64Value(2)})
	if err != nil {
		t.Fatalf("WriteGCVs() after table change error = %v", err)
	}

	want := "" +
		"INSERT INTO `db`.`users` (`id`) VALUES (1);\n" +
		"INSERT INTO `archive`.`users` (`id`) VALUES (2);\n"
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

func TestSQLInsertWriterWriteValuesEmptyQualifiedTableSegment(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := NewSQLInsertWriter(&out, "db..users")

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
	w := NewSQLInsertWriter(&out, "", WithMetadata(metadataWithColumnNames("id")))

	err := w.WriteGCVs([]spanner.GenericColumnValue{gcvctor.Int64Value(42)})
	if !errors.Is(err, ErrEmptyTableName) {
		t.Fatalf("WriteGCVs() error = %v, want ErrEmptyTableName", err)
	}
}

func TestRowDataAndOneRowFormatHelpers(t *testing.T) {
	t.Parallel()

	row, err := spanner.NewRow([]string{"id", "note"}, []interface{}{int64(42), "comma, ok"})
	if err != nil {
		t.Fatalf("spanner.NewRow() error = %v", err)
	}

	columnNames, values, err := RowData(row)
	if err != nil {
		t.Fatalf("RowData() error = %v", err)
	}
	if diff := cmp.Diff([]string{"id", "note"}, columnNames); diff != "" {
		t.Fatalf("RowData() column names mismatch (-want +got):\n%s", diff)
	}

	csvRow, err := FormatDelimitedValues(nil, columnNames, values, Comma)
	if err != nil {
		t.Fatalf("FormatDelimitedValues() error = %v", err)
	}
	if want := `42,"comma, ok"`; csvRow != want {
		t.Fatalf("FormatDelimitedValues() = %q, want %q", csvRow, want)
	}

	tsvRow, err := FormatDelimitedRow(nil, row, '\t')
	if err != nil {
		t.Fatalf("FormatDelimitedRow() error = %v", err)
	}
	if want := "42\tcomma, ok"; tsvRow != want {
		t.Fatalf("FormatDelimitedRow() = %q, want %q", tsvRow, want)
	}

	jsonRow, err := FormatJSONLRow(nil, row, spanvalue.IndexedUnnamedFieldNamer)
	if err != nil {
		t.Fatalf("FormatJSONLRow() error = %v", err)
	}
	if want := `{"id":42,"note":"comma, ok"}`; jsonRow != want {
		t.Fatalf("FormatJSONLRow() = %q, want %q", jsonRow, want)
	}
}

func TestFormatDelimitedValuesInvalidDelimiter(t *testing.T) {
	t.Parallel()

	values := []spanner.GenericColumnValue{gcvctor.StringValue("Alice")}
	columnNames := []string{"name"}

	tests := []struct {
		name      string
		delimiter rune
		format    func(rune) error
	}{
		{
			name:      "newline in FormatDelimitedValues",
			delimiter: '\n',
			format: func(delim rune) error {
				_, err := FormatDelimitedValues(nil, columnNames, values, delim)
				return err
			},
		},
		{
			name:      "zero in FormatDelimitedValues",
			delimiter: 0,
			format: func(delim rune) error {
				_, err := FormatDelimitedValues(nil, columnNames, values, delim)
				return err
			},
		},
		{
			name:      "zero in FormatDelimitedRow",
			delimiter: 0,
			format: func(delim rune) error {
				row, err := spanner.NewRow(columnNames, []interface{}{"Alice"})
				if err != nil {
					return err
				}
				_, err = FormatDelimitedRow(nil, row, delim)
				return err
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if err := tt.format(tt.delimiter); !errors.Is(err, ErrInvalidDelimiter) {
				t.Fatalf("format error = %v, want ErrInvalidDelimiter", err)
			}
		})
	}
}

func TestWithColumnNamesHeaderlessDelimited(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := NewDelimitedWriter(&out, ',', WithColumnNames([]string{"id", "name"}), WithHeader(false))
	if err := w.WriteGCVs([]spanner.GenericColumnValue{
		gcvctor.Int64Value(1),
		gcvctor.StringValue("a"),
	}); err != nil {
		t.Fatalf("WriteGCVs() error = %v", err)
	}
	flushDelimitedWriter(t, w)

	want := "1,a\n"
	if diff := cmp.Diff(want, out.String()); diff != "" {
		t.Fatalf("output mismatch (-want +got):\n%s", diff)
	}
}

func TestWithColumnNamesWriteGCVs(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := NewDelimitedWriter(&out, '\t', WithColumnNames([]string{"id", "name"}))
	if err := w.WriteGCVs([]spanner.GenericColumnValue{
		gcvctor.Int64Value(1),
		gcvctor.StringValue("a"),
	}); err != nil {
		t.Fatalf("WriteGCVs() error = %v", err)
	}
	flushDelimitedWriter(t, w)

	want := "id\tname\n1\ta\n"
	if diff := cmp.Diff(want, out.String()); diff != "" {
		t.Fatalf("output mismatch (-want +got):\n%s", diff)
	}
}

func TestWithRowTypeWriteProtoValues(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := NewJSONLWriter(&out, WithRowType(rowTypeWithColumnNames("id", "name")))
	if err := w.WriteProtoValues([]*structpb.Value{
		structpb.NewStringValue("42"),
		structpb.NewStringValue("Alice"),
	}); err != nil {
		t.Fatalf("WriteProtoValues() error = %v", err)
	}

	want := "{\"id\":42,\"name\":\"Alice\"}\n"
	if diff := cmp.Diff(want, out.String()); diff != "" {
		t.Fatalf("output mismatch (-want +got):\n%s", diff)
	}
}

func TestWriteProtoValuesMissingRowType(t *testing.T) {
	t.Parallel()

	w := NewDelimitedWriter(&bytes.Buffer{}, ',')
	err := w.WriteProtoValues([]*structpb.Value{structpb.NewStringValue("1")})
	if !errors.Is(err, ErrMissingRowType) {
		t.Fatalf("WriteProtoValues() error = %v, want ErrMissingRowType", err)
	}
}

func TestWriteProtoValuesNilFieldType(t *testing.T) {
	t.Parallel()

	rowType := &sppb.StructType{
		Fields: []*sppb.StructType_Field{
			{Name: "id", Type: nil},
		},
	}
	w := NewSQLInsertWriter(&bytes.Buffer{}, "users", WithRowType(rowType))
	err := w.WriteProtoValues([]*structpb.Value{structpb.NewStringValue("1")})
	if !errors.Is(err, spanvalue.ErrNilStructField) {
		t.Fatalf("WriteProtoValues() error = %v, want ErrNilStructField", err)
	}
}

func TestDelimitedWriterPrepareColumnNames(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := NewDelimitedWriter(&out, ',')
	if err := w.PrepareColumnNames([]string{"name", "age"}); err != nil {
		t.Fatalf("PrepareColumnNames() error = %v", err)
	}
	if err := w.WriteGCVs([]spanner.GenericColumnValue{
		gcvctor.StringValue("Ada"),
		gcvctor.Int64Value(30),
	}); err != nil {
		t.Fatalf("WriteGCVs() error = %v", err)
	}
	flushDelimitedWriter(t, w)

	want := "name,age\nAda,30\n"
	if diff := cmp.Diff(want, out.String()); diff != "" {
		t.Fatalf("output mismatch (-want +got):\n%s", diff)
	}

	err := w.PrepareColumnNames([]string{"name", "score"})
	if !errors.Is(err, ErrColumnNamesMismatch) {
		t.Fatalf("PrepareColumnNames() mismatch error = %v, want ErrColumnNamesMismatch", err)
	}
}

func TestPrepareRowTypeMatchesPrepareMetadata(t *testing.T) {
	t.Parallel()

	meta := metadataWithColumnNames("id", "name")
	var delimited, jsonl bytes.Buffer
	dw := NewDelimitedWriter(&delimited, ',')
	jw := NewJSONLWriter(&jsonl)
	if err := dw.PrepareRowType(meta.GetRowType()); err != nil {
		t.Fatalf("DelimitedWriter.PrepareRowType() error = %v", err)
	}
	if err := jw.Prepare(meta); err != nil {
		t.Fatalf("JSONLWriter.Prepare() error = %v", err)
	}
	if err := dw.WriteGCVs([]spanner.GenericColumnValue{gcvctor.Int64Value(1), gcvctor.StringValue("a")}); err != nil {
		t.Fatalf("WriteGCVs() error = %v", err)
	}
	if err := jw.WriteGCVs([]spanner.GenericColumnValue{gcvctor.Int64Value(2), gcvctor.StringValue("b")}); err != nil {
		t.Fatalf("WriteGCVs() error = %v", err)
	}
	flushDelimitedWriter(t, dw)
	if want := "id,name\n1,a\n"; delimited.String() != want {
		t.Fatalf("delimited = %q, want %q", delimited.String(), want)
	}
	if want := "{\"id\":2,\"name\":\"b\"}\n"; jsonl.String() != want {
		t.Fatalf("jsonl = %q, want %q", jsonl.String(), want)
	}
}
