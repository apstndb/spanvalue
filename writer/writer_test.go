package writer

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	databasepb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
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

func emptyRowType() *sppb.StructType {
	return &sppb.StructType{Fields: []*sppb.StructType_Field{}}
}

func rowTypeWithColumnNames(names ...string) *sppb.StructType {
	fields := make([]*sppb.StructType_Field, len(names))
	for i, name := range names {
		code := sppb.TypeCode_INT64
		switch name {
		case "name", "note", "payload", "full_name":
			code = sppb.TypeCode_STRING
		case "amount":
			code = sppb.TypeCode_NUMERIC
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
	w := mustNewCSVWriter(t, &out, WithMetadata(metadataWithColumnNames("name")))

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
	w := mustNewDelimitedWriter(t, &out, Comma)

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
	w := mustNewDelimitedWriter(t, &out, '\t')

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
	w := mustNewDelimitedWriter(t,
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

func TestDelimitedWriterFormatConfig(t *testing.T) {
	t.Parallel()

	cfg := spanvalue.LiteralFormatConfig()
	w := mustNewDelimitedWriter(t, &bytes.Buffer{}, ',', WithFormatter(cfg))
	if w.FormatConfig() != cfg {
		t.Fatalf("FormatConfig() = %p, want %p", w.FormatConfig(), cfg)
	}

	nilFormatterW := mustNewDelimitedWriter(t, &bytes.Buffer{}, ',', WithFormatter(nil))
	gotFormatter := nilFormatterW.FormatConfig()
	if gotFormatter == nil {
		t.Fatal("FormatConfig() with nil formatter = nil, want effective simple formatter")
	}
	if nilFormatterW.FormatConfig() != gotFormatter {
		t.Fatal("FormatConfig() should return the same cached formatter on repeat calls")
	}
}

func TestJSONLWriterFormatConfig(t *testing.T) {
	t.Parallel()

	cfg := spanvalue.LiteralFormatConfig()
	w := mustNewJSONLWriter(t, &bytes.Buffer{}, WithFormatter(cfg))
	if w.FormatConfig() != cfg {
		t.Fatalf("FormatConfig() = %p, want %p", w.FormatConfig(), cfg)
	}

	nilFormatterW := mustNewJSONLWriter(t, &bytes.Buffer{}, WithFormatter(nil))
	gotFormatter := nilFormatterW.FormatConfig()
	if gotFormatter == nil {
		t.Fatal("FormatConfig() with nil formatter = nil, want effective JSON formatter")
	}
	if nilFormatterW.FormatConfig() != gotFormatter {
		t.Fatal("FormatConfig() should return the same cached formatter on repeat calls")
	}
}

func TestDelimitedWriterWriteValuesZeroDelimiterInvalid(t *testing.T) {
	t.Parallel()

	_, err := NewDelimitedWriter(&bytes.Buffer{}, 0)
	if !errors.Is(err, ErrInvalidDelimiter) {
		t.Fatalf("NewDelimitedWriter() error = %v, want ErrInvalidDelimiter", err)
	}
}

func TestDelimitedWriterWriteValuesInvalidDelimiter(t *testing.T) {
	t.Parallel()

	_, err := NewDelimitedWriter(&bytes.Buffer{}, '\n')
	if !errors.Is(err, ErrInvalidDelimiter) {
		t.Fatalf("NewDelimitedWriter() error = %v, want ErrInvalidDelimiter", err)
	}
}

func TestDelimitedWriterPrepare(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := mustNewDelimitedWriter(t, &out, Comma)
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
	w := mustNewJSONLWriter(t, &out)
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
	w := mustNewSQLInsertWriter(t, &out, "users")
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

func TestWritersPrepareNilMetadataRegistersEmptySchema(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func() error
	}{
		{
			name: "csv",
			run: func() error {
				w := mustNewDelimitedWriter(t, &bytes.Buffer{}, Comma, WithHeader(true))
				if err := w.Prepare(nil); err != nil {
					return err
				}
				return w.Flush()
			},
		},
		{
			name: "jsonl",
			run: func() error {
				w := mustNewJSONLWriter(t, &bytes.Buffer{})
				if err := w.Prepare(nil); err != nil {
					return err
				}
				return w.Flush()
			},
		},
		{
			name: "sql",
			run: func() error {
				w := mustNewSQLInsertWriter(t, &bytes.Buffer{}, "users")
				if err := w.Prepare(nil); err != nil {
					return err
				}
				return w.Flush()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if err := tt.run(); err != nil {
				t.Fatalf("run() error = %v", err)
			}
		})
	}
}

func TestDelimitedWriterWriteGCVsWithMetadata(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := mustNewDelimitedWriter(t, &out, Comma, WithMetadata(metadataWithColumnNames("name", "age")))

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

func TestNewCSVWriter_numericWireAsIs(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := mustNewCSVWriter(t, &out, WithMetadata(metadataWithColumnNames("amount")))
	if err := w.WriteGCVs([]spanner.GenericColumnValue{
		gcvctor.StringBasedValueFromCode(sppb.TypeCode_NUMERIC, "99.5"),
	}); err != nil {
		t.Fatalf("WriteGCVs() error = %v", err)
	}
	flushDelimitedWriter(t, w)

	want := "amount\n99.5\n"
	if diff := cmp.Diff(want, out.String()); diff != "" {
		t.Fatalf("CSV output mismatch (-want +got):\n%s", diff)
	}
}

func TestDelimitedWriterWriteRow(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := mustNewDelimitedWriter(t, &out, Comma)

	row, err := spanner.NewRow([]string{"id", ""}, []any{int64(42), "hello"})
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
	w := mustNewDelimitedWriter(t, &out, Comma, WithMetadata(metadataWithColumnNames("name", "age")))

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
			writer: mustNewJSONLWriter(t, &bytes.Buffer{}),
		},
		{
			name:   "sql",
			writer: mustNewSQLInsertWriter(t, &bytes.Buffer{}, "users"),
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
	w := mustNewDelimitedWriter(t, &out, Comma, WithMetadata(metadataWithColumnNames("name", "age")))

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
	w := mustNewDelimitedWriter(t, &out, Comma)

	err := w.WriteHeader()
	if !errors.Is(err, ErrMissingColumnNames) {
		t.Fatalf("WriteHeader() error = %v, want ErrMissingColumnNames", err)
	}
}

func TestDelimitedWriterWriteHeaderAfterData(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := mustNewDelimitedWriter(t, &out, Comma, WithMetadata(metadataWithColumnNames("name", "age")))
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
	w := mustNewDelimitedWriter(t, &out, Comma)

	err := w.WriteGCVs([]spanner.GenericColumnValue{gcvctor.StringValue("Alice")})
	if !errors.Is(err, ErrMissingColumnNames) {
		t.Fatalf("WriteGCVs() error = %v, want ErrMissingColumnNames", err)
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
				_, err := NewDelimitedWriter(nil, Comma, WithMetadata(metadataWithColumnNames("name")))
				return err
			},
		},
		{
			name: "jsonl",
			run: func() error {
				_, err := NewJSONLWriter(nil, WithMetadata(metadataWithColumnNames("name")))
				return err
			},
		},
		{
			name: "sql",
			run: func() error {
				_, err := NewSQLInsertWriter(nil, "users", WithMetadata(metadataWithColumnNames("name")))
				return err
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
				return mustNewDelimitedWriter(t, &bytes.Buffer{}, Comma).WriteRow(nil)
			},
		},
		{
			name: "jsonl",
			run: func() error {
				return mustNewJSONLWriter(t, &bytes.Buffer{}).WriteRow(nil)
			},
		},
		{
			name: "sql",
			run: func() error {
				return mustNewSQLInsertWriter(t, &bytes.Buffer{}, "users").WriteRow(nil)
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
	w := mustNewDelimitedWriter(t, &out, Comma)

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
	w := mustNewJSONLWriter(t, &out)
	row, err := spanner.NewRow([]string{"id", ""}, []any{int64(42), "hello"})
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
	w := mustNewJSONLWriter(t,
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
	w := mustNewJSONLWriter(t, &out)

	row, err := spanner.NewRow([]string{"id", "name"}, []any{int64(42), "hello"})
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
	w := mustNewJSONLWriter(t, &out)

	row, err := spanner.NewRow([]string{"", ""}, []any{int64(42), "hello"})
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
	w := mustNewJSONLWriter(t, &out, WithMetadata(metadataWithColumnNames("name", "age")))
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
			w := mustNewSQLInsertWriter(t, &out, tt.table)

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

func TestSQLInsertWriterSQLDialect(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		dialect     databasepb.DatabaseDialect
		table       string
		columnNames []string
		values      []spanner.GenericColumnValue
		want        string
	}{
		{
			name:        "PostgreSQL identifier escaping",
			dialect:     databasepb.DatabaseDialect_POSTGRESQL,
			table:       `user"table`,
			columnNames: []string{"id", `na"me`},
			values: []spanner.GenericColumnValue{
				gcvctor.Int64Value(42),
				gcvctor.Int64Value(7),
			},
			want: `INSERT INTO "user""table" ("id", "na""me") VALUES (42, 7);` + "\n",
		},
		{
			name:        "PostgreSQL qualified table name escaping",
			dialect:     databasepb.DatabaseDialect_POSTGRESQL,
			table:       `my"db.users`,
			columnNames: []string{"id"},
			values: []spanner.GenericColumnValue{
				gcvctor.Int64Value(42),
			},
			want: `INSERT INTO "my""db"."users" ("id") VALUES (42);` + "\n",
		},
		{
			name:        "default GoogleSQL unchanged",
			dialect:     databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED,
			table:       "users",
			columnNames: []string{"id"},
			values: []spanner.GenericColumnValue{
				gcvctor.Int64Value(42),
			},
			want: "INSERT INTO `users` (`id`) VALUES (42);\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var out bytes.Buffer
			w := mustNewSQLInsertWriter(t, &out, tt.table, WithSQLDialect(tt.dialect))

			if err := w.WriteValues(tt.columnNames, tt.values); err != nil {
				t.Fatalf("WriteValues() error = %v", err)
			}

			if diff := cmp.Diff(tt.want, out.String()); diff != "" {
				t.Fatalf("SQL output mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestSQLInsertWriterBatchSize(t *testing.T) {
	t.Parallel()

	columnNames := []string{"id", "name"}
	row := func(id int64, name string) []spanner.GenericColumnValue {
		return []spanner.GenericColumnValue{
			gcvctor.Int64Value(id),
			gcvctor.StringValue(name),
		}
	}

	t.Run("default one row per statement", func(t *testing.T) {
		t.Parallel()

		var out bytes.Buffer
		w := mustNewSQLInsertWriter(t, &out, "users")
		for _, values := range [][]spanner.GenericColumnValue{row(1, "a"), row(2, "b")} {
			if err := w.WriteValues(columnNames, values); err != nil {
				t.Fatalf("WriteValues() error = %v", err)
			}
		}
		want := "" +
			"INSERT INTO `users` (`id`, `name`) VALUES (1, \"a\");\n" +
			"INSERT INTO `users` (`id`, `name`) VALUES (2, \"b\");\n"
		if diff := cmp.Diff(want, out.String()); diff != "" {
			t.Fatalf("SQL output mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("batch size two", func(t *testing.T) {
		t.Parallel()

		var out bytes.Buffer
		w := mustNewSQLInsertWriter(t, &out, "users", WithSQLBatchSize(2))
		for _, values := range [][]spanner.GenericColumnValue{row(1, "a"), row(2, "b"), row(3, "c")} {
			if err := w.WriteValues(columnNames, values); err != nil {
				t.Fatalf("WriteValues() error = %v", err)
			}
		}
		// Intentionally no Flush(): the third row leaves a partial batch without a trailing semicolon.
		want := "" +
			"INSERT INTO `users` (`id`, `name`) VALUES\n" +
			"  (1, \"a\"),\n" +
			"  (2, \"b\");\n" +
			"INSERT INTO `users` (`id`, `name`) VALUES\n" +
			"  (3, \"c\")"
		if diff := cmp.Diff(want, out.String()); diff != "" {
			t.Fatalf("SQL output mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("flush remainder", func(t *testing.T) {
		t.Parallel()

		var out bytes.Buffer
		w := mustNewSQLInsertWriter(t, &out, "users", WithSQLBatchSize(2))
		for _, values := range [][]spanner.GenericColumnValue{row(1, "a"), row(2, "b"), row(3, "c")} {
			if err := w.WriteValues(columnNames, values); err != nil {
				t.Fatalf("WriteValues() error = %v", err)
			}
		}
		if err := w.Flush(); err != nil {
			t.Fatalf("Flush() error = %v", err)
		}
		want := "" +
			"INSERT INTO `users` (`id`, `name`) VALUES\n" +
			"  (1, \"a\"),\n" +
			"  (2, \"b\");\n" +
			"INSERT INTO `users` (`id`, `name`) VALUES\n" +
			"  (3, \"c\");\n"
		if diff := cmp.Diff(want, out.String()); diff != "" {
			t.Fatalf("SQL output mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("batch size zero same as one", func(t *testing.T) {
		t.Parallel()

		var out bytes.Buffer
		w := mustNewSQLInsertWriter(t, &out, "users", WithSQLBatchSize(0))
		if err := w.WriteValues(columnNames, row(1, "a")); err != nil {
			t.Fatalf("WriteValues() error = %v", err)
		}
		want := "INSERT INTO `users` (`id`, `name`) VALUES (1, \"a\");\n"
		if diff := cmp.Diff(want, out.String()); diff != "" {
			t.Fatalf("SQL output mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("table change after batch boundary", func(t *testing.T) {
		t.Parallel()

		var out bytes.Buffer
		w := mustNewSQLInsertWriter(t, &out, "db.users", WithSQLBatchSize(2))
		for _, values := range [][]spanner.GenericColumnValue{row(1, "a"), row(2, "b")} {
			if err := w.WriteValues(columnNames, values); err != nil {
				t.Fatalf("WriteValues() error = %v", err)
			}
		}
		w.table = "archive.users"
		if err := w.WriteValues(columnNames, row(3, "c")); err != nil {
			t.Fatalf("WriteValues() after table change error = %v", err)
		}
		want := "" +
			"INSERT INTO `db`.`users` (`id`, `name`) VALUES\n" +
			"  (1, \"a\"),\n" +
			"  (2, \"b\");\n" +
			"INSERT INTO `archive`.`users` (`id`, `name`) VALUES\n" +
			"  (3, \"c\")"
		if diff := cmp.Diff(want, out.String()); diff != "" {
			t.Fatalf("SQL output mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("table change mid-batch rejects", func(t *testing.T) {
		t.Parallel()

		var out bytes.Buffer
		w := mustNewSQLInsertWriter(t, &out, "db.users", WithSQLBatchSize(2))
		if err := w.WriteValues(columnNames, row(1, "a")); err != nil {
			t.Fatalf("WriteValues() error = %v", err)
		}
		// Mid-batch table mutation must not silently append rows to the previous table.
		w.table = "archive.users"
		err := w.WriteValues(columnNames, row(2, "b"))
		if !errors.Is(err, ErrTableNameChangedMidBatch) {
			t.Fatalf("WriteValues() after table change error = %v, want ErrTableNameChangedMidBatch", err)
		}
		want := "" +
			"INSERT INTO `db`.`users` (`id`, `name`) VALUES\n" +
			"  (1, \"a\")"
		if diff := cmp.Diff(want, out.String()); diff != "" {
			t.Fatalf("SQL output mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("PostgreSQL dialect batched", func(t *testing.T) {
		t.Parallel()

		var out bytes.Buffer
		w := mustNewSQLInsertWriter(t, &out, "users", WithSQLBatchSize(2), WithSQLDialect(databasepb.DatabaseDialect_POSTGRESQL))
		for _, id := range []int64{1, 2} {
			if err := w.WriteValues([]string{"id"}, []spanner.GenericColumnValue{gcvctor.Int64Value(id)}); err != nil {
				t.Fatalf("WriteValues() error = %v", err)
			}
		}
		want := "INSERT INTO \"users\" (\"id\") VALUES\n  (1),\n  (2);\n"
		if diff := cmp.Diff(want, out.String()); diff != "" {
			t.Fatalf("SQL output mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("PostgreSQL identifiers with formatter value literals", func(t *testing.T) {
		t.Parallel()

		var out bytes.Buffer
		w := mustNewSQLInsertWriter(t, &out, "users",
			WithSQLDialect(databasepb.DatabaseDialect_POSTGRESQL),
			WithFormatter(spanvalue.LiteralFormatConfig()),
		)
		if err := w.WriteValues([]string{"name"}, []spanner.GenericColumnValue{gcvctor.StringValue("Alice")}); err != nil {
			t.Fatalf("WriteValues() error = %v", err)
		}
		want := `INSERT INTO "users" ("name") VALUES ("Alice");` + "\n"
		if diff := cmp.Diff(want, out.String()); diff != "" {
			t.Fatalf("SQL output mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("insert or ignore batched", func(t *testing.T) {
		t.Parallel()

		var out bytes.Buffer
		w := mustNewSQLInsertWriter(t, &out, "users", WithSQLBatchSize(2), WithSQLInsertKind(SQLInsertOrIgnore))
		for _, values := range [][]spanner.GenericColumnValue{row(1, "a"), row(2, "b")} {
			if err := w.WriteValues(columnNames, values); err != nil {
				t.Fatalf("WriteValues() error = %v", err)
			}
		}
		want := "INSERT OR IGNORE INTO `users` (`id`, `name`) VALUES\n  (1, \"a\"),\n  (2, \"b\");\n"
		if diff := cmp.Diff(want, out.String()); diff != "" {
			t.Fatalf("SQL output mismatch (-want +got):\n%s", diff)
		}
	})
}

func TestSQLInsertWriterPostgreSQLInsertOrKindsRejected(t *testing.T) {
	t.Parallel()

	kinds := []SQLInsertKind{SQLInsertOrIgnore, SQLInsertOrUpdate}
	for _, kind := range kinds {
		t.Run(kind.String(), func(t *testing.T) {
			t.Parallel()

			var out bytes.Buffer
			_, err := NewSQLInsertWriter(&out, "users",
				WithSQLDialect(databasepb.DatabaseDialect_POSTGRESQL),
				WithSQLInsertKind(kind),
			)
			if !errors.Is(err, ErrInvalidSQLInsertKindForDialect) {
				t.Fatalf("NewSQLInsertWriter() error = %v, want ErrInvalidSQLInsertKindForDialect", err)
			}
			if out.Len() != 0 {
				t.Fatalf("NewSQLInsertWriter() wrote %q, want no output", out.String())
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
			w := mustNewSQLInsertWriter(t, &out, "users", WithSQLInsertKind(tt.kind))
			if err := w.WriteValues(columnNames, values); err != nil {
				t.Fatalf("WriteValues() error = %v", err)
			}
			if diff := cmp.Diff(tt.want, out.String()); diff != "" {
				t.Fatalf("SQL output mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestSQLInsertWriterWithSingleQuotedLiterals(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	cfg := spanvalue.LiteralFormatConfigWithSingleQuotedLiterals()
	w := mustNewSQLInsertWriter(t,
		&out,
		"users",
		WithMetadata(metadataWithColumnNames("event_date", "note")),
		WithFormatter(cfg),
	)
	if err := w.WriteGCVs([]spanner.GenericColumnValue{
		gcvctor.DateValue(civil.Date{Year: 2014, Month: 9, Day: 27}),
		gcvctor.StringValue("it's fine"),
	}); err != nil {
		t.Fatalf("WriteGCVs() error = %v", err)
	}

	want := "INSERT INTO `users` (`event_date`, `note`) VALUES (DATE '2014-09-27', 'it\\'s fine');\n"
	if diff := cmp.Diff(want, out.String()); diff != "" {
		t.Fatalf("SQL output mismatch (-want +got):\n%s", diff)
	}
}

func TestSQLInsertWriterWithOptions(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	cfg := spanvalue.LiteralFormatConfig()
	w := mustNewSQLInsertWriter(t,
		&out,
		"users",
		WithMetadata(metadataWithColumnNames("id", "name")),
		WithFormatter(cfg),
	)
	if w.TableName() != "users" {
		t.Fatalf("TableName() = %q, want users", w.TableName())
	}
	if w.FormatConfig() != cfg {
		t.Fatalf("FormatConfig() = %p, want %p", w.FormatConfig(), cfg)
	}

	var nilFormatterOut bytes.Buffer
	nilFormatterW := mustNewSQLInsertWriter(t,
		&nilFormatterOut,
		"users",
		WithMetadata(metadataWithColumnNames("id")),
		WithFormatter(nil),
	)
	gotFormatter := nilFormatterW.FormatConfig()
	if gotFormatter == nil {
		t.Fatal("FormatConfig() with nil formatter = nil, want effective literal formatter")
	}
	if nilFormatterW.FormatConfig() != gotFormatter {
		t.Fatal("FormatConfig() should return the same cached formatter on repeat calls")
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
}

func TestSQLInsertWriterWriteRow(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := mustNewSQLInsertWriter(t, &out, "db.user`table")

	row, err := spanner.NewRow([]string{"na`me", "payload"}, []any{"Alice", "semi;\nline"})
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

func TestNewSQLInsertWriterEmptyColumnNameAtConstruction(t *testing.T) {
	t.Parallel()

	_, err := NewSQLInsertWriter(&bytes.Buffer{}, "users", WithColumnNames([]string{""}))
	if !errors.Is(err, ErrEmptyColumnName) {
		t.Fatalf("NewSQLInsertWriter() error = %v, want ErrEmptyColumnName", err)
	}
}

func TestSQLInsertWriterWriteValuesEmptyColumnName(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := mustNewSQLInsertWriter(t, &out, "users")

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
	w := mustNewSQLInsertWriter(t, &out, "users")

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
	w := mustNewSQLInsertWriter(t, &out, "db.users")

	err := w.WriteValues(
		[]string{"id"},
		[]spanner.GenericColumnValue{gcvctor.Int64Value(1)},
	)
	if err != nil {
		t.Fatalf("first WriteValues() error = %v", err)
	}

	w.table = "archive.users"
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
	w := mustNewSQLInsertWriter(t, &out, "")

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
	w := mustNewSQLInsertWriter(t, &out, "db..users")

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
	w := mustNewSQLInsertWriter(t, &out, "users")

	err := w.WriteGCVs([]spanner.GenericColumnValue{gcvctor.Int64Value(42)})
	if !errors.Is(err, ErrMissingColumnNames) {
		t.Fatalf("WriteGCVs() error = %v, want ErrMissingColumnNames", err)
	}
}

func TestSQLInsertWriterWriteGCVsEmptyTableName(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := mustNewSQLInsertWriter(t, &out, "", WithMetadata(metadataWithColumnNames("id")))

	err := w.WriteGCVs([]spanner.GenericColumnValue{gcvctor.Int64Value(42)})
	if !errors.Is(err, ErrEmptyTableName) {
		t.Fatalf("WriteGCVs() error = %v, want ErrEmptyTableName", err)
	}
}

func TestRowDataAndOneRowFormatHelpers(t *testing.T) {
	t.Parallel()

	row, err := spanner.NewRow([]string{"id", "note"}, []any{int64(42), "comma, ok"})
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
				row, err := spanner.NewRow(columnNames, []any{"Alice"})
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

func TestDelimitedWriterFlushWritesHeaderWithNoRows(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := mustNewDelimitedWriter(t, &out, ',', WithColumnNames([]string{"id", "name"}), WithHeader(true))
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	want := "id,name\n"
	if diff := cmp.Diff(want, out.String()); diff != "" {
		t.Fatalf("output mismatch (-want +got):\n%s", diff)
	}
}

func TestDelimitedWriterFlushDoesNotDuplicateHeader(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := mustNewDelimitedWriter(t, &out, ',', WithColumnNames([]string{"id", "name"}), WithHeader(true))
	if err := w.WriteGCVs([]spanner.GenericColumnValue{
		gcvctor.Int64Value(1),
		gcvctor.StringValue("a"),
	}); err != nil {
		t.Fatalf("WriteGCVs() error = %v", err)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	want := "id,name\n1,a\n"
	if diff := cmp.Diff(want, out.String()); diff != "" {
		t.Fatalf("output mismatch (-want +got):\n%s", diff)
	}
}

func TestDelimitedWriterFlushWithoutColumnNames(t *testing.T) {
	t.Parallel()

	err := mustNewDelimitedWriter(t, &bytes.Buffer{}, ',', WithHeader(true)).Flush()
	if !errors.Is(err, ErrMissingColumnNames) {
		t.Fatalf("Flush() error = %v, want ErrMissingColumnNames", err)
	}
}

func TestDelimitedWriterPrepareRowTypeNilRegistersEmptySchema(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := mustNewDelimitedWriter(t, &out, ',', WithHeader(true))
	if err := w.PrepareRowType(nil); err != nil {
		t.Fatalf("PrepareRowType(nil) error = %v", err)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	if out.Len() != 0 {
		t.Fatalf("output = %q, want empty", out.String())
	}
}

func TestDelimitedWriterPrepareRowTypeEmptyAfterNonEmptyErrors(t *testing.T) {
	t.Parallel()

	w := mustNewDelimitedWriter(t, &bytes.Buffer{}, ',')
	if err := w.PrepareRowType(rowTypeWithColumnNames("id")); err != nil {
		t.Fatalf("PrepareRowType() error = %v", err)
	}
	err := w.PrepareRowType(emptyRowType())
	if !errors.Is(err, ErrColumnNamesMismatch) {
		t.Fatalf("PrepareRowType(empty) error = %v, want ErrColumnNamesMismatch", err)
	}
	if len(w.schema.names) != 1 || w.schema.names[0] != "id" {
		t.Fatalf("schema.names = %v, want [id]", w.schema.names)
	}
}

func TestDelimitedWriterPrepareColumnNamesEmptyErrors(t *testing.T) {
	t.Parallel()

	err := mustNewDelimitedWriter(t, &bytes.Buffer{}, ',').PrepareColumnNames(nil)
	if !errors.Is(err, ErrMissingColumnNames) {
		t.Fatalf("PrepareColumnNames(nil) error = %v, want ErrMissingColumnNames", err)
	}
}

func TestDelimitedWriterWithColumnNamesEmptyErrors(t *testing.T) {
	t.Parallel()

	_, err := NewDelimitedWriter(&bytes.Buffer{}, ',', WithColumnNames(nil), WithHeader(true))
	if !errors.Is(err, ErrMissingColumnNames) {
		t.Fatalf("NewDelimitedWriter error = %v, want ErrMissingColumnNames", err)
	}
}

func TestDelimitedWriterPrepareEmptyRowTypeFlushWritesNothing(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := mustNewDelimitedWriter(t, &out, ',', WithHeader(true))
	if err := w.PrepareRowType(emptyRowType()); err != nil {
		t.Fatalf("PrepareRowType() error = %v", err)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	if out.Len() != 0 {
		t.Fatalf("output = %q, want empty", out.String())
	}
}

func TestDelimitedWriterPrepareEmptyRowTypeWriteGCVsNoOp(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := mustNewDelimitedWriter(t, &out, ',', WithHeader(true))
	if err := w.PrepareRowType(emptyRowType()); err != nil {
		t.Fatalf("PrepareRowType() error = %v", err)
	}
	if err := w.WriteGCVs(nil); err != nil {
		t.Fatalf("WriteGCVs() error = %v", err)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	if out.Len() != 0 {
		t.Fatalf("output = %q, want empty", out.String())
	}
}

func TestJSONLWriterPrepareEmptyRowTypeFlushWritesNothing(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := mustNewJSONLWriter(t, &out)
	if err := w.PrepareRowType(emptyRowType()); err != nil {
		t.Fatalf("PrepareRowType() error = %v", err)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	if out.Len() != 0 {
		t.Fatalf("output = %q, want empty", out.String())
	}
}

func TestSQLInsertWriterPrepareEmptyRowTypeFlushNoOp(t *testing.T) {
	t.Parallel()

	w := mustNewSQLInsertWriter(t, &bytes.Buffer{}, "users")
	if err := w.PrepareRowType(emptyRowType()); err != nil {
		t.Fatalf("PrepareRowType() error = %v", err)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	err := w.WriteGCVs(nil)
	if !errors.Is(err, ErrMissingColumnNames) {
		t.Fatalf("WriteGCVs() error = %v, want ErrMissingColumnNames", err)
	}
}

func TestWriterRejectsNonEmptyColumnNamesAfterRegisteredZeroColumnSchema(t *testing.T) {
	t.Parallel()

	columnNames := []string{"id"}
	values := []spanner.GenericColumnValue{gcvctor.Int64Value(1)}

	tests := []struct {
		name   string
		setup  func(t *testing.T) Writer
		invoke func(t *testing.T, w Writer) error
	}{
		{
			name: "delimited PrepareColumnNames",
			setup: func(t *testing.T) Writer {
				t.Helper()
				w := mustNewDelimitedWriter(t, &bytes.Buffer{}, ',')
				if err := w.PrepareRowType(nil); err != nil {
					t.Fatalf("PrepareRowType(nil) error = %v", err)
				}
				return w
			},
			invoke: func(t *testing.T, w Writer) error {
				t.Helper()
				return w.(*DelimitedWriter).PrepareColumnNames(columnNames)
			},
		},
		{
			name: "delimited WriteValues",
			setup: func(t *testing.T) Writer {
				t.Helper()
				w := mustNewDelimitedWriter(t, &bytes.Buffer{}, ',')
				if err := w.PrepareRowType(emptyRowType()); err != nil {
					t.Fatalf("PrepareRowType() error = %v", err)
				}
				return w
			},
			invoke: func(t *testing.T, w Writer) error {
				t.Helper()
				return w.(*DelimitedWriter).WriteValues(columnNames, values)
			},
		},
		{
			name: "jsonl PrepareColumnNames",
			setup: func(t *testing.T) Writer {
				t.Helper()
				w := mustNewJSONLWriter(t, &bytes.Buffer{})
				if err := w.PrepareRowType(nil); err != nil {
					t.Fatalf("PrepareRowType(nil) error = %v", err)
				}
				return w
			},
			invoke: func(t *testing.T, w Writer) error {
				t.Helper()
				return w.(*JSONLWriter).PrepareColumnNames(columnNames)
			},
		},
		{
			name: "jsonl WriteValues",
			setup: func(t *testing.T) Writer {
				t.Helper()
				w := mustNewJSONLWriter(t, &bytes.Buffer{})
				if err := w.PrepareRowType(emptyRowType()); err != nil {
					t.Fatalf("PrepareRowType() error = %v", err)
				}
				return w
			},
			invoke: func(t *testing.T, w Writer) error {
				t.Helper()
				return w.(*JSONLWriter).WriteValues(columnNames, values)
			},
		},
		{
			name: "sql insert PrepareColumnNames",
			setup: func(t *testing.T) Writer {
				t.Helper()
				w := mustNewSQLInsertWriter(t, &bytes.Buffer{}, "users")
				if err := w.PrepareRowType(nil); err != nil {
					t.Fatalf("PrepareRowType(nil) error = %v", err)
				}
				return w
			},
			invoke: func(t *testing.T, w Writer) error {
				t.Helper()
				return w.(*SQLInsertWriter).PrepareColumnNames(columnNames)
			},
		},
		{
			name: "sql insert WriteValues",
			setup: func(t *testing.T) Writer {
				t.Helper()
				w := mustNewSQLInsertWriter(t, &bytes.Buffer{}, "users")
				if err := w.PrepareRowType(emptyRowType()); err != nil {
					t.Fatalf("PrepareRowType() error = %v", err)
				}
				return w
			},
			invoke: func(t *testing.T, w Writer) error {
				t.Helper()
				return w.(*SQLInsertWriter).WriteValues(columnNames, values)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			w := tt.setup(t)
			err := tt.invoke(t, w)
			if !errors.Is(err, ErrColumnNamesMismatch) {
				t.Fatalf("error = %v, want ErrColumnNamesMismatch", err)
			}
		})
	}
}

func TestWithColumnNamesHeaderlessDelimited(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := mustNewDelimitedWriter(t, &out, ',', WithColumnNames([]string{"id", "name"}), WithHeader(false))
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

func TestDelimitedWriterWriteGCVsEnumProto(t *testing.T) {
	t.Parallel()

	const (
		enumFQN  = "my.proto.Status"
		protoFQN = "examples.spanner.music.SingerInfo"
	)
	var out bytes.Buffer
	w := mustNewDelimitedWriter(t, &out, ',', WithColumnNames([]string{"status", "payload"}))
	if err := w.WriteGCVs([]spanner.GenericColumnValue{
		gcvctor.EnumValue(enumFQN, 1),
		gcvctor.ProtoValue(protoFQN, []byte("abcd")),
	}); err != nil {
		t.Fatalf("WriteGCVs() error = %v", err)
	}
	flushDelimitedWriter(t, w)

	// ENUM is the stored INT64 string; PROTO is base64 on the wire but decoded for SimpleFormatConfig CSV.
	want := "status,payload\n1,abcd\n"
	if diff := cmp.Diff(want, out.String()); diff != "" {
		t.Fatalf("output mismatch (-want +got):\n%s", diff)
	}
}

func TestWithColumnNamesWriteGCVs(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := mustNewDelimitedWriter(t, &out, '\t', WithColumnNames([]string{"id", "name"}))
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

func TestWithRowTypeWriteStructValues(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := mustNewJSONLWriter(t, &out, WithRowType(rowTypeWithColumnNames("id", "name")))
	if err := w.WriteStructValues([]*structpb.Value{
		structpb.NewStringValue("42"),
		structpb.NewStringValue("Alice"),
	}); err != nil {
		t.Fatalf("WriteStructValues() error = %v", err)
	}

	want := "{\"id\":42,\"name\":\"Alice\"}\n"
	if diff := cmp.Diff(want, out.String()); diff != "" {
		t.Fatalf("output mismatch (-want +got):\n%s", diff)
	}
}

func TestWriteStructValuesMissingFieldTypes(t *testing.T) {
	t.Parallel()

	w := mustNewDelimitedWriter(t, &bytes.Buffer{}, ',')
	err := w.WriteStructValues([]*structpb.Value{structpb.NewStringValue("1")})
	if !errors.Is(err, ErrMissingFieldTypes) {
		t.Fatalf("WriteStructValues() error = %v, want ErrMissingFieldTypes", err)
	}
}

func TestWriteStructValuesNilFieldType(t *testing.T) {
	t.Parallel()

	rowType := &sppb.StructType{
		Fields: []*sppb.StructType_Field{
			{Name: "id", Type: nil},
		},
	}
	w := mustNewSQLInsertWriter(t, &bytes.Buffer{}, "users", WithRowType(rowType))
	err := w.WriteStructValues([]*structpb.Value{structpb.NewStringValue("1")})
	if !errors.Is(err, spanvalue.ErrNilStructField) {
		t.Fatalf("WriteStructValues() error = %v, want ErrNilStructField", err)
	}
}

func TestDelimitedWriterPrepareColumnNamesAfterConstruction(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := mustNewDelimitedWriter(t, &out, ',')
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

func TestSQLInsertWriterPrepareColumnNamesRecoversAfterQuoteError(t *testing.T) {
	t.Parallel()

	w := mustNewSQLInsertWriter(t, &bytes.Buffer{}, "users")
	err := w.PrepareColumnNames([]string{""})
	if !errors.Is(err, ErrEmptyColumnName) {
		t.Fatalf("PrepareColumnNames() error = %v, want ErrEmptyColumnName", err)
	}
	if err := w.PrepareColumnNames([]string{"id"}); err != nil {
		t.Fatalf("PrepareColumnNames() retry error = %v", err)
	}
	if w.quotedColumnNames == "" {
		t.Fatal("quotedColumnNames not cached after successful PrepareColumnNames")
	}
}

func TestSQLInsertWriterPrepareRowTypeCachesQuotedColumns(t *testing.T) {
	t.Parallel()

	w := mustNewSQLInsertWriter(t, &bytes.Buffer{}, "users")
	if err := w.PrepareRowType(rowTypeWithColumnNames("id", "name")); err != nil {
		t.Fatalf("PrepareRowType() error = %v", err)
	}
	if w.quotedColumnNames == "" {
		t.Fatal("quotedColumnNames not cached after PrepareRowType")
	}
}

func TestPrepareRowTypeAfterConstruction(t *testing.T) {
	t.Parallel()

	rowType := rowTypeWithColumnNames("id", "name")
	var delimited, jsonl bytes.Buffer
	dw := mustNewDelimitedWriter(t, &delimited, ',')
	jw := mustNewJSONLWriter(t, &jsonl)
	if err := dw.PrepareRowType(rowType); err != nil {
		t.Fatalf("DelimitedWriter.PrepareRowType() error = %v", err)
	}
	if err := jw.PrepareRowType(rowType); err != nil {
		t.Fatalf("JSONLWriter.PrepareRowType() error = %v", err)
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

func TestWithRowTypeConsistentAcrossWriters(t *testing.T) {
	t.Parallel()

	rowType := rowTypeWithColumnNames("id", "name")
	var delimited, jsonl bytes.Buffer
	dw := mustNewDelimitedWriter(t, &delimited, ',', WithRowType(rowType))
	jw := mustNewJSONLWriter(t, &jsonl, WithRowType(rowType))
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
