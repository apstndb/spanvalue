package writer

import (
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"unicode/utf8"

	"cloud.google.com/go/spanner"
	databasepb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/apstndb/spanvalue"
	"github.com/apstndb/spanvalue/internal"
)

const (
	// Comma is the standard CSV field delimiter. Pass Comma to
	// NewDelimitedWriter for CSV output.
	Comma rune = ','
)

var (
	// ErrEmptyTableName reports that the SQL INSERT writer table name is empty.
	ErrEmptyTableName = errors.New("empty table name")
	// ErrEmptyColumnName reports that a SQL writer received an empty column name.
	ErrEmptyColumnName = errors.New("empty column name")
	// ErrNilOutputWriter reports that a writer was constructed without an output.
	ErrNilOutputWriter = errors.New("nil output writer")
	// ErrNilRow reports that WriteRow was called with a nil row.
	ErrNilRow = spanvalue.ErrNilRow
	// ErrMissingColumnNames reports that an operation requires a registered column schema
	// when none was registered yet, or column names/types are insufficient for the write
	// (for example values without names). It is not returned for a registered zero-column
	// schema (see package doc "Registered schema vs missing schema"). [PrepareColumnNames]
	// and [WithColumnNames] with an empty name list return this error; use [PrepareRowType]
	// or [WithRowType] for zero-column result sets.
	ErrMissingColumnNames = errors.New("missing column names")
	// ErrColumnNamesMismatch reports that provided column names differ from initialized schema.
	ErrColumnNamesMismatch = errors.New("column names mismatch")
	// ErrHeaderAfterData reports that DelimitedWriter.WriteHeader was called after data rows were emitted.
	ErrHeaderAfterData = errors.New("header after data")
	// ErrInvalidDelimiter reports that DelimitedWriter received an invalid delimiter.
	ErrInvalidDelimiter = errors.New("invalid delimiter")
	// ErrMissingFieldTypes reports that WriteStructValues requires registered field types.
	ErrMissingFieldTypes = errors.New("missing field types schema")
	// ErrMismatchedStructValueCount reports that WriteStructValues value count does not match the schema.
	ErrMismatchedStructValueCount = errors.New("mismatched struct value count")
	// ErrInvalidSQLInsertKindForDialect reports that [WithSQLInsertKind] selected INSERT OR IGNORE
	// or INSERT OR UPDATE with a PostgreSQL dialect. Those prefixes are GoogleSQL-only; use plain
	// [SQLInsert] with [WithSQLDialect](databasepb.DatabaseDialect_POSTGRESQL) instead.
	ErrInvalidSQLInsertKindForDialect = errors.New("INSERT OR IGNORE/UPDATE not supported for PostgreSQL dialect")
	// ErrTableNameChangedMidBatch reports that the SQL INSERT table name was mutated while
	// a multi-row INSERT batch was open.
	ErrTableNameChangedMidBatch = errors.New("table name changed mid-batch")
)

// Writer writes Spanner rows to an output stream.
//
// WriteRow supplies column names and values on each call; see package doc
// "Column names and field types". Writer intentionally models row streaming only. Some concrete writers also
// implement [Flusher]; callers that own the full write lifecycle must call
// Flush after the final row when it is available. Factories that may return a
// buffered writer should return a concrete type or [FlushWriter], not Writer
// alone.
type Writer interface {
	WriteRow(row *spanner.Row) error
}

// Flusher finalizes any buffered output. Flush does not close the underlying
// io.Writer. DelimitedWriter uses Flush to forward buffered CSV-style data;
// JSONLWriter implements Flush as a no-op. [SQLInsertWriter.Flush] closes a
// partial multi-row INSERT when [WithSQLBatchSize] is greater than 1.
type Flusher interface {
	Flush() error
}

// FlushWriter streams Spanner rows and finalizes any buffered output.
type FlushWriter interface {
	Writer
	Flusher
}

// Option configures any writer type created by a writer constructor.
type Option interface {
	DelimitedOption
	JSONLOption
	SQLInsertOption
}

// NameOption configures field-name handling for delimited and JSONL writers.
type NameOption interface {
	DelimitedOption
	JSONLOption
}

// DelimitedOption configures a DelimitedWriter created by [NewDelimitedWriter] or [NewCSVWriter].
type DelimitedOption interface {
	applyDelimitedOption(*DelimitedWriter) error
}

// JSONLOption configures a JSONLWriter created by [NewJSONLWriter].
type JSONLOption interface {
	applyJSONLOption(*JSONLWriter) error
}

// SQLInsertKind selects the INSERT statement prefix written by [SQLInsertWriter].
//
// Variants follow Spanner GoogleSQL DML: INSERT OR IGNORE skips rows whose primary
// key already exists; INSERT OR UPDATE inserts or updates by primary key. They
// cannot be combined with ON CONFLICT in the same statement, and INSERT is not
// supported in Partitioned DML. See https://cloud.google.com/spanner/docs/reference/standard-sql/dml-syntax .
//
// [SQLInsertOrIgnore] and [SQLInsertOrUpdate] are invalid with
// [WithSQLDialect](databasepb.DatabaseDialect_POSTGRESQL); [NewSQLInsertWriter] rejects
// that combination via [ErrInvalidSQLInsertKindForDialect] on the first write.
type SQLInsertKind int

const (
	// SQLInsert writes plain INSERT INTO statements.
	SQLInsert SQLInsertKind = iota
	// SQLInsertOrIgnore writes INSERT OR IGNORE INTO statements.
	SQLInsertOrIgnore
	// SQLInsertOrUpdate writes INSERT OR UPDATE INTO statements.
	SQLInsertOrUpdate
)

func (k SQLInsertKind) String() string {
	switch k {
	case SQLInsert:
		return "INSERT"
	case SQLInsertOrIgnore:
		return "INSERT OR IGNORE"
	case SQLInsertOrUpdate:
		return "INSERT OR UPDATE"
	default:
		return "INSERT"
	}
}

// WithSQLInsertKind sets the INSERT statement variant for a [SQLInsertWriter].
func WithSQLInsertKind(kind SQLInsertKind) SQLInsertOption {
	return sqlInsertKindOption{kind: kind}
}

type sqlInsertKindOption struct {
	kind SQLInsertKind
}

func (o sqlInsertKindOption) applySQLInsertOption(w *SQLInsertWriter) error {
	w.insertKind = o.kind
	return nil
}

type sqlDialectOption struct {
	dialect databasepb.DatabaseDialect
}

// WithSQLDialect sets identifier quoting for table and column names in SQL INSERT
// output. It does not change INSERT statement prefixes ([WithSQLInsertKind]) or
// value literal formatting ([WithFormatter]). The default is GoogleSQL
// ([databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL]).
//
// PostgreSQL dialect does not support [SQLInsertOrIgnore] or [SQLInsertOrUpdate]
// prefixes; combining them returns [ErrInvalidSQLInsertKindForDialect] on write.
func WithSQLDialect(dialect databasepb.DatabaseDialect) SQLInsertOption {
	return sqlDialectOption{dialect: dialect}
}

func (o sqlDialectOption) applySQLInsertOption(w *SQLInsertWriter) error {
	w.sqlDialect = o.dialect
	return nil
}

// WithSQLBatchSize sets how many rows [SQLInsertWriter] combines into one INSERT
// statement. Values 0 or 1 keep the default of one row per statement. Values greater
// than 1 emit multi-row INSERT ... VALUES (...), (...); up to n rows per statement.
// Call [SQLInsertWriter.Flush] after the final row to close a partial batch (Flush is
// also safe when the last batch closed exactly on a size boundary).
//
// Batching applies the same [SQLInsertKind] prefix once per batched statement. Multi-row
// INSERT OR IGNORE and INSERT OR UPDATE follow Spanner GoogleSQL DML rules and require
// GoogleSQL dialect; PostgreSQL rejects those prefixes ([ErrInvalidSQLInsertKindForDialect]).
// Identifier quoting follows [WithSQLDialect]; value literals still use [WithFormatter].
func WithSQLBatchSize(n int) SQLInsertOption {
	return sqlBatchSizeOption{batchSize: n}
}

type sqlBatchSizeOption struct {
	batchSize int
}

func (o sqlBatchSizeOption) applySQLInsertOption(w *SQLInsertWriter) error {
	w.batchSize = o.batchSize
	return nil
}

// SQLInsertOption configures a SQLInsertWriter created by [NewSQLInsertWriter].
type SQLInsertOption interface {
	applySQLInsertOption(*SQLInsertWriter) error
}

type delimitedOptionFunc func(*DelimitedWriter) error

func (f delimitedOptionFunc) applyDelimitedOption(w *DelimitedWriter) error {
	return f(w)
}

func applyDelimitedOptions(w *DelimitedWriter, options ...DelimitedOption) error {
	for _, opt := range options {
		if opt == nil {
			continue
		}
		if err := opt.applyDelimitedOption(w); err != nil {
			return err
		}
	}
	return nil
}

func applyJSONLOptions(w *JSONLWriter, options ...JSONLOption) error {
	for _, opt := range options {
		if opt == nil {
			continue
		}
		if err := opt.applyJSONLOption(w); err != nil {
			return err
		}
	}
	return nil
}

func applySQLInsertOptions(w *SQLInsertWriter, options ...SQLInsertOption) error {
	for _, opt := range options {
		if opt == nil {
			continue
		}
		if err := opt.applySQLInsertOption(w); err != nil {
			return err
		}
	}
	return nil
}

type metadataOption struct {
	metadata *sppb.ResultSetMetadata
}

// WithMetadata registers names and types from metadata.GetRowType(); same as
// [WithRowType]. Other metadata fields are ignored.
//
// When printing table headers or other column labels outside the writer, use
// [spanvalue.ColumnNames] with the same [spanvalue.UnnamedFieldNamer] as
// [WithUnnamedFieldNamer] so export columns match displayed headers.
func WithMetadata(metadata *sppb.ResultSetMetadata) Option {
	return metadataOption{metadata: metadata}
}

func (o metadataOption) applyDelimitedOption(w *DelimitedWriter) error {
	w.setRowType(rowTypeFromMetadata(o.metadata))
	return nil
}

func (o metadataOption) applyJSONLOption(w *JSONLWriter) error {
	w.setRowType(rowTypeFromMetadata(o.metadata))
	return nil
}

func (o metadataOption) applySQLInsertOption(w *SQLInsertWriter) error {
	w.setRowType(rowTypeFromMetadata(o.metadata))
	return nil
}

type rowTypeOption struct {
	rowType *sppb.StructType
}

// WithRowType registers column names and field types at construction.
// Nil rowType registers an empty schema (see package doc).
func WithRowType(rowType *sppb.StructType) Option {
	return rowTypeOption{rowType: rowType}
}

func (o rowTypeOption) applyDelimitedOption(w *DelimitedWriter) error {
	w.setRowType(o.rowType)
	return nil
}

func (o rowTypeOption) applyJSONLOption(w *JSONLWriter) error {
	w.setRowType(o.rowType)
	return nil
}

func (o rowTypeOption) applySQLInsertOption(w *SQLInsertWriter) error {
	w.setRowType(o.rowType)
	return nil
}

type columnNamesOption struct {
	names []string
}

// WithColumnNames registers column names only and clears any registered field types.
// An empty names slice returns [ErrMissingColumnNames]; use [WithRowType] for a
// zero-column result set.
func WithColumnNames(names []string) Option {
	return columnNamesOption{names: slices.Clone(names)}
}

func (o columnNamesOption) applyDelimitedOption(w *DelimitedWriter) error {
	if len(o.names) == 0 {
		return ErrMissingColumnNames
	}
	w.setColumnNames(o.names)
	return nil
}

func (o columnNamesOption) applyJSONLOption(w *JSONLWriter) error {
	if len(o.names) == 0 {
		return ErrMissingColumnNames
	}
	w.setColumnNames(o.names)
	return nil
}

func (o columnNamesOption) applySQLInsertOption(w *SQLInsertWriter) error {
	if len(o.names) == 0 {
		return ErrMissingColumnNames
	}
	w.setColumnNames(o.names)
	return nil
}

type formatterOption struct {
	formatter *spanvalue.FormatConfig
}

// WithFormatter sets the FormatConfig used by a writer.
// A nil formatter selects the writer-type default:
// [DelimitedWriter] uses [spanvalue.SimpleFormatConfig],
// [JSONLWriter] uses [spanvalue.JSONFormatConfig],
// and [SQLInsertWriter] uses [spanvalue.LiteralFormatConfig].
func WithFormatter(formatter *spanvalue.FormatConfig) Option {
	return formatterOption{formatter: formatter}
}

func (o formatterOption) applyDelimitedOption(w *DelimitedWriter) error {
	if o.formatter != nil {
		w.formatter = o.formatter
	} else {
		w.formatter = spanvalue.SimpleFormatConfig()
	}
	return nil
}

func (o formatterOption) applyJSONLOption(w *JSONLWriter) error {
	if o.formatter != nil {
		w.formatter = o.formatter
	} else {
		w.formatter = spanvalue.JSONFormatConfig()
	}
	return nil
}

func (o formatterOption) applySQLInsertOption(w *SQLInsertWriter) error {
	if o.formatter != nil {
		w.formatter = o.formatter
	} else {
		w.formatter = spanvalue.LiteralFormatConfig()
	}
	return nil
}

type unnamedFieldNamerOption struct {
	namer spanvalue.UnnamedFieldNamer
}

// WithUnnamedFieldNamer sets the unnamed-field naming policy for delimited and JSONL writers.
// The same namer must be passed to [spanvalue.ColumnNames] when resolving display headers
// outside the writer (for example CLI table output alongside CSV export).
func WithUnnamedFieldNamer(namer spanvalue.UnnamedFieldNamer) NameOption {
	return unnamedFieldNamerOption{namer: namer}
}

func (o unnamedFieldNamerOption) applyDelimitedOption(w *DelimitedWriter) error {
	w.UnnamedFieldNamer = o.namer
	return nil
}

func (o unnamedFieldNamerOption) applyJSONLOption(w *JSONLWriter) error {
	w.UnnamedFieldNamer = o.namer
	return nil
}

// WithFlushEachRow configures [DelimitedWriter] to flush the underlying encoding/csv
// buffer after each successful data row. Use for interactive streaming when consumers
// should see output before the export finishes; the default buffers until [Flusher.Flush].
func WithFlushEachRow() DelimitedOption {
	return delimitedOptionFunc(func(w *DelimitedWriter) error {
		w.flushEachRow = true
		return nil
	})
}

// WithHeader sets whether [DelimitedWriter] emits a CSV/TSV header (default true).
// The header is written before the first data row, or on [DelimitedWriter.Flush] if only
// names were registered. See [DelimitedWriter.WriteHeader] to emit it earlier.
func WithHeader(header bool) DelimitedOption {
	return delimitedOptionFunc(func(w *DelimitedWriter) error {
		w.Header = header
		return nil
	})
}

// columnSchema holds column names for output labeling and optional field types for
// WriteStructValues. When types is non-empty, len(types) equals len(names).
// registered is true after Prepare*, With*, or the first row supplies a schema
// (including a zero-field row type).
type columnSchema struct {
	names      []string
	types      []*sppb.Type
	registered bool
}

func (s *columnSchema) applyRowType(rowType *sppb.StructType) {
	rowType = normalizeRowType(rowType)
	s.names = columnNamesFromRowType(rowType)
	s.types = fieldTypesFromRowType(rowType)
	s.registered = true
}

func (s *columnSchema) applyNamesOnly(names []string) {
	s.names = slices.Clone(names)
	s.types = nil
	s.registered = true
}

// DelimitedWriter writes rows as CSV-style delimited text. By default, call Flush after the
// final write; [WithFlushEachRow] flushes encoding/csv after each data row instead.
// Header controls automatic header output; see [WithHeader] and [DelimitedWriter.WriteHeader].
type DelimitedWriter struct {
	formatter *spanvalue.FormatConfig
	// Header enables a header line before the first data row when true (default).
	// See [WithHeader].
	Header bool
	// Set before the first write. Once names have been resolved for the current
	// schema, later changes do not retroactively rewrite cached header names.
	UnnamedFieldNamer spanvalue.UnnamedFieldNamer

	schema              columnSchema
	resolvedColumnNames []string
	out                 io.Writer
	writer              *csv.Writer
	delimiter           rune
	flushEachRow        bool
	wroteHeader         bool
	wroteData           bool
}

// NewCSVWriter returns a comma-delimited CSV writer configured by options.
// It is a thin helper for NewDelimitedWriter(out, Comma, opts...).
func NewCSVWriter(out io.Writer, opts ...DelimitedOption) (*DelimitedWriter, error) {
	return NewDelimitedWriter(out, Comma, opts...)
}

func newDelimitedWriter(out io.Writer) *DelimitedWriter {
	return &DelimitedWriter{
		formatter:         spanvalue.SimpleFormatConfig(),
		Header:            true,
		UnnamedFieldNamer: spanvalue.IndexedUnnamedFieldNamer,
		out:               out,
	}
}

// NewDelimitedWriter returns a CSV-style writer using delimiter as the field
// delimiter and configured by options.
//
// It supports CSV (delimiter [Comma]), quoted TSV (delimiter '\t'), or other
// single-rune delimiters. Output follows encoding/csv quoting rules, not raw
// field joins. Delimiter must be non-zero and a valid encoding/csv delimiter.
// See the package-level section "Quoted delimited text vs raw tab-separated".
func NewDelimitedWriter(out io.Writer, delimiter rune, options ...DelimitedOption) (*DelimitedWriter, error) {
	if out == nil {
		return nil, ErrNilOutputWriter
	}
	if !validDelimiter(delimiter) {
		return nil, fmt.Errorf("%w: %q", ErrInvalidDelimiter, delimiter)
	}
	w := newDelimitedWriter(out)
	w.delimiter = delimiter
	if err := applyDelimitedOptions(w, options...); err != nil {
		return nil, err
	}
	return w, nil
}

// NewDelimitedWriterWithOptions forwards to [NewDelimitedWriter].
//
// Deprecated: Use [NewDelimitedWriter] instead.
func NewDelimitedWriterWithOptions(out io.Writer, delimiter rune, options ...DelimitedOption) (*DelimitedWriter, error) {
	return NewDelimitedWriter(out, delimiter, options...)
}

// WriteRow writes one delimited row; see package doc "Column names and field types".
func (w *DelimitedWriter) WriteRow(row *spanner.Row) error {
	columnNames, values, err := rowData(row)
	if err != nil {
		return err
	}
	return w.WriteValues(columnNames, values)
}

// Prepare initializes the delimited schema from result-set metadata before the first
// row is written.
//
// Deprecated: Use [DelimitedWriter.PrepareRowType] or [DelimitedWriter.PrepareColumnNames].
func (w *DelimitedWriter) Prepare(metadata *sppb.ResultSetMetadata) error {
	return w.PrepareRowType(rowTypeFromMetadata(metadata))
}

// PrepareRowType registers column names and field types; same as [WithRowType].
// Nil rowType and a row type with no fields both register an empty schema (see package doc).
func (w *DelimitedWriter) PrepareRowType(rowType *sppb.StructType) error {
	return w.prepareRowType(rowType)
}

// PrepareColumnNames registers column names only; same as [WithColumnNames] for non-empty
// names. Unlike [WithColumnNames], an empty names slice returns [ErrMissingColumnNames];
// for zero-column result sets use [DelimitedWriter.PrepareRowType] instead.
func (w *DelimitedWriter) PrepareColumnNames(names []string) error {
	return w.prepareColumnNames(names)
}

func (w *DelimitedWriter) prepareRowType(rowType *sppb.StructType) error {
	rowType = normalizeRowType(rowType)
	columnNames := columnNamesFromRowType(rowType)
	if err := validatePrepareRowTypeTransition(&w.schema, columnNames); err != nil {
		return err
	}
	w.setRowType(rowType)
	return nil
}

func (w *DelimitedWriter) prepareColumnNames(names []string) error {
	if len(names) == 0 {
		return ErrMissingColumnNames
	}
	if err := w.initOrValidateColumnNames(names); err != nil {
		return err
	}
	w.setColumnNames(names)
	return nil
}

// WriteHeader writes the CSV/TSV header once; a column schema must already be registered.
// With zero registered column names (empty row type), WriteHeader succeeds without writing.
// With no registered schema, it returns [ErrMissingColumnNames].
// When [DelimitedWriter.Header] is true, [DelimitedWriter.Flush] also writes a pending header.
func (w *DelimitedWriter) WriteHeader() error {
	if w.wroteHeader {
		return nil
	}
	if w.wroteData {
		return ErrHeaderAfterData
	}

	csvWriter, err := w.csvWriter()
	if err != nil {
		return err
	}
	if len(w.schema.names) == 0 {
		if !w.schema.registered {
			return ErrMissingColumnNames
		}
		return nil
	}

	resolvedNames, err := w.resolvedNames()
	if err != nil {
		return err
	}
	if err := csvWriter.Write(resolvedNames); err != nil {
		return err
	}
	w.wroteHeader = true
	return nil
}

// WriteValues writes one row from column names and GCVs.
func (w *DelimitedWriter) WriteValues(columnNames []string, values []spanner.GenericColumnValue) error {
	if err := w.initOrValidateColumnNames(columnNames); err != nil {
		return err
	}
	return w.WriteGCVs(values)
}

// WriteGCVs writes one row from GCVs; see package doc "Column names and field types".
func (w *DelimitedWriter) WriteGCVs(values []spanner.GenericColumnValue) error {
	csvWriter, err := w.csvWriter()
	if err != nil {
		return err
	}
	if !w.schema.registered {
		return ErrMissingColumnNames
	}
	if len(w.schema.names) == 0 {
		if len(values) == 0 {
			return nil
		}
		return ErrMissingColumnNames
	}

	formattedValues, err := spanvalue.FormatRowColumns(w.delimitedFormatter(), w.schema.names, values)
	if err != nil {
		return err
	}

	if w.Header {
		if err := w.WriteHeader(); err != nil {
			return err
		}
	}

	if err := csvWriter.Write(formattedValues); err != nil {
		return err
	}
	w.wroteData = true
	if w.flushEachRow {
		csvWriter.Flush()
		return csvWriter.Error()
	}
	return nil
}

// WriteStructValues writes one row from []*structpb.Value; see package doc
// "Column names and field types".
func (w *DelimitedWriter) WriteStructValues(values []*structpb.Value) error {
	gcvs, err := gcvsFromStructValues(w.schema.types, values)
	if err != nil {
		return err
	}
	return w.WriteGCVs(gcvs)
}

func (w *DelimitedWriter) setRowType(rowType *sppb.StructType) {
	w.schema.applyRowType(rowType)
	w.resolvedColumnNames = nil
}

func (w *DelimitedWriter) setColumnNames(names []string) {
	if len(names) == 0 {
		return
	}
	w.schema.applyNamesOnly(names)
	w.resolvedColumnNames = nil
}

func (w *DelimitedWriter) initOrValidateColumnNames(columnNames []string) error {
	initialized := len(w.schema.names) == 0
	if err := initOrValidateColumnNames(&w.schema, columnNames); err != nil {
		return err
	}
	if len(w.schema.names) > 0 {
		w.schema.registered = true
	}
	if initialized && len(w.schema.names) > 0 {
		w.resolvedColumnNames = nil
	}
	return nil
}

// FormatConfig returns the effective formatter used for delimited value cells.
// When no formatter is configured, this returns [spanvalue.SimpleFormatConfig].
// Configure it only via [NewDelimitedWriter], [NewCSVWriter], or [WithFormatter].
func (w *DelimitedWriter) FormatConfig() *spanvalue.FormatConfig {
	return w.delimitedFormatter()
}

func (w *DelimitedWriter) delimitedFormatter() *spanvalue.FormatConfig {
	if w.formatter == nil {
		return spanvalue.SimpleFormatConfig()
	}
	return w.formatter
}

func (w *DelimitedWriter) csvWriter() (*csv.Writer, error) {
	delimiter := w.delimiter
	if w.writer != nil {
		return w.writer, nil
	}
	if !validDelimiter(delimiter) {
		return nil, fmt.Errorf("%w: %q", ErrInvalidDelimiter, delimiter)
	}
	if w.out == nil {
		return nil, ErrNilOutputWriter
	}
	w.writer = csv.NewWriter(w.out)
	w.writer.Comma = delimiter
	w.delimiter = delimiter
	return w.writer, nil
}

func validDelimiter(delimiter rune) bool {
	return delimiter != 0 &&
		delimiter != '"' &&
		delimiter != '\r' &&
		delimiter != '\n' &&
		utf8.ValidRune(delimiter) &&
		delimiter != utf8.RuneError
}

// Flush flushes buffered delimited data to the underlying writer. When [DelimitedWriter.Header]
// is true, a schema is registered, len(column names) > 0, and no header was written yet,
// Flush writes the header first (including zero-row SELECT exports). With a registered
// zero-column schema, Flush succeeds without writing. With no registered schema and
// [DelimitedWriter.Header] true, Flush returns [ErrMissingColumnNames]. Flush does not close
// the underlying writer.
func (w *DelimitedWriter) Flush() error {
	if w.Header && !w.wroteHeader {
		if !w.schema.registered {
			return ErrMissingColumnNames
		}
		if err := w.WriteHeader(); err != nil {
			return err
		}
	}
	if w.writer == nil {
		return nil
	}
	w.writer.Flush()
	return w.writer.Error()
}

func (w *DelimitedWriter) resolvedNames() ([]string, error) {
	if len(w.resolvedColumnNames) != 0 || len(w.schema.names) == 0 {
		return w.resolvedColumnNames, nil
	}
	if w.UnnamedFieldNamer == nil {
		return w.schema.names, nil
	}
	resolvedNames, err := internal.ResolveColumnNames(w.schema.names, w.UnnamedFieldNamer)
	if err != nil {
		return nil, err
	}
	w.resolvedColumnNames = resolvedNames
	return resolvedNames, nil
}

// JSONLWriter writes one JSON object per line.
// JSONLWriter streams one JSON object per line using [github.com/apstndb/spanvalue] JSON formatting.
type JSONLWriter struct {
	formatter *spanvalue.FormatConfig
	// Set before the first write. Once names have been resolved for the current
	// schema, later changes do not retroactively rewrite cached object keys.
	UnnamedFieldNamer spanvalue.UnnamedFieldNamer

	schema              columnSchema
	resolvedColumnNames []string
	marshaledKeys       [][]byte
	out                 io.Writer
}

// NewJSONLWriter returns a JSONL writer configured by options.
func NewJSONLWriter(out io.Writer, options ...JSONLOption) (*JSONLWriter, error) {
	if out == nil {
		return nil, ErrNilOutputWriter
	}
	w := newJSONLWriter(out)
	if err := applyJSONLOptions(w, options...); err != nil {
		return nil, err
	}
	return w, nil
}

// NewJSONLWriterWithOptions forwards to [NewJSONLWriter].
//
// Deprecated: Use [NewJSONLWriter] instead.
func NewJSONLWriterWithOptions(out io.Writer, options ...JSONLOption) (*JSONLWriter, error) {
	return NewJSONLWriter(out, options...)
}

func newJSONLWriter(out io.Writer) *JSONLWriter {
	return &JSONLWriter{
		formatter:         spanvalue.JSONFormatConfig(),
		UnnamedFieldNamer: spanvalue.IndexedUnnamedFieldNamer,
		out:               out,
	}
}

// WriteRow writes one JSONL row. Does not require With* or Prepare*; see [DelimitedWriter.WriteRow].
func (w *JSONLWriter) WriteRow(row *spanner.Row) error {
	columnNames, values, err := rowData(row)
	if err != nil {
		return err
	}
	return w.WriteValues(columnNames, values)
}

// Prepare initializes the JSONL schema from result-set metadata before the first
// row is written. If a schema is already initialized, Prepare verifies that the
// metadata column names match the existing schema.
//
// Deprecated: Use [JSONLWriter.PrepareRowType], [JSONLWriter.PrepareColumnNames],
// [WithRowType], [WithColumnNames], or [WithMetadata].
func (w *JSONLWriter) Prepare(metadata *sppb.ResultSetMetadata) error {
	return w.PrepareRowType(rowTypeFromMetadata(metadata))
}

// PrepareRowType registers names and field types; see [DelimitedWriter.PrepareRowType].
// Nil rowType registers an empty schema.
func (w *JSONLWriter) PrepareRowType(rowType *sppb.StructType) error {
	return w.prepareRowType(rowType)
}

// PrepareColumnNames registers column names; see [DelimitedWriter.PrepareColumnNames].
func (w *JSONLWriter) PrepareColumnNames(names []string) error {
	return w.prepareColumnNames(names)
}

func (w *JSONLWriter) prepareRowType(rowType *sppb.StructType) error {
	rowType = normalizeRowType(rowType)
	columnNames := columnNamesFromRowType(rowType)
	if err := validatePrepareRowTypeTransition(&w.schema, columnNames); err != nil {
		return err
	}
	w.setRowType(rowType)
	return nil
}

func (w *JSONLWriter) prepareColumnNames(names []string) error {
	if len(names) == 0 {
		return ErrMissingColumnNames
	}
	if err := w.initOrValidateColumnNames(names); err != nil {
		return err
	}
	w.setColumnNames(names)
	return nil
}

// WriteValues writes one row; see [DelimitedWriter.WriteValues].
func (w *JSONLWriter) WriteValues(columnNames []string, values []spanner.GenericColumnValue) error {
	if err := w.initOrValidateColumnNames(columnNames); err != nil {
		return err
	}
	return w.WriteGCVs(values)
}

// WriteStructValues writes one row; see [DelimitedWriter.WriteStructValues].
func (w *JSONLWriter) WriteStructValues(values []*structpb.Value) error {
	gcvs, err := gcvsFromStructValues(w.schema.types, values)
	if err != nil {
		return err
	}
	return w.WriteGCVs(gcvs)
}

// WriteGCVs writes one row; see [DelimitedWriter.WriteGCVs].
func (w *JSONLWriter) WriteGCVs(values []spanner.GenericColumnValue) error {
	if w.out == nil {
		return ErrNilOutputWriter
	}
	if !w.schema.registered {
		return ErrMissingColumnNames
	}
	if len(w.schema.names) == 0 {
		if len(values) == 0 {
			return nil
		}
		return ErrMissingColumnNames
	}
	formattedValues, err := spanvalue.FormatRowColumns(w.jsonlFormatter(), w.schema.names, values)
	if err != nil {
		return err
	}
	resolvedNames, err := w.resolvedNames()
	if err != nil {
		return err
	}
	marshaledKeys, err := w.marshalResolvedNames(resolvedNames)
	if err != nil {
		return err
	}
	s, err := internal.AssembleJSONObjectWithMarshaledKeys(marshaledKeys, formattedValues)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(w.out, s)
	return err
}

// Flush finalizes JSONL output. JSONLWriter is unbuffered, so this is a no-op.
func (w *JSONLWriter) Flush() error {
	return nil
}

func (w *JSONLWriter) setRowType(rowType *sppb.StructType) {
	w.schema.applyRowType(rowType)
	w.resolvedColumnNames = nil
	w.marshaledKeys = nil
}

func (w *JSONLWriter) setColumnNames(names []string) {
	if len(names) == 0 {
		return
	}
	w.schema.applyNamesOnly(names)
	w.resolvedColumnNames = nil
	w.marshaledKeys = nil
}

func (w *JSONLWriter) initOrValidateColumnNames(columnNames []string) error {
	initialized := len(w.schema.names) == 0
	if err := initOrValidateColumnNames(&w.schema, columnNames); err != nil {
		return err
	}
	if len(w.schema.names) > 0 {
		w.schema.registered = true
	}
	if initialized && len(w.schema.names) > 0 {
		w.resolvedColumnNames = nil
		w.marshaledKeys = nil
	}
	return nil
}

// FormatConfig returns the effective formatter used for JSONL value encoding.
// When no formatter is configured, this returns [spanvalue.JSONFormatConfig].
// Configure it only via [NewJSONLWriter] or [WithFormatter].
func (w *JSONLWriter) FormatConfig() *spanvalue.FormatConfig {
	return w.jsonlFormatter()
}

func (w *JSONLWriter) jsonlFormatter() *spanvalue.FormatConfig {
	if w.formatter == nil {
		return spanvalue.JSONFormatConfig()
	}
	return w.formatter
}

func (w *JSONLWriter) resolvedNames() ([]string, error) {
	if len(w.resolvedColumnNames) != 0 || len(w.schema.names) == 0 {
		return w.resolvedColumnNames, nil
	}
	if w.UnnamedFieldNamer == nil {
		return w.schema.names, nil
	}
	resolvedNames, err := internal.ResolveColumnNames(w.schema.names, w.UnnamedFieldNamer)
	if err != nil {
		return nil, err
	}
	w.resolvedColumnNames = resolvedNames
	return resolvedNames, nil
}

func (w *JSONLWriter) marshalResolvedNames(resolvedNames []string) ([][]byte, error) {
	if w.marshaledKeys != nil {
		return w.marshaledKeys, nil
	}
	marshaledKeys, err := internal.MarshalJSONObjectKeys(resolvedNames)
	if err != nil {
		return nil, err
	}
	w.marshaledKeys = marshaledKeys
	return marshaledKeys, nil
}

// SQLInsertWriter writes rows as SQL INSERT statements with dialect-aware identifier quoting.
//
// After any error from [SQLInsertWriter.WriteRow], [SQLInsertWriter.WriteGCVs],
// [SQLInsertWriter.WriteValues], or [SQLInsertWriter.WriteStructValues], discard the
// writer; partial batched INSERT output may be unrecoverable on retry.
// SQLInsertWriter streams INSERT (or INSERT OR …) statements for a fixed table.
type SQLInsertWriter struct {
	table     string
	formatter *spanvalue.FormatConfig

	insertKind        SQLInsertKind
	sqlDialect        databasepb.DatabaseDialect
	batchSize         int
	batchPending      int
	schema            columnSchema
	quotedColumnNames string
	quotedTable       string
	quotedTableInput  string
	out               io.Writer
}

// NewSQLInsertWriter returns a SQL INSERT writer configured by options.
func NewSQLInsertWriter(out io.Writer, table string, options ...SQLInsertOption) (*SQLInsertWriter, error) {
	if out == nil {
		return nil, ErrNilOutputWriter
	}
	w := newSQLInsertWriter(out, table)
	if err := applySQLInsertOptions(w, options...); err != nil {
		return nil, err
	}
	if err := w.validateSQLInsertConfig(); err != nil {
		return nil, err
	}
	if len(w.schema.names) > 0 {
		if _, err := w.initOrValidateQuotedColumns(nil); err != nil {
			return nil, err
		}
	}
	return w, nil
}

// NewSQLInsertWriterWithOptions forwards to [NewSQLInsertWriter].
//
// Deprecated: Use [NewSQLInsertWriter] instead.
func NewSQLInsertWriterWithOptions(out io.Writer, table string, options ...SQLInsertOption) (*SQLInsertWriter, error) {
	return NewSQLInsertWriter(out, table, options...)
}

func newSQLInsertWriter(out io.Writer, table string) *SQLInsertWriter {
	return &SQLInsertWriter{
		table:      table,
		formatter:  spanvalue.LiteralFormatConfig(),
		sqlDialect: databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL,
		out:        out,
	}
}

// TableName returns the qualified table name used in INSERT statements.
// Configure it only via [NewSQLInsertWriter]; create a new writer to use a different table.
func (w *SQLInsertWriter) TableName() string {
	return w.table
}

// FormatConfig returns the effective formatter used for INSERT value literals.
// When no formatter is configured, this returns [spanvalue.LiteralFormatConfig].
// Configure it only via [NewSQLInsertWriter] or [WithFormatter].
func (w *SQLInsertWriter) FormatConfig() *spanvalue.FormatConfig {
	return w.insertFormatter()
}

func (w *SQLInsertWriter) WriteRow(row *spanner.Row) error {
	columnNames, values, err := rowData(row)
	if err != nil {
		return err
	}
	return w.WriteValues(columnNames, values)
}

// Prepare initializes the SQL INSERT schema from result-set metadata before the
// first row is written. If a schema is already initialized, Prepare verifies
// that the metadata column names match the existing schema.
//
// Deprecated: Use [SQLInsertWriter.PrepareRowType], [SQLInsertWriter.PrepareColumnNames],
// [WithRowType], [WithColumnNames], or [WithMetadata].
func (w *SQLInsertWriter) Prepare(metadata *sppb.ResultSetMetadata) error {
	return w.PrepareRowType(rowTypeFromMetadata(metadata))
}

// PrepareRowType initializes the SQL INSERT schema from a row type before the first row is written.
// When the row type comes from a [cloud.google.com/go/spanner.RowIterator], use [RunRowIterator]
// or [PrepareRowType] with iter.Metadata.GetRowType() after the first Next. Nil rowType registers an empty schema;
// [SQLInsertWriter.WriteGCVs] still requires at least one column to emit SQL.
func (w *SQLInsertWriter) PrepareRowType(rowType *sppb.StructType) error {
	return w.prepareRowType(rowType)
}

// PrepareColumnNames initializes the SQL INSERT schema from column names before the first row is written.
// See [DelimitedWriter.PrepareColumnNames] for empty-name behavior.
func (w *SQLInsertWriter) PrepareColumnNames(names []string) error {
	return w.prepareColumnNames(names)
}

func (w *SQLInsertWriter) prepareRowType(rowType *sppb.StructType) error {
	rowType = normalizeRowType(rowType)
	columnNames := columnNamesFromRowType(rowType)
	if err := validatePrepareRowTypeTransition(&w.schema, columnNames); err != nil {
		return err
	}
	if len(columnNames) == 0 {
		w.setRowType(rowType)
		return nil
	}
	quotedColumns, err := quoteIdentifiers(columnNames, w.sqlDialect)
	if err != nil {
		return err
	}
	w.setRowType(rowType)
	w.quotedColumnNames = strings.Join(quotedColumns, ", ")
	return nil
}

func (w *SQLInsertWriter) prepareColumnNames(names []string) error {
	if len(names) == 0 {
		return ErrMissingColumnNames
	}
	if _, err := w.initOrValidateQuotedColumns(names); err != nil {
		return err
	}
	w.schema.types = nil
	return nil
}

func (w *SQLInsertWriter) WriteValues(columnNames []string, values []spanner.GenericColumnValue) error {
	quotedColumns, err := w.initOrValidateQuotedColumns(columnNames)
	if err != nil {
		return err
	}
	return w.writeGCVs(values, quotedColumns)
}

// WriteStructValues writes one row from structpb values using the field-type schema
// registered by [WithRowType], [WithMetadata], or [PrepareRowType].
func (w *SQLInsertWriter) WriteStructValues(values []*structpb.Value) error {
	gcvs, err := gcvsFromStructValues(w.schema.types, values)
	if err != nil {
		return err
	}
	return w.WriteGCVs(gcvs)
}

func (w *SQLInsertWriter) WriteGCVs(values []spanner.GenericColumnValue) error {
	if !w.schema.registered {
		return ErrMissingColumnNames
	}
	if len(w.schema.names) == 0 {
		return ErrMissingColumnNames
	}
	quotedColumns, err := w.initOrValidateQuotedColumns(nil)
	if err != nil {
		return err
	}
	return w.writeGCVs(values, quotedColumns)
}

// Flush finalizes a partial multi-row INSERT batch started by [WithSQLBatchSize].
// When batch size is 0 or 1, or when the last batch closed on a size boundary,
// Flush is a no-op. Flush is safe to call unconditionally after the final row.
func (w *SQLInsertWriter) Flush() error {
	if w.sqlBatchSize() <= 1 || w.batchPending == 0 {
		return nil
	}
	return w.closePendingBatch()
}

func (w *SQLInsertWriter) closePendingBatch() error {
	if w.batchPending == 0 {
		return nil
	}
	if _, err := io.WriteString(w.out, ";\n"); err != nil {
		return err
	}
	w.batchPending = 0
	return nil
}

func (w *SQLInsertWriter) sqlBatchSize() int {
	if w.batchSize <= 1 {
		return 1
	}
	return w.batchSize
}

func (w *SQLInsertWriter) validateSQLInsertConfig() error {
	if w.sqlDialect != databasepb.DatabaseDialect_POSTGRESQL {
		return nil
	}
	switch w.insertKind {
	case SQLInsertOrIgnore, SQLInsertOrUpdate:
		return ErrInvalidSQLInsertKindForDialect
	default:
		return nil
	}
}

func (w *SQLInsertWriter) writeGCVs(values []spanner.GenericColumnValue, quotedColumns string) error {
	if w.out == nil {
		return ErrNilOutputWriter
	}
	if w.table == "" {
		return ErrEmptyTableName
	}
	formattedValues, err := spanvalue.FormatRowColumns(w.insertFormatter(), w.schema.names, values)
	if err != nil {
		return err
	}
	if w.sqlBatchSize() <= 1 {
		return w.writeSingleInsert(quotedColumns, formattedValues)
	}
	return w.appendBatchedInsert(quotedColumns, formattedValues)
}

func (w *SQLInsertWriter) writeSingleInsert(quotedColumns string, formattedValues []string) error {
	quotedTable, err := w.quotedQualifiedTable()
	if err != nil {
		return err
	}
	prefix := w.insertKind.String()
	if _, err := fmt.Fprintf(w.out, "%s INTO %s (%s) VALUES (", prefix, quotedTable, quotedColumns); err != nil {
		return err
	}
	if err := w.writeFormattedValues(formattedValues); err != nil {
		return err
	}
	_, err = io.WriteString(w.out, ");\n")
	return err
}

func (w *SQLInsertWriter) rejectTableChangeMidBatch() error {
	if w.batchPending == 0 || w.quotedTableInput == "" || w.table == w.quotedTableInput {
		return nil
	}
	return fmt.Errorf("%w: %q to %q", ErrTableNameChangedMidBatch, w.quotedTableInput, w.table)
}

func (w *SQLInsertWriter) appendBatchedInsert(quotedColumns string, formattedValues []string) error {
	if err := w.rejectTableChangeMidBatch(); err != nil {
		return err
	}
	if w.batchPending == 0 {
		quotedTable, err := w.quotedQualifiedTable()
		if err != nil {
			return err
		}
		prefix := w.insertKind.String()
		if _, err := fmt.Fprintf(w.out, "%s INTO %s (%s) VALUES\n  (", prefix, quotedTable, quotedColumns); err != nil {
			return err
		}
	} else if _, err := io.WriteString(w.out, ",\n  ("); err != nil {
		return err
	}
	if err := w.writeFormattedValues(formattedValues); err != nil {
		return err
	}
	if _, err := io.WriteString(w.out, ")"); err != nil {
		return err
	}
	w.batchPending++
	if w.batchPending >= w.sqlBatchSize() {
		return w.closePendingBatch()
	}
	return nil
}

func (w *SQLInsertWriter) writeFormattedValues(formattedValues []string) error {
	for i, val := range formattedValues {
		if i > 0 {
			if _, err := io.WriteString(w.out, ", "); err != nil {
				return err
			}
		}
		if _, err := io.WriteString(w.out, val); err != nil {
			return err
		}
	}
	return nil
}

func (w *SQLInsertWriter) setRowType(rowType *sppb.StructType) {
	w.schema.applyRowType(rowType)
	w.quotedColumnNames = ""
}

func (w *SQLInsertWriter) setColumnNames(names []string) {
	if len(names) == 0 {
		return
	}
	w.schema.applyNamesOnly(names)
	w.quotedColumnNames = ""
}

func (w *SQLInsertWriter) insertFormatter() *spanvalue.FormatConfig {
	if w.formatter == nil {
		// Zero-initialized writers are unsupported; avoid mutating w in this getter.
		return spanvalue.LiteralFormatConfig()
	}
	return w.formatter
}

func (w *SQLInsertWriter) initOrValidateQuotedColumns(columnNames []string) (string, error) {
	if len(columnNames) == 0 && w.quotedColumnNames != "" {
		return w.quotedColumnNames, nil
	}
	names, err := validatedColumnNames(w.schema.names, w.schema.registered, columnNames)
	if err != nil {
		return "", err
	}
	quotedColumns, err := quoteIdentifiers(names, w.sqlDialect)
	if err != nil {
		return "", err
	}
	if len(w.schema.names) == 0 {
		w.schema.names = names
	}
	w.schema.registered = true
	w.quotedColumnNames = strings.Join(quotedColumns, ", ")
	return w.quotedColumnNames, nil
}

func (w *SQLInsertWriter) quotedQualifiedTable() (string, error) {
	if w.quotedTable != "" && w.quotedTableInput == w.table {
		return w.quotedTable, nil
	}
	quotedTable, err := quoteQualifiedIdentifier(w.table, w.sqlDialect)
	if err != nil {
		return "", err
	}
	w.quotedTable = quotedTable
	w.quotedTableInput = w.table
	return quotedTable, nil
}

// FormatDelimitedRow formats one row as a CSV-style delimited record without a
// trailing newline. Pass Comma for CSV output.
func FormatDelimitedRow(fc *spanvalue.FormatConfig, row *spanner.Row, delimiter rune) (string, error) {
	columnNames, values, err := RowData(row)
	if err != nil {
		return "", err
	}
	return FormatDelimitedValues(fc, columnNames, values, delimiter)
}

// FormatDelimitedValues formats one row represented as column names plus GCV
// values as a CSV-style delimited record without a trailing newline. Pass Comma
// for CSV output.
func FormatDelimitedValues(fc *spanvalue.FormatConfig, columnNames []string, values []spanner.GenericColumnValue, delimiter rune) (string, error) {
	formattedValues, err := spanvalue.FormatRowColumns(simpleFormatter(fc), columnNames, values)
	if err != nil {
		return "", err
	}
	return formatDelimitedRecord(formattedValues, delimiter)
}

// FormatJSONLRow formats one row as a JSON object string without a trailing
// newline. Callers writing JSONL streams should add the newline at the stream
// boundary.
func FormatJSONLRow(fc *spanvalue.FormatConfig, row *spanner.Row, namer spanvalue.UnnamedFieldNamer) (string, error) {
	columnNames, values, err := RowData(row)
	if err != nil {
		return "", err
	}
	return FormatJSONLValues(fc, columnNames, values, namer)
}

// FormatJSONLValues formats one row represented as column names plus GCV values
// as a JSON object string without a trailing newline. Callers writing JSONL
// streams should add the newline at the stream boundary.
func FormatJSONLValues(fc *spanvalue.FormatConfig, columnNames []string, values []spanner.GenericColumnValue, namer spanvalue.UnnamedFieldNamer) (string, error) {
	return spanvalue.FormatRowJSONObjectFromColumns(jsonFormatter(fc), columnNames, values, namer)
}

// RowData extracts column names and GenericColumnValue cells from row.
func RowData(row *spanner.Row) ([]string, []spanner.GenericColumnValue, error) {
	if row == nil {
		return nil, nil, ErrNilRow
	}
	values := make([]spanner.GenericColumnValue, row.Size())
	for i := range values {
		if err := row.Column(i, &values[i]); err != nil {
			return nil, nil, err
		}
	}
	return slices.Clone(row.ColumnNames()), values, nil
}

func rowData(row *spanner.Row) ([]string, []spanner.GenericColumnValue, error) {
	return RowData(row)
}

func formatDelimitedRecord(values []string, delimiter rune) (string, error) {
	if !validDelimiter(delimiter) {
		return "", fmt.Errorf("%w: %q", ErrInvalidDelimiter, delimiter)
	}
	var out bytes.Buffer
	w := csv.NewWriter(&out)
	w.Comma = delimiter
	if err := w.Write(values); err != nil {
		return "", err
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return "", err
	}
	return strings.TrimSuffix(out.String(), "\n"), nil
}

func simpleFormatter(fc *spanvalue.FormatConfig) *spanvalue.FormatConfig {
	if fc != nil {
		return fc
	}
	return spanvalue.SimpleFormatConfig()
}

func jsonFormatter(fc *spanvalue.FormatConfig) *spanvalue.FormatConfig {
	if fc != nil {
		return fc
	}
	return spanvalue.JSONFormatConfig()
}

func normalizeRowType(rowType *sppb.StructType) *sppb.StructType {
	if rowType == nil {
		return &sppb.StructType{}
	}
	return rowType
}

func rowTypeFromMetadata(metadata *sppb.ResultSetMetadata) *sppb.StructType {
	if metadata == nil {
		return nil
	}
	return metadata.GetRowType()
}

func columnNamesFromRowType(rowType *sppb.StructType) []string {
	if rowType == nil {
		return nil
	}
	fields := rowType.GetFields()
	names := make([]string, len(fields))
	for i, field := range fields {
		if field != nil {
			names[i] = field.GetName()
		}
	}
	return names
}

func fieldTypesFromRowType(rowType *sppb.StructType) []*sppb.Type {
	if rowType == nil {
		return nil
	}
	fields := rowType.GetFields()
	types := make([]*sppb.Type, len(fields))
	for i, field := range fields {
		if field != nil {
			types[i] = field.GetType()
		}
	}
	return types
}

func gcvFromTypeValue(typ *sppb.Type, value *structpb.Value) (spanner.GenericColumnValue, error) {
	if typ == nil {
		return spanner.GenericColumnValue{}, spanvalue.ErrNilStructField
	}
	return spanner.GenericColumnValue{Type: typ, Value: value}, nil
}

func gcvsFromStructValues(types []*sppb.Type, values []*structpb.Value) ([]spanner.GenericColumnValue, error) {
	if len(types) == 0 {
		return nil, ErrMissingFieldTypes
	}
	if len(values) != len(types) {
		return nil, fmt.Errorf("%w: got %d values for %d fields", ErrMismatchedStructValueCount, len(values), len(types))
	}
	gcvs := make([]spanner.GenericColumnValue, len(values))
	for i, value := range values {
		gcv, err := gcvFromTypeValue(types[i], value)
		if err != nil {
			return nil, fmt.Errorf("column %d: %w", i, err)
		}
		gcvs[i] = gcv
	}
	return gcvs, nil
}

// initOrValidateColumnNames initializes schema.names from the first non-empty
// columnNames slice it sees. Once initialized, subsequent non-empty inputs must
// match exactly; empty inputs are accepted only after initialization.
func initOrValidateColumnNames(schema *columnSchema, columnNames []string) error {
	validated, err := validatedColumnNames(schema.names, schema.registered, columnNames)
	if err != nil {
		return err
	}
	if len(schema.names) == 0 {
		schema.names = validated
	}
	return nil
}

func validatedColumnNames(existing []string, registered bool, columnNames []string) ([]string, error) {
	if len(existing) == 0 {
		if len(columnNames) == 0 {
			if registered {
				return nil, nil
			}
			return nil, ErrMissingColumnNames
		}
		if registered {
			return nil, fmt.Errorf("%w: got %v, want zero-column schema (registered empty row type)", ErrColumnNamesMismatch, columnNames)
		}
		return slices.Clone(columnNames), nil
	}
	if len(columnNames) == 0 {
		return existing, nil
	}
	if !slices.Equal(existing, columnNames) {
		return nil, fmt.Errorf("%w: got %v want %v", ErrColumnNamesMismatch, columnNames, existing)
	}
	return existing, nil
}

// validatePrepareRowTypeTransition checks a PrepareRowType call against the current schema
// before setRowType mutates names, types, or cached derived fields.
func validatePrepareRowTypeTransition(schema *columnSchema, columnNames []string) error {
	if len(columnNames) == 0 {
		if schema.registered && len(schema.names) > 0 {
			return fmt.Errorf("%w: got empty row type, want %v", ErrColumnNamesMismatch, schema.names)
		}
		return nil
	}
	_, err := validatedColumnNames(schema.names, schema.registered, columnNames)
	return err
}

// quoteIdentifiers quotes identifiers for dialect and rejects empty names.
func quoteIdentifiers(names []string, dialect databasepb.DatabaseDialect) ([]string, error) {
	quoted := make([]string, len(names))
	for i, name := range names {
		if name == "" {
			return nil, ErrEmptyColumnName
		}
		quoted[i] = spanvalue.QuoteIdentifier(dialect, name)
	}
	return quoted, nil
}

// quoteQualifiedIdentifier quotes each identifier segment in a dotted path for dialect.
func quoteQualifiedIdentifier(name string, dialect databasepb.DatabaseDialect) (string, error) {
	parts := strings.Split(name, ".")
	for i, part := range parts {
		if part == "" {
			return "", fmt.Errorf("%w: qualified table name contains empty segment", ErrEmptyTableName)
		}
		parts[i] = spanvalue.QuoteIdentifier(dialect, part)
	}
	return strings.Join(parts, "."), nil
}
