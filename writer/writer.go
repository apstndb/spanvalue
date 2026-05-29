// Package writer provides small streaming helpers for exporting Spanner rows
// using spanvalue formatters.
//
// DelimitedWriter is the primary writer for CSV-style delimited text.
// NewCSVWriter is a thin helper for the common comma-delimited CSV case, while
// NewDelimitedWriter accepts an explicit delimiter for TSV and other delimited
// output. DelimitedWriter and JSONLWriter preserve explicit duplicate column
// names. Their UnnamedFieldNamer only fills empty column names, and generated
// names avoid collisions with existing names. DelimitedWriter buffers through
// encoding/csv, so callers must call Flush after the final write.
//
// Use [Writer] when an adapter only streams [cloud.google.com/go/spanner.Row]
// values from the Spanner client. Use [DelimitedWriter.WriteStructValues] on a concrete writer
// when the row is already represented as []*structpb.Value plus a registered
// field-type schema (spannerpb + structpb only at the writer boundary). Use
// [DelimitedWriter.WriteGCVs] when each cell is already a GenericColumnValue.
// [FlushWriter] covers writers that need finalization; call Flush after the
// last row. Flush does not close the underlying io.Writer.
//
// # Primary API
//
// [DelimitedWriter], [NewDelimitedWriter], [NewCSVWriter], [JSONLWriter],
// [NewJSONLWriter], [SQLInsertWriter], and [NewSQLInsertWriter] stream rows.
// [WithSQLInsertKind] selects the INSERT statement prefix. INSERT OR IGNORE and
// INSERT OR UPDATE are valid Spanner GoogleSQL DML forms; see the INSERT section
// in https://cloud.google.com/spanner/docs/reference/standard-sql/dml-syntax .
// Constructors accept options such as [WithRowType], [WithColumnNames],
// [WithMetadata], and [WithFormatter]. When schema is known only after
// construction, call the matching Prepare method on the concrete writer.
// [RowData], [FormatDelimitedRow], and [FormatJSONLRow] support one-row paths.
//
// # Schema registration and row input
//
// Writers need column names for labeling output (CSV headers, JSON keys, INSERT
// column lists). Some paths also register per-column *sppb.Type values so rows
// can be streamed as []*structpb.Value without fabricating metadata wrappers.
//
// Prefer registering schema with [WithRowType], [WithColumnNames], or
// [WithMetadata] at construction. When schema is known only after construction,
// call [DelimitedWriter.PrepareRowType] or [DelimitedWriter.PrepareColumnNames].
// For metadata only: PrepareRowType(metadata.GetRowType()).
//
//   - [WithRowType] / PrepareRowType: names plus field types (internal columnSchema).
//     Required for [WriteStructValues].
//   - [WithColumnNames] / PrepareColumnNames: names only; types come from each GCV.
//   - [WithMetadata]: metadata.GetRowType(); other metadata fields are ignored.
//
// [DelimitedWriter.Prepare] is deprecated; use PrepareRowType or PrepareColumnNames.
//
// Row write layers (high to low):
//
//   - [Writer.WriteRow]: Spanner client row (*spanner.Row).
//   - WriteValues: explicit column names plus GCVs (may initialize names per row).
//   - [WriteGCVs]: registered column names; each GCV carries Type and Value.
//   - [DelimitedWriter.WriteStructValues]: registered field types; []*structpb.Value per row.
//
// Delimited, JSONL, and SQL writers use different output encodings after spanvalue
// formats each column; there is no shared "formatted row" interface.
//
// spanvalue formats cells from Type+Value pairs internally. ResultSetMetadata is
// only a carrier for RowType when registering schema, not used while formatting.
//
// DelimitedWriter defaults to emitting a header row. Use [WithHeader] with false
// for headerless CSV/TSV when names are registered but must not appear as the
// first output line.
//
// # Compatibility constructors
//
// [NewDelimitedWriterWithOptions], [NewJSONLWriterWithOptions], and
// [NewSQLInsertWriterWithOptions] forward to the primary constructors above.
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
	// ErrEmptyTableName reports that SQLInsertWriter.Table is empty.
	ErrEmptyTableName = errors.New("empty table name")
	// ErrEmptyColumnName reports that a SQL writer received an empty column name.
	ErrEmptyColumnName = errors.New("empty column name")
	// ErrNilOutputWriter reports that a writer was constructed without an output.
	ErrNilOutputWriter = errors.New("nil output writer")
	// ErrNilRow reports that WriteRow was called with a nil row.
	ErrNilRow = spanvalue.ErrNilRow
	// ErrMissingColumnNames reports that writing values requires initialized column names.
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
)

// Writer writes Spanner rows to an output stream.
//
// Writer intentionally models row streaming only. Some concrete writers also
// implement [Flusher]; callers that own the full write lifecycle must call
// Flush after the final row when it is available. Factories that may return a
// buffered writer should return a concrete type or [FlushWriter], not Writer
// alone.
type Writer interface {
	WriteRow(row *spanner.Row) error
}

// Flusher finalizes any buffered output. Flush does not close the underlying
// io.Writer. DelimitedWriter uses Flush to forward buffered CSV-style data;
// JSONLWriter and SQLInsertWriter implement it as a no-op so adapters can use
// one finalize path for all writer implementations.
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
	applyDelimitedOption(*DelimitedWriter)
}

// JSONLOption configures a JSONLWriter created by [NewJSONLWriter].
type JSONLOption interface {
	applyJSONLOption(*JSONLWriter)
}

// SQLInsertKind selects the INSERT statement prefix written by [SQLInsertWriter].
//
// Variants follow Spanner GoogleSQL DML: INSERT OR IGNORE skips rows whose primary
// key already exists; INSERT OR UPDATE inserts or updates by primary key. They
// cannot be combined with ON CONFLICT in the same statement, and INSERT is not
// supported in Partitioned DML. See https://cloud.google.com/spanner/docs/reference/standard-sql/dml-syntax .
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

func (o sqlInsertKindOption) applySQLInsertOption(w *SQLInsertWriter) {
	w.insertKind = o.kind
}

// SQLInsertOption configures a SQLInsertWriter created by [NewSQLInsertWriter].
type SQLInsertOption interface {
	applySQLInsertOption(*SQLInsertWriter)
}

type delimitedOptionFunc func(*DelimitedWriter)

func (f delimitedOptionFunc) applyDelimitedOption(w *DelimitedWriter) {
	f(w)
}

type metadataOption struct {
	metadata *sppb.ResultSetMetadata
}

// WithMetadata initializes a writer schema from metadata.GetRowType(), including
// field types for WriteStructValues. Other metadata fields are ignored.
// Prefer [WithRowType] when metadata is not available.
func WithMetadata(metadata *sppb.ResultSetMetadata) Option {
	return metadataOption{metadata: metadata}
}

func (o metadataOption) applyDelimitedOption(w *DelimitedWriter) {
	w.setRowType(rowTypeFromMetadata(o.metadata))
}

func (o metadataOption) applyJSONLOption(w *JSONLWriter) {
	w.setRowType(rowTypeFromMetadata(o.metadata))
}

func (o metadataOption) applySQLInsertOption(w *SQLInsertWriter) {
	w.setRowType(rowTypeFromMetadata(o.metadata))
}

type rowTypeOption struct {
	rowType *sppb.StructType
}

// WithRowType initializes a writer schema from a Spanner row type.
func WithRowType(rowType *sppb.StructType) Option {
	return rowTypeOption{rowType: rowType}
}

func (o rowTypeOption) applyDelimitedOption(w *DelimitedWriter) {
	w.setRowType(o.rowType)
}

func (o rowTypeOption) applyJSONLOption(w *JSONLWriter) {
	w.setRowType(o.rowType)
}

func (o rowTypeOption) applySQLInsertOption(w *SQLInsertWriter) {
	w.setRowType(o.rowType)
}

type columnNamesOption struct {
	names []string
}

// WithColumnNames initializes a writer schema from column names only.
// Use [DelimitedWriter.WriteGCVs] (or other WriteGCVs methods) so each row supplies types in GCVs.
func WithColumnNames(names []string) Option {
	return columnNamesOption{names: slices.Clone(names)}
}

func (o columnNamesOption) applyDelimitedOption(w *DelimitedWriter) {
	w.setColumnNames(o.names)
}

func (o columnNamesOption) applyJSONLOption(w *JSONLWriter) {
	w.setColumnNames(o.names)
}

func (o columnNamesOption) applySQLInsertOption(w *SQLInsertWriter) {
	w.setColumnNames(o.names)
}

type formatterOption struct {
	formatter *spanvalue.FormatConfig
}

// WithFormatter sets the FormatConfig used by a writer.
func WithFormatter(formatter *spanvalue.FormatConfig) Option {
	return formatterOption{formatter: formatter}
}

func (o formatterOption) applyDelimitedOption(w *DelimitedWriter) {
	w.Formatter = o.formatter
}

func (o formatterOption) applyJSONLOption(w *JSONLWriter) {
	w.Formatter = o.formatter
}

func (o formatterOption) applySQLInsertOption(w *SQLInsertWriter) {
	w.Formatter = o.formatter
}

type unnamedFieldNamerOption struct {
	namer spanvalue.UnnamedFieldNamer
}

// WithUnnamedFieldNamer sets the unnamed-field naming policy for delimited and JSONL writers.
func WithUnnamedFieldNamer(namer spanvalue.UnnamedFieldNamer) NameOption {
	return unnamedFieldNamerOption{namer: namer}
}

func (o unnamedFieldNamerOption) applyDelimitedOption(w *DelimitedWriter) {
	w.UnnamedFieldNamer = o.namer
}

func (o unnamedFieldNamerOption) applyJSONLOption(w *JSONLWriter) {
	w.UnnamedFieldNamer = o.namer
}

// WithHeader sets whether DelimitedWriter writes a header before data rows.
func WithHeader(header bool) DelimitedOption {
	return delimitedOptionFunc(func(w *DelimitedWriter) {
		w.Header = header
	})
}

// columnSchema holds column names for output labeling and optional field types for
// WriteStructValues. When types is non-empty, len(types) equals len(names).
type columnSchema struct {
	names []string
	types []*sppb.Type
}

func (s *columnSchema) applyRowType(rowType *sppb.StructType) {
	s.names = columnNamesFromRowType(rowType)
	s.types = fieldTypesFromRowType(rowType)
}

func (s *columnSchema) applyNamesOnly(names []string) {
	s.names = slices.Clone(names)
	s.types = nil
}

// DelimitedWriter writes rows as CSV-style delimited text. Call Flush after the final write.
type DelimitedWriter struct {
	Formatter *spanvalue.FormatConfig
	Header    bool
	// Set before the first write. Once names have been resolved for the current
	// schema, later changes do not retroactively rewrite cached header names.
	UnnamedFieldNamer spanvalue.UnnamedFieldNamer

	schema              columnSchema
	resolvedColumnNames []string
	out                 io.Writer
	writer              *csv.Writer
	delimiter           rune
	wroteHeader         bool
	wroteData           bool
}

// NewCSVWriter returns a comma-delimited CSV writer configured by options.
// It is a thin helper for NewDelimitedWriter(out, Comma, opts...).
func NewCSVWriter(out io.Writer, opts ...DelimitedOption) *DelimitedWriter {
	return NewDelimitedWriter(out, Comma, opts...)
}

func newDelimitedWriter(out io.Writer) *DelimitedWriter {
	return &DelimitedWriter{
		Formatter:         spanvalue.SimpleFormatConfig(),
		Header:            true,
		UnnamedFieldNamer: spanvalue.IndexedUnnamedFieldNamer,
		out:               out,
	}
}

// NewDelimitedWriter returns a CSV-style writer using delimiter as the field
// delimiter and configured by options. Pass Comma for CSV output or '\t' for TSV
// output. Delimiter must be non-zero and a valid encoding/csv delimiter.
func NewDelimitedWriter(out io.Writer, delimiter rune, options ...DelimitedOption) *DelimitedWriter {
	w := newDelimitedWriter(out)
	w.delimiter = delimiter
	for _, opt := range options {
		if opt != nil {
			opt.applyDelimitedOption(w)
		}
	}
	return w
}

// NewDelimitedWriterWithOptions forwards to [NewDelimitedWriter].
//
// Deprecated: Use [NewDelimitedWriter] instead.
func NewDelimitedWriterWithOptions(out io.Writer, delimiter rune, options ...DelimitedOption) *DelimitedWriter {
	return NewDelimitedWriter(out, delimiter, options...)
}

// WriteRow writes one delimited row, initializing the schema from row metadata if needed.
func (w *DelimitedWriter) WriteRow(row *spanner.Row) error {
	columnNames, values, err := rowData(row)
	if err != nil {
		return err
	}
	return w.WriteValues(columnNames, values)
}

// Prepare initializes the delimited schema from result-set metadata before the first
// row is written. If a schema is already initialized, Prepare verifies that the
// metadata column names match the existing schema.
//
// Deprecated: Prefer [WithRowType], [WithColumnNames], or [WithMetadata] at
// construction, or [DelimitedWriter.PrepareRowType] / [DelimitedWriter.PrepareColumnNames].
func (w *DelimitedWriter) Prepare(metadata *sppb.ResultSetMetadata) error {
	return w.PrepareRowType(rowTypeFromMetadata(metadata))
}

// PrepareRowType initializes the delimited schema from a row type before the first
// row is written. If a schema is already initialized, column names must match.
func (w *DelimitedWriter) PrepareRowType(rowType *sppb.StructType) error {
	columnNames, err := prepareRowType(rowType)
	if err != nil {
		return err
	}
	if err := w.initOrValidateColumnNames(columnNames); err != nil {
		return err
	}
	w.setRowType(rowType)
	return nil
}

// PrepareColumnNames initializes the delimited schema from column names before the
// first row is written. If a schema is already initialized, names must match.
func (w *DelimitedWriter) PrepareColumnNames(names []string) error {
	if len(names) == 0 {
		return ErrMissingColumnNames
	}
	if err := w.initOrValidateColumnNames(names); err != nil {
		return err
	}
	w.setColumnNames(names)
	return nil
}

// WriteHeader writes the delimited header once using the initialized column names.
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
		return ErrMissingColumnNames
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

func (w *DelimitedWriter) WriteValues(columnNames []string, values []spanner.GenericColumnValue) error {
	if err := w.initOrValidateColumnNames(columnNames); err != nil {
		return err
	}
	return w.WriteGCVs(values)
}

// WriteStructValues writes one row from structpb values using the field-type schema
// registered by [WithRowType], [WithMetadata], or [PrepareRowType].
func (w *DelimitedWriter) WriteStructValues(values []*structpb.Value) error {
	gcvs, err := gcvsFromStructValues(w.schema.types, values)
	if err != nil {
		return err
	}
	return w.WriteGCVs(gcvs)
}

func (w *DelimitedWriter) WriteGCVs(values []spanner.GenericColumnValue) error {
	csvWriter, err := w.csvWriter()
	if err != nil {
		return err
	}
	if len(w.schema.names) == 0 {
		return ErrMissingColumnNames
	}

	formattedValues, err := spanvalue.FormatRowColumns(w.formatter(), w.schema.names, values)
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
	return nil
}

func (w *DelimitedWriter) setRowType(rowType *sppb.StructType) {
	w.schema.applyRowType(rowType)
	w.resolvedColumnNames = nil
}

func (w *DelimitedWriter) setColumnNames(names []string) {
	w.schema.applyNamesOnly(names)
	w.resolvedColumnNames = nil
}

func (w *DelimitedWriter) initOrValidateColumnNames(columnNames []string) error {
	initialized := len(w.schema.names) == 0
	if err := initOrValidateColumnNames(&w.schema.names, columnNames); err != nil {
		return err
	}
	if initialized && len(w.schema.names) > 0 {
		w.resolvedColumnNames = nil
	}
	return nil
}

func (w *DelimitedWriter) formatter() *spanvalue.FormatConfig {
	if w.Formatter != nil {
		return w.Formatter
	}
	return spanvalue.SimpleFormatConfig()
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

// Flush flushes buffered delimited data to the underlying writer. It does not
// close the underlying writer.
func (w *DelimitedWriter) Flush() error {
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
type JSONLWriter struct {
	Formatter *spanvalue.FormatConfig
	// Set before the first write. Once names have been resolved for the current
	// schema, later changes do not retroactively rewrite cached object keys.
	UnnamedFieldNamer spanvalue.UnnamedFieldNamer

	schema              columnSchema
	resolvedColumnNames []string
	marshaledKeys       [][]byte
	out                 io.Writer
}

// NewJSONLWriter returns a JSONL writer configured by options.
func NewJSONLWriter(out io.Writer, options ...JSONLOption) *JSONLWriter {
	w := newJSONLWriter(out)
	for _, opt := range options {
		if opt != nil {
			opt.applyJSONLOption(w)
		}
	}
	return w
}

// NewJSONLWriterWithOptions forwards to [NewJSONLWriter].
//
// Deprecated: Use [NewJSONLWriter] instead.
func NewJSONLWriterWithOptions(out io.Writer, options ...JSONLOption) *JSONLWriter {
	return NewJSONLWriter(out, options...)
}

func newJSONLWriter(out io.Writer) *JSONLWriter {
	return &JSONLWriter{
		Formatter:         spanvalue.JSONFormatConfig(),
		UnnamedFieldNamer: spanvalue.IndexedUnnamedFieldNamer,
		out:               out,
	}
}

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
// Deprecated: Prefer [WithRowType], [WithColumnNames], or [WithMetadata] at
// construction, or [JSONLWriter.PrepareRowType] / [JSONLWriter.PrepareColumnNames].
func (w *JSONLWriter) Prepare(metadata *sppb.ResultSetMetadata) error {
	return w.PrepareRowType(rowTypeFromMetadata(metadata))
}

// PrepareRowType initializes the JSONL schema from a row type before the first row is written.
func (w *JSONLWriter) PrepareRowType(rowType *sppb.StructType) error {
	columnNames, err := prepareRowType(rowType)
	if err != nil {
		return err
	}
	if err := w.initOrValidateColumnNames(columnNames); err != nil {
		return err
	}
	w.setRowType(rowType)
	return nil
}

// PrepareColumnNames initializes the JSONL schema from column names before the first row is written.
func (w *JSONLWriter) PrepareColumnNames(names []string) error {
	if len(names) == 0 {
		return ErrMissingColumnNames
	}
	if err := w.initOrValidateColumnNames(names); err != nil {
		return err
	}
	w.setColumnNames(names)
	return nil
}

func (w *JSONLWriter) WriteValues(columnNames []string, values []spanner.GenericColumnValue) error {
	if err := w.initOrValidateColumnNames(columnNames); err != nil {
		return err
	}
	return w.WriteGCVs(values)
}

// WriteStructValues writes one row from structpb values using the field-type schema
// registered by [WithRowType], [WithMetadata], or [PrepareRowType].
func (w *JSONLWriter) WriteStructValues(values []*structpb.Value) error {
	gcvs, err := gcvsFromStructValues(w.schema.types, values)
	if err != nil {
		return err
	}
	return w.WriteGCVs(gcvs)
}

func (w *JSONLWriter) WriteGCVs(values []spanner.GenericColumnValue) error {
	if w.out == nil {
		return ErrNilOutputWriter
	}
	if len(w.schema.names) == 0 {
		return ErrMissingColumnNames
	}
	formattedValues, err := spanvalue.FormatRowColumns(w.formatter(), w.schema.names, values)
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
	w.schema.applyNamesOnly(names)
	w.resolvedColumnNames = nil
	w.marshaledKeys = nil
}

func (w *JSONLWriter) initOrValidateColumnNames(columnNames []string) error {
	initialized := len(w.schema.names) == 0
	if err := initOrValidateColumnNames(&w.schema.names, columnNames); err != nil {
		return err
	}
	if initialized && len(w.schema.names) > 0 {
		w.resolvedColumnNames = nil
		w.marshaledKeys = nil
	}
	return nil
}

func (w *JSONLWriter) formatter() *spanvalue.FormatConfig {
	if w.Formatter != nil {
		return w.Formatter
	}
	return spanvalue.JSONFormatConfig()
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

// SQLInsertWriter writes rows as GoogleSQL INSERT statements.
type SQLInsertWriter struct {
	Table     string
	Formatter *spanvalue.FormatConfig

	insertKind        SQLInsertKind
	schema            columnSchema
	quotedColumnNames string
	quotedTable       string
	quotedTableInput  string
	out               io.Writer
}

// NewSQLInsertWriter returns a SQL INSERT writer configured by options.
func NewSQLInsertWriter(out io.Writer, table string, options ...SQLInsertOption) *SQLInsertWriter {
	w := newSQLInsertWriter(out, table)
	for _, opt := range options {
		if opt != nil {
			opt.applySQLInsertOption(w)
		}
	}
	return w
}

// NewSQLInsertWriterWithOptions forwards to [NewSQLInsertWriter].
//
// Deprecated: Use [NewSQLInsertWriter] instead.
func NewSQLInsertWriterWithOptions(out io.Writer, table string, options ...SQLInsertOption) *SQLInsertWriter {
	return NewSQLInsertWriter(out, table, options...)
}

func newSQLInsertWriter(out io.Writer, table string) *SQLInsertWriter {
	return &SQLInsertWriter{
		Table:     table,
		Formatter: spanvalue.LiteralFormatConfig(),
		out:       out,
	}
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
// Deprecated: Prefer [WithRowType], [WithColumnNames], or [WithMetadata] at
// construction, or [SQLInsertWriter.PrepareRowType] / [SQLInsertWriter.PrepareColumnNames].
func (w *SQLInsertWriter) Prepare(metadata *sppb.ResultSetMetadata) error {
	return w.PrepareRowType(rowTypeFromMetadata(metadata))
}

// PrepareRowType initializes the SQL INSERT schema from a row type before the first row is written.
func (w *SQLInsertWriter) PrepareRowType(rowType *sppb.StructType) error {
	columnNames, err := prepareRowType(rowType)
	if err != nil {
		return err
	}
	if _, err := w.initOrValidateQuotedColumns(columnNames); err != nil {
		return err
	}
	w.schema.names = columnNamesFromRowType(rowType)
	w.schema.types = fieldTypesFromRowType(rowType)
	return nil
}

// PrepareColumnNames initializes the SQL INSERT schema from column names before the first row is written.
func (w *SQLInsertWriter) PrepareColumnNames(names []string) error {
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
	quotedColumns, err := w.initOrValidateQuotedColumns(nil)
	if err != nil {
		return err
	}
	return w.writeGCVs(values, quotedColumns)
}

// Flush finalizes SQL INSERT output. SQLInsertWriter is unbuffered, so this is
// a no-op.
func (w *SQLInsertWriter) Flush() error {
	return nil
}

func (w *SQLInsertWriter) writeGCVs(values []spanner.GenericColumnValue, quotedColumns string) error {
	if w.out == nil {
		return ErrNilOutputWriter
	}
	if w.Table == "" {
		return ErrEmptyTableName
	}
	quotedTable, err := w.quotedQualifiedTable()
	if err != nil {
		return err
	}
	formattedValues, err := spanvalue.FormatRowColumns(w.formatter(), w.schema.names, values)
	if err != nil {
		return err
	}
	prefix := w.insertKind.String()
	if _, err := fmt.Fprintf(w.out, "%s INTO %s (%s) VALUES (", prefix, quotedTable, quotedColumns); err != nil {
		return err
	}
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
	_, err = io.WriteString(w.out, ");\n")
	return err
}

func (w *SQLInsertWriter) setRowType(rowType *sppb.StructType) {
	w.schema.applyRowType(rowType)
	w.quotedColumnNames = ""
}

func (w *SQLInsertWriter) setColumnNames(names []string) {
	w.schema.applyNamesOnly(names)
	w.quotedColumnNames = ""
}

func (w *SQLInsertWriter) formatter() *spanvalue.FormatConfig {
	if w.Formatter != nil {
		return w.Formatter
	}
	return spanvalue.LiteralFormatConfig()
}

func (w *SQLInsertWriter) initOrValidateQuotedColumns(columnNames []string) (string, error) {
	if len(columnNames) == 0 && w.quotedColumnNames != "" {
		return w.quotedColumnNames, nil
	}
	names, err := validatedColumnNames(w.schema.names, columnNames)
	if err != nil {
		return "", err
	}
	quotedColumns, err := quoteIdentifiers(names)
	if err != nil {
		return "", err
	}
	if len(w.schema.names) == 0 {
		w.schema.names = names
	}
	w.quotedColumnNames = strings.Join(quotedColumns, ", ")
	return w.quotedColumnNames, nil
}

func (w *SQLInsertWriter) quotedQualifiedTable() (string, error) {
	if w.quotedTable != "" && w.quotedTableInput == w.Table {
		return w.quotedTable, nil
	}
	quotedTable, err := quoteQualifiedIdentifier(w.Table)
	if err != nil {
		return "", err
	}
	w.quotedTable = quotedTable
	w.quotedTableInput = w.Table
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

func prepareRowType(rowType *sppb.StructType) ([]string, error) {
	columnNames := columnNamesFromRowType(rowType)
	if len(columnNames) == 0 {
		return nil, ErrMissingColumnNames
	}
	return columnNames, nil
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

// initOrValidateColumnNames initializes dst from the first non-empty
// columnNames slice it sees. Once initialized, subsequent non-empty inputs must
// match exactly; empty inputs are accepted only after initialization.
func initOrValidateColumnNames(dst *[]string, columnNames []string) error {
	validated, err := validatedColumnNames(*dst, columnNames)
	if err != nil {
		return err
	}
	if len(*dst) == 0 {
		*dst = validated
	}
	return nil
}

func validatedColumnNames(existing []string, columnNames []string) ([]string, error) {
	if len(existing) == 0 {
		if len(columnNames) == 0 {
			return nil, ErrMissingColumnNames
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

// quoteIdentifiers quotes GoogleSQL identifiers and rejects empty names.
func quoteIdentifiers(names []string) ([]string, error) {
	quoted := make([]string, len(names))
	for i, name := range names {
		if name == "" {
			return nil, ErrEmptyColumnName
		}
		quoted[i] = spanvalue.QuoteIdentifier(databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, name)
	}
	return quoted, nil
}

// quoteQualifiedIdentifier quotes each identifier segment in a dotted path.
func quoteQualifiedIdentifier(name string) (string, error) {
	parts := strings.Split(name, ".")
	for i, part := range parts {
		if part == "" {
			return "", fmt.Errorf("%w: qualified table name contains empty segment", ErrEmptyTableName)
		}
		parts[i] = spanvalue.QuoteIdentifier(databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, part)
	}
	return strings.Join(parts, "."), nil
}
