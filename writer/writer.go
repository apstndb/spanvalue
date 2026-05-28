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
// Use Writer when an adapter only needs row streaming, and FlushWriter when it
// owns the full write lifecycle. DelimitedWriter, JSONLWriter, and SQLInsertWriter
// implement FlushWriter. If an adapter exposes a Close method, that Close
// method should call Flush; Flush does not close the underlying io.Writer.
//
// # Primary API
//
// [DelimitedWriter], [NewDelimitedWriter], [NewCSVWriter], [JSONLWriter],
// [NewJSONLWriter], [SQLInsertWriter], and [NewSQLInsertWriter] stream rows.
// [NewDelimitedWriterWithOptions], [NewJSONLWriterWithOptions], and
// [NewSQLInsertWriterWithOptions] accept explicit options such as [WithMetadata]
// and [WithFormatter]. Each writer's Prepare method initializes schema from
// result-set metadata (for example [DelimitedWriter.Prepare]).
// [RowData], [FormatDelimitedRow], and [FormatJSONLRow] support one-row paths.
//
// # Compatibility API
//
// [CSVWriter] is a type alias for [DelimitedWriter]. [DelimitedWriter.Comma]
// and a zero delimiter passed to [NewDelimitedWriter] select comma for
// compatibility; prefer [NewCSVWriter] or [NewDelimitedWriter] with [Comma].
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
	// ErrDelimiterAfterWrite reports that DelimitedWriter delimiter changed after the underlying CSV writer was initialized.
	ErrDelimiterAfterWrite = errors.New("delimiter changed after writer initialization")
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

// Option configures any writer type created by a WithOptions constructor.
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

// DelimitedOption configures a DelimitedWriter created by NewDelimitedWriterWithOptions.
type DelimitedOption interface {
	applyDelimitedOption(*DelimitedWriter)
}

// JSONLOption configures a JSONLWriter created by NewJSONLWriterWithOptions.
type JSONLOption interface {
	applyJSONLOption(*JSONLWriter)
}

// SQLInsertOption configures a SQLInsertWriter created by NewSQLInsertWriterWithOptions.
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

// WithMetadata initializes a writer schema from result-set metadata.
func WithMetadata(metadata *sppb.ResultSetMetadata) Option {
	return metadataOption{metadata: metadata}
}

func (o metadataOption) applyDelimitedOption(w *DelimitedWriter) {
	w.setMetadata(o.metadata)
}

func (o metadataOption) applyJSONLOption(w *JSONLWriter) {
	w.setMetadata(o.metadata)
}

func (o metadataOption) applySQLInsertOption(w *SQLInsertWriter) {
	w.setMetadata(o.metadata)
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

// DelimitedWriter writes rows as CSV-style delimited text. Call Flush after the final write.
type DelimitedWriter struct {
	Formatter *spanvalue.FormatConfig
	Header    bool
	// Comma is the field delimiter. The zero value selects Comma for
	// compatibility. Set it before the first write; use '\t' for TSV output.
	// Any non-zero delimiter must be a valid encoding/csv delimiter: a valid
	// rune other than '"', '\r', '\n', or utf8.RuneError.
	//
	// Deprecated: prefer the delimiter argument to NewDelimitedWriter or
	// NewDelimitedWriterWithOptions.
	Comma rune
	// Set before the first write. Once names have been resolved for the current
	// schema, later changes do not retroactively rewrite cached header names.
	UnnamedFieldNamer spanvalue.UnnamedFieldNamer

	columnNames         []string
	resolvedColumnNames []string
	out                 io.Writer
	writer              *csv.Writer
	delimiter           rune
	wroteHeader         bool
	wroteData           bool
}

// CSVWriter is a compatibility alias for DelimitedWriter.
//
// Deprecated: use DelimitedWriter.
type CSVWriter = DelimitedWriter

// NewCSVWriter returns a CSV writer optionally initialized from result-set metadata.
// It is a thin helper for NewDelimitedWriter(out, Comma, metadata...).
func NewCSVWriter(out io.Writer, metadata ...*sppb.ResultSetMetadata) *DelimitedWriter {
	return NewDelimitedWriter(out, Comma, metadata...)
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
// delimiter. Pass Comma for CSV output or '\t' for TSV output. A zero delimiter
// is accepted for compatibility and is treated as Comma.
func NewDelimitedWriter(out io.Writer, delimiter rune, metadata ...*sppb.ResultSetMetadata) *DelimitedWriter {
	w := newDelimitedWriter(out)
	w.Comma = delimiter
	w.setMetadata(firstMetadata(metadata))
	return w
}

// NewDelimitedWriterWithOptions returns a CSV-style writer using delimiter as
// the field delimiter and configured by options. Pass Comma for CSV output or
// '\t' for TSV output. A zero delimiter is accepted for compatibility and is
// treated as Comma.
func NewDelimitedWriterWithOptions(out io.Writer, delimiter rune, options ...DelimitedOption) *DelimitedWriter {
	w := newDelimitedWriter(out)
	w.Comma = delimiter
	for _, opt := range options {
		if opt != nil {
			opt.applyDelimitedOption(w)
		}
	}
	return w
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
func (w *DelimitedWriter) Prepare(metadata *sppb.ResultSetMetadata) error {
	columnNames, err := prepareColumnNames(metadata)
	if err != nil {
		return err
	}
	return w.initOrValidateColumnNames(columnNames)
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
	if len(w.columnNames) == 0 {
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

func (w *DelimitedWriter) WriteGCVs(values []spanner.GenericColumnValue) error {
	csvWriter, err := w.csvWriter()
	if err != nil {
		return err
	}
	if len(w.columnNames) == 0 {
		return ErrMissingColumnNames
	}

	formattedValues, err := spanvalue.FormatRowColumns(w.formatter(), w.columnNames, values)
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

func (w *DelimitedWriter) setMetadata(metadata *sppb.ResultSetMetadata) {
	w.columnNames = metadataColumnNames(metadata)
	w.resolvedColumnNames = nil
}

func (w *DelimitedWriter) initOrValidateColumnNames(columnNames []string) error {
	initialized := len(w.columnNames) == 0
	if err := initOrValidateColumnNames(&w.columnNames, columnNames); err != nil {
		return err
	}
	if initialized && len(w.columnNames) > 0 {
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
	delimiter := w.effectiveDelimiter()
	if w.writer != nil {
		if w.delimiter != delimiter {
			return nil, ErrDelimiterAfterWrite
		}
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

func (w *DelimitedWriter) effectiveDelimiter() rune {
	return effectiveDelimiter(w.Comma)
}

func effectiveDelimiter(delimiter rune) rune {
	if delimiter == 0 {
		return Comma
	}
	return delimiter
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
	if len(w.resolvedColumnNames) != 0 || len(w.columnNames) == 0 {
		return w.resolvedColumnNames, nil
	}
	if w.UnnamedFieldNamer == nil {
		return w.columnNames, nil
	}
	resolvedNames, err := internal.ResolveColumnNames(w.columnNames, w.UnnamedFieldNamer)
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

	columnNames         []string
	resolvedColumnNames []string
	marshaledKeys       [][]byte
	out                 io.Writer
}

// NewJSONLWriter returns a JSONL writer optionally initialized from result-set metadata.
func NewJSONLWriter(out io.Writer, metadata ...*sppb.ResultSetMetadata) *JSONLWriter {
	w := newJSONLWriter(out)
	w.setMetadata(firstMetadata(metadata))
	return w
}

// NewJSONLWriterWithOptions returns a JSONL writer configured by options.
func NewJSONLWriterWithOptions(out io.Writer, options ...JSONLOption) *JSONLWriter {
	w := newJSONLWriter(out)
	for _, opt := range options {
		if opt != nil {
			opt.applyJSONLOption(w)
		}
	}
	return w
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
func (w *JSONLWriter) Prepare(metadata *sppb.ResultSetMetadata) error {
	columnNames, err := prepareColumnNames(metadata)
	if err != nil {
		return err
	}
	return w.initOrValidateColumnNames(columnNames)
}

func (w *JSONLWriter) WriteValues(columnNames []string, values []spanner.GenericColumnValue) error {
	if err := w.initOrValidateColumnNames(columnNames); err != nil {
		return err
	}
	return w.WriteGCVs(values)
}

func (w *JSONLWriter) WriteGCVs(values []spanner.GenericColumnValue) error {
	if w.out == nil {
		return ErrNilOutputWriter
	}
	if len(w.columnNames) == 0 {
		return ErrMissingColumnNames
	}
	formattedValues, err := spanvalue.FormatRowColumns(w.formatter(), w.columnNames, values)
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

func (w *JSONLWriter) setMetadata(metadata *sppb.ResultSetMetadata) {
	w.columnNames = metadataColumnNames(metadata)
	w.resolvedColumnNames = nil
	w.marshaledKeys = nil
}

func (w *JSONLWriter) initOrValidateColumnNames(columnNames []string) error {
	initialized := len(w.columnNames) == 0
	if err := initOrValidateColumnNames(&w.columnNames, columnNames); err != nil {
		return err
	}
	if initialized && len(w.columnNames) > 0 {
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
	if len(w.resolvedColumnNames) != 0 || len(w.columnNames) == 0 {
		return w.resolvedColumnNames, nil
	}
	if w.UnnamedFieldNamer == nil {
		return w.columnNames, nil
	}
	resolvedNames, err := internal.ResolveColumnNames(w.columnNames, w.UnnamedFieldNamer)
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

	columnNames       []string
	quotedColumnNames string
	quotedTable       string
	quotedTableInput  string
	out               io.Writer
}

// NewSQLInsertWriter returns a SQL INSERT writer optionally initialized from result-set metadata.
func NewSQLInsertWriter(out io.Writer, table string, metadata ...*sppb.ResultSetMetadata) *SQLInsertWriter {
	w := newSQLInsertWriter(out, table)
	w.setMetadata(firstMetadata(metadata))
	return w
}

// NewSQLInsertWriterWithOptions returns a SQL INSERT writer configured by options.
func NewSQLInsertWriterWithOptions(out io.Writer, table string, options ...SQLInsertOption) *SQLInsertWriter {
	w := newSQLInsertWriter(out, table)
	for _, opt := range options {
		if opt != nil {
			opt.applySQLInsertOption(w)
		}
	}
	return w
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
func (w *SQLInsertWriter) Prepare(metadata *sppb.ResultSetMetadata) error {
	columnNames, err := prepareColumnNames(metadata)
	if err != nil {
		return err
	}
	_, err = w.initOrValidateQuotedColumns(columnNames)
	return err
}

func (w *SQLInsertWriter) WriteValues(columnNames []string, values []spanner.GenericColumnValue) error {
	quotedColumns, err := w.initOrValidateQuotedColumns(columnNames)
	if err != nil {
		return err
	}
	return w.writeGCVs(values, quotedColumns)
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
	formattedValues, err := spanvalue.FormatRowColumns(w.formatter(), w.columnNames, values)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w.out, "INSERT INTO %s (%s) VALUES (", quotedTable, quotedColumns); err != nil {
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

func (w *SQLInsertWriter) setMetadata(metadata *sppb.ResultSetMetadata) {
	w.columnNames = metadataColumnNames(metadata)
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
	names, err := validatedColumnNames(w.columnNames, columnNames)
	if err != nil {
		return "", err
	}
	quotedColumns, err := quoteIdentifiers(names)
	if err != nil {
		return "", err
	}
	if len(w.columnNames) == 0 {
		w.columnNames = names
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
// trailing newline. Pass Comma for CSV output. A zero delimiter is accepted for
// compatibility and is treated as Comma.
func FormatDelimitedRow(fc *spanvalue.FormatConfig, row *spanner.Row, delimiter rune) (string, error) {
	columnNames, values, err := RowData(row)
	if err != nil {
		return "", err
	}
	return FormatDelimitedValues(fc, columnNames, values, delimiter)
}

// FormatDelimitedValues formats one row represented as column names plus GCV
// values as a CSV-style delimited record without a trailing newline. Pass Comma
// for CSV output. A zero delimiter is accepted for compatibility and is treated
// as Comma.
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
	delimiter = effectiveDelimiter(delimiter)
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

// firstMetadata keeps the constructors backward-compatible while allowing an
// optional single metadata argument for eager schema initialization.
func firstMetadata(metadata []*sppb.ResultSetMetadata) *sppb.ResultSetMetadata {
	if len(metadata) == 0 {
		return nil
	}
	return metadata[0]
}

func prepareColumnNames(metadata *sppb.ResultSetMetadata) ([]string, error) {
	columnNames := metadataColumnNames(metadata)
	if len(columnNames) == 0 {
		return nil, ErrMissingColumnNames
	}
	return columnNames, nil
}

func metadataColumnNames(metadata *sppb.ResultSetMetadata) []string {
	if metadata == nil || metadata.GetRowType() == nil {
		return nil
	}
	fields := metadata.GetRowType().GetFields()
	names := make([]string, len(fields))
	for i, field := range fields {
		names[i] = field.GetName()
	}
	return names
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
