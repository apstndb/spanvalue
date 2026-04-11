// Package writer provides small streaming helpers for exporting Spanner rows
// using spanvalue formatters.
package writer

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"

	"github.com/apstndb/spanvalue"
	"github.com/apstndb/spanvalue/internal"
)

var (
	// ErrEmptyTableName reports that SQLInsertWriter.Table is empty.
	ErrEmptyTableName = errors.New("empty table name")
	// ErrEmptyColumnName reports that a SQL writer received an empty column name.
	ErrEmptyColumnName = errors.New("empty column name")
	// ErrNilOutputWriter reports that a writer was constructed without an output.
	ErrNilOutputWriter = errors.New("nil output writer")
	// ErrNilRow reports that WriteRow was called with a nil row.
	ErrNilRow = errors.New("nil row")
	// ErrMissingColumnNames reports that writing values requires initialized column names.
	ErrMissingColumnNames = errors.New("missing column names")
	// ErrColumnNamesMismatch reports that provided column names differ from initialized schema.
	ErrColumnNamesMismatch = errors.New("column names mismatch")
)

// Writer writes rows to an output stream.
type Writer interface {
	WriteRow(row *spanner.Row) error
}

// CSVWriter writes rows as CSV. Call Flush after the final write.
type CSVWriter struct {
	Formatter *spanvalue.FormatConfig
	Header    bool
	// Set before the first write. Once names have been resolved for the current
	// schema, later changes do not retroactively rewrite cached header names.
	UnnamedFieldNamer spanvalue.UnnamedFieldNamer

	columnNames         []string
	resolvedColumnNames []string
	out                 io.Writer
	writer              *csv.Writer
	wroteHeader         bool
}

// NewCSVWriter returns a CSV writer optionally initialized from result-set metadata.
func NewCSVWriter(out io.Writer, metadata ...*sppb.ResultSetMetadata) *CSVWriter {
	w := &CSVWriter{
		Formatter:         spanvalue.SimpleFormatConfig(),
		Header:            true,
		UnnamedFieldNamer: spanvalue.IndexedUnnamedFieldNamer,
		out:               out,
	}
	w.setMetadata(firstMetadata(metadata))
	return w
}

// WriteRow writes one CSV row, initializing the schema from row metadata if needed.
func (w *CSVWriter) WriteRow(row *spanner.Row) error {
	columnNames, values, err := rowData(row)
	if err != nil {
		return err
	}
	return w.WriteValues(columnNames, values)
}

// WriteHeader writes the CSV header once using the initialized column names.
func (w *CSVWriter) WriteHeader() error {
	if w.wroteHeader {
		return nil
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

func (w *CSVWriter) WriteValues(columnNames []string, values []spanner.GenericColumnValue) error {
	if err := w.initOrValidateColumnNames(columnNames); err != nil {
		return err
	}
	return w.WriteGCVs(values)
}

func (w *CSVWriter) WriteGCVs(values []spanner.GenericColumnValue) error {
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
	return nil
}

func (w *CSVWriter) setMetadata(metadata *sppb.ResultSetMetadata) {
	w.columnNames = metadataColumnNames(metadata)
	w.resolvedColumnNames = nil
}

func (w *CSVWriter) initOrValidateColumnNames(columnNames []string) error {
	initialized := len(w.columnNames) == 0
	if err := initOrValidateColumnNames(&w.columnNames, columnNames); err != nil {
		return err
	}
	if initialized && len(w.columnNames) > 0 {
		w.resolvedColumnNames = nil
	}
	return nil
}

func (w *CSVWriter) formatter() *spanvalue.FormatConfig {
	if w.Formatter != nil {
		return w.Formatter
	}
	return spanvalue.SimpleFormatConfig()
}

func (w *CSVWriter) csvWriter() (*csv.Writer, error) {
	if w.writer != nil {
		return w.writer, nil
	}
	if w.out == nil {
		return nil, ErrNilOutputWriter
	}
	w.writer = csv.NewWriter(w.out)
	return w.writer, nil
}

// Flush flushes buffered CSV data to the underlying writer.
func (w *CSVWriter) Flush() error {
	if w.writer == nil {
		return nil
	}
	w.writer.Flush()
	return w.writer.Error()
}

func (w *CSVWriter) resolvedNames() ([]string, error) {
	if len(w.resolvedColumnNames) != 0 || len(w.columnNames) == 0 || w.UnnamedFieldNamer == nil {
		if w.UnnamedFieldNamer == nil {
			return w.columnNames, nil
		}
		return w.resolvedColumnNames, nil
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
	out                 io.Writer
}

// NewJSONLWriter returns a JSONL writer optionally initialized from result-set metadata.
func NewJSONLWriter(out io.Writer, metadata ...*sppb.ResultSetMetadata) *JSONLWriter {
	w := &JSONLWriter{
		Formatter:         spanvalue.JSONFormatConfig(),
		UnnamedFieldNamer: spanvalue.IndexedUnnamedFieldNamer,
		out:               out,
	}
	w.setMetadata(firstMetadata(metadata))
	return w
}

func (w *JSONLWriter) WriteRow(row *spanner.Row) error {
	columnNames, values, err := rowData(row)
	if err != nil {
		return err
	}
	return w.WriteValues(columnNames, values)
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
	s, err := internal.AssembleResolvedJSONObject(resolvedNames, formattedValues)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(w.out, s)
	return err
}

func (w *JSONLWriter) setMetadata(metadata *sppb.ResultSetMetadata) {
	w.columnNames = metadataColumnNames(metadata)
	w.resolvedColumnNames = nil
}

func (w *JSONLWriter) initOrValidateColumnNames(columnNames []string) error {
	initialized := len(w.columnNames) == 0
	if err := initOrValidateColumnNames(&w.columnNames, columnNames); err != nil {
		return err
	}
	if initialized && len(w.columnNames) > 0 {
		w.resolvedColumnNames = nil
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
	if len(w.resolvedColumnNames) != 0 || len(w.columnNames) == 0 || w.UnnamedFieldNamer == nil {
		if w.UnnamedFieldNamer == nil {
			return w.columnNames, nil
		}
		return w.resolvedColumnNames, nil
	}
	resolvedNames, err := internal.ResolveColumnNames(w.columnNames, w.UnnamedFieldNamer)
	if err != nil {
		return nil, err
	}
	w.resolvedColumnNames = resolvedNames
	return resolvedNames, nil
}

// SQLInsertWriter writes rows as GoogleSQL INSERT statements.
type SQLInsertWriter struct {
	Table     string
	Formatter *spanvalue.FormatConfig

	columnNames       []string
	quotedColumnNames string
	out               io.Writer
}

// NewSQLInsertWriter returns a SQL INSERT writer optionally initialized from result-set metadata.
func NewSQLInsertWriter(out io.Writer, table string, metadata ...*sppb.ResultSetMetadata) *SQLInsertWriter {
	w := &SQLInsertWriter{
		Table:     table,
		Formatter: spanvalue.LiteralFormatConfig(),
		out:       out,
	}
	w.setMetadata(firstMetadata(metadata))
	return w
}

func (w *SQLInsertWriter) WriteRow(row *spanner.Row) error {
	columnNames, values, err := rowData(row)
	if err != nil {
		return err
	}
	return w.WriteValues(columnNames, values)
}

func (w *SQLInsertWriter) WriteValues(columnNames []string, values []spanner.GenericColumnValue) error {
	if len(w.columnNames) == 0 && len(columnNames) > 0 {
		if _, err := quoteIdentifiers(columnNames); err != nil {
			return err
		}
	}
	if err := w.initOrValidateColumnNames(columnNames); err != nil {
		return err
	}
	return w.WriteGCVs(values)
}

func (w *SQLInsertWriter) WriteGCVs(values []spanner.GenericColumnValue) error {
	if w.out == nil {
		return ErrNilOutputWriter
	}
	if w.Table == "" {
		return ErrEmptyTableName
	}
	if len(w.columnNames) == 0 {
		return ErrMissingColumnNames
	}

	quotedColumns, err := w.quotedColumns()
	if err != nil {
		return err
	}

	formattedValues, err := spanvalue.FormatRowColumns(w.formatter(), w.columnNames, values)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(
		w.out,
		"INSERT INTO %s (%s) VALUES (%s);\n",
		quoteIdentifier(w.Table),
		quotedColumns,
		strings.Join(formattedValues, ", "),
	)
	return err
}

func (w *SQLInsertWriter) setMetadata(metadata *sppb.ResultSetMetadata) {
	w.columnNames = metadataColumnNames(metadata)
	w.quotedColumnNames = ""
}

func (w *SQLInsertWriter) initOrValidateColumnNames(columnNames []string) error {
	initialized := len(w.columnNames) == 0
	if err := initOrValidateColumnNames(&w.columnNames, columnNames); err != nil {
		return err
	}
	if initialized && len(w.columnNames) > 0 {
		w.quotedColumnNames = ""
	}
	return nil
}

func (w *SQLInsertWriter) formatter() *spanvalue.FormatConfig {
	if w.Formatter != nil {
		return w.Formatter
	}
	return spanvalue.LiteralFormatConfig()
}

func (w *SQLInsertWriter) quotedColumns() (string, error) {
	if w.quotedColumnNames != "" {
		return w.quotedColumnNames, nil
	}
	quotedColumns, err := quoteIdentifiers(w.columnNames)
	if err != nil {
		return "", err
	}
	w.quotedColumnNames = strings.Join(quotedColumns, ", ")
	return w.quotedColumnNames, nil
}

// rowData extracts column names and GenericColumnValue cells from row.
func rowData(row *spanner.Row) ([]string, []spanner.GenericColumnValue, error) {
	if row == nil {
		return nil, nil, ErrNilRow
	}
	values := make([]spanner.GenericColumnValue, row.Size())
	for i := range values {
		if err := row.Column(i, &values[i]); err != nil {
			return nil, nil, err
		}
	}
	return row.ColumnNames(), values, nil
}

// firstMetadata keeps the constructors backward-compatible while allowing an
// optional single metadata argument for eager schema initialization.
func firstMetadata(metadata []*sppb.ResultSetMetadata) *sppb.ResultSetMetadata {
	if len(metadata) == 0 {
		return nil
	}
	return metadata[0]
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
	if len(*dst) == 0 {
		if len(columnNames) == 0 {
			return ErrMissingColumnNames
		}
		*dst = slices.Clone(columnNames)
		return nil
	}
	if len(columnNames) == 0 {
		return nil
	}
	if !slices.Equal(*dst, columnNames) {
		return fmt.Errorf("%w: got %v want %v", ErrColumnNamesMismatch, columnNames, *dst)
	}
	return nil
}

// quoteIdentifiers quotes GoogleSQL identifiers and rejects empty names.
func quoteIdentifiers(names []string) ([]string, error) {
	quoted := make([]string, len(names))
	for i, name := range names {
		if name == "" {
			return nil, ErrEmptyColumnName
		}
		quoted[i] = quoteIdentifier(name)
	}
	return quoted, nil
}

// quoteIdentifier quotes a GoogleSQL identifier, escaping backticks by doubling them.
func quoteIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}
