// Package writer provides small streaming helpers for exporting Spanner rows
// using spanvalue formatters.
package writer

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strings"

	"cloud.google.com/go/spanner"

	"github.com/apstndb/spanvalue"
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
)

// Writer writes rows or column/value pairs to an output stream.
type Writer interface {
	WriteRow(row *spanner.Row) error
	WriteValues(columnNames []string, values []spanner.GenericColumnValue) error
}

// CSVWriter writes rows as CSV.
type CSVWriter struct {
	Formatter         *spanvalue.FormatConfig
	Header            bool
	UnnamedFieldNamer spanvalue.UnnamedFieldNamer

	out         io.Writer
	writer      *csv.Writer
	wroteHeader bool
}

func NewCSVWriter(out io.Writer) *CSVWriter {
	return &CSVWriter{
		Formatter:         spanvalue.SimpleFormatConfig(),
		Header:            true,
		UnnamedFieldNamer: spanvalue.IndexedUnnamedFieldNamer,
		out:               out,
	}
}

func (w *CSVWriter) WriteRow(row *spanner.Row) error {
	columnNames, values, err := rowData(row)
	if err != nil {
		return err
	}
	return w.WriteValues(columnNames, values)
}

func (w *CSVWriter) WriteValues(columnNames []string, values []spanner.GenericColumnValue) error {
	formattedValues, err := spanvalue.FormatRowColumns(w.formatter(), columnNames, values)
	if err != nil {
		return err
	}

	csvWriter, err := w.csvWriter()
	if err != nil {
		return err
	}

	if w.Header && !w.wroteHeader {
		resolvedNames, err := spanvalue.ResolveColumnNames(columnNames, w.UnnamedFieldNamer)
		if err != nil {
			return err
		}
		if err := csvWriter.Write(resolvedNames); err != nil {
			return err
		}
		w.wroteHeader = true
	}

	if err := csvWriter.Write(formattedValues); err != nil {
		return err
	}
	csvWriter.Flush()
	return csvWriter.Error()
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

// JSONLWriter writes one JSON object per line.
type JSONLWriter struct {
	Formatter         *spanvalue.FormatConfig
	UnnamedFieldNamer spanvalue.UnnamedFieldNamer

	out io.Writer
}

func NewJSONLWriter(out io.Writer) *JSONLWriter {
	return &JSONLWriter{
		Formatter:         spanvalue.JSONFormatConfig(),
		UnnamedFieldNamer: spanvalue.IndexedUnnamedFieldNamer,
		out:               out,
	}
}

func (w *JSONLWriter) WriteRow(row *spanner.Row) error {
	if w.out == nil {
		return ErrNilOutputWriter
	}
	s, err := spanvalue.FormatRowJSONObject(w.formatter(), row, w.UnnamedFieldNamer)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(w.out, s)
	return err
}

func (w *JSONLWriter) WriteValues(columnNames []string, values []spanner.GenericColumnValue) error {
	if w.out == nil {
		return ErrNilOutputWriter
	}
	s, err := spanvalue.FormatRowJSONObjectFromColumns(w.formatter(), columnNames, values, w.UnnamedFieldNamer)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(w.out, s)
	return err
}

func (w *JSONLWriter) formatter() *spanvalue.FormatConfig {
	if w.Formatter != nil {
		return w.Formatter
	}
	return spanvalue.JSONFormatConfig()
}

// SQLInsertWriter writes rows as GoogleSQL INSERT statements.
type SQLInsertWriter struct {
	Table     string
	Formatter *spanvalue.FormatConfig

	out io.Writer
}

func NewSQLInsertWriter(out io.Writer, table string) *SQLInsertWriter {
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

func (w *SQLInsertWriter) WriteValues(columnNames []string, values []spanner.GenericColumnValue) error {
	if w.out == nil {
		return ErrNilOutputWriter
	}
	if w.Table == "" {
		return ErrEmptyTableName
	}

	formattedValues, err := spanvalue.FormatRowColumns(w.formatter(), columnNames, values)
	if err != nil {
		return err
	}

	quotedColumns, err := quoteIdentifiers(columnNames)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintf(
		w.out,
		"INSERT INTO %s (%s) VALUES (%s);\n",
		quoteIdentifier(w.Table),
		strings.Join(quotedColumns, ", "),
		strings.Join(formattedValues, ", "),
	)
	return err
}

func (w *SQLInsertWriter) formatter() *spanvalue.FormatConfig {
	if w.Formatter != nil {
		return w.Formatter
	}
	return spanvalue.LiteralFormatConfig()
}

// rowData extracts column names and GenericColumnValue cells from row.
func rowData(row *spanner.Row) ([]string, []spanner.GenericColumnValue, error) {
	if row == nil {
		return nil, nil, ErrNilRow
	}
	values := make([]spanner.GenericColumnValue, row.Size())
	ptrs := make([]interface{}, len(values))
	for i := range values {
		ptrs[i] = &values[i]
	}
	if err := row.Columns(ptrs...); err != nil {
		return nil, nil, err
	}
	return row.ColumnNames(), values, nil
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
