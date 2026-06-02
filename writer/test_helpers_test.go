package writer

import (
	"io"
	"testing"
)

func mustNewDelimitedWriter(t *testing.T, out io.Writer, delimiter rune, options ...DelimitedOption) *DelimitedWriter {
	t.Helper()
	w, err := NewDelimitedWriter(out, delimiter, options...)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func mustNewCSVWriter(t *testing.T, out io.Writer, opts ...DelimitedOption) *DelimitedWriter {
	t.Helper()
	w, err := NewCSVWriter(out, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func mustNewJSONLWriter(t *testing.T, out io.Writer, options ...JSONLOption) *JSONLWriter {
	t.Helper()
	w, err := NewJSONLWriter(out, options...)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func mustNewSQLInsertWriter(t *testing.T, out io.Writer, table string, options ...SQLInsertOption) *SQLInsertWriter {
	t.Helper()
	w, err := NewSQLInsertWriter(out, table, options...)
	if err != nil {
		t.Fatal(err)
	}
	return w
}
