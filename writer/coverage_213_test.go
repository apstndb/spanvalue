package writer

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"unicode/utf8"

	"cloud.google.com/go/spanner"

	"github.com/apstndb/spanvalue/gcvctor"
)

func TestRunRowIteratorFinishErrorReturnsResult(t *testing.T) {
	t.Parallel()

	md := metadataWithColumnNames("id")
	finishErr := errors.New("finish failed")
	got, err := runRowIterator(&stubRowIterator{md: md}, RowIteratorHooks{
		Finish: func(*RowIteratorResult) error { return finishErr },
	})
	if !errors.Is(err, finishErr) {
		t.Fatalf("error = %v, want finish failed", err)
	}
	if got == nil || got.Metadata != md {
		t.Fatalf("result = %#v, want metadata preserved", got)
	}
}

func TestSQLInsertNegativeBatchSizeIsOne(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := mustNewSQLInsertWriter(t, &out, "users", WithSQLBatchSize(-1))
	for id := int64(1); id <= 2; id++ {
		row := []spanner.GenericColumnValue{gcvctor.Int64Value(id)}
		if err := w.WriteValues([]string{"id"}, row); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	got := out.String()
	if strings.Count(got, "VALUES (1)") != 1 || strings.Count(got, "VALUES (2)") != 1 || strings.Contains(got, "),(") {
		t.Fatalf("output = %q, want two single-row statements", got)
	}
}

func TestInvalidDelimiters(t *testing.T) {
	t.Parallel()

	for _, delim := range []rune{'"', '\r', utf8.RuneError} {
		_, err := NewDelimitedWriter(&bytes.Buffer{}, delim)
		if !errors.Is(err, ErrInvalidDelimiter) {
			t.Fatalf("delimiter %q error = %v, want ErrInvalidDelimiter", delim, err)
		}
	}
}
