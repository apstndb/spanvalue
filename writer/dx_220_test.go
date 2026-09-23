package writer

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"cloud.google.com/go/spanner"

	"github.com/apstndb/spanvalue/gcvctor"
)

func TestTabDelimiter(t *testing.T) {
	t.Parallel()
	if Tab != '\t' {
		t.Fatalf("Tab = %q, want tab", Tab)
	}
}

func TestEmptySchemaMismatchHintsNilMetadata(t *testing.T) {
	t.Parallel()

	w := mustNewDelimitedWriter(t, &bytes.Buffer{}, Tab)
	if err := w.PrepareRowType(emptyRowType()); err != nil {
		t.Fatal(err)
	}
	err := w.WriteValues([]string{"id"}, []spanner.GenericColumnValue{gcvctor.Int64Value(1)})
	if !errors.Is(err, ErrColumnNamesMismatch) || !strings.Contains(err.Error(), "nil metadata registered before the first Next") {
		t.Fatalf("error = %v, want nil-metadata hint", err)
	}
}

func TestOmitRowsReadSurvivesLaterWithWriteRow(t *testing.T) {
	t.Parallel()

	hooks := WithRowOrdinal(RowIteratorHooks{}, &RowOrdinal{}).WithWriteRow(func(*spanner.Row) error {
		return nil
	})
	if !hooks.omitRowsRead {
		t.Fatal("later WithWriteRow cleared omitRowsRead")
	}
}
