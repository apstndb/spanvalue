package dbsqlrows

import (
	"errors"
	"testing"

	"cloud.google.com/go/spanner"

	"github.com/apstndb/spanvalue/gcvctor"
)

var errScan = errors.New("scan failed")

type scanFailFacade struct {
	columns int
	nexts   int
}

func (f *scanFailFacade) next() bool {
	f.nexts++
	return f.nexts == 1
}

func (f *scanFailFacade) nextResultSet() bool { return true }
func (f *scanFailFacade) scan(...any) error   { return errScan }
func (f *scanFailFacade) columnCount() (int, error) {
	return f.columns, nil
}
func (f *scanFailFacade) err() error { return nil }

func TestErrMissingMetadataRow(t *testing.T) {
	t.Parallel()

	_, err := runRows(&stubSQLRows{}, SQLRowsHooks{}, sqlRowsRunConfig{readMetadataPseudoRow: true})
	if !errors.Is(err, ErrMissingMetadataRow) {
		t.Fatalf("error = %v, want ErrMissingMetadataRow", err)
	}
}

func TestScanErrOnMetadataAndData(t *testing.T) {
	t.Parallel()

	md := metadataWithNames("id")

	t.Run("metadata", func(t *testing.T) {
		t.Parallel()
		_, err := runRows(&scanFailFacade{}, SQLRowsHooks{}, sqlRowsRunConfig{readMetadataPseudoRow: true})
		if !errors.Is(err, errScan) {
			t.Fatalf("error = %v, want scan failed", err)
		}
	})

	t.Run("data", func(t *testing.T) {
		t.Parallel()
		_, err := runRows(&scanFailFacade{columns: 1}, SQLRowsHooks{
			WriteDataRow: func([]spanner.GenericColumnValue) error { return nil },
		}, sqlRowsRunConfig{metadata: md})
		if !errors.Is(err, errScan) {
			t.Fatalf("error = %v, want scan failed", err)
		}
	})
}

func TestFlushErrorAndSkippedFlushOnWriteError(t *testing.T) {
	t.Parallel()

	md := metadataWithNames("id")

	t.Run("flush error after success", func(t *testing.T) {
		t.Parallel()
		flushErr := errors.New("flush failed")
		w := &stubGCVWriter{flushErr: flushErr}
		stub := &stubSQLRows{columns: []string{"id"}, resultSets: [][]stubRow{{}}}
		got, err := runRowsWithGCVWriter(stub, w, sqlRowsRunConfig{metadata: md})
		if !errors.Is(err, flushErr) {
			t.Fatalf("error = %v, want flush failed", err)
		}
		if !w.flushed || got == nil || got.RowsRead != 0 {
			t.Fatalf("flushed=%v result=%#v", w.flushed, got)
		}
	})

	t.Run("write error skips flush", func(t *testing.T) {
		t.Parallel()
		writeErr := errors.New("write failed")
		w := &stubGCVWriter{writeErr: writeErr}
		stub := &stubSQLRows{
			columns:    []string{"id"},
			resultSets: [][]stubRow{{{values: []any{gcvctor.Int64Value(1)}}}},
		}
		_, err := runRowsWithGCVWriter(stub, w, sqlRowsRunConfig{metadata: md})
		if !errors.Is(err, writeErr) {
			t.Fatalf("error = %v, want write failed", err)
		}
		if w.flushed {
			t.Fatal("Flush was called after WriteGCVs error")
		}
	})
}
