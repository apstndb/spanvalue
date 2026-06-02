package writer

import (
	"bytes"
	"errors"
	"testing"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/api/iterator"
)

var (
	_ rowIteratorFacade = (*stubRowIterator)(nil)
	_ RowIteratorWriter = (*DelimitedWriter)(nil)
	_ RowIteratorWriter = (*JSONLWriter)(nil)
	_ RowIteratorWriter = (*SQLInsertWriter)(nil)
)

type stubRowIterator struct {
	md       *sppb.ResultSetMetadata
	wantStat RowIteratorStats
	rows     []*spanner.Row
	err      error
	i        int
	stopped  bool
}

func (s *stubRowIterator) next() (*spanner.Row, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.i < len(s.rows) {
		row := s.rows[s.i]
		s.i++
		return row, nil
	}
	return nil, iterator.Done
}

func (s *stubRowIterator) stop() {
	s.stopped = true
}

func (s *stubRowIterator) metadata() *sppb.ResultSetMetadata {
	return s.md
}

func (s *stubRowIterator) stats() RowIteratorStats {
	if !s.stopped {
		return RowIteratorStats{}
	}
	return s.wantStat
}

func TestRunRowIterator_nilIterator(t *testing.T) {
	t.Parallel()

	_, err := RunRowIterator(nil, RowIteratorHooks{})
	if !errors.Is(err, ErrNilRowIterator) {
		t.Fatalf("error = %v, want ErrNilRowIterator", err)
	}
}

func TestWriteRowIterator_nilIterator(t *testing.T) {
	t.Parallel()

	_, err := WriteRowIterator(nil, mustNewDelimitedWriter(t, &bytes.Buffer{}, ','))
	if !errors.Is(err, ErrNilRowIterator) {
		t.Fatalf("error = %v, want ErrNilRowIterator", err)
	}
}

func TestWriteRowIterator_emptyWithMetadata(t *testing.T) {
	t.Parallel()

	md := metadataWithColumnNames("id", "name")
	wantStats := RowIteratorStats{RowCount: 0, QueryStats: map[string]any{"elapsed_ms": 1.0}}
	stub := &stubRowIterator{md: md, wantStat: wantStats}

	var out bytes.Buffer
	w := mustNewDelimitedWriter(t, &out, ',', WithHeader(true))
	got, err := runRowIterator(stub, RowIteratorHooksFromWriter(w))
	if err != nil {
		t.Fatal(err)
	}
	if got.Metadata != md {
		t.Fatal("metadata not returned")
	}
	if diff := cmp.Diff(wantStats, got.Stats); diff != "" {
		t.Fatalf("Stats mismatch (-want +got):\n%s", diff)
	}
	want := "id,name\n"
	if out.String() != want {
		t.Fatalf("output = %q, want %q", out.String(), want)
	}
}

func TestWriteRowIterator_emptyZeroColumnMetadata(t *testing.T) {
	t.Parallel()

	md := metadataWithColumnNames()
	stub := &stubRowIterator{md: md}

	var out bytes.Buffer
	w := mustNewDelimitedWriter(t, &out, ',', WithHeader(true))
	got, err := runRowIterator(stub, RowIteratorHooksFromWriter(w))
	if err != nil {
		t.Fatal(err)
	}
	if got.Metadata != md {
		t.Fatal("metadata not returned")
	}
	if out.Len() != 0 {
		t.Fatalf("output = %q, want empty", out.String())
	}
}

func TestWriteRowIterator_withRows(t *testing.T) {
	t.Parallel()

	md := metadataWithColumnNames("id", "name")
	row, err := spanner.NewRow([]string{"id", "name"}, []interface{}{int64(1), "a"})
	if err != nil {
		t.Fatal(err)
	}
	stub := &stubRowIterator{
		md:       md,
		wantStat: RowIteratorStats{RowCount: 1},
		rows:     []*spanner.Row{row},
	}

	var out bytes.Buffer
	w := mustNewDelimitedWriter(t, &out, ',', WithHeader(true))
	got, err := runRowIterator(stub, RowIteratorHooksFromWriter(w))
	if err != nil {
		t.Fatal(err)
	}
	if got.Metadata != md {
		t.Fatal("metadata not returned")
	}
	if got.Stats.RowCount != 1 {
		t.Fatalf("RowCount = %d, want 1", got.Stats.RowCount)
	}
	want := "id,name\n1,a\n"
	if out.String() != want {
		t.Fatalf("output = %q, want %q", out.String(), want)
	}
}

func TestRunRowIterator_finishReceivesResult(t *testing.T) {
	t.Parallel()

	md := metadataWithColumnNames("id")
	wantStats := RowIteratorStats{RowCount: 42}
	stub := &stubRowIterator{md: md, wantStat: wantStats}

	var finishResult *RowIteratorResult
	hooks := RowIteratorHooks{
		PrepareMetadata: func(*sppb.ResultSetMetadata) error { return nil },
		Finish: func(res *RowIteratorResult) error {
			finishResult = res
			return nil
		},
	}
	got, err := runRowIterator(stub, hooks)
	if err != nil {
		t.Fatal(err)
	}
	if finishResult == nil {
		t.Fatal("Finish was not called")
	}
	if finishResult != got {
		t.Fatal("Finish should receive pointer to returned result")
	}
	if finishResult.Metadata != md {
		t.Fatal("Finish metadata mismatch")
	}
	if finishResult.Stats.RowCount != 42 {
		t.Fatalf("Finish stats RowCount = %d, want 42", finishResult.Stats.RowCount)
	}
}

func TestRunRowIterator_prepareMetadataSeesFullMetadata(t *testing.T) {
	t.Parallel()

	md := metadataWithColumnNames("id")
	stub := &stubRowIterator{md: md}

	var prepared *sppb.ResultSetMetadata
	hooks := RowIteratorHooks{
		PrepareMetadata: func(m *sppb.ResultSetMetadata) error {
			prepared = m
			return nil
		},
	}
	_, err := runRowIterator(stub, hooks)
	if err != nil {
		t.Fatal(err)
	}
	if prepared != md {
		t.Fatal("PrepareMetadata did not receive iter metadata")
	}
}

func TestWriteRowIterator_writeErrorStillReturnsOutcome(t *testing.T) {
	t.Parallel()

	md := metadataWithColumnNames("id")
	row, err := spanner.NewRow([]string{"id"}, []interface{}{int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	stub := &stubRowIterator{md: md, rows: []*spanner.Row{row}}

	hooks := RowIteratorHooksFromWriter(mustNewDelimitedWriter(t, &bytes.Buffer{}, ','))
	hooks.WriteRow = func(*spanner.Row) error {
		return errors.New("write failed")
	}
	got, err := runRowIterator(stub, hooks)
	if err == nil {
		t.Fatal("expected error")
	}
	if got.Metadata != md {
		t.Fatal("metadata not returned on error")
	}
	if !stub.stopped {
		t.Fatal("stop not called")
	}
}

func TestWriteRowIterator_nilWriter(t *testing.T) {
	t.Parallel()

	_, err := WriteRowIterator(&spanner.RowIterator{}, nil)
	if !errors.Is(err, ErrNilWriter) {
		t.Fatalf("error = %v, want ErrNilWriter", err)
	}
}

func TestRunRowIterator_queryErrorDoesNotPrepare(t *testing.T) {
	t.Parallel()

	queryErr := errors.New("query failed")
	stub := &stubRowIterator{err: queryErr}

	var prepared bool
	hooks := RowIteratorHooks{
		PrepareMetadata: func(*sppb.ResultSetMetadata) error {
			prepared = true
			return nil
		},
	}
	_, err := runRowIterator(stub, hooks)
	if !errors.Is(err, queryErr) {
		t.Fatalf("error = %v, want %v", err, queryErr)
	}
	if prepared {
		t.Fatal("PrepareMetadata should not be called on query error")
	}
	if !stub.stopped {
		t.Fatal("stop not called")
	}
}
