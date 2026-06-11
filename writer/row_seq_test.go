package writer

import (
	"bytes"
	"errors"
	"testing"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
)

var _ rowIteratorFacade = (*seqRowFacade)(nil)

func mustNewSpannerRow(t *testing.T, names []string, values []any) *spanner.Row {
	t.Helper()
	row, err := spanner.NewRow(names, values)
	if err != nil {
		t.Fatal(err)
	}
	return row
}

func TestRunRowSeq_nilSeq(t *testing.T) {
	t.Parallel()

	if _, err := RunRowSeq(nil, nil, RowIteratorHooks{}); !errors.Is(err, ErrNilRowSeq) {
		t.Fatalf("error = %v, want ErrNilRowSeq", err)
	}
}

func TestWriteRowSeq_nilArguments(t *testing.T) {
	t.Parallel()

	if _, err := WriteRowSeq(nil, nil, mustNewDelimitedWriter(t, &bytes.Buffer{}, ',')); !errors.Is(err, ErrNilRowSeq) {
		t.Fatalf("error = %v, want ErrNilRowSeq", err)
	}
	if _, err := WriteRowSeq(nil, RowSeq(), nil); !errors.Is(err, ErrNilWriter) {
		t.Fatalf("error = %v, want ErrNilWriter", err)
	}
}

func TestWriteRowSeq_csv(t *testing.T) {
	t.Parallel()

	md := metadataWithColumnNames("id", "name")
	names := []string{"id", "name"}
	rows := RowSeq(
		mustNewSpannerRow(t, names, []any{int64(1), "a"}),
		mustNewSpannerRow(t, names, []any{int64(2), "b"}),
	)

	var out bytes.Buffer
	w := mustNewDelimitedWriter(t, &out, ',', WithHeader(true))
	got, err := WriteRowSeq(md, rows, w)
	if err != nil {
		t.Fatal(err)
	}
	if got.Metadata != md {
		t.Fatal("metadata not returned")
	}
	if got.RowsRead != 2 {
		t.Fatalf("RowsRead = %d, want 2", got.RowsRead)
	}
	want := "id,name\n1,a\n2,b\n"
	if out.String() != want {
		t.Fatalf("output = %q, want %q", out.String(), want)
	}
}

func TestWriteRowSeq_emptyWithMetadata(t *testing.T) {
	t.Parallel()

	md := metadataWithColumnNames("id", "name")

	var out bytes.Buffer
	w := mustNewDelimitedWriter(t, &out, ',', WithHeader(true))
	got, err := WriteRowSeq(md, RowSeq(), w)
	if err != nil {
		t.Fatal(err)
	}
	if got.RowsRead != 0 {
		t.Fatalf("RowsRead = %d, want 0", got.RowsRead)
	}
	want := "id,name\n"
	if out.String() != want {
		t.Fatalf("output = %q, want %q", out.String(), want)
	}
}

func TestWriteRowSeq_jsonl(t *testing.T) {
	t.Parallel()

	md := metadataWithColumnNames("id", "name")
	names := []string{"id", "name"}
	rows := RowSeq(mustNewSpannerRow(t, names, []any{int64(1), "a"}))

	var out bytes.Buffer
	w := mustNewJSONLWriter(t, &out)
	if _, err := WriteRowSeq(md, rows, w); err != nil {
		t.Fatal(err)
	}
	want := "{\"id\":1,\"name\":\"a\"}\n"
	if out.String() != want {
		t.Fatalf("output = %q, want %q", out.String(), want)
	}
}

func TestWriteRowSeq_yieldedErrorAborts(t *testing.T) {
	t.Parallel()

	md := metadataWithColumnNames("id", "name")
	names := []string{"id", "name"}
	row := mustNewSpannerRow(t, names, []any{int64(1), "a"})
	wantErr := errors.New("encode failed")
	var yieldedAfterError bool
	rows := func(yield func(*spanner.Row, error) bool) {
		if !yield(row, nil) {
			return
		}
		if !yield(nil, wantErr) {
			return
		}
		yieldedAfterError = true
	}

	var out bytes.Buffer
	w := mustNewDelimitedWriter(t, &out, ',', WithHeader(true))
	got, err := WriteRowSeq(md, rows, w)
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}
	if got.RowsRead != 1 {
		t.Fatalf("RowsRead = %d, want 1", got.RowsRead)
	}
	if yieldedAfterError {
		t.Fatal("sequence consumed past the yielded error")
	}
	// Finish (and therefore Flush) must not run on abort, so the buffered
	// csv.Writer has emitted nothing.
	if out.Len() != 0 {
		t.Fatalf("output = %q, want empty", out.String())
	}
}

func TestRunRowSeq_hookOrderingAndStats(t *testing.T) {
	t.Parallel()

	md := metadataWithColumnNames("id")
	rows := RowSeq(
		mustNewSpannerRow(t, []string{"id"}, []any{int64(1)}),
		mustNewSpannerRow(t, []string{"id"}, []any{int64(2)}),
	)

	var calls []string
	hooks := NewRowIteratorHooks().
		WithPrepareMetadata(func(got *sppb.ResultSetMetadata) error {
			if got != md {
				t.Error("PrepareMetadata did not receive md")
			}
			calls = append(calls, "prepare")
			return nil
		}).
		WithWriteRow(func(*spanner.Row) error {
			calls = append(calls, "write")
			return nil
		}).
		WithFinish(func(result *RowIteratorResult) error {
			if result.Stats.QueryPlan != nil || result.Stats.QueryStats != nil || result.Stats.RowCount != 0 {
				t.Errorf("Stats = %+v, want zero", result.Stats)
			}
			calls = append(calls, "finish")
			return nil
		})

	got, err := RunRowSeq(md, rows, hooks)
	if err != nil {
		t.Fatal(err)
	}
	if got.RowsRead != 2 {
		t.Fatalf("RowsRead = %d, want 2", got.RowsRead)
	}
	want := []string{"prepare", "write", "write", "finish"}
	if len(calls) != len(want) {
		t.Fatalf("calls = %v, want %v", calls, want)
	}
	for i := range want {
		if calls[i] != want[i] {
			t.Fatalf("calls = %v, want %v", calls, want)
		}
	}
}

func TestRunRowSeq_emptyNilMetadata(t *testing.T) {
	t.Parallel()

	var prepared bool
	hooks := NewRowIteratorHooks().WithPrepareMetadata(func(md *sppb.ResultSetMetadata) error {
		if md != nil {
			t.Errorf("metadata = %v, want nil", md)
		}
		prepared = true
		return nil
	})

	got, err := RunRowSeq(nil, RowSeq(), hooks)
	if err != nil {
		t.Fatal(err)
	}
	if !prepared {
		t.Fatal("PrepareMetadata not called for empty sequence")
	}
	if got.Metadata != nil || got.RowsRead != 0 {
		t.Fatalf("result = %+v, want nil metadata and 0 rows", got)
	}
}
