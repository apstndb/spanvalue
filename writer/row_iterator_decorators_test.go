package writer

import (
	"errors"
	"testing"

	"cloud.google.com/go/spanner"
)

func TestRunRowIterator_rowsRead(t *testing.T) {
	t.Parallel()

	md := metadataWithColumnNames("id")
	row, err := spanner.NewRow([]string{"id"}, []interface{}{int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	row2, err := spanner.NewRow([]string{"id"}, []interface{}{int64(2)})
	if err != nil {
		t.Fatal(err)
	}

	t.Run("success", func(t *testing.T) {
		t.Parallel()
		stub := &stubRowIterator{md: md, rows: []*spanner.Row{row, row2}}
		var written int
		got, err := runRowIterator(stub, RowIteratorHooks{
			WriteRow: func(*spanner.Row) error {
				written++
				return nil
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		if written != 2 || got.RowsRead != 2 {
			t.Fatalf("written=%d RowsRead=%d, want 2", written, got.RowsRead)
		}
	})

	t.Run("partial abort", func(t *testing.T) {
		t.Parallel()
		stub := &stubRowIterator{md: md, rows: []*spanner.Row{row, row2}}
		var n int
		got, err := runRowIterator(stub, RowIteratorHooks{
			WriteRow: func(*spanner.Row) error {
				n++
				if n == 2 {
					return errors.New("write failed")
				}
				return nil
			},
		})
		if err == nil {
			t.Fatal("expected error")
		}
		if got.RowsRead != 1 {
			t.Fatalf("RowsRead = %d, want 1", got.RowsRead)
		}
	})

	t.Run("nil WriteRow", func(t *testing.T) {
		t.Parallel()
		stub := &stubRowIterator{md: md, rows: []*spanner.Row{row}}
		got, err := runRowIterator(stub, RowIteratorHooks{})
		if err != nil {
			t.Fatal(err)
		}
		if got.RowsRead != 0 {
			t.Fatalf("RowsRead = %d, want 0", got.RowsRead)
		}
	})

	t.Run("decorator nil WriteRow updates ordinal not RowsRead", func(t *testing.T) {
		t.Parallel()
		var ord RowOrdinal
		stub := &stubRowIterator{md: md, rows: []*spanner.Row{row}}
		got, err := runRowIterator(stub, WithRowOrdinal(RowIteratorHooks{}, &ord))
		if err != nil {
			t.Fatal(err)
		}
		if ord.Current != 1 {
			t.Fatalf("Current = %d, want 1", ord.Current)
		}
		if got.RowsRead != 0 {
			t.Fatalf("RowsRead = %d, want 0 with nil base WriteRow", got.RowsRead)
		}
	})
}

func TestWithRowOrdinal(t *testing.T) {
	t.Parallel()

	md := metadataWithColumnNames("id")
	row, err := spanner.NewRow([]string{"id"}, []interface{}{int64(1)})
	if err != nil {
		t.Fatal(err)
	}

	t.Run("zero rows", func(t *testing.T) {
		t.Parallel()
		var ord RowOrdinal
		_, err := runRowIterator(&stubRowIterator{md: md}, WithRowOrdinal(RowIteratorHooks{}, &ord))
		if err != nil {
			t.Fatal(err)
		}
		if ord.Current != 0 {
			t.Fatalf("Current = %d, want 0", ord.Current)
		}
	})

	t.Run("success", func(t *testing.T) {
		t.Parallel()
		var ord RowOrdinal
		stub := &stubRowIterator{md: md, rows: []*spanner.Row{row, row}}
		_, err := runRowIterator(stub, WithRowOrdinal(RowIteratorHooks{
			WriteRow: func(*spanner.Row) error { return nil },
		}, &ord))
		if err != nil {
			t.Fatal(err)
		}
		if ord.Current != 2 {
			t.Fatalf("Current = %d, want 2", ord.Current)
		}
	})

	t.Run("write error", func(t *testing.T) {
		t.Parallel()
		var ord RowOrdinal
		stub := &stubRowIterator{md: md, rows: []*spanner.Row{row, row}}
		_, err := runRowIterator(stub, WithRowOrdinal(RowIteratorHooks{
			WriteRow: func(*spanner.Row) error {
				if ord.Current == 2 {
					return errors.New("fail")
				}
				return nil
			},
		}, &ord))
		if err == nil {
			t.Fatal("expected error")
		}
		if ord.Current != 2 {
			t.Fatalf("Current = %d, want 2 on failure", ord.Current)
		}
	})
}

func TestObserveWriteRow(t *testing.T) {
	t.Parallel()

	md := metadataWithColumnNames("id")
	row, err := spanner.NewRow([]string{"id"}, []interface{}{int64(1)})
	if err != nil {
		t.Fatal(err)
	}

	t.Run("with WriteRow", func(t *testing.T) {
		t.Parallel()
		stub := &stubRowIterator{md: md, rows: []*spanner.Row{row}}
		var seen []int
		hooks := ObserveWriteRow(RowIteratorHooks{
			WriteRow: func(*spanner.Row) error { return nil },
		}, func(n int) error {
			seen = append(seen, n)
			return nil
		})
		_, err := runRowIterator(stub, hooks)
		if err != nil {
			t.Fatal(err)
		}
		if len(seen) != 1 || seen[0] != 1 {
			t.Fatalf("seen = %v, want [1]", seen)
		}
	})

	t.Run("nil WriteRow", func(t *testing.T) {
		t.Parallel()
		stub := &stubRowIterator{md: md, rows: []*spanner.Row{row}}
		var seen []int
		_, err := runRowIterator(stub, ObserveWriteRow(RowIteratorHooks{}, func(n int) error {
			seen = append(seen, n)
			return nil
		}))
		if err != nil {
			t.Fatal(err)
		}
		if len(seen) != 1 || seen[0] != 1 {
			t.Fatalf("seen = %v, want [1]", seen)
		}
	})
}

func TestAfterEachSuccessfulWriteRow(t *testing.T) {
	t.Parallel()

	md := metadataWithColumnNames("id")
	row, err := spanner.NewRow([]string{"id"}, []interface{}{int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	row2, err := spanner.NewRow([]string{"id"}, []interface{}{int64(2)})
	if err != nil {
		t.Fatal(err)
	}

	t.Run("success", func(t *testing.T) {
		t.Parallel()
		stub := &stubRowIterator{md: md, rows: []*spanner.Row{row, row}}
		var flushes int
		hooks := AfterEachSuccessfulWriteRow(RowIteratorHooks{
			WriteRow: func(*spanner.Row) error { return nil },
		}, func() error {
			flushes++
			return nil
		})
		_, err = runRowIterator(stub, hooks)
		if err != nil {
			t.Fatal(err)
		}
		if flushes != 2 {
			t.Fatalf("flushes = %d, want 2", flushes)
		}
	})

	t.Run("WriteRow error skips after", func(t *testing.T) {
		t.Parallel()
		stub := &stubRowIterator{md: md, rows: []*spanner.Row{row, row2}}
		var flushes int
		var n int
		hooks := AfterEachSuccessfulWriteRow(RowIteratorHooks{
			WriteRow: func(*spanner.Row) error {
				n++
				if n == 2 {
					return errors.New("write failed")
				}
				return nil
			},
		}, func() error {
			flushes++
			return nil
		})
		_, err = runRowIterator(stub, hooks)
		if err == nil {
			t.Fatal("expected error")
		}
		if flushes != 1 {
			t.Fatalf("flushes = %d, want 1", flushes)
		}
	})

	t.Run("after error aborts", func(t *testing.T) {
		t.Parallel()
		stub := &stubRowIterator{md: md, rows: []*spanner.Row{row, row2}}
		var flushes int
		hooks := AfterEachSuccessfulWriteRow(RowIteratorHooks{
			WriteRow: func(*spanner.Row) error { return nil },
		}, func() error {
			flushes++
			if flushes == 2 {
				return errors.New("flush failed")
			}
			return nil
		})
		_, err = runRowIterator(stub, hooks)
		if err == nil {
			t.Fatal("expected error")
		}
		if flushes != 2 {
			t.Fatalf("flushes = %d, want 2", flushes)
		}
	})
}

func TestDecoratorResetOnQueryErrorBeforeMetadata(t *testing.T) {
	t.Parallel()

	md := metadataWithColumnNames("id")
	row, err := spanner.NewRow([]string{"id"}, []interface{}{int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	queryErr := errors.New("query failed")

	var ord RowOrdinal
	hooks := WithRowOrdinal(RowIteratorHooks{}, &ord)

	okStub := &stubRowIterator{md: md, rows: []*spanner.Row{row}}
	_, err = runRowIterator(okStub, hooks)
	if err != nil {
		t.Fatal(err)
	}
	if ord.Current != 1 {
		t.Fatalf("after success Current = %d, want 1", ord.Current)
	}

	failStub := &stubRowIterator{err: queryErr}
	_, err = runRowIterator(failStub, hooks)
	if !errors.Is(err, queryErr) {
		t.Fatalf("error = %v, want %v", err, queryErr)
	}
	if ord.Current != 0 {
		t.Fatalf("after query error Current = %d, want 0", ord.Current)
	}
}

func TestResetEachRunRunsOncePerRun(t *testing.T) {
	t.Parallel()

	md := metadataWithColumnNames("id")
	row, err := spanner.NewRow([]string{"id"}, []interface{}{int64(1)})
	if err != nil {
		t.Fatal(err)
	}

	var resets int
	hooks := resetEachRun(RowIteratorHooks{
		WriteRow: func(*spanner.Row) error { return nil },
	}, func() {
		resets++
	})
	_, err = runRowIterator(&stubRowIterator{md: md, rows: []*spanner.Row{row}}, hooks)
	if err != nil {
		t.Fatal(err)
	}
	if resets != 1 {
		t.Fatalf("resets = %d, want 1", resets)
	}
}

func TestRowIteratorHooksExtensibility(t *testing.T) {
	t.Parallel()

	md := metadataWithColumnNames("id")
	row, err := spanner.NewRow([]string{"id"}, []interface{}{int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	stub := &stubRowIterator{md: md, rows: []*spanner.Row{row, row}}

	var runStarts int
	hooks := RowIteratorHooks{
		WriteRow: func(*spanner.Row) error { return nil },
	}
	hooks.MarkOmitRowsRead()
	hooks.OnRunStart(func() { runStarts++ })

	got, err := runRowIterator(stub, hooks)
	if err != nil {
		t.Fatal(err)
	}
	if runStarts != 1 {
		t.Fatalf("runStarts = %d, want 1", runStarts)
	}
	if got.RowsRead != 0 {
		t.Fatalf("RowsRead = %d, want 0 with MarkOmitRowsRead", got.RowsRead)
	}
}
