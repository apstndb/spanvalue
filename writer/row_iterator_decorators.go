package writer

import (
	"cloud.google.com/go/spanner"
)

// RowOrdinal holds a 1-based row index for diagnostics while streaming rows.
// Use [WithRowOrdinal] to keep it updated around WriteRow.
type RowOrdinal struct {
	// Current is the 1-based index of the row about to be written, or the last
	// row whose WriteRow completed without error. It remains 0 until the first
	// data row is reached. On WriteRow error, Current is the failing row index.
	Current int
}

// WithRowOrdinal wraps base so ord.Current is set to the 1-based row index
// immediately before each WriteRow attempt on base. Reset ord.Current to 0
// before each RunRowIterator when reusing the returned hooks. PrepareMetadata
// and Finish are forwarded unchanged. A nil ord is ignored and base is returned as-is.
func WithRowOrdinal(base RowIteratorHooks, ord *RowOrdinal) RowIteratorHooks {
	if ord == nil {
		return base
	}
	writeRow := base.WriteRow
	if writeRow == nil {
		return base
	}
	base.WriteRow = func(row *spanner.Row) error {
		ord.Current++
		return writeRow(row)
	}
	return base
}

// ObserveWriteRow wraps base.WriteRow to call observe with the 1-based row index
// before delegating. Returning an error from observe aborts without calling
// base.WriteRow. PrepareMetadata and Finish are forwarded unchanged.
func ObserveWriteRow(base RowIteratorHooks, observe func(rowNum int) error) RowIteratorHooks {
	if observe == nil {
		return base
	}
	writeRow := base.WriteRow
	if writeRow == nil {
		return base
	}
	next := 0
	base.WriteRow = func(row *spanner.Row) error {
		next++
		if err := observe(next); err != nil {
			return err
		}
		return writeRow(row)
	}
	return base
}

// AfterEachSuccessfulWriteRow wraps base.WriteRow to call after on each
// successful delegation. after is typically a buffered I/O flush for interactive
// streaming; do not pass [SQLInsertWriter.Flush] here because that finalizes an
// open INSERT batch and disables batching.
func AfterEachSuccessfulWriteRow(base RowIteratorHooks, after func() error) RowIteratorHooks {
	if after == nil {
		return base
	}
	writeRow := base.WriteRow
	if writeRow == nil {
		return base
	}
	base.WriteRow = func(row *spanner.Row) error {
		if err := writeRow(row); err != nil {
			return err
		}
		return after()
	}
	return base
}
