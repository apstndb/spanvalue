package writer

import (
	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
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
// immediately before each WriteRow attempt on base. ord.Current resets to 0 at
// the start of each [RunRowIterator] call (including when the first Next returns
// a query error before PrepareMetadata). A nil ord is ignored and base is
// returned as-is. When base.WriteRow is nil, the wrapper still runs so ord is
// updated for each streamed row.
func WithRowOrdinal(base RowIteratorHooks, ord *RowOrdinal) RowIteratorHooks {
	if ord == nil {
		return base
	}
	writeRow := base.WriteRow
	base = resetEachRun(base, func() { ord.Current = 0 })
	base.omitRowsRead = base.omitRowsRead || writeRow == nil
	base.WriteRow = func(row *spanner.Row) error {
		ord.Current++
		if writeRow != nil {
			return writeRow(row)
		}
		return nil
	}
	return base
}

// ObserveWriteRow wraps base.WriteRow to call observe with the 1-based row index
// before delegating. Returning an error from observe aborts without calling
// base.WriteRow. When base.WriteRow is nil, observe still runs for each row.
func ObserveWriteRow(base RowIteratorHooks, observe func(rowNum int) error) RowIteratorHooks {
	if observe == nil {
		return base
	}
	writeRow := base.WriteRow
	var rowNum int
	base = resetEachRun(base, func() { rowNum = 0 })
	base.omitRowsRead = base.omitRowsRead || writeRow == nil
	base.WriteRow = func(row *spanner.Row) error {
		rowNum++
		if err := observe(rowNum); err != nil {
			return err
		}
		if writeRow != nil {
			return writeRow(row)
		}
		return nil
	}
	return base
}

// AfterEachSuccessfulWriteRow wraps base.WriteRow to call after on each
// successful delegation. after is typically a buffered I/O flush for interactive
// streaming; do not pass [SQLInsertWriter.Flush] here because that finalizes an
// open INSERT batch and disables batching. When base.WriteRow is nil, after
// still runs once per streamed row.
func AfterEachSuccessfulWriteRow(base RowIteratorHooks, after func() error) RowIteratorHooks {
	if after == nil {
		return base
	}
	writeRow := base.WriteRow
	base.omitRowsRead = base.omitRowsRead || writeRow == nil
	base.WriteRow = func(row *spanner.Row) error {
		if writeRow != nil {
			if err := writeRow(row); err != nil {
				return err
			}
		}
		return after()
	}
	return base
}

func resetEachRun(base RowIteratorHooks, reset func()) RowIteratorHooks {
	prev := base.onRunStart
	base.onRunStart = func() {
		if prev != nil {
			prev()
		}
		reset()
	}
	prep := base.PrepareMetadata
	base.PrepareMetadata = func(md *sppb.ResultSetMetadata) error {
		reset()
		if prep != nil {
			return prep(md)
		}
		return nil
	}
	return base
}
