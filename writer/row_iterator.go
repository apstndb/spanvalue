package writer

import (
	"errors"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/api/iterator"
)

// ErrNilRowIterator reports that [RunRowIterator] or [WriteRowIterator] was called with a nil iterator.
var ErrNilRowIterator = errors.New("nil row iterator")

// ErrNilWriter reports that [WriteRowIterator] was called with a nil writer.
var ErrNilWriter = errors.New("nil row iterator writer")

// RowIteratorStats holds execution information populated on a
// [cloud.google.com/go/spanner.RowIterator] after iteration completes.
// QueryPlan and QueryStats are set when the query used QueryWithStats.
// RowCount is set for DML after iterator.Done.
type RowIteratorStats struct {
	QueryPlan  *sppb.QueryPlan
	QueryStats map[string]any
	RowCount   int64
}

// RowIteratorResult is the metadata and stats available from a
// [cloud.google.com/go/spanner.RowIterator] after [RunRowIterator] returns.
// On the error path, stats fields reflect whatever the iterator had populated at
// the abort point and may be zero until [iterator.Done] (QueryStats and RowCount
// are only fully populated after a successful run).
//
// RowsRead counts data rows for which hooks.WriteRow returned nil during the
// run, including on the error path when iteration aborts mid-stream. It stays
// zero when WriteRow is nil (rows may still be consumed from the iterator).
// Hook decorators that install a WriteRow wrapper only for ordinal or observe
// side effects do not increment RowsRead unless the wrapped hooks already had
// WriteRow set. RowsRead is distinct from [RowIteratorStats.RowCount], which
// follows Spanner iterator semantics (DML row count after iterator.Done).
type RowIteratorResult struct {
	Metadata *sppb.ResultSetMetadata
	Stats    RowIteratorStats
	RowsRead int
}

// RowIteratorHooks drives [RunRowIterator]. Nil function fields are skipped.
//
// PrepareMetadata runs once after the first [spanner.RowIterator.Next], with
// whatever [cloud.google.com/go/spanner.RowIterator.Metadata] holds at that point
// (including nil for DML or stats-only iterators, and including when the only
// result is iterator.Done). It is not called when the first Next returns a query
// error other than iterator.Done.
//
// Finish runs only after all rows are consumed without error. If PrepareMetadata
// or WriteRow returns an error, the loop aborts and Finish is not called. The
// returned [RowIteratorResult] is still populated with whatever iter.Metadata and
// stats are available at the abort point. Stats is fully populated when Finish
// runs successfully. QueryPlan and QueryStats require QueryWithStats.
// Finish may read Metadata again for end-of-stream processing.
type RowIteratorHooks struct {
	PrepareMetadata func(*sppb.ResultSetMetadata) error
	WriteRow        func(*spanner.Row) error
	Finish          func(*RowIteratorResult) error

	// omitRowsRead is set by hook decorators that add WriteRow only for side
	// effects when the wrapped hooks had no WriteRow.
	omitRowsRead bool
	// onRunStart runs once at the beginning of each RunRowIterator call.
	onRunStart func()
}

// MarkOmitRowsRead configures the hooks so successful WriteRow calls do not
// increment [RowIteratorResult.RowsRead]. Use when WriteRow exists only for
// side effects (for example a custom decorator) and should not count as exported rows.
func (h *RowIteratorHooks) MarkOmitRowsRead() {
	h.omitRowsRead = true
}

// OnRunStart registers fn to run once at the beginning of each [RunRowIterator]
// call. Multiple calls chain in registration order. A nil fn is ignored.
func (h *RowIteratorHooks) OnRunStart(fn func()) {
	if fn == nil {
		return
	}
	prev := h.onRunStart
	h.onRunStart = func() {
		if prev != nil {
			prev()
		}
		fn()
	}
}

// RowIteratorWriter streams rows from a [cloud.google.com/go/spanner.RowIterator]
// through [WriteRowIterator]. [DelimitedWriter], [JSONLWriter], and
// [SQLInsertWriter] implement it.
type RowIteratorWriter interface {
	FlushWriter
	PrepareRowType(*sppb.StructType) error
}

// RowIteratorHooksFromWriter returns hooks that register metadata via
// [RowIteratorWriter.PrepareRowType], write each row, and call [Flusher.Flush]
// in Finish. Flush is not called when PrepareRowType or WriteRow returns an error.
// A nil writer returns empty hooks.
func RowIteratorHooksFromWriter(w RowIteratorWriter) RowIteratorHooks {
	if w == nil {
		return RowIteratorHooks{}
	}
	return RowIteratorHooks{
		PrepareMetadata: func(md *sppb.ResultSetMetadata) error {
			return w.PrepareRowType(rowTypeFromMetadata(md))
		},
		WriteRow: w.WriteRow,
		Finish: func(*RowIteratorResult) error {
			return w.Flush()
		},
	}
}

// RunRowIterator streams all rows from iter using hooks. It always calls
// [spanner.RowIterator.Stop] on return.
//
// The returned [RowIteratorResult] reflects iter.Metadata and iter stats fields
// after Stop and the loop (including when no data rows were written). When
// hooks.Finish is set, it receives the same pointer that is returned.
func RunRowIterator(iter *spanner.RowIterator, hooks RowIteratorHooks) (*RowIteratorResult, error) {
	if iter == nil {
		return nil, ErrNilRowIterator
	}
	return runRowIterator(spannerRowIteratorFacade{iter}, hooks)
}

// WriteRowIterator streams all rows from iter into w using [RowIteratorHooksFromWriter].
// See [RunRowIterator] for iterator metadata, stats, and zero-row behavior.
//
// When an application drives [cloud.google.com/go/spanner.RowIterator.Next] directly
// (instead of [WriteRowIterator] or [RunRowIterator]), call
// [cloud.google.com/go/spanner.RowIterator.Stop] on return (typically defer iter.Stop())
// so streams and resources are released.
//
// When an application skips row bodies but still needs a header-only delimited finish,
// call [RowIteratorWriter.PrepareRowType] with iter.Metadata.GetRowType() after the Next
// loop, then [Flusher.Flush]; see README "Metadata-only finish after skipping rows".
func WriteRowIterator(iter *spanner.RowIterator, w RowIteratorWriter) (*RowIteratorResult, error) {
	if iter == nil {
		return nil, ErrNilRowIterator
	}
	if w == nil {
		return nil, ErrNilWriter
	}
	return RunRowIterator(iter, RowIteratorHooksFromWriter(w))
}

type rowIteratorFacade interface {
	next() (*spanner.Row, error)
	stop()
	metadata() *sppb.ResultSetMetadata
	stats() RowIteratorStats
}

type spannerRowIteratorFacade struct {
	*spanner.RowIterator
}

func (f spannerRowIteratorFacade) next() (*spanner.Row, error) {
	return f.Next()
}

func (f spannerRowIteratorFacade) stop() {
	f.Stop()
}

func (f spannerRowIteratorFacade) metadata() *sppb.ResultSetMetadata {
	return f.Metadata
}

func (f spannerRowIteratorFacade) stats() RowIteratorStats {
	return RowIteratorStats{
		QueryPlan:  f.QueryPlan,
		QueryStats: f.QueryStats,
		RowCount:   f.RowCount,
	}
}

func runRowIterator(fac rowIteratorFacade, hooks RowIteratorHooks) (*RowIteratorResult, error) {
	stopped := false
	stopOnce := func() {
		if !stopped {
			stopped = true
			fac.stop()
		}
	}
	defer stopOnce()

	if hooks.onRunStart != nil {
		hooks.onRunStart()
	}

	var rowsRead int
	outcome := func() *RowIteratorResult {
		return &RowIteratorResult{
			Metadata: fac.metadata(),
			Stats:    fac.stats(),
			RowsRead: rowsRead,
		}
	}
	abort := func(err error) (*RowIteratorResult, error) {
		stopOnce()
		return outcome(), err
	}

	first := true
	for {
		row, err := fac.next()
		if err != nil && !errors.Is(err, iterator.Done) {
			return abort(err)
		}
		if first {
			first = false
			if hooks.PrepareMetadata != nil {
				if err := hooks.PrepareMetadata(fac.metadata()); err != nil {
					return abort(err)
				}
			}
		}
		if errors.Is(err, iterator.Done) {
			break
		}
		if hooks.WriteRow != nil {
			if err := hooks.WriteRow(row); err != nil {
				return abort(err)
			}
			if !hooks.omitRowsRead {
				rowsRead++
			}
		}
	}
	stopOnce()
	result := outcome()
	if hooks.Finish != nil {
		if err := hooks.Finish(result); err != nil {
			return result, err
		}
	}
	return result, nil
}
