package writer

import (
	"errors"
	"iter"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/api/iterator"
)

// ErrNilRowSeq reports that [RunRowSeq] or [WriteRowSeq] was called with a nil
// row sequence.
var ErrNilRowSeq = errors.New("nil row sequence")

// RowSeq adapts already-built rows to the fallible sequence shape consumed by
// [RunRowSeq] and [WriteRowSeq]. Every pair it yields has a nil error.
//
// Row sources that can fail per row (for example lazy encoders) should
// produce their own [iter.Seq2] instead of pre-building a slice for RowSeq.
func RowSeq(rows ...*spanner.Row) iter.Seq2[*spanner.Row, error] {
	return func(yield func(*spanner.Row, error) bool) {
		for _, row := range rows {
			if !yield(row, nil) {
				return
			}
		}
	}
}

// RunRowSeq drives hooks over rows that do not come from a
// [cloud.google.com/go/spanner.RowIterator] — client-side (virtual) result
// sets, locally constructed rows, or lazily encoded rows. It shares the hook
// contract of [RunRowIterator]: PrepareMetadata runs once with md before the
// first data row (including when the sequence is empty), WriteRow runs per
// row, a failure aborts the run without calling Finish, and Finish runs only
// after all rows succeed. [RowIteratorResult.RowsRead] counts successful
// WriteRow calls.
//
// md is passed to PrepareMetadata as-is; nil is allowed and mirrors a
// metadata-less iterator, but writer-backed hooks generally need row-type
// metadata to emit headers. [RowIteratorStats] in the result is always zero:
// there is no Spanner iterator to report stats.
//
// A non-nil error yielded by rows aborts the run and is returned; the row
// paired with it is ignored and the sequence is not consumed further.
func RunRowSeq(md *sppb.ResultSetMetadata, rows iter.Seq2[*spanner.Row, error], hooks RowIteratorHooks) (*RowIteratorResult, error) {
	if rows == nil {
		return nil, ErrNilRowSeq
	}
	next, release := iter.Pull2(rows)
	return runRowIterator(&seqRowFacade{md: md, nextPair: next, release: release}, hooks)
}

// WriteRowSeq streams rows into w using [RowIteratorHooksFromWriter], the
// in-memory counterpart of [WriteRowIterator]. See [RunRowSeq] for the hook
// contract, metadata, and error semantics; like [WriteRowIterator], an empty
// sequence still registers metadata and flushes, so delimited writers emit a
// header-only result.
func WriteRowSeq(md *sppb.ResultSetMetadata, rows iter.Seq2[*spanner.Row, error], w RowIteratorWriter) (*RowIteratorResult, error) {
	if rows == nil {
		return nil, ErrNilRowSeq
	}
	if w == nil {
		return nil, ErrNilWriter
	}
	return RunRowSeq(md, rows, RowIteratorHooksFromWriter(w))
}

// seqRowFacade adapts (metadata, pulled iter.Seq2) to rowIteratorFacade so
// RunRowSeq shares runRowIterator and its hook ordering with RunRowIterator.
type seqRowFacade struct {
	md       *sppb.ResultSetMetadata
	nextPair func() (*spanner.Row, error, bool)
	release  func()
}

func (f *seqRowFacade) next() (*spanner.Row, error) {
	row, err, ok := f.nextPair()
	if !ok {
		return nil, iterator.Done
	}
	if err != nil {
		return nil, err
	}
	return row, nil
}

func (f *seqRowFacade) stop() {
	f.release()
}

func (f *seqRowFacade) metadata() *sppb.ResultSetMetadata {
	return f.md
}

func (f *seqRowFacade) stats() RowIteratorStats {
	return RowIteratorStats{}
}
