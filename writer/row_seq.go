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
// [RunRowSeq] and [WriteRowSeq]. Every pair it yields has a nil error; a nil
// row in rows is rejected by the consumer with [ErrNilRow].
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
// paired with it is ignored and the sequence is not consumed further. A nil
// row yielded with a nil error aborts the run with [ErrNilRow].
func RunRowSeq(md *sppb.ResultSetMetadata, rows iter.Seq2[*spanner.Row, error], hooks RowIteratorHooks) (*RowIteratorResult, error) {
	if rows == nil {
		return nil, ErrNilRowSeq
	}
	next, release := iter.Pull2(rows)
	return runRowIterator(&seqRowFacade{md: func() *sppb.ResultSetMetadata { return md }, nextPair: next, release: release}, hooks)
}

// RunRowSeqDeferredMetadata is [RunRowSeq] for producers that learn the row
// type only after producing begins — merged concurrent sources, lazily
// decoded streams. metadata is called lazily on the [RunRowIterator]
// schedule: once immediately before PrepareMetadata, which runs after the
// first pair has been pulled from rows (or after rows ends when it is
// empty), and again when assembling [RowIteratorResult]. A producer
// therefore only has to publish the row type before yielding its first
// pair, with no need to hold rows back; a nil metadata func is treated as
// always-nil metadata.
//
// All other semantics match [RunRowSeq].
func RunRowSeqDeferredMetadata(metadata func() *sppb.ResultSetMetadata, rows iter.Seq2[*spanner.Row, error], hooks RowIteratorHooks) (*RowIteratorResult, error) {
	if rows == nil {
		return nil, ErrNilRowSeq
	}
	if metadata == nil {
		metadata = func() *sppb.ResultSetMetadata { return nil }
	}
	next, release := iter.Pull2(rows)
	return runRowIterator(&seqRowFacade{md: metadata, nextPair: next, release: release}, hooks)
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
// md is a func so [RunRowSeqDeferredMetadata] can defer row-type resolution
// until runRowIterator evaluates it (after the first next call).
type seqRowFacade struct {
	md       func() *sppb.ResultSetMetadata
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
	if row == nil {
		return nil, ErrNilRow
	}
	return row, nil
}

func (f *seqRowFacade) stop() {
	f.release()
}

func (f *seqRowFacade) metadata() *sppb.ResultSetMetadata {
	return f.md()
}

func (f *seqRowFacade) stats() RowIteratorStats {
	return RowIteratorStats{}
}
