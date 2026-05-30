package writer

import (
	"errors"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/api/iterator"
)

// ErrNilRowIterator reports that [RunRowIterator] or [WriteRowIterator] was called with a nil iterator.
var ErrNilRowIterator = errors.New("nil RowIterator")

// RowIteratorStats holds execution information populated on a
// [cloud.google.com/go/spanner.RowIterator] after iteration completes.
// QueryPlan and QueryStats are set when the query used QueryWithStats.
// RowCount is set for DML after iterator.Done.
type RowIteratorStats struct {
	QueryPlan  *sppb.QueryPlan
	QueryStats map[string]interface{}
	RowCount   int64
}

// RowIteratorResult is the metadata and stats available from a
// [cloud.google.com/go/spanner.RowIterator] after [RunRowIterator] finishes.
type RowIteratorResult struct {
	Metadata *sppb.ResultSetMetadata
	Stats    RowIteratorStats
}

// RowIteratorHooks drives [RunRowIterator]. Nil function fields are skipped.
//
// PrepareMetadata runs after the first [spanner.RowIterator.Next] when
// [cloud.google.com/go/spanner.RowIterator.Metadata] is non-nil (including when the
// only result is iterator.Done). Finish runs after all rows are consumed; Stats is
// fully populated at that point. QueryPlan and QueryStats require QueryWithStats.
// Finish may read Metadata again for end-of-stream processing.
type RowIteratorHooks struct {
	PrepareMetadata func(*sppb.ResultSetMetadata) error
	WriteRow        func(*spanner.Row) error
	Finish          func(*RowIteratorResult) error
}

// RowIteratorWriter streams rows from a [cloud.google.com/go/spanner.RowIterator]
// through [WriteRowIterator]. [DelimitedWriter], [JSONLWriter], and
// [SQLInsertWriter] implement it.
type RowIteratorWriter interface {
	FlushWriter
	PrepareMetadata(*sppb.ResultSetMetadata) error
}

// RowIteratorHooksFromWriter returns hooks that register metadata, write each row,
// and call [Flusher.Flush] in Finish (ignoring the result).
func RowIteratorHooksFromWriter(w RowIteratorWriter) RowIteratorHooks {
	return RowIteratorHooks{
		PrepareMetadata: w.PrepareMetadata,
		WriteRow:        w.WriteRow,
		Finish: func(*RowIteratorResult) error {
			return w.Flush()
		},
	}
}

// RunRowIterator streams all rows from iter using hooks. It always calls
// [spanner.RowIterator.Stop] on return.
//
// The returned [RowIteratorResult] reflects iter.Metadata and iter stats fields
// after the loop (including when no data rows were written). When hooks.Finish is
// set, it receives the same pointer that is returned.
func RunRowIterator(iter *spanner.RowIterator, hooks RowIteratorHooks) (*RowIteratorResult, error) {
	if iter == nil {
		return nil, ErrNilRowIterator
	}
	fac := spannerRowIteratorFacade{iter}
	defer fac.stop()
	return runRowIterator(fac, hooks)
}

// WriteRowIterator streams all rows from iter into w using [RowIteratorHooksFromWriter].
// See [RunRowIterator] for iterator metadata, stats, and zero-row behavior.
func WriteRowIterator(iter *spanner.RowIterator, w RowIteratorWriter) (*RowIteratorResult, error) {
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
	outcome := func() *RowIteratorResult {
		return &RowIteratorResult{
			Metadata: fac.metadata(),
			Stats:    fac.stats(),
		}
	}

	first := true
	for {
		row, err := fac.next()
		if first {
			first = false
			if hooks.PrepareMetadata != nil {
				if md := fac.metadata(); md != nil {
					if err := hooks.PrepareMetadata(md); err != nil {
						return outcome(), err
					}
				}
			}
		}
		if err == iterator.Done {
			break
		}
		if err != nil {
			return outcome(), err
		}
		if hooks.WriteRow != nil {
			if err := hooks.WriteRow(row); err != nil {
				return outcome(), err
			}
		}
	}
	result := outcome()
	if hooks.Finish != nil {
		if err := hooks.Finish(result); err != nil {
			return result, err
		}
	}
	return result, nil
}
