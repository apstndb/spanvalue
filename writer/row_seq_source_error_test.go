package writer

import (
	"errors"
	"fmt"
	"testing"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/api/iterator"
)

func TestRunRowSeqPreservesDoneErrors(t *testing.T) {
	t.Parallel()
	for _, sourceErr := range []error{iterator.Done, fmt.Errorf("wrapped: %w", iterator.Done), errors.Join(errors.New("source failure"), iterator.Done)} {
		for _, deferred := range []bool{false, true} {
			for _, firstRow := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/deferred=%v/firstRow=%v", sourceErr, deferred, firstRow), func(t *testing.T) {
					t.Parallel()
					row := mustNewSpannerRow(t, []string{"id"}, []any{int64(1)})
					md := metadataWithColumnNames("id")
					var released, prepared, written, finished, afterError int
					rows := func(yield func(*spanner.Row, error) bool) {
						defer func() { released++ }()
						if firstRow && !yield(row, nil) {
							return
						}
						if !yield(row, sourceErr) {
							return
						}
						afterError++
					}
					hooks := RowIteratorHooks{
						PrepareMetadata: func(*sppb.ResultSetMetadata) error { prepared++; return nil },
						WriteRow:        func(*spanner.Row) error { written++; return nil },
						Finish:          func(*RowIteratorResult) error { finished++; return nil },
					}
					var result *RowIteratorResult
					var err error
					if deferred {
						result, err = RunRowSeqDeferredMetadata(func() *sppb.ResultSetMetadata { return md }, rows, hooks)
					} else {
						result, err = RunRowSeq(md, rows, hooks)
					}
					if err != sourceErr { //nolint:errorlint // Assert the original error identity, not just an errors.Is match.
						t.Errorf("error identity changed: got %v, want %v", err, sourceErr)
					}
					wantRows := 0
					if firstRow {
						wantRows = 1
					}
					if released != 1 || finished != 0 || afterError != 0 || prepared != wantRows || written != wantRows {
						t.Errorf("released=%d finished=%d afterError=%d prepared=%d written=%d", released, finished, afterError, prepared, written)
					}
					if result == nil || result.RowsRead != wantRows || result.Metadata != md {
						t.Errorf("result=%+v", result)
					}
				})
			}
		}
	}
}
