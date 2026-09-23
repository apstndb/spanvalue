package writer

import (
	"fmt"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/protobuf/types/known/structpb"
)

// StatsEncoding selects how [RowIteratorResult.StatsProto] encodes RowCount.
// Writer does not record whether the run completed or whether the stats came
// from a query or from DML, so the caller passes the encoding and must check
// the iterator or export error first. This is not spaniter's captured-stats
// lifecycle: spaniter can suppress a row count after a partial error, and
// this helper cannot.
type StatsEncoding int

const (
	// StatsEncodingDefault omits row_count_exact when RowCount is zero.
	// The Go client cannot tell an absent count from an exact zero.
	StatsEncodingDefault StatsEncoding = iota
	// StatsEncodingDMLExact always encodes RowCount as row_count_exact,
	// including zero. Use it only on a completed standard-DML result.
	// A failed or partial run can still have RowCount zero; this encoding
	// will then emit row_count_exact:0 even though no completed count exists.
	StatsEncodingDMLExact
)

// StatsProto rebuilds protobuf stats from the decoded [RowIteratorStats].
//
// It does not know whether the run finished successfully. Check the error
// from [RunRowIterator] or [WriteRowIterator] before treating RowCount as a
// completed DML count. [StatsEncodingDMLExact] on a partial result can emit
// row_count_exact:0.
//
// A nil QueryStats map stays nil. An empty map becomes an empty struct.
// QueryPlan is borrowed, not cloned. When there is no plan, no query stats,
// and no row count to encode, StatsProto returns nil, nil.
// Values structpb.NewStruct cannot encode return a nil result and an error.
func (r RowIteratorResult) StatsProto(enc StatsEncoding) (*sppb.ResultSetStats, error) {
	encodeRowCount := enc == StatsEncodingDMLExact || r.Stats.RowCount != 0
	if !encodeRowCount && r.Stats.QueryPlan == nil && r.Stats.QueryStats == nil {
		return nil, nil
	}

	var queryStats *structpb.Struct
	if r.Stats.QueryStats != nil {
		var err error
		queryStats, err = structpb.NewStruct(r.Stats.QueryStats)
		if err != nil {
			return nil, fmt.Errorf("encode query stats: %w", err)
		}
	}

	stats := &sppb.ResultSetStats{
		QueryPlan:  r.Stats.QueryPlan,
		QueryStats: queryStats,
	}
	if encodeRowCount {
		stats.RowCount = &sppb.ResultSetStats_RowCountExact{RowCountExact: r.Stats.RowCount}
	}
	return stats, nil
}
