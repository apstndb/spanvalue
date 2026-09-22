package writer

import (
	"fmt"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/protobuf/types/known/structpb"
)

// StatsEncoding selects how [RowIteratorResult.StatsProto] encodes RowCount.
// Writer does not record whether a drain was a query or DML, so the caller
// passes the encoding. This matches the spaniter policy without a dependency.
type StatsEncoding int

const (
	// StatsEncodingDefault omits row_count_exact when RowCount is zero.
	// The Go client cannot tell an absent count from an exact zero.
	StatsEncodingDefault StatsEncoding = iota
	// StatsEncodingDMLExact always encodes RowCount as row_count_exact,
	// including zero. Use only when the caller knows the stats came from
	// executed standard DML, not from a plan or a read-only query.
	StatsEncodingDMLExact
)

// StatsProto rebuilds protobuf stats from the decoded [RowIteratorStats].
//
// A nil QueryStats map stays nil. An empty map becomes an empty struct.
// QueryPlan is borrowed, not cloned. When there is no plan, no query stats,
// and no row count to encode, StatsProto returns nil, nil.
// Values structpb.NewStruct cannot encode return an error and no stats.
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
