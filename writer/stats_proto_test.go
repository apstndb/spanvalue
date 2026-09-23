package writer

import (
	"strings"
	"testing"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
)

func TestRowIteratorResultStatsProto(t *testing.T) {
	t.Parallel()

	t.Run("default omits zero row count", func(t *testing.T) {
		t.Parallel()
		got, err := (RowIteratorResult{Stats: RowIteratorStats{RowCount: 0}}).StatsProto(StatsEncodingDefault)
		if err != nil {
			t.Fatal(err)
		}
		if got != nil {
			t.Fatalf("StatsProto = %v, want nil", got)
		}
	})

	t.Run("DML exact includes zero row count", func(t *testing.T) {
		t.Parallel()
		got, err := (RowIteratorResult{Stats: RowIteratorStats{RowCount: 0}}).StatsProto(StatsEncodingDMLExact)
		if err != nil {
			t.Fatal(err)
		}
		exact, ok := got.GetRowCount().(*sppb.ResultSetStats_RowCountExact)
		if !ok || exact.RowCountExact != 0 {
			t.Fatalf("RowCount = %#v, want exact 0", got.GetRowCount())
		}
	})

	t.Run("non-zero row count and plan", func(t *testing.T) {
		t.Parallel()
		plan := &sppb.QueryPlan{}
		got, err := (RowIteratorResult{Stats: RowIteratorStats{
			QueryPlan:  plan,
			QueryStats: map[string]any{"elapsed_time": "1 ms"},
			RowCount:   2,
		}}).StatsProto(StatsEncodingDefault)
		if err != nil {
			t.Fatal(err)
		}
		if got.GetQueryPlan() != plan {
			t.Fatal("QueryPlan was not the borrowed pointer")
		}
		if got.GetQueryStats().AsMap()["elapsed_time"] != "1 ms" {
			t.Fatalf("QueryStats = %v", got.GetQueryStats().AsMap())
		}
		if got.GetRowCountExact() != 2 {
			t.Fatalf("RowCountExact = %d, want 2", got.GetRowCountExact())
		}
	})

	t.Run("nil vs empty query stats", func(t *testing.T) {
		t.Parallel()
		gotNil, err := (RowIteratorResult{}).StatsProto(StatsEncodingDefault)
		if err != nil {
			t.Fatal(err)
		}
		if gotNil != nil {
			t.Fatalf("StatsProto = %v, want nil", gotNil)
		}
		gotEmpty, err := (RowIteratorResult{Stats: RowIteratorStats{QueryStats: map[string]any{}}}).StatsProto(StatsEncodingDefault)
		if err != nil {
			t.Fatal(err)
		}
		if gotEmpty == nil || gotEmpty.GetQueryStats() == nil {
			t.Fatalf("StatsProto = %v, want empty query_stats", gotEmpty)
		}
	})

	t.Run("query stats with zero row count omit the count", func(t *testing.T) {
		t.Parallel()
		got, err := (RowIteratorResult{Stats: RowIteratorStats{
			QueryPlan:  &sppb.QueryPlan{},
			QueryStats: map[string]any{"elapsed_time": "1 ms"},
		}}).StatsProto(StatsEncodingDefault)
		if err != nil {
			t.Fatal(err)
		}
		if got.GetQueryStats() == nil || got.GetQueryPlan() == nil {
			t.Fatalf("StatsProto = %v, want plan and query stats", got)
		}
		if got.GetRowCount() != nil {
			t.Fatalf("RowCount = %#v, want absent", got.GetRowCount())
		}
	})

	t.Run("query stats encoding error", func(t *testing.T) {
		t.Parallel()
		got, err := (RowIteratorResult{Stats: RowIteratorStats{
			QueryStats: map[string]any{"unsupported": func() {}},
		}}).StatsProto(StatsEncodingDefault)
		if got != nil || err == nil || !strings.Contains(err.Error(), "encode query stats") {
			t.Fatalf("StatsProto = (%v, %v), want nil result and encode query stats", got, err)
		}
	})
}
