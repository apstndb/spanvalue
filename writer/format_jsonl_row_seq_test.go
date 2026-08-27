package writer

import (
	"errors"
	"iter"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/google/go-cmp/cmp"

	"github.com/apstndb/spanvalue"
)

func collectJSONLRows(seq iter.Seq2[string, error]) ([]string, []error) {
	var values []string
	var errs []error
	for value, err := range seq {
		values = append(values, value)
		errs = append(errs, err)
	}
	return values, errs
}

func TestFormatJSONLRowSeq(t *testing.T) {
	t.Parallel()

	names := []string{"id", "name"}
	rows := RowSeq(
		mustNewSpannerRow(t, names, []any{int64(1), "a"}),
		mustNewSpannerRow(t, names, []any{int64(2), "b"}),
	)

	got, errs := collectJSONLRows(FormatJSONLRowSeq(nil, rows, spanvalue.IndexedUnnamedFieldNamer))
	if diff := cmp.Diff([]string{`{"id":1,"name":"a"}`, `{"id":2,"name":"b"}`}, got); diff != "" {
		t.Errorf("formatted rows mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]error{nil, nil}, errs); diff != "" {
		t.Errorf("errors mismatch (-want +got):\n%s", diff)
	}
}

func TestFormatJSONLRowSeqNilSource(t *testing.T) {
	t.Parallel()

	values, errs := collectJSONLRows(FormatJSONLRowSeq(nil, nil, nil))
	if diff := cmp.Diff([]string{""}, values); diff != "" {
		t.Errorf("values mismatch (-want +got):\n%s", diff)
	}
	if len(errs) != 1 || !errors.Is(errs[0], ErrNilRowSeq) {
		t.Fatalf("errors = %v, want one ErrNilRowSeq", errs)
	}
}

func TestFormatJSONLRowSeqStopsOnError(t *testing.T) {
	t.Parallel()

	wantSourceErr := errors.New("source error")
	tests := []struct {
		name string
		rows iter.Seq2[*spanner.Row, error]
		want error
	}{
		{
			name: "source error",
			rows: func(yield func(*spanner.Row, error) bool) {
				yield(mustNewSpannerRow(t, []string{"ignored"}, []any{int64(1)}), wantSourceErr)
			},
			want: wantSourceErr,
		},
		{
			name: "nil row",
			rows: RowSeq(nil),
			want: ErrNilRow,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			values, errs := collectJSONLRows(FormatJSONLRowSeq(nil, tt.rows, nil))
			if diff := cmp.Diff([]string{""}, values); diff != "" {
				t.Errorf("values mismatch (-want +got):\n%s", diff)
			}
			if len(errs) != 1 || !errors.Is(errs[0], tt.want) {
				t.Fatalf("errors = %v, want one %v", errs, tt.want)
			}
		})
	}
}

func TestFormatJSONLRowSeqStopsOnSourceErrorAfterSuccess(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("source error")
	reachedAfterError := false
	rows := func(yield func(*spanner.Row, error) bool) {
		if !yield(mustNewSpannerRow(t, []string{"id"}, []any{int64(1)}), nil) {
			return
		}
		if !yield(mustNewSpannerRow(t, []string{"ignored"}, []any{int64(2)}), wantErr) {
			return
		}
		reachedAfterError = true
		yield(mustNewSpannerRow(t, []string{"id"}, []any{int64(3)}), nil)
	}

	values, errs := collectJSONLRows(FormatJSONLRowSeq(nil, rows, nil))
	if diff := cmp.Diff([]string{`{"id":1}`, ""}, values); diff != "" {
		t.Errorf("values mismatch (-want +got):\n%s", diff)
	}
	if len(errs) != 2 || errs[0] != nil || !errors.Is(errs[1], wantErr) {
		t.Fatalf("errors = %v, want success then unchanged source error", errs)
	}
	if reachedAfterError {
		t.Fatal("source was consumed after its error")
	}
}

func TestFormatJSONLRowSeqStopsOnFormattingError(t *testing.T) {
	t.Parallel()

	reachedSecondRow := false
	rows := func(yield func(*spanner.Row, error) bool) {
		if !yield(mustNewSpannerRow(t, []string{""}, []any{int64(1)}), nil) {
			return
		}
		reachedSecondRow = true
		yield(mustNewSpannerRow(t, []string{"id"}, []any{int64(2)}), nil)
	}

	values, errs := collectJSONLRows(FormatJSONLRowSeq(nil, rows, func(int) string { return "" }))
	if diff := cmp.Diff([]string{""}, values); diff != "" {
		t.Errorf("values mismatch (-want +got):\n%s", diff)
	}
	if len(errs) != 1 || errs[0] == nil {
		t.Fatalf("errors = %v, want one formatting error", errs)
	}
	if reachedSecondRow {
		t.Fatal("source was consumed after the formatting error")
	}
}

func TestFormatJSONLRowSeqConsumerStopsEarly(t *testing.T) {
	t.Parallel()

	pulled := 0
	rows := func(yield func(*spanner.Row, error) bool) {
		for i := range 3 {
			pulled++
			if !yield(mustNewSpannerRow(t, []string{"id"}, []any{int64(i)}), nil) {
				return
			}
		}
	}

	for range FormatJSONLRowSeq(nil, rows, nil) {
		break
	}
	if pulled != 1 {
		t.Fatalf("pulled = %d, want 1", pulled)
	}
}
