package spanvalue

import (
	"errors"
	"iter"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/google/go-cmp/cmp"
)

func mustRow(t *testing.T, values ...any) *spanner.Row {
	t.Helper()
	names := make([]string, len(values))
	for i := range names {
		names[i] = "col"
	}
	row, err := spanner.NewRow(names, values)
	if err != nil {
		t.Fatal(err)
	}
	return row
}

func collectFormattedRows(seq iter.Seq2[[]string, error]) ([][]string, []error) {
	var values [][]string
	var errs []error
	for value, err := range seq {
		values = append(values, value)
		errs = append(errs, err)
	}
	return values, errs
}

func TestFormatRowSeq(t *testing.T) {
	t.Parallel()

	row1 := mustRow(t, int64(1), "a")
	row2 := mustRow(t, int64(2), "b")
	rows := func(yield func(*spanner.Row, error) bool) {
		for _, row := range []*spanner.Row{row1, row2} {
			if !yield(row, nil) {
				return
			}
		}
	}

	got, errs := collectFormattedRows(SimpleFormatConfig().FormatRowSeq(rows))
	if diff := cmp.Diff([][]string{{"1", "a"}, {"2", "b"}}, got); diff != "" {
		t.Errorf("formatted rows mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]error{nil, nil}, errs); diff != "" {
		t.Errorf("errors mismatch (-want +got):\n%s", diff)
	}
}

func TestFormatRowSeqEmpty(t *testing.T) {
	t.Parallel()

	empty := func(func(*spanner.Row, error) bool) {}
	values, errs := collectFormattedRows(SimpleFormatConfig().FormatRowSeq(empty))
	if values != nil || errs != nil {
		t.Fatalf("got values %v and errors %v, want no yields", values, errs)
	}
}

func TestFormatRowSeqNilArguments(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		seq  iter.Seq2[[]string, error]
		want error
	}{
		{
			name: "nil format config",
			seq:  (*FormatConfig)(nil).FormatRowSeq(func(func(*spanner.Row, error) bool) {}),
			want: ErrNilFormatConfig,
		},
		{
			name: "nil row sequence",
			seq:  SimpleFormatConfig().FormatRowSeq(nil),
			want: ErrNilRowSeq,
		},
		{
			name: "nil format config takes precedence",
			seq:  (*FormatConfig)(nil).FormatRowSeq(nil),
			want: ErrNilFormatConfig,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			values, errs := collectFormattedRows(tt.seq)
			if diff := cmp.Diff([][]string{nil}, values); diff != "" {
				t.Errorf("values mismatch (-want +got):\n%s", diff)
			}
			if len(errs) != 1 || !errors.Is(errs[0], tt.want) {
				t.Fatalf("errors = %v, want one %v", errs, tt.want)
			}
		})
	}
}

func TestFormatRowSeqStopsOnSourceError(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("source error")
	reachedAfterError := false
	rows := func(yield func(*spanner.Row, error) bool) {
		if !yield(mustRow(t, int64(1)), nil) {
			return
		}
		if !yield(mustRow(t, int64(2)), wantErr) {
			return
		}
		reachedAfterError = true
		yield(mustRow(t, int64(3)), nil)
	}

	values, errs := collectFormattedRows(SimpleFormatConfig().FormatRowSeq(rows))
	if diff := cmp.Diff([][]string{{"1"}, nil}, values); diff != "" {
		t.Errorf("values mismatch (-want +got):\n%s", diff)
	}
	if len(errs) != 2 || errs[0] != nil || !errors.Is(errs[1], wantErr) {
		t.Fatalf("errors = %v, want unchanged source error", errs)
	}
	if reachedAfterError {
		t.Fatal("source was consumed after its error")
	}
}

func TestFormatRowSeqStopsOnRowError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		fc   *FormatConfig
		row  *spanner.Row
		want error
	}{
		{
			name: "nil row",
			fc:   SimpleFormatConfig(),
			want: ErrNilRow,
		},
		{
			name: "format error",
			fc:   &FormatConfig{},
			row:  mustRow(t, int64(1)),
			want: ErrUnhandledValue,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			reachedSecondRow := false
			rows := func(yield func(*spanner.Row, error) bool) {
				if !yield(tt.row, nil) {
					return
				}
				reachedSecondRow = true
				yield(mustRow(t, int64(2)), nil)
			}

			values, errs := collectFormattedRows(tt.fc.FormatRowSeq(rows))
			if diff := cmp.Diff([][]string{nil}, values); diff != "" {
				t.Errorf("values mismatch (-want +got):\n%s", diff)
			}
			if len(errs) != 1 || !errors.Is(errs[0], tt.want) {
				t.Fatalf("errors = %v, want one %v", errs, tt.want)
			}
			if reachedSecondRow {
				t.Fatal("source was consumed after the row error")
			}
		})
	}
}

func TestFormatRowSeqConsumerStopsEarly(t *testing.T) {
	t.Parallel()

	pulled := 0
	rows := func(yield func(*spanner.Row, error) bool) {
		for i := range 3 {
			pulled++
			if !yield(mustRow(t, int64(i)), nil) {
				return
			}
		}
	}

	for range SimpleFormatConfig().FormatRowSeq(rows) {
		break
	}
	if pulled != 1 {
		t.Fatalf("pulled = %d, want 1", pulled)
	}
}

func TestFormatRowSeqReturnsIndependentSlices(t *testing.T) {
	t.Parallel()

	row := mustRow(t, int64(1))
	rows := func(yield func(*spanner.Row, error) bool) {
		if !yield(row, nil) {
			return
		}
		yield(row, nil)
	}

	values, errs := collectFormattedRows(SimpleFormatConfig().FormatRowSeq(rows))
	if diff := cmp.Diff([]error{nil, nil}, errs); diff != "" {
		t.Fatalf("errors mismatch (-want +got):\n%s", diff)
	}
	values[0][0] = "changed"
	if values[1][0] != "1" {
		t.Fatalf("second row changed with first slice: %q", values[1][0])
	}
}
