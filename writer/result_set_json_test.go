package writer

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"math"
	"strings"
	"testing"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestWriteResultSetJSONInvalidArguments(t *testing.T) {
	t.Parallel()

	if _, err := WriteResultSetJSON(nil, io.Discard, StatsEncodingDefault); !errors.Is(err, ErrNilRowIterator) {
		t.Fatalf("nil iterator error = %v, want ErrNilRowIterator", err)
	}
	if _, err := WriteResultSetJSON(&spanner.RowIterator{}, nil, StatsEncodingDefault); !errors.Is(err, ErrNilWriter) {
		t.Fatalf("nil writer error = %v, want ErrNilWriter", err)
	}
}

func TestResultSetJSONRoundTrip(t *testing.T) {
	t.Parallel()

	structType := rowTypeWithColumnNames("id")
	row, err := spanner.NewRow(
		[]string{"dup", "dup", "", "data", "json", "record", "flt"},
		[]any{
			int64(9007199254740993),
			[]string{"a", "b"},
			spanner.NullString{},
			[]byte{0, 255},
			spanner.GenericColumnValue{Type: &sppb.Type{Code: sppb.TypeCode_JSON}, Value: structpb.NewStringValue(`{"n":1}`)},
			spanner.GenericColumnValue{
				Type:  &sppb.Type{Code: sppb.TypeCode_STRUCT, StructType: structType},
				Value: structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{structpb.NewStringValue("7")}}),
			},
			spanner.GenericColumnValue{Type: &sppb.Type{Code: sppb.TypeCode_FLOAT64}, Value: structpb.NewStringValue("NaN")},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	md := &sppb.ResultSetMetadata{
		Transaction: &sppb.Transaction{Id: []byte("txn")},
		RowType:     &sppb.StructType{},
	}
	for i := range row.Size() {
		md.RowType.Fields = append(md.RowType.Fields, &sppb.StructType_Field{Name: row.ColumnName(i), Type: row.ColumnType(i)})
	}
	var out bytes.Buffer
	stub := &resultSetJSONStub{
		stubRowIterator: stubRowIterator{
			md:   md,
			rows: []*spanner.Row{row, row},
			wantStat: RowIteratorStats{
				QueryPlan:  &sppb.QueryPlan{PlanNodes: []*sppb.PlanNode{{DisplayName: "Scan"}}},
				QueryStats: map[string]any{"elapsed": "12 ms", "nested": map[string]any{"rows": 2.0}},
			},
		},
		beforeNext: func(s *stubRowIterator) {
			if s.i == 1 && !strings.Contains(out.String(), `"9007199254740993"`) {
				t.Fatal("first row must be written before requesting the second")
			}
		},
	}

	result, err := runRowIterator(stub, resultSetJSONHooks(&out, StatsEncodingDefault))
	if err != nil {
		t.Fatal(err)
	}
	if !stub.stopped || result.RowsRead != 2 || result.Metadata != md {
		t.Fatalf("iterator result = %+v, stopped = %t", result, stub.stopped)
	}
	if !strings.HasPrefix(out.String(), `{"metadata":`) {
		t.Fatalf("metadata should precede rows, got %s", out.String())
	}

	var want sppb.ResultSet
	if err := protojson.Unmarshal([]byte(`{
		"rows": [
			["9007199254740993", ["a", "b"], null, "AP8=", "{\"n\":1}", ["7"], "NaN"],
			["9007199254740993", ["a", "b"], null, "AP8=", "{\"n\":1}", ["7"], "NaN"]
		],
		"stats": {"queryPlan":{"planNodes":[{"displayName":"Scan"}]},"queryStats":{"elapsed":"12 ms","nested":{"rows":2}}}
	}`), &want); err != nil {
		t.Fatal(err)
	}
	want.Metadata = md
	assertResultSetJSON(t, out.Bytes(), &want)
}

func TestResultSetJSONEmpty(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		metadata *sppb.ResultSetMetadata
		stats    RowIteratorStats
		encoding StatsEncoding
		want     *sppb.ResultSetStats
	}{
		{name: "no metadata or stats"},
		{name: "metadata only", metadata: metadataWithColumnNames("id")},
		{
			name:  "stats only",
			stats: RowIteratorStats{QueryStats: map[string]any{}},
			want:  &sppb.ResultSetStats{QueryStats: &structpb.Struct{}},
		},
		{
			name:     "exact zero",
			encoding: StatsEncodingDMLExact,
			want:     &sppb.ResultSetStats{RowCount: &sppb.ResultSetStats_RowCountExact{RowCountExact: 0}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var out bytes.Buffer
			stub := &stubRowIterator{md: tc.metadata, wantStat: tc.stats}
			if _, err := runRowIterator(stub, resultSetJSONHooks(&out, tc.encoding)); err != nil {
				t.Fatal(err)
			}
			assertResultSetJSON(t, out.Bytes(), &sppb.ResultSet{Metadata: tc.metadata, Stats: tc.want})

			var fields map[string]json.RawMessage
			if err := json.Unmarshal(out.Bytes(), &fields); err != nil {
				t.Fatal(err)
			}
			if string(fields["rows"]) != "[]" || (tc.metadata == nil && fields["metadata"] != nil) {
				t.Fatalf("unexpected empty result framing: %s", out.Bytes())
			}
		})
	}
}

func TestResultSetJSONSourceErrors(t *testing.T) {
	t.Parallel()

	cause := errors.New("source failed")
	for _, afterRow := range []bool{false, true} {
		var out bytes.Buffer
		stub := &resultSetJSONStub{
			stubRowIterator: stubRowIterator{rows: []*spanner.Row{resultSetJSONRow(t, int64(1))}},
			beforeNext: func(s *stubRowIterator) {
				if !afterRow || s.i > 0 {
					s.err = cause
				}
			},
		}
		result, err := runRowIterator(stub, resultSetJSONHooks(&out, StatsEncodingDefault))
		if !errors.Is(err, cause) || !stub.stopped {
			t.Fatalf("source error = %v, stopped = %t", err, stub.stopped)
		}
		if json.Valid(out.Bytes()) || !afterRow && out.Len() != 0 ||
			afterRow && (!strings.Contains(out.String(), `["1"]`) || result.RowsRead != 1) {
			t.Fatalf("unexpected output after source failure: %q, rows = %d", out.String(), result.RowsRead)
		}
	}
}

func TestResultSetJSONEncodingErrors(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		cell any
	}{
		{name: "non-finite number", cell: math.Inf(1)},
		{name: "unset value", cell: spanner.GenericColumnValue{
			Type: &sppb.Type{Code: sppb.TypeCode_STRING}, Value: &structpb.Value{},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var out bytes.Buffer
			stub := &stubRowIterator{rows: []*spanner.Row{resultSetJSONRow(t, tc.cell)}}
			result, err := runRowIterator(stub, resultSetJSONHooks(&out, StatsEncodingDefault))
			if err == nil || !strings.Contains(err.Error(), "column 0") || result.RowsRead != 0 || json.Valid(out.Bytes()) {
				t.Fatalf("output = %q, rows = %d, err = %v", out.String(), result.RowsRead, err)
			}
		})
	}

	t.Run("stats", func(t *testing.T) {
		t.Parallel()
		var out bytes.Buffer
		stub := &stubRowIterator{
			rows:     []*spanner.Row{resultSetJSONRow(t, int64(1))},
			wantStat: RowIteratorStats{QueryStats: map[string]any{"unsupported": []string{"x"}}},
		}
		result, err := runRowIterator(stub, resultSetJSONHooks(&out, StatsEncodingDefault))
		if err == nil || !strings.Contains(err.Error(), "encode query stats") || result.RowsRead != 1 || json.Valid(out.Bytes()) {
			t.Fatalf("output = %q, rows = %d, err = %v", out.String(), result.RowsRead, err)
		}
	})
}

func TestResultSetJSONMetadataChanged(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	stub := &resultSetJSONStub{
		stubRowIterator: stubRowIterator{md: metadataWithColumnNames("id"), rows: []*spanner.Row{resultSetJSONRow(t, int64(1))}},
		beforeNext: func(s *stubRowIterator) {
			if s.i > 0 {
				s.md = metadataWithColumnNames("name")
			}
		},
	}
	if _, err := runRowIterator(stub, resultSetJSONHooks(&out, StatsEncodingDefault)); err == nil ||
		!strings.Contains(err.Error(), "metadata changed") || json.Valid(out.Bytes()) {
		t.Fatalf("output = %q, err = %v", out.String(), err)
	}
}

func TestResultSetJSONSinkErrors(t *testing.T) {
	t.Parallel()

	cause := errors.New("sink failed")
	for _, tc := range []struct {
		name          string
		failAt        int
		accept        func([]byte) []byte
		err           error
		want          error
		validDocument bool
	}{
		{name: "header", failAt: 1, err: cause, want: cause},
		{name: "row", failAt: 2, err: cause, want: cause},
		{name: "short write", failAt: 2, accept: func(p []byte) []byte { return p[:len(p)-1] }, want: io.ErrShortWrite},
		{name: "footer", failAt: 3, err: cause, want: cause},
		{name: "footer fully accepted with error", failAt: 3, accept: func(p []byte) []byte { return p }, err: cause, want: cause, validDocument: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var out bytes.Buffer
			calls := 0
			sink := resultSetJSONWriterFunc(func(p []byte) (int, error) {
				calls++
				if calls == tc.failAt {
					if tc.accept == nil {
						return 0, tc.err
					}
					n, _ := out.Write(tc.accept(p))
					return n, tc.err
				}
				return out.Write(p)
			})
			stub := &stubRowIterator{rows: []*spanner.Row{resultSetJSONRow(t, int64(1))}}
			if _, err := runRowIterator(stub, resultSetJSONHooks(sink, StatsEncodingDefault)); !errors.Is(err, tc.want) ||
				calls != tc.failAt || !stub.stopped || json.Valid(out.Bytes()) != tc.validDocument {
				t.Fatalf("err = %v, calls = %d, stopped = %t, output = %q", err, calls, stub.stopped, out.String())
			}
		})
	}
}

type resultSetJSONStub struct {
	stubRowIterator
	beforeNext func(*stubRowIterator)
}

func (s *resultSetJSONStub) next() (*spanner.Row, error) {
	s.beforeNext(&s.stubRowIterator)
	return s.stubRowIterator.next()
}

type resultSetJSONWriterFunc func([]byte) (int, error)

func (f resultSetJSONWriterFunc) Write(p []byte) (int, error) { return f(p) }

func resultSetJSONRow(t *testing.T, value any) *spanner.Row {
	t.Helper()
	row, err := spanner.NewRow([]string{"value"}, []any{value})
	if err != nil {
		t.Fatal(err)
	}
	return row
}

func assertResultSetJSON(t *testing.T, output []byte, want *sppb.ResultSet) {
	t.Helper()
	var got sppb.ResultSet
	if err := protojson.Unmarshal(output, &got); err != nil {
		t.Fatalf("decode ResultSet JSON %q: %v", output, err)
	}
	if !proto.Equal(want, &got) {
		t.Fatalf("ResultSet mismatch (-want +got):\n%s", cmp.Diff(want, &got, protocmp.Transform()))
	}
}
