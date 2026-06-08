package dbsqlrows

import (
	"bytes"
	"database/sql"
	"errors"
	"testing"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/apstndb/spanvalue"
	"github.com/apstndb/spanvalue/gcvctor"
	"github.com/apstndb/spanvalue/writer"
)

var _ rowsFacade = (*stubSQLRows)(nil)

type stubSQLRows struct {
	resultSets [][]stubRow
	set        int
	row        int
	scanErr    error
	nextRSOK   bool
	columns    []string
}

type stubRow struct {
	values []any
}

func (s *stubSQLRows) next() bool {
	if s.scanErr != nil {
		return false
	}
	if s.set >= len(s.resultSets) {
		return false
	}
	if s.row >= len(s.resultSets[s.set]) {
		return false
	}
	s.row++
	return true
}

func (s *stubSQLRows) nextResultSet() bool {
	if s.set+1 >= len(s.resultSets) {
		s.nextRSOK = false
		return false
	}
	s.set++
	s.row = 0
	s.nextRSOK = true
	return true
}

func (s *stubSQLRows) scan(dest ...any) error {
	if s.scanErr != nil {
		return s.scanErr
	}
	if s.row == 0 || s.row > len(s.resultSets[s.set]) {
		return errors.New("stubSQLRows: scan without prior Next")
	}
	row := s.resultSets[s.set][s.row-1]
	if len(dest) != len(row.values) {
		return errors.New("stubSQLRows: scan arity mismatch")
	}
	for i, d := range dest {
		switch target := d.(type) {
		case *spanner.GenericColumnValue:
			v, ok := row.values[i].(spanner.GenericColumnValue)
			if !ok {
				return errors.New("stubSQLRows: expected GenericColumnValue")
			}
			*target = v
		case **sppb.ResultSetMetadata:
			v, ok := row.values[i].(*sppb.ResultSetMetadata)
			if !ok {
				return errors.New("stubSQLRows: expected *ResultSetMetadata")
			}
			*target = v
		case **sppb.ResultSetStats:
			v, ok := row.values[i].(*sppb.ResultSetStats)
			if !ok {
				return errors.New("stubSQLRows: expected *ResultSetStats")
			}
			*target = v
		default:
			return errors.New("stubSQLRows: unsupported scan target")
		}
	}
	return nil
}

func (s *stubSQLRows) columnCount() (int, error) {
	if len(s.columns) > 0 {
		return len(s.columns), nil
	}
	if s.set >= len(s.resultSets) || len(s.resultSets[s.set]) == 0 {
		return 0, nil
	}
	return len(s.resultSets[s.set][0].values), nil
}

func (s *stubSQLRows) err() error {
	return nil
}

func metadataWithNames(names ...string) *sppb.ResultSetMetadata {
	fields := make([]*sppb.StructType_Field, len(names))
	for i, name := range names {
		code := sppb.TypeCode_INT64
		if name == "name" {
			code = sppb.TypeCode_STRING
		}
		fields[i] = &sppb.StructType_Field{
			Name: name,
			Type: &sppb.Type{Code: code},
		}
	}
	return &sppb.ResultSetMetadata{
		RowType: &sppb.StructType{Fields: fields},
	}
}

func TestExportRows_nilRows(t *testing.T) {
	t.Parallel()

	_, err := ExportRows(nil, &stubGCVWriter{}, ExportConfig{})
	if !errors.Is(err, ErrNilRows) {
		t.Fatalf("error = %v, want ErrNilRows", err)
	}
}

func TestExportRows_nilWriter(t *testing.T) {
	t.Parallel()

	_, err := ExportRows(&sql.Rows{}, nil, ExportConfig{})
	if !errors.Is(err, ErrNilWriter) {
		t.Fatalf("error = %v, want ErrNilWriter", err)
	}
}

func TestExportRowsAtData_nilMetadata(t *testing.T) {
	t.Parallel()

	_, err := ExportRowsAtData(&sql.Rows{}, nil, &stubGCVWriter{}, ExportConfig{})
	if !errors.Is(err, ErrNilMetadata) {
		t.Fatalf("error = %v, want ErrNilMetadata", err)
	}
}

func TestExportRows_metadataAndDataRows(t *testing.T) {
	t.Parallel()

	md := metadataWithNames("id", "name")
	stub := &stubSQLRows{
		columns: []string{"id", "name"},
		resultSets: [][]stubRow{
			{{values: []any{md}}},
			{
				{values: []any{
					gcvctor.Int64Value(1),
					gcvctor.StringValue("Alice"),
				}},
				{values: []any{
					gcvctor.Int64Value(2),
					gcvctor.StringValue("Bob"),
				}},
			},
		},
	}

	var out bytes.Buffer
	w, err := writer.NewCSVWriter(&out, writer.WithHeader(true))
	if err != nil {
		t.Fatal(err)
	}

	got, err := exportRows(stub, w, exportRunConfig{readMetadataPseudoRow: true})
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(md, got.Metadata, protocmp.Transform()); diff != "" {
		t.Fatalf("Metadata mismatch (-want +got):\n%s", diff)
	}
	if got.RowsRead != 2 {
		t.Fatalf("RowsRead = %d, want 2", got.RowsRead)
	}
	want := "id,name\n1,Alice\n2,Bob\n"
	if out.String() != want {
		t.Fatalf("output = %q, want %q", out.String(), want)
	}
}

func TestExportRows_zeroDataRowsFlushHeader(t *testing.T) {
	t.Parallel()

	md := metadataWithNames("id")
	stub := &stubSQLRows{
		columns: []string{"id"},
		resultSets: [][]stubRow{
			{{values: []any{md}}},
			nil,
		},
	}

	var out bytes.Buffer
	w, err := writer.NewCSVWriter(&out, writer.WithHeader(true))
	if err != nil {
		t.Fatal(err)
	}

	got, err := exportRows(stub, w, exportRunConfig{readMetadataPseudoRow: true})
	if err != nil {
		t.Fatal(err)
	}
	if got.Metadata == nil {
		t.Fatal("Metadata is nil")
	}
	if got.RowsRead != 0 {
		t.Fatalf("RowsRead = %d, want 0", got.RowsRead)
	}
	want := "id\n"
	if out.String() != want {
		t.Fatalf("output = %q, want %q", out.String(), want)
	}
}

func TestExportRowsAtData_zeroDataRowsFlushMultiColumnHeader(t *testing.T) {
	t.Parallel()

	md := metadataWithNames("id", "name", "score")
	stub := &stubSQLRows{
		columns:    []string{"id", "name", "score"},
		resultSets: [][]stubRow{nil},
	}

	var out bytes.Buffer
	w, err := writer.NewCSVWriter(&out, writer.DelimitedGCVExportOptions(
		md,
		spanvalue.SimpleFormatConfig(),
		spanvalue.IndexedUnnamedFieldNamer,
	)...)
	if err != nil {
		t.Fatal(err)
	}

	got, err := exportRows(stub, w, exportRunConfig{metadata: md})
	if err != nil {
		t.Fatal(err)
	}
	if got.RowsRead != 0 {
		t.Fatalf("RowsRead = %d, want 0", got.RowsRead)
	}
	want := "id,name,score\n"
	if out.String() != want {
		t.Fatalf("output = %q, want %q", out.String(), want)
	}
}

func TestExportRows_statsPseudoRow(t *testing.T) {
	t.Parallel()

	md := metadataWithNames("id")
	stats := &sppb.ResultSetStats{RowCount: &sppb.ResultSetStats_RowCountExact{RowCountExact: 0}}
	stub := &stubSQLRows{
		columns: []string{"id"},
		resultSets: [][]stubRow{
			{{values: []any{md}}},
			nil,
			{{values: []any{stats}}},
		},
	}

	var out bytes.Buffer
	w, err := writer.NewCSVWriter(&out, writer.WithHeader(true))
	if err != nil {
		t.Fatal(err)
	}

	got, err := exportRows(stub, w, exportRunConfig{
		readMetadataPseudoRow: true,
		readResultSetStats:    true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(stats, got.Stats, protocmp.Transform()); diff != "" {
		t.Fatalf("Stats mismatch (-want +got):\n%s", diff)
	}
}

func TestExportRows_skipsStatsByDefault(t *testing.T) {
	t.Parallel()

	md := metadataWithNames("id")
	stats := &sppb.ResultSetStats{RowCount: &sppb.ResultSetStats_RowCountExact{RowCountExact: 1}}
	stub := &stubSQLRows{
		columns: []string{"id"},
		resultSets: [][]stubRow{
			{{values: []any{md}}},
			nil,
			{{values: []any{stats}}},
		},
	}

	got, err := exportRows(stub, &stubGCVWriter{}, exportRunConfig{readMetadataPseudoRow: true})
	if err != nil {
		t.Fatal(err)
	}
	if got.Stats != nil {
		t.Fatalf("Stats = %v, want nil when stats not consumed", got.Stats)
	}
	if stub.set != 1 {
		t.Fatalf("cursor set = %d, want data result set (1) after export", stub.set)
	}
}

func TestExportRows_writeErrorPartialResult(t *testing.T) {
	t.Parallel()

	md := metadataWithNames("id")
	stub := &stubSQLRows{
		columns: []string{"id"},
		resultSets: [][]stubRow{
			{
				{values: []any{gcvctor.Int64Value(1)}},
				{values: []any{gcvctor.Int64Value(2)}},
			},
		},
	}
	wantErr := errors.New("write failed")
	sw := &stubGCVWriter{writeErr: wantErr}

	got, err := exportRows(stub, sw, exportRunConfig{
		metadata:              md,
		readMetadataPseudoRow: false,
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}
	if got.Metadata == nil {
		t.Fatal("Metadata is nil on write error")
	}
	if got.RowsRead != 0 {
		t.Fatalf("RowsRead = %d, want 0 on first-row write error", got.RowsRead)
	}
}

func TestExportRows_prepareErrorPartialResult(t *testing.T) {
	t.Parallel()

	md := metadataWithNames("id")
	stub := &stubSQLRows{
		columns: []string{"id"},
		resultSets: [][]stubRow{
			{{values: []any{gcvctor.Int64Value(1)}}},
		},
	}
	wantErr := errors.New("prepare failed")
	sw := &stubGCVWriter{prepareErr: wantErr}

	got, err := exportRows(stub, sw, exportRunConfig{
		metadata:              md,
		readMetadataPseudoRow: false,
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}
	if got.Metadata != md {
		t.Fatal("Metadata not set on prepare error")
	}
	if got.RowsRead != 0 {
		t.Fatalf("RowsRead = %d, want 0", got.RowsRead)
	}
}

func TestReadMetadataAndAdvanceToData(t *testing.T) {
	t.Parallel()

	md := metadataWithNames("id", "name")
	stub := &stubSQLRows{
		columns: []string{"id", "name"},
		resultSets: [][]stubRow{
			{{values: []any{md}}},
			{{values: []any{gcvctor.Int64Value(1), gcvctor.StringValue("x")}}},
		},
	}

	gotMD, ok, err := readMetadataAndAdvanceToData(stub)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("ok = false, want true")
	}
	if diff := cmp.Diff(md, gotMD, protocmp.Transform()); diff != "" {
		t.Fatalf("Metadata mismatch (-want +got):\n%s", diff)
	}
	if stub.set != 1 || stub.row != 0 {
		t.Fatalf("cursor set=%d row=%d, want set=1 row=0 on data result set", stub.set, stub.row)
	}
}

func TestExportRowsAtData_oneRowDelimitedGCVExportOptions(t *testing.T) {
	t.Parallel()

	md := metadataWithNames("id")
	stub := &stubSQLRows{
		columns: []string{"id"},
		resultSets: [][]stubRow{
			{{values: []any{gcvctor.Int64Value(42)}}},
		},
	}

	var out bytes.Buffer
	w, err := writer.NewCSVWriter(&out, writer.DelimitedGCVExportOptions(
		md,
		spanvalue.SimpleFormatConfig(),
		spanvalue.IndexedUnnamedFieldNamer,
	)...)
	if err != nil {
		t.Fatal(err)
	}

	got, err := exportRows(stub, w, exportRunConfig{metadata: md})
	if err != nil {
		t.Fatal(err)
	}
	if got.RowsRead != 1 {
		t.Fatalf("RowsRead = %d, want 1", got.RowsRead)
	}
	if got.Metadata != md {
		t.Fatal("Metadata not set")
	}
	if !bytes.Contains(out.Bytes(), []byte("42")) {
		t.Fatalf("output = %q, want row with 42", out.String())
	}
}

func TestExportRowsAtData_readsStatsWhenRequested(t *testing.T) {
	t.Parallel()

	md := metadataWithNames("id")
	stats := &sppb.ResultSetStats{
		RowCount: &sppb.ResultSetStats_RowCountExact{RowCountExact: 0},
	}
	stub := &stubSQLRows{
		columns: []string{"id"},
		resultSets: [][]stubRow{
			nil,
			{{values: []any{stats}}},
		},
	}

	got, err := exportRows(stub, &stubGCVWriter{}, exportRunConfig{
		metadata:           md,
		readResultSetStats: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Stats == nil {
		t.Fatal("Stats is nil")
	}
}

func TestExportRows_statsThenReadMetadataMultiStatement(t *testing.T) {
	t.Parallel()

	md1 := metadataWithNames("id")
	md2 := metadataWithNames("name")
	stats := &sppb.ResultSetStats{
		RowCount: &sppb.ResultSetStats_RowCountExact{RowCountExact: 0},
	}
	stub := &stubSQLRows{
		columns: []string{"id"},
		resultSets: [][]stubRow{
			{{values: []any{md1}}},
			nil,
			{{values: []any{stats}}},
			{{values: []any{md2}}},
			{{values: []any{gcvctor.StringValue("x")}}},
		},
	}

	got, err := exportRows(stub, &stubGCVWriter{}, exportRunConfig{
		readMetadataPseudoRow: true,
		readResultSetStats:    true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(stats, got.Stats, protocmp.Transform()); diff != "" {
		t.Fatalf("Stats mismatch (-want +got):\n%s", diff)
	}
	if stub.set != 3 || stub.row != 0 {
		t.Fatalf("cursor set=%d row=%d, want set=3 row=0 before next metadata", stub.set, stub.row)
	}

	gotMD, ok, err := readMetadataAndAdvanceToData(stub)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("ok = false, want true")
	}
	if diff := cmp.Diff(md2, gotMD, protocmp.Transform()); diff != "" {
		t.Fatalf("Metadata mismatch (-want +got):\n%s", diff)
	}
	if stub.set != 4 || stub.row != 0 {
		t.Fatalf("cursor set=%d row=%d, want set=4 row=0 on second data result set", stub.set, stub.row)
	}
}

type stubGCVWriter struct {
	writeErr   error
	flushErr   error
	prepareErr error
	flushed    bool
}

func (s *stubGCVWriter) WriteGCVs([]spanner.GenericColumnValue) error {
	return s.writeErr
}

func (s *stubGCVWriter) Flush() error {
	s.flushed = true
	return s.flushErr
}

func (s *stubGCVWriter) PrepareRowType(*sppb.StructType) error {
	return s.prepareErr
}
