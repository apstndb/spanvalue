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
	resultSets  [][]stubRow
	set         int
	row         int
	scanErr     error
	nextErr     error
	nextErrOn   int
	nextCalls   int
	nextRSErr   error
	nextRSErrOn int
	nextRSCalls int
	lastErr     error
	nextRSOK    bool
	columns     []string
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
	s.nextCalls++
	errOn := s.nextErrOn
	if s.nextErr != nil && errOn != 0 && s.nextCalls == errOn {
		s.lastErr = s.nextErr
		return false
	}
	s.row++
	return true
}

func (s *stubSQLRows) nextResultSet() bool {
	s.nextRSCalls++
	errOn := s.nextRSErrOn
	if errOn == 0 {
		errOn = 1
	}
	if s.nextRSErr != nil && s.nextRSCalls == errOn {
		s.lastErr = s.nextRSErr
		return false
	}
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
	return s.lastErr
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

func TestWriteRows_nilRows(t *testing.T) {
	t.Parallel()

	_, err := WriteRows(nil, &stubGCVWriter{}, SQLRowsConfig{})
	if !errors.Is(err, ErrNilRows) {
		t.Fatalf("error = %v, want ErrNilRows", err)
	}
}

func TestWriteRows_nilWriter(t *testing.T) {
	t.Parallel()

	_, err := WriteRows(&sql.Rows{}, nil, SQLRowsConfig{})
	if !errors.Is(err, ErrNilWriter) {
		t.Fatalf("error = %v, want ErrNilWriter", err)
	}
}

func TestWriteRowsAtData_nilMetadata(t *testing.T) {
	t.Parallel()

	_, err := WriteRowsAtData(&sql.Rows{}, nil, &stubGCVWriter{}, SQLRowsConfig{})
	if !errors.Is(err, ErrNilMetadata) {
		t.Fatalf("error = %v, want ErrNilMetadata", err)
	}
}

func TestWriteRows_metadataAndDataRows(t *testing.T) {
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

	got, err := runRowsWithGCVWriter(stub, w, sqlRowsRunConfig{readMetadataPseudoRow: true})
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

func TestWriteRows_zeroDataRowsFlushHeader(t *testing.T) {
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

	got, err := runRowsWithGCVWriter(stub, w, sqlRowsRunConfig{readMetadataPseudoRow: true})
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

func TestWriteRowsAtData_zeroDataRowsFlushMultiColumnHeader(t *testing.T) {
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

	got, err := runRowsWithGCVWriter(stub, w, sqlRowsRunConfig{metadata: md})
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

func TestWriteRows_statsPseudoRow(t *testing.T) {
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

	got, err := runRowsWithGCVWriter(stub, w, sqlRowsRunConfig{
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

func TestWriteRows_skipsStatsByDefault(t *testing.T) {
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

	got, err := runRowsWithGCVWriter(stub, &stubGCVWriter{}, sqlRowsRunConfig{readMetadataPseudoRow: true})
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

func TestWriteRows_writeErrorPartialResult(t *testing.T) {
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

	got, err := runRowsWithGCVWriter(stub, sw, sqlRowsRunConfig{
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

func TestWriteRows_nextResultSetErrorAfterMetadataPartialResult(t *testing.T) {
	t.Parallel()

	md := metadataWithNames("id")
	wantErr := errors.New("next result set failed")
	stub := &stubSQLRows{
		nextRSErr: wantErr,
		resultSets: [][]stubRow{
			{{values: []any{md}}},
		},
	}

	got, err := runRowsWithGCVWriter(stub, &stubGCVWriter{}, sqlRowsRunConfig{readMetadataPseudoRow: true})
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}
	if got.Metadata == nil {
		t.Fatal("Metadata is nil on nextResultSet error after metadata")
	}
	if diff := cmp.Diff(md, got.Metadata, protocmp.Transform()); diff != "" {
		t.Fatalf("Metadata mismatch (-want +got):\n%s", diff)
	}
}

func TestReadMetadataAndAdvanceToData_missingDataResultSet(t *testing.T) {
	t.Parallel()

	md := metadataWithNames("id")
	stub := &stubSQLRows{
		resultSets: [][]stubRow{
			{{values: []any{md}}},
		},
	}

	_, ok, err := readMetadataAndAdvanceToData(stub)
	if !errors.Is(err, ErrMissingDataResultSet) {
		t.Fatalf("error = %v, want ErrMissingDataResultSet", err)
	}
	if ok {
		t.Fatal("ok = true, want false")
	}
}

func TestReadMetadataAndAdvanceToData_nextResultSetError(t *testing.T) {
	t.Parallel()

	md := metadataWithNames("id")
	wantErr := errors.New("next result set failed")
	stub := &stubSQLRows{
		nextRSErr: wantErr,
		resultSets: [][]stubRow{
			{{values: []any{md}}},
		},
	}

	_, ok, err := readMetadataAndAdvanceToData(stub)
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}
	if ok {
		t.Fatal("ok = true, want false")
	}
}

func TestWriteRows_missingDataResultSet(t *testing.T) {
	t.Parallel()

	md := metadataWithNames("id")
	stub := &stubSQLRows{
		resultSets: [][]stubRow{
			{{values: []any{md}}},
		},
	}

	got, err := runRowsWithGCVWriter(stub, &stubGCVWriter{}, sqlRowsRunConfig{readMetadataPseudoRow: true})
	if !errors.Is(err, ErrMissingDataResultSet) {
		t.Fatalf("error = %v, want ErrMissingDataResultSet", err)
	}
	if got.Metadata == nil {
		t.Fatal("Metadata is nil on missing data result set")
	}
	if diff := cmp.Diff(md, got.Metadata, protocmp.Transform()); diff != "" {
		t.Fatalf("Metadata mismatch (-want +got):\n%s", diff)
	}
}

func TestWriteRows_missingStatsRow(t *testing.T) {
	t.Parallel()

	md := metadataWithNames("id")
	stub := &stubSQLRows{
		columns: []string{"id"},
		resultSets: [][]stubRow{
			{{values: []any{md}}},
			nil,
			nil,
		},
	}

	_, err := runRowsWithGCVWriter(stub, &stubGCVWriter{}, sqlRowsRunConfig{
		readMetadataPseudoRow: true,
		readResultSetStats:    true,
	})
	if !errors.Is(err, ErrMissingStatsRow) {
		t.Fatalf("error = %v, want ErrMissingStatsRow", err)
	}
}

func TestWriteRows_statsNextError(t *testing.T) {
	t.Parallel()

	md := metadataWithNames("id")
	wantErr := errors.New("stats next failed")
	stats := &sppb.ResultSetStats{RowCount: &sppb.ResultSetStats_RowCountExact{RowCountExact: 1}}
	stub := &stubSQLRows{
		columns:   []string{"id"},
		nextErr:   wantErr,
		nextErrOn: 2,
		resultSets: [][]stubRow{
			{{values: []any{md}}},
			nil,
			{{values: []any{stats}}},
		},
	}

	got, err := runRowsWithGCVWriter(stub, &stubGCVWriter{}, sqlRowsRunConfig{
		readMetadataPseudoRow: true,
		readResultSetStats:    true,
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}
	if got.Metadata == nil {
		t.Fatal("Metadata is nil on stats next error")
	}
}

func TestWriteRows_statsNextResultSetError(t *testing.T) {
	t.Parallel()

	md := metadataWithNames("id")
	stats := &sppb.ResultSetStats{RowCount: &sppb.ResultSetStats_RowCountExact{RowCountExact: 1}}
	wantErr := errors.New("stats next result set failed")
	stub := &stubSQLRows{
		columns:     []string{"id"},
		nextRSErr:   wantErr,
		nextRSErrOn: 3,
		resultSets: [][]stubRow{
			{{values: []any{md}}},
			nil,
			{{values: []any{stats}}},
		},
	}

	got, err := runRowsWithGCVWriter(stub, &stubGCVWriter{}, sqlRowsRunConfig{
		readMetadataPseudoRow: true,
		readResultSetStats:    true,
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}
	if got.Metadata == nil {
		t.Fatal("Metadata is nil on stats nextResultSet error")
	}
	if got.Stats != nil {
		t.Fatalf("Stats = %v, want nil on error", got.Stats)
	}
}

func TestWriteRows_prepareErrorPartialResult(t *testing.T) {
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

	got, err := runRowsWithGCVWriter(stub, sw, sqlRowsRunConfig{
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

func TestWriteRowsAtData_oneRowDelimitedGCVExportOptions(t *testing.T) {
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

	got, err := runRowsWithGCVWriter(stub, w, sqlRowsRunConfig{metadata: md})
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

func TestWriteRowsAtData_readsStatsWhenRequested(t *testing.T) {
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

	got, err := runRowsWithGCVWriter(stub, &stubGCVWriter{}, sqlRowsRunConfig{
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

func TestWriteRows_statsThenReadMetadataMultiStatement(t *testing.T) {
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

	got, err := runRowsWithGCVWriter(stub, &stubGCVWriter{}, sqlRowsRunConfig{
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

func TestRunRowsAtData_zeroRowsPrepareAndFinish(t *testing.T) {
	t.Parallel()

	md := metadataWithNames("id", "name", "score")
	stub := &stubSQLRows{
		columns:    []string{"id", "name", "score"},
		resultSets: [][]stubRow{nil},
	}

	var prepared bool
	var finished bool
	var writeCalls int

	hooks := NewSQLRowsHooks().
		WithPrepareMetadata(func(got *sppb.ResultSetMetadata) error {
			prepared = true
			if diff := cmp.Diff(md, got, protocmp.Transform()); diff != "" {
				t.Fatalf("PrepareMetadata metadata mismatch (-want +got):\n%s", diff)
			}
			return nil
		}).
		WithWriteDataRow(func([]spanner.GenericColumnValue) error {
			writeCalls++
			return nil
		}).
		WithFinish(func(*SQLRowsResult) error {
			finished = true
			return nil
		})

	got, err := runRows(stub, hooks, sqlRowsRunConfig{metadata: md})
	if err != nil {
		t.Fatal(err)
	}
	if !prepared || !finished {
		t.Fatalf("prepared=%v finished=%v, want both true", prepared, finished)
	}
	if writeCalls != 0 {
		t.Fatalf("WriteDataRow calls = %d, want 0", writeCalls)
	}
	if got.RowsRead != 0 {
		t.Fatalf("RowsRead = %d, want 0", got.RowsRead)
	}
}

func TestRunRowsAtData_writeErrorPartialResult(t *testing.T) {
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
	wantErr := errors.New("write row failed")

	got, err := runRows(stub, NewSQLRowsHooks().
		WithPrepareMetadata(func(*sppb.ResultSetMetadata) error { return nil }).
		WithWriteDataRow(func([]spanner.GenericColumnValue) error { return wantErr }),
		sqlRowsRunConfig{metadata: md})
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}
	if got.Metadata != md {
		t.Fatal("Metadata not set on write error")
	}
	if got.RowsRead != 0 {
		t.Fatalf("RowsRead = %d, want 0 on first-row write error", got.RowsRead)
	}
}

func TestRunRowsAtData_prepareErrorPartialResult(t *testing.T) {
	t.Parallel()

	md := metadataWithNames("id")
	stub := &stubSQLRows{
		columns: []string{"id"},
		resultSets: [][]stubRow{
			{{values: []any{gcvctor.Int64Value(1)}}},
		},
	}
	wantErr := errors.New("prepare failed")

	got, err := runRows(stub, NewSQLRowsHooks().
		WithPrepareMetadata(func(*sppb.ResultSetMetadata) error { return wantErr }),
		sqlRowsRunConfig{metadata: md})
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

func TestRunRowsAtData_tableShapedHooks(t *testing.T) {
	t.Parallel()

	md := metadataWithNames("id", "name")
	stub := &stubSQLRows{
		columns: []string{"id", "name"},
		resultSets: [][]stubRow{
			{
				{values: []any{gcvctor.Int64Value(1), gcvctor.StringValue("Alice")}},
				{values: []any{gcvctor.Int64Value(2), gcvctor.StringValue("Bob")}},
			},
		},
	}

	var header []string
	var rows [][]string

	hooks := NewSQLRowsHooks().
		WithPrepareMetadata(func(m *sppb.ResultSetMetadata) error {
			for _, f := range m.GetRowType().GetFields() {
				header = append(header, f.GetName())
			}
			return nil
		}).
		WithWriteDataRow(func(gcvs []spanner.GenericColumnValue) error {
			row := make([]string, len(gcvs))
			for i, gcv := range gcvs {
				s, err := spanvalue.FormatColumnLiteral(gcv)
				if err != nil {
					return err
				}
				row[i] = s
			}
			rows = append(rows, row)
			return nil
		}).
		WithFinish(func(res *SQLRowsResult) error {
			if res.RowsRead != len(rows) {
				return errors.New("finish: row count mismatch")
			}
			return nil
		})

	got, err := runRows(stub, hooks, sqlRowsRunConfig{metadata: md})
	if err != nil {
		t.Fatal(err)
	}
	if got.RowsRead != 2 {
		t.Fatalf("RowsRead = %d, want 2", got.RowsRead)
	}
	wantHeader := []string{"id", "name"}
	if diff := cmp.Diff(wantHeader, header); diff != "" {
		t.Fatalf("header mismatch (-want +got):\n%s", diff)
	}
	wantRows := [][]string{{"1", `"Alice"`}, {"2", `"Bob"`}}
	if diff := cmp.Diff(wantRows, rows); diff != "" {
		t.Fatalf("rows mismatch (-want +got):\n%s", diff)
	}
}

func TestRunRowsAtData_emptyHooksDrainWithStats(t *testing.T) {
	t.Parallel()

	md := metadataWithNames("id")
	stats := &sppb.ResultSetStats{
		RowCount: &sppb.ResultSetStats_RowCountExact{RowCountExact: 2},
	}
	stub := &stubSQLRows{
		columns: []string{"id"},
		resultSets: [][]stubRow{
			{
				{values: []any{gcvctor.Int64Value(1)}},
				{values: []any{gcvctor.Int64Value(2)}},
			},
			{{values: []any{stats}}},
		},
	}

	got, err := runRows(stub, NewSQLRowsHooks(), sqlRowsRunConfig{
		metadata:           md,
		readResultSetStats: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.RowsRead != 2 {
		t.Fatalf("RowsRead = %d, want 2", got.RowsRead)
	}
	if diff := cmp.Diff(stats, got.Stats, protocmp.Transform()); diff != "" {
		t.Fatalf("Stats mismatch (-want +got):\n%s", diff)
	}
}

func TestSQLRowsHooksFromGCVWriter_matchesWriteRowsAtData(t *testing.T) {
	t.Parallel()

	md := metadataWithNames("id")
	stub := &stubSQLRows{
		columns: []string{"id"},
		resultSets: [][]stubRow{
			{{values: []any{gcvctor.Int64Value(7)}}},
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

	got, err := runRows(stub, SQLRowsHooksFromGCVWriter(w), sqlRowsRunConfig{metadata: md})
	if err != nil {
		t.Fatal(err)
	}
	if got.RowsRead != 1 {
		t.Fatalf("RowsRead = %d, want 1", got.RowsRead)
	}
	if !bytes.Contains(out.Bytes(), []byte("7")) {
		t.Fatalf("output = %q, want row with 7", out.String())
	}
}
